/// Graph Daemon: Unix socket server for Cypher queries with token auth.
///
/// Phase 1A: Read-only queries, no authentication.
/// Phase 3.2: Token-based authentication added. Clients receive a
///   CapabilityToken at connection time; each query must pass token
///   verification and scope checks.
///
/// Protocol:
///   Client sends:  4-byte BE length + UTF-8 Cypher string
///   Server replies: 4-byte BE length + UTF-8 result string
///
/// See `docs/architecture/DAEMON-COMMUNICATION.md` Section 8.

use std::collections::HashMap;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use prost::Message;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::auth::Authenticator;
use crate::events::{self, GraphEvent};
use crate::graph::GraphHandle;
use crate::identity::app_id_from_pid;
use crate::proto::Event;
use crate::quota::{QuotaConfig, RateLimiter};

/// Producer socket default (overridable via `LUNARIS_PRODUCER_SOCKET`).
const DEFAULT_PRODUCER_SOCKET: &str = "/run/lunaris/event-bus-producer.sock";

/// One `graph.rate_limited` event per app at most this often, so a
/// query flood does not hammer the Event Bus producer.
const EMIT_THROTTLE: Duration = Duration::from_secs(5);

/// Per-identity rate-limit state shared across all query connections,
/// so a caller's many connections share one token bucket (per
/// *identity*, not per connection).
struct RateState {
    limiter: RateLimiter,
    last_emit: HashMap<String, Instant>,
}

impl RateState {
    fn new() -> Self {
        Self {
            limiter: RateLimiter::new(QuotaConfig::lunaris_default()),
            last_emit: HashMap::new(),
        }
    }

    /// Whether a violation event should be emitted for `app_id` now
    /// (edge-throttled to one per [`EMIT_THROTTLE`]).
    fn should_emit(&mut self, app_id: &str) -> bool {
        let now = Instant::now();
        match self.last_emit.get(app_id) {
            Some(&t) if now.duration_since(t) < EMIT_THROTTLE => false,
            _ => {
                self.last_emit.insert(app_id.to_string(), now);
                true
            }
        }
    }
}

/// Fire-and-forget emitter for `graph.rate_limited` Event Bus events.
struct RateLimitEmitter {
    socket_path: PathBuf,
}

impl RateLimitEmitter {
    fn new() -> Self {
        let path = std::env::var("LUNARIS_PRODUCER_SOCKET")
            .unwrap_or_else(|_| DEFAULT_PRODUCER_SOCKET.to_string());
        Self {
            socket_path: PathBuf::from(path),
        }
    }

    /// Emit `graph.rate_limited`; the payload is the offending app_id.
    /// Consumed by the Anomaly Detector (foundation §8.4). Best-effort.
    fn emit(&self, app_id: &str) {
        let event = Event {
            id: uuid::Uuid::now_v7().to_string(),
            r#type: "graph.rate_limited".to_string(),
            timestamp: chrono::Utc::now().timestamp_micros(),
            source: "knowledge".to_string(),
            pid: std::process::id(),
            // The Event Bus rejects an empty session_id; a daemon has
            // no user session, so a stable daemon identifier is used.
            session_id: "knowledge-daemon".to_string(),
            payload: app_id.as_bytes().to_vec(),
            uid: unsafe { libc::getuid() },
            project_id: String::new(),
        };
        let encoded = event.encode_to_vec();
        let len = (encoded.len() as u32).to_be_bytes();
        if let Ok(mut stream) = std::os::unix::net::UnixStream::connect(&self.socket_path) {
            use std::io::Write;
            let _ = stream
                .write_all(&len)
                .and_then(|_| stream.write_all(&encoded))
                .and_then(|_| stream.flush());
        }
    }
}

/// `SO_PEERCRED` → `(pid, uid)` of the peer that opened `fd`.
fn so_peercred(fd: std::os::unix::io::RawFd) -> std::io::Result<(i32, u32)> {
    let mut cred = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut len = std::mem::size_of::<libc::ucred>() as libc::socklen_t;
    // SAFETY: cred + len are valid for the call; fd is a live socket.
    let r = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            &mut cred as *mut _ as *mut libc::c_void,
            &mut len,
        )
    };
    if r != 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok((cred.pid, cred.uid))
}

/// Sleep a small random jitter (0–10 ms) before a reply so two
/// *equivalent* queries are not timing-distinguishable (foundation
/// §8.4: "a small random noise value added"). Uses `getrandom`
/// (already a dependency).
///
/// Scope: this is additive noise, the mechanism the foundation
/// describes. It makes equivalent queries indistinguishable, but it
/// does not pad cost-dependent runtime to a fixed floor, so the cost
/// *difference* between two structurally-different queries can still
/// be recovered by an attacker who samples within the rate limit.
/// Bucketed/fixed response deadlines per query class are the stronger
/// follow-up; additive noise is the §8.4 baseline.
async fn timing_noise() {
    let mut b = [0u8; 1];
    let jitter = if getrandom::getrandom(&mut b).is_ok() {
        (b[0] % 11) as u64
    } else {
        5
    };
    tokio::time::sleep(Duration::from_millis(jitter)).await;
}

/// Start the Graph Daemon listener and event subscriber.
///
/// Spawns two concurrent tasks:
/// 1. Socket listener for client queries.
/// 2. Event Bus subscriber for permission/schema change events.
pub async fn listen(socket_path: &str, graph: GraphHandle) -> Result<()> {
    let auth = Arc::new(Mutex::new(Authenticator::new()));
    info!("graph daemon: HMAC key generated");

    // Per-identity rate limiting + the violation emitter are shared
    // across all query connections.
    let rate = Arc::new(Mutex::new(RateState::new()));
    let emitter = Arc::new(RateLimitEmitter::new());

    tokio::try_join!(
        listen_queries(socket_path, graph, auth.clone(), rate, emitter),
        listen_events(auth),
    )?;

    Ok(())
}

/// Accept and handle client connections.
async fn listen_queries(
    socket_path: &str,
    graph: GraphHandle,
    auth: Arc<Mutex<Authenticator>>,
    rate: Arc<Mutex<RateState>>,
    emitter: Arc<RateLimitEmitter>,
) -> Result<()> {
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }
    if let Some(parent) = Path::new(socket_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    info!(socket = socket_path, "graph daemon listening");

    // SAFETY: getuid() has no preconditions and cannot fail.
    let our_uid = unsafe { libc::getuid() };

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let graph = graph.clone();
                let auth = auth.clone();
                let rate = rate.clone();
                let emitter = emitter.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_client(stream, graph, auth, rate, emitter, our_uid).await
                    {
                        error!("graph daemon client error: {e}");
                    }
                });
            }
            Err(e) => error!("graph daemon accept error: {e}"),
        }
    }
}

/// Subscribe to Event Bus and process permission/schema events.
async fn listen_events(auth: Arc<Mutex<Authenticator>>) -> Result<()> {
    let uid = unsafe { libc::getuid() };
    let consumer_id = format!("graph-daemon-{uid}");

    // Event Bus connection is optional -- daemon works without it.
    let mut stream = match events::connect(&consumer_id, uid).await {
        Ok(s) => {
            info!("graph daemon: connected to event bus");
            s
        }
        Err(e) => {
            warn!("graph daemon: event bus not available ({e}), running without live updates");
            // Block forever so try_join doesn't exit.
            std::future::pending::<()>().await;
            return Ok(());
        }
    };

    loop {
        match events::recv_event(&mut stream).await {
            Some(event) => {
                handle_graph_event(&auth, event).await;
            }
            None => {
                warn!("graph daemon: event bus disconnected, attempting reconnect");
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                match events::connect(&consumer_id, uid).await {
                    Ok(s) => {
                        stream = s;
                        info!("graph daemon: reconnected to event bus");
                    }
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }
}

/// Process a graph-relevant event.
async fn handle_graph_event(auth: &Arc<Mutex<Authenticator>>, event: GraphEvent) {
    match event {
        GraphEvent::PermissionChanged { app_id } => {
            info!("permission changed for {app_id}, invalidating token");
            auth.lock().await.invalidate(&app_id);
        }
        GraphEvent::AiLevelChanged => {
            info!("AI level changed, invalidating ai-daemon token");
            auth.lock().await.invalidate("ai-daemon");
        }
        GraphEvent::SchemaRegistered { app_id } => {
            info!("schema registered: {app_id}");
            // Schema loading comes in Phase 3.3.
        }
        GraphEvent::SchemaRemoved { app_id } => {
            info!("schema removed: {app_id}");
        }
    }
}

/// Handle a single client connection.
///
/// Phase 3.2 adds token awareness, but for backward compatibility the
/// daemon still accepts raw Cypher queries. Full token enforcement
/// (token on every request) is deferred to when the Request/Response
/// protobuf protocol replaces the current plaintext protocol.
async fn handle_client(
    mut stream: UnixStream,
    graph: GraphHandle,
    _auth: Arc<Mutex<Authenticator>>,
    rate: Arc<Mutex<RateState>>,
    emitter: Arc<RateLimitEmitter>,
    our_uid: u32,
) -> Result<()> {
    // Resolve the peer identity once at connection for per-identity
    // rate limiting (foundation §8.4). The socket is per-user, so a
    // cross-uid peer is rejected; an unresolvable binary is treated as
    // the strictest tier via the `unknown` sentinel (ThirdParty).
    //
    // Known limitation (same-uid, F3): the tier is derived from the
    // resolved `app_id`, and `lunaris-permissions` maps a user-
    // installed `~/.local/share/lunaris/apps/{id}/` binary to `{id}`.
    // So a same-uid attacker could squat a reserved id (`system` →
    // unlimited, `ai-daemon` → FirstParty) to escape ThirdParty. This
    // does not regress vs. pre-S15 (which rate-limited no one) and is
    // the same gap as the audit daemon's ingest admission; the global
    // fix is the installd inode-keyed identity registry
    // (`docs/architecture/identity-spoof-mitigation.md`). A
    // provenance check (privileged tiers only from canonical /usr
    // paths) is the interim hardening when that lands.
    let app_id = match so_peercred(stream.as_raw_fd()) {
        Ok((pid, uid)) => {
            if uid != our_uid {
                warn!(peer_uid = uid, "graph daemon: rejecting cross-uid client");
                return Ok(());
            }
            if pid > 0 {
                app_id_from_pid(pid as u32).unwrap_or_else(|_| "unknown".to_string())
            } else {
                "unknown".to_string()
            }
        }
        Err(e) => {
            warn!("graph daemon: SO_PEERCRED failed ({e}); treating peer as untrusted");
            "unknown".to_string()
        }
    };
    debug!(app_id = %app_id, "new graph daemon client");

    loop {
        // Read query length.
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                debug!("graph daemon client disconnected");
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }

        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > 64 * 1024 {
            warn!(len, "invalid query length");
            return Ok(());
        }

        // Read query string. A leading 0x01 byte selects the structured
        // (typed JSON RowSet) response mode; without it the request is a
        // legacy raw-Cypher text query, so existing clients are unaffected.
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        let typed_rows = buf.first() == Some(&0x01);
        let cypher_bytes = if typed_rows { &buf[1..] } else { &buf[..] };
        let cypher = String::from_utf8(cypher_bytes.to_vec())?;

        debug!(cypher = %cypher, typed_rows, "received query");

        // Per-identity rate limit, before any work.
        let violation = {
            let mut rs = rate.lock().await;
            match rs.limiter.check_query(&app_id) {
                Ok(()) => None,
                Err(e) => Some((e.to_string(), rs.should_emit(&app_id))),
            }
        };

        // Failure responses are the plaintext `ERROR: ...` form in both
        // modes; a typed client detects the `ERROR:` prefix before parsing
        // JSON (the SDK does). Wrapping every typed failure in a structured
        // JSON envelope (stable code + message) so a typed client can tell
        // RateLimited from QueryTimeout is a follow-up; today the sole typed
        // consumer fails closed on any error, so the category is not needed.
        let (response, emit_violation) = if let Some((reason, emit)) = violation {
            warn!(app_id = %app_id, "graph query rate limit exceeded");
            (format!("ERROR: RateLimited: {reason}"), emit)
        } else if is_write_query(&cypher) {
            // Reject write queries (Phase 1A constraint, relaxed in 3.4).
            (
                "ERROR: write queries are not permitted via the query interface"
                    .to_string(),
                false,
            )
        } else {
            // Bounded *client wait*: the connection returns QueryTimeout
            // after 500 ms (foundation §8.4) so the caller is never
            // stuck. Because the graph enqueue now yields under
            // backpressure rather than blocking a worker (see graph.rs),
            // this deadline also covers queue admission. NB it unblocks
            // the caller; it does not abort the graph worker's in-flight
            // query, which runs to completion — a true execution deadline
            // needs an interruptible graph API and is a follow-up.
            let r = if typed_rows {
                // The Ladybug thread serialises the rows to JSON, so this
                // deadline bounds the query AND its serialisation together
                // (the text branch below serialises on that thread too).
                match tokio::time::timeout(
                    Duration::from_millis(500),
                    graph.query_rows_json(cypher),
                )
                .await
                {
                    Ok(Ok(json)) => json,
                    Ok(Err(e)) => format!("ERROR: {e}"),
                    Err(_elapsed) => "ERROR: QueryTimeout".to_string(),
                }
            } else {
                match tokio::time::timeout(Duration::from_millis(500), graph.query(cypher)).await {
                    Ok(Ok(result)) => result,
                    Ok(Err(e)) => format!("ERROR: {e}"),
                    Err(_elapsed) => "ERROR: QueryTimeout".to_string(),
                }
            };
            (r, false)
        };

        // Best-effort violation telemetry for the Anomaly Detector,
        // scheduled BEFORE the reply. `spawn_blocking` returns
        // immediately (the blocking Event Bus socket I/O runs on the
        // blocking pool, so the reply is never delayed), and scheduling
        // it here means a client that disconnects mid-write cannot
        // suppress the signal whose throttle slot it already consumed.
        if emit_violation {
            let emitter = emitter.clone();
            let app_id = app_id.clone();
            tokio::task::spawn_blocking(move || emitter.emit(&app_id));
        }

        // Small random delay before replying, so two equivalent
        // queries are not timing-distinguishable.
        timing_noise().await;

        // Write response.
        let response_bytes = response.as_bytes();
        let response_len = u32::try_from(response_bytes.len())
            .expect("response too large")
            .to_be_bytes();

        stream.write_all(&response_len).await?;
        stream.write_all(response_bytes).await?;
    }
}

/// Check if a Cypher query contains write operations.
///
/// A write clause can appear anywhere, not only at the start (`MATCH (n)
/// DELETE n`, `MATCH (a) MERGE (b)`), so this scans whole-word tokens rather
/// than the leading keyword, skipping single-quoted string literals so a
/// value that merely contains a keyword (e.g. a path with `DELETE` in it) is
/// not mistaken for a write. It over-rejects rather than under-rejects (a
/// read whose identifier collides with a keyword is refused, never a write
/// let through), which is the safe direction for a read-only socket.
///
/// This is a lexical guard, not a parser; the robust form is read-only
/// enforcement in the graph engine (a read-only connection/transaction), a
/// follow-up that would protect both this and the text query path at the
/// execution layer rather than by inspecting the query text.
fn is_write_query(cypher: &str) -> bool {
    const WRITE_KEYWORDS: [&str; 7] = [
        "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP", "DETACH",
    ];
    let mut in_string = false;
    let mut escaped = false;
    let mut token = String::new();
    for ch in cypher.chars() {
        if in_string {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '\'' {
                in_string = false;
            }
            continue;
        }
        match ch {
            '\'' => in_string = true,
            c if c.is_ascii_alphanumeric() || c == '_' => token.push(c.to_ascii_uppercase()),
            _ => {
                if WRITE_KEYWORDS.contains(&token.as_str()) {
                    return true;
                }
                token.clear();
            }
        }
    }
    WRITE_KEYWORDS.contains(&token.as_str())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_write_queries() {
        assert!(is_write_query("CREATE (n:File)"));
        assert!(is_write_query("MERGE (n:App)"));
        assert!(is_write_query("DELETE n"));
        assert!(is_write_query("SET n.name = 'x'"));
        assert!(is_write_query("  create (n)"));
    }

    #[test]
    fn detects_writes_that_do_not_start_with_the_keyword() {
        // The leading-token check missed these; the token scan catches them.
        assert!(is_write_query("MATCH (n:File) DELETE n"));
        assert!(is_write_query("MATCH (a:App) MERGE (b:Session)"));
        assert!(is_write_query("MATCH (n) SET n.name = 'x' RETURN n"));
        assert!(is_write_query("MATCH (n) DETACH DELETE n"));
    }

    #[test]
    fn allows_read_queries() {
        assert!(!is_write_query("MATCH (n:File) RETURN n"));
        assert!(!is_write_query("MATCH (a:App) WHERE a.id = 'x' RETURN a.name"));
        // A write keyword inside a string literal is a value, not a clause.
        assert!(!is_write_query(
            "MATCH (f:File) WHERE f.path = '/home/tim/DELETE/x' RETURN f.id"
        ));
    }

    #[tokio::test]
    async fn test_handle_graph_event_permission_changed() {
        let auth = Arc::new(Mutex::new(Authenticator::new()));
        handle_graph_event(
            &auth,
            GraphEvent::PermissionChanged {
                app_id: "com.test".into(),
            },
        )
        .await;
        // Should not panic; cache invalidation is internal.
    }

    #[tokio::test]
    async fn test_handle_graph_event_ai_level() {
        let auth = Arc::new(Mutex::new(Authenticator::new()));
        handle_graph_event(&auth, GraphEvent::AiLevelChanged).await;
    }

    #[test]
    fn rate_state_emit_is_throttled() {
        let mut rs = RateState::new();
        assert!(rs.should_emit("com.test"), "first violation emits");
        assert!(!rs.should_emit("com.test"), "a repeat within the window is throttled");
        // A different identity emits independently.
        assert!(rs.should_emit("com.other"));
    }

    #[test]
    fn lunaris_default_throttles_apps_but_not_the_ai_daemon() {
        // A normal (ThirdParty) caller bursting past its 200-token
        // burst is rate-limited; the AI daemon's higher FirstParty
        // limit (2000 burst) is not tripped by the same burst.
        let mut rs = RateState::new();

        let mut app_limited = false;
        for _ in 0..201 {
            if rs.limiter.check_query("com.test").is_err() {
                app_limited = true;
                break;
            }
        }
        assert!(app_limited, "a normal app bursting past 200 must be RateLimited");

        let mut ai_limited = false;
        for _ in 0..201 {
            if rs.limiter.check_query("ai-daemon").is_err() {
                ai_limited = true;
                break;
            }
        }
        assert!(!ai_limited, "the AI daemon's higher tier must not be tripped at 201");
    }
}
