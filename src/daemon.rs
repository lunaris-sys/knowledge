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
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::auth::Authenticator;
use crate::events::{self, GraphEvent};
use crate::graph::GraphHandle;
use crate::identity::{app_id_from_pid, pid_start_time};
use crate::proto::Event;
use crate::quota::{AppTier, QuotaConfig, RateLimiter};
use crate::schema::SchemaRegistry;
use crate::utils::escape_cypher;
use crate::write::{create_relation, retract_relation, RelationResult};

/// Producer socket default (overridable via `LUNARIS_PRODUCER_SOCKET`).
const DEFAULT_PRODUCER_SOCKET: &str = "/run/lunaris/event-bus-producer.sock";

/// One `graph.rate_limited` event per app at most this often, so a
/// query flood does not hammer the Event Bus producer.
const EMIT_THROTTLE: Duration = Duration::from_secs(5);

/// Upper bound on a write request's `op_id` (the agent's operation digest is a
/// fixed-length hex string; this bounds an abusive caller's literal).
const MAX_OP_ID_LEN: usize = 128;

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

    // Schema registry for write-mode relation validation. Built with the
    // compiled-in system entity types only; that is sufficient for the agent's
    // built-in system relations (the only write op today), since
    // `create_relation` refuses anything outside the built-in allowlist anyway.
    // Loading app-defined schemas for app-relation writes is a follow-up.
    let registry = Arc::new(SchemaRegistry::new(vec![]));

    tokio::try_join!(
        listen_queries(socket_path, graph, auth.clone(), rate, emitter, registry),
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
    registry: Arc<SchemaRegistry>,
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
                let registry = registry.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_client(stream, graph, auth, rate, emitter, registry, our_uid).await
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

/// The kernel-attested write peer: the SO_PEERCRED pid plus that pid's start
/// time captured at connection. The start time is the PID-reuse guard (E7): a
/// write re-reads it and refuses if it changed, so a recycled pid (the original
/// peer exited and the number was reused by another process) cannot borrow the
/// connection's authority. `start_time` is `None` only if it could not be read
/// at connection, in which case a write fails closed (reuse is unguardable).
#[derive(Clone, Copy)]
struct WritePeer {
    pid: u32,
    start_time: Option<u64>,
}

/// A structured graph write request, sent with a leading `0x02` byte (the
/// write-mode prefix, beside the legacy raw-Cypher text query and the `0x01`
/// typed-rows query). The body is JSON, tagged by `op`.
///
/// The boundary is deliberately narrow: the only ops are creating a built-in
/// graph relation between two existing nodes, and retracting (compensating) a
/// relation the caller previously created, keyed by its operation id. There is
/// no free-form Cypher write path. The agent's executor is the only intended
/// caller, and the authorisation primitives (`create_relation` /
/// `retract_relation`) refuse anything outside the declared relation allowlist
/// regardless of what is sent.
#[derive(Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum WriteRequest {
    /// Create `from -[relation_type]-> to` between two existing nodes,
    /// identified by their namespaced entity type and concrete id.
    CreateRelation {
        from_type: String,
        from_id: String,
        to_type: String,
        to_id: String,
        relation_type: String,
        /// Durable operation identity: the caller's stable id for this logical
        /// write, persisted on the edge so a lost-response retry can reconcile
        /// by asking whether *this* operation's edge exists. Optional; an empty
        /// id is not persisted (the edge's `op_id` stays NULL, as for the
        /// promotion pipeline). Only `FILE_PART_OF` carries the column today.
        #[serde(default)]
        op_id: String,
    },
    /// Retract (compensate) a relation the caller previously created, deleting
    /// only the edge that carries this exact `op_id`. The `op_id` is mandatory
    /// and non-empty: a retract is always a precise undo of the caller's own
    /// write, never a bare-edge delete. Idempotent (a no-match is success).
    RetractRelation {
        from_type: String,
        from_id: String,
        to_type: String,
        to_id: String,
        relation_type: String,
        op_id: String,
    },
}

/// Authorise and persist a structured write request, returning the plaintext
/// response (`OK` / `ERROR: ...`).
///
/// Fail-closed at every layer: the request must come from a live, non-recycled
/// peer process (PID-reuse guard) whose permission profile grants the relation
/// (token issuance), the relation must pass `create_relation` (scope +
/// anchor/privilege + declared + known types), and persistence is a *checked*
/// MATCH/MERGE that reports not-found when an endpoint instance is absent rather
/// than a silent no-op success.
///
/// SECURITY BOUNDARY (same-uid): the peer's identity comes from
/// `app_id_from_pid`, and both it and the permission profile under
/// `~/.config/permissions/` are user-writable. A same-uid process can therefore
/// squat a privileged app id and grant itself the relation scope, so this write
/// mode does not defend against a same-uid attacker. That is the documented F3
/// gap shared across the daemon (the read rate-limiter, the audit daemon's
/// ingest admission), closed only by the installd inode-keyed identity registry
/// and root-owned profiles (`docs/architecture/identity-spoof-mitigation.md`).
/// It is *sharper* here than on the read path because this authorises a graph
/// mutation, so enabling this socket for real first-party-only use is gated on
/// that hardening (canonical-executable provenance as the interim step); a hard
/// path gate is not applied now because the agent runs from a dev tree during
/// development. Cross-uid peers are already rejected at connection.
async fn handle_write_request(
    body: &[u8],
    peer: Option<WritePeer>,
    registry: &SchemaRegistry,
    graph: &GraphHandle,
    auth: &Arc<Mutex<Authenticator>>,
) -> String {
    let req: WriteRequest = match serde_json::from_slice(body) {
        Ok(r) => r,
        Err(e) => return format!("ERROR: malformed write request: {e}"),
    };

    // A write must be attributable to a live peer process.
    let Some(peer) = peer else {
        return "ERROR: write requires a resolvable peer process".to_string();
    };

    // PID-reuse guard (E7): the pid's start time must be readable now and match
    // the value captured at connection. If it changed, the original peer exited
    // and another process inherited the pid number, so it must not borrow this
    // connection's write authority. Re-checked immediately before token
    // issuance, which itself re-resolves the app_id from the same pid.
    let Some(captured_start) = peer.start_time else {
        return "ERROR: write requires a verifiable peer process".to_string();
    };
    match pid_start_time(peer.pid) {
        Ok(now) if now == captured_start => {}
        _ => return "ERROR: peer process changed since connection".to_string(),
    }

    // The token is issued from the pid's permission profile and fails closed if
    // it has no graph access or no matching relation scope.
    let token = match auth.lock().await.issue_token_for_pid(peer.pid) {
        Ok(t) => t,
        Err(e) => return format!("ERROR: {e}"),
    };

    match req {
        WriteRequest::CreateRelation {
            from_type,
            from_id,
            to_type,
            to_id,
            relation_type,
            op_id,
        } => {
            let rel = match create_relation(
                registry,
                &from_type,
                &from_id,
                &to_type,
                &to_id,
                &relation_type,
                &token,
            ) {
                Ok(r) => r,
                Err(e) => return format!("ERROR: {e}"),
            };
            persist_relation(graph, &rel, &op_id).await
        }
        WriteRequest::RetractRelation {
            from_type,
            from_id,
            to_type,
            to_id,
            relation_type,
            op_id,
        } => {
            let rel = match retract_relation(
                registry,
                &from_type,
                &from_id,
                &to_type,
                &to_id,
                &relation_type,
                &op_id,
                &token,
            ) {
                Ok(r) => r,
                Err(e) => return format!("ERROR: {e}"),
            };
            persist_retract(graph, &rel, &op_id).await
        }
    }
}

/// Persist an authorised relation with an **atomic conditional create** that
/// reports whether it actually created the edge.
///
/// The endpoint types were validated as built-in system types, so their graph
/// table name is the type minus the `system.` prefix and the relation label is
/// a known identifier from the allowlist; none of those are attacker-controlled.
/// Only the endpoint ids are caller-supplied, so they are escaped into the
/// Cypher string literals.
///
/// The create is a single statement (`OPTIONAL MATCH ... WHERE r IS NULL CREATE
/// ... RETURN count`) on the dedicated, serial graph thread, so create-only-
/// if-absent cannot race a concurrent create: a second creator's statement runs
/// after the first and sees the edge, so it creates nothing. `created` is 1 iff
/// THIS statement created the edge, so a single attempt can distinguish a create
/// from a no-op and never double-creates. Three outcomes: `OK: created`,
/// `OK: exists` (idempotent no-op), or `ERROR: relation endpoints not found`.
///
/// The signal is per-*statement*, not per logical operation: if a create commits
/// but its response is lost and the call is retried, the retry sees the edge and
/// reports `exists`. Which logical *operation* created the edge is recorded
/// separately by the `op_id` set on the edge (see below): a caller that loses
/// the response reconciles by reading whether *its* `op_id` edge exists, a
/// causally-tied verdict the bare `created`/`exists` flag cannot give.
///
/// Row-level ownership/visibility on the matched endpoints is intentionally not
/// enforced here. The authorisation gate in `create_relation` already requires a
/// privileged `InstanceScope::All` token for a relation between nodes the caller
/// does not own, and the only write caller today is the agent, a first-party
/// component curating the global graph. Enforcing per-row `_owner`/`_deleted`
/// filters becomes load-bearing when an unprivileged app links its own
/// (anchored) nodes; that is the documented follow-up alongside app-relation
/// support.
async fn persist_relation(graph: &GraphHandle, rel: &RelationResult, op_id: &str) -> String {
    let from_label = rel
        .from_type
        .strip_prefix("system.")
        .unwrap_or(&rel.from_type);
    let to_label = rel.to_type.strip_prefix("system.").unwrap_or(&rel.to_type);
    let rel_type = &rel.relation_type;
    let from_id = escape_cypher(&rel.from_id);
    let to_id = escape_cypher(&rel.to_id);

    // Durable operation identity (idempotency key): persisted on the edge so a
    // lost-response retry can reconcile by reading whether *this* op's edge
    // exists. Only `FILE_PART_OF` carries the `op_id` column today, so it is set
    // only there; a missing/empty id leaves it NULL (as the promotion pipeline's
    // own creates do). The caller-supplied id is bounded and escaped into the
    // literal (it is untrusted; the agent derives a fixed-length digest).
    if op_id.len() > MAX_OP_ID_LEN {
        return "ERROR: op_id too long".to_string();
    }
    let op_clause = if rel_type == "FILE_PART_OF" && !op_id.is_empty() {
        format!(" {{op_id: '{}'}}", escape_cypher(op_id))
    } else {
        String::new()
    };

    // Atomic conditional create. `created` = 1 only if this statement created
    // the edge; 0 if the edge already existed (the `WHERE r IS NULL` filters its
    // row out) OR an endpoint is missing (the MATCH binds nothing). The write
    // awaits its definitive result with no client-side timeout: the graph worker
    // is not cancellable, so a timeout would unblock the caller while the queued
    // CREATE could still commit, mis-reporting a committed write.
    let create_cypher = format!(
        "MATCH (a:{from_label} {{id: '{from_id}'}}), (b:{to_label} {{id: '{to_id}'}}) \
         OPTIONAL MATCH (a)-[r:{rel_type}]->(b) WITH a, b, r WHERE r IS NULL \
         CREATE (a)-[:{rel_type}{op_clause}]->(b) RETURN count(*) AS created"
    );
    let created = match graph.query_rows(create_cypher).await {
        Ok(rs) => row_count(&rs),
        Err(e) => return format!("ERROR: {e}"),
    };
    if created > 0 {
        return "OK: created".to_string();
    }

    // created == 0 means either the edge already existed or an endpoint was
    // missing at create time. Disambiguate by checking the EDGE itself, never
    // merely the endpoints: an endpoint that a concurrent writer adds *after*
    // the create matched nothing must not be mistaken for a successful link.
    // `OK: exists` is therefore reported only when the edge genuinely exists
    // now (benign idempotent no-op); otherwise the link was not made and a
    // retryable not-found is returned. A concurrent writer that created the
    // edge meanwhile makes `exists` honest (the link is present, just not by
    // this call), so compensation still correctly treats it as not-created.
    let edge_cypher = format!(
        "MATCH (a:{from_label} {{id: '{from_id}'}})-[r:{rel_type}]->(b:{to_label} {{id: '{to_id}'}}) \
         RETURN count(*) AS edge"
    );
    match graph.query_rows(edge_cypher).await {
        Ok(rs) if row_count(&rs) > 0 => "OK: exists".to_string(),
        Ok(_) => "ERROR: relation endpoints not found".to_string(),
        Err(e) => format!("ERROR: {e}"),
    }
}

/// Persist an authorised relation *retract* (compensation): delete the single
/// edge that carries this `op_id`, and only that edge.
///
/// The match is keyed by the `op_id` property, so this deletes exactly the edge
/// the caller's own create stamped, never a bare edge it did not write. Only
/// `FILE_PART_OF` carries the `op_id` column, so a retract of any other relation
/// is refused fail-closed (there is no precise key, and we never delete an
/// op-id-less edge here). The `op_id` non-emptiness was already enforced by
/// `retract_relation`; it is re-checked and length-bounded here before it goes
/// into the literal, mirroring `persist_relation`.
///
/// Deletion is **idempotent**: a match-nothing run (the edge was already gone,
/// or never existed) is `OK: absent`, a successful no-op. A run that deleted the
/// edge is `OK: retracted`. The two differ only in their label; both guarantee
/// the same post-state (no edge with this op_id), which is what compensation
/// needs. The single statement runs on the serial graph thread, so a concurrent
/// retract of the same edge cannot double-delete: the second sees nothing and
/// reports `absent`.
async fn persist_retract(graph: &GraphHandle, rel: &RelationResult, op_id: &str) -> String {
    if op_id.is_empty() {
        return "ERROR: retract requires an op_id".to_string();
    }
    if op_id.len() > MAX_OP_ID_LEN {
        return "ERROR: op_id too long".to_string();
    }
    let rel_type = &rel.relation_type;
    // Only relations that carry the `op_id` column can be precisely retracted.
    // Anything else has no per-operation key, so deleting it would be a
    // bare-edge delete; refuse it rather than risk removing an untracked edge.
    if rel_type != "FILE_PART_OF" {
        return "ERROR: relation does not support op-id retract".to_string();
    }
    let from_label = rel
        .from_type
        .strip_prefix("system.")
        .unwrap_or(&rel.from_type);
    let to_label = rel.to_type.strip_prefix("system.").unwrap_or(&rel.to_type);
    let from_id = escape_cypher(&rel.from_id);
    let to_id = escape_cypher(&rel.to_id);
    let op = escape_cypher(op_id);

    // Endpoint labels and the relation type are validated identifiers (not
    // attacker-controlled); only the ids and op_id are caller-supplied and are
    // escaped into the literals. `deleted` counts the edges this statement
    // matched and removed (RETURN runs over the same bound rows as DELETE).
    let cypher = format!(
        "MATCH (a:{from_label} {{id: '{from_id}'}})-[r:{rel_type} {{op_id: '{op}'}}]->(b:{to_label} {{id: '{to_id}'}}) \
         DELETE r RETURN count(*) AS deleted"
    );
    match graph.query_rows(cypher).await {
        Ok(rs) if row_count(&rs) > 0 => "OK: retracted".to_string(),
        Ok(_) => "OK: absent".to_string(),
        Err(e) => format!("ERROR: {e}"),
    }
}

/// Extract the first cell of the first row as an i64 (a `count(*)` result),
/// defaulting to 0 for an empty result.
fn row_count(rs: &crate::graph::RowSet) -> i64 {
    rs.rows
        .first()
        .and_then(|r| r.first())
        .map(|c| c.as_i64())
        .unwrap_or(0)
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
    auth: Arc<Mutex<Authenticator>>,
    rate: Arc<Mutex<RateState>>,
    emitter: Arc<RateLimitEmitter>,
    registry: Arc<SchemaRegistry>,
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
    // `peer` is the kernel-attested write peer (pid + start time), captured
    // once for write-mode token issuance and the PID-reuse guard; `None` when
    // it cannot be trusted, so a write fails closed.
    let (app_id, peer) = match so_peercred(stream.as_raw_fd()) {
        Ok((pid, uid)) => {
            if uid != our_uid {
                warn!(peer_uid = uid, "graph daemon: rejecting cross-uid client");
                return Ok(());
            }
            if pid > 0 {
                let pid = pid as u32;
                let id = app_id_from_pid(pid).unwrap_or_else(|_| "unknown".to_string());
                let start_time = pid_start_time(pid).ok();
                (id, Some(WritePeer { pid, start_time }))
            } else {
                ("unknown".to_string(), None)
            }
        }
        Err(e) => {
            warn!("graph daemon: SO_PEERCRED failed ({e}); treating peer as untrusted");
            ("unknown".to_string(), None)
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

        // Read the request body; its leading byte selects the mode.
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        // Write mode: a leading 0x02 byte selects a structured graph write
        // request (JSON body) instead of a Cypher read query. It is rate-
        // limited on the per-identity *write* bucket and authorised + persisted
        // by `handle_write_request`; the read path below is untouched.
        if buf.first() == Some(&0x02) {
            let body = &buf[1..];

            // Least-privilege dispatch gate: only first-party / system callers
            // may mutate the graph; a ThirdParty (or unresolved `unknown`) peer
            // is refused before any token work. This does not by itself defeat
            // same-uid app_id spoofing (see `handle_write_request`), but it
            // keeps the system-relation write path off-limits to ordinary apps.
            let tier = QuotaConfig::lunaris_default().tier_for_app(&app_id);
            if tier == AppTier::ThirdParty {
                warn!(app_id = %app_id, "graph write refused for non-first-party caller");
                let response = "ERROR: write mode not permitted for this caller";
                let response_len = (response.len() as u32).to_be_bytes();
                stream.write_all(&response_len).await?;
                stream.write_all(response.as_bytes()).await?;
                continue;
            }

            let violation = {
                let mut rs = rate.lock().await;
                match rs.limiter.check_write(&app_id) {
                    Ok(()) => None,
                    Err(e) => Some((e.to_string(), rs.should_emit(&app_id))),
                }
            };

            let (response, emit_violation) = if let Some((reason, emit)) = violation {
                warn!(app_id = %app_id, "graph write rate limit exceeded");
                (format!("ERROR: RateLimited: {reason}"), emit)
            } else {
                (
                    handle_write_request(body, peer, &registry, &graph, &auth).await,
                    false,
                )
            };

            if emit_violation {
                let emitter = emitter.clone();
                let app_id = app_id.clone();
                tokio::task::spawn_blocking(move || emitter.emit(&app_id));
            }

            timing_noise().await;

            let response_bytes = response.as_bytes();
            let response_len = u32::try_from(response_bytes.len())
                .expect("response too large")
                .to_be_bytes();
            stream.write_all(&response_len).await?;
            stream.write_all(response_bytes).await?;
            continue;
        }

        // Read mode. A leading 0x01 byte selects the structured (typed JSON
        // RowSet) response; without it the request is a legacy raw-Cypher text
        // query, so existing clients are unaffected.
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
/// The blocklist covers not only the graph-mutation clauses but the
/// data-definition, attach, copy, and extension verbs, because on this socket
/// those are the dangerous ones: `LOAD EXTENSION`/`INSTALL` run native code,
/// `COPY` does filesystem I/O, `ATTACH`/`USE` reach another database, and
/// `ALTER`/`EXPORT`/`IMPORT` change or dump the schema/data. None appear in the
/// agent's read queries (`MATCH`/`WHERE`/`WITH`/`RETURN`/`ORDER`/`LIMIT`).
///
/// This is a lexical guard, not a parser. Engine-level read-only enforcement
/// (a read-only `lbug` connection for the read path) was investigated and is
/// NOT viable with the current engine: a read-only `Database` handle is a
/// snapshot at open time and does not observe writes committed through the
/// read-write handle, so the agent would read stale data (its own writes
/// invisible), and opening a fresh handle per query is far too costly. Until
/// the engine exposes a per-statement / per-transaction read-only flag, this
/// expanded blocklist is the ceiling; it closes the known privilege-escalation
/// verbs while keeping the over-reject-not-under-reject safety direction.
fn is_write_query(cypher: &str) -> bool {
    const WRITE_KEYWORDS: [&str; 15] = [
        "CREATE", "MERGE", "DELETE", "SET", "REMOVE", "DROP", "DETACH", "ALTER",
        "ATTACH", "USE", "COPY", "LOAD", "INSTALL", "EXPORT", "IMPORT",
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
    fn rejects_dangerous_non_mutation_verbs() {
        // Code execution, file I/O, cross-database, and schema/dump verbs are
        // refused on the read socket even though they are not graph mutations.
        assert!(is_write_query("LOAD EXTENSION 'evil.so'"));
        assert!(is_write_query("INSTALL httpfs"));
        assert!(is_write_query("COPY File TO '/tmp/exfil.csv'"));
        assert!(is_write_query("ATTACH '/other/db' AS x (dbtype kuzu)"));
        assert!(is_write_query("ALTER TABLE File ADD col STRING"));
        assert!(is_write_query("USE other_db"));
        assert!(is_write_query("EXPORT DATABASE '/tmp/dump'"));
        assert!(is_write_query("IMPORT DATABASE '/tmp/dump'"));
    }

    #[test]
    fn allows_read_queries() {
        assert!(!is_write_query("MATCH (n:File) RETURN n"));
        assert!(!is_write_query("MATCH (a:App) WHERE a.id = 'x' RETURN a.name"));
        // A write/admin keyword inside a string literal is a value, not a clause.
        assert!(!is_write_query(
            "MATCH (f:File) WHERE f.path = '/home/tim/DELETE/x' RETURN f.id"
        ));
        assert!(!is_write_query(
            "MATCH (f:File) WHERE f.path = '/var/COPY/load' RETURN f.id"
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

    /// Spawn a fresh graph in a temp dir and wait for schema init. These tests
    /// touch a real Ladybug instance, so they flake under a parallel `cargo
    /// test` (multi-instance); run the suite with `--test-threads=1`.
    async fn spawn_test_graph() -> (GraphHandle, tempfile::TempDir) {
        let tmp = tempfile::TempDir::new().unwrap();
        let graph = crate::graph::spawn(tmp.path().join("graph").to_str().unwrap()).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        (graph, tmp)
    }

    fn file_part_of(from_id: &str, to_id: &str) -> RelationResult {
        RelationResult {
            from_type: "system.File".into(),
            from_id: from_id.into(),
            to_type: "system.Project".into(),
            to_id: to_id.into(),
            relation_type: "FILE_PART_OF".into(),
        }
    }

    #[tokio::test]
    async fn persist_relation_links_existing_nodes() {
        let (graph, _tmp) = spawn_test_graph().await;
        graph
            .write("CREATE (f:File {id: 'f1', path: '/x', app_id: 'test', last_accessed: 0})".into())
            .await
            .unwrap();
        graph
            .write("CREATE (p:Project {id: 'p1'})".into())
            .await
            .unwrap();

        let resp = persist_relation(&graph, &file_part_of("f1", "p1"), "op-1").await;
        assert_eq!(resp, "OK: created", "the first link creates the edge");

        // The edge is actually present, exactly once.
        let rows = graph
            .query_rows(
                "MATCH (:File {id: 'f1'})-[:FILE_PART_OF]->(:Project {id: 'p1'}) RETURN count(*) AS n"
                    .into(),
            )
            .await
            .unwrap();
        assert_eq!(rows.rows[0][0].as_i64(), 1, "the FILE_PART_OF edge exists");

        // The op_id is persisted, so a reconciliation by op_id finds THIS
        // operation's edge but not a different operation's.
        let mine = graph
            .query_rows(
                "MATCH (:File {id: 'f1'})-[:FILE_PART_OF {op_id: 'op-1'}]->(:Project {id: 'p1'}) RETURN count(*) AS n".into(),
            )
            .await
            .unwrap();
        assert_eq!(mine.rows[0][0].as_i64(), 1, "op-1 reconciles to its own edge");
        let other = graph
            .query_rows(
                "MATCH (:File {id: 'f1'})-[:FILE_PART_OF {op_id: 'op-other'}]->(:Project {id: 'p1'}) RETURN count(*) AS n".into(),
            )
            .await
            .unwrap();
        assert_eq!(other.rows[0][0].as_i64(), 0, "a different op does not match");

        // A second create is an idempotent no-op reported as `exists`, and does
        // not duplicate the edge (the conditional create is strict).
        let again = persist_relation(&graph, &file_part_of("f1", "p1"), "op-2").await;
        assert_eq!(again, "OK: exists", "a repeat link reports exists, not created");
        let rows = graph
            .query_rows(
                "MATCH (:File {id: 'f1'})-[:FILE_PART_OF]->(:Project {id: 'p1'}) RETURN count(*) AS n"
                    .into(),
            )
            .await
            .unwrap();
        assert_eq!(rows.rows[0][0].as_i64(), 1, "no duplicate edge after a repeat");
    }

    #[tokio::test]
    async fn persist_retract_deletes_only_the_op_id_edge() {
        let (graph, _tmp) = spawn_test_graph().await;
        graph
            .write("CREATE (f:File {id: 'f1', path: '/x', app_id: 'test', last_accessed: 0})".into())
            .await
            .unwrap();
        graph
            .write("CREATE (p:Project {id: 'p1'})".into())
            .await
            .unwrap();

        // Create the edge under op-1.
        assert_eq!(persist_relation(&graph, &file_part_of("f1", "p1"), "op-1").await, "OK: created");

        // A retract under a *different* op_id matches nothing: the edge is the
        // caller's own only when the op_id matches, so this is an idempotent
        // no-op and the edge survives.
        let miss = persist_retract(&graph, &file_part_of("f1", "p1"), "op-other").await;
        assert_eq!(miss, "OK: absent", "a non-matching op_id retracts nothing");
        let rows = graph
            .query_rows(
                "MATCH (:File {id: 'f1'})-[:FILE_PART_OF]->(:Project {id: 'p1'}) RETURN count(*) AS n".into(),
            )
            .await
            .unwrap();
        assert_eq!(rows.rows[0][0].as_i64(), 1, "the edge survives a wrong-op retract");

        // The matching op_id deletes exactly that edge.
        let hit = persist_retract(&graph, &file_part_of("f1", "p1"), "op-1").await;
        assert_eq!(hit, "OK: retracted", "the owning op_id retracts its edge");
        let rows = graph
            .query_rows(
                "MATCH (:File {id: 'f1'})-[:FILE_PART_OF]->(:Project {id: 'p1'}) RETURN count(*) AS n".into(),
            )
            .await
            .unwrap();
        assert_eq!(rows.rows[0][0].as_i64(), 0, "the edge is gone after its retract");

        // Retracting again is an idempotent success (the edge is already gone).
        let again = persist_retract(&graph, &file_part_of("f1", "p1"), "op-1").await;
        assert_eq!(again, "OK: absent", "a repeat retract is an idempotent no-op");
    }

    #[tokio::test]
    async fn persist_retract_refuses_a_relation_without_op_id_column() {
        let (graph, _tmp) = spawn_test_graph().await;
        // ACCESSED_BY carries no op_id column, so it has no precise per-operation
        // key; a retract of it must be refused rather than risk a bare delete.
        let rel = RelationResult {
            from_type: "system.File".into(),
            from_id: "f1".into(),
            to_type: "system.App".into(),
            to_id: "a1".into(),
            relation_type: "ACCESSED_BY".into(),
        };
        let resp = persist_retract(&graph, &rel, "op-1").await;
        assert_eq!(resp, "ERROR: relation does not support op-id retract");
    }

    const VALID_REL_BODY: &str = r#"{"op":"create_relation","from_type":"system.File","from_id":"f1","to_type":"system.Project","to_id":"p1","relation_type":"FILE_PART_OF"}"#;

    #[tokio::test]
    async fn write_rejects_recycled_pid() {
        let (graph, _tmp) = spawn_test_graph().await;
        let auth = Arc::new(Mutex::new(Authenticator::new()));
        let registry = SchemaRegistry::new(vec![]);
        // A start time that cannot match the live process: the reuse guard must
        // fire before any token issuance or graph write.
        let peer = WritePeer {
            pid: std::process::id(),
            start_time: Some(0),
        };
        let resp =
            handle_write_request(VALID_REL_BODY.as_bytes(), Some(peer), &registry, &graph, &auth)
                .await;
        assert_eq!(resp, "ERROR: peer process changed since connection");
    }

    #[tokio::test]
    async fn write_rejects_unverifiable_peer() {
        let (graph, _tmp) = spawn_test_graph().await;
        let auth = Arc::new(Mutex::new(Authenticator::new()));
        let registry = SchemaRegistry::new(vec![]);
        // No captured start time: reuse cannot be guarded, so fail closed.
        let peer = WritePeer {
            pid: std::process::id(),
            start_time: None,
        };
        let resp =
            handle_write_request(VALID_REL_BODY.as_bytes(), Some(peer), &registry, &graph, &auth)
                .await;
        assert_eq!(resp, "ERROR: write requires a verifiable peer process");
    }

    #[tokio::test]
    async fn write_rejects_absent_peer_and_malformed_body() {
        let (graph, _tmp) = spawn_test_graph().await;
        let auth = Arc::new(Mutex::new(Authenticator::new()));
        let registry = SchemaRegistry::new(vec![]);

        let no_peer =
            handle_write_request(VALID_REL_BODY.as_bytes(), None, &registry, &graph, &auth).await;
        assert_eq!(no_peer, "ERROR: write requires a resolvable peer process");

        // A malformed body is rejected before the peer is even consulted.
        let bad = handle_write_request(b"not json", None, &registry, &graph, &auth).await;
        assert!(bad.starts_with("ERROR: malformed write request"), "got: {bad}");
    }

    #[tokio::test]
    async fn persist_relation_reports_absent_endpoint() {
        let (graph, _tmp) = spawn_test_graph().await;
        graph
            .write("CREATE (f:File {id: 'f1', path: '/x', app_id: 'test', last_accessed: 0})".into())
            .await
            .unwrap();
        // No Project node exists, so the MATCH binds nothing and the checked
        // persistence must report not-found rather than a silent success.
        let resp = persist_relation(&graph, &file_part_of("f1", "missing"), "").await;
        assert_eq!(resp, "ERROR: relation endpoints not found");
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
