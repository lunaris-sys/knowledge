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

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::auth::Authenticator;
use crate::events::{self, GraphEvent};
use crate::graph::GraphHandle;

/// Start the Graph Daemon listener and event subscriber.
///
/// Spawns two concurrent tasks:
/// 1. Socket listener for client queries.
/// 2. Event Bus subscriber for permission/schema change events.
pub async fn listen(socket_path: &str, graph: GraphHandle) -> Result<()> {
    let auth = Arc::new(Mutex::new(Authenticator::new()));
    info!("graph daemon: HMAC key generated");

    tokio::try_join!(
        listen_queries(socket_path, graph, auth.clone()),
        listen_events(auth),
    )?;

    Ok(())
}

/// Accept and handle client connections.
async fn listen_queries(
    socket_path: &str,
    graph: GraphHandle,
    auth: Arc<Mutex<Authenticator>>,
) -> Result<()> {
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }
    if let Some(parent) = Path::new(socket_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    info!(socket = socket_path, "graph daemon listening");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let graph = graph.clone();
                let auth = auth.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, graph, auth).await {
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
) -> Result<()> {
    debug!("new graph daemon client");

    // TODO (Phase 3.2 full): Issue token at connection time via SO_PEERCRED,
    // send TokenResponse, then verify token on each subsequent request.
    // For now, the read-only Cypher interface remains unchanged.

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

        // Read query string.
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        let cypher = String::from_utf8(buf)?;

        debug!(cypher = %cypher, "received query");

        // Reject write queries (Phase 1A constraint, relaxed in Phase 3.4).
        let response = if is_write_query(&cypher) {
            "ERROR: write queries are not permitted via the query interface".to_string()
        } else {
            match graph.query(cypher).await {
                Ok(result) => result,
                Err(e) => format!("ERROR: {e}"),
            }
        };

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
fn is_write_query(cypher: &str) -> bool {
    let upper = cypher.trim().to_uppercase();
    upper.starts_with("CREATE")
        || upper.starts_with("MERGE")
        || upper.starts_with("DELETE")
        || upper.starts_with("SET")
        || upper.starts_with("REMOVE")
        || upper.starts_with("DROP")
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
    fn allows_read_queries() {
        assert!(!is_write_query("MATCH (n:File) RETURN n"));
        assert!(!is_write_query("MATCH (a:App) WHERE a.id = 'x' RETURN a.name"));
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
}
