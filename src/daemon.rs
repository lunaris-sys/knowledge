use crate::graph::GraphHandle;
use anyhow::Result;
use std::path::Path;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, warn};

/// Start the Graph Daemon: a Unix socket that accepts Cypher queries
/// and returns results as UTF-8 strings.
///
/// Protocol (read-only queries only for Phase 1A):
///   Client sends:  4-byte big-endian length + UTF-8 Cypher string
///   Server replies: 4-byte big-endian length + UTF-8 result string
///
/// Write queries are rejected with an error response.
pub async fn listen(socket_path: &str, graph: GraphHandle) -> Result<()> {
    if Path::new(socket_path).exists() {
        std::fs::remove_file(socket_path)?;
    }
    if let Some(parent) = Path::new(socket_path).parent() {
        std::fs::create_dir_all(parent)?;
    }

    let listener = UnixListener::bind(socket_path)?;
    tracing::info!(socket = socket_path, "graph daemon listening");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let graph = graph.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(stream, graph).await {
                        error!("graph daemon client error: {e}");
                    }
                });
            }
            Err(e) => error!("graph daemon accept error: {e}"),
        }
    }
}

/// Handle a single client connection.
async fn handle_client(mut stream: UnixStream, graph: GraphHandle) -> Result<()> {
    debug!("new graph daemon client");

    loop {
        // Read query length
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

        // Read query string
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;
        let cypher = String::from_utf8(buf)?;

        debug!(cypher = %cypher, "received query");

        // Reject write queries in Phase 1A.
        // Write access will be scoped via capability tokens in Phase 2.
        let response = if is_write_query(&cypher) {
            "ERROR: write queries are not permitted via the query interface".to_string()
        } else {
            match graph.query(cypher).await {
                Ok(result) => result,
                Err(e) => format!("ERROR: {e}"),
            }
        };

        // Write response
        let response_bytes = response.as_bytes();
        let response_len = u32::try_from(response_bytes.len())
            .expect("response too large")
            .to_be_bytes();

        stream.write_all(&response_len).await?;
        stream.write_all(response_bytes).await?;
    }
}

/// Check if a Cypher query contains write operations.
/// This is a conservative prefix check, not a full parser.
/// Any query that starts with a write keyword is rejected.
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
}
