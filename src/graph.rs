use anyhow::{anyhow, Result};
use lbug::{Connection, Database, SystemConfig};
use std::sync::mpsc;
use std::thread;
use tracing::{debug, info};

/// A message sent to the Ladybug thread.
/// Each variant carries a one-shot channel to send the result back.
///
/// We use `std::sync::mpsc` (not tokio) because Ladybug's Connection is not
/// Send and must stay on the same thread. The Ladybug thread owns a regular
/// std channel receiver; async callers send via the std sender and then
/// await a tokio oneshot for the response.
pub enum GraphRequest {
    /// Execute a Cypher query and return the raw result as a string.
    Query {
        cypher: String,
        reply: tokio::sync::oneshot::Sender<Result<String>>,
    },
    /// Shut down the Ladybug thread cleanly.
    Shutdown,
}

/// Handle to the dedicated Ladybug thread.
/// Clone this to get additional senders to the same thread.
#[derive(Clone)]
pub struct GraphHandle {
    sender: mpsc::SyncSender<GraphRequest>,
}

impl GraphHandle {
    /// Execute a read-only Cypher query and return the result as a string.
    ///
    /// This sends the query to the dedicated Ladybug thread and awaits
    /// the response on a tokio oneshot channel.
    pub async fn query(&self, cypher: String) -> Result<String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(GraphRequest::Query {
                cypher,
                reply: reply_tx,
            })
            .map_err(|_| anyhow!("ladybug thread has stopped"))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("ladybug thread dropped reply sender"))?
    }

    /// Write a node or relationship to Ladybug.
    /// Internally this is just a query that happens to be a write.
    pub async fn write(&self, cypher: String) -> Result<String> {
        self.query(cypher).await
    }
}

/// Spawn the dedicated Ladybug thread and return a handle to it.
///
/// The thread opens the database at `path`, creates the schema if needed,
/// and then loops waiting for `GraphRequest` messages. It runs until it
/// receives `GraphRequest::Shutdown` or the channel is closed.
///
/// # Why a dedicated thread?
/// `lbug::Connection` is not `Send`. It cannot be moved between threads or
/// shared across async tasks. Keeping it on one dedicated thread and
/// communicating via channels is the standard pattern for non-Send resources
/// in async Rust. It is similar to how you would marshal calls to a COM object
/// on a single-threaded apartment in Windows.
pub fn spawn(path: &str) -> Result<GraphHandle> {
    let path = path.to_string();

    // SyncSender with a bounded buffer of 1024 pending requests.
    // If the Ladybug thread falls behind, senders will block.
    // 1024 is generous; normal load is much lower.
    let (tx, rx) = mpsc::sync_channel::<GraphRequest>(1024);

    thread::Builder::new()
        .name("ladybug".to_string())
        .spawn(move || {
            if let Err(e) = ladybug_thread(&path, rx) {
                tracing::error!("ladybug thread exited with error: {e}");
            }
        })?;

    Ok(GraphHandle { sender: tx })
}

/// The body of the dedicated Ladybug thread.
fn ladybug_thread(path: &str, rx: mpsc::Receiver<GraphRequest>) -> Result<()> {
    let db = Database::new(path, SystemConfig::default())
        .map_err(|e| anyhow!("failed to open ladybug database: {e}"))?;
    let conn = Connection::new(&db)
        .map_err(|e| anyhow!("failed to create ladybug connection: {e}"))?;

    info!(path, "ladybug database opened");
    create_schema(&conn)?;
    info!("ladybug schema ready");

    for request in rx {
        match request {
            GraphRequest::Query { cypher, reply } => {
                debug!(cypher = %cypher, "executing cypher");
                let result = conn
                    .query(&cypher)
                    .map(|r| r.to_string())
                    .map_err(|e| anyhow!("{e}"));
                // If the caller dropped the oneshot receiver we just ignore the error.
                reply.send(result).ok();
            }
            GraphRequest::Shutdown => {
                info!("ladybug thread shutting down");
                break;
            }
        }
    }

    Ok(())
}

/// Create the Knowledge Graph node and relationship tables.
///
/// Uses `CREATE ... IF NOT EXISTS` so this is safe to call on every startup.
/// Schema changes require a migration strategy; for Phase 1A we keep the
/// schema minimal and stable.
fn create_schema(conn: &Connection) -> Result<()> {
    // Node tables
    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS File(
            id          STRING,
            path        STRING,
            app_id      STRING,
            last_accessed INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create File table: {e}"))?;

    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS App(
            id      STRING,
            name    STRING,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create App table: {e}"))?;

    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS Session(
            id         STRING,
            started_at INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create Session table: {e}"))?;

    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS Event(
            id         STRING,
            type       STRING,
            timestamp  INT64,
            source     STRING,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create Event table: {e}"))?;

    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS UserAction(
            id        STRING,
            category  STRING,
            action    STRING,
            subject   STRING,
            timestamp INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create UserAction table: {e}"))?;

    // Relationship tables
    conn.query(
        "CREATE REL TABLE IF NOT EXISTS ACCESSED_BY(FROM File TO App)",
    )
    .map_err(|e| anyhow!("create ACCESSED_BY rel: {e}"))?;

    conn.query(
        "CREATE REL TABLE IF NOT EXISTS ACTIVE_IN(FROM App TO Session)",
    )
    .map_err(|e| anyhow!("create ACTIVE_IN rel: {e}"))?;

    conn.query(
        "CREATE REL TABLE IF NOT EXISTS EMITTED_BY(FROM Event TO App)",
    )
    .map_err(|e| anyhow!("create EMITTED_BY rel: {e}"))?;

    conn.query(
        "CREATE REL TABLE IF NOT EXISTS DERIVED_FROM(FROM UserAction TO Event)",
    )
    .map_err(|e| anyhow!("create DERIVED_FROM rel: {e}"))?;

    debug!("schema created");
    Ok(())
}
