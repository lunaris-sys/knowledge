use anyhow::{anyhow, Result};
use lbug::{Connection, Database, SystemConfig, Value};
use std::sync::mpsc;
use std::thread;
use tracing::{debug, info};

/// A cell value extracted from a Ladybug QueryResult, safe to send
/// across threads.
#[derive(Debug, Clone)]
pub enum CellValue {
    Null,
    String(String),
    Int64(i64),
    Bool(bool),
    Float(f64),
}

impl CellValue {
    /// Extract a string reference, returning empty string for non-string values.
    pub fn as_str(&self) -> &str {
        match self {
            CellValue::String(s) => s,
            _ => "",
        }
    }

    /// Extract an i64, returning 0 for non-integer values.
    pub fn as_i64(&self) -> i64 {
        match self {
            CellValue::Int64(i) => *i,
            _ => 0,
        }
    }
}

/// Structured query result with column names and typed rows.
#[derive(Debug, Clone)]
pub struct RowSet {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<CellValue>>,
}

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
    /// Execute a Cypher query and return structured rows.
    QueryRows {
        cypher: String,
        reply: tokio::sync::oneshot::Sender<Result<RowSet>>,
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

    /// Execute a Cypher query and return structured rows (async).
    pub async fn query_rows(&self, cypher: String) -> Result<RowSet> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(GraphRequest::QueryRows {
                cypher,
                reply: reply_tx,
            })
            .map_err(|_| anyhow!("ladybug thread has stopped"))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("ladybug thread dropped reply sender"))?
    }

    /// Execute a Cypher query and return structured rows (blocking).
    ///
    /// Intended for use on non-tokio threads (e.g. the FUSE thread).
    /// Must NOT be called from within a tokio async context.
    pub fn query_rows_sync(&self, cypher: String) -> Result<RowSet> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(GraphRequest::QueryRows {
                cypher,
                reply: reply_tx,
            })
            .map_err(|_| anyhow!("ladybug thread has stopped"))?;
        reply_rx
            .blocking_recv()
            .map_err(|_| anyhow!("ladybug thread dropped reply sender"))?
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
            GraphRequest::QueryRows { cypher, reply } => {
                debug!(cypher = %cypher, "executing cypher (rows)");
                let result = conn.query(&cypher).map_err(|e| anyhow!("{e}"));
                let row_set = result.map(|mut qr| {
                    let columns = qr.get_column_names();
                    let rows = qr
                        .by_ref()
                        .map(|row| row.into_iter().map(value_to_cell).collect())
                        .collect();
                    RowSet { columns, rows }
                });
                reply.send(row_set).ok();
            }
            GraphRequest::Shutdown => {
                info!("ladybug thread shutting down");
                break;
            }
        }
    }

    Ok(())
}

/// Convert a Ladybug Value to a thread-safe CellValue.
fn value_to_cell(v: Value) -> CellValue {
    match v {
        Value::String(s) => CellValue::String(s),
        Value::Int64(i) => CellValue::Int64(i),
        Value::Int32(i) => CellValue::Int64(i64::from(i)),
        Value::Int16(i) => CellValue::Int64(i64::from(i)),
        Value::Int8(i) => CellValue::Int64(i64::from(i)),
        Value::UInt64(i) => CellValue::Int64(i as i64),
        Value::UInt32(i) => CellValue::Int64(i64::from(i)),
        Value::UInt16(i) => CellValue::Int64(i64::from(i)),
        Value::UInt8(i) => CellValue::Int64(i64::from(i)),
        Value::Bool(b) => CellValue::Bool(b),
        Value::Double(f) => CellValue::Float(f),
        Value::Float(f) => CellValue::Float(f64::from(f)),
        Value::Null(_) => CellValue::Null,
        other => CellValue::String(other.to_string()),
    }
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

    // Project system: project detection and file association.
    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS Project(
            id             STRING,
            name           STRING,
            description    STRING,
            root_path      STRING,
            accent_color   STRING,
            icon           STRING,
            status         STRING,
            created_at     INT64,
            last_accessed  INT64,
            inferred       BOOL,
            confidence     INT64,
            promoted       BOOL,
            archived_at    INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create Project table: {e}"))?;

    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS Directory(
            id         STRING,
            path       STRING,
            name       STRING,
            project_id STRING,
            created_at INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create Directory table: {e}"))?;

    conn.query(
        "CREATE REL TABLE IF NOT EXISTS FILE_PART_OF(FROM File TO Project)",
    )
    .map_err(|e| anyhow!("create FILE_PART_OF rel: {e}"))?;

    conn.query(
        "CREATE REL TABLE IF NOT EXISTS DIR_PART_OF(FROM Directory TO Project)",
    )
    .map_err(|e| anyhow!("create DIR_PART_OF rel: {e}"))?;

    // Annotation: structured per-app metadata attached to existing graph
    // nodes. Foundation §395. The composite identity is
    // (target_type, target_id, namespace) — a re-set on the same
    // triple replaces the previous data. We store target as
    // properties rather than edges so the schema stays flat across
    // target types (File, App, Project, Session, ...) and so the
    // common "fetch all annotations targeting X" query is a single
    // property scan.
    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS Annotation(
            id            STRING,
            namespace     STRING,
            target_type   STRING,
            target_id     STRING,
            data          STRING,
            created_at    INT64,
            last_modified INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create Annotation table: {e}"))?;

    // Retention policy: summary nodes for compacted old data.
    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS Summary(
            id                   STRING,
            type                 STRING,
            app_id               STRING,
            access_count         INT64,
            primary_application  STRING,
            active_period_start  INT64,
            active_period_end    INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create Summary table: {e}"))?;

    conn.query(
        "CREATE REL TABLE IF NOT EXISTS SUMMARIZES(FROM Summary TO App)",
    )
    .map_err(|e| anyhow!("create SUMMARIZES rel: {e}"))?;

    // Pin marker: separate node table to mark nodes as permanent.
    // Using a separate table avoids ALTER TABLE on existing node tables.
    conn.query(
        "CREATE NODE TABLE IF NOT EXISTS PinnedMarker(
            id         STRING,
            node_id    STRING,
            node_type  STRING,
            pinned_at  INT64,
            PRIMARY KEY(id)
        )",
    )
    .map_err(|e| anyhow!("create PinnedMarker table: {e}"))?;

    debug!("schema created");
    Ok(())
}
