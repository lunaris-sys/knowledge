use anyhow::{anyhow, Result};
use lbug::{Connection, Database, SystemConfig, Value};
use std::thread;
use tokio::sync::mpsc;
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

/// Convert a Ladybug [`Value`] directly to a JSON value for the typed query
/// response, faithfully or not at all. Returns `None` for anything JSON
/// cannot represent without loss — a non-finite float, or a complex/temporal/
/// binary value — so the query fails rather than silently shipping a wrapped
/// integer or a `Display` string the client would mistake for typed data.
/// (The KG's stored fields are string/int64/bool, so the fallible cases do
/// not arise for real node fields; they only guard exotic query expressions.)
fn value_to_json(v: Value) -> Option<serde_json::Value> {
    use serde_json::Value as J;
    Some(match v {
        Value::Null(_) => J::Null,
        Value::String(s) => J::String(s),
        Value::Bool(b) => J::Bool(b),
        Value::Int64(i) => J::Number(i.into()),
        Value::Int32(i) => J::Number(i64::from(i).into()),
        Value::Int16(i) => J::Number(i64::from(i).into()),
        Value::Int8(i) => J::Number(i64::from(i).into()),
        // Unsigned: preserved as-is (JSON numbers cover u64), never `as i64`.
        Value::UInt64(i) => J::Number(i.into()),
        Value::UInt32(i) => J::Number(u64::from(i).into()),
        Value::UInt16(i) => J::Number(u64::from(i).into()),
        Value::UInt8(i) => J::Number(u64::from(i).into()),
        Value::Double(f) => J::Number(serde_json::Number::from_f64(f)?),
        Value::Float(f) => J::Number(serde_json::Number::from_f64(f64::from(f))?),
        // Int128, temporal, blob, list, struct, etc.: not faithfully
        // representable here, so fail closed rather than stringify.
        _ => return None,
    })
}

/// A conservative upper bound on a cell's serialised cost, used to bound a
/// typed response *before* serialising it. Includes fixed structural overhead
/// (quotes, the separating comma, array brackets) so an empty or short cell is
/// never counted as free, and doubles string length to cover worst-case JSON
/// escaping (a control char becomes `\uXXXX`). Overcounting only makes the cap
/// fire sooner, which is the safe direction.
fn cell_cost(v: &serde_json::Value) -> usize {
    const OVERHEAD: usize = 8;
    OVERHEAD
        + match v {
            serde_json::Value::String(s) => s.len().saturating_mul(2),
            _ => 24,
        }
}

/// A message sent to the Ladybug thread.
/// Each variant carries a one-shot channel to send the result back.
///
/// The request channel is a bounded `tokio::sync::mpsc` (1024 slots) because
/// Ladybug's Connection is not Send and must stay on the dedicated thread.
/// Async callers `send().await` the request (which yields cooperatively under
/// backpressure instead of blocking a runtime worker, the difference that lets
/// the query daemon's per-request timeout actually bound the client wait), and
/// then await a tokio oneshot for the response. The Ladybug thread is a plain
/// OS thread with no runtime and drains the receiver via `blocking_recv`.
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
    /// Execute a Cypher query and return structured rows serialised to a JSON
    /// string. The serialisation happens on the Ladybug thread, inside the
    /// work the query daemon's deadline covers, so a large typed result is
    /// bounded by the same client-wait timeout as a text query rather than
    /// serialising unbounded after it.
    QueryRowsJson {
        cypher: String,
        reply: tokio::sync::oneshot::Sender<Result<String>>,
    },
    /// Shut down the Ladybug thread cleanly.
    Shutdown,
}

/// Maximum rows a typed (JSON) query result may carry. A query exceeding it
/// fails rather than serialising an unbounded response; the agent's targeted
/// slices are far below this.
const MAX_TYPED_ROWS: usize = 10_000;

/// Maximum approximate byte size of a typed result's cells, so a query with
/// few rows but huge string cells is also bounded (the row cap alone is not).
const MAX_TYPED_BYTES: usize = 4 * 1024 * 1024;

/// Maximum columns a typed result may have, so a very wide result cannot
/// multiply the cell count past what the row + byte caps assume. The agent's
/// targeted slices return a handful of columns.
const MAX_TYPED_COLUMNS: usize = 256;

/// Handle to the dedicated Ladybug thread.
/// Clone this to get additional senders to the same thread.
#[derive(Clone)]
pub struct GraphHandle {
    sender: mpsc::Sender<GraphRequest>,
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
            .await
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
            .await
            .map_err(|_| anyhow!("ladybug thread has stopped"))?;
        reply_rx
            .await
            .map_err(|_| anyhow!("ladybug thread dropped reply sender"))?
    }

    /// Execute a Cypher query and return structured rows serialised to a JSON
    /// string (async). Serialisation runs on the Ladybug thread, so the query
    /// daemon's per-request deadline bounds it together with the query.
    pub async fn query_rows_json(&self, cypher: String) -> Result<String> {
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(GraphRequest::QueryRowsJson {
                cypher,
                reply: reply_tx,
            })
            .await
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
            .blocking_send(GraphRequest::QueryRows {
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

    // Bounded channel with 1024 pending-request slots. If the Ladybug thread
    // falls behind, async senders await (yielding the worker) rather than
    // blocking it. 1024 is generous; normal load is much lower.
    let (tx, rx) = mpsc::channel::<GraphRequest>(1024);

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
fn ladybug_thread(path: &str, mut rx: mpsc::Receiver<GraphRequest>) -> Result<()> {
    let db = Database::new(path, SystemConfig::default())
        .map_err(|e| anyhow!("failed to open ladybug database: {e}"))?;
    let conn = Connection::new(&db)
        .map_err(|e| anyhow!("failed to create ladybug connection: {e}"))?;

    info!(path, "ladybug database opened");
    create_schema(&conn)?;
    info!("ladybug schema ready");

    while let Some(request) = rx.blocking_recv() {
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
            GraphRequest::QueryRowsJson { cypher, reply } => {
                debug!(cypher = %cypher, "executing cypher (rows -> json)");
                // Build and serialise on this thread, so the work is bounded
                // by the daemon's per-request deadline (the text path
                // serialises here too). Bound the result by row count AND
                // byte size, and stop early if the caller has already given
                // up (its reply channel closed), so a large result cannot
                // monopolise the thread or grow unbounded after a timeout.
                // (A single Kuzu query that itself runs past the deadline
                // still cannot be aborted mid-execution; that interruptible
                // graph API is the same documented follow-up the text path
                // notes.)
                // The lbug result iterator converts each cell with an internal
                // `try_into().unwrap()`, so an unrepresentable database value
                // (e.g. an out-of-range temporal) panics *during* iteration,
                // before `value_to_json` can reject it. Arbitrary client Cypher
                // reaches this path, so contain any such panic: it becomes an
                // error and the dedicated graph thread keeps serving instead of
                // unwinding and taking the whole graph service down.
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                    || -> Result<String> {
                        // Drop a request whose caller already gave up before we
                        // even execute it (it may have queued behind a slow one).
                        if reply.is_closed() {
                            return Err(anyhow!("client disconnected before execution"));
                        }
                        let mut qr = conn.query(&cypher).map_err(|e| anyhow!("{e}"))?;
                        let columns = qr.get_column_names();
                        if columns.len() > MAX_TYPED_COLUMNS {
                            return Err(anyhow!(
                                "typed result exceeds {MAX_TYPED_COLUMNS} columns"
                            ));
                        }
                        let mut rows: Vec<Vec<serde_json::Value>> = Vec::new();
                        let mut bytes = 0usize;
                        for row in qr.by_ref() {
                            if reply.is_closed() {
                                return Err(anyhow!(
                                    "client disconnected before the result was ready"
                                ));
                            }
                            if rows.len() >= MAX_TYPED_ROWS {
                                return Err(anyhow!("typed result exceeds {MAX_TYPED_ROWS} rows"));
                            }
                            let mut cells = Vec::with_capacity(row.len());
                            for v in row {
                                let cell = value_to_json(v).ok_or_else(|| {
                                    anyhow!(
                                        "query result has a value JSON cannot represent faithfully"
                                    )
                                })?;
                                // Bound per cell, not per row: a single wide
                                // row of large strings must trip the cap before
                                // it is fully materialised.
                                bytes = bytes.saturating_add(cell_cost(&cell));
                                if bytes > MAX_TYPED_BYTES {
                                    return Err(anyhow!(
                                        "typed result exceeds {MAX_TYPED_BYTES} bytes"
                                    ));
                                }
                                cells.push(cell);
                            }
                            rows.push(cells);
                        }
                        Ok(serde_json::json!({ "columns": columns, "rows": rows }).to_string())
                    },
                ))
                .unwrap_or_else(|_| {
                    Err(anyhow!("the graph engine could not convert a value in the result"))
                });
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_to_json_preserves_types() {
        assert_eq!(value_to_json(Value::Int64(3)), Some(serde_json::json!(3)));
        assert_eq!(value_to_json(Value::Bool(true)), Some(serde_json::json!(true)));
        assert_eq!(
            value_to_json(Value::String("p1".into())),
            Some(serde_json::json!("p1"))
        );
    }

    #[test]
    fn unsigned_64bit_is_preserved_not_wrapped() {
        // The old CellValue path did `UInt64 as i64`, wrapping a large value
        // to a negative; the direct converter keeps it faithful.
        let big = u64::MAX;
        assert_eq!(value_to_json(Value::UInt64(big)), Some(serde_json::json!(big)));
    }

    #[test]
    fn a_string_with_a_delimiter_or_newline_is_json_escaped() {
        // The reason for the typed path: JSON escaping makes a value with `|`
        // or a newline safe, where the pipe-delimited text would corrupt it.
        let v = value_to_json(Value::String("a|b\nc".into())).unwrap();
        assert_eq!(v, serde_json::json!("a|b\nc"));
        // Round-trips through serialisation intact.
        let json = serde_json::json!({ "rows": [[v]] }).to_string();
        let back: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(back["rows"][0][0], serde_json::json!("a|b\nc"));
    }

    #[test]
    fn a_non_finite_float_fails_rather_than_corrupting() {
        assert_eq!(value_to_json(Value::Double(f64::NAN)), None);
        assert_eq!(value_to_json(Value::Double(f64::INFINITY)), None);
    }

    #[test]
    fn cell_cost_charges_overhead_so_empty_cells_are_not_free() {
        // An empty/short cell must still cost something (structural overhead),
        // so a wide result of empty strings cannot slip under the byte cap.
        assert!(cell_cost(&serde_json::json!("")) >= 8);
        assert!(cell_cost(&serde_json::json!("ab")) > cell_cost(&serde_json::json!("")));
        // Strings are charged extra for worst-case escaping.
        let long = "x".repeat(100);
        assert!(cell_cost(&serde_json::json!(long)) >= 200);
        // A wide row of empty strings accrues real cost.
        let row_cost: usize = (0..256).map(|_| cell_cost(&serde_json::json!(""))).sum();
        assert!(row_cost >= 256 * 8);
    }
}
