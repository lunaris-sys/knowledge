use crate::graph::GraphHandle;
use crate::proto::{FileOpenedPayload, WindowFocusedPayload};
use crate::utils::escape_cypher;
use anyhow::Result;
use prost::Message;
use sqlx::SqlitePool;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

/// How often the promotion pass runs.
const PROMOTION_INTERVAL: Duration = Duration::from_secs(30);

/// High-water mark key in a metadata table we use to track progress.
/// The promotion pass only processes events newer than the last run.
const HWM_KEY: &str = "promotion_hwm";

/// Run the promotion pass forever, waking every `PROMOTION_INTERVAL`.
///
/// The promotion pass reads events from SQLite that have not yet been
/// promoted to Ladybug and creates the corresponding graph nodes.
/// It tracks progress via a high-water mark (the timestamp of the last
/// promoted event) so each run only processes new events.
pub async fn run(pool: SqlitePool, graph: GraphHandle) -> Result<()> {
    // Ensure the metadata table exists for high-water mark tracking.
    ensure_metadata_table(&pool).await?;

    let mut interval = time::interval(PROMOTION_INTERVAL);
    // Skip the first immediate tick so we don't run before the write store
    // has had a chance to accumulate events.
    interval.tick().await;

    loop {
        interval.tick().await;
        if let Err(e) = run_pass(&pool, &graph).await {
            error!("promotion pass failed: {e}");
        }
    }
}

/// Create the metadata table if it does not exist.
async fn ensure_metadata_table(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS metadata (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    Ok(())
}

/// Read the current high-water mark timestamp from the metadata table.
/// Returns 0 if no HWM has been recorded yet (first run).
pub(crate) async fn read_hwm(pool: &SqlitePool) -> Result<i64> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT value FROM metadata WHERE key = ?")
            .bind(HWM_KEY)
            .fetch_optional(pool)
            .await?;
    Ok(row
        .and_then(|(v,)| v.parse().ok())
        .unwrap_or(0))
}

/// Write a new high-water mark to the metadata table.
async fn write_hwm(pool: &SqlitePool, hwm: i64) -> Result<()> {
    sqlx::query(
        "INSERT INTO metadata (key, value) VALUES (?, ?)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
    )
    .bind(HWM_KEY)
    .bind(hwm.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

/// Run a single promotion pass.
///
/// Reads all events from SQLite with timestamp > high-water mark,
/// promotes them to Ladybug, and updates the HWM only if every event
/// in the batch was promoted successfully.
///
/// Promotion criteria for Phase 1A:
/// - All `file.opened` events become `File` and `App` nodes with an `ACCESSED_BY` edge.
/// - All `window.focused` events become `App`, `Session`, and `Event` nodes with `ACTIVE_IN` edge.
/// - Other event types are stored in SQLite but not yet promoted (Phase 2).
async fn run_pass(pool: &SqlitePool, graph: &GraphHandle) -> Result<()> {
    let hwm = read_hwm(pool).await?;

    // Fetch unprocessed events ordered by timestamp, including the payload.
    let rows: Vec<(String, String, i64, String, i64, String, Vec<u8>)> = sqlx::query_as(
        "SELECT id, type, timestamp, source, pid, session_id, payload
         FROM events
         WHERE timestamp > ?
         ORDER BY timestamp ASC
         LIMIT 1000",
    )
    .bind(hwm)
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        debug!("no new events to promote");
        return Ok(());
    }

    info!(count = rows.len(), "promoting events to ladybug");

    let mut all_ok = true;
    let mut last_timestamp = hwm;

    for (id, event_type, timestamp, source, pid, session_id, payload) in &rows {
        let result = match event_type.as_str() {
            "file.opened" => {
                promote_file_opened(graph, id, timestamp, source, pid, session_id, payload)
                    .await
            }
            "window.focused" => {
                promote_window_focused(graph, id, timestamp, session_id, payload).await
            }
            _ => {
                // Not yet promoted; will be handled in a later phase.
                debug!(event_type, "skipping promotion for unhandled event type");
                Ok(())
            }
        };

        if let Err(e) = result {
            error!(event_id = %id, event_type, "promotion failed: {e}");
            all_ok = false;
            // Stop advancing HWM: we do not skip failed events.
            break;
        }

        last_timestamp = *timestamp;
    }

    // Only advance the HWM if every event in the batch succeeded.
    // On failure, the next pass will retry from the same position.
    if all_ok && last_timestamp > hwm {
        write_hwm(pool, last_timestamp).await?;
        info!(hwm = last_timestamp, promoted = rows.len(), "promotion pass complete");
    } else if !all_ok {
        // Advance to the last successfully promoted event so we do not
        // re-process events that already succeeded.
        if last_timestamp > hwm {
            write_hwm(pool, last_timestamp).await?;
        }
        info!(hwm = last_timestamp, "promotion pass incomplete, will retry failed events");
    }

    Ok(())
}

/// Promote a `file.opened` event.
///
/// Deserializes the payload to obtain the file path and app ID, then
/// creates or merges a `File` node (keyed by path) and an `App` node,
/// with an `ACCESSED_BY` edge between them.
async fn promote_file_opened(
    graph: &GraphHandle,
    event_id: &str,
    timestamp: &i64,
    source: &str,
    pid: &i64,
    _session_id: &str,
    payload: &[u8],
) -> Result<()> {
    let file_payload = FileOpenedPayload::decode(payload)?;

    // Use the file path as the node ID so repeated opens of the same file
    // merge into a single node rather than creating duplicates.
    let path = if file_payload.path.is_empty() {
        // Fallback: eBPF events from Phase 1A may not have a resolved path yet.
        format!("unknown:{event_id}")
    } else {
        file_payload.path.clone()
    };

    // Prefer the app_id from the payload; fall back to source:pid.
    let app_id = if file_payload.app_id.is_empty() {
        format!("{source}:{pid}")
    } else {
        file_payload.app_id.clone()
    };

    let path_esc = escape_cypher(&path);
    let app_id_esc = escape_cypher(&app_id);
    let source_esc = escape_cypher(source);

    graph
        .write(format!(
            "MERGE (a:App {{id: '{app_id_esc}'}}) SET a.name = '{source_esc}'"
        ))
        .await?;

    graph
        .write(format!(
            "MERGE (f:File {{id: '{path_esc}'}})
             SET f.path = '{path_esc}', f.last_accessed = {timestamp}, f.app_id = '{app_id_esc}'"
        ))
        .await?;

    graph
        .write(format!(
            "MATCH (f:File {{id: '{path_esc}'}}), (a:App {{id: '{app_id_esc}'}})
             MERGE (f)-[:ACCESSED_BY]->(a)"
        ))
        .await?;

    debug!(event_id, path = %file_payload.path, "promoted file.opened");
    Ok(())
}

/// Promote a `window.focused` event.
///
/// Deserializes the payload to obtain the app ID and window title, then
/// creates or merges `App`, `Session`, and `Event` nodes with an
/// `ACTIVE_IN` edge from App to Session.
async fn promote_window_focused(
    graph: &GraphHandle,
    event_id: &str,
    timestamp: &i64,
    session_id: &str,
    payload: &[u8],
) -> Result<()> {
    let win_payload = WindowFocusedPayload::decode(payload)?;

    let app_id = if win_payload.app_id.is_empty() {
        "unknown".to_string()
    } else {
        win_payload.app_id.clone()
    };

    let app_id_esc = escape_cypher(&app_id);
    let session_id_esc = escape_cypher(session_id);
    let event_id_esc = escape_cypher(event_id);
    let title_esc = escape_cypher(&win_payload.window_title);

    graph
        .write(format!(
            "MERGE (a:App {{id: '{app_id_esc}'}}) SET a.name = '{app_id_esc}'"
        ))
        .await?;

    graph
        .write(format!(
            "MERGE (s:Session {{id: '{session_id_esc}'}})
             SET s.started_at = {timestamp}"
        ))
        .await?;

    graph
        .write(format!(
            "MERGE (e:Event {{id: '{event_id_esc}'}})
             SET e.type = 'window.focused', e.timestamp = {timestamp},
                 e.source = 'wayland', e.title = '{title_esc}'"
        ))
        .await?;

    // Create the ACTIVE_IN edge: the focused app is active in this session.
    graph
        .write(format!(
            "MATCH (a:App {{id: '{app_id_esc}'}}), (s:Session {{id: '{session_id_esc}'}})
             MERGE (a)-[:ACTIVE_IN]->(s)"
        ))
        .await?;

    debug!(event_id, app_id = %app_id, "promoted window.focused");
    Ok(())
}
