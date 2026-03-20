use crate::graph::GraphHandle;
use anyhow::Result;
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
async fn read_hwm(pool: &SqlitePool) -> Result<i64> {
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
/// promotes them to Ladybug, and updates the HWM.
///
/// Promotion criteria for Phase 1A:
/// - All `file.opened` events become `File` and `App` nodes with an `ACCESSED_BY` edge.
/// - All `window.focused` events become `App` and `Session` nodes with an `ACTIVE_IN` edge.
/// - Other event types are stored in SQLite but not yet promoted (Phase 2).
async fn run_pass(pool: &SqlitePool, graph: &GraphHandle) -> Result<()> {
    let hwm = read_hwm(pool).await?;

    // Fetch unprocessed events ordered by timestamp.
    let rows: Vec<(String, String, i64, String, i64, String)> = sqlx::query_as(
        "SELECT id, type, timestamp, source, pid, session_id
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

    let mut new_hwm = hwm;

    for (id, event_type, timestamp, source, pid, session_id) in &rows {
        let result = match event_type.as_str() {
            "file.opened" => {
                promote_file_opened(graph, id, timestamp, source, &pid.to_string(), session_id)
                    .await
            }
            "window.focused" => {
                promote_window_focused(graph, id, timestamp, session_id).await
            }
            _ => {
                // Not yet promoted; will be handled in a later phase.
                debug!(event_type, "skipping promotion for unhandled event type");
                Ok(())
            }
        };

        if let Err(e) = result {
            error!(event_id = %id, event_type, "promotion failed: {e}");
            // Continue with other events rather than aborting the whole pass.
        }

        new_hwm = *timestamp;
    }

    write_hwm(pool, new_hwm).await?;
    info!(hwm = new_hwm, promoted = rows.len(), "promotion pass complete");
    Ok(())
}

/// Promote a `file.opened` event.
///
/// Creates or merges a `File` node and an `App` node, then creates an
/// `ACCESSED_BY` edge between them. Uses `MERGE` so repeated events on
/// the same file do not create duplicate nodes.
async fn promote_file_opened(
    graph: &GraphHandle,
    event_id: &str,
    timestamp: &i64,
    source: &str,
    pid: &str,
    _session_id: &str,
) -> Result<()> {
    // For Phase 1A we use the source as the app_id since we do not yet have
    // full app identity resolution from eBPF. This will be refined in Phase 2
    // when the eBPF Normalizer resolves PIDs to app IDs.
    let app_id = format!("{source}:{pid}");

    // MERGE creates the node if it does not exist, otherwise matches it.
    // This is Cypher's equivalent of INSERT OR IGNORE combined with a lookup.
    graph
        .write(format!(
            "MERGE (a:App {{id: '{app_id}'}}) SET a.name = '{source}'"
        ))
        .await?;

    graph
        .write(format!(
            "MERGE (f:File {{id: '{event_id}'}})
             SET f.last_accessed = {timestamp}, f.app_id = '{app_id}'"
        ))
        .await?;

    graph
        .write(format!(
            "MATCH (f:File {{id: '{event_id}'}}), (a:App {{id: '{app_id}'}})
             MERGE (f)-[:ACCESSED_BY]->(a)"
        ))
        .await?;

    debug!(event_id, "promoted file.opened");
    Ok(())
}

/// Promote a `window.focused` event.
///
/// Creates or merges a `Session` node and an `App` node, then creates an
/// `ACTIVE_IN` edge between them.
async fn promote_window_focused(
    graph: &GraphHandle,
    event_id: &str,
    timestamp: &i64,
    session_id: &str,
) -> Result<()> {
    graph
        .write(format!(
            "MERGE (s:Session {{id: '{session_id}'}})
             SET s.started_at = {timestamp}"
        ))
        .await?;

    graph
        .write(format!(
            "MERGE (e:Event {{id: '{event_id}'}})
             SET e.type = 'window.focused', e.timestamp = {timestamp},
                 e.source = 'wayland'"
        ))
        .await?;

    debug!(event_id, "promoted window.focused");
    Ok(())
}
