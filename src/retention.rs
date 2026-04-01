use crate::graph::GraphHandle;
use crate::promotion;
use crate::utils::escape_cypher;
use anyhow::Result;
use sqlx::SqlitePool;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time;
use tracing::{debug, error, info, warn};

/// Raw SQLite events older than this are deleted (if already promoted).
const RAW_EVENT_TTL: Duration = Duration::from_secs(30 * 24 * 3600);

/// Semantic Ladybug nodes older than this are compacted into summaries.
const SEMANTIC_NODE_TTL: Duration = Duration::from_secs(365 * 24 * 3600);

/// Return the current time as microseconds since Unix epoch.
fn now_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Run the retention daemon. Wakes once per day at approximately 03:00
/// and performs cleanup of old data across both stores.
pub async fn run(pool: SqlitePool, graph: GraphHandle) -> Result<()> {
    // Wait until the first 03:00 boundary, then run every 24 hours.
    // For simplicity we use a fixed 24h interval starting after a short
    // initial delay. A precise wall-clock scheduler is Phase 4 work.
    let initial_delay = Duration::from_secs(60);
    time::sleep(initial_delay).await;

    let mut interval = time::interval(Duration::from_secs(24 * 3600));
    loop {
        interval.tick().await;
        info!("retention pass starting");

        if let Err(e) = purge_raw_events(&pool).await {
            error!("retention: purge_raw_events failed: {e}");
        }

        if let Err(e) = compact_semantic_nodes(&graph).await {
            error!("retention: compact_semantic_nodes failed: {e}");
        }

        info!("retention pass complete");
    }
}

/// Tier 1: Delete raw SQLite events older than 30 days that have already
/// been promoted (timestamp < HWM).
async fn purge_raw_events(pool: &SqlitePool) -> Result<()> {
    let hwm = promotion::read_hwm(pool).await?;
    let age_cutoff = now_micros() - RAW_EVENT_TTL.as_micros() as i64;
    // Only delete events that are both old enough AND already promoted.
    let safe_cutoff = age_cutoff.min(hwm);

    if safe_cutoff <= 0 {
        debug!("retention: no raw events eligible for purge");
        return Ok(());
    }

    let result = sqlx::query("DELETE FROM events WHERE timestamp < ?")
        .bind(safe_cutoff)
        .execute(pool)
        .await?;

    let deleted = result.rows_affected();
    if deleted > 0 {
        info!(deleted, cutoff = safe_cutoff, "retention: purged raw events");
    } else {
        debug!("retention: no raw events to purge");
    }

    Ok(())
}

/// Tier 2: Compact Ladybug File nodes older than 12 months into Summary
/// nodes grouped by app. Pinned nodes are skipped.
async fn compact_semantic_nodes(graph: &GraphHandle) -> Result<()> {
    let cutoff = now_micros() - SEMANTIC_NODE_TTL.as_micros() as i64;

    // Find distinct apps that have old, non-pinned File nodes.
    let apps_result = graph
        .query(format!(
            "MATCH (f:File)-[:ACCESSED_BY]->(a:App)
             WHERE f.last_accessed < {cutoff}
             AND NOT EXISTS {{
                 MATCH (p:PinnedMarker) WHERE p.node_id = f.id AND p.node_type = 'File'
             }}
             RETURN DISTINCT a.id"
        ))
        .await?;

    let app_ids = parse_string_rows(&apps_result);
    if app_ids.is_empty() {
        debug!("retention: no apps with old nodes to compact");
        return Ok(());
    }

    for app_id in &app_ids {
        if let Err(e) = compact_app_files(graph, app_id, cutoff).await {
            warn!(app_id, "retention: compaction failed for app: {e}");
            // Continue with other apps.
        }
    }

    Ok(())
}

/// Compact all old File nodes for a single app into a Summary node.
///
/// Safety: the Summary node is created before deleting the originals.
/// If deletion fails, the next pass will find the Summary already exists
/// and skip re-creation; the originals will be retried.
async fn compact_app_files(graph: &GraphHandle, app_id: &str, cutoff: i64) -> Result<()> {
    let app_esc = escape_cypher(app_id);

    // Aggregate old non-pinned File nodes for this app.
    let agg_result = graph
        .query(format!(
            "MATCH (f:File)-[:ACCESSED_BY]->(a:App {{id: '{app_esc}'}})
             WHERE f.last_accessed < {cutoff}
             AND NOT EXISTS {{
                 MATCH (p:PinnedMarker) WHERE p.node_id = f.id AND p.node_type = 'File'
             }}
             RETURN count(f), min(f.last_accessed), max(f.last_accessed)"
        ))
        .await?;

    let (count, period_start, period_end) = parse_aggregation(&agg_result);
    if count == 0 {
        return Ok(());
    }

    let summary_id = format!("summary:{app_id}:{cutoff}");
    let summary_id_esc = escape_cypher(&summary_id);

    // Step 1: Create the Summary node (idempotent via MERGE).
    graph
        .write(format!(
            "MERGE (s:Summary {{id: '{summary_id_esc}'}})
             SET s.type = 'file_access',
                 s.app_id = '{app_esc}',
                 s.access_count = {count},
                 s.primary_application = '{app_esc}',
                 s.active_period_start = {period_start},
                 s.active_period_end = {period_end}"
        ))
        .await?;

    // Step 2: Create the SUMMARIZES edge.
    graph
        .write(format!(
            "MATCH (s:Summary {{id: '{summary_id_esc}'}}), (a:App {{id: '{app_esc}'}})
             MERGE (s)-[:SUMMARIZES]->(a)"
        ))
        .await?;

    info!(
        app_id,
        count, period_start, period_end, "retention: created summary node"
    );

    // Step 3: Delete the original File nodes (and their edges).
    graph
        .write(format!(
            "MATCH (f:File)-[:ACCESSED_BY]->(a:App {{id: '{app_esc}'}})
             WHERE f.last_accessed < {cutoff}
             AND NOT EXISTS {{
                 MATCH (p:PinnedMarker) WHERE p.node_id = f.id AND p.node_type = 'File'
             }}
             DETACH DELETE f"
        ))
        .await?;

    info!(app_id, count, "retention: deleted compacted file nodes");
    Ok(())
}

/// Parse a Cypher result that returns rows of a single string column.
///
/// The lbug query result is a formatted string table. This parser
/// extracts non-header, non-empty lines as string values.
fn parse_string_rows(result: &str) -> Vec<String> {
    result
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('-') && !line.contains("a.id"))
        .map(|line| line.trim().trim_matches('|').trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Parse an aggregation result: count, min, max from a single-row result.
///
/// Returns (0, 0, 0) if the result cannot be parsed.
fn parse_aggregation(result: &str) -> (i64, i64, i64) {
    let values: Vec<&str> = result
        .lines()
        .filter(|line| !line.is_empty() && !line.starts_with('-') && !line.contains("count"))
        .flat_map(|line| {
            line.split('|')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>()
        })
        .collect();

    if values.len() >= 3 {
        let count = values[0].parse().unwrap_or(0);
        let min = values[1].parse().unwrap_or(0);
        let max = values[2].parse().unwrap_or(0);
        (count, min, max)
    } else {
        (0, 0, 0)
    }
}
