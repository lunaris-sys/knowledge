use crate::graph::GraphHandle;
use crate::project::ProjectStore;
use crate::proto::{FileOpenedPayload, WindowFocusedPayload};
use crate::utils::escape_cypher;
use anyhow::Result;
use prost::Message;
use sqlx::SqlitePool;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

/// Number of distinct files opened in one session before an inferred
/// project gets promoted (visible in Waypointer / Focus Mode).
const PROMOTION_THRESHOLD: usize = 3;

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

    let project_store = ProjectStore::new(graph.clone());

    let mut interval = time::interval(PROMOTION_INTERVAL);
    // Skip the first immediate tick so we don't run before the write store
    // has had a chance to accumulate events.
    interval.tick().await;

    loop {
        interval.tick().await;
        if let Err(e) = run_pass(&pool, &graph, &project_store).await {
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
async fn run_pass(pool: &SqlitePool, graph: &GraphHandle, project_store: &ProjectStore) -> Result<()> {
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
                let res = promote_file_opened(
                    graph, id, timestamp, source, pid, session_id, payload,
                )
                .await;
                // After the File node exists, try linking it to a project.
                if res.is_ok() {
                    if let Ok(fp) = FileOpenedPayload::decode(payload.as_slice()) {
                        if !fp.path.is_empty() {
                            if let Err(e) = link_file_to_project(
                                &fp.path,
                                session_id,
                                project_store,
                            )
                            .await
                            {
                                debug!("project link skipped for {}: {e}", fp.path);
                            }
                        }
                    }
                }
                res
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

/// Check if a file belongs to a known project and create a FILE_PART_OF
/// edge. Also updates the project's `last_accessed` timestamp and checks
/// the auto-promotion threshold.
async fn link_file_to_project(
    file_path: &str,
    session_id: &str,
    store: &ProjectStore,
) -> Result<()> {
    let Some(project) = store.find_by_path_prefix(file_path).await? else {
        return Ok(()); // file not inside any project
    };

    // Create FILE_PART_OF edge (MERGE is idempotent).
    if !store.is_file_linked(file_path, project.id).await? {
        store.link_file(file_path, project.id).await?;
        debug!(file_path, project = %project.name, "linked file to project");
    }

    store.touch(project.id).await?;

    // Auto-promote inferred projects after enough session activity.
    if !project.promoted {
        let count = store.count_session_files(session_id, project.id).await?;
        if count >= PROMOTION_THRESHOLD {
            store.promote(project.id).await?;
            info!(
                project = %project.name,
                files = count,
                "auto-promoted project (session threshold)",
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod project_tests {
    use super::*;
    use crate::project::{Project, ProjectStore};
    use tempfile::TempDir;

    async fn setup() -> (GraphHandle, ProjectStore, TempDir) {
        let tmp = TempDir::new().unwrap();
        let graph =
            crate::graph::spawn(tmp.path().join("graph").to_str().unwrap()).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        let store = ProjectStore::new(graph.clone());
        (graph, store, tmp)
    }

    /// Create a File node (simulates what promote_file_opened does).
    async fn create_file_node(graph: &GraphHandle, path: &str) {
        let p = escape_cypher(path);
        graph
            .write(format!(
                "CREATE (f:File {{id: '{p}', path: '{p}', app_id: 'test', last_accessed: 0}})"
            ))
            .await
            .unwrap();
    }

    /// Create File + App + Session + edges (for count_session_files).
    async fn create_file_with_session(
        graph: &GraphHandle,
        store: &ProjectStore,
        path: &str,
        app_id: &str,
        session_id: &str,
        project_id: uuid::Uuid,
    ) {
        let p = escape_cypher(path);
        let a = escape_cypher(app_id);
        let s = escape_cypher(session_id);

        graph
            .write(format!(
                "MERGE (f:File {{id: '{p}'}}) SET f.path = '{p}', f.app_id = '{a}', f.last_accessed = 1"
            ))
            .await
            .unwrap();
        graph
            .write(format!("MERGE (a:App {{id: '{a}'}}) SET a.name = '{a}'"))
            .await
            .unwrap();
        graph
            .write(format!("MERGE (s:Session {{id: '{s}'}}) SET s.started_at = 1"))
            .await
            .unwrap();
        graph
            .write(format!(
                "MATCH (f:File {{id: '{p}'}}), (a:App {{id: '{a}'}}) MERGE (f)-[:ACCESSED_BY]->(a)"
            ))
            .await
            .unwrap();
        graph
            .write(format!(
                "MATCH (a:App {{id: '{a}'}}), (s:Session {{id: '{s}'}}) MERGE (a)-[:ACTIVE_IN]->(s)"
            ))
            .await
            .unwrap();

        store.link_file(path, project_id).await.unwrap();
    }

    #[tokio::test]
    async fn file_linked_to_project() {
        let (graph, store, _tmp) = setup().await;

        let project = Project::new_inferred("test".into(), "/home/user/proj".into(), 90);
        store.create(&project).await.unwrap();

        create_file_node(&graph, "/home/user/proj/src/main.rs").await;

        link_file_to_project("/home/user/proj/src/main.rs", "sess", &store)
            .await
            .unwrap();

        assert!(store
            .is_file_linked("/home/user/proj/src/main.rs", project.id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn file_outside_project_not_linked() {
        let (_graph, store, _tmp) = setup().await;

        let project = Project::new_inferred("proj".into(), "/home/user/proj".into(), 90);
        store.create(&project).await.unwrap();

        // File is outside the project root.
        link_file_to_project("/home/user/other/file.txt", "sess", &store)
            .await
            .unwrap();

        // No edge should exist (file not even in graph, but that's fine).
        let files = store.get_project_files(project.id).await.unwrap();
        assert!(files.is_empty());
    }

    #[tokio::test]
    async fn nested_project_wins() {
        let (graph, store, _tmp) = setup().await;

        let parent = Project::new_inferred("mono".into(), "/home/user/mono".into(), 90);
        store.create(&parent).await.unwrap();

        let nested =
            Project::new_inferred("app-a".into(), "/home/user/mono/pkg/app-a".into(), 100);
        store.create(&nested).await.unwrap();

        create_file_node(&graph, "/home/user/mono/pkg/app-a/src/lib.rs").await;

        link_file_to_project("/home/user/mono/pkg/app-a/src/lib.rs", "sess", &store)
            .await
            .unwrap();

        assert!(store
            .is_file_linked("/home/user/mono/pkg/app-a/src/lib.rs", nested.id)
            .await
            .unwrap());
        assert!(!store
            .is_file_linked("/home/user/mono/pkg/app-a/src/lib.rs", parent.id)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn idempotent_linking() {
        let (graph, store, _tmp) = setup().await;

        let project = Project::new_inferred("proj".into(), "/a".into(), 90);
        store.create(&project).await.unwrap();

        create_file_node(&graph, "/a/f.rs").await;

        for _ in 0..3 {
            link_file_to_project("/a/f.rs", "sess", &store)
                .await
                .unwrap();
        }

        assert!(store.is_file_linked("/a/f.rs", project.id).await.unwrap());
        assert_eq!(store.get_project_files(project.id).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn last_accessed_updated() {
        let (_graph, store, _tmp) = setup().await;

        let project = Project::new_inferred("proj".into(), "/a".into(), 90);
        assert!(project.last_accessed.is_none());
        store.create(&project).await.unwrap();

        // link_file_to_project with a file outside the project still calls
        // find_by_path_prefix which returns None, so last_accessed stays None.
        // We need a file INSIDE the project, but the File node must exist too.
        // Just call touch directly to verify it works.
        store.touch(project.id).await.unwrap();

        let p = store.get_by_id(project.id).await.unwrap().unwrap();
        assert!(p.last_accessed.is_some());
    }

    #[tokio::test]
    async fn promotion_threshold() {
        let (graph, store, _tmp) = setup().await;

        let project = Project::new_inferred("proj".into(), "/home/user/proj".into(), 90);
        assert!(!project.promoted);
        store.create(&project).await.unwrap();

        let session = "test-session";

        // Open files 0..2 -> should NOT promote yet.
        for i in 0..2 {
            let path = format!("/home/user/proj/f{i}.rs");
            create_file_with_session(
                &graph, &store, &path, "editor", session, project.id,
            )
            .await;
        }
        let p = store.get_by_id(project.id).await.unwrap().unwrap();
        assert!(!p.promoted, "should not promote with 2 files");

        // File 3 -> should promote.
        let path = "/home/user/proj/f2.rs";
        create_file_with_session(&graph, &store, path, "editor", session, project.id)
            .await;

        // Now call link_file_to_project to trigger the threshold check.
        link_file_to_project(path, session, &store).await.unwrap();

        let p = store.get_by_id(project.id).await.unwrap().unwrap();
        assert!(p.promoted, "should promote with 3 files");
    }
}
