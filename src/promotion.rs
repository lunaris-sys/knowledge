use crate::graph::GraphHandle;
use crate::project::ProjectStore;
use crate::proto::{
    AnnotationClearPayload, AnnotationSetPayload, FileOpenedPayload, PresenceClearPayload,
    PresenceSetPayload, TimelineRecordPayload, WindowFocusedPayload,
};
use crate::utils::escape_cypher;
use anyhow::Result;
use prost::Message;
use sqlx::SqlitePool;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

/// Fixed UUIDv5 namespace for deriving deterministic annotation ids
/// from the `(target_type, target_id, namespace)` triple. The exact
/// bytes are arbitrary but must stay stable forever — they are baked
/// into every Annotation node ever written. Changing this would
/// orphan existing annotations on the next set.
const ANNOTATION_UUID_NAMESPACE: uuid::Uuid = uuid::Uuid::from_bytes([
    0x6e, 0xed, 0x73, 0x05, 0xc4, 0x83, 0x4d, 0x73, 0xa6, 0x86, 0xc1, 0x73, 0x4d, 0xb1, 0x29, 0x7e,
]);

/// Derive the deterministic Annotation node id from the spec's
/// composite identity (target_type, target_id, namespace). UUIDv5 so
/// the same triple always maps to the same id, enabling MERGE-based
/// dedup in promotion without a separate lookup query.
pub(crate) fn annotation_id(target_type: &str, target_id: &str, namespace: &str) -> uuid::Uuid {
    let key = format!("{target_type}\x1f{target_id}\x1f{namespace}");
    uuid::Uuid::new_v5(&ANNOTATION_UUID_NAMESPACE, key.as_bytes())
}

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
            "app.presence.set" => {
                promote_presence_set(graph, id, timestamp, payload).await
            }
            "app.presence.clear" => {
                promote_presence_clear(graph, id, timestamp, payload).await
            }
            "app.timeline.record" => {
                promote_timeline_record(graph, id, timestamp, payload).await
            }
            "app.annotation.set" => {
                promote_annotation_set(graph, timestamp, payload).await
            }
            "app.annotation.cleared" => promote_annotation_cleared(graph, payload).await,
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

/// Promote an `app.presence.set` event into a UserAction node with
/// `category = "presence"`. The metadata map and auto_clear hint stay in
/// the SQLite event row — the graph node is intentionally lightweight so
/// presence queries (e.g. "what was I editing yesterday at 14:00") stay
/// fast and the per-app metadata schemas don't pollute the graph schema.
async fn promote_presence_set(
    graph: &GraphHandle,
    event_id: &str,
    timestamp: &i64,
    payload: &[u8],
) -> Result<()> {
    let p = PresenceSetPayload::decode(payload)?;
    let id_esc = escape_cypher(event_id);
    let activity_esc = escape_cypher(&p.activity);
    let subject_esc = escape_cypher(&p.subject);

    graph
        .write(format!(
            "MERGE (u:UserAction {{id: '{id_esc}'}})
             SET u.category = 'presence',
                 u.action   = '{activity_esc}',
                 u.subject  = '{subject_esc}',
                 u.timestamp = {timestamp}"
        ))
        .await?;

    debug!(event_id, app_id = %p.app_id, activity = %p.activity, "promoted app.presence.set");
    Ok(())
}

/// Promote an `app.presence.clear` event. Apps emit this when their
/// previous presence state is no longer accurate — explicit clear, or
/// auto-clear from the SDK's window-blur listener. We record the clear
/// as its own UserAction so a query can reconstruct presence intervals
/// (set timestamp .. clear timestamp).
async fn promote_presence_clear(
    graph: &GraphHandle,
    event_id: &str,
    timestamp: &i64,
    payload: &[u8],
) -> Result<()> {
    let p = PresenceClearPayload::decode(payload)?;
    let id_esc = escape_cypher(event_id);
    let app_esc = escape_cypher(&p.app_id);

    graph
        .write(format!(
            "MERGE (u:UserAction {{id: '{id_esc}'}})
             SET u.category = 'presence',
                 u.action   = 'clear',
                 u.subject  = '{app_esc}',
                 u.timestamp = {timestamp}"
        ))
        .await?;

    debug!(event_id, app_id = %p.app_id, "promoted app.presence.clear");
    Ok(())
}

/// Promote an `app.timeline.record` event into a UserAction node with
/// `category = "timeline"`. Persistent semantic record — distinct from
/// presence which is ephemeral. Started/ended timestamps and metadata
/// remain in the SQLite event row; the graph node carries the type as
/// `action` and the user-facing label as `subject`.
async fn promote_timeline_record(
    graph: &GraphHandle,
    event_id: &str,
    _timestamp: &i64,
    payload: &[u8],
) -> Result<()> {
    let p = TimelineRecordPayload::decode(payload)?;
    let id_esc = escape_cypher(event_id);
    let type_esc = escape_cypher(&p.r#type);
    let label_esc = escape_cypher(&p.label);
    // Use ended_at when present (duration event), otherwise started_at,
    // and finally fall back to the wall-clock timestamp from the
    // Event envelope. This keeps timeline queries time-ordered by the
    // *user-meaningful* moment rather than when the event arrived.
    let ts = if p.ended_at != 0 {
        p.ended_at
    } else if p.started_at != 0 {
        p.started_at
    } else {
        *_timestamp
    };

    graph
        .write(format!(
            "MERGE (u:UserAction {{id: '{id_esc}'}})
             SET u.category = 'timeline',
                 u.action   = '{type_esc}',
                 u.subject  = '{label_esc}',
                 u.timestamp = {ts}"
        ))
        .await?;

    debug!(event_id, app_id = %p.app_id, label = %p.label, "promoted app.timeline.record");
    Ok(())
}

/// Promote an `app.annotation.set` event into an Annotation node
/// keyed by the deterministic UUIDv5 of (target_type, target_id,
/// namespace). MERGE-style upsert: re-setting on the same triple
/// updates `data` and `last_modified` while preserving `created_at`.
///
/// Foundation §395 — apps write only to their own namespace, the
/// daemon does not enforce that here yet (write-token-authentication
/// is Phase 3.2-full); for now the SDK declares its own namespace
/// honestly and the trust boundary is the SO_PEERCRED-derived uid on
/// the producer socket.
async fn promote_annotation_set(
    graph: &GraphHandle,
    timestamp: &i64,
    payload: &[u8],
) -> Result<()> {
    let p = AnnotationSetPayload::decode(payload)?;
    let id = annotation_id(&p.target_type, &p.target_id, &p.namespace);

    let id_esc = escape_cypher(&id.to_string());
    let ns_esc = escape_cypher(&p.namespace);
    let tt_esc = escape_cypher(&p.target_type);
    let ti_esc = escape_cypher(&p.target_id);
    let data_esc = escape_cypher(&p.data_json);

    // MERGE with ON CREATE / ON MATCH split so created_at is set
    // exactly once on the very first write and `last_modified`
    // advances on every subsequent re-set. Kuzu accepts the full
    // openCypher MERGE clause; if a future Kuzu release narrows
    // this we fall back to two queries (the test for replace-keeps-
    // created_at would catch a regression).
    graph
        .write(format!(
            "MERGE (a:Annotation {{id: '{id_esc}'}})
             ON CREATE SET a.namespace = '{ns_esc}',
                           a.target_type = '{tt_esc}',
                           a.target_id = '{ti_esc}',
                           a.data = '{data_esc}',
                           a.created_at = {timestamp},
                           a.last_modified = {timestamp}
             ON MATCH SET a.data = '{data_esc}',
                          a.last_modified = {timestamp}"
        ))
        .await?;

    debug!(
        target_type = %p.target_type,
        target_id = %p.target_id,
        namespace = %p.namespace,
        annotation_id = %id,
        "promoted app.annotation.set"
    );
    Ok(())
}

/// Promote an `app.annotation.cleared` event by removing the
/// Annotation node keyed on the same deterministic id. Idempotent:
/// clearing a non-existent annotation is a no-op (Kuzu's MATCH +
/// DELETE silently affects zero rows).
async fn promote_annotation_cleared(graph: &GraphHandle, payload: &[u8]) -> Result<()> {
    let p = AnnotationClearPayload::decode(payload)?;
    let id = annotation_id(&p.target_type, &p.target_id, &p.namespace);
    let id_esc = escape_cypher(&id.to_string());

    graph
        .write(format!(
            "MATCH (a:Annotation {{id: '{id_esc}'}}) DETACH DELETE a"
        ))
        .await?;

    debug!(
        target_type = %p.target_type,
        target_id = %p.target_id,
        namespace = %p.namespace,
        annotation_id = %id,
        "promoted app.annotation.cleared"
    );
    Ok(())
}

#[cfg(test)]
mod shell_event_tests {
    use super::*;
    use std::collections::HashMap;
    use tempfile::TempDir;

    async fn setup() -> (GraphHandle, TempDir) {
        let tmp = TempDir::new().unwrap();
        let graph =
            crate::graph::spawn(tmp.path().join("graph").to_str().unwrap()).unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
        (graph, tmp)
    }

    fn encode_presence_set(p: &PresenceSetPayload) -> Vec<u8> {
        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        buf
    }

    fn encode_presence_clear(p: &PresenceClearPayload) -> Vec<u8> {
        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        buf
    }

    fn encode_timeline(p: &TimelineRecordPayload) -> Vec<u8> {
        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        buf
    }

    async fn count_user_actions_by_category(graph: &GraphHandle, category: &str) -> i64 {
        let rs = graph
            .query_rows(format!(
                "MATCH (u:UserAction) WHERE u.category = '{category}' RETURN count(*) AS cnt"
            ))
            .await
            .unwrap();
        rs.rows
            .first()
            .and_then(|r| r.first())
            .map(|v| v.as_i64())
            .unwrap_or(0)
    }

    #[tokio::test]
    async fn presence_set_creates_user_action() {
        let (graph, _tmp) = setup().await;
        let payload = PresenceSetPayload {
            app_id: "com.example.editor".into(),
            activity: "editing".into(),
            subject: "/home/tim/notes.md".into(),
            project: String::new(),
            auto_clear: "on-blur".into(),
            metadata: HashMap::new(),
        };
        let bytes = encode_presence_set(&payload);

        promote_presence_set(&graph, "evt-presence-1", &1_000_000, &bytes)
            .await
            .unwrap();

        assert_eq!(count_user_actions_by_category(&graph, "presence").await, 1);
    }

    #[tokio::test]
    async fn presence_clear_creates_user_action() {
        let (graph, _tmp) = setup().await;
        let payload = PresenceClearPayload {
            app_id: "com.example.editor".into(),
        };
        let bytes = encode_presence_clear(&payload);

        promote_presence_clear(&graph, "evt-presence-clear-1", &2_000_000, &bytes)
            .await
            .unwrap();

        // Clear is also a presence-category record.
        let rs = graph
            .query_rows(
                "MATCH (u:UserAction) WHERE u.category = 'presence' AND u.action = 'clear' \
                 RETURN u.subject"
                    .to_string(),
            )
            .await
            .unwrap();
        assert_eq!(rs.rows.len(), 1);
    }

    #[tokio::test]
    async fn timeline_record_uses_ended_at_when_set() {
        let (graph, _tmp) = setup().await;
        let payload = TimelineRecordPayload {
            app_id: "com.example.builder".into(),
            label: "Build succeeded".into(),
            subject: "coffeeshop".into(),
            r#type: "build".into(),
            started_at: 5_000_000,
            ended_at: 9_500_000,
            metadata: HashMap::new(),
        };
        let bytes = encode_timeline(&payload);

        promote_timeline_record(&graph, "evt-timeline-1", &10_000_000, &bytes)
            .await
            .unwrap();

        let rs = graph
            .query_rows(
                "MATCH (u:UserAction) WHERE u.category = 'timeline' \
                 RETURN u.timestamp, u.action, u.subject"
                    .to_string(),
            )
            .await
            .unwrap();
        let row = rs.rows.first().expect("user action created");
        assert_eq!(row[0].as_i64(), 9_500_000); // ended_at wins
        assert_eq!(row[1].as_str(), "build");
        assert_eq!(row[2].as_str(), "Build succeeded");
    }

    #[tokio::test]
    async fn timeline_record_falls_back_to_envelope_timestamp() {
        let (graph, _tmp) = setup().await;
        // Point-in-time event: both started_at and ended_at are 0 in the
        // payload; promotion must fall back to the Event envelope's
        // wall-clock timestamp.
        let payload = TimelineRecordPayload {
            app_id: "com.example.editor".into(),
            label: "Exported PDF".into(),
            subject: "/home/tim/report.pdf".into(),
            r#type: "export".into(),
            started_at: 0,
            ended_at: 0,
            metadata: HashMap::new(),
        };
        let bytes = encode_timeline(&payload);

        promote_timeline_record(&graph, "evt-timeline-2", &7_777_777, &bytes)
            .await
            .unwrap();

        let rs = graph
            .query_rows(
                "MATCH (u:UserAction) WHERE u.category = 'timeline' \
                 RETURN u.timestamp"
                    .to_string(),
            )
            .await
            .unwrap();
        assert_eq!(rs.rows[0][0].as_i64(), 7_777_777);
    }

    fn encode_annotation_set(p: &AnnotationSetPayload) -> Vec<u8> {
        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        buf
    }

    fn encode_annotation_clear(p: &AnnotationClearPayload) -> Vec<u8> {
        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        buf
    }

    async fn fetch_annotation(
        graph: &GraphHandle,
        target_type: &str,
        target_id: &str,
        namespace: &str,
    ) -> Option<(String, i64, i64)> {
        // Returns (data, created_at, last_modified) for the matching annotation.
        let rs = graph
            .query_rows(format!(
                "MATCH (a:Annotation) WHERE a.target_type = '{target_type}' \
                 AND a.target_id = '{target_id}' AND a.namespace = '{namespace}' \
                 RETURN a.data, a.created_at, a.last_modified"
            ))
            .await
            .unwrap();
        rs.rows.first().map(|r| {
            (
                r[0].as_str().to_string(),
                r[1].as_i64(),
                r[2].as_i64(),
            )
        })
    }

    #[tokio::test]
    async fn annotation_set_creates_node() {
        let (graph, _tmp) = setup().await;
        let payload = AnnotationSetPayload {
            app_id: "com.example.editor".into(),
            namespace: "com.example.editor".into(),
            target_type: "File".into(),
            target_id: "/home/tim/report.md".into(),
            data_json: r#"{"word_count":1240}"#.into(),
        };

        promote_annotation_set(&graph, &1_000_000, &encode_annotation_set(&payload))
            .await
            .unwrap();

        let got = fetch_annotation(&graph, "File", "/home/tim/report.md", "com.example.editor")
            .await
            .expect("annotation should exist");
        assert_eq!(got.0, r#"{"word_count":1240}"#);
        assert_eq!(got.1, 1_000_000); // created_at
        assert_eq!(got.2, 1_000_000); // last_modified
    }

    #[tokio::test]
    async fn annotation_re_set_replaces_data_and_keeps_created_at() {
        let (graph, _tmp) = setup().await;
        let p1 = AnnotationSetPayload {
            app_id: "com.example.editor".into(),
            namespace: "com.example.editor".into(),
            target_type: "File".into(),
            target_id: "/home/tim/notes.md".into(),
            data_json: r#"{"word_count":100}"#.into(),
        };
        let p2 = AnnotationSetPayload {
            app_id: "com.example.editor".into(),
            namespace: "com.example.editor".into(),
            target_type: "File".into(),
            target_id: "/home/tim/notes.md".into(),
            data_json: r#"{"word_count":250}"#.into(),
        };

        promote_annotation_set(&graph, &1_000, &encode_annotation_set(&p1))
            .await
            .unwrap();
        promote_annotation_set(&graph, &5_000, &encode_annotation_set(&p2))
            .await
            .unwrap();

        let got = fetch_annotation(&graph, "File", "/home/tim/notes.md", "com.example.editor")
            .await
            .unwrap();
        assert_eq!(got.0, r#"{"word_count":250}"#); // new data
        assert_eq!(got.1, 1_000); // original created_at preserved
        assert_eq!(got.2, 5_000); // last_modified advanced
    }

    #[tokio::test]
    async fn annotation_clear_removes_node() {
        let (graph, _tmp) = setup().await;
        let set_payload = AnnotationSetPayload {
            app_id: "com.example.editor".into(),
            namespace: "com.example.editor".into(),
            target_type: "File".into(),
            target_id: "/x".into(),
            data_json: "{}".into(),
        };
        let clear_payload = AnnotationClearPayload {
            app_id: "com.example.editor".into(),
            namespace: "com.example.editor".into(),
            target_type: "File".into(),
            target_id: "/x".into(),
        };

        promote_annotation_set(&graph, &100, &encode_annotation_set(&set_payload))
            .await
            .unwrap();
        assert!(fetch_annotation(&graph, "File", "/x", "com.example.editor")
            .await
            .is_some());

        promote_annotation_cleared(&graph, &encode_annotation_clear(&clear_payload))
            .await
            .unwrap();
        assert!(fetch_annotation(&graph, "File", "/x", "com.example.editor")
            .await
            .is_none());
    }

    #[tokio::test]
    async fn annotation_clear_on_missing_is_noop() {
        let (graph, _tmp) = setup().await;
        let clear_payload = AnnotationClearPayload {
            app_id: "com.example.editor".into(),
            namespace: "com.example.editor".into(),
            target_type: "File".into(),
            target_id: "/never-set".into(),
        };
        // Must not panic / error.
        promote_annotation_cleared(&graph, &encode_annotation_clear(&clear_payload))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn annotation_id_is_deterministic_across_triple() {
        // The UUIDv5 derivation is part of the wire contract: SDK
        // queries by (target_type, target_id, namespace) but the
        // graph node uses the derived id. If this drifts, queries
        // return empty when annotations exist.
        let id1 = annotation_id("File", "/x", "com.app");
        let id2 = annotation_id("File", "/x", "com.app");
        assert_eq!(id1, id2);

        let id3 = annotation_id("File", "/y", "com.app");
        assert_ne!(id1, id3);

        let id4 = annotation_id("File", "/x", "com.other");
        assert_ne!(id1, id4);
    }

    #[tokio::test]
    async fn annotation_namespaces_are_independent_for_same_target() {
        // Two apps annotate the same File — must produce two
        // independent Annotation nodes, each addressable by its own
        // namespace.
        let (graph, _tmp) = setup().await;
        let editor = AnnotationSetPayload {
            app_id: "com.example.editor".into(),
            namespace: "com.example.editor".into(),
            target_type: "File".into(),
            target_id: "/shared.md".into(),
            data_json: r#"{"word_count":500}"#.into(),
        };
        let git = AnnotationSetPayload {
            app_id: "com.example.git".into(),
            namespace: "com.example.git".into(),
            target_type: "File".into(),
            target_id: "/shared.md".into(),
            data_json: r#"{"branch":"main"}"#.into(),
        };

        promote_annotation_set(&graph, &10, &encode_annotation_set(&editor))
            .await
            .unwrap();
        promote_annotation_set(&graph, &20, &encode_annotation_set(&git))
            .await
            .unwrap();

        let editor_got = fetch_annotation(&graph, "File", "/shared.md", "com.example.editor")
            .await
            .unwrap();
        let git_got = fetch_annotation(&graph, "File", "/shared.md", "com.example.git")
            .await
            .unwrap();
        assert_eq!(editor_got.0, r#"{"word_count":500}"#);
        assert_eq!(git_got.0, r#"{"branch":"main"}"#);
    }
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
