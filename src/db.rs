use crate::proto::Event;
use anyhow::Result;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use tracing::debug;

/// Open (or create) the `SQLite` database and run schema migrations.
pub async fn open(path: &str) -> Result<SqlitePool> {
    // CREATE DATABASE IF NOT EXISTS equivalent in SQLite:
    // the file is created on first open.
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect(&format!("sqlite:{path}?mode=rwc"))
        .await?;

    create_schema(&pool).await?;
    Ok(pool)
}

/// Create the events table if it does not exist.
/// In production we would use ``sqlx::migrate!`` with versioned migration files.
/// For Phase 1A we keep it simple with a single CREATE TABLE IF NOT EXISTS.
async fn create_schema(pool: &SqlitePool) -> Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS events (
            id          TEXT    PRIMARY KEY,
            type        TEXT    NOT NULL,
            timestamp   INTEGER NOT NULL,
            source      TEXT    NOT NULL,
            pid         INTEGER NOT NULL,
            session_id  TEXT    NOT NULL,
            payload     BLOB
        )",
    )
    .execute(pool)
    .await?;

    // Index on timestamp for time-range queries (most common access pattern).
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events (timestamp)",
    )
    .execute(pool)
    .await?;

    // Index on type for event-type filtering.
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_events_type ON events (type)",
    )
    .execute(pool)
    .await?;

    debug!("schema ready");
    Ok(())
}

/// Write a batch of events to `SQLite` in a single transaction.
///
/// A transaction groups multiple writes into one atomic operation.
/// Either all writes succeed or none do. This is faster than individual
/// inserts because `SQLite` only has to sync to disk once per transaction
/// rather than once per row.
///
/// We use INSERT OR IGNORE to handle duplicate IDs gracefully:
/// if an event with the same UUID v7 already exists (e.g. due to a retry),
/// the insert is skipped silently rather than returning an error.
pub async fn write_batch(pool: &SqlitePool, events: &[Event]) -> Result<usize> {
    if events.is_empty() {
        return Ok(0);
    }

    // Begin a transaction. In Rust's sqlx, the transaction is committed
    // when `tx.commit()` is called, or rolled back when it is dropped.
    // This is RAII (Resource Acquisition Is Initialization), similar to
    // `using var tx = connection.BeginTransaction()` in C# but enforced
    // by the type system.
    let mut tx = pool.begin().await?;

    for event in events {
        sqlx::query(
            "INSERT OR IGNORE INTO events
                (id, type, timestamp, source, pid, session_id, payload)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&event.id)
        .bind(&event.r#type)
        .bind(event.timestamp)
        .bind(&event.source)
        .bind(i64::from(event.pid))
        .bind(&event.session_id)
        .bind(event.payload.as_slice())
        .execute(&mut *tx)
        .await?;
    }

    tx.commit().await?;

    debug!(count = events.len(), "wrote batch to SQLite");
    Ok(events.len())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_event(id: &str, event_type: &str) -> Event {
        Event {
            id: id.to_string(),
            r#type: event_type.to_string(),
            timestamp: 1_000_000,
            source: "test".to_string(),
            pid: 1,
            session_id: "session-test".to_string(),
            payload: vec![],
        }
    }

    #[tokio::test]
    async fn write_and_count() {
        // :memory: creates an in-memory SQLite database that is discarded
        // when the pool is closed. Perfect for tests: no file cleanup needed.
        let pool = open(":memory:").await.expect("failed to open db");

        let events = vec![
            make_event("id-1", "file.opened"),
            make_event("id-2", "window.focused"),
        ];

        let written = write_batch(&pool, &events).await.expect("write failed");
        assert_eq!(written, 2);

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .expect("count failed");
        assert_eq!(count.0, 2);
    }

    #[tokio::test]
    async fn duplicate_id_is_ignored() {
        let pool = open(":memory:").await.expect("failed to open db");

        let event = make_event("id-1", "file.opened");
        write_batch(&pool, std::slice::from_ref(&event)).await.expect("first write failed");
        write_batch(&pool, &[event]).await.expect("second write failed");

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM events")
            .fetch_one(&pool)
            .await
            .expect("count failed");

        // Duplicate should be silently ignored, not inserted twice.
        assert_eq!(count.0, 1);
    }
}
