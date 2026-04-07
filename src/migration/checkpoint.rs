/// Migration checkpoint for resumable migrations.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Tracks the progress of a running migration so it can be resumed after
/// a daemon restart.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MigrationCheckpoint {
    pub app_id: String,
    pub from_version: String,
    pub to_version: String,
    pub operation_index: usize,
    pub entities_processed: usize,
    pub last_entity_id: Option<String>,
    pub started_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl MigrationCheckpoint {
    /// Create a new checkpoint at the start of a migration.
    pub fn new(app_id: &str, from: &str, to: &str) -> Self {
        let now = Utc::now();
        Self {
            app_id: app_id.into(),
            from_version: from.into(),
            to_version: to.into(),
            operation_index: 0,
            entities_processed: 0,
            last_entity_id: None,
            started_at: now,
            updated_at: now,
        }
    }

    /// File path for this checkpoint.
    pub fn path_for(app_id: &str) -> PathBuf {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join("lunaris/graph/migrations")
            .join(format!("{app_id}.checkpoint.json"))
    }

    /// Load a checkpoint from disk (returns None if file missing or invalid).
    pub fn load(app_id: &str) -> Option<Self> {
        let path = Self::path_for(app_id);
        let content = std::fs::read_to_string(&path).ok()?;
        serde_json::from_str(&content).ok()
    }

    /// Persist checkpoint to disk.
    pub fn save(&self) -> std::io::Result<()> {
        let path = Self::path_for(&self.app_id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, content)
    }

    /// Delete checkpoint (migration completed successfully).
    pub fn delete(app_id: &str) -> std::io::Result<()> {
        let path = Self::path_for(app_id);
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        Ok(())
    }

    /// Move to the next operation.
    pub fn advance_operation(&mut self) {
        self.operation_index += 1;
        self.entities_processed = 0;
        self.last_entity_id = None;
        self.updated_at = Utc::now();
    }

    /// Record processing of one entity within the current operation.
    pub fn advance_entity(&mut self, entity_id: &str) {
        self.entities_processed += 1;
        self.last_entity_id = Some(entity_id.into());
        self.updated_at = Utc::now();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_checkpoint() {
        let cp = MigrationCheckpoint::new("com.anki", "1.0.0", "1.1.0");
        assert_eq!(cp.app_id, "com.anki");
        assert_eq!(cp.from_version, "1.0.0");
        assert_eq!(cp.to_version, "1.1.0");
        assert_eq!(cp.operation_index, 0);
        assert_eq!(cp.entities_processed, 0);
        assert!(cp.last_entity_id.is_none());
    }

    #[test]
    fn test_advance_operation() {
        let mut cp = MigrationCheckpoint::new("com.anki", "1.0.0", "1.1.0");
        cp.advance_entity("e-1");
        cp.advance_entity("e-2");
        assert_eq!(cp.entities_processed, 2);

        cp.advance_operation();
        assert_eq!(cp.operation_index, 1);
        assert_eq!(cp.entities_processed, 0);
        assert!(cp.last_entity_id.is_none());
    }

    #[test]
    fn test_advance_entity() {
        let mut cp = MigrationCheckpoint::new("com.anki", "1.0.0", "1.1.0");
        cp.advance_entity("entity-abc");
        assert_eq!(cp.entities_processed, 1);
        assert_eq!(cp.last_entity_id.as_deref(), Some("entity-abc"));
    }

    #[test]
    fn test_save_load_roundtrip() {
        let mut cp = MigrationCheckpoint::new("com.test.checkpoint", "1.0.0", "2.0.0");
        cp.advance_operation();
        cp.advance_entity("e-42");

        cp.save().unwrap();
        let loaded = MigrationCheckpoint::load("com.test.checkpoint").unwrap();
        assert_eq!(loaded.app_id, "com.test.checkpoint");
        assert_eq!(loaded.operation_index, 1);
        assert_eq!(loaded.entities_processed, 1);
        assert_eq!(loaded.last_entity_id.as_deref(), Some("e-42"));

        MigrationCheckpoint::delete("com.test.checkpoint").unwrap();
        assert!(MigrationCheckpoint::load("com.test.checkpoint").is_none());
    }

    #[test]
    fn test_load_missing() {
        assert!(MigrationCheckpoint::load("com.nonexistent.xyz").is_none());
    }
}
