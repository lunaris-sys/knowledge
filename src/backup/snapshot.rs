/// Snapper (Btrfs) snapshot integration for backup.

use std::path::PathBuf;

use chrono::{DateTime, Utc};
use thiserror::Error;

/// Snapshot configuration.
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Snapper config name (e.g. "home").
    pub snapper_config: String,
    /// Run WAL checkpoint before snapshot.
    pub pre_snapshot_hook: bool,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            snapper_config: "home".into(),
            pre_snapshot_hook: true,
        }
    }
}

/// Information about a created snapshot.
#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub number: u64,
    pub description: String,
    pub created_at: DateTime<Utc>,
}

/// Snapshot errors.
#[derive(Debug, Error)]
pub enum SnapshotError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapper: {0}")]
    Snapper(String),
    #[error("not implemented")]
    NotImplemented,
}

/// Integrates with Snapper for Btrfs-level backups.
pub struct SnapperIntegration {
    config: SnapshotConfig,
    _db_path: PathBuf,
}

impl SnapperIntegration {
    pub fn new(db_path: PathBuf, config: SnapshotConfig) -> Self {
        Self {
            config,
            _db_path: db_path,
        }
    }

    /// Build the snapper create command (without executing).
    pub fn create_command(&self, description: &str) -> Vec<String> {
        vec![
            "snapper".into(),
            "-c".into(),
            self.config.snapper_config.clone(),
            "create".into(),
            "--description".into(),
            description.into(),
            "--userdata".into(),
            format!("lunaris-backup={}", Utc::now().to_rfc3339()),
            "--print-number".into(),
        ]
    }

    /// Build the snapper list command.
    pub fn list_command(&self) -> Vec<String> {
        vec![
            "snapper".into(),
            "-c".into(),
            self.config.snapper_config.clone(),
            "list".into(),
            "--columns".into(),
            "number,date,description,userdata".into(),
        ]
    }

    /// Check if snapper is available on the system.
    pub fn is_available() -> bool {
        std::process::Command::new("snapper")
            .arg("--version")
            .output()
            .map(|o| o.status.success())
            .unwrap_or(false)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let c = SnapshotConfig::default();
        assert_eq!(c.snapper_config, "home");
        assert!(c.pre_snapshot_hook);
    }

    #[test]
    fn test_create_command() {
        let snap = SnapperIntegration::new(
            PathBuf::from("/tmp/test.db"),
            SnapshotConfig::default(),
        );
        let cmd = snap.create_command("daily backup");
        assert_eq!(cmd[0], "snapper");
        assert!(cmd.contains(&"-c".to_string()));
        assert!(cmd.contains(&"home".to_string()));
        assert!(cmd.contains(&"create".to_string()));
        assert!(cmd.contains(&"daily backup".to_string()));
        assert!(cmd.iter().any(|s| s.starts_with("lunaris-backup=")));
    }

    #[test]
    fn test_list_command() {
        let snap = SnapperIntegration::new(
            PathBuf::from("/tmp/test.db"),
            SnapshotConfig::default(),
        );
        let cmd = snap.list_command();
        assert!(cmd.contains(&"list".to_string()));
    }

    #[test]
    fn test_snapshot_info() {
        let info = SnapshotInfo {
            number: 42,
            description: "test".into(),
            created_at: Utc::now(),
        };
        assert_eq!(info.number, 42);
    }
}
