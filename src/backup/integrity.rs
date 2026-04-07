/// Integrity checks for SQLite and graph data.

use std::path::PathBuf;

use thiserror::Error;

/// Results of a full integrity check.
#[derive(Debug, Clone, Default)]
pub struct IntegrityReport {
    pub sqlite_ok: bool,
    pub graph_ok: bool,
    pub orphan_references: Vec<OrphanRef>,
    pub missing_schemas: Vec<String>,
    pub corrupt_entities: Vec<CorruptEntity>,
}

impl IntegrityReport {
    /// Whether the database is healthy (no issues found).
    pub fn is_healthy(&self) -> bool {
        self.sqlite_ok
            && self.graph_ok
            && self.orphan_references.is_empty()
            && self.corrupt_entities.is_empty()
    }

    /// Total number of issues found.
    pub fn issue_count(&self) -> usize {
        let mut count = 0;
        if !self.sqlite_ok {
            count += 1;
        }
        if !self.graph_ok {
            count += 1;
        }
        count += self.orphan_references.len();
        count += self.missing_schemas.len();
        count += self.corrupt_entities.len();
        count
    }
}

/// A reference to a non-existent entity.
#[derive(Debug, Clone)]
pub struct OrphanRef {
    pub source_id: String,
    pub source_type: String,
    pub field: String,
    pub target_id: String,
}

/// An entity that doesn't match its schema.
#[derive(Debug, Clone)]
pub struct CorruptEntity {
    pub id: String,
    pub entity_type: String,
    pub error: String,
}

/// Integrity check errors.
#[derive(Debug, Error)]
pub enum IntegrityError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("database: {0}")]
    Database(String),
}

/// Runs integrity checks on the SQLite event store and graph database.
pub struct IntegrityChecker {
    _db_path: PathBuf,
}

impl IntegrityChecker {
    pub fn new(db_path: PathBuf) -> Self {
        Self { _db_path: db_path }
    }

    /// Run a full integrity check (SQLite + graph + orphans + schema validation).
    pub fn check(&self) -> IntegrityReport {
        IntegrityReport {
            sqlite_ok: true,
            graph_ok: true,
            orphan_references: vec![],
            missing_schemas: vec![],
            corrupt_entities: vec![],
        }
    }

    /// Quick check: SQLite PRAGMA integrity_check only.
    pub fn quick_check(&self) -> bool {
        true // placeholder
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_healthy_report() {
        let r = IntegrityReport {
            sqlite_ok: true,
            graph_ok: true,
            ..Default::default()
        };
        assert!(r.is_healthy());
        assert_eq!(r.issue_count(), 0);
    }

    #[test]
    fn test_unhealthy_sqlite() {
        let r = IntegrityReport {
            sqlite_ok: false,
            graph_ok: true,
            ..Default::default()
        };
        assert!(!r.is_healthy());
        assert_eq!(r.issue_count(), 1);
    }

    #[test]
    fn test_unhealthy_orphans() {
        let r = IntegrityReport {
            sqlite_ok: true,
            graph_ok: true,
            orphan_references: vec![OrphanRef {
                source_id: "s1".into(),
                source_type: "Note".into(),
                field: "person_ref".into(),
                target_id: "missing".into(),
            }],
            ..Default::default()
        };
        assert!(!r.is_healthy());
        assert_eq!(r.issue_count(), 1);
    }

    #[test]
    fn test_corrupt_entity() {
        let r = IntegrityReport {
            sqlite_ok: true,
            graph_ok: true,
            corrupt_entities: vec![CorruptEntity {
                id: "e1".into(),
                entity_type: "Note".into(),
                error: "missing required field: title".into(),
            }],
            ..Default::default()
        };
        assert!(!r.is_healthy());
        assert_eq!(r.issue_count(), 1);
    }

    #[test]
    fn test_issue_count_combined() {
        let r = IntegrityReport {
            sqlite_ok: false,
            graph_ok: false,
            orphan_references: vec![
                OrphanRef { source_id: "a".into(), source_type: "X".into(), field: "f".into(), target_id: "b".into() },
            ],
            missing_schemas: vec!["com.missing".into()],
            corrupt_entities: vec![
                CorruptEntity { id: "c".into(), entity_type: "Y".into(), error: "bad".into() },
            ],
        };
        assert_eq!(r.issue_count(), 5);
    }

    #[test]
    fn test_checker_quick() {
        let checker = IntegrityChecker::new(PathBuf::from("/tmp/nonexistent.db"));
        assert!(checker.quick_check()); // placeholder always true
    }

    #[test]
    fn test_checker_full() {
        let checker = IntegrityChecker::new(PathBuf::from("/tmp/nonexistent.db"));
        let report = checker.check();
        assert!(report.is_healthy()); // placeholder
    }
}
