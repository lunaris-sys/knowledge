/// Background cleanup: expired trash, orphan references, cascade deletes.

use chrono::Duration;

use crate::schema::SchemaRegistry;
use crate::utils::escape_cypher;

/// Configuration for cleanup jobs.
#[derive(Debug, Clone)]
pub struct CleanupConfig {
    /// Default trash retention in days.
    pub default_retention_days: u32,
    /// Max entities to process per batch.
    pub batch_size: usize,
    /// Whether to clean orphan references.
    pub clean_orphans: bool,
}

impl Default for CleanupConfig {
    fn default() -> Self {
        Self {
            default_retention_days: 30,
            batch_size: 1000,
            clean_orphans: true,
        }
    }
}

/// A cleanup job that generates Cypher queries for maintenance tasks.
pub struct CleanupJob<'a> {
    registry: &'a SchemaRegistry,
    config: CleanupConfig,
}

impl<'a> CleanupJob<'a> {
    pub fn new(registry: &'a SchemaRegistry, config: CleanupConfig) -> Self {
        Self { registry, config }
    }

    /// Get the retention period for an entity type (from schema or default).
    pub fn retention_for_type(&self, entity_type: &str) -> Duration {
        self.registry
            .get_entity(entity_type)
            .map(|def| Duration::days(def.lifecycle.trash_retention_days as i64))
            .unwrap_or_else(|| Duration::days(self.config.default_retention_days as i64))
    }

    /// Generate Cypher to find expired trash for a specific entity type.
    pub fn expired_trash_query(&self, entity_type: &str) -> String {
        let retention = self.retention_for_type(entity_type);
        let cutoff = chrono::Utc::now() - retention;
        format!(
            "MATCH (n) WHERE n._type = '{}' AND n._deleted = true \
             AND n._deleted_at < '{}' \
             RETURN n.id LIMIT {}",
            escape_cypher(entity_type),
            cutoff.to_rfc3339(),
            self.config.batch_size,
        )
    }

    /// Generate Cypher to permanently delete an entity and its relations.
    pub fn permanent_delete_query(entity_id: &str) -> String {
        format!(
            "MATCH (n) WHERE n.id = '{}' DETACH DELETE n",
            escape_cypher(entity_id),
        )
    }

    /// Generate Cypher to find orphan references (edges pointing to deleted nodes).
    pub fn orphan_reference_query(&self) -> String {
        format!(
            "MATCH (n)-[r]->(m) WHERE m._deleted = true \
             RETURN n.id, type(r), m.id LIMIT {}",
            self.config.batch_size,
        )
    }

    /// Generate Cypher to nullify an orphan reference.
    pub fn nullify_reference_query(source_id: &str, field: &str) -> String {
        format!(
            "MATCH (n) WHERE n.id = '{}' SET n.{} = null",
            escape_cypher(source_id),
            escape_cypher(field),
        )
    }
}

/// Outcome of a cleanup action.
#[derive(Debug)]
pub enum CleanupAction {
    PermanentDelete {
        entity_id: String,
        entity_type: String,
    },
    NullifyReference {
        source_id: String,
        field: String,
    },
}

/// Statistics from a cleanup run.
#[derive(Debug, Default)]
pub struct CleanupStats {
    pub entities_deleted: usize,
    pub references_nullified: usize,
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaRegistry;

    fn registry() -> SchemaRegistry {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(
            r#"
[meta]
namespace = "com.test"

[entities.Note]
[entities.Note.fields.title]
type = "string"

[entities.Note.lifecycle]
trash_retention_days = 60

[entities.Log]
[entities.Log.fields.msg]
type = "string"
"#,
        )
        .unwrap();
        reg
    }

    #[test]
    fn test_retention_from_schema() {
        let reg = registry();
        let job = CleanupJob::new(&reg, CleanupConfig::default());
        let ret = job.retention_for_type("com.test.Note");
        assert_eq!(ret.num_days(), 60);
    }

    #[test]
    fn test_retention_default() {
        let reg = registry();
        let job = CleanupJob::new(&reg, CleanupConfig::default());
        // Log has default lifecycle (30 days).
        let ret = job.retention_for_type("com.test.Log");
        assert_eq!(ret.num_days(), 30);
    }

    #[test]
    fn test_retention_unknown_type() {
        let reg = registry();
        let config = CleanupConfig {
            default_retention_days: 45,
            ..Default::default()
        };
        let job = CleanupJob::new(&reg, config);
        let ret = job.retention_for_type("com.unknown.Thing");
        assert_eq!(ret.num_days(), 45);
    }

    #[test]
    fn test_expired_trash_query() {
        let reg = registry();
        let job = CleanupJob::new(&reg, CleanupConfig::default());
        let q = job.expired_trash_query("com.test.Note");
        assert!(q.contains("n._type = 'com.test.Note'"));
        assert!(q.contains("n._deleted = true"));
        assert!(q.contains("LIMIT 1000"));
    }

    #[test]
    fn test_permanent_delete_query() {
        let q = CleanupJob::permanent_delete_query("abc-123");
        assert!(q.contains("n.id = 'abc-123'"));
        assert!(q.contains("DETACH DELETE"));
    }

    #[test]
    fn test_orphan_reference_query() {
        let reg = registry();
        let job = CleanupJob::new(&reg, CleanupConfig::default());
        let q = job.orphan_reference_query();
        assert!(q.contains("m._deleted = true"));
        assert!(q.contains("LIMIT 1000"));
    }

    #[test]
    fn test_nullify_reference_query() {
        let q = CleanupJob::nullify_reference_query("src-1", "deck_ref");
        assert!(q.contains("n.id = 'src-1'"));
        assert!(q.contains("n.deck_ref = null"));
    }
}
