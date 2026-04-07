/// Trash query helpers: filter deleted entities in Cypher queries.

use std::collections::HashMap;

use crate::utils::escape_cypher;

/// Controls whether queries include or exclude soft-deleted entities.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeletedFilter {
    /// Exclude deleted entities (default for all user queries).
    #[default]
    ExcludeDeleted,
    /// Return only deleted entities (trash view).
    OnlyDeleted,
    /// Return all entities regardless of deletion state.
    IncludeAll,
}

impl DeletedFilter {
    /// Cypher WHERE clause fragment (assumes the node alias is `n`).
    pub fn to_cypher(&self) -> &'static str {
        match self {
            Self::ExcludeDeleted => "AND n._deleted = false",
            Self::OnlyDeleted => "AND n._deleted = true",
            Self::IncludeAll => "",
        }
    }
}

/// Query to list trash contents for an app.
pub struct TrashQuery {
    pub app_id: String,
    pub entity_type: Option<String>,
    pub limit: usize,
    pub offset: usize,
}

impl TrashQuery {
    /// Generate Cypher for listing trash contents.
    pub fn to_cypher(&self) -> String {
        let type_filter = match &self.entity_type {
            Some(t) => format!("AND n._type = '{}'", escape_cypher(t)),
            None => String::new(),
        };

        format!(
            "MATCH (n) WHERE n._owner = '{}' AND n._deleted = true {} \
             RETURN n ORDER BY n._deleted_at DESC SKIP {} LIMIT {}",
            escape_cypher(&self.app_id),
            type_filter,
            self.offset,
            self.limit,
        )
    }
}

/// Summary statistics for an app's trash contents.
#[derive(Debug, Default)]
pub struct TrashSummary {
    pub total_count: usize,
    pub by_type: HashMap<String, usize>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deleted_filter_exclude() {
        let f = DeletedFilter::ExcludeDeleted;
        assert_eq!(f.to_cypher(), "AND n._deleted = false");
    }

    #[test]
    fn test_deleted_filter_only() {
        let f = DeletedFilter::OnlyDeleted;
        assert_eq!(f.to_cypher(), "AND n._deleted = true");
    }

    #[test]
    fn test_deleted_filter_all() {
        let f = DeletedFilter::IncludeAll;
        assert_eq!(f.to_cypher(), "");
    }

    #[test]
    fn test_deleted_filter_default() {
        let f = DeletedFilter::default();
        assert_eq!(f, DeletedFilter::ExcludeDeleted);
    }

    #[test]
    fn test_trash_query_basic() {
        let q = TrashQuery {
            app_id: "com.test".into(),
            entity_type: None,
            limit: 50,
            offset: 0,
        };
        let cypher = q.to_cypher();
        assert!(cypher.contains("n._owner = 'com.test'"));
        assert!(cypher.contains("n._deleted = true"));
        assert!(cypher.contains("SKIP 0 LIMIT 50"));
        assert!(!cypher.contains("n._type"));
    }

    #[test]
    fn test_trash_query_with_type_filter() {
        let q = TrashQuery {
            app_id: "com.test".into(),
            entity_type: Some("com.test.Note".into()),
            limit: 10,
            offset: 5,
        };
        let cypher = q.to_cypher();
        assert!(cypher.contains("n._type = 'com.test.Note'"));
        assert!(cypher.contains("SKIP 5 LIMIT 10"));
    }

    #[test]
    fn test_trash_query_escapes() {
        let q = TrashQuery {
            app_id: "com.test's".into(),
            entity_type: None,
            limit: 10,
            offset: 0,
        };
        let cypher = q.to_cypher();
        assert!(cypher.contains("com.test\\'s"));
    }
}
