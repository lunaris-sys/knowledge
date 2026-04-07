/// Staged uninstall: mark entities for pending deletion with a grace period.
///
/// When an app is uninstalled, its entities are marked `_pending_delete = true`
/// with a timestamp. If the app is reinstalled within the grace period (30 days),
/// the mark is removed. After the grace period, entities are permanently deleted.

use chrono::Utc;

use crate::utils::escape_cypher;

/// Mark all entities from an uninstalled app for pending deletion.
pub fn mark_pending(app_id: &str) -> PendingDeleteQuery {
    PendingDeleteQuery {
        app_id: app_id.into(),
        marked_at: Utc::now(),
    }
}

/// Remove pending-delete marks when an app is reinstalled.
pub fn unmark_pending(app_id: &str) -> UnmarkPendingQuery {
    UnmarkPendingQuery {
        app_id: app_id.into(),
    }
}

/// Find entities past the grace period for permanent deletion.
pub fn find_expired_pending(grace_period_days: u32) -> ExpiredPendingQuery {
    ExpiredPendingQuery { grace_period_days }
}

/// Cypher query to mark entities as pending delete.
pub struct PendingDeleteQuery {
    pub app_id: String,
    pub marked_at: chrono::DateTime<Utc>,
}

impl PendingDeleteQuery {
    pub fn to_cypher(&self) -> String {
        format!(
            "MATCH (n) WHERE n._owner = '{}' \
             SET n._pending_delete = true, n._pending_delete_at = '{}'",
            escape_cypher(&self.app_id),
            self.marked_at.to_rfc3339(),
        )
    }
}

/// Cypher query to remove pending-delete marks (reinstall).
pub struct UnmarkPendingQuery {
    pub app_id: String,
}

impl UnmarkPendingQuery {
    pub fn to_cypher(&self) -> String {
        format!(
            "MATCH (n) WHERE n._owner = '{}' AND n._pending_delete = true \
             REMOVE n._pending_delete, n._pending_delete_at",
            escape_cypher(&self.app_id),
        )
    }
}

/// Cypher query to find entities past the grace period.
pub struct ExpiredPendingQuery {
    pub grace_period_days: u32,
}

impl ExpiredPendingQuery {
    pub fn to_cypher(&self) -> String {
        let cutoff = Utc::now() - chrono::Duration::days(self.grace_period_days as i64);
        format!(
            "MATCH (n) WHERE n._pending_delete = true \
             AND n._pending_delete_at < '{}' \
             RETURN n.id, n._type",
            cutoff.to_rfc3339(),
        )
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mark_pending_query() {
        let q = mark_pending("com.anki");
        let cypher = q.to_cypher();
        assert!(cypher.contains("n._owner = 'com.anki'"));
        assert!(cypher.contains("n._pending_delete = true"));
        assert!(cypher.contains("n._pending_delete_at"));
    }

    #[test]
    fn test_unmark_pending_query() {
        let q = unmark_pending("com.anki");
        let cypher = q.to_cypher();
        assert!(cypher.contains("n._owner = 'com.anki'"));
        assert!(cypher.contains("n._pending_delete = true"));
        assert!(cypher.contains("REMOVE n._pending_delete"));
    }

    #[test]
    fn test_expired_pending_query() {
        let q = find_expired_pending(30);
        let cypher = q.to_cypher();
        assert!(cypher.contains("n._pending_delete = true"));
        assert!(cypher.contains("n._pending_delete_at <"));
        assert!(cypher.contains("RETURN n.id, n._type"));
    }

    #[test]
    fn test_mark_pending_escapes() {
        let q = mark_pending("com.test's");
        let cypher = q.to_cypher();
        assert!(cypher.contains("com.test\\'s"));
    }
}
