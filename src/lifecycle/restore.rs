/// Restore soft-deleted entities from trash.

use chrono::Utc;
use thiserror::Error;

use crate::token::{CapabilityToken, InstanceScope};
use crate::write::ExistingEntity;

/// Errors from restore operations.
#[derive(Debug, Error)]
pub enum RestoreError {
    #[error("entity not deleted")]
    NotDeleted,
    #[error("permission denied: cannot write to {0}")]
    PermissionDenied(String),
    #[error("not owner: entity owned by {owner}")]
    NotOwner { owner: String },
}

/// Result of a successful restore.
pub struct RestoreResult {
    pub entity_id: String,
    pub entity_type: String,
    pub restored_at: String,
    pub new_version: i64,
}

/// Restore a soft-deleted entity.
///
/// After restore the entity has:
/// - `_deleted = false`
/// - `_deleted_at` removed
/// - `_version` incremented
/// - `_modified_at` updated
pub fn restore_entity(
    entity_type: &str,
    entity_id: &str,
    token: &CapabilityToken,
    existing: &ExistingEntity,
) -> Result<RestoreResult, RestoreError> {
    if !existing.deleted {
        return Err(RestoreError::NotDeleted);
    }

    if !token.can_write(entity_type) {
        return Err(RestoreError::PermissionDenied(entity_type.into()));
    }

    if token.instance_scope == InstanceScope::Own && existing.owner != token.app_id {
        return Err(RestoreError::NotOwner {
            owner: existing.owner.clone(),
        });
    }

    let now = Utc::now();

    Ok(RestoreResult {
        entity_id: entity_id.into(),
        entity_type: entity_type.into(),
        restored_at: now.to_rfc3339(),
        new_version: existing.version + 1,
    })
}

/// Generate the Cypher SET clause for a restore operation.
pub fn restore_cypher(result: &RestoreResult) -> String {
    format!(
        "MATCH (n) WHERE n.id = '{}' \
         SET n._deleted = false, n._deleted_at = null, \
         n._version = {}, n._modified_at = '{}'",
        crate::utils::escape_cypher(&result.entity_id),
        result.new_version,
        crate::utils::escape_cypher(&result.restored_at),
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{CapabilityToken, EntityScope, InstanceScope};

    fn write_token(app_id: &str) -> CapabilityToken {
        CapabilityToken::new(
            app_id.into(),
            1234,
            vec![],
            vec![EntityScope {
                entity_type: format!("{app_id}.*"),
                fields: None,
                exclude_fields: vec![],
            }],
            vec![],
            InstanceScope::Own,
        )
    }

    fn deleted_entity(owner: &str) -> ExistingEntity {
        ExistingEntity {
            id: "e-1".into(),
            owner: owner.into(),
            version: 3,
            deleted: true,
        }
    }

    #[test]
    fn test_restore_success() {
        let token = write_token("com.test");
        let existing = deleted_entity("com.test");
        let result =
            restore_entity("com.test.Note", "e-1", &token, &existing).unwrap();
        assert_eq!(result.entity_id, "e-1");
        assert_eq!(result.new_version, 4);
        assert!(!result.restored_at.is_empty());
    }

    #[test]
    fn test_restore_not_deleted() {
        let token = write_token("com.test");
        let existing = ExistingEntity {
            id: "e-1".into(),
            owner: "com.test".into(),
            version: 1,
            deleted: false,
        };
        assert!(matches!(
            restore_entity("com.test.Note", "e-1", &token, &existing),
            Err(RestoreError::NotDeleted)
        ));
    }

    #[test]
    fn test_restore_permission_denied() {
        let token = CapabilityToken::new(
            "com.test".into(), 1, vec![], vec![], vec![], InstanceScope::Own,
        );
        let existing = deleted_entity("com.test");
        assert!(matches!(
            restore_entity("com.test.Note", "e-1", &token, &existing),
            Err(RestoreError::PermissionDenied(_))
        ));
    }

    #[test]
    fn test_restore_not_owner() {
        let token = write_token("com.test");
        let existing = deleted_entity("com.other");
        assert!(matches!(
            restore_entity("com.test.Note", "e-1", &token, &existing),
            Err(RestoreError::NotOwner { .. })
        ));
    }

    #[test]
    fn test_restore_cypher() {
        let result = RestoreResult {
            entity_id: "e-1".into(),
            entity_type: "com.test.Note".into(),
            restored_at: "2026-04-07T12:00:00Z".into(),
            new_version: 4,
        };
        let cypher = restore_cypher(&result);
        assert!(cypher.contains("n._deleted = false"));
        assert!(cypher.contains("n._deleted_at = null"));
        assert!(cypher.contains("n._version = 4"));
    }
}
