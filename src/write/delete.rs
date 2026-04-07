/// Entity deletion (soft and permanent) with scope and ownership enforcement.

use chrono::Utc;
use thiserror::Error;

use crate::token::{CapabilityToken, InstanceScope};

use super::update::ExistingEntity;

/// Errors from delete operations.
#[derive(Debug, Error)]
pub enum DeleteError {
    #[error("permission denied: cannot write to {0}")]
    PermissionDenied(String),
    #[error("not owner: entity owned by {owner}, requester is {requester}")]
    NotOwner { owner: String, requester: String },
    #[error("entity already deleted")]
    AlreadyDeleted,
}

/// Result of a successful delete operation (before DB write).
pub struct DeleteResult {
    pub entity_id: String,
    pub entity_type: String,
    /// Whether this was a soft delete (true) or permanent (false).
    pub soft: bool,
    pub deleted_at: String,
}

/// Soft-delete: sets `_deleted = true`, `_deleted_at = now`.
/// Entity remains in the graph for `trash_retention_days`.
pub fn soft_delete(
    entity_type: &str,
    entity_id: &str,
    token: &CapabilityToken,
    existing: &ExistingEntity,
) -> Result<DeleteResult, DeleteError> {
    check_delete_permissions(entity_type, token, existing)?;

    if existing.deleted {
        return Err(DeleteError::AlreadyDeleted);
    }

    Ok(DeleteResult {
        entity_id: entity_id.into(),
        entity_type: entity_type.into(),
        soft: true,
        deleted_at: Utc::now().to_rfc3339(),
    })
}

/// Permanent delete: removes the entity and all its relations from the graph.
pub fn permanent_delete(
    entity_type: &str,
    entity_id: &str,
    token: &CapabilityToken,
    existing: &ExistingEntity,
) -> Result<DeleteResult, DeleteError> {
    check_delete_permissions(entity_type, token, existing)?;

    Ok(DeleteResult {
        entity_id: entity_id.into(),
        entity_type: entity_type.into(),
        soft: false,
        deleted_at: Utc::now().to_rfc3339(),
    })
}

fn check_delete_permissions(
    entity_type: &str,
    token: &CapabilityToken,
    existing: &ExistingEntity,
) -> Result<(), DeleteError> {
    if !token.can_write(entity_type) {
        return Err(DeleteError::PermissionDenied(entity_type.into()));
    }

    if token.instance_scope == InstanceScope::Own && existing.owner != token.app_id {
        return Err(DeleteError::NotOwner {
            owner: existing.owner.clone(),
            requester: token.app_id.clone(),
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{CapabilityToken, EntityScope, InstanceScope};

    fn token_with_write() -> CapabilityToken {
        CapabilityToken::new(
            "com.test".into(),
            1234,
            vec![],
            vec![EntityScope {
                entity_type: "com.test.*".into(),
                fields: None,
                exclude_fields: vec![],
            }],
            vec![],
            InstanceScope::Own,
        )
    }

    fn existing(owner: &str, deleted: bool) -> ExistingEntity {
        ExistingEntity {
            id: "entity-1".into(),
            owner: owner.into(),
            version: 1,
            deleted,
        }
    }

    #[test]
    fn test_soft_delete_success() {
        let token = token_with_write();
        let ent = existing("com.test", false);
        let result = soft_delete("com.test.Note", "entity-1", &token, &ent).unwrap();
        assert!(result.soft);
        assert!(!result.deleted_at.is_empty());
    }

    #[test]
    fn test_soft_delete_already_deleted() {
        let token = token_with_write();
        let ent = existing("com.test", true);
        assert!(matches!(
            soft_delete("com.test.Note", "entity-1", &token, &ent),
            Err(DeleteError::AlreadyDeleted)
        ));
    }

    #[test]
    fn test_soft_delete_not_owner() {
        let token = token_with_write();
        let ent = existing("com.other", false);
        assert!(matches!(
            soft_delete("com.test.Note", "entity-1", &token, &ent),
            Err(DeleteError::NotOwner { .. })
        ));
    }

    #[test]
    fn test_soft_delete_permission_denied() {
        let token = CapabilityToken::new(
            "com.test".into(), 1, vec![], vec![], vec![], InstanceScope::Own,
        );
        let ent = existing("com.test", false);
        assert!(matches!(
            soft_delete("com.test.Note", "entity-1", &token, &ent),
            Err(DeleteError::PermissionDenied(_))
        ));
    }

    #[test]
    fn test_permanent_delete_success() {
        let token = token_with_write();
        let ent = existing("com.test", false);
        let result = permanent_delete("com.test.Note", "entity-1", &token, &ent).unwrap();
        assert!(!result.soft);
    }

    #[test]
    fn test_permanent_delete_of_deleted_entity() {
        let token = token_with_write();
        let ent = existing("com.test", true);
        // Permanent delete should work even on already soft-deleted entities.
        let result = permanent_delete("com.test.Note", "entity-1", &token, &ent).unwrap();
        assert!(!result.soft);
    }

    #[test]
    fn test_delete_all_scope_bypasses_owner() {
        let token = CapabilityToken::new(
            "com.test".into(),
            1234,
            vec![],
            vec![EntityScope {
                entity_type: "com.test.*".into(),
                fields: None,
                exclude_fields: vec![],
            }],
            vec![],
            InstanceScope::All,
        );
        let ent = existing("com.other", false);
        assert!(soft_delete("com.test.Note", "entity-1", &token, &ent).is_ok());
    }
}
