/// Entity update with optimistic locking, ownership checks, and scope enforcement.

use std::collections::HashMap;

use chrono::Utc;
use thiserror::Error;

use crate::schema::SchemaRegistry;
use crate::token::{CapabilityToken, InstanceScope};

use super::validation::{FieldValidator, WriteValidationError};

/// Errors from update operations.
#[derive(Debug, Error)]
pub enum UpdateError {
    #[error("validation: {0}")]
    Validation(#[from] WriteValidationError),
    #[error("permission denied: cannot write to {0}")]
    PermissionDenied(String),
    #[error("entity not found: {0}")]
    NotFound(String),
    #[error("version conflict: expected {expected}, found {actual}")]
    VersionConflict { expected: i64, actual: i64 },
    #[error("not owner: entity owned by {owner}, requester is {requester}")]
    NotOwner { owner: String, requester: String },
}

/// Existing entity state fetched from the database before updating.
pub struct ExistingEntity {
    pub id: String,
    pub owner: String,
    pub version: i64,
    pub deleted: bool,
}

/// Result of a successful update operation (before DB write).
pub struct UpdateResult {
    pub entity_id: String,
    pub entity_type: String,
    pub updates: HashMap<String, serde_json::Value>,
    pub new_version: i64,
}

/// Execute an update operation: validates, checks scopes/ownership/version,
/// injects _version and _modified_at.
pub fn update_entity(
    registry: &SchemaRegistry,
    entity_type: &str,
    entity_id: &str,
    data: HashMap<String, serde_json::Value>,
    expected_version: i64,
    token: &CapabilityToken,
    existing: &ExistingEntity,
) -> Result<UpdateResult, UpdateError> {
    // 1. Write scope check.
    if !token.can_write(entity_type) {
        return Err(UpdateError::PermissionDenied(entity_type.into()));
    }

    // 2. Ownership check (InstanceScope::Own).
    if token.instance_scope == InstanceScope::Own && existing.owner != token.app_id {
        return Err(UpdateError::NotOwner {
            owner: existing.owner.clone(),
            requester: token.app_id.clone(),
        });
    }

    // 3. Optimistic locking.
    if existing.version != expected_version {
        return Err(UpdateError::VersionConflict {
            expected: expected_version,
            actual: existing.version,
        });
    }

    // 4. Field validation (type checks, immutable fields).
    let validator = FieldValidator::new(registry);
    validator.validate_update(entity_type, &data)?;

    // 5. Build update with reserved field bumps.
    let now = Utc::now().to_rfc3339();
    let new_version = existing.version + 1;

    let mut updates = data;
    updates.insert("_version".into(), serde_json::json!(new_version));
    updates.insert("_modified_at".into(), serde_json::json!(now));

    Ok(UpdateResult {
        entity_id: entity_id.into(),
        entity_type: entity_type.into(),
        updates,
        new_version,
    })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaRegistry;
    use crate::token::{CapabilityToken, EntityScope, InstanceScope};

    fn setup() -> (SchemaRegistry, CapabilityToken, ExistingEntity) {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(
            r#"
[meta]
namespace = "com.test"

[entities.Note]
[entities.Note.fields.title]
type = "string"
required = true

[entities.Note.fields.body]
type = "text"

[entities.Note.fields.created_at]
type = "datetime"
immutable = true
"#,
        )
        .unwrap();

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
            InstanceScope::Own,
        );

        let existing = ExistingEntity {
            id: "entity-1".into(),
            owner: "com.test".into(),
            version: 1,
            deleted: false,
        };

        (reg, token, existing)
    }

    #[test]
    fn test_update_success() {
        let (reg, token, existing) = setup();
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Updated"));

        let result =
            update_entity(&reg, "com.test.Note", "entity-1", data, 1, &token, &existing)
                .unwrap();
        assert_eq!(result.new_version, 2);
        assert_eq!(result.updates["title"], "Updated");
        assert!(result.updates.contains_key("_modified_at"));
    }

    #[test]
    fn test_update_version_conflict() {
        let (reg, token, existing) = setup();
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Updated"));

        let result =
            update_entity(&reg, "com.test.Note", "entity-1", data, 99, &token, &existing);
        assert!(matches!(
            result,
            Err(UpdateError::VersionConflict {
                expected: 99,
                actual: 1
            })
        ));
    }

    #[test]
    fn test_update_not_owner() {
        let (reg, token, _) = setup();
        let existing = ExistingEntity {
            id: "entity-1".into(),
            owner: "com.other".into(),
            version: 1,
            deleted: false,
        };
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Updated"));

        assert!(matches!(
            update_entity(&reg, "com.test.Note", "entity-1", data, 1, &token, &existing),
            Err(UpdateError::NotOwner { .. })
        ));
    }

    #[test]
    fn test_update_permission_denied() {
        let (reg, _, existing) = setup();
        let token =
            CapabilityToken::new("com.test".into(), 1, vec![], vec![], vec![], InstanceScope::Own);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Updated"));

        assert!(matches!(
            update_entity(&reg, "com.test.Note", "entity-1", data, 1, &token, &existing),
            Err(UpdateError::PermissionDenied(_))
        ));
    }

    #[test]
    fn test_update_immutable_field_rejected() {
        let (reg, token, existing) = setup();
        let mut data = HashMap::new();
        data.insert("created_at".into(), serde_json::json!("2026-01-01T00:00:00Z"));

        assert!(matches!(
            update_entity(&reg, "com.test.Note", "entity-1", data, 1, &token, &existing),
            Err(UpdateError::Validation(_))
        ));
    }

    #[test]
    fn test_update_all_scope_skips_owner_check() {
        let (reg, _, existing_other) = setup();
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
        let existing = ExistingEntity {
            id: "entity-1".into(),
            owner: "com.other".into(), // different owner
            version: 1,
            deleted: false,
        };
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Updated"));

        // InstanceScope::All should bypass owner check.
        let result =
            update_entity(&reg, "com.test.Note", "entity-1", data, 1, &token, &existing);
        assert!(result.is_ok());
    }
}
