/// Entity creation with token scope enforcement and reserved field injection.

use std::collections::HashMap;

use chrono::Utc;
use thiserror::Error;
use uuid::Uuid;

use crate::schema::SchemaRegistry;
use crate::token::CapabilityToken;

use super::validation::{FieldValidator, WriteValidationError};

/// Errors from create operations.
#[derive(Debug, Error)]
pub enum CreateError {
    #[error("validation: {0}")]
    Validation(#[from] WriteValidationError),
    #[error("permission denied: cannot write to {0}")]
    PermissionDenied(String),
    #[error("namespace violation: {app_id} cannot create {entity_type}")]
    NamespaceViolation { app_id: String, entity_type: String },
}

/// Result of a successful create operation (before DB write).
pub struct CreateResult {
    pub id: Uuid,
    pub entity_type: String,
    pub data: HashMap<String, serde_json::Value>,
}

/// Executes a create operation: validates, checks scopes, injects reserved fields.
pub fn create_entity(
    registry: &SchemaRegistry,
    entity_type: &str,
    data: HashMap<String, serde_json::Value>,
    token: &CapabilityToken,
) -> Result<CreateResult, CreateError> {
    // 1. Token write scope check.
    if !token.can_write(entity_type) {
        return Err(CreateError::PermissionDenied(entity_type.into()));
    }

    // 2. Namespace check: app can only create in its own namespace.
    check_namespace(entity_type, &token.app_id)?;

    // 3. Field validation.
    let validator = FieldValidator::new(registry);
    validator.validate_create(entity_type, &data)?;

    // 4. Build entity with reserved fields.
    let id = Uuid::now_v7();
    let now = Utc::now().to_rfc3339();

    let mut entity = data;
    entity.insert("id".into(), serde_json::json!(id.to_string()));
    entity.insert("_version".into(), serde_json::json!(1));
    entity.insert("_owner".into(), serde_json::json!(token.app_id));
    entity.insert("_created_at".into(), serde_json::json!(now));
    entity.insert("_modified_at".into(), serde_json::json!(now));
    entity.insert("_deleted".into(), serde_json::json!(false));

    // 5. Apply defaults for missing optional fields.
    if let Some(entity_def) = registry.get_entity(entity_type) {
        for (field_name, field_def) in &entity_def.fields {
            if !entity.contains_key(field_name) {
                if let Some(default) = &field_def.default {
                    entity.insert(field_name.clone(), toml_to_json(default));
                }
            }
        }
    }

    Ok(CreateResult {
        id,
        entity_type: entity_type.into(),
        data: entity,
    })
}

fn check_namespace(entity_type: &str, app_id: &str) -> Result<(), CreateError> {
    if app_id == "system" && entity_type.starts_with("system.") {
        return Ok(());
    }
    let prefix = format!("{app_id}.");
    if !entity_type.starts_with(&prefix) {
        return Err(CreateError::NamespaceViolation {
            app_id: app_id.into(),
            entity_type: entity_type.into(),
        });
    }
    Ok(())
}

fn toml_to_json(v: &toml::Value) -> serde_json::Value {
    match v {
        toml::Value::String(s) => serde_json::json!(s),
        toml::Value::Integer(i) => serde_json::json!(i),
        toml::Value::Float(f) => serde_json::json!(f),
        toml::Value::Boolean(b) => serde_json::json!(b),
        toml::Value::Array(a) => {
            serde_json::Value::Array(a.iter().map(toml_to_json).collect())
        }
        _ => serde_json::Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaRegistry;
    use crate::token::{CapabilityToken, EntityScope, InstanceScope};

    fn setup() -> (SchemaRegistry, CapabilityToken) {
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

[entities.Note.fields.score]
type = "float"
default = 1.0
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

        (reg, token)
    }

    #[test]
    fn test_create_success() {
        let (reg, token) = setup();
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        let result = create_entity(&reg, "com.test.Note", data, &token).unwrap();
        assert_eq!(result.entity_type, "com.test.Note");
        assert!(result.data.contains_key("id"));
        assert_eq!(result.data["_version"], 1);
        assert_eq!(result.data["_owner"], "com.test");
        assert_eq!(result.data["_deleted"], false);
        assert!(result.data.contains_key("_created_at"));
        assert!(result.data.contains_key("_modified_at"));
    }

    #[test]
    fn test_create_defaults_applied() {
        let (reg, token) = setup();
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        let result = create_entity(&reg, "com.test.Note", data, &token).unwrap();
        assert_eq!(result.data["score"], 1.0);
    }

    #[test]
    fn test_create_permission_denied() {
        let (reg, _) = setup();
        let token = CapabilityToken::new("com.test".into(), 1, vec![], vec![], vec![], InstanceScope::Own);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        assert!(matches!(
            create_entity(&reg, "com.test.Note", data, &token),
            Err(CreateError::PermissionDenied(_))
        ));
    }

    #[test]
    fn test_create_namespace_violation() {
        let (reg, _) = setup();
        // Token with write scope for com.other.* but app_id is com.test.
        // This tests that even with write scope, namespace must match app_id.
        let token = CapabilityToken::new(
            "com.test".into(),
            1234,
            vec![],
            vec![EntityScope {
                entity_type: "com.other.*".into(),
                fields: None,
                exclude_fields: vec![],
            }],
            vec![],
            InstanceScope::Own,
        );
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));

        assert!(matches!(
            create_entity(&reg, "com.other.Note", data, &token),
            Err(CreateError::NamespaceViolation { .. })
        ));
    }

    #[test]
    fn test_create_validation_error() {
        let (reg, token) = setup();
        let data = HashMap::new(); // missing required title

        assert!(matches!(
            create_entity(&reg, "com.test.Note", data, &token),
            Err(CreateError::Validation(_))
        ));
    }
}
