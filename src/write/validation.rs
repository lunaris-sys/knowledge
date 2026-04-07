/// Field validation against the Schema Registry.

use std::collections::HashMap;

use thiserror::Error;

use crate::schema::{EntityDefinition, FieldDefinition, FieldType, SchemaRegistry};

/// Validation errors for write operations.
#[derive(Debug, Error)]
pub enum WriteValidationError {
    #[error("entity type not found: {0}")]
    EntityTypeNotFound(String),
    #[error("required field missing: {0}")]
    RequiredFieldMissing(String),
    #[error("unknown field: {0}")]
    UnknownField(String),
    #[error("type mismatch for '{field}': expected {expected}, got {actual}")]
    TypeMismatch {
        field: String,
        expected: String,
        actual: String,
    },
    #[error("immutable field cannot be changed: {0}")]
    ImmutableField(String),
}

/// Validates field data against entity schema definitions.
pub struct FieldValidator<'a> {
    registry: &'a SchemaRegistry,
}

impl<'a> FieldValidator<'a> {
    pub fn new(registry: &'a SchemaRegistry) -> Self {
        Self { registry }
    }

    /// Validate data for a create operation.
    ///
    /// Checks: entity type exists, required fields present, field types match,
    /// no unknown fields.
    pub fn validate_create(
        &self,
        entity_type: &str,
        data: &HashMap<String, serde_json::Value>,
    ) -> Result<(), WriteValidationError> {
        let entity_def = self
            .registry
            .get_entity(entity_type)
            .ok_or_else(|| WriteValidationError::EntityTypeNotFound(entity_type.into()))?;

        // Required fields must be present (unless they have a default).
        for (name, def) in &entity_def.fields {
            if def.required && !data.contains_key(name) && def.default.is_none() {
                return Err(WriteValidationError::RequiredFieldMissing(name.clone()));
            }
        }

        // All provided fields must exist in schema and match type.
        for (name, value) in data {
            let def = entity_def
                .fields
                .get(name)
                .ok_or_else(|| WriteValidationError::UnknownField(name.clone()))?;
            validate_field_type(name, value, def)?;
        }

        Ok(())
    }

    /// Validate data for an update operation.
    ///
    /// Same as create but also rejects immutable fields.
    pub fn validate_update(
        &self,
        entity_type: &str,
        data: &HashMap<String, serde_json::Value>,
    ) -> Result<(), WriteValidationError> {
        let entity_def = self
            .registry
            .get_entity(entity_type)
            .ok_or_else(|| WriteValidationError::EntityTypeNotFound(entity_type.into()))?;

        for (name, value) in data {
            let def = entity_def
                .fields
                .get(name)
                .ok_or_else(|| WriteValidationError::UnknownField(name.clone()))?;

            if def.immutable {
                return Err(WriteValidationError::ImmutableField(name.clone()));
            }

            validate_field_type(name, value, def)?;
        }

        Ok(())
    }

    /// Get the entity definition for a type.
    pub fn entity_def(&self, entity_type: &str) -> Option<&EntityDefinition> {
        self.registry.get_entity(entity_type)
    }
}

/// Check that a JSON value matches the expected field type.
fn validate_field_type(
    name: &str,
    value: &serde_json::Value,
    def: &FieldDefinition,
) -> Result<(), WriteValidationError> {
    let ok = match (&def.field_type, value) {
        // String-like types.
        (
            FieldType::String
            | FieldType::Text
            | FieldType::Url
            | FieldType::Email
            | FieldType::Path
            | FieldType::Markdown
            | FieldType::Color
            | FieldType::Uuid,
            serde_json::Value::String(_),
        ) => true,
        // Numeric types.
        (FieldType::Int, serde_json::Value::Number(n)) => n.is_i64(),
        (FieldType::Float, serde_json::Value::Number(_)) => true,
        // Boolean.
        (FieldType::Bool, serde_json::Value::Bool(_)) => true,
        // Temporal (stored as ISO strings).
        (FieldType::Datetime | FieldType::Date | FieldType::Duration, serde_json::Value::String(_)) => true,
        // Binary (base64 string).
        (FieldType::Bytes, serde_json::Value::String(_)) => true,
        // JSON (any value).
        (FieldType::Json, _) => true,
        // Array types.
        (FieldType::StringList, serde_json::Value::Array(arr)) => {
            arr.iter().all(|v| v.is_string())
        }
        (FieldType::IntList, serde_json::Value::Array(arr)) => {
            arr.iter().all(|v| v.as_i64().is_some())
        }
        (FieldType::FloatList, serde_json::Value::Array(arr)) => {
            arr.iter().all(|v| v.is_number())
        }
        (FieldType::BoolList, serde_json::Value::Array(arr)) => {
            arr.iter().all(|v| v.is_boolean())
        }
        // Reference (UUID string).
        (FieldType::Reference(_), serde_json::Value::String(_)) => true,
        // Null is allowed for non-required fields.
        (_, serde_json::Value::Null) => !def.required,
        _ => false,
    };

    if !ok {
        return Err(WriteValidationError::TypeMismatch {
            field: name.into(),
            expected: format!("{:?}", def.field_type),
            actual: json_type_name(value).into(),
        });
    }

    Ok(())
}

fn json_type_name(v: &serde_json::Value) -> &'static str {
    match v {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::SchemaRegistry;

    fn registry_with_schema() -> SchemaRegistry {
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

[entities.Note.fields.count]
type = "int"

[entities.Note.fields.created_at]
type = "datetime"
immutable = true

[entities.Note.fields.tags]
type = "string[]"

[entities.Note.fields.score]
type = "float"
default = 1.0
"#,
        )
        .unwrap();
        reg
    }

    #[test]
    fn test_create_valid() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));
        data.insert("body".into(), serde_json::json!("World"));
        assert!(v.validate_create("com.test.Note", &data).is_ok());
    }

    #[test]
    fn test_create_missing_required() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let data = HashMap::new(); // missing title
        assert!(matches!(
            v.validate_create("com.test.Note", &data),
            Err(WriteValidationError::RequiredFieldMissing(f)) if f == "title"
        ));
    }

    #[test]
    fn test_create_unknown_field() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Hello"));
        data.insert("nonexistent".into(), serde_json::json!("x"));
        assert!(matches!(
            v.validate_create("com.test.Note", &data),
            Err(WriteValidationError::UnknownField(f)) if f == "nonexistent"
        ));
    }

    #[test]
    fn test_create_type_mismatch() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!(123)); // should be string
        assert!(matches!(
            v.validate_create("com.test.Note", &data),
            Err(WriteValidationError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn test_create_entity_not_found() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let data = HashMap::new();
        assert!(matches!(
            v.validate_create("com.test.Missing", &data),
            Err(WriteValidationError::EntityTypeNotFound(_))
        ));
    }

    #[test]
    fn test_update_immutable_rejected() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("created_at".into(), serde_json::json!("2026-01-01T00:00:00Z"));
        assert!(matches!(
            v.validate_update("com.test.Note", &data),
            Err(WriteValidationError::ImmutableField(f)) if f == "created_at"
        ));
    }

    #[test]
    fn test_update_valid() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("Updated"));
        data.insert("count".into(), serde_json::json!(42));
        assert!(v.validate_update("com.test.Note", &data).is_ok());
    }

    #[test]
    fn test_array_field_validation() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("T"));
        data.insert("tags".into(), serde_json::json!(["a", "b"]));
        assert!(v.validate_create("com.test.Note", &data).is_ok());

        let mut bad = HashMap::new();
        bad.insert("title".into(), serde_json::json!("T"));
        bad.insert("tags".into(), serde_json::json!([1, 2])); // wrong type
        assert!(matches!(
            v.validate_create("com.test.Note", &bad),
            Err(WriteValidationError::TypeMismatch { .. })
        ));
    }

    #[test]
    fn test_null_allowed_for_optional() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::json!("T"));
        data.insert("body".into(), serde_json::Value::Null);
        assert!(v.validate_create("com.test.Note", &data).is_ok());
    }

    #[test]
    fn test_null_rejected_for_required() {
        let reg = registry_with_schema();
        let v = FieldValidator::new(&reg);
        let mut data = HashMap::new();
        data.insert("title".into(), serde_json::Value::Null);
        assert!(matches!(
            v.validate_create("com.test.Note", &data),
            Err(WriteValidationError::TypeMismatch { .. })
        ));
    }
}
