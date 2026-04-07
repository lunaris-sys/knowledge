/// Schema validation for entity definitions.
///
/// Ensures schemas follow naming conventions, don't use reserved names,
/// respect namespace boundaries, and have valid relation targets.

use super::SchemaFile;

/// Reserved field names (injected by the Graph Daemon at runtime).
const RESERVED_FIELDS: &[&str] = &[
    "id",
    "_version",
    "_owner",
    "_created_at",
    "_modified_at",
    "_deleted",
    "_deleted_at",
    "_pending_delete",
];

/// Reserved entity names (system and shared entities, plus internal names).
const RESERVED_ENTITY_NAMES: &[&str] = &[
    // System
    "File", "App", "Session", "Project", "UserAction", "Notification",
    // Shared
    "Person", "Organization", "Event", "Location", "Tag",
    // Internal
    "Entity", "Node", "Edge", "Relation", "Schema",
];

/// Validation errors for schema definitions.
#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("reserved field name: {0}")]
    ReservedField(String),
    #[error("reserved entity name: {0}")]
    ReservedEntityName(String),
    #[error("system namespace is immutable: {0}")]
    SystemSchemaImmutable(String),
    #[error("shared namespace restricted to first-party apps: {0}")]
    SharedSchemaRestricted(String),
    #[error("invalid relation target '{target}' in entity '{entity}'")]
    InvalidRelationTarget { entity: String, target: String },
    #[error("entity name must be PascalCase: {0}")]
    InvalidEntityName(String),
    #[error("field name must be snake_case: {0}")]
    InvalidFieldName(String),
}

/// Validates schema files against naming and namespace rules.
pub struct SchemaValidator {
    /// Entity types already registered in the system (for relation target checks).
    existing_types: Vec<String>,
    /// Apps allowed to register `shared.*` entity types.
    first_party_apps: Vec<String>,
}

impl SchemaValidator {
    /// Create a new validator with known existing types and first-party apps.
    pub fn new(existing_types: Vec<String>, first_party_apps: Vec<String>) -> Self {
        Self {
            existing_types,
            first_party_apps,
        }
    }

    /// Validate a schema file.
    pub fn validate(&self, schema: &SchemaFile) -> Result<(), ValidationError> {
        let ns = &schema.meta.namespace;

        // System namespace is immutable.
        if ns == "system" {
            return Err(ValidationError::SystemSchemaImmutable(ns.clone()));
        }

        // Shared namespace restricted to first-party apps.
        if ns == "shared" && !self.first_party_apps.contains(ns) {
            if !self.first_party_apps.iter().any(|a| a == ns) {
                return Err(ValidationError::SharedSchemaRestricted(ns.clone()));
            }
        }

        for (entity_name, entity_def) in &schema.entities {
            // Check reserved entity names.
            if RESERVED_ENTITY_NAMES.contains(&entity_name.as_str()) {
                return Err(ValidationError::ReservedEntityName(entity_name.clone()));
            }

            // Entity names must be PascalCase (start with uppercase, no underscores).
            if !is_pascal_case(entity_name) {
                return Err(ValidationError::InvalidEntityName(entity_name.clone()));
            }

            // Check reserved field names.
            for field_name in entity_def.fields.keys() {
                if RESERVED_FIELDS.contains(&field_name.as_str()) {
                    return Err(ValidationError::ReservedField(field_name.clone()));
                }
                if !is_snake_case(field_name) {
                    return Err(ValidationError::InvalidFieldName(field_name.clone()));
                }
            }
        }

        // Validate relation targets exist (in this schema or globally).
        for (rel_name, rel_def) in &schema.relations {
            if !self.type_exists(&rel_def.from, schema) {
                return Err(ValidationError::InvalidRelationTarget {
                    entity: rel_name.clone(),
                    target: rel_def.from.clone(),
                });
            }
            if !self.type_exists(&rel_def.to, schema) {
                return Err(ValidationError::InvalidRelationTarget {
                    entity: rel_name.clone(),
                    target: rel_def.to.clone(),
                });
            }
        }

        Ok(())
    }

    /// Check if an entity type exists globally or in the current schema.
    fn type_exists(&self, type_name: &str, schema: &SchemaFile) -> bool {
        // Local name (within this schema's namespace).
        if schema.entities.contains_key(type_name) {
            return true;
        }

        // Fully qualified name in existing types (system.*, shared.*, or
        // previously registered app types like com.anki.Card).
        if self.existing_types.contains(&type_name.to_string()) {
            return true;
        }

        // Try as fully qualified with this namespace.
        let full = format!("{}.{}", schema.meta.namespace, type_name);
        self.existing_types.contains(&full)
    }

    /// Add a newly registered type to the known types.
    pub fn register_type(&mut self, full_type: String) {
        if !self.existing_types.contains(&full_type) {
            self.existing_types.push(full_type);
        }
    }
}

/// Check if a name follows PascalCase (starts uppercase, no underscores).
fn is_pascal_case(s: &str) -> bool {
    !s.is_empty()
        && s.chars().next().unwrap().is_uppercase()
        && !s.contains('_')
        && s.chars().all(|c| c.is_alphanumeric())
}

/// Check if a name follows snake_case (all lowercase, underscores allowed).
fn is_snake_case(s: &str) -> bool {
    !s.is_empty()
        && s.chars()
            .all(|c| c.is_lowercase() || c.is_ascii_digit() || c == '_')
        && !s.starts_with('_')
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::parser::SchemaFile;

    fn validator() -> SchemaValidator {
        SchemaValidator::new(
            vec![
                "system.File".into(),
                "system.App".into(),
                "system.Session".into(),
                "shared.Person".into(),
            ],
            vec!["org.lunaris.contacts".into()],
        )
    }

    fn valid_schema() -> SchemaFile {
        SchemaFile::parse(
            r#"
[meta]
namespace = "com.test"

[entities.Note]
[entities.Note.fields.title]
type = "string"
required = true

[entities.Note.fields.body]
type = "text"

[relations.REFERENCES]
from = "Note"
to = "system.File"
"#,
        )
        .unwrap()
    }

    #[test]
    fn test_valid_schema_passes() {
        assert!(validator().validate(&valid_schema()).is_ok());
    }

    #[test]
    fn test_reserved_field_rejected() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "com.test"

[entities.Item]
[entities.Item.fields.id]
type = "string"
"#,
        )
        .unwrap();
        let result = validator().validate(&schema);
        assert!(matches!(result, Err(ValidationError::ReservedField(f)) if f == "id"));
    }

    #[test]
    fn test_reserved_entity_name_rejected() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "com.test"

[entities.File]
[entities.File.fields.name]
type = "string"
"#,
        )
        .unwrap();
        assert!(matches!(
            validator().validate(&schema),
            Err(ValidationError::ReservedEntityName(n)) if n == "File"
        ));
    }

    #[test]
    fn test_system_namespace_immutable() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "system"

[entities.Custom]
[entities.Custom.fields.x]
type = "string"
"#,
        )
        .unwrap();
        assert!(matches!(
            validator().validate(&schema),
            Err(ValidationError::SystemSchemaImmutable(_))
        ));
    }

    #[test]
    fn test_shared_namespace_restricted() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "shared"

[entities.Custom]
[entities.Custom.fields.x]
type = "string"
"#,
        )
        .unwrap();
        assert!(matches!(
            validator().validate(&schema),
            Err(ValidationError::SharedSchemaRestricted(_))
        ));
    }

    #[test]
    fn test_invalid_relation_target() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "com.test"

[entities.Item]
[entities.Item.fields.name]
type = "string"

[relations.LINKS_TO]
from = "Item"
to = "com.other.Thing"
"#,
        )
        .unwrap();
        assert!(matches!(
            validator().validate(&schema),
            Err(ValidationError::InvalidRelationTarget { .. })
        ));
    }

    #[test]
    fn test_local_relation_target_ok() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "com.test"

[entities.A]
[entities.A.fields.x]
type = "string"

[entities.B]
[entities.B.fields.y]
type = "string"

[relations.LINKS]
from = "A"
to = "B"
"#,
        )
        .unwrap();
        assert!(validator().validate(&schema).is_ok());
    }

    #[test]
    fn test_system_relation_target_ok() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "com.test"

[entities.Note]
[entities.Note.fields.x]
type = "string"

[relations.REFS]
from = "Note"
to = "system.File"
"#,
        )
        .unwrap();
        assert!(validator().validate(&schema).is_ok());
    }

    #[test]
    fn test_pascal_case_check() {
        assert!(is_pascal_case("Card"));
        assert!(is_pascal_case("FlashCard"));
        assert!(!is_pascal_case("card"));
        assert!(!is_pascal_case("flash_card"));
        assert!(!is_pascal_case(""));
    }

    #[test]
    fn test_snake_case_check() {
        assert!(is_snake_case("name"));
        assert!(is_snake_case("due_date"));
        assert!(is_snake_case("ease_factor2"));
        assert!(!is_snake_case("Name"));
        assert!(!is_snake_case("dueDate"));
        assert!(!is_snake_case("_private"));
        assert!(!is_snake_case(""));
    }

    #[test]
    fn test_invalid_entity_name() {
        let schema = SchemaFile::parse(
            r#"
[meta]
namespace = "com.test"

[entities.my_item]
[entities.my_item.fields.name]
type = "string"
"#,
        )
        .unwrap();
        assert!(matches!(
            validator().validate(&schema),
            Err(ValidationError::InvalidEntityName(n)) if n == "my_item"
        ));
    }
}
