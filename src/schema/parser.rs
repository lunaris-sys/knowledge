/// Schema file parser for `entities.toml` files.
///
/// Parses the TOML schema format defined in ENTITY-SCHEMA-SYSTEM.md
/// into structured Rust types.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use super::SchemaError;

/// A complete schema file for one application.
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaFile {
    /// Schema metadata.
    pub meta: SchemaMeta,
    /// Entity type definitions keyed by local name (e.g. "Card", "Deck").
    #[serde(default)]
    pub entities: HashMap<String, EntityDefinition>,
    /// Relation definitions keyed by relation name (e.g. "SIMILAR_TO").
    #[serde(default)]
    pub relations: HashMap<String, RelationDefinition>,
}

/// Schema metadata section.
#[derive(Debug, Clone, Deserialize)]
pub struct SchemaMeta {
    /// Schema format version (currently 1).
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,
    /// Application namespace (reverse-domain, e.g. "com.anki").
    pub namespace: String,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
}

fn default_schema_version() -> u32 {
    1
}

/// Definition of a single entity type.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct EntityDefinition {
    /// Entity schema version for migrations.
    #[serde(default = "default_entity_version")]
    pub version: u32,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Lucide icon name for UI.
    #[serde(default)]
    pub icon: String,
    /// Field definitions.
    #[serde(default)]
    pub fields: HashMap<String, FieldDefinition>,
    /// Lifecycle configuration.
    #[serde(default)]
    pub lifecycle: LifecycleConfig,
}

fn default_entity_version() -> u32 {
    1
}

/// Definition of a single field on an entity.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct FieldDefinition {
    /// Field type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Whether the field is required on entity creation.
    #[serde(default)]
    pub required: bool,
    /// Default value (must match field type).
    pub default: Option<toml::Value>,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Whether the field is indexed for fast queries.
    #[serde(default)]
    pub indexed: bool,
    /// Whether the field has a uniqueness constraint.
    #[serde(default)]
    pub unique: bool,
    /// Whether the field is immutable after creation.
    #[serde(default)]
    pub immutable: bool,
    /// Whether the field contains sensitive data.
    #[serde(default)]
    pub sensitive: bool,
    /// On-delete behavior for reference fields.
    #[serde(default)]
    pub on_delete: Option<OnDelete>,
}

/// Supported field types.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    #[default]
    String,
    Text,
    Int,
    Float,
    Bool,
    Datetime,
    Date,
    Duration,
    Url,
    Email,
    Path,
    Json,
    Markdown,
    Color,
    Bytes,
    Uuid,
    #[serde(rename = "string[]")]
    StringList,
    #[serde(rename = "int[]")]
    IntList,
    #[serde(rename = "float[]")]
    FloatList,
    #[serde(rename = "bool[]")]
    BoolList,
    /// Reference to another entity, e.g. "ref:Deck" or "ref:system.File".
    #[serde(untagged)]
    Reference(String),
}

/// On-delete behavior for reference fields.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OnDelete {
    /// Set reference to null (default).
    #[default]
    Nullify,
    /// Delete this entity too.
    Cascade,
    /// Prevent deletion of the referenced entity.
    Restrict,
    /// Do nothing (orphan reference).
    NoAction,
}

/// Definition of a relation (typed edge) between entities.
#[derive(Debug, Clone, Deserialize)]
pub struct RelationDefinition {
    /// Source entity type (local name or fully qualified).
    pub from: String,
    /// Target entity type.
    pub to: String,
    /// Cardinality constraint.
    #[serde(default)]
    pub cardinality: Cardinality,
    /// Whether the relation is symmetric (A->B implies B->A).
    #[serde(default)]
    pub symmetric: bool,
    /// Human-readable description.
    #[serde(default)]
    pub description: String,
    /// Properties on the relation edge.
    #[serde(default)]
    pub properties: HashMap<String, FieldDefinition>,
}

/// Cardinality constraint for relations.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Cardinality {
    OneToOne,
    OneToMany,
    ManyToOne,
    #[default]
    ManyToMany,
}

/// Entity lifecycle configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct LifecycleConfig {
    /// Days in trash before permanent deletion.
    #[serde(default = "default_trash_retention")]
    pub trash_retention_days: u32,
    /// Whether to keep history (versioning).
    #[serde(default)]
    pub history_enabled: bool,
}

fn default_trash_retention() -> u32 {
    30
}

impl Default for LifecycleConfig {
    fn default() -> Self {
        Self {
            trash_retention_days: default_trash_retention(),
            history_enabled: false,
        }
    }
}

impl SchemaFile {
    /// Load and parse a schema file from disk.
    pub fn load(path: &Path) -> Result<Self, SchemaError> {
        let content = std::fs::read_to_string(path)?;
        Self::parse(&content)
    }

    /// Parse a schema from a TOML string.
    pub fn parse(content: &str) -> Result<Self, SchemaError> {
        toml::from_str(content).map_err(|e| SchemaError::Parse(e.to_string()))
    }

    /// Get the fully qualified type name for an entity.
    pub fn full_type(&self, entity_name: &str) -> String {
        format!("{}.{}", self.meta.namespace, entity_name)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_SCHEMA: &str = r#"
[meta]
schema_version = 1
namespace = "com.anki"
description = "Anki flashcard app"

[entities.Card]
version = 1
description = "A flashcard"
icon = "book-open"

[entities.Card.fields.front]
type = "string"
required = true
description = "Question side"

[entities.Card.fields.back]
type = "string"
required = true

[entities.Card.fields.tags]
type = "string[]"

[entities.Card.fields.ease_factor]
type = "float"
default = 2.5

[entities.Card.fields.due_date]
type = "datetime"
indexed = true

[entities.Card.fields.is_suspended]
type = "bool"
default = false

[entities.Card.lifecycle]
trash_retention_days = 60
history_enabled = true

[entities.Deck]
version = 1
description = "A collection of cards"

[entities.Deck.fields.name]
type = "string"
required = true
unique = true

[entities.Deck.fields.description]
type = "text"

[relations.SIMILAR_TO]
from = "Card"
to = "Card"
cardinality = "many-to-many"
symmetric = true
description = "Cards covering similar topics"

[relations.BELONGS_TO]
from = "Card"
to = "Deck"
cardinality = "many-to-one"
"#;

    #[test]
    fn test_parse_valid_schema() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        assert_eq!(schema.meta.namespace, "com.anki");
        assert_eq!(schema.meta.schema_version, 1);
        assert_eq!(schema.entities.len(), 2);
        assert!(schema.entities.contains_key("Card"));
        assert!(schema.entities.contains_key("Deck"));
    }

    #[test]
    fn test_entity_fields() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        let card = &schema.entities["Card"];
        assert_eq!(card.fields.len(), 6);
        assert!(card.fields["front"].required);
        assert_eq!(card.fields["front"].field_type, FieldType::String);
        assert!(!card.fields["ease_factor"].required);
        assert_eq!(card.fields["ease_factor"].field_type, FieldType::Float);
        assert!(card.fields["due_date"].indexed);
    }

    #[test]
    fn test_field_types() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        let card = &schema.entities["Card"];
        assert_eq!(card.fields["front"].field_type, FieldType::String);
        assert_eq!(card.fields["ease_factor"].field_type, FieldType::Float);
        assert_eq!(card.fields["due_date"].field_type, FieldType::Datetime);
        assert_eq!(card.fields["is_suspended"].field_type, FieldType::Bool);
        assert_eq!(card.fields["tags"].field_type, FieldType::StringList);
    }

    #[test]
    fn test_lifecycle_config() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        let card = &schema.entities["Card"];
        assert_eq!(card.lifecycle.trash_retention_days, 60);
        assert!(card.lifecycle.history_enabled);

        let deck = &schema.entities["Deck"];
        assert_eq!(deck.lifecycle.trash_retention_days, 30); // default
        assert!(!deck.lifecycle.history_enabled);
    }

    #[test]
    fn test_relations() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        assert_eq!(schema.relations.len(), 2);

        let similar = &schema.relations["SIMILAR_TO"];
        assert_eq!(similar.from, "Card");
        assert_eq!(similar.to, "Card");
        assert!(similar.symmetric);
        assert_eq!(similar.cardinality, Cardinality::ManyToMany);

        let belongs = &schema.relations["BELONGS_TO"];
        assert_eq!(belongs.from, "Card");
        assert_eq!(belongs.to, "Deck");
        assert!(!belongs.symmetric);
        assert_eq!(belongs.cardinality, Cardinality::ManyToOne);
    }

    #[test]
    fn test_full_type() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        assert_eq!(schema.full_type("Card"), "com.anki.Card");
        assert_eq!(schema.full_type("Deck"), "com.anki.Deck");
    }

    #[test]
    fn test_unique_field() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        assert!(schema.entities["Deck"].fields["name"].unique);
        assert!(!schema.entities["Card"].fields["front"].unique);
    }

    #[test]
    fn test_default_values() {
        let schema = SchemaFile::parse(SAMPLE_SCHEMA).unwrap();
        let ease = &schema.entities["Card"].fields["ease_factor"];
        assert_eq!(ease.default.as_ref().unwrap().as_float(), Some(2.5));

        let suspended = &schema.entities["Card"].fields["is_suspended"];
        assert_eq!(
            suspended.default.as_ref().unwrap().as_bool(),
            Some(false)
        );
    }

    #[test]
    fn test_minimal_schema() {
        let minimal = r#"
[meta]
namespace = "com.test"

[entities.Item]
[entities.Item.fields.name]
type = "string"
"#;
        let schema = SchemaFile::parse(minimal).unwrap();
        assert_eq!(schema.meta.namespace, "com.test");
        assert_eq!(schema.meta.schema_version, 1); // default
        assert_eq!(schema.entities.len(), 1);
    }
}
