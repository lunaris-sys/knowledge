/// Schema Registry: loads, stores, and queries entity schemas.
///
/// Schemas are loaded from `/var/lib/lunaris/schemas/` at startup and
/// reloaded when `schema.registered` events arrive from the Event Bus.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use tracing::{info, warn};

use super::{SchemaError, SchemaFile, SchemaValidator};

/// Default schema directory.
const DEFAULT_SCHEMA_DIR: &str = "/var/lib/lunaris/schemas";

/// System entity types compiled into the Graph Daemon.
fn system_entity_types() -> Vec<String> {
    vec![
        "system.File".into(),
        "system.App".into(),
        "system.Session".into(),
        "system.Event".into(),
        "system.UserAction".into(),
        "system.Summary".into(),
        "system.Project".into(),
    ]
}

/// Central registry of all loaded entity schemas.
pub struct SchemaRegistry {
    /// Loaded schemas keyed by namespace (app_id).
    schemas: HashMap<String, SchemaFile>,
    /// Validator instance (tracks known types for relation checks).
    validator: SchemaValidator,
    /// Path to the schema directory.
    schema_dir: PathBuf,
}

impl SchemaRegistry {
    /// Create a new registry with the given first-party apps and schema dir.
    pub fn new(first_party_apps: Vec<String>) -> Self {
        Self::with_dir(first_party_apps, PathBuf::from(DEFAULT_SCHEMA_DIR))
    }

    /// Create a registry with a custom schema directory (for testing).
    pub fn with_dir(first_party_apps: Vec<String>, schema_dir: PathBuf) -> Self {
        let validator = SchemaValidator::new(system_entity_types(), first_party_apps);
        Self {
            schemas: HashMap::new(),
            validator,
            schema_dir,
        }
    }

    /// Load all `.toml` schema files from the schema directory.
    pub fn load_all(&mut self) -> Result<(), SchemaError> {
        if !self.schema_dir.exists() {
            return Ok(());
        }

        for entry in std::fs::read_dir(&self.schema_dir)? {
            let path = entry?.path();
            if path.extension().map(|e| e == "toml").unwrap_or(false) {
                match self.load_schema(&path) {
                    Ok(ns) => info!("loaded schema: {ns} from {}", path.display()),
                    Err(e) => warn!("failed to load schema {}: {e}", path.display()),
                }
            }
        }

        Ok(())
    }

    /// Load and validate a single schema file. Returns the namespace on success.
    pub fn load_schema(&mut self, path: &Path) -> Result<String, SchemaError> {
        let schema = SchemaFile::load(path)?;
        self.validator.validate(&schema)?;

        let ns = schema.meta.namespace.clone();

        // Register all entity types for future relation target checks.
        for entity_name in schema.entities.keys() {
            self.validator.register_type(schema.full_type(entity_name));
        }

        self.schemas.insert(ns.clone(), schema);
        Ok(ns)
    }

    /// Load a schema from a TOML string (for testing).
    pub fn load_from_str(&mut self, content: &str) -> Result<String, SchemaError> {
        let schema = SchemaFile::parse(content)?;
        self.validator.validate(&schema)?;

        let ns = schema.meta.namespace.clone();
        for entity_name in schema.entities.keys() {
            self.validator.register_type(schema.full_type(entity_name));
        }
        self.schemas.insert(ns.clone(), schema);
        Ok(ns)
    }

    /// Get the schema for a namespace.
    pub fn get(&self, namespace: &str) -> Option<&SchemaFile> {
        self.schemas.get(namespace)
    }

    /// Get an entity definition by fully qualified type name.
    ///
    /// Parses "com.anki.Card" into namespace="com.anki", entity="Card".
    pub fn get_entity(
        &self,
        full_type: &str,
    ) -> Option<&super::parser::EntityDefinition> {
        let (ns, name) = parse_full_type(full_type)?;
        self.schemas.get(ns)?.entities.get(name)
    }

    /// Check if an entity type exists (system or registered).
    pub fn entity_exists(&self, full_type: &str) -> bool {
        if system_entity_types().contains(&full_type.to_string()) {
            return true;
        }
        self.get_entity(full_type).is_some()
    }

    /// List all registered entity types (system + app).
    pub fn all_entity_types(&self) -> Vec<String> {
        let mut types = system_entity_types();
        for schema in self.schemas.values() {
            for entity_name in schema.entities.keys() {
                types.push(schema.full_type(entity_name));
            }
        }
        types.sort();
        types
    }

    /// Handle a `schema.registered` event: load the schema from the directory.
    pub fn on_schema_registered(&mut self, app_id: &str) -> Result<(), SchemaError> {
        let path = self.schema_dir.join(format!("{app_id}.toml"));
        self.load_schema(&path)?;
        Ok(())
    }

    /// Handle a `schema.removed` event: unload the schema.
    pub fn on_schema_removed(&mut self, app_id: &str) {
        self.schemas.remove(app_id);
    }

    /// Number of loaded schemas (not counting system types).
    pub fn len(&self) -> usize {
        self.schemas.len()
    }
}

/// Parse "com.anki.Card" into ("com.anki", "Card").
fn parse_full_type(full_type: &str) -> Option<(&str, &str)> {
    let last_dot = full_type.rfind('.')?;
    if last_dot == 0 || last_dot == full_type.len() - 1 {
        return None;
    }
    Some((&full_type[..last_dot], &full_type[last_dot + 1..]))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn sample_schema() -> &'static str {
        r#"
[meta]
namespace = "com.anki"
description = "Anki flashcards"

[entities.Card]
[entities.Card.fields.front]
type = "string"
required = true

[entities.Card.fields.back]
type = "string"

[entities.Deck]
[entities.Deck.fields.name]
type = "string"

[relations.BELONGS_TO]
from = "Card"
to = "Deck"
"#
    }

    fn setup_schema_dir(schemas: &[(&str, &str)]) -> TempDir {
        let dir = TempDir::new().unwrap();
        for (filename, content) in schemas {
            let path = dir.path().join(filename);
            let mut f = std::fs::File::create(path).unwrap();
            f.write_all(content.as_bytes()).unwrap();
        }
        dir
    }

    #[test]
    fn test_load_from_str() {
        let mut reg = SchemaRegistry::new(vec![]);
        let ns = reg.load_from_str(sample_schema()).unwrap();
        assert_eq!(ns, "com.anki");
        assert_eq!(reg.len(), 1);
    }

    #[test]
    fn test_get_entity() {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(sample_schema()).unwrap();

        assert!(reg.get_entity("com.anki.Card").is_some());
        assert!(reg.get_entity("com.anki.Deck").is_some());
        assert!(reg.get_entity("com.anki.Missing").is_none());
        assert!(reg.get_entity("com.other.Card").is_none());
    }

    #[test]
    fn test_entity_exists() {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(sample_schema()).unwrap();

        assert!(reg.entity_exists("system.File"));
        assert!(reg.entity_exists("system.App"));
        assert!(reg.entity_exists("com.anki.Card"));
        assert!(!reg.entity_exists("com.other.Thing"));
    }

    #[test]
    fn test_all_entity_types() {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(sample_schema()).unwrap();

        let types = reg.all_entity_types();
        assert!(types.contains(&"system.File".to_string()));
        assert!(types.contains(&"com.anki.Card".to_string()));
        assert!(types.contains(&"com.anki.Deck".to_string()));
    }

    #[test]
    fn test_load_all_from_dir() {
        let dir = setup_schema_dir(&[("com.anki.toml", sample_schema())]);
        let mut reg = SchemaRegistry::with_dir(vec![], dir.path().to_path_buf());
        reg.load_all().unwrap();

        assert_eq!(reg.len(), 1);
        assert!(reg.entity_exists("com.anki.Card"));
    }

    #[test]
    fn test_load_all_empty_dir() {
        let dir = TempDir::new().unwrap();
        let mut reg = SchemaRegistry::with_dir(vec![], dir.path().to_path_buf());
        reg.load_all().unwrap();
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_load_all_missing_dir() {
        let mut reg =
            SchemaRegistry::with_dir(vec![], PathBuf::from("/tmp/nonexistent-schema-dir-xyz"));
        assert!(reg.load_all().is_ok());
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_on_schema_registered() {
        let dir = setup_schema_dir(&[("com.anki.toml", sample_schema())]);
        let mut reg = SchemaRegistry::with_dir(vec![], dir.path().to_path_buf());

        reg.on_schema_registered("com.anki").unwrap();
        assert!(reg.entity_exists("com.anki.Card"));
    }

    #[test]
    fn test_on_schema_removed() {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(sample_schema()).unwrap();
        assert!(reg.entity_exists("com.anki.Card"));

        reg.on_schema_removed("com.anki");
        assert!(!reg.entity_exists("com.anki.Card"));
        assert_eq!(reg.len(), 0);
    }

    #[test]
    fn test_parse_full_type() {
        assert_eq!(parse_full_type("com.anki.Card"), Some(("com.anki", "Card")));
        assert_eq!(
            parse_full_type("system.File"),
            Some(("system", "File"))
        );
        assert_eq!(parse_full_type("NoNamespace"), None);
    }

    #[test]
    fn test_multiple_schemas() {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(sample_schema()).unwrap();
        reg.load_from_str(
            r#"
[meta]
namespace = "com.notes"

[entities.Note]
[entities.Note.fields.title]
type = "string"
"#,
        )
        .unwrap();

        assert_eq!(reg.len(), 2);
        assert!(reg.entity_exists("com.anki.Card"));
        assert!(reg.entity_exists("com.notes.Note"));
    }

    #[test]
    fn test_cross_schema_relation_after_registration() {
        let mut reg = SchemaRegistry::new(vec![]);
        reg.load_from_str(sample_schema()).unwrap();

        // Second schema references com.anki.Card which was just registered.
        let result = reg.load_from_str(
            r#"
[meta]
namespace = "com.review"

[entities.Review]
[entities.Review.fields.score]
type = "int"

[relations.REVIEWS]
from = "Review"
to = "com.anki.Card"
"#,
        );
        assert!(result.is_ok());
    }
}
