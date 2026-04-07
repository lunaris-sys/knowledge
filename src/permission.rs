/// Permission Profile parser for Knowledge Graph access.
///
/// Reads `~/.config/permissions/{app_id}.toml` and converts the `[graph]`
/// section into token scopes. No `[graph]` section means no graph access.
///
/// See `docs/architecture/CAPABILITY-TOKENS.md` Section 6.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;
use thiserror::Error;

use crate::token::{EntityScope, InstanceScope, RelationScope};

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors that can occur when loading a permission profile.
#[derive(Debug, Error)]
pub enum PermissionError {
    #[error("home directory not found")]
    NoHomeDir,
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse error: {0}")]
    Parse(String),
}

// ---------------------------------------------------------------------------
// TOML deserialization types
// ---------------------------------------------------------------------------

/// Top-level permission profile. Only the `[graph]` section is relevant here;
/// `[filesystem]`, `[network]`, `[devices]` are consumed by the sandbox layer.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PermissionProfile {
    #[serde(default)]
    pub graph: Option<GraphPermissions>,
}

/// The `[graph]` section of a permission profile.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GraphPermissions {
    #[serde(default)]
    pub read: Vec<String>,
    #[serde(default)]
    pub write: Vec<String>,
    #[serde(default)]
    pub relations: Vec<RelationPermission>,
    #[serde(default)]
    pub read_sensitive: Vec<String>,
    #[serde(default)]
    pub instance_scope: InstanceScopeConfig,
}

/// A relation permission entry from TOML.
#[derive(Debug, Clone, Deserialize)]
pub struct RelationPermission {
    pub from: String,
    pub to: String,
    #[serde(rename = "type")]
    pub relation_type: String,
}

/// TOML-level instance scope (lowercase string).
#[derive(Debug, Clone, Default, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum InstanceScopeConfig {
    #[default]
    Own,
    All,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl PermissionProfile {
    /// Load a permission profile from `~/.config/permissions/{app_id}.toml`.
    ///
    /// Returns a default (no graph access) if the file does not exist.
    pub fn load(app_id: &str) -> Result<Self, PermissionError> {
        let path = Self::profile_path(app_id)?;
        Self::load_from(&path)
    }

    /// Load from an explicit path (for testing).
    pub fn load_from(path: &Path) -> Result<Self, PermissionError> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let content = std::fs::read_to_string(path)?;
        toml::from_str(&content).map_err(|e| PermissionError::Parse(e.to_string()))
    }

    /// Resolve the profile path for an app.
    fn profile_path(app_id: &str) -> Result<PathBuf, PermissionError> {
        let home = dirs::home_dir().ok_or(PermissionError::NoHomeDir)?;
        Ok(home
            .join(".config")
            .join("permissions")
            .join(format!("{app_id}.toml")))
    }

    /// Get the modification time of the profile file for cache staleness.
    pub fn profile_mtime(app_id: &str) -> Result<std::time::SystemTime, PermissionError> {
        let path = Self::profile_path(app_id)?;
        let meta = std::fs::metadata(&path)?;
        meta.modified().map_err(|e| PermissionError::Io(e))
    }

    /// Whether this profile grants any graph access.
    pub fn has_graph_access(&self) -> bool {
        self.graph.is_some()
    }

    /// Convert read entries to token read scopes.
    ///
    /// Parsing rules:
    /// - `"system.File.path"` -> EntityScope { entity_type: "system.File", fields: Some(["path"]) }
    /// - `"system.Session"` -> EntityScope { entity_type: "system.Session", fields: None }
    /// - `"com.anki.*"` -> EntityScope { entity_type: "com.anki.*", fields: None }
    ///
    /// Multiple field entries for the same type are merged into one scope.
    pub fn to_read_scopes(&self) -> Vec<EntityScope> {
        let entries = match &self.graph {
            Some(g) => &g.read,
            None => return vec![],
        };
        parse_scope_entries(entries)
    }

    /// Convert write entries to token write scopes.
    pub fn to_write_scopes(&self) -> Vec<EntityScope> {
        let entries = match &self.graph {
            Some(g) => &g.write,
            None => return vec![],
        };
        parse_scope_entries(entries)
    }

    /// Convert relation entries to token relation scopes.
    pub fn to_relation_scopes(&self) -> Vec<RelationScope> {
        match &self.graph {
            Some(g) => g
                .relations
                .iter()
                .map(|r| RelationScope {
                    from: r.from.clone(),
                    to: r.to.clone(),
                    relation_type: r.relation_type.clone(),
                })
                .collect(),
            None => vec![],
        }
    }

    /// Convert instance scope config to token instance scope.
    pub fn to_instance_scope(&self) -> InstanceScope {
        match &self.graph {
            Some(g) => match g.instance_scope {
                InstanceScopeConfig::Own => InstanceScope::Own,
                InstanceScopeConfig::All => InstanceScope::All,
            },
            None => InstanceScope::Own,
        }
    }
}

// ---------------------------------------------------------------------------
// Scope entry parsing
// ---------------------------------------------------------------------------

/// Parse a list of scope strings into EntityScope structs.
///
/// An entry with 3 segments (`"system.File.path"`) is a field-level grant.
/// An entry with 2 segments (`"system.Session"`) grants all fields.
/// An entry ending in `.*` (`"com.anki.*"`) is a wildcard type grant.
fn parse_scope_entries(entries: &[String]) -> Vec<EntityScope> {
    // Group field-level entries by entity type.
    let mut type_fields: HashMap<String, Vec<String>> = HashMap::new();
    let mut full_types: Vec<String> = Vec::new();

    for entry in entries {
        if entry.ends_with(".*") {
            // Wildcard: "com.anki.*" -> full type grant
            full_types.push(entry.clone());
        } else {
            // Count dot-separated segments.
            let parts: Vec<&str> = entry.splitn(3, '.').collect();
            match parts.len() {
                3 => {
                    // "system.File.path" -> type = "system.File", field = "path"
                    let entity_type = format!("{}.{}", parts[0], parts[1]);
                    type_fields
                        .entry(entity_type)
                        .or_default()
                        .push(parts[2].to_string());
                }
                2 => {
                    // "system.Session" -> full entity grant
                    full_types.push(entry.clone());
                }
                _ => {
                    // Invalid entry, skip.
                }
            }
        }
    }

    let mut scopes = Vec::new();

    // Full-type entries (fields: None).
    for entity_type in full_types {
        // If we also have field-level entries for this type, the full grant wins.
        let base = if entity_type.ends_with(".*") {
            entity_type.clone()
        } else {
            entity_type.clone()
        };
        type_fields.remove(&base);
        scopes.push(EntityScope {
            entity_type,
            fields: None,
            exclude_fields: vec![],
        });
    }

    // Field-level entries.
    for (entity_type, fields) in type_fields {
        scopes.push(EntityScope {
            entity_type,
            fields: Some(fields),
            exclude_fields: vec![],
        });
    }

    // Sort for deterministic output.
    scopes.sort_by(|a, b| a.entity_type.cmp(&b.entity_type));
    scopes
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_temp_profile(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_parse_valid_profile() {
        let f = write_temp_profile(
            r#"
[graph]
read = ["system.File.path", "system.File.name", "system.Session", "com.anki.*"]
write = ["com.anki.*"]
relations = [
    { from = "com.anki.Card", to = "system.File", type = "REFERENCES" },
]
read_sensitive = []
instance_scope = "own"
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        assert!(profile.has_graph_access());

        let graph = profile.graph.as_ref().unwrap();
        assert_eq!(graph.read.len(), 4);
        assert_eq!(graph.write.len(), 1);
        assert_eq!(graph.relations.len(), 1);
        assert_eq!(graph.instance_scope, InstanceScopeConfig::Own);
    }

    #[test]
    fn test_parse_no_graph_section() {
        let f = write_temp_profile(
            r#"
[filesystem]
allow = ["~/Documents"]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        assert!(!profile.has_graph_access());
    }

    #[test]
    fn test_parse_empty_graph_section() {
        let f = write_temp_profile("[graph]\n");
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        assert!(profile.has_graph_access());
        let graph = profile.graph.as_ref().unwrap();
        assert!(graph.read.is_empty());
        assert!(graph.write.is_empty());
    }

    #[test]
    fn test_to_read_scopes_field_parsing() {
        let f = write_temp_profile(
            r#"
[graph]
read = ["system.File.path"]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        let scopes = profile.to_read_scopes();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].entity_type, "system.File");
        assert_eq!(scopes[0].fields, Some(vec!["path".to_string()]));
    }

    #[test]
    fn test_to_read_scopes_full_entity() {
        let f = write_temp_profile(
            r#"
[graph]
read = ["system.Session"]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        let scopes = profile.to_read_scopes();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].entity_type, "system.Session");
        assert!(scopes[0].fields.is_none());
    }

    #[test]
    fn test_to_read_scopes_wildcard() {
        let f = write_temp_profile(
            r#"
[graph]
read = ["com.anki.*"]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        let scopes = profile.to_read_scopes();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].entity_type, "com.anki.*");
        assert!(scopes[0].fields.is_none());
    }

    #[test]
    fn test_to_read_scopes_merge_fields() {
        let f = write_temp_profile(
            r#"
[graph]
read = ["system.File.path", "system.File.name", "system.File.modified_at"]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        let scopes = profile.to_read_scopes();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].entity_type, "system.File");
        let fields = scopes[0].fields.as_ref().unwrap();
        assert_eq!(fields.len(), 3);
        assert!(fields.contains(&"path".to_string()));
        assert!(fields.contains(&"name".to_string()));
        assert!(fields.contains(&"modified_at".to_string()));
    }

    #[test]
    fn test_to_read_scopes_full_overrides_fields() {
        // If both "system.File" and "system.File.path" are listed,
        // the full grant wins (fields: None).
        let f = write_temp_profile(
            r#"
[graph]
read = ["system.File.path", "system.File"]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        let scopes = profile.to_read_scopes();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].entity_type, "system.File");
        assert!(scopes[0].fields.is_none(), "full entity grant should override field-level");
    }

    #[test]
    fn test_to_write_scopes() {
        let f = write_temp_profile(
            r#"
[graph]
write = ["com.anki.*"]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        let scopes = profile.to_write_scopes();
        assert_eq!(scopes.len(), 1);
        assert_eq!(scopes[0].entity_type, "com.anki.*");
    }

    #[test]
    fn test_to_relation_scopes() {
        let f = write_temp_profile(
            r#"
[graph]
relations = [
    { from = "com.anki.Card", to = "system.File", type = "REFERENCES" },
    { from = "com.anki.Card", to = "shared.Person", type = "MENTIONS" },
]
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        let scopes = profile.to_relation_scopes();
        assert_eq!(scopes.len(), 2);
        assert_eq!(scopes[0].from, "com.anki.Card");
        assert_eq!(scopes[0].to, "system.File");
        assert_eq!(scopes[0].relation_type, "REFERENCES");
        assert_eq!(scopes[1].relation_type, "MENTIONS");
    }

    #[test]
    fn test_instance_scope_own() {
        let f = write_temp_profile(
            r#"
[graph]
instance_scope = "own"
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        assert_eq!(profile.to_instance_scope(), InstanceScope::Own);
    }

    #[test]
    fn test_instance_scope_all() {
        let f = write_temp_profile(
            r#"
[graph]
instance_scope = "all"
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        assert_eq!(profile.to_instance_scope(), InstanceScope::All);
    }

    #[test]
    fn test_instance_scope_default() {
        let f = write_temp_profile("[graph]\n");
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        assert_eq!(profile.to_instance_scope(), InstanceScope::Own);
    }

    #[test]
    fn test_missing_profile() {
        let profile = PermissionProfile::load_from(Path::new("/tmp/nonexistent-profile-xyz.toml")).unwrap();
        assert!(!profile.has_graph_access());
        assert!(profile.to_read_scopes().is_empty());
    }

    #[test]
    fn test_complex_profile() {
        let f = write_temp_profile(
            r#"
[filesystem]
allow = ["~/Documents/Anki"]

[network]
allow = ["sync.ankiweb.net"]

[graph]
read = [
    "system.File.path",
    "system.File.name",
    "system.Session",
    "shared.Person.name",
    "com.anki.*",
]
write = ["com.anki.*"]
relations = [
    { from = "com.anki.Card", to = "system.File", type = "REFERENCES" },
]
read_sensitive = []
instance_scope = "own"
"#,
        );
        let profile = PermissionProfile::load_from(f.path()).unwrap();
        assert!(profile.has_graph_access());

        let read = profile.to_read_scopes();
        // com.anki.*, shared.Person (field: name), system.File (fields: path, name), system.Session
        assert_eq!(read.len(), 4);

        let write = profile.to_write_scopes();
        assert_eq!(write.len(), 1);
        assert_eq!(write[0].entity_type, "com.anki.*");

        let relations = profile.to_relation_scopes();
        assert_eq!(relations.len(), 1);

        assert_eq!(profile.to_instance_scope(), InstanceScope::Own);
    }
}
