/// Migration file parser and version chain resolver.

use std::collections::{HashSet, VecDeque};
use std::path::Path;

use serde::Deserialize;
use thiserror::Error;

/// A complete migration file.
#[derive(Debug, Clone, Deserialize)]
pub struct MigrationFile {
    pub from_version: String,
    pub to_version: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub operations: Vec<MigrationOperation>,
}

/// A single migration operation.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum MigrationOperation {
    AddField {
        entity: String,
        field: String,
        #[serde(rename = "type")]
        field_type: String,
        #[serde(default)]
        default: Option<toml::Value>,
        #[serde(default)]
        populate_from: Option<String>,
    },
    RemoveField {
        entity: String,
        field: String,
    },
    RenameField {
        entity: String,
        from: String,
        to: String,
    },
    ChangeType {
        entity: String,
        field: String,
        from_type: String,
        to_type: String,
        #[serde(default)]
        transform: Option<String>,
    },
    AddIndex {
        entity: String,
        fields: Vec<String>,
        #[serde(default)]
        unique: bool,
    },
    RemoveIndex {
        entity: String,
        fields: Vec<String>,
    },
    AddRelation {
        entity: String,
        name: String,
        target: String,
        #[serde(default)]
        cardinality: Option<String>,
    },
    RemoveRelation {
        entity: String,
        name: String,
    },
    RenameEntity {
        from: String,
        to: String,
    },
    Custom {
        description: String,
        query: String,
    },
}

/// Migration errors.
#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse: {0}")]
    Parse(String),
    #[error("no migration path from {from} to {to}")]
    NoPath { from: String, to: String },
    #[error("unknown transform function: {0}")]
    UnknownFunction(String),
    #[error("operation {index} failed: {message}")]
    OperationFailed { index: usize, message: String },
}

impl MigrationFile {
    /// Load from a TOML file.
    pub fn load(path: &Path) -> Result<Self, MigrationError> {
        let content = std::fs::read_to_string(path)?;
        Self::parse(&content)
    }

    /// Parse from a TOML string.
    pub fn parse(content: &str) -> Result<Self, MigrationError> {
        toml::from_str(content).map_err(|e| MigrationError::Parse(e.to_string()))
    }
}

/// Extract (from_version, to_version) from a filename like `1.0.0_to_1.1.0.toml`.
pub fn parse_migration_filename(filename: &str) -> Option<(String, String)> {
    let name = filename.strip_suffix(".toml")?;
    let (from, to) = name.split_once("_to_")?;
    if from.is_empty() || to.is_empty() {
        return None;
    }
    Some((from.into(), to.into()))
}

/// Find the shortest migration chain from `current` to `target` using BFS.
pub fn find_migration_chain(
    available: &[(String, String)],
    current: &str,
    target: &str,
) -> Option<Vec<(String, String)>> {
    if current == target {
        return Some(vec![]);
    }

    let mut queue: VecDeque<(String, Vec<(String, String)>)> = VecDeque::new();
    let mut visited = HashSet::new();

    queue.push_back((current.into(), vec![]));
    visited.insert(current.to_string());

    while let Some((version, path)) = queue.pop_front() {
        for (from, to) in available {
            if from == &version && !visited.contains(to) {
                let mut new_path = path.clone();
                new_path.push((from.clone(), to.clone()));

                if to == target {
                    return Some(new_path);
                }

                visited.insert(to.clone());
                queue.push_back((to.clone(), new_path));
            }
        }
    }

    None
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_migration_file() {
        let content = r#"
from_version = "1.0.0"
to_version = "1.1.0"
description = "Add tags field"

[[operations]]
op = "add_field"
entity = "Card"
field = "tags"
type = "string[]"
default = ["untagged"]

[[operations]]
op = "add_index"
entity = "Card"
fields = ["tags"]
"#;
        let m = MigrationFile::parse(content).unwrap();
        assert_eq!(m.from_version, "1.0.0");
        assert_eq!(m.to_version, "1.1.0");
        assert_eq!(m.operations.len(), 2);
    }

    #[test]
    fn test_parse_all_operation_types() {
        let content = r#"
from_version = "1.0.0"
to_version = "2.0.0"

[[operations]]
op = "add_field"
entity = "Card"
field = "tags"
type = "string[]"

[[operations]]
op = "remove_field"
entity = "Card"
field = "old_field"

[[operations]]
op = "rename_field"
entity = "Card"
from = "front_text"
to = "front"

[[operations]]
op = "change_type"
entity = "Card"
field = "tags"
from_type = "string"
to_type = "string[]"
transform = "split_comma"

[[operations]]
op = "add_index"
entity = "Card"
fields = ["due_date"]

[[operations]]
op = "remove_index"
entity = "Card"
fields = ["old_index"]

[[operations]]
op = "add_relation"
entity = "Card"
name = "SIMILAR_TO"
target = "Card"

[[operations]]
op = "remove_relation"
entity = "Card"
name = "OLD_REL"

[[operations]]
op = "rename_entity"
from = "OldCard"
to = "Card"

[[operations]]
op = "custom"
description = "Backfill data"
query = "MATCH (n:Card) SET n.migrated = true"
"#;
        let m = MigrationFile::parse(content).unwrap();
        assert_eq!(m.operations.len(), 10);
    }

    #[test]
    fn test_parse_filename() {
        assert_eq!(
            parse_migration_filename("1.0.0_to_1.1.0.toml"),
            Some(("1.0.0".into(), "1.1.0".into()))
        );
        assert_eq!(
            parse_migration_filename("1.0.0_to_2.0.0.toml"),
            Some(("1.0.0".into(), "2.0.0".into()))
        );
        assert_eq!(parse_migration_filename("invalid.toml"), None);
        assert_eq!(parse_migration_filename("_to_.toml"), None);
        assert_eq!(parse_migration_filename("no_extension"), None);
    }

    #[test]
    fn test_find_chain_direct() {
        let available = vec![("1.0.0".into(), "1.1.0".into())];
        let chain = find_migration_chain(&available, "1.0.0", "1.1.0").unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(chain[0], ("1.0.0".into(), "1.1.0".into()));
    }

    #[test]
    fn test_find_chain_multi_step() {
        let available = vec![
            ("1.0.0".into(), "1.1.0".into()),
            ("1.1.0".into(), "1.2.0".into()),
            ("1.2.0".into(), "2.0.0".into()),
        ];
        let chain = find_migration_chain(&available, "1.0.0", "2.0.0").unwrap();
        assert_eq!(chain.len(), 3);
    }

    #[test]
    fn test_find_chain_no_path() {
        let available = vec![("1.0.0".into(), "1.1.0".into())];
        assert!(find_migration_chain(&available, "1.0.0", "3.0.0").is_none());
    }

    #[test]
    fn test_find_chain_same_version() {
        let available = vec![];
        let chain = find_migration_chain(&available, "1.0.0", "1.0.0").unwrap();
        assert!(chain.is_empty());
    }

    #[test]
    fn test_find_chain_shortest_path() {
        let available = vec![
            ("1.0.0".into(), "1.1.0".into()),
            ("1.1.0".into(), "2.0.0".into()),
            ("1.0.0".into(), "2.0.0".into()), // direct path
        ];
        let chain = find_migration_chain(&available, "1.0.0", "2.0.0").unwrap();
        assert_eq!(chain.len(), 1); // should pick direct
    }
}
