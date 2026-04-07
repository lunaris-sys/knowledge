/// Graph data import from JSON-LD ZIP with conflict resolution.

use std::io::Read;
use std::path::Path;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// How to handle entities that already exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ConflictMode {
    /// Keep existing entity, ignore imported.
    #[default]
    Skip,
    /// Overwrite existing with imported.
    Replace,
    /// Field-level merge (imported values win for non-null fields).
    Merge,
}

/// Options for import.
#[derive(Debug, Clone, Default)]
pub struct ImportOptions {
    pub conflict_mode: ConflictMode,
    pub validate_schemas: bool,
    pub dry_run: bool,
}

/// Statistics from an import run.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ImportResult {
    pub entities_imported: usize,
    pub entities_skipped: usize,
    pub entities_replaced: usize,
    pub entities_merged: usize,
    pub relations_imported: usize,
    pub errors: Vec<String>,
}

/// Import errors.
#[derive(Debug, Error)]
pub enum ImportError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("ZIP: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid manifest")]
    InvalidManifest,
    #[error("schema mismatch: {0}")]
    SchemaMismatch(String),
}

/// Outcome of a single entity conflict resolution.
#[derive(Debug)]
pub enum ConflictResolution {
    KeepExisting,
    UseImported,
    UseMerged(serde_json::Value),
}

/// Imports graph data from a ZIP file.
pub struct Importer {
    options: ImportOptions,
}

impl Importer {
    pub fn new(options: ImportOptions) -> Self {
        Self { options }
    }

    /// Read and validate the manifest from an export ZIP.
    pub fn read_manifest(
        input_path: &Path,
    ) -> Result<super::export::ExportManifest, ImportError> {
        let file = std::fs::File::open(input_path)?;
        let mut archive = zip::ZipArchive::new(file)?;
        let mut manifest_file = archive.by_name("manifest.json")?;
        let mut content = String::new();
        manifest_file.read_to_string(&mut content)?;
        let manifest = serde_json::from_str(&content)?;
        Ok(manifest)
    }

    /// Resolve a conflict between an existing and imported entity.
    pub fn resolve_conflict(
        &self,
        existing: &serde_json::Value,
        imported: &serde_json::Value,
    ) -> ConflictResolution {
        match self.options.conflict_mode {
            ConflictMode::Skip => ConflictResolution::KeepExisting,
            ConflictMode::Replace => ConflictResolution::UseImported,
            ConflictMode::Merge => {
                ConflictResolution::UseMerged(merge_entities(existing, imported))
            }
        }
    }
}

/// Field-level merge: imported non-null values win, reserved fields kept.
pub fn merge_entities(
    existing: &serde_json::Value,
    imported: &serde_json::Value,
) -> serde_json::Value {
    let mut result = existing.clone();

    if let (Some(base), Some(incoming)) = (result.as_object_mut(), imported.as_object()) {
        for (key, value) in incoming {
            if key.starts_with('_') || key == "id" {
                continue;
            }
            if !value.is_null() {
                base.insert(key.clone(), value.clone());
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_conflict_skip() {
        let imp = Importer::new(ImportOptions {
            conflict_mode: ConflictMode::Skip,
            ..Default::default()
        });
        match imp.resolve_conflict(&json!({"a": 1}), &json!({"a": 2})) {
            ConflictResolution::KeepExisting => {}
            _ => panic!("expected KeepExisting"),
        }
    }

    #[test]
    fn test_conflict_replace() {
        let imp = Importer::new(ImportOptions {
            conflict_mode: ConflictMode::Replace,
            ..Default::default()
        });
        match imp.resolve_conflict(&json!({"a": 1}), &json!({"a": 2})) {
            ConflictResolution::UseImported => {}
            _ => panic!("expected UseImported"),
        }
    }

    #[test]
    fn test_conflict_merge() {
        let imp = Importer::new(ImportOptions {
            conflict_mode: ConflictMode::Merge,
            ..Default::default()
        });
        let existing = json!({"id": "1", "a": 1, "b": 2, "_version": 3});
        let imported = json!({"id": "1", "a": 99, "c": 3, "_version": 1});

        match imp.resolve_conflict(&existing, &imported) {
            ConflictResolution::UseMerged(merged) => {
                assert_eq!(merged["a"], 99); // imported wins
                assert_eq!(merged["b"], 2); // kept from existing
                assert_eq!(merged["c"], 3); // added from imported
                assert_eq!(merged["_version"], 3); // reserved kept
                assert_eq!(merged["id"], "1"); // reserved kept
            }
            _ => panic!("expected UseMerged"),
        }
    }

    #[test]
    fn test_merge_null_skipped() {
        let existing = json!({"name": "Alice", "email": "alice@test.com"});
        let imported = json!({"name": "Bob", "email": null});
        let merged = merge_entities(&existing, &imported);
        assert_eq!(merged["name"], "Bob"); // non-null imported wins
        assert_eq!(merged["email"], "alice@test.com"); // null skipped
    }

    #[test]
    fn test_merge_reserved_fields_preserved() {
        let existing = json!({"_owner": "com.a", "_version": 5, "title": "old"});
        let imported = json!({"_owner": "com.b", "_version": 1, "title": "new"});
        let merged = merge_entities(&existing, &imported);
        assert_eq!(merged["_owner"], "com.a");
        assert_eq!(merged["_version"], 5);
        assert_eq!(merged["title"], "new");
    }

    #[test]
    fn test_import_result_default() {
        let r = ImportResult::default();
        assert_eq!(r.entities_imported, 0);
        assert!(r.errors.is_empty());
    }

    #[test]
    fn test_read_manifest_from_export() {
        // Create a minimal export first, then read it.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.zip");

        let exporter =
            super::super::export::Exporter::new(super::super::export::ExportOptions::default());
        exporter
            .export_to_file(&path, &std::collections::HashMap::new())
            .unwrap();

        let manifest = Importer::read_manifest(&path).unwrap();
        assert_eq!(manifest.version, "1.0");
    }
}
