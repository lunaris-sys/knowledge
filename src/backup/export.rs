/// Graph data export to JSON-LD + ZIP.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Manifest stored inside the export ZIP.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExportManifest {
    pub version: String,
    pub created_at: DateTime<Utc>,
    pub app_id: Option<String>,
    pub entity_counts: HashMap<String, usize>,
    pub relation_counts: HashMap<String, usize>,
    pub schemas_included: Vec<String>,
}

/// Options controlling what gets exported.
#[derive(Debug, Clone, Default)]
pub struct ExportOptions {
    /// Export a single app's data (None = full export).
    pub app_id: Option<String>,
    /// Include soft-deleted entities.
    pub include_deleted: bool,
    /// Include schema TOML files.
    pub include_schemas: bool,
    /// Filter to specific entity types.
    pub entity_types: Option<Vec<String>>,
}

/// Export errors.
#[derive(Debug, Error)]
pub enum ExportError {
    #[error("IO: {0}")]
    Io(#[from] std::io::Error),
    #[error("ZIP: {0}")]
    Zip(#[from] zip::result::ZipError),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
}

/// Exports graph data to a ZIP file containing JSON-LD and a manifest.
pub struct Exporter {
    options: ExportOptions,
}

impl Exporter {
    pub fn new(options: ExportOptions) -> Self {
        Self { options }
    }

    /// Write an export ZIP to disk.
    ///
    /// Structure:
    /// ```text
    /// manifest.json
    /// entities/{type}.jsonld
    /// relations/{type}.jsonld
    /// schemas/{app_id}.toml  (if include_schemas)
    /// ```
    pub fn export_to_file(
        &self,
        output_path: &Path,
        entities_by_type: &HashMap<String, Vec<serde_json::Value>>,
    ) -> Result<ExportManifest, ExportError> {
        let file = std::fs::File::create(output_path)?;
        let mut zip = zip::ZipWriter::new(file);
        let opts = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);

        let mut manifest = ExportManifest {
            version: "1.0".into(),
            created_at: Utc::now(),
            app_id: self.options.app_id.clone(),
            entity_counts: HashMap::new(),
            relation_counts: HashMap::new(),
            schemas_included: Vec::new(),
        };

        for (entity_type, entities) in entities_by_type {
            if entities.is_empty() {
                continue;
            }
            let jsonld = Self::entities_to_jsonld(entity_type, entities);
            let filename = format!("entities/{entity_type}.jsonld");
            zip.start_file(&filename, opts)?;
            zip.write_all(jsonld.as_bytes())?;
            manifest
                .entity_counts
                .insert(entity_type.clone(), entities.len());
        }

        // Write manifest last.
        zip.start_file("manifest.json", opts)?;
        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        zip.write_all(manifest_json.as_bytes())?;

        zip.finish()?;
        Ok(manifest)
    }

    /// Serialize entities as JSON-LD.
    pub fn entities_to_jsonld(
        entity_type: &str,
        entities: &[serde_json::Value],
    ) -> String {
        let doc = serde_json::json!({
            "@context": {
                "@vocab": "https://lunaris.dev/schema/",
                "id": "@id",
                "type": "@type",
            },
            "@type": entity_type,
            "@graph": entities,
        });
        serde_json::to_string_pretty(&doc).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Read;

    #[test]
    fn test_export_manifest_serialize() {
        let m = ExportManifest {
            version: "1.0".into(),
            created_at: Utc::now(),
            app_id: Some("com.test".into()),
            entity_counts: [("com.test.Note".into(), 5)].into(),
            relation_counts: HashMap::new(),
            schemas_included: vec!["com.test".into()],
        };
        let json = serde_json::to_string(&m).unwrap();
        let parsed: ExportManifest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, "1.0");
        assert_eq!(parsed.entity_counts["com.test.Note"], 5);
    }

    #[test]
    fn test_jsonld_format() {
        let entities = vec![json!({"id": "1", "title": "Hello"})];
        let jsonld = Exporter::entities_to_jsonld("com.test.Note", &entities);
        assert!(jsonld.contains("@context"));
        assert!(jsonld.contains("@graph"));
        assert!(jsonld.contains("com.test.Note"));
    }

    #[test]
    fn test_export_to_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("export.zip");

        let mut entities = HashMap::new();
        entities.insert(
            "com.test.Note".into(),
            vec![json!({"id": "1", "title": "A"}), json!({"id": "2", "title": "B"})],
        );

        let exporter = Exporter::new(ExportOptions::default());
        let manifest = exporter.export_to_file(&path, &entities).unwrap();

        assert_eq!(manifest.entity_counts["com.test.Note"], 2);

        // Verify ZIP structure.
        let file = std::fs::File::open(&path).unwrap();
        let mut archive = zip::ZipArchive::new(file).unwrap();
        assert!(archive.by_name("manifest.json").is_ok());
        assert!(archive.by_name("entities/com.test.Note.jsonld").is_ok());
    }

    #[test]
    fn test_export_empty() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("empty.zip");

        let exporter = Exporter::new(ExportOptions::default());
        let manifest = exporter.export_to_file(&path, &HashMap::new()).unwrap();

        assert!(manifest.entity_counts.is_empty());
    }
}
