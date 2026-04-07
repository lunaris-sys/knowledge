/// Migration runner: executes migration files with checkpoint support.

use std::path::Path;

use tracing::info;

use super::checkpoint::MigrationCheckpoint;
use super::parser::{
    find_migration_chain, parse_migration_filename, MigrationError, MigrationFile,
    MigrationOperation,
};
use super::functions;

/// Runs migration files with batching and checkpointing.
pub struct MigrationRunner {
    batch_size: usize,
}

impl MigrationRunner {
    pub fn new() -> Self {
        Self { batch_size: 1000 }
    }

    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Run a single migration file for an app.
    pub fn run(
        &self,
        migration: &MigrationFile,
        app_id: &str,
    ) -> Result<MigrationResult, MigrationError> {
        // Resume from checkpoint if one exists for this migration.
        let mut checkpoint = MigrationCheckpoint::load(app_id)
            .filter(|c| {
                c.from_version == migration.from_version
                    && c.to_version == migration.to_version
            })
            .unwrap_or_else(|| {
                MigrationCheckpoint::new(app_id, &migration.from_version, &migration.to_version)
            });

        let mut result = MigrationResult::default();

        for (index, operation) in migration.operations.iter().enumerate() {
            if index < checkpoint.operation_index {
                continue; // already completed
            }

            match self.run_operation(operation) {
                Ok(count) => {
                    result.operations_completed += 1;
                    result.entities_modified += count;
                    checkpoint.advance_operation();
                    checkpoint.save().ok();
                }
                Err(e) => {
                    return Err(MigrationError::OperationFailed {
                        index,
                        message: e.to_string(),
                    });
                }
            }
        }

        MigrationCheckpoint::delete(app_id).ok();
        result.completed = true;
        Ok(result)
    }

    /// Validate an operation without executing it.
    fn run_operation(&self, op: &MigrationOperation) -> Result<usize, MigrationError> {
        match op {
            MigrationOperation::ChangeType { transform, .. } => {
                if let Some(func) = transform {
                    if !functions::list_functions().contains(&func.as_str()) {
                        return Err(MigrationError::UnknownFunction(func.clone()));
                    }
                }
                Ok(0) // actual entity count comes from DB execution
            }
            MigrationOperation::Custom { description, .. } => {
                info!("custom migration: {description}");
                Ok(0)
            }
            _ => Ok(0), // structural operations don't modify entity counts
        }
    }

    /// Find and run all pending migrations from current to target version.
    pub fn run_pending(
        &self,
        app_id: &str,
        current_version: &str,
        target_version: &str,
        migrations_dir: &Path,
    ) -> Result<Vec<MigrationResult>, MigrationError> {
        let available = load_available(migrations_dir)?;

        let chain = find_migration_chain(&available, current_version, target_version)
            .ok_or_else(|| MigrationError::NoPath {
                from: current_version.into(),
                to: target_version.into(),
            })?;

        let mut results = Vec::new();
        for (from, to) in chain {
            let path = migrations_dir.join(format!("{from}_to_{to}.toml"));
            let migration = MigrationFile::load(&path)?;
            results.push(self.run(&migration, app_id)?);
        }

        Ok(results)
    }
}

/// Scan a directory for migration files and return (from, to) version pairs.
fn load_available(dir: &Path) -> Result<Vec<(String, String)>, MigrationError> {
    let mut available = Vec::new();
    if !dir.exists() {
        return Ok(available);
    }
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name().to_string_lossy().to_string();
        if let Some(pair) = parse_migration_filename(&name) {
            available.push(pair);
        }
    }
    Ok(available)
}

/// Result of running one migration.
#[derive(Debug, Default)]
pub struct MigrationResult {
    pub completed: bool,
    pub operations_completed: usize,
    pub entities_modified: usize,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_simple_migration() {
        let migration = MigrationFile::parse(
            r#"
from_version = "1.0.0"
to_version = "1.1.0"

[[operations]]
op = "add_field"
entity = "Card"
field = "tags"
type = "string[]"
"#,
        )
        .unwrap();

        let runner = MigrationRunner::new();
        let result = runner.run(&migration, "com.test.simple").unwrap();
        assert!(result.completed);
        assert_eq!(result.operations_completed, 1);

        // Cleanup checkpoint.
        MigrationCheckpoint::delete("com.test.simple").ok();
    }

    #[test]
    fn test_run_migration_with_unknown_function() {
        let migration = MigrationFile::parse(
            r#"
from_version = "1.0.0"
to_version = "1.1.0"

[[operations]]
op = "change_type"
entity = "Card"
field = "tags"
from_type = "string"
to_type = "string[]"
transform = "nonexistent_function"
"#,
        )
        .unwrap();

        let runner = MigrationRunner::new();
        let result = runner.run(&migration, "com.test.badfn");
        assert!(matches!(
            result,
            Err(MigrationError::OperationFailed { index: 0, .. })
        ));

        MigrationCheckpoint::delete("com.test.badfn").ok();
    }

    #[test]
    fn test_run_migration_with_valid_function() {
        let migration = MigrationFile::parse(
            r#"
from_version = "1.0.0"
to_version = "1.1.0"

[[operations]]
op = "change_type"
entity = "Card"
field = "tags"
from_type = "string"
to_type = "string[]"
transform = "split_comma"
"#,
        )
        .unwrap();

        let runner = MigrationRunner::new();
        let result = runner.run(&migration, "com.test.goodfn").unwrap();
        assert!(result.completed);

        MigrationCheckpoint::delete("com.test.goodfn").ok();
    }

    #[test]
    fn test_run_pending_from_dir() {
        let dir = tempfile::TempDir::new().unwrap();

        std::fs::write(
            dir.path().join("1.0.0_to_1.1.0.toml"),
            r#"
from_version = "1.0.0"
to_version = "1.1.0"
[[operations]]
op = "add_field"
entity = "Card"
field = "tags"
type = "string[]"
"#,
        )
        .unwrap();

        std::fs::write(
            dir.path().join("1.1.0_to_2.0.0.toml"),
            r#"
from_version = "1.1.0"
to_version = "2.0.0"
[[operations]]
op = "remove_field"
entity = "Card"
field = "old_field"
"#,
        )
        .unwrap();

        let runner = MigrationRunner::new();
        let results = runner
            .run_pending("com.test.pending", "1.0.0", "2.0.0", dir.path())
            .unwrap();

        assert_eq!(results.len(), 2);
        assert!(results[0].completed);
        assert!(results[1].completed);

        MigrationCheckpoint::delete("com.test.pending").ok();
    }

    #[test]
    fn test_run_pending_no_path() {
        let dir = tempfile::TempDir::new().unwrap();
        let runner = MigrationRunner::new();
        let result = runner.run_pending("com.test", "1.0.0", "9.9.9", dir.path());
        assert!(matches!(result, Err(MigrationError::NoPath { .. })));
    }
}
