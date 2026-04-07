/// Entity Schema System for the Knowledge Graph.
///
/// Apps define custom entity types via `entities.toml` in their packages.
/// The Install Daemon validates and writes schemas to `/var/lib/lunaris/schemas/`.
/// The Graph Daemon loads schemas at startup and on `schema.registered` events.
///
/// See `docs/architecture/ENTITY-SCHEMA-SYSTEM.md`.

mod parser;
mod registry;
mod validator;

pub use parser::*;
pub use registry::*;
pub use validator::*;

/// Errors from the schema system.
#[derive(Debug, thiserror::Error)]
pub enum SchemaError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("validation: {0}")]
    Validation(#[from] ValidationError),
}
