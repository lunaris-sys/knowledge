/// Schema migrations for the Knowledge Graph.
///
/// Migrations are TOML files named `{from}_to_{to}.toml` that describe
/// operations to transform entity schemas between versions. The runner
/// executes migrations in order with checkpoint support for resumability.
///
/// See `docs/architecture/SCHEMA-MIGRATIONS.md`.

mod checkpoint;
mod functions;
mod parser;
mod runner;

pub use checkpoint::*;
pub use functions::{apply_transform, list_functions};
pub use parser::*;
pub use runner::*;
