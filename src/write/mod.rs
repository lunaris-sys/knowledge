/// Write operations for the Knowledge Graph (Create, Update, Delete).
///
/// All operations enforce token scopes, namespace isolation, and field
/// validation against the Schema Registry. Reserved fields (id, _version,
/// _owner, _created_at, _modified_at, _deleted) are auto-set.
///
/// See `docs/architecture/ENTITY-SCHEMA-SYSTEM.md` Section 4-5.

mod create;
mod delete;
mod update;
mod validation;

pub use create::*;
pub use delete::*;
pub use update::*;
pub use validation::*;
