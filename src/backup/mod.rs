/// Export/Import and backup for the Knowledge Graph.
///
/// - Export: JSON-LD + ZIP with manifest
/// - Import: Conflict resolution (skip/replace/merge)
/// - Snapshots: Snapper (Btrfs) integration
/// - Integrity: SQLite + graph consistency checks
///
/// See `docs/architecture/GRAPH-OPERATIONS.md` Sections 5-6.

mod export;
mod import;
mod integrity;
mod snapshot;

pub use export::*;
pub use import::*;
pub use integrity::*;
pub use snapshot::*;
