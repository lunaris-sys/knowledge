/// Entity lifecycle: soft delete, trash, restore, cleanup, staged uninstall.
///
/// See `docs/architecture/ENTITY-SCHEMA-SYSTEM.md` Section 6 and
/// `docs/architecture/GRAPH-OPERATIONS.md` Sections 3-4.

mod cleanup;
mod restore;
mod staged_uninstall;
mod trash;

pub use cleanup::*;
pub use restore::*;
pub use staged_uninstall::*;
pub use trash::*;
