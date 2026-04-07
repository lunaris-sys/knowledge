/// Shared entities: cross-app entity types in the `shared.*` namespace.
///
/// First-party apps create them, third-party apps read (with permission)
/// and create relations to them. Includes duplicate detection and merge
/// suggestions.
///
/// See `docs/architecture/SHARED-ENTITIES.md`.

mod access;
mod duplicate;
mod schemas;
mod suggestion;

pub use access::*;
pub use duplicate::*;
pub use schemas::*;
pub use suggestion::*;
