//! App identity resolution.
//!
//! Sprint C consolidated the canonical implementation into
//! `sdk/permissions::identity`. This module re-exports the
//! types so existing `knowledge::auth::Authenticator` callsites
//! keep working unchanged.
//!
//! See `docs/architecture/AUTH-CANONICAL.md` section 4.

pub use lunaris_permissions::identity::{
    app_id_from_pid, path_to_app_id, pid_start_time, process_alive, IdentityError,
};
