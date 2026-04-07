/// Authentication handler for the Graph Daemon.
///
/// Issues capability tokens to connecting applications based on their
/// permission profiles, and verifies tokens on every subsequent request.
///
/// See `docs/architecture/CAPABILITY-TOKENS.md` Sections 7-8.

use crate::identity::{app_id_from_pid, process_alive, IdentityError};
use crate::permission::{PermissionError, PermissionProfile};
use crate::token::{CapabilityToken, TokenSigner};
use crate::token_cache::TokenCache;

use thiserror::Error;

/// Errors from authentication operations.
#[derive(Debug, Error)]
pub enum AuthError {
    #[error("graph access not granted for {0}")]
    GraphAccessNotGranted(String),
    #[error("token signature invalid")]
    TokenInvalid,
    #[error("token expired")]
    TokenExpired,
    #[error("process {0} no longer alive")]
    ProcessDead(u32),
    #[error("identity: {0}")]
    Identity(#[from] IdentityError),
    #[error("permission: {0}")]
    Permission(#[from] PermissionError),
}

/// Manages token issuing, caching, and verification.
pub struct Authenticator {
    signer: TokenSigner,
    cache: TokenCache,
}

impl Authenticator {
    /// Create a new authenticator with a fresh HMAC key.
    pub fn new() -> Self {
        Self {
            signer: TokenSigner::new(),
            cache: TokenCache::new(),
        }
    }

    /// Issue a token for a connecting process.
    ///
    /// 1. Resolves app_id from PID via `/proc/{pid}/exe`
    /// 2. Loads permission profile
    /// 3. Checks `[graph]` access
    /// 4. Builds and signs token from profile scopes
    /// 5. Caches token with profile mtime
    pub fn issue_token_for_pid(&mut self, pid: u32) -> Result<CapabilityToken, AuthError> {
        let app_id = app_id_from_pid(pid)?;
        self.issue_token_for_app(&app_id, pid)
    }

    /// Issue a token for a known app_id and PID (skips identity resolution).
    /// Useful for testing and for cases where app_id is already known.
    pub fn issue_token_for_app(
        &mut self,
        app_id: &str,
        pid: u32,
    ) -> Result<CapabilityToken, AuthError> {
        let profile = PermissionProfile::load(app_id)?;
        self.issue_token_from_profile(app_id, pid, &profile)
    }

    /// Issue a token from an already-loaded profile.
    pub fn issue_token_from_profile(
        &mut self,
        app_id: &str,
        pid: u32,
        profile: &PermissionProfile,
    ) -> Result<CapabilityToken, AuthError> {
        if !profile.has_graph_access() {
            return Err(AuthError::GraphAccessNotGranted(app_id.to_string()));
        }

        let mut token = CapabilityToken::new(
            app_id.to_string(),
            pid,
            profile.to_read_scopes(),
            profile.to_write_scopes(),
            profile.to_relation_scopes(),
            profile.to_instance_scope(),
        );

        self.signer.sign(&mut token);

        let mtime = PermissionProfile::profile_mtime(app_id).ok();
        self.cache
            .insert(app_id.to_string(), token.clone(), mtime);

        Ok(token)
    }

    /// Verify a token presented with a request.
    ///
    /// Checks: HMAC signature, expiration, process liveness.
    pub fn verify_token(&self, token: &CapabilityToken) -> Result<(), AuthError> {
        if !self.signer.verify(token) {
            return Err(AuthError::TokenInvalid);
        }
        if token.is_expired() {
            return Err(AuthError::TokenExpired);
        }
        if !process_alive(token.pid) {
            return Err(AuthError::ProcessDead(token.pid));
        }
        Ok(())
    }

    /// Invalidate a cached token for an app (on `permission.changed` event).
    pub fn invalidate(&mut self, app_id: &str) {
        self.cache.invalidate(app_id);
    }

    /// Invalidate all cached tokens (key rotation, daemon restart).
    pub fn invalidate_all(&mut self) {
        self.cache.invalidate_all();
    }

    /// Get a reference to the signer (for testing).
    #[cfg(test)]
    pub fn signer(&self) -> &TokenSigner {
        &self.signer
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{EntityScope, InstanceScope, RelationScope};
    use std::io::Write;
    use tempfile::TempDir;

    fn load_profile(content: &str) -> PermissionProfile {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(content.as_bytes()).unwrap();
        f.flush().unwrap();
        PermissionProfile::load_from(f.path()).unwrap()
    }

    #[test]
    fn test_issue_token_success() {
        let profile = load_profile(
            r#"
[graph]
read = ["system.File", "com.test.*"]
write = ["com.test.*"]
relations = [
    { from = "com.test.Note", to = "system.File", type = "REFERENCES" },
]
instance_scope = "own"
"#,
        );

        let mut auth = Authenticator::new();
        let token = auth
            .issue_token_from_profile("com.test", std::process::id(), &profile)
            .unwrap();

        assert_eq!(token.app_id, "com.test");
        assert!(token.can_read("system.File"));
        assert!(token.can_read("com.test.Note"));
        assert!(token.can_write("com.test.Note"));
        assert!(!token.can_write("system.File"));
        assert!(token.can_create_relation("com.test.Note", "system.File", "REFERENCES"));
        assert_eq!(token.instance_scope, InstanceScope::Own);
        assert!(auth.signer().verify(&token));
    }

    #[test]
    fn test_no_graph_permission() {
        let profile = load_profile(
            r#"
[filesystem]
allow = ["~/Documents"]
"#,
        );

        let mut auth = Authenticator::new();
        let result = auth.issue_token_from_profile("com.nograph", 1234, &profile);
        assert!(result.is_err());
        match result.unwrap_err() {
            AuthError::GraphAccessNotGranted(id) => assert_eq!(id, "com.nograph"),
            other => panic!("expected GraphAccessNotGranted, got: {other}"),
        }
    }

    #[test]
    fn test_missing_profile_no_access() {
        let profile = PermissionProfile::default();
        let mut auth = Authenticator::new();
        let result = auth.issue_token_from_profile("com.nonexistent", 1234, &profile);
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_token_valid() {
        let profile = load_profile("[graph]\nread = [\"system.File\"]\n");

        let mut auth = Authenticator::new();
        let token = auth
            .issue_token_from_profile("com.verify", std::process::id(), &profile)
            .unwrap();

        assert!(auth.verify_token(&token).is_ok());
    }

    #[test]
    fn test_verify_token_tampered() {
        let profile = load_profile("[graph]\nread = [\"system.File\"]\n");

        let mut auth = Authenticator::new();
        let mut token = auth
            .issue_token_from_profile("com.tamper", std::process::id(), &profile)
            .unwrap();

        token.app_id = "com.evil".to_string();
        assert!(matches!(
            auth.verify_token(&token),
            Err(AuthError::TokenInvalid)
        ));
    }

    #[test]
    fn test_verify_token_dead_process() {
        let profile = load_profile("[graph]\nread = [\"system.File\"]\n");

        let mut auth = Authenticator::new();
        let mut token = auth
            .issue_token_from_profile("com.dead", std::process::id(), &profile)
            .unwrap();

        token.pid = 999_999_999;
        auth.signer.sign(&mut token);

        assert!(matches!(
            auth.verify_token(&token),
            Err(AuthError::ProcessDead(999_999_999))
        ));
    }

    #[test]
    fn test_invalidate_cache() {
        let profile = load_profile("[graph]\nread = [\"system.File\"]\n");

        let mut auth = Authenticator::new();
        let _ = auth
            .issue_token_from_profile("com.cache", std::process::id(), &profile)
            .unwrap();

        assert!(auth.cache.get("com.cache").is_some());
        auth.invalidate("com.cache");
        assert!(auth.cache.get("com.cache").is_none());
    }
}
