/// Token cache with profile-mtime-based staleness detection.
///
/// Caches issued capability tokens per app_id. Entries are invalidated
/// when a `permission.changed` event is received or when the permission
/// profile file has been modified on disk.

use std::collections::HashMap;
use std::time::SystemTime;

use crate::token::CapabilityToken;

/// Per-app token cache.
pub struct TokenCache {
    tokens: HashMap<String, CachedToken>,
}

struct CachedToken {
    token: CapabilityToken,
    profile_mtime: Option<SystemTime>,
}

impl TokenCache {
    /// Create an empty cache.
    pub fn new() -> Self {
        Self {
            tokens: HashMap::new(),
        }
    }

    /// Look up a cached token by app_id.
    pub fn get(&self, app_id: &str) -> Option<&CapabilityToken> {
        self.tokens.get(app_id).map(|c| &c.token)
    }

    /// Insert or replace a token in the cache.
    pub fn insert(
        &mut self,
        app_id: String,
        token: CapabilityToken,
        profile_mtime: Option<SystemTime>,
    ) {
        self.tokens.insert(
            app_id,
            CachedToken {
                token,
                profile_mtime,
            },
        );
    }

    /// Remove a specific app's token (e.g. on `permission.changed`).
    pub fn invalidate(&mut self, app_id: &str) {
        self.tokens.remove(app_id);
    }

    /// Remove all cached tokens (e.g. on daemon restart / key rotation).
    pub fn invalidate_all(&mut self) {
        self.tokens.clear();
    }

    /// Check whether the cached entry is stale (profile file modified since
    /// the token was issued).
    pub fn is_stale(&self, app_id: &str, current_mtime: Option<SystemTime>) -> bool {
        match self.tokens.get(app_id) {
            None => true,
            Some(cached) => cached.profile_mtime != current_mtime,
        }
    }

    /// Number of cached tokens.
    pub fn len(&self) -> usize {
        self.tokens.len()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::token::{CapabilityToken, EntityScope, InstanceScope};
    use std::time::Duration;

    fn dummy_token(app_id: &str) -> CapabilityToken {
        CapabilityToken::new(
            app_id.to_string(),
            1234,
            vec![EntityScope {
                entity_type: "system.File".into(),
                fields: None,
                exclude_fields: vec![],
            }],
            vec![],
            vec![],
            InstanceScope::Own,
        )
    }

    #[test]
    fn test_cache_insert_get() {
        let mut cache = TokenCache::new();
        let token = dummy_token("com.test");
        cache.insert("com.test".into(), token, None);

        assert!(cache.get("com.test").is_some());
        assert_eq!(cache.get("com.test").unwrap().app_id, "com.test");
        assert!(cache.get("com.other").is_none());
    }

    #[test]
    fn test_cache_invalidate() {
        let mut cache = TokenCache::new();
        cache.insert("com.a".into(), dummy_token("com.a"), None);
        cache.insert("com.b".into(), dummy_token("com.b"), None);
        assert_eq!(cache.len(), 2);

        cache.invalidate("com.a");
        assert!(cache.get("com.a").is_none());
        assert!(cache.get("com.b").is_some());
        assert_eq!(cache.len(), 1);
    }

    #[test]
    fn test_cache_invalidate_all() {
        let mut cache = TokenCache::new();
        cache.insert("com.a".into(), dummy_token("com.a"), None);
        cache.insert("com.b".into(), dummy_token("com.b"), None);

        cache.invalidate_all();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_cache_is_stale_missing() {
        let cache = TokenCache::new();
        assert!(cache.is_stale("com.test", None));
    }

    #[test]
    fn test_cache_is_stale_same_mtime() {
        let mut cache = TokenCache::new();
        let mtime = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1000));
        cache.insert("com.test".into(), dummy_token("com.test"), mtime);
        assert!(!cache.is_stale("com.test", mtime));
    }

    #[test]
    fn test_cache_is_stale_different_mtime() {
        let mut cache = TokenCache::new();
        let old = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(1000));
        let new = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(2000));
        cache.insert("com.test".into(), dummy_token("com.test"), old);
        assert!(cache.is_stale("com.test", new));
    }

    #[test]
    fn test_cache_is_stale_none_vs_some() {
        let mut cache = TokenCache::new();
        cache.insert("com.test".into(), dummy_token("com.test"), None);
        let now = Some(SystemTime::now());
        assert!(cache.is_stale("com.test", now));
    }
}
