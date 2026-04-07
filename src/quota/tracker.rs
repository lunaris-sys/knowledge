/// Quota tracker: monitors per-app entity count and storage usage.

use std::collections::HashMap;

use thiserror::Error;

use super::QuotaConfig;

/// Quota enforcement errors.
#[derive(Debug, Error)]
pub enum QuotaError {
    #[error("entity quota exceeded: {current}/{max} entities")]
    EntityQuotaExceeded { current: usize, max: usize },
    #[error("storage quota exceeded: {current_mb:.1}/{max_mb:.1} MB")]
    StorageQuotaExceeded { current_mb: f64, max_mb: f64 },
    #[error("query result limit exceeded: max {max} results")]
    ResultLimitExceeded { max: usize },
}

/// Current resource usage for one app.
#[derive(Debug, Default, Clone)]
pub struct AppUsage {
    pub entity_count: usize,
    pub storage_bytes: usize,
}

/// Tracks resource usage per app and enforces quota limits.
pub struct QuotaTracker {
    config: QuotaConfig,
    usage: HashMap<String, AppUsage>,
}

impl QuotaTracker {
    pub fn new(config: QuotaConfig) -> Self {
        Self {
            config,
            usage: HashMap::new(),
        }
    }

    /// Check if app can create another entity.
    pub fn check_entity_quota(&self, app_id: &str) -> Result<(), QuotaError> {
        let quotas = self.config.quotas_for_app(app_id);
        let usage = self.usage.get(app_id).cloned().unwrap_or_default();

        if let Some(max) = quotas.max_entities {
            if usage.entity_count >= max {
                return Err(QuotaError::EntityQuotaExceeded {
                    current: usage.entity_count,
                    max,
                });
            }
        }
        Ok(())
    }

    /// Check if app can use additional storage.
    pub fn check_storage_quota(
        &self,
        app_id: &str,
        additional_bytes: usize,
    ) -> Result<(), QuotaError> {
        let quotas = self.config.quotas_for_app(app_id);
        let usage = self.usage.get(app_id).cloned().unwrap_or_default();

        if let Some(max) = quotas.max_storage_bytes {
            let new_total = usage.storage_bytes + additional_bytes;
            if new_total > max {
                return Err(QuotaError::StorageQuotaExceeded {
                    current_mb: new_total as f64 / (1024.0 * 1024.0),
                    max_mb: max as f64 / (1024.0 * 1024.0),
                });
            }
        }
        Ok(())
    }

    /// Get max query results allowed for app.
    pub fn max_query_results(&self, app_id: &str) -> usize {
        self.config.quotas_for_app(app_id).max_query_results
    }

    /// Record an entity creation.
    pub fn record_create(&mut self, app_id: &str, bytes: usize) {
        let usage = self.usage.entry(app_id.to_string()).or_default();
        usage.entity_count += 1;
        usage.storage_bytes += bytes;
    }

    /// Record an entity deletion.
    pub fn record_delete(&mut self, app_id: &str, bytes: usize) {
        if let Some(usage) = self.usage.get_mut(app_id) {
            usage.entity_count = usage.entity_count.saturating_sub(1);
            usage.storage_bytes = usage.storage_bytes.saturating_sub(bytes);
        }
    }

    /// Load usage from database (call at startup).
    pub fn load_usage(&mut self, app_usage: HashMap<String, AppUsage>) {
        self.usage = app_usage;
    }

    /// Get current usage for an app.
    pub fn get_usage(&self, app_id: &str) -> AppUsage {
        self.usage.get(app_id).cloned().unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> QuotaConfig {
        QuotaConfig {
            first_party_apps: vec![],
            overrides: HashMap::new(),
        }
    }

    fn config_with_limit(max_entities: usize, max_bytes: usize) -> QuotaConfig {
        let mut overrides = HashMap::new();
        overrides.insert(
            "com.test".into(),
            super::super::TierQuotas {
                max_entities: Some(max_entities),
                max_storage_bytes: Some(max_bytes),
                max_query_results: 100,
                queries_per_second: Some(10),
                writes_per_second: Some(5),
            },
        );
        QuotaConfig {
            first_party_apps: vec![],
            overrides,
        }
    }

    #[test]
    fn test_entity_quota_ok() {
        let c = config_with_limit(10, 1024 * 1024);
        let mut tracker = QuotaTracker::new(c);
        tracker.record_create("com.test", 100);
        assert!(tracker.check_entity_quota("com.test").is_ok());
    }

    #[test]
    fn test_entity_quota_exceeded() {
        let c = config_with_limit(2, 1024 * 1024);
        let mut tracker = QuotaTracker::new(c);
        tracker.record_create("com.test", 100);
        tracker.record_create("com.test", 100);
        assert!(matches!(
            tracker.check_entity_quota("com.test"),
            Err(QuotaError::EntityQuotaExceeded {
                current: 2,
                max: 2
            })
        ));
    }

    #[test]
    fn test_storage_quota_ok() {
        let c = config_with_limit(1000, 1024);
        let mut tracker = QuotaTracker::new(c);
        tracker.record_create("com.test", 500);
        assert!(tracker.check_storage_quota("com.test", 400).is_ok());
    }

    #[test]
    fn test_storage_quota_exceeded() {
        let c = config_with_limit(1000, 1024);
        let mut tracker = QuotaTracker::new(c);
        tracker.record_create("com.test", 800);
        assert!(matches!(
            tracker.check_storage_quota("com.test", 300),
            Err(QuotaError::StorageQuotaExceeded { .. })
        ));
    }

    #[test]
    fn test_record_create_delete() {
        let c = config_with_limit(10, 10_000);
        let mut tracker = QuotaTracker::new(c);

        tracker.record_create("com.test", 500);
        tracker.record_create("com.test", 300);
        let usage = tracker.get_usage("com.test");
        assert_eq!(usage.entity_count, 2);
        assert_eq!(usage.storage_bytes, 800);

        tracker.record_delete("com.test", 300);
        let usage = tracker.get_usage("com.test");
        assert_eq!(usage.entity_count, 1);
        assert_eq!(usage.storage_bytes, 500);
    }

    #[test]
    fn test_system_unlimited() {
        let tracker = QuotaTracker::new(config());
        // System apps have no entity limit.
        assert!(tracker.check_entity_quota("system").is_ok());
        assert!(tracker.check_storage_quota("system", usize::MAX / 2).is_ok());
    }

    #[test]
    fn test_max_query_results() {
        let c = config_with_limit(100, 1024);
        let tracker = QuotaTracker::new(c);
        assert_eq!(tracker.max_query_results("com.test"), 100);
        // Third-party default.
        assert_eq!(tracker.max_query_results("com.other"), 10_000);
    }

    #[test]
    fn test_delete_saturates_at_zero() {
        let c = config();
        let mut tracker = QuotaTracker::new(c);
        tracker.record_delete("com.test", 9999);
        let usage = tracker.get_usage("com.test");
        assert_eq!(usage.entity_count, 0);
        assert_eq!(usage.storage_bytes, 0);
    }
}
