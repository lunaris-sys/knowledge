/// Token bucket rate limiter for query and write operations.

use std::collections::HashMap;
use std::time::Instant;

use thiserror::Error;

use super::QuotaConfig;

/// Rate limit errors.
#[derive(Debug, Error)]
pub enum RateLimitError {
    #[error("query rate limit exceeded: max {max}/sec")]
    QueryRateExceeded { max: usize },
    #[error("write rate limit exceeded: max {max}/sec")]
    WriteRateExceeded { max: usize },
}

/// Token bucket algorithm for rate limiting.
///
/// Tokens refill continuously at `refill_rate` per second. Each operation
/// consumes one token. Burst capacity is 2x the per-second rate.
#[derive(Debug, Clone)]
pub struct TokenBucket {
    capacity: usize,
    tokens: f64,
    refill_rate: f64,
    last_refill: Instant,
}

impl TokenBucket {
    /// Create a bucket with the given burst capacity and refill rate.
    pub fn new(capacity: usize, per_second: usize) -> Self {
        Self {
            capacity,
            tokens: capacity as f64,
            refill_rate: per_second as f64,
            last_refill: Instant::now(),
        }
    }

    /// Try to consume one token. Returns true if successful.
    pub fn try_consume(&mut self) -> bool {
        self.refill();
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }

    /// Current number of available tokens.
    pub fn available(&mut self) -> f64 {
        self.refill();
        self.tokens
    }

    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity as f64);
        self.last_refill = now;
    }
}

/// Per-app rate limiter for queries and writes.
pub struct RateLimiter {
    config: QuotaConfig,
    query_buckets: HashMap<String, TokenBucket>,
    write_buckets: HashMap<String, TokenBucket>,
}

impl RateLimiter {
    pub fn new(config: QuotaConfig) -> Self {
        Self {
            config,
            query_buckets: HashMap::new(),
            write_buckets: HashMap::new(),
        }
    }

    /// Check and consume a query rate token.
    pub fn check_query(&mut self, app_id: &str) -> Result<(), RateLimitError> {
        let quotas = self.config.quotas_for_app(app_id);
        let Some(max_rate) = quotas.queries_per_second else {
            return Ok(());
        };

        let bucket = self
            .query_buckets
            .entry(app_id.to_string())
            .or_insert_with(|| TokenBucket::new(max_rate * 2, max_rate));

        if bucket.try_consume() {
            Ok(())
        } else {
            Err(RateLimitError::QueryRateExceeded { max: max_rate })
        }
    }

    /// Check and consume a write rate token.
    pub fn check_write(&mut self, app_id: &str) -> Result<(), RateLimitError> {
        let quotas = self.config.quotas_for_app(app_id);
        let Some(max_rate) = quotas.writes_per_second else {
            return Ok(());
        };

        let bucket = self
            .write_buckets
            .entry(app_id.to_string())
            .or_insert_with(|| TokenBucket::new(max_rate * 2, max_rate));

        if bucket.try_consume() {
            Ok(())
        } else {
            Err(RateLimitError::WriteRateExceeded { max: max_rate })
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_consume() {
        let mut bucket = TokenBucket::new(5, 5);
        // Should have 5 tokens initially.
        for _ in 0..5 {
            assert!(bucket.try_consume());
        }
        // 6th should fail.
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_refill() {
        let mut bucket = TokenBucket::new(10, 1000);
        // Consume all tokens.
        for _ in 0..10 {
            bucket.try_consume();
        }
        assert!(bucket.available() < 1.0);

        // Wait a bit for refill (1000/sec = 1/ms).
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(bucket.available() >= 10.0);
    }

    #[test]
    fn test_token_bucket_capacity_cap() {
        let mut bucket = TokenBucket::new(5, 1000);
        std::thread::sleep(std::time::Duration::from_millis(50));
        // Should not exceed capacity even after long wait.
        assert!(bucket.available() <= 5.0);
    }

    #[test]
    fn test_query_rate_limit_ok() {
        let config = QuotaConfig::default(); // third-party default: 100 qps
        let mut limiter = RateLimiter::new(config);
        // Should allow queries (bucket starts full with 2x burst).
        assert!(limiter.check_query("com.test").is_ok());
    }

    #[test]
    fn test_query_rate_limit_exceeded() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "com.test".into(),
            super::super::TierQuotas {
                max_entities: None,
                max_storage_bytes: None,
                max_query_results: 100,
                queries_per_second: Some(2),
                writes_per_second: Some(1),
            },
        );
        let config = QuotaConfig {
            first_party_apps: vec![],
            overrides,
        };
        let mut limiter = RateLimiter::new(config);

        // Burst capacity is 2x = 4 tokens.
        assert!(limiter.check_query("com.test").is_ok());
        assert!(limiter.check_query("com.test").is_ok());
        assert!(limiter.check_query("com.test").is_ok());
        assert!(limiter.check_query("com.test").is_ok());
        // 5th should fail.
        assert!(matches!(
            limiter.check_query("com.test"),
            Err(RateLimitError::QueryRateExceeded { max: 2 })
        ));
    }

    #[test]
    fn test_write_rate_limit() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "com.test".into(),
            super::super::TierQuotas {
                max_entities: None,
                max_storage_bytes: None,
                max_query_results: 100,
                queries_per_second: Some(100),
                writes_per_second: Some(1),
            },
        );
        let config = QuotaConfig {
            first_party_apps: vec![],
            overrides,
        };
        let mut limiter = RateLimiter::new(config);

        // Burst = 2 tokens.
        assert!(limiter.check_write("com.test").is_ok());
        assert!(limiter.check_write("com.test").is_ok());
        assert!(matches!(
            limiter.check_write("com.test"),
            Err(RateLimitError::WriteRateExceeded { max: 1 })
        ));
    }

    #[test]
    fn test_system_no_rate_limit() {
        let config = QuotaConfig::default();
        let mut limiter = RateLimiter::new(config);
        // System has no rate limit.
        for _ in 0..1000 {
            assert!(limiter.check_query("system").is_ok());
            assert!(limiter.check_write("system").is_ok());
        }
    }

    #[test]
    fn test_bucket_refills_after_drain() {
        // Use a small bucket to avoid loop-time refill artifacts.
        let mut bucket = TokenBucket::new(3, 100); // 3 burst, 100/sec
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume()); // exhausted

        // 100/sec = 1 token per 10ms. Wait 50ms for ~5 tokens.
        std::thread::sleep(std::time::Duration::from_millis(50));
        assert!(bucket.try_consume()); // should have refilled
    }
}
