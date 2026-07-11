//! Retry policy and backoff computation.
//!
//! Retries are **off by default**. When a [`RetryPolicy`] is configured on the
//! client, it applies only to idempotent HTTP methods (`GET`, `HEAD`, `DELETE`,
//! `PUT`) plus any request the caller explicitly marks retriable. The backoff
//! delay is a pure function ([`RetryPolicy::backoff_delay`]) so it can be
//! unit-tested; the actual sleeping is performed by the client via `tokio`.

use reqwest::Method;
use std::time::Duration;

/// Exponential-backoff retry policy with a delay cap.
///
/// `max_attempts` counts total attempts (not retries): a value of `3` means one
/// initial try plus up to two retries.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total number of attempts, including the first. Values below 1 behave as 1.
    pub max_attempts: u32,
    /// Base delay used for the first backoff interval.
    pub base_delay: Duration,
    /// Upper bound on any single backoff interval.
    pub max_delay: Duration,
}

impl Default for RetryPolicy {
    /// Three attempts, 100ms base delay, capped at 5s.
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
        }
    }
}

impl RetryPolicy {
    /// Create a policy with the given attempt count and default timings.
    #[must_use]
    pub fn with_max_attempts(max_attempts: u32) -> Self {
        Self {
            max_attempts,
            ..Self::default()
        }
    }

    /// Set the base delay.
    #[must_use]
    pub fn base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Set the maximum delay cap.
    #[must_use]
    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Compute the backoff delay before the retry following `attempt`.
    ///
    /// `attempt` is 1-based: the delay after the first attempt uses exponent 0
    /// (i.e. `base_delay`), the delay after the second uses exponent 1, and so
    /// on. The result is clamped to `max_delay`. This is a pure function.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// let p = RetryPolicy {
    ///     max_attempts: 5,
    ///     base_delay: Duration::from_millis(100),
    ///     max_delay: Duration::from_secs(1),
    /// };
    /// assert_eq!(p.backoff_delay(1), Duration::from_millis(100));
    /// assert_eq!(p.backoff_delay(2), Duration::from_millis(200));
    /// assert_eq!(p.backoff_delay(3), Duration::from_millis(400));
    /// // Capped at max_delay.
    /// assert_eq!(p.backoff_delay(20), Duration::from_secs(1));
    /// ```
    #[must_use]
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1);
        let base_ms = self.base_delay.as_millis() as u64;
        // Saturating exponential growth: base * 2^exponent, clamped to max_delay.
        let multiplier = 1u64.checked_shl(exponent.min(63)).unwrap_or(u64::MAX);
        let delay_ms = base_ms.saturating_mul(multiplier);
        let capped = delay_ms.min(self.max_delay.as_millis() as u64);
        Duration::from_millis(capped)
    }

    /// Whether another attempt is permitted after `attempt` (1-based).
    #[must_use]
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }
}

/// Whether an HTTP method is idempotent and therefore safe to retry by default.
///
/// `GET`, `HEAD`, `DELETE`, and `PUT` are idempotent; `POST` and `PATCH` are
/// not and require an explicit per-request opt-in.
///
/// # Examples
///
/// ```
/// use acton_service_client::retry::is_idempotent;
/// use reqwest::Method;
///
/// assert!(is_idempotent(&Method::GET));
/// assert!(is_idempotent(&Method::PUT));
/// assert!(!is_idempotent(&Method::POST));
/// ```
#[must_use]
pub fn is_idempotent(method: &Method) -> bool {
    matches!(
        *method,
        Method::GET | Method::HEAD | Method::DELETE | Method::PUT
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_policy_values() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.base_delay, Duration::from_millis(100));
        assert_eq!(p.max_delay, Duration::from_secs(5));
    }

    #[test]
    fn backoff_is_exponential_then_capped() {
        let p = RetryPolicy {
            max_attempts: 10,
            base_delay: Duration::from_millis(50),
            max_delay: Duration::from_millis(1000),
        };
        assert_eq!(p.backoff_delay(1), Duration::from_millis(50));
        assert_eq!(p.backoff_delay(2), Duration::from_millis(100));
        assert_eq!(p.backoff_delay(3), Duration::from_millis(200));
        assert_eq!(p.backoff_delay(4), Duration::from_millis(400));
        assert_eq!(p.backoff_delay(5), Duration::from_millis(800));
        assert_eq!(p.backoff_delay(6), Duration::from_millis(1000));
        assert_eq!(p.backoff_delay(100), Duration::from_millis(1000));
    }

    #[test]
    fn backoff_does_not_overflow_on_huge_attempt() {
        let p = RetryPolicy {
            max_attempts: u32::MAX,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
        };
        assert_eq!(p.backoff_delay(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn should_retry_respects_attempts() {
        let p = RetryPolicy::with_max_attempts(3);
        assert!(p.should_retry(1));
        assert!(p.should_retry(2));
        assert!(!p.should_retry(3));
    }

    #[test]
    fn should_retry_treats_zero_attempts_as_one() {
        let p = RetryPolicy::with_max_attempts(0);
        assert!(!p.should_retry(1));
    }

    #[test]
    fn idempotency_classification() {
        assert!(is_idempotent(&Method::GET));
        assert!(is_idempotent(&Method::HEAD));
        assert!(is_idempotent(&Method::DELETE));
        assert!(is_idempotent(&Method::PUT));
        assert!(!is_idempotent(&Method::POST));
        assert!(!is_idempotent(&Method::PATCH));
    }
}
