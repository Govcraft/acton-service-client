//! Retry policy, backoff, jitter, and deadline computation.
//!
//! Retries are **off by default**. When a [`RetryPolicy`] is configured on the
//! client, it applies only to idempotent HTTP methods (`GET`, `HEAD`, `DELETE`,
//! `PUT`) plus any request the caller explicitly marks retriable.
//!
//! Every number the retry loop acts on comes from a pure function in this
//! module, so it can be unit-tested (and pinned by cross-language parity
//! vectors) without a network or a clock:
//!
//! - [`RetryPolicy::backoff_delay`] is the exponential ceiling for a pause.
//! - [`RetryPolicy::pause`] applies [`Jitter`] under that ceiling and refuses a
//!   pause that would reach the deadline.
//!
//! The only randomness (the `draw` fed to [`RetryPolicy::pause`]) and the only
//! sleeping happen in the client's send loop.

use std::time::{Duration, Instant};

use reqwest::{Method, StatusCode};

use crate::error::status_is_retriable;

/// How a retry pause is spread below its exponential ceiling.
///
/// The ceiling for the pause after attempt `n` is
/// [`RetryPolicy::backoff_delay(n)`](RetryPolicy::backoff_delay).
///
/// # Why [`Full`](Jitter::Full) keeps a floor
///
/// Textbook "full jitter" (as popularized by AWS) draws a pause uniformly from
/// `[0, ceiling]`. This crate deliberately departs from that: `Full` draws from
/// `[base_delay, ceiling]`, so a pause is **never shorter than
/// [`RetryPolicy::base_delay`]**. With a zero floor, an upstream that fails on
/// every attempt, combined with a large `max_attempts`, can be hammered by a
/// run of near-zero pauses: a tight loop exactly when the upstream most needs
/// relief. The floor keeps the minimum spacing between attempts equal to
/// `base_delay`, so the attempt count is always bounded by
/// `deadline / base_delay` as well as by `max_attempts`.
///
/// # Examples
///
/// ```
/// use acton_service_client::retry::{Jitter, RetryPolicy};
/// use std::time::Duration;
///
/// let policy = RetryPolicy::default().jitter(Jitter::Full);
/// // Even the smallest possible draw pauses for at least base_delay.
/// assert_eq!(policy.pause(3, 0.0, None), Some(policy.base_delay));
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Jitter {
    /// No jitter: every pause is exactly the exponential ceiling. The default,
    /// and the 0.1 behaviour.
    #[default]
    None,
    /// Uniform jitter over `[base_delay, ceiling]`, floored at `base_delay`
    /// (see the [type-level docs](Jitter) for why the floor is not zero).
    Full,
}

/// Exponential-backoff retry policy with a delay cap, optional jitter, and an
/// optional total deadline.
///
/// `max_attempts` counts total attempts (not retries): a value of `3` means one
/// initial try plus up to two retries. A [`deadline`](Self::deadline) bounds the
/// whole call, first send to last response, alongside `max_attempts`; whichever
/// is reached first ends the loop, and the last error or response is returned.
///
/// The struct is `#[non_exhaustive]`: build it from [`RetryPolicy::default`] or
/// [`RetryPolicy::with_max_attempts`] and the builder methods, one per field.
///
/// # Examples
///
/// ```
/// use acton_service_client::retry::{Jitter, RetryPolicy};
/// use std::time::Duration;
///
/// let policy = RetryPolicy::default()
///     .max_attempts(8)
///     .base_delay(Duration::from_millis(50))
///     .max_delay(Duration::from_secs(2))
///     .jitter(Jitter::Full)
///     .deadline(Duration::from_secs(5));
/// assert_eq!(policy.max_attempts, 8);
/// assert_eq!(policy.deadline, Some(Duration::from_secs(5)));
/// ```
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    /// Total number of attempts, including the first. Values below 1 behave as 1.
    pub max_attempts: u32,
    /// Base delay used for the first backoff interval, and the floor of every
    /// [`Jitter::Full`] pause.
    pub base_delay: Duration,
    /// Upper bound on any single backoff interval.
    pub max_delay: Duration,
    /// Total time budget for one call, measured from its first send. `None`
    /// (the default) leaves the call bounded by `max_attempts` alone.
    pub deadline: Option<Duration>,
    /// How each pause is spread below its ceiling. Defaults to [`Jitter::None`].
    pub jitter: Jitter,
}

impl Default for RetryPolicy {
    /// Three attempts, 100ms base delay, capped at 5s, no jitter, no deadline.
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            deadline: None,
            jitter: Jitter::None,
        }
    }
}

/// The largest `f64` strictly below `1.0` (`0.9999999999999999`), the top of
/// the clamped draw range.
const DRAW_MAX: f64 = 1.0 - f64::EPSILON / 2.0;

impl RetryPolicy {
    /// Create a policy with the given attempt count and default timings.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    ///
    /// assert_eq!(RetryPolicy::with_max_attempts(5).max_attempts, 5);
    /// ```
    #[must_use]
    pub fn with_max_attempts(max_attempts: u32) -> Self {
        Self::default().max_attempts(max_attempts)
    }

    /// Set the total number of attempts, including the first. Values below 1
    /// behave as 1.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    ///
    /// let p = RetryPolicy::default().max_attempts(1);
    /// assert!(!p.should_retry(1)); // a single attempt, never retried
    /// ```
    #[must_use]
    pub fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set the base delay.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// let p = RetryPolicy::default().base_delay(Duration::from_millis(250));
    /// assert_eq!(p.backoff_delay(1), Duration::from_millis(250));
    /// ```
    #[must_use]
    pub fn base_delay(mut self, base_delay: Duration) -> Self {
        self.base_delay = base_delay;
        self
    }

    /// Set the maximum delay cap.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// let p = RetryPolicy::default().max_delay(Duration::from_millis(150));
    /// assert_eq!(p.backoff_delay(10), Duration::from_millis(150));
    /// ```
    #[must_use]
    pub fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Bound the whole call, first send to last response, by `deadline`.
    ///
    /// - Each attempt's timeout becomes the smaller of the configured timeout
    ///   (the client's [`timeout`](crate::ServiceClientBuilder::timeout) or the
    ///   request's [`timeout`](crate::RequestBuilder::timeout)) and the time
    ///   remaining.
    /// - A pause, from backoff or from a server `Retry-After`, that would end
    ///   at or after the deadline is not taken: the loop stops and returns the
    ///   last error or response.
    ///
    /// The deadline applies to every call through the client, including one
    /// that is never retried (a `POST` without
    /// [`retriable`](crate::RequestBuilder::retriable)), whose single attempt
    /// is then bounded by it. To share one budget across several calls, use
    /// [`RequestBuilder::deadline_at`](crate::RequestBuilder::deadline_at).
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// // Retry for as long as it takes, but never longer than 10 seconds.
    /// let p = RetryPolicy::default()
    ///     .max_attempts(u32::MAX)
    ///     .deadline(Duration::from_secs(10));
    /// assert_eq!(p.deadline, Some(Duration::from_secs(10)));
    /// ```
    #[must_use]
    pub fn deadline(mut self, deadline: Duration) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// Spread each pause below its ceiling (see [`Jitter`]).
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::retry::{Jitter, RetryPolicy};
    ///
    /// let p = RetryPolicy::default().jitter(Jitter::Full);
    /// assert_eq!(p.jitter, Jitter::Full);
    /// ```
    #[must_use]
    pub fn jitter(mut self, jitter: Jitter) -> Self {
        self.jitter = jitter;
        self
    }

    /// Compute the exponential backoff ceiling for the pause after `attempt`.
    ///
    /// `attempt` is 1-based: the delay after the first attempt uses exponent 0
    /// (i.e. `base_delay`), the delay after the second uses exponent 1, and so
    /// on. The result is clamped to `max_delay` and works in whole
    /// milliseconds. This is a pure function, and with [`Jitter::None`] it is
    /// exactly the pause taken.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// let p = RetryPolicy::default()
    ///     .max_attempts(5)
    ///     .base_delay(Duration::from_millis(100))
    ///     .max_delay(Duration::from_secs(1));
    /// assert_eq!(p.backoff_delay(1), Duration::from_millis(100));
    /// assert_eq!(p.backoff_delay(2), Duration::from_millis(200));
    /// assert_eq!(p.backoff_delay(3), Duration::from_millis(400));
    /// // Capped at max_delay.
    /// assert_eq!(p.backoff_delay(20), Duration::from_secs(1));
    /// ```
    #[must_use]
    pub fn backoff_delay(&self, attempt: u32) -> Duration {
        Duration::from_millis(self.ceiling_ms(attempt))
    }

    /// The pause to take after `attempt` (1-based), or `None` when that pause
    /// would reach the deadline.
    ///
    /// - `draw` is a uniform random sample the caller supplies; it is clamped
    ///   to `[0, 1)`, and a NaN counts as `0`. It is ignored under
    ///   [`Jitter::None`].
    /// - `remaining` is the time left before the deadline, or `None` when there
    ///   is no deadline.
    ///
    /// The computation works in whole milliseconds, so it is reproducible in
    /// any language with IEEE-754 doubles. With `ceiling = backoff_delay(attempt)`
    /// and `floor = min(base_delay, ceiling)`:
    ///
    /// - [`Jitter::None`]: `pause = ceiling`
    /// - [`Jitter::Full`]: `pause = floor + ⌊(ceiling − floor) × draw⌋`
    ///
    /// and the result is `None` if `remaining` is `Some(r)` with `pause ≥ r`.
    /// The pause never counts attempts: whether `attempt` may be followed by
    /// another at all is [`should_retry`](Self::should_retry).
    ///
    /// # Examples
    ///
    /// No jitter reproduces [`backoff_delay`](Self::backoff_delay), whatever the draw:
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// let ms = Duration::from_millis;
    /// let p = RetryPolicy::default()
    ///     .base_delay(ms(100))
    ///     .max_delay(ms(1000));
    /// assert_eq!(p.pause(1, 0.0, None), Some(ms(100)));
    /// assert_eq!(p.pause(1, 0.9, None), Some(ms(100)));
    /// assert_eq!(p.pause(3, 0.5, None), Some(ms(400)));
    /// assert_eq!(p.pause(20, 0.5, None), Some(ms(1000)));
    /// ```
    ///
    /// Full jitter spreads the pause over `[base_delay, ceiling)`:
    ///
    /// ```
    /// use acton_service_client::retry::{Jitter, RetryPolicy};
    /// use std::time::Duration;
    ///
    /// let ms = Duration::from_millis;
    /// let p = RetryPolicy::default()
    ///     .base_delay(ms(100))
    ///     .max_delay(ms(1000))
    ///     .jitter(Jitter::Full);
    /// // Attempt 1: the ceiling is the base, so there is nothing to spread.
    /// assert_eq!(p.pause(1, 0.0, None), Some(ms(100)));
    /// assert_eq!(p.pause(1, 0.99, None), Some(ms(100)));
    /// // Attempt 3: ceiling 400, floor 100, span 300.
    /// assert_eq!(p.pause(3, 0.0, None), Some(ms(100)));   // never below the floor
    /// assert_eq!(p.pause(3, 0.25, None), Some(ms(175)));
    /// assert_eq!(p.pause(3, 0.5, None), Some(ms(250)));
    /// assert_eq!(p.pause(3, 0.999, None), Some(ms(399))); // ⌊299.7⌋
    /// // Attempt 20: the ceiling is capped at max_delay.
    /// assert_eq!(p.pause(20, 0.5, None), Some(ms(550)));
    /// ```
    ///
    /// An out-of-range draw is clamped, not rejected:
    ///
    /// ```
    /// use acton_service_client::retry::{Jitter, RetryPolicy};
    /// use std::time::Duration;
    ///
    /// let ms = Duration::from_millis;
    /// let p = RetryPolicy::default()
    ///     .base_delay(ms(100))
    ///     .max_delay(ms(1000))
    ///     .jitter(Jitter::Full);
    /// assert_eq!(p.pause(3, -1.0, None), Some(ms(100)));     // clamped to 0
    /// assert_eq!(p.pause(3, f64::NAN, None), Some(ms(100))); // NaN counts as 0
    /// assert_eq!(p.pause(3, 1.0, None), Some(ms(399)));      // clamped below 1
    /// assert_eq!(p.pause(3, 7.5, None), Some(ms(399)));
    /// ```
    ///
    /// A pause that would reach the deadline is refused, at the boundary too:
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    /// use std::time::Duration;
    ///
    /// let ms = Duration::from_millis;
    /// let p = RetryPolicy::default()
    ///     .base_delay(ms(100))
    ///     .max_delay(ms(1000));
    /// assert_eq!(p.pause(3, 0.0, Some(ms(401))), Some(ms(400))); // ends 1ms before
    /// assert_eq!(p.pause(3, 0.0, Some(ms(400))), None);          // would end at it
    /// assert_eq!(p.pause(3, 0.0, Some(ms(50))), None);
    /// assert_eq!(p.pause(1, 0.0, Some(Duration::ZERO)), None);  // already expired
    /// ```
    ///
    /// When `base_delay` exceeds `max_delay`, the floor drops to the cap:
    ///
    /// ```
    /// use acton_service_client::retry::{Jitter, RetryPolicy};
    /// use std::time::Duration;
    ///
    /// let ms = Duration::from_millis;
    /// let p = RetryPolicy::default()
    ///     .base_delay(ms(500))
    ///     .max_delay(ms(200))
    ///     .jitter(Jitter::Full);
    /// assert_eq!(p.pause(1, 0.0, None), Some(ms(200)));
    /// assert_eq!(p.pause(4, 0.9, None), Some(ms(200)));
    /// ```
    #[must_use]
    pub fn pause(&self, attempt: u32, draw: f64, remaining: Option<Duration>) -> Option<Duration> {
        let ceiling = self.ceiling_ms(attempt);
        let pause = match self.jitter {
            Jitter::None => ceiling,
            Jitter::Full => {
                let floor = duration_ms(self.base_delay).min(ceiling);
                floor + spread(ceiling - floor, draw)
            }
        };
        fits_before(Duration::from_millis(pause), remaining)
    }

    /// Whether another attempt is permitted after `attempt` (1-based).
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RetryPolicy;
    ///
    /// let p = RetryPolicy::with_max_attempts(3);
    /// assert!(p.should_retry(2));
    /// assert!(!p.should_retry(3));
    /// ```
    #[must_use]
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts.max(1)
    }

    /// The pause before the attempt after `attempt`, or `None` to stop.
    ///
    /// Stops when `max_attempts` is spent or when the pause would reach the
    /// deadline. A server `Retry-After` replaces the computed pause (unjittered,
    /// uncapped, as in 0.1) but is still refused if it would reach the deadline.
    pub(crate) fn next_pause(
        &self,
        attempt: u32,
        draw: f64,
        retry_after: Option<Duration>,
        remaining: Option<Duration>,
    ) -> Option<Duration> {
        if !self.should_retry(attempt) {
            return None;
        }
        match retry_after {
            Some(server) => fits_before(server, remaining),
            None => self.pause(attempt, draw, remaining),
        }
    }

    /// `min(base_delay × 2^(attempt − 1), max_delay)` in whole milliseconds,
    /// saturating rather than overflowing.
    fn ceiling_ms(&self, attempt: u32) -> u64 {
        let exponent = attempt.saturating_sub(1);
        let multiplier = 1u64.checked_shl(exponent.min(63)).unwrap_or(u64::MAX);
        duration_ms(self.base_delay)
            .saturating_mul(multiplier)
            .min(duration_ms(self.max_delay))
    }
}

/// A duration in whole milliseconds, saturating at `u64::MAX`.
fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// `⌊span × draw⌋` with `draw` clamped to `[0, 1)` (NaN as 0), never above `span`.
fn spread(span: u64, draw: f64) -> u64 {
    let draw = if draw.is_nan() {
        0.0
    } else {
        draw.clamp(0.0, DRAW_MAX)
    };
    // `span` is at most `u64::MAX`, so the product is finite and non-negative;
    // the float-to-int conversion saturates, and `min` absorbs any rounding.
    let offset = (span as f64 * draw).floor() as u64;
    offset.min(span)
}

/// `Some(pause)` unless a deadline exists and the pause would end at or after it.
fn fits_before(pause: Duration, remaining: Option<Duration>) -> Option<Duration> {
    match remaining {
        Some(left) if pause >= left => None,
        _ => Some(pause),
    }
}

/// Time left before `deadline` as seen at `now`: `None` without a deadline,
/// zero once it has passed.
pub(crate) fn remaining_until(deadline: Option<Instant>, now: Instant) -> Option<Duration> {
    deadline.map(|at| at.saturating_duration_since(now))
}

/// The timeout for one attempt, or `None` to leave the HTTP client's own.
///
/// - `request` is the per-request override ([`RequestBuilder::timeout`](crate::RequestBuilder::timeout)).
/// - `client` is the builder-configured client timeout, or `None` for a
///   client supplied via `with_http_client`, whose timeout cannot be observed.
/// - `remaining` is the time left before the deadline, if there is one.
///
/// With neither an override nor a deadline, the request carries no timeout of
/// its own, exactly as in 0.1.
pub(crate) fn attempt_timeout(
    request: Option<Duration>,
    client: Option<Duration>,
    remaining: Option<Duration>,
) -> Option<Duration> {
    match (request, remaining) {
        (None, None) => None,
        (Some(own), None) => Some(own),
        (request, Some(left)) => Some(request.or(client).map_or(left, |own| own.min(left))),
    }
}

/// Whether a non-success `status` asks for another attempt.
///
/// An explicit per-request `retry_on` status always does (it is checked before
/// the accepted list). Otherwise an accepted status is returned as-is, and any
/// other status is retried when it is retriable by default (see
/// [`ApiError::is_retriable`](crate::ApiError::is_retriable)).
pub(crate) fn wants_retry(
    status: StatusCode,
    retry_after: Option<Duration>,
    accepted: bool,
    retry_on: &[StatusCode],
) -> bool {
    retry_on.contains(&status) || (!accepted && status_is_retriable(status, retry_after))
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
    use crate::error::build_api_error;
    use reqwest::header::HeaderMap;

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    fn policy(base: u64, max: u64) -> RetryPolicy {
        RetryPolicy::default()
            .base_delay(ms(base))
            .max_delay(ms(max))
    }

    #[test]
    fn default_policy_values() {
        let p = RetryPolicy::default();
        assert_eq!(p.max_attempts, 3);
        assert_eq!(p.base_delay, ms(100));
        assert_eq!(p.max_delay, Duration::from_secs(5));
        assert_eq!(p.deadline, None);
        assert_eq!(p.jitter, Jitter::None);
    }

    #[test]
    fn builders_set_every_field() {
        let p = RetryPolicy::with_max_attempts(2)
            .max_attempts(9)
            .base_delay(ms(7))
            .max_delay(ms(70))
            .deadline(ms(700))
            .jitter(Jitter::Full);
        assert_eq!(
            p,
            RetryPolicy {
                max_attempts: 9,
                base_delay: ms(7),
                max_delay: ms(70),
                deadline: Some(ms(700)),
                jitter: Jitter::Full,
            }
        );
    }

    #[test]
    fn backoff_is_exponential_then_capped() {
        let p = policy(50, 1000).max_attempts(10);
        assert_eq!(p.backoff_delay(1), ms(50));
        assert_eq!(p.backoff_delay(2), ms(100));
        assert_eq!(p.backoff_delay(3), ms(200));
        assert_eq!(p.backoff_delay(4), ms(400));
        assert_eq!(p.backoff_delay(5), ms(800));
        assert_eq!(p.backoff_delay(6), ms(1000));
        assert_eq!(p.backoff_delay(100), ms(1000));
    }

    #[test]
    fn backoff_does_not_overflow_on_huge_attempt() {
        let p = policy(100, 30_000).max_attempts(u32::MAX);
        assert_eq!(p.backoff_delay(u32::MAX), Duration::from_secs(30));
    }

    #[test]
    fn backoff_attempt_zero_behaves_as_one() {
        assert_eq!(policy(100, 1000).backoff_delay(0), ms(100));
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
    fn full_jitter_stays_within_floor_and_ceiling() {
        let p = policy(40, 5_000).jitter(Jitter::Full);
        for attempt in 1..=12 {
            let ceiling = p.backoff_delay(attempt);
            for step in 0..=100 {
                let draw = f64::from(step) / 100.0;
                let pause = p.pause(attempt, draw, None).unwrap();
                assert!(pause >= ms(40), "attempt {attempt} draw {draw}: {pause:?}");
                assert!(pause <= ceiling, "attempt {attempt} draw {draw}: {pause:?}");
            }
        }
    }

    #[test]
    fn full_jitter_is_monotonic_in_the_draw() {
        let p = policy(10, 10_000).jitter(Jitter::Full);
        let mut last = Duration::ZERO;
        for step in 0..=1000 {
            let pause = p.pause(8, f64::from(step) / 1000.0, None).unwrap();
            assert!(pause >= last);
            last = pause;
        }
    }

    #[test]
    fn full_jitter_with_zero_base_can_reach_zero() {
        // The floor is base_delay; a zero base is the caller's explicit choice.
        let p = policy(0, 1000).jitter(Jitter::Full);
        assert_eq!(p.pause(5, 0.0, None), Some(Duration::ZERO));
    }

    #[test]
    fn spread_clamps_and_never_exceeds_span() {
        assert_eq!(spread(0, 0.7), 0);
        assert_eq!(spread(300, 0.0), 0);
        assert_eq!(spread(300, -3.0), 0);
        assert_eq!(spread(300, f64::NAN), 0);
        assert_eq!(spread(300, f64::INFINITY), 299);
        assert_eq!(spread(300, f64::NEG_INFINITY), 0);
        assert_eq!(spread(300, 1.0), 299);
        assert_eq!(spread(u64::MAX, 0.5), 1 << 63);
        assert!(spread(u64::MAX, DRAW_MAX) < u64::MAX);
    }

    #[test]
    fn draw_max_is_the_largest_double_below_one() {
        const { assert!(DRAW_MAX < 1.0) };
        assert_eq!(DRAW_MAX.to_bits() + 1, 1.0f64.to_bits());
    }

    #[test]
    fn pause_refuses_to_reach_the_deadline() {
        let p = policy(100, 1000);
        assert_eq!(p.pause(1, 0.0, Some(ms(101))), Some(ms(100)));
        assert_eq!(p.pause(1, 0.0, Some(ms(100))), None);
        assert_eq!(p.pause(1, 0.0, Some(ms(99))), None);
        assert_eq!(p.pause(1, 0.0, Some(Duration::ZERO)), None);
    }

    #[test]
    fn next_pause_stops_when_attempts_are_spent() {
        let p = policy(10, 100).max_attempts(2);
        assert_eq!(p.next_pause(1, 0.0, None, None), Some(ms(10)));
        assert_eq!(p.next_pause(2, 0.0, None, None), None);
        assert_eq!(p.next_pause(2, 0.0, Some(ms(1)), None), None);
    }

    #[test]
    fn next_pause_honours_retry_after_unless_it_crosses_the_deadline() {
        let p = policy(10, 100).jitter(Jitter::Full);
        let retry_after = Some(Duration::from_secs(2));
        // Retry-After wins over backoff, uncapped by max_delay and unjittered.
        assert_eq!(
            p.next_pause(1, 0.9, retry_after, None),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            p.next_pause(1, 0.9, retry_after, Some(Duration::from_secs(3))),
            Some(Duration::from_secs(2))
        );
        assert_eq!(
            p.next_pause(1, 0.9, retry_after, Some(Duration::from_secs(2))),
            None
        );
    }

    #[test]
    fn huge_max_attempts_is_bounded_by_the_deadline_through_the_floor() {
        // Jitter floor: even when every draw is the minimum, each pause costs
        // base_delay, so a deadline caps the number of pauses taken.
        let p = policy(20, 1000).max_attempts(u32::MAX).jitter(Jitter::Full);
        let deadline = ms(300);
        let mut elapsed = Duration::ZERO;
        let mut attempt = 1;
        while let Some(pause) = p.next_pause(attempt, 0.0, None, Some(deadline - elapsed)) {
            assert!(pause >= ms(20));
            elapsed += pause;
            attempt += 1;
        }
        assert!(elapsed < deadline);
        assert!(
            attempt <= 15,
            "{attempt} attempts inside 300ms at a 20ms floor"
        );
    }

    #[test]
    fn remaining_until_saturates_at_zero() {
        let now = Instant::now();
        assert_eq!(remaining_until(None, now), None);
        assert_eq!(remaining_until(Some(now + ms(40)), now), Some(ms(40)));
        assert_eq!(
            remaining_until(Some(now), now + ms(5)),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn attempt_timeout_is_the_min_of_timeout_and_remaining() {
        let s = Duration::from_secs;
        // No override, no deadline: leave the HTTP client's own timeout alone.
        assert_eq!(attempt_timeout(None, Some(s(30)), None), None);
        assert_eq!(attempt_timeout(None, None, None), None);
        // An override without a deadline is used as-is.
        assert_eq!(attempt_timeout(Some(s(2)), Some(s(30)), None), Some(s(2)));
        // Under a deadline: min(override or client timeout, remaining).
        assert_eq!(attempt_timeout(None, Some(s(30)), Some(s(4))), Some(s(4)));
        assert_eq!(attempt_timeout(None, Some(s(3)), Some(s(4))), Some(s(3)));
        assert_eq!(
            attempt_timeout(Some(s(1)), Some(s(30)), Some(s(4))),
            Some(s(1))
        );
        assert_eq!(
            attempt_timeout(Some(s(9)), Some(s(3)), Some(s(4))),
            Some(s(4))
        );
        // A supplied client's timeout is unknown: the deadline governs.
        assert_eq!(attempt_timeout(None, None, Some(s(4))), Some(s(4)));
        assert_eq!(
            attempt_timeout(None, None, Some(Duration::ZERO)),
            Some(Duration::ZERO)
        );
    }

    #[test]
    fn wants_retry_puts_retry_on_before_accept() {
        let s503 = StatusCode::SERVICE_UNAVAILABLE;
        let s421 = StatusCode::MISDIRECTED_REQUEST;
        // Default-retriable, not accepted: retried.
        assert!(wants_retry(s503, None, false, &[]));
        // Accepted without retry_on: returned as-is, as in 0.1.
        assert!(!wants_retry(s503, None, true, &[]));
        // Accepted AND retry_on: retried.
        assert!(wants_retry(s503, None, true, &[s503]));
        // Not retriable by default, but asked for.
        assert!(!wants_retry(s421, None, false, &[]));
        assert!(wants_retry(s421, None, false, &[s421]));
        assert!(!wants_retry(s421, None, false, &[s503]));
    }

    /// Defaults must reproduce 0.1.2 exactly: same retriable statuses, same
    /// pauses, same attempt count, no per-request timeout.
    #[test]
    fn defaults_reproduce_the_0_1_2_schedule() {
        let p = RetryPolicy::default();
        // 0.1.2: sleep(retry_after.unwrap_or(backoff_delay(attempt))) while
        // should_retry(attempt), whatever the draw.
        for draw in [0.0, 0.3, 0.5, 0.999, 1.0, f64::NAN] {
            assert_eq!(p.next_pause(1, draw, None, None), Some(ms(100)));
            assert_eq!(p.next_pause(2, draw, None, None), Some(ms(200)));
            assert_eq!(p.next_pause(3, draw, None, None), None);
            let server = Some(Duration::from_secs(42));
            assert_eq!(p.next_pause(1, draw, server, None), server);
            assert_eq!(p.next_pause(3, draw, server, None), None);
        }
        let big = RetryPolicy::with_max_attempts(40);
        for attempt in 1..40 {
            assert_eq!(
                big.next_pause(attempt, 0.42, None, None),
                Some(big.backoff_delay(attempt))
            );
        }
        // Retriable statuses are exactly ApiError::is_retriable's, and an
        // accepted status is never retried.
        let mut locked = HeaderMap::new();
        locked.insert("retry-after", "30".parse().unwrap());
        for code in 100..600u16 {
            let status = StatusCode::from_u16(code).unwrap();
            for headers in [HeaderMap::new(), locked.clone()] {
                let api = build_api_error(status, &headers, "");
                assert_eq!(
                    wants_retry(status, api.retry_after, false, &[]),
                    api.is_retriable(),
                    "{status}"
                );
                assert!(!wants_retry(status, api.retry_after, true, &[]));
            }
        }
        // No override, no deadline: no per-request timeout is set.
        assert_eq!(
            attempt_timeout(None, Some(Duration::from_secs(30)), None),
            None
        );
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
