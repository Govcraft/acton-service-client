# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - Unreleased

### Added

- `RetryPolicy::deadline(Duration)`: a total time budget per call, measured
  from the first send. Each attempt's timeout is the smaller of the configured
  timeout and the time remaining. A pause, from backoff or a server
  `Retry-After`, that would reach the deadline is not taken, and the last
  error or response is returned. It works alongside `max_attempts`, and
  whichever is reached first ends the loop.
- `Jitter` (`None`, `Full`) and `RetryPolicy::jitter(Jitter)`. `Full` draws
  each pause uniformly from `[base_delay, ceiling]`. The floor at `base_delay`
  departs from textbook full jitter on purpose, so an always-failing upstream
  is never retried in a tight loop.
- `RetryPolicy::pause(attempt, draw, remaining)`: the pure pause computation
  (integer milliseconds, reproducible across languages).
- `RetryPolicy::max_attempts(u32)` builder, so every field has a builder.
- `RequestBuilder::retry_on_status(StatusCode)`: extends the retriable
  statuses for one request, checked before `accept_status`. It takes effect
  only when retries apply (idempotent method or `.retriable(true)`).
- `RequestBuilder::timeout(Duration)`: a per-attempt timeout override.
- `RequestBuilder::deadline_at(Instant)`: an absolute deadline, so several
  sends of one operation share one budget. A request whose deadline has
  already passed is not sent and fails with `ClientError::Config`.
- `Jitter` is re-exported at the crate root.

### Changed

- **Breaking:** `RetryPolicy` is now `#[non_exhaustive]` and has two new
  fields (`deadline`, `jitter`). Callers outside the crate can no longer build
  it with a struct literal.

### Migration

Replace a `RetryPolicy` struct literal with the builders. Every field has one:

```rust
// 0.1
let policy = RetryPolicy {
    max_attempts: 5,
    base_delay: Duration::from_millis(100),
    max_delay: Duration::from_secs(1),
};

// 0.2
let policy = RetryPolicy::default()
    .max_attempts(5)
    .base_delay(Duration::from_millis(100))
    .max_delay(Duration::from_secs(1));
```

Reading the fields (`policy.max_attempts`, ...) is unchanged. With the defaults
(no deadline, `Jitter::None`, no extra statuses, no per-request overrides),
retry attempts and delays are identical to 0.1.2.

## [0.1.2]

### Added

- `ServiceClientBuilder::with_http_client` to supply a custom `reqwest::Client`.

## [0.1.1]

### Added

- `RequestBuilder::body` for a raw body with an explicit content type.

## [0.1.0]

### Added

- Initial release: typed async client for acton-service wire conventions.
