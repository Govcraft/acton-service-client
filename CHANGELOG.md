# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.1] - Unreleased

### Added

- **Endpoint failover under one deadline.** `ServiceClientBuilder::failover_endpoint`
  and `failover_endpoints` add endpoints after the base URL to form an ordered
  endpoint set. When retries apply to a request (a policy is set and the method
  is idempotent or the request is `.retriable(true)`), a status listed with
  `retry_on_status`, a connect failure, or an attempt timeout moves the next
  attempt to the next endpoint at once. After a full cycle the client pauses
  with the policy's backoff, or the smallest `Retry-After` when every endpoint
  in the cycle sent one. One deadline covers every attempt on every endpoint,
  and `max_attempts` counts every send. Statuses retriable by default but not
  listed still retry the same endpoint, and every other answer is returned as
  before.
- `Endpoint` (`new`, `with_http_client`, `url`, from `&str`/`String`): a
  failover endpoint, optionally with its own HTTP client (for example its own
  TLS configuration).
- `ServiceClient::endpoints()` and `ServiceClient::preferred_endpoint()`. The
  preference is sticky: the next call starts at the endpoint that last gave a
  definitive answer (any status that is not a rotation status, a `4xx`
  included); transport failures and rotation statuses never move it.
- `EndpointOrigin`: a normalized `scheme://host:port` origin.
- `RetryReason` (`Status`, `Connect`, `Timeout`) with `label()` for metrics
  and `proves_not_processed()` (true only for `Connect` and `Status(421)`).
  `Connect` is reqwest's `is_connect()`: the connection could not be
  established, so no byte of the request was written. A transport failure
  after the request was written (a reset mid-body) is never `Connect`; as in
  0.2.0 it is not retried and comes back as `ClientError::Transport`.
- `RetryObserver` and `ServiceClientBuilder::retry_observer`: the metrics
  seam, with no metrics dependency. `on_rotation(left, reason)` is called for
  each move to the next endpoint, `on_retry(endpoint, reason, attempt)` for
  each re-send to the same endpoint (a single-endpoint client's included).
  Both default to doing nothing and run synchronously on the caller's task.
  The client's observer hears the union of every request's re-sends, in order
  within a request and with no order across concurrent requests: it is for
  counters.
- `RequestBuilder::retry_observer`: an observer for one request, called in
  addition to the client's (the client's first). Guaranteed: it hears exactly
  that request's re-sends, every attempt but the last exactly once, in order,
  with its outcome, so its reports plus the request's result are the complete
  per-attempt record of that call, even with concurrent requests on the same
  client. It is the only record on the paths that carry no trace:
  `DeadlineExceeded`, and errors returned as they are (a single endpoint's,
  or a call that never rotated).
- `ClientError::InvalidEndpoints(EndpointSetError)`: a duplicate origin after
  normalization, mixed schemes, or a failover endpoint that is not a bare
  origin is a typed build error, never a panic.
- `ClientError::EndpointsExhausted(Box<FailoverTrace>)` and
  `ClientError::failover_trace()`: a call that runs out of budget after failing
  over returns every attempt (endpoint, outcome, whether it rotated) and the
  final attempt's own error. `FailoverTrace::proves_not_processed()` says
  whether nothing was applied anywhere.
- `AttemptTrace` and `AttemptTrace::of(&response)`: every response `send`
  returns carries, in its extensions, the attempts of the call that asked for
  another try (endpoint, outcome, whether it rotated), with
  `proves_not_processed()`. When the budget runs out on an accepted status,
  that response is listed last, so a caller can prove no endpoint processed
  the request on the success path too. No existing signature changes.
- `spec/fixtures/endpoint-failover-v1.json`: the cross-language fixture for the
  failover rules, run on a virtual clock and over real HTTP.

### Changed

- A client with several endpoints that this crate builds follows redirects
  only to origins in the set. A supplied client that follows a redirect out of
  the set fails the call with `ClientError::Config`. Single-endpoint clients
  are unchanged.
- `ClientError::DeadlineExceeded` is documented as "the deadline expired;
  `attempts` requests were started; any of them may have been applied". Only
  `attempts == 0` guarantees nothing was sent. Its message now says which case
  applies. Behaviour is unchanged: it is still returned only with
  `attempts == 0`.

### Migration

- Purely additive. `ClientError` gains two variants, which a wildcard arm
  already covers, since it is `#[non_exhaustive]`. A client with one endpoint
  behaves exactly as 0.2.0.

## [0.2.0] - 2026-09-22

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
- `ServiceClientBuilder::attempt_timeout(Duration)`: a client-wide
  per-attempt timeout that works for a built client and for one supplied via
  `with_http_client`. Per-attempt precedence: the request's `.timeout()`, then
  `attempt_timeout`, then the builder's `timeout` (built client only). Under a
  deadline, whichever applies is clamped to the time remaining. With no
  deadline and neither override, no per-request timeout is set, as in 0.1.
- `RequestBuilder::timeout(Duration)`: a per-attempt timeout override.
- `RequestBuilder::deadline_at(Instant)`: an absolute deadline, so several
  sends of one operation share one budget.
- `ClientError::DeadlineExceeded { attempts, elapsed }`: the deadline expired
  after `attempts` requests were started, and any of them may have been
  applied; only `attempts == 0` guarantees nothing was sent. 0.2.0 returns it
  only with `attempts == 0`, when a call's deadline has passed before its
  first attempt (for example, an already-elapsed `deadline_at`). It is not
  retriable, and its message says to widen the deadline or re-drive the
  operation with the same idempotency identity. Once an attempt has gone out,
  running out of budget returns that attempt's own outcome instead.
- `Jitter` is re-exported at the crate root.

### Notes

- Under a deadline, a client supplied via `with_http_client` has its own
  timeout **replaced** on every attempt by the remaining budget. This crate
  cannot read that timeout, and reqwest applies one timeout per request. Set
  `attempt_timeout` (or a request `.timeout()`) to keep a tighter bound.
- A server `Retry-After` is honoured as in 0.1: not capped by `max_delay` and,
  without a deadline, not bounded at all: set a deadline to bound it.

### Changed

- **Breaking:** `RetryPolicy` is now `#[non_exhaustive]` and has two new
  fields (`deadline`, `jitter`). Callers outside the crate can no longer build
  it with a struct literal.
- **Breaking:** `ClientError` is now `#[non_exhaustive]` and has a new variant,
  `DeadlineExceeded`.

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

Reading the fields (`policy.max_attempts`, ...) is unchanged.

`ClientError` is now `#[non_exhaustive]`, so an exhaustive `match` on it needs
a wildcard arm (`_ => ...`). Handle `ClientError::DeadlineExceeded` explicitly
if you set a deadline.

Without a deadline, a server `Retry-After` is still honoured uncapped, as in
0.1.2: set a deadline to bound it. With the defaults
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
