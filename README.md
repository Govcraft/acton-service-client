# acton-service-client

A typed async HTTP client for services built on the
[`acton-service`](https://crates.io/crates/acton-service) framework.

`acton-service-client` is the **consumer-side counterpart** to `acton-service`.
Every service built on that framework shares a fixed set of wire conventions —
error-body shape, versioned route layout, health/readiness endpoints,
request-tracking headers, rate-limit signalling, and bearer auth. This crate
encodes those conventions once, as typed Rust, so downstream clients don't have
to reinvent them.

The mirrored types (`ErrorResponse`, `ApiVersion`, `HealthResponse`,
`ReadinessResponse`, `DependencyStatus`) are verified against the
`acton-service` 0.27 source and are `Serialize + Deserialize`, so they
round-trip against the genuine framework structs.

## Conventions it encodes

| Convention | acton-service source | Types here |
|------------|----------------------|------------|
| Error body `{error, code?, status}` | `error.rs::ErrorResponse` | `ErrorResponse`, `ApiError` |
| Error codes (SCREAMING_SNAKE) | `error.rs` (`NOT_FOUND`, `RATE_LIMIT_EXCEEDED`, `ACCOUNT_LOCKED`, …) | `ApiError::code` |
| Versioned routes `{base_path}/{version}` | `versioning.rs::ApiVersion` | `ApiVersion` (V1–V5) |
| `GET /health` (unversioned) | `health.rs::HealthResponse` | `HealthResponse` |
| `GET /ready` (unversioned) | `health.rs::ReadinessResponse` | `ReadinessResponse`, `DependencyStatus` |
| Request-tracking headers | `middleware/request_tracking.rs` | `RequestContext` (5-header set) |
| Rate limiting (`RateLimit-*` on 429) | rate-limit middleware | `RateLimitInfo` |
| `Retry-After` on 429 / 423 / 503 | `error.rs` (423 `Retry-After`) | `ApiError::retry_after` |
| Bearer auth (`Authorization: Bearer …`) | auth middleware | `ServiceClientBuilder::bearer_token` |

## Quickstart

```rust
use acton_service_client::{ApiVersion, RetryPolicy, ServiceClient};
use std::time::Duration;

#[derive(serde::Serialize, serde::Deserialize)]
struct User { id: u64, name: String }

# async fn run() -> Result<(), acton_service_client::ClientError> {
let client = ServiceClient::builder("https://api.example.com")
    .api_version(ApiVersion::V1)          // default V1
    .base_path("/api")                    // default "/api"
    .bearer_token("token")                // optional
    .timeout(Duration::from_secs(30))     // sane default
    .retry(RetryPolicy::default())        // optional; off by default
    .build()?;

let user: User = client.get("users/42").await?;                 // GET /api/v1/users/42
let created: User = client.post("users", &user).await?;         // 200 or 201
client.delete("users/42").await?;                               // 204 -> ()

let health = client.health().await?;                            // unversioned /health
let ready = client.ready().await?;                              // unversioned /ready
# let _ = (created, health, ready);
# Ok(())
# }
```

### Escape hatch

`client.request(method, path)` returns a `RequestBuilder` for query params,
extra headers, a per-request propagation `RequestContext`, a retriable override,
and additional accepted statuses:

```rust,no_run
# use acton_service_client::{RequestContext, ServiceClient};
# use reqwest::Method;
# async fn run(client: ServiceClient) -> Result<(), acton_service_client::ClientError> {
# #[derive(serde::Deserialize)] struct Page;
let page: Page = client
    .request(Method::GET, "users")
    .query("page", "2")
    .context(RequestContext::new().with_correlation_id("corr-123"))
    .send_json()
    .await?;
# let _ = page; Ok(())
# }
```

### Custom HTTP client (mutual TLS, proxies, pools)

For anything the builder does not surface — a client certificate for mutual
TLS, a custom root store, a proxy, or a shared connection pool — build a
`reqwest::Client` and hand it to `with_http_client`. This is how you pair the
crate with an `acton-service` listener that verifies client certificates: give
reqwest the client identity, then pass the client in. The `reqwest` crate is
re-exported at the crate root so the client you build matches the type the
builder expects.

```rust,no_run
# use acton_service_client::ServiceClient;
# fn make_tls_client() -> reqwest::Client { unimplemented!() }
# fn run() -> Result<(), acton_service_client::ClientError> {
let mtls: reqwest::Client = make_tls_client();  // use_rustls_tls() + Identity::from_pem(..)
let client = ServiceClient::builder("https://api.example.com")
    .bearer_token("token")          // still sent, per-request
    .with_http_client(mtls)
    .build()?;
# let _ = client; Ok(())
# }
```

`bearer_token`, `default_header` and `timeout` are applied per request, so
they hold with a supplied client too. The builder's `timeout` (30s unless
changed) replaces the supplied client's own on every attempt, so a client
built without one cannot hang a request forever. To keep the supplied
client's own timeout, opt out explicitly with `.no_timeout()`.

## Error handling

Every fallible call returns `ClientError`:

- **`Api(Box<ApiError>)`** — a non-success HTTP status. Carries the deserialized
  `ErrorResponse`, the `StatusCode`, any parsed `RateLimitInfo`, and any
  `Retry-After`. A body that is *not* valid `ErrorResponse` JSON is preserved as
  the error message (raw text) rather than lost — the status code is never
  dropped and the client never panics.
- **`Transport(reqwest::Error)`** — connection/TLS/timeout failures.
- **`Decode { status, snippet, source }`** — a success body that failed to
  deserialize; keeps a truncated snippet for diagnostics.
- **`Config(String)`** — builder-time validation (e.g. bad base URL).
- **`DeadlineExceeded { attempts, elapsed }`**: the call's deadline expired
  after `attempts` requests were started, and any of them may have been
  applied. Only `attempts == 0` guarantees nothing was sent (today the crate
  returns it only in that case). Not retriable.
- **`InvalidEndpoints(EndpointSetError)`**: the endpoint set was refused at
  build time (duplicate origin, mixed schemes, or a failover endpoint that is
  not a bare origin).
- **`EndpointsExhausted(Box<FailoverTrace>)`**: a call over an endpoint set ran
  out of budget after failing over. The trace lists every attempt (endpoint,
  outcome, whether it rotated) and the final attempt's own error. Not
  retriable.

`ClientError` is `#[non_exhaustive]`: match it with a wildcard arm.

`ApiError::is_retriable()` is true for `429`, `502`, `503`, `504`, and for `423`
only when a `Retry-After` was supplied.

## Retries

Retries are **off by default**. Configure a `RetryPolicy` to enable exponential
backoff (with a cap). Retries apply only to idempotent methods
(`GET`/`HEAD`/`DELETE`/`PUT`) plus any request explicitly marked
`.retriable(true)`. A server `Retry-After` is honored when present; otherwise
the pause comes from `RetryPolicy::pause`, a pure, unit-tested function of the
attempt, a random draw, and the time remaining.

```rust,no_run
# use acton_service_client::{Jitter, Method, RetryPolicy, ServiceClient, StatusCode};
# use std::time::Duration;
# async fn run() -> Result<(), acton_service_client::ClientError> {
# #[derive(serde::Deserialize)] struct Answer;
let client = ServiceClient::builder("https://api.example.com")
    .retry(
        RetryPolicy::default()
            .max_attempts(10)
            .jitter(Jitter::Full)                 // spread pauses over [base, ceiling]
            .deadline(Duration::from_secs(3)),    // total budget per call
    )
    .build()?;

let answer: Answer = client
    .request(Method::POST, "authorize")
    .retriable(true)                              // POST: explicit opt-in
    .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
    .timeout(Duration::from_millis(500))          // per attempt
    .send_json()
    .await?;
# let _ = answer; Ok(())
# }
```

- **Deadline.** `RetryPolicy::deadline` bounds a whole call from its first
  send. Each attempt's timeout is `min(timeout, remaining)`, and a pause (from
  backoff or `Retry-After`) that would reach the deadline is not taken: the last
  error or response is returned. `RequestBuilder::deadline_at(Instant)` sets an
  absolute deadline instead, so several sends of one operation share one budget.
  If the budget is already spent before anything is sent, the call fails with
  `ClientError::DeadlineExceeded` and the server never sees it. Without a
  deadline, `Retry-After` is honoured uncapped, as in 0.1: set a deadline to
  bound it.
- **Jitter.** `Jitter::Full` draws each pause uniformly from `[base_delay,
  ceiling]`. The floor at `base_delay` is deliberate: unlike textbook full
  jitter, no pause is ever near zero, so an always-failing upstream is never
  hammered in a tight loop.
- **Per-attempt timeout.** The request's `.timeout()` wins, then the client's
  `ServiceClientBuilder::attempt_timeout`, then the builder's `timeout`. Under a
  deadline, whichever applies is clamped to the time remaining. All three apply
  per request, on a built or supplied client alike. Only after `.no_timeout()`
  does an attempt carry none of its own; a deadline then bounds it by the time
  remaining.
- **Extra statuses.** `RequestBuilder::retry_on_status` extends the retriable
  set per request. It is checked before `accept_status`, so an accepted status
  listed for retry is retried first and still returned raw once retries run
  out. It only applies where retries do (idempotent method or `.retriable(true)`).

Defaults (no deadline, no jitter, no extra statuses) behave exactly as 0.1.

## Endpoint failover

Give a client an ordered set of endpoints serving the same API, and one
deadline covers every attempt on every endpoint:

```rust,no_run
# use acton_service_client::{
#     ClientError, EndpointOrigin, Method, RetryObserver, RetryPolicy, RetryReason, ServiceClient,
#     StatusCode,
# };
# use std::time::Duration;
# async fn run() -> Result<(), ClientError> {
struct Log;

impl RetryObserver for Log {
    fn on_rotation(&self, left: &EndpointOrigin, reason: RetryReason) {
        eprintln!("left {left}: {}", reason.label()); // or count it as a metric
    }
}

let client = ServiceClient::builder("https://replica-a.example.com")
    .failover_endpoints(["https://replica-b.example.com", "https://replica-c.example.com"])
    .attempt_timeout(Duration::from_secs(5))
    .retry(RetryPolicy::default().max_attempts(u32::MAX).deadline(Duration::from_secs(15)))
    .retry_observer(Log)
    .build()?;

match client
    .request(Method::POST, "authorize")
    .retriable(true)
    .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
    .send()
    .await
{
    Err(ClientError::EndpointsExhausted(trace)) if trace.proves_not_processed() => {
        // Every endpoint refused it (421 or connect): nothing was applied.
    }
    other => { let _ = other?; }
}
# Ok(())
# }
```

- **Validated at build.** Failover endpoints are bare origins
  (`scheme://host[:port]`); each request reuses the base URL's path. Duplicate
  origins (after normalization) and mixed schemes are a typed
  `ClientError::InvalidEndpoints`, never a panic. `Endpoint::with_http_client`
  gives one endpoint its own client (for example its own TLS configuration).
- **Retriable requests only.** Failover follows the retry rules: idempotent
  methods, or `.retriable(true)`. A request that is not retried goes to one
  endpoint only, exactly as without a set.
- **Rotation.** A `retry_on_status` status, a connect failure, or an attempt
  timeout moves the next attempt to the next endpoint at once. After a full
  cycle, the client pauses with the policy's backoff (or the smallest
  `Retry-After`, when every endpoint in the cycle sent one). Statuses retriable
  by default but not listed (`429`, `502`, `503`, `504`) retry the same
  endpoint, and every other answer is returned, as before. A transport failure
  after the request was written (a reset mid-body) is not a connect failure:
  it is returned as `ClientError::Transport` and never retried, exactly as in
  0.2.0, since the endpoint may have processed it and a patch release must not
  change what a single-endpoint caller sees. Treat it as ambiguous and re-send
  with the same idempotency identity.
- **Sticky.** The next call starts at the endpoint that last gave a definitive
  answer (any status that is not a rotation status, a `4xx` included).
- **Stays in the set.** A built client follows redirects only within the set;
  a supplied client that follows one out of it fails with `ClientError::Config`.
- **Diagnosable.** Running out of budget after a rotation returns
  `EndpointsExhausted` with the trace. Every response `send` returns carries
  its own trace too, read with `AttemptTrace::of(&response)`: the attempts
  that asked for another try, ending with the response itself when the budget
  ran out on an accepted status. `RetryReason::proves_not_processed`
  is true only for a connect failure and `421`.
- **Metrics.** A `RetryObserver` hears of every re-send: `on_rotation` when
  the call moves to the next endpoint, `on_retry` when it re-sends to the same
  one (so a single-endpoint client answering `421` is visible too). Both
  default to doing nothing, and the crate has no metrics dependency. In an
  `acton-service` application, count them on
  `acton_service::observability::get_meter()` as
  `acton_service_client.endpoint.rotations{reason}` and
  `acton_service_client.endpoint.retries{reason}`, with `reason.label()`.
  The observer runs synchronously on the caller's task.
- **Per request.** `RequestBuilder::retry_observer` installs an observer on
  one request, called in addition to the client's (the client's first). A
  request's observer hears exactly that request's re-sends, every attempt but
  the last, in order, with its outcome; the request's result is the last.
  That is the complete per-attempt record of one call, on the success path
  and under concurrency. The client's observer hears the union of every
  request's re-sends: in order within each request, with no order promised
  across concurrent requests, which is right for counters and wrong for
  deciding what happened to one call. To derive a per-call answer (such as
  "not submitted anywhere"), install a fresh collector on that request.
- **Where the record lives.** An `Ok` response carries its attempts in
  `AttemptTrace::of(&response)`. `EndpointsExhausted` carries every attempt in
  its `FailoverTrace`. `DeadlineExceeded` and every other error (a single
  endpoint's, or a call that never rotated, returned exactly as in 0.2.0) carry
  no trace: the request's own observer is the record there, and it covers the
  other paths too.
- **Parity.** `spec/fixtures/endpoint-failover-v1.json` pins the rules for
  every port; the Rust crate runs it on a virtual clock and over real HTTP.

A client with one endpoint behaves exactly as 0.2.0.

## Feature table

| Area | What you get |
|------|--------------|
| Transport | `reqwest` with `rustls` TLS (no OpenSSL), JSON, and query support |
| Verbs | `get`, `post`, `put`, `patch`, `delete`, plus `request` / `request_unversioned` |
| Versioning | `ApiVersion` V1–V5, path segment + parsing that agrees with `acton-service` |
| Health | `health()` and `ready()` against the unversioned endpoints |
| Tracking | `RequestContext` for the five propagation headers; auto `x-request-id` (UUID v4) |
| Rate limits | `RateLimitInfo` surfaced on `ApiError` |
| Auth | Bearer tokens (JWT or PASETO, opaque to the client) |
| Retries | Opt-in `RetryPolicy`: pure backoff math, floored jitter, total deadline, per-request extra statuses and timeouts |
| Failover | Ordered endpoint set under one deadline: validated at build, sticky, in-set redirects only, typed trace on exhaustion, retry observer |

## Development

```sh
cargo fmt --all
cargo clippy --all-targets -- -D warnings   # zero warnings
cargo nextest run                           # unit + integration
cargo test --doc                            # doctests
cargo doc --no-deps                         # warning-free
```

Integration tests exercise a real HTTP round-trip against an ephemeral
plain-`axum` server that reproduces the `acton-service` wire shapes
byte-for-byte (see `tests/integration.rs` for the rationale behind the fake vs.
a genuine `acton-service` router).

## License

MIT
