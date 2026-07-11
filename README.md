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

`ApiError::is_retriable()` is true for `429`, `502`, `503`, `504`, and for `423`
only when a `Retry-After` was supplied.

## Retries

Retries are **off by default**. Configure a `RetryPolicy` to enable exponential
backoff (with a cap). Retries apply only to idempotent methods
(`GET`/`HEAD`/`DELETE`/`PUT`) plus any request explicitly marked
`.retriable(true)`. A server `Retry-After` is honored when present; otherwise
the delay comes from `RetryPolicy::backoff_delay`, a pure, unit-tested function.

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
| Retries | Opt-in `RetryPolicy` with pure backoff math |

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
