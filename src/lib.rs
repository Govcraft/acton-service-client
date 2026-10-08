//! Typed async HTTP client for services built on the
//! [`acton-service`](https://crates.io/crates/acton-service) framework.
//!
//! `acton-service-client` is the consumer-side counterpart to `acton-service`.
//! Services built on that framework share fixed wire conventions; this crate
//! encodes those conventions once — as typed Rust — so downstream clients
//! don't reinvent them.
//!
//! # What it mirrors
//!
//! | Convention | Type(s) here |
//! |------------|--------------|
//! | Error body `{error, code?, status}` | [`ErrorResponse`], [`ApiError`] |
//! | Versioned routes `{base_path}/{version}` | [`ApiVersion`] |
//! | `GET /health`, `GET /ready` | [`HealthResponse`], [`ReadinessResponse`], [`DependencyStatus`] |
//! | Request-tracking headers | [`RequestContext`] |
//! | Rate-limit `RateLimit-*` headers | [`RateLimitInfo`] |
//! | `Retry-After` on `429`/`423`/`503` | [`ApiError::retry_after`] |
//! | Bearer auth (`Authorization: Bearer …`) | [`ServiceClientBuilder::bearer_token`] |
//!
//! # Quickstart
//!
//! ```no_run
//! use acton_service_client::{ApiVersion, RetryPolicy, ServiceClient};
//! use std::time::Duration;
//!
//! # #[derive(serde::Serialize, serde::Deserialize)]
//! # struct User { id: u64, name: String }
//! # async fn run() -> Result<(), acton_service_client::ClientError> {
//! let client = ServiceClient::builder("https://api.example.com")
//!     .api_version(ApiVersion::V1)          // default V1
//!     .base_path("/api")                    // default "/api"
//!     .bearer_token("token")                // optional
//!     .timeout(Duration::from_secs(30))     // sane default
//!     .retry(RetryPolicy::default())        // optional; off by default
//!     .build()?;
//!
//! let user: User = client.get("users/42").await?;
//! let new_user = User { id: 0, name: "Ada".into() };
//! let created: User = client.post("users", &new_user).await?;
//! client.delete("users/42").await?;         // 204 -> ()
//!
//! let health = client.health().await?;      // unversioned /health
//! let ready = client.ready().await?;        // unversioned /ready
//! # let _ = (user, created, health, ready);
//! # Ok(())
//! # }
//! ```
//!
//! # Error handling
//!
//! Every fallible call returns [`ClientError`]. Non-success HTTP responses
//! become [`ClientError::Api`] carrying the deserialized [`ErrorResponse`], the
//! [`StatusCode`], and any parsed [`RateLimitInfo`] /
//! `Retry-After`. A body that is *not* valid `ErrorResponse` JSON is preserved
//! as the error message rather than lost. Use [`ApiError::is_retriable`] and
//! [`ApiError::code`] to branch on the failure.
//!
//! # Retries
//!
//! Retries are off unless a [`RetryPolicy`] is configured, and then apply only
//! to idempotent methods (`GET`/`HEAD`/`DELETE`/`PUT`) plus requests explicitly
//! marked with [`RequestBuilder::retriable`]. A server `Retry-After` is honored
//! when present; otherwise the pause is computed by [`RetryPolicy::pause`]: the
//! [`RetryPolicy::backoff_delay`] ceiling, spread by [`Jitter`] when enabled.
//!
//! A [`RetryPolicy::deadline`] bounds a whole call, and
//! [`RequestBuilder::deadline_at`] lets several calls share one absolute budget.
//! [`RequestBuilder::retry_on_status`] and [`RequestBuilder::timeout`] adjust
//! the retriable statuses and the per-attempt timeout for one request.
//!
//! ```
//! use acton_service_client::{Jitter, RetryPolicy, ServiceClient};
//! use std::time::Duration;
//!
//! let client = ServiceClient::builder("https://api.example.com")
//!     .retry(
//!         RetryPolicy::default()
//!             .max_attempts(10)
//!             .jitter(Jitter::Full)
//!             .deadline(Duration::from_secs(3)),
//!     )
//!     .build()
//!     .expect("valid base url");
//! # let _ = client;
//! ```
//!
//! # Endpoint failover
//!
//! A client can hold an ordered set of endpoints serving the same API: the
//! base URL, then each [`ServiceClientBuilder::failover_endpoint`]. When
//! retries apply to a request, a listed [`RequestBuilder::retry_on_status`]
//! (such as `421 Misdirected Request`), a connect failure, or an attempt
//! timeout moves the next attempt to the next endpoint at once, all under one
//! deadline. After a full cycle the client backs off with the
//! [`RetryPolicy`]. The next call starts at the endpoint that last answered
//! ([`ServiceClient::preferred_endpoint`]). A call that runs out of budget after
//! failing over returns [`ClientError::EndpointsExhausted`] with a
//! [`FailoverTrace`] of every attempt (a returned response carries its own
//! [`AttemptTrace`]), and [`RetryReason::proves_not_processed`]
//! tells a caller whether anything may have been applied. A
//! [`RetryObserver`] hears of every re-send, rotation or same-endpoint retry,
//! and is the metrics seam. A client with one endpoint behaves
//! exactly as before. See the [`failover`] module for the full rules.
//!
//! ```
//! use acton_service_client::{RetryPolicy, ServiceClient};
//! use std::time::Duration;
//!
//! let client = ServiceClient::builder("https://replica-a.example.com")
//!     .failover_endpoints(["https://replica-b.example.com", "https://replica-c.example.com"])
//!     .attempt_timeout(Duration::from_secs(5))
//!     .retry(RetryPolicy::default().deadline(Duration::from_secs(15)))
//!     .build()
//!     .expect("a valid endpoint set");
//! assert_eq!(client.endpoints().len(), 3);
//! ```
//!
//! # Custom HTTP client (mutual TLS, proxies, pools)
//!
//! For anything the builder does not surface — a client certificate for mutual
//! TLS, a custom root store, a proxy, or a shared connection pool — build a
//! [`reqwest::Client`] and pass it to
//! [`ServiceClientBuilder::with_http_client`]. The `reqwest` crate is
//! re-exported at the crate root so the client you construct matches the type
//! the builder expects. [`bearer_token`](ServiceClientBuilder::bearer_token)
//! and [`default_header`](ServiceClientBuilder::default_header) are sent
//! per-request, so they keep working with a supplied client.
//!
//! # Cargo features
//!
//! - **`transport`** (default): the HTTP client and everything it sends and
//!   reads: [`ServiceClient`], [`RequestBuilder`], endpoint failover, the
//!   error, health, versioning and request-tracking types, and the
//!   [`reqwest`] re-export. It brings in `reqwest` and `tokio`.
//!
//! With `default-features = false` the crate is the [`retry`] module alone:
//! [`RetryPolicy`], [`Jitter`], [`retry::is_idempotent`], and the decision the
//! client's send loop makes about a response that is not a success
//! ([`retry::parse_retry_after_value`], [`retry::wants_retry`],
//! [`RetryPolicy::next_pause`]), plus the [`Method`] and [`StatusCode`] types
//! they take. None of it sends, reads a clock or draws randomness: the caller
//! supplies the time remaining and the jitter draw. It builds for
//! `wasm32-unknown-unknown`, so a sans-IO caller that does its own sending,
//! such as a state machine in a browser, retries by exactly the client's
//! rules.
//!
//! ```toml
//! [dependencies]
//! acton-service-client = { version = "0.3", default-features = false }
//! ```

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(clippy::all)]

#[cfg(feature = "transport")]
pub mod context;
#[cfg(feature = "transport")]
pub mod error;
#[cfg(feature = "transport")]
pub mod failover;
#[cfg(feature = "transport")]
pub mod health;
pub mod retry;
#[cfg(feature = "transport")]
pub mod url;
#[cfg(feature = "transport")]
pub mod versioning;

#[cfg(feature = "transport")]
mod client;
#[cfg(feature = "transport")]
mod request;

// Lets the fixture model shared with the integration tests name this crate.
#[cfg(test)]
extern crate self as acton_service_client;

#[cfg(feature = "transport")]
pub use client::{ServiceClient, ServiceClientBuilder};
#[cfg(feature = "transport")]
pub use context::{
    PROPAGATED_HEADERS, RequestContext, X_CLIENT_ID, X_CORRELATION_ID, X_REQUEST_ID, X_SPAN_ID,
    X_TRACE_ID,
};
#[cfg(feature = "transport")]
pub use error::{ApiError, ClientError, ErrorResponse, RateLimitInfo};
#[cfg(feature = "transport")]
pub use failover::{
    AttemptTrace, Endpoint, EndpointOrigin, EndpointSetError, FailoverTrace, RetryObserver,
    RetryReason, TracedAttempt,
};
#[cfg(feature = "transport")]
pub use health::{DependencyStatus, HealthResponse, ReadinessResponse};
#[cfg(feature = "transport")]
pub use request::RequestBuilder;
pub use retry::{Jitter, RetryPolicy};
#[cfg(feature = "transport")]
pub use versioning::ApiVersion;

/// Re-export of the `http` crate's `Method`, the type `reqwest` uses.
pub use http::Method;
/// Re-export of the `http` crate's `StatusCode`, the type `reqwest` uses.
pub use http::StatusCode;

/// Re-export of the `reqwest` crate.
///
/// Build a client against this exact version — with a client certificate, a
/// custom root store, or a proxy — and hand it to
/// [`ServiceClientBuilder::with_http_client`] without risking a version
/// mismatch on the `reqwest::Client` type.
#[cfg(feature = "transport")]
pub use reqwest;
