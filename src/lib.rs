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
//! when present; otherwise the delay is computed by
//! [`RetryPolicy::backoff_delay`].
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

#![deny(missing_docs)]
#![deny(rustdoc::broken_intra_doc_links)]
#![warn(clippy::all)]

pub mod context;
pub mod error;
pub mod health;
pub mod retry;
pub mod url;
pub mod versioning;

mod client;
mod request;

pub use client::{ServiceClient, ServiceClientBuilder};
pub use context::{
    PROPAGATED_HEADERS, RequestContext, X_CLIENT_ID, X_CORRELATION_ID, X_REQUEST_ID, X_SPAN_ID,
    X_TRACE_ID,
};
pub use error::{ApiError, ClientError, ErrorResponse, RateLimitInfo};
pub use health::{DependencyStatus, HealthResponse, ReadinessResponse};
pub use request::RequestBuilder;
pub use retry::RetryPolicy;
pub use versioning::ApiVersion;

/// Re-export of `reqwest`'s `Method` for convenience.
pub use reqwest::Method;
/// Re-export of `reqwest`'s `StatusCode` for convenience.
pub use reqwest::StatusCode;

/// Re-export of the `reqwest` crate.
///
/// Build a client against this exact version — with a client certificate, a
/// custom root store, or a proxy — and hand it to
/// [`ServiceClientBuilder::with_http_client`] without risking a version
/// mismatch on the `reqwest::Client` type.
pub use reqwest;
