//! Error types mirroring the `acton-service` error wire contract.
//!
//! Every non-success response from an `acton-service` deployment carries a JSON
//! body of shape `{"error": String, "code": Option<String>, "status": u16}`
//! ([`ErrorResponse`]) emitted with the matching HTTP status. This module maps
//! those responses — plus transport, decode, and configuration failures — into
//! a single [`ClientError`] enum, and exposes rate-limit and retry metadata on
//! [`ApiError`].
//!
//! Bodies that are *not* valid [`ErrorResponse`] JSON never cause a panic or a
//! lost status code: they are synthesized into an [`ErrorResponse`] whose
//! `error` field holds the raw response text.

use reqwest::StatusCode;
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Header name for the rate-limit ceiling.
pub const RATELIMIT_LIMIT: &str = "RateLimit-Limit";
/// Header name for the remaining rate-limit budget.
pub const RATELIMIT_REMAINING: &str = "RateLimit-Remaining";
/// Header name for the rate-limit reset (seconds until the window resets).
pub const RATELIMIT_RESET: &str = "RateLimit-Reset";
/// Standard `Retry-After` header (delta-seconds form).
pub const RETRY_AFTER: &str = "Retry-After";

/// Deserialized error body emitted by `acton-service`.
///
/// Mirrors `acton_service::error::ErrorResponse`. `code` is omitted from the
/// wire form when `None`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorResponse {
    /// Human-readable error message.
    pub error: String,
    /// Machine-readable SCREAMING_SNAKE code, when present (e.g. `CONFIG_ERROR`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// HTTP status code echoed in the body.
    pub status: u16,
}

impl ErrorResponse {
    /// Build an [`ErrorResponse`] from a raw body that is not valid
    /// `ErrorResponse` JSON, preserving the transport status and raw text.
    #[must_use]
    pub fn from_raw(status: StatusCode, raw_body: impl Into<String>) -> Self {
        Self {
            error: raw_body.into(),
            code: None,
            status: status.as_u16(),
        }
    }
}

/// Rate-limit metadata parsed from `RateLimit-*` response headers.
///
/// Each field is optional because a server may emit any subset of the headers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RateLimitInfo {
    /// Value of `RateLimit-Limit` (request ceiling for the window).
    pub limit: Option<u64>,
    /// Value of `RateLimit-Remaining` (requests left in the window).
    pub remaining: Option<u64>,
    /// Value of `RateLimit-Reset` (seconds until the window resets).
    pub reset: Option<u64>,
}

impl RateLimitInfo {
    /// Returns `true` when at least one field was populated.
    #[must_use]
    pub fn is_present(&self) -> bool {
        self.limit.is_some() || self.remaining.is_some() || self.reset.is_some()
    }
}

/// A structured API error: the deserialized (or synthesized) body plus the HTTP
/// status and any rate-limit / retry metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    /// The HTTP status of the failing response.
    pub status: StatusCode,
    /// The parsed error body.
    pub body: ErrorResponse,
    /// Rate-limit metadata, when the response carried `RateLimit-*` headers.
    pub rate_limit: Option<RateLimitInfo>,
    /// Server-directed retry delay from a `Retry-After` header, when present.
    pub retry_after: Option<Duration>,
}

impl ApiError {
    /// The HTTP status code of the error.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// The machine-readable error code, when the body carried one.
    #[must_use]
    pub fn code(&self) -> Option<&str> {
        self.body.code.as_deref()
    }

    /// The human-readable error message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.body.error
    }

    /// Rate-limit metadata, when available.
    #[must_use]
    pub fn rate_limit(&self) -> Option<RateLimitInfo> {
        self.rate_limit
    }

    /// Server-directed retry delay, when a `Retry-After` header was present.
    #[must_use]
    pub fn retry_after(&self) -> Option<Duration> {
        self.retry_after
    }

    /// Whether this error is worth retrying.
    ///
    /// True for `429`, `502`, `503`, `504`, and for `423` **only** when the
    /// server supplied a `Retry-After` delay.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{ApiError, ErrorResponse};
    /// use reqwest::StatusCode;
    /// use std::time::Duration;
    ///
    /// let mut e = ApiError {
    ///     status: StatusCode::SERVICE_UNAVAILABLE,
    ///     body: ErrorResponse::from_raw(StatusCode::SERVICE_UNAVAILABLE, "down"),
    ///     rate_limit: None,
    ///     retry_after: None,
    /// };
    /// assert!(e.is_retriable());
    ///
    /// e.status = StatusCode::LOCKED;
    /// assert!(!e.is_retriable());          // 423 without Retry-After
    /// e.retry_after = Some(Duration::from_secs(30));
    /// assert!(e.is_retriable());           // 423 with Retry-After
    /// ```
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self.status {
            StatusCode::TOO_MANY_REQUESTS
            | StatusCode::BAD_GATEWAY
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => true,
            StatusCode::LOCKED => self.retry_after.is_some(),
            _ => false,
        }
    }
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.code() {
            Some(code) => write!(
                f,
                "{} {} [{}]: {}",
                self.status.as_u16(),
                status_reason(self.status),
                code,
                self.message()
            ),
            None => write!(
                f,
                "{} {}: {}",
                self.status.as_u16(),
                status_reason(self.status),
                self.message()
            ),
        }
    }
}

impl std::error::Error for ApiError {}

fn status_reason(status: StatusCode) -> &'static str {
    status.canonical_reason().unwrap_or("Unknown")
}

/// The top-level error type returned by every fallible client operation.
#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The server returned a non-success HTTP status.
    ///
    /// Boxed to keep [`ClientError`] small on the success path.
    #[error(transparent)]
    Api(Box<ApiError>),

    /// A transport-level failure (connection, TLS, timeout, redirect, …).
    #[error("transport error: {0}")]
    Transport(#[from] reqwest::Error),

    /// A success response whose body failed to deserialize into the expected type.
    #[error("failed to decode {status} response body: {source} (snippet: {snippet:?})")]
    Decode {
        /// The HTTP status of the response whose body could not be decoded.
        status: StatusCode,
        /// A truncated snippet of the offending body, for diagnostics.
        snippet: String,
        /// The underlying deserialization error.
        source: serde_json::Error,
    },

    /// A builder-time configuration error (e.g. an invalid base URL).
    #[error("configuration error: {0}")]
    Config(String),
}

impl From<ApiError> for ClientError {
    fn from(error: ApiError) -> Self {
        Self::Api(Box::new(error))
    }
}

impl ClientError {
    /// Borrow the inner [`ApiError`], if this is an API error.
    #[must_use]
    pub fn as_api(&self) -> Option<&ApiError> {
        match self {
            Self::Api(e) => Some(e.as_ref()),
            _ => None,
        }
    }

    /// Whether this error is worth retrying.
    ///
    /// Transport errors that are timeouts or connection failures are retriable;
    /// API errors defer to [`ApiError::is_retriable`]; decode and config errors
    /// are never retriable.
    #[must_use]
    pub fn is_retriable(&self) -> bool {
        match self {
            Self::Api(e) => e.is_retriable(),
            Self::Transport(e) => e.is_timeout() || e.is_connect(),
            Self::Decode { .. } | Self::Config(_) => false,
        }
    }
}

/// Maximum number of body bytes retained in a [`ClientError::Decode`] snippet.
pub(crate) const SNIPPET_LEN: usize = 512;

/// Truncate a body to at most [`SNIPPET_LEN`] characters for diagnostics.
#[must_use]
pub(crate) fn snippet(body: &str) -> String {
    if body.len() <= SNIPPET_LEN {
        return body.to_string();
    }
    let mut end = SNIPPET_LEN;
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &body[..end])
}

/// Parse a single unsigned-integer header value.
fn parse_u64_header(headers: &HeaderMap, name: &str) -> Option<u64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// Parse `RateLimit-*` headers into a [`RateLimitInfo`].
///
/// Returns `None` when none of the three headers are present or parseable.
///
/// # Examples
///
/// ```
/// use acton_service_client::error::parse_rate_limit;
/// use reqwest::header::{HeaderMap, HeaderValue};
///
/// let mut h = HeaderMap::new();
/// h.insert("RateLimit-Limit", HeaderValue::from_static("100"));
/// h.insert("RateLimit-Remaining", HeaderValue::from_static("0"));
/// h.insert("RateLimit-Reset", HeaderValue::from_static("42"));
/// let info = parse_rate_limit(&h).unwrap();
/// assert_eq!(info.limit, Some(100));
/// assert_eq!(info.remaining, Some(0));
/// assert_eq!(info.reset, Some(42));
/// ```
#[must_use]
pub fn parse_rate_limit(headers: &HeaderMap) -> Option<RateLimitInfo> {
    let info = RateLimitInfo {
        limit: parse_u64_header(headers, RATELIMIT_LIMIT),
        remaining: parse_u64_header(headers, RATELIMIT_REMAINING),
        reset: parse_u64_header(headers, RATELIMIT_RESET),
    };
    info.is_present().then_some(info)
}

/// Parse a `Retry-After` header expressed in delta-seconds.
///
/// Only the numeric (seconds) form is recognized; HTTP-date forms yield `None`.
///
/// # Examples
///
/// ```
/// use acton_service_client::error::parse_retry_after;
/// use reqwest::header::{HeaderMap, HeaderValue};
/// use std::time::Duration;
///
/// let mut h = HeaderMap::new();
/// h.insert("Retry-After", HeaderValue::from_static("30"));
/// assert_eq!(parse_retry_after(&h), Some(Duration::from_secs(30)));
/// ```
#[must_use]
pub fn parse_retry_after(headers: &HeaderMap) -> Option<Duration> {
    parse_u64_header(headers, RETRY_AFTER).map(Duration::from_secs)
}

/// Build an [`ApiError`] from a failing response's status, headers, and raw body.
///
/// If `body` deserializes as [`ErrorResponse`] it is used verbatim; otherwise a
/// synthesized body is produced from the raw text so the status code and
/// message are never lost. This is a pure function.
#[must_use]
pub fn build_api_error(status: StatusCode, headers: &HeaderMap, body: &str) -> ApiError {
    let parsed = serde_json::from_str::<ErrorResponse>(body).ok();
    let error_body = parsed.unwrap_or_else(|| ErrorResponse::from_raw(status, body));
    ApiError {
        status,
        body: error_body,
        rate_limit: parse_rate_limit(headers),
        retry_after: parse_retry_after(headers),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                reqwest::header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    #[test]
    fn error_response_roundtrips_and_skips_none_code() {
        let e = ErrorResponse {
            error: "not found".into(),
            code: None,
            status: 404,
        };
        let json = serde_json::to_string(&e).unwrap();
        assert_eq!(json, r#"{"error":"not found","status":404}"#);
        let back: ErrorResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(e, back);
    }

    #[test]
    fn build_api_error_parses_valid_body() {
        let body = r#"{"error":"missing config","code":"CONFIG_ERROR","status":500}"#;
        let e = build_api_error(StatusCode::INTERNAL_SERVER_ERROR, &HeaderMap::new(), body);
        assert_eq!(e.code(), Some("CONFIG_ERROR"));
        assert_eq!(e.message(), "missing config");
        assert_eq!(e.status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[test]
    fn build_api_error_synthesizes_from_non_json() {
        let body = "<html>502 Bad Gateway</html>";
        let e = build_api_error(StatusCode::BAD_GATEWAY, &HeaderMap::new(), body);
        assert_eq!(e.code(), None);
        assert_eq!(e.message(), body);
        assert_eq!(e.body.status, 502);
        assert!(e.is_retriable());
    }

    #[test]
    fn rate_limit_headers_parsed() {
        let h = headers(&[
            ("RateLimit-Limit", "100"),
            ("RateLimit-Remaining", "3"),
            ("RateLimit-Reset", "60"),
        ]);
        let info = parse_rate_limit(&h).unwrap();
        assert_eq!(info.limit, Some(100));
        assert_eq!(info.remaining, Some(3));
        assert_eq!(info.reset, Some(60));
    }

    #[test]
    fn rate_limit_absent_is_none() {
        assert!(parse_rate_limit(&HeaderMap::new()).is_none());
    }

    #[test]
    fn rate_limit_partial_is_some() {
        let h = headers(&[("RateLimit-Remaining", "0")]);
        let info = parse_rate_limit(&h).unwrap();
        assert_eq!(info.remaining, Some(0));
        assert_eq!(info.limit, None);
    }

    #[test]
    fn retry_after_numeric_only() {
        let h = headers(&[("Retry-After", "12")]);
        assert_eq!(parse_retry_after(&h), Some(Duration::from_secs(12)));
        let bad = headers(&[("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT")]);
        assert_eq!(parse_retry_after(&bad), None);
    }

    #[test]
    fn locked_retriable_only_with_retry_after() {
        let h = headers(&[("Retry-After", "30")]);
        let e = build_api_error(
            StatusCode::LOCKED,
            &h,
            r#"{"error":"locked","code":"ACCOUNT_LOCKED","status":423}"#,
        );
        assert_eq!(e.retry_after, Some(Duration::from_secs(30)));
        assert!(e.is_retriable());

        let e2 = build_api_error(
            StatusCode::LOCKED,
            &HeaderMap::new(),
            r#"{"error":"locked","status":423}"#,
        );
        assert!(!e2.is_retriable());
    }

    #[test]
    fn non_retriable_statuses() {
        for s in [
            StatusCode::NOT_FOUND,
            StatusCode::BAD_REQUEST,
            StatusCode::UNAUTHORIZED,
            StatusCode::CONFLICT,
        ] {
            let e = build_api_error(s, &HeaderMap::new(), "{}");
            assert!(!e.is_retriable(), "{s} should not be retriable");
        }
    }

    #[test]
    fn snippet_truncates_on_char_boundary() {
        let long = "é".repeat(400); // 800 bytes, > SNIPPET_LEN
        let s = snippet(&long);
        assert!(s.len() <= SNIPPET_LEN + 4);
        assert!(s.ends_with('…'));
    }

    #[test]
    fn snippet_passthrough_when_short() {
        assert_eq!(snippet("short"), "short");
    }

    #[test]
    fn display_includes_code_and_status() {
        let e = build_api_error(
            StatusCode::NOT_FOUND,
            &HeaderMap::new(),
            r#"{"error":"gone","code":"NOT_FOUND","status":404}"#,
        );
        let s = e.to_string();
        assert!(s.contains("404"));
        assert!(s.contains("NOT_FOUND"));
        assert!(s.contains("gone"));
    }
}
