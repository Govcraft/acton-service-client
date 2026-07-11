//! Request-tracking context propagation.
//!
//! `acton-service` propagates a fixed set of tracking headers across service
//! boundaries: `x-request-id`, `x-trace-id`, `x-span-id`, `x-correlation-id`,
//! and `x-client-id`. [`RequestContext`] models that set and converts to and
//! from a [`reqwest::header::HeaderMap`].
//!
//! The client always sends an `x-request-id` (a fresh UUID v4) when the caller
//! has not supplied one; see [`RequestContext::ensure_request_id`].
//!
//! # Examples
//!
//! ```
//! use acton_service_client::RequestContext;
//!
//! let ctx = RequestContext::new()
//!     .with_request_id("req-123")
//!     .with_correlation_id("corr-abc");
//!
//! let headers = ctx.to_headers();
//! assert_eq!(headers.get("x-request-id").unwrap(), "req-123");
//! assert_eq!(headers.get("x-correlation-id").unwrap(), "corr-abc");
//! ```

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

/// Header carrying the per-request identifier.
pub const X_REQUEST_ID: &str = "x-request-id";
/// Header carrying the distributed trace identifier.
pub const X_TRACE_ID: &str = "x-trace-id";
/// Header carrying the current span identifier.
pub const X_SPAN_ID: &str = "x-span-id";
/// Header carrying a cross-service correlation identifier.
pub const X_CORRELATION_ID: &str = "x-correlation-id";
/// Header carrying the originating client identifier.
pub const X_CLIENT_ID: &str = "x-client-id";

/// The exact set of tracking headers `acton-service` propagates, in order.
pub const PROPAGATED_HEADERS: [&str; 5] = [
    X_REQUEST_ID,
    X_TRACE_ID,
    X_SPAN_ID,
    X_CORRELATION_ID,
    X_CLIENT_ID,
];

/// A propagation context carrying the five `acton-service` tracking headers.
///
/// Every field is optional; only populated fields are emitted as headers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RequestContext {
    /// Value for `x-request-id`.
    pub request_id: Option<String>,
    /// Value for `x-trace-id`.
    pub trace_id: Option<String>,
    /// Value for `x-span-id`.
    pub span_id: Option<String>,
    /// Value for `x-correlation-id`.
    pub correlation_id: Option<String>,
    /// Value for `x-client-id`.
    pub client_id: Option<String>,
}

impl RequestContext {
    /// Create an empty context.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the `x-request-id` value.
    #[must_use]
    pub fn with_request_id(mut self, value: impl Into<String>) -> Self {
        self.request_id = Some(value.into());
        self
    }

    /// Set the `x-trace-id` value.
    #[must_use]
    pub fn with_trace_id(mut self, value: impl Into<String>) -> Self {
        self.trace_id = Some(value.into());
        self
    }

    /// Set the `x-span-id` value.
    #[must_use]
    pub fn with_span_id(mut self, value: impl Into<String>) -> Self {
        self.span_id = Some(value.into());
        self
    }

    /// Set the `x-correlation-id` value.
    #[must_use]
    pub fn with_correlation_id(mut self, value: impl Into<String>) -> Self {
        self.correlation_id = Some(value.into());
        self
    }

    /// Set the `x-client-id` value.
    #[must_use]
    pub fn with_client_id(mut self, value: impl Into<String>) -> Self {
        self.client_id = Some(value.into());
        self
    }

    /// Ensure a `request_id` is present, generating a UUID v4 if absent.
    ///
    /// Returns the effective request id.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::RequestContext;
    ///
    /// let mut ctx = RequestContext::new();
    /// let id = ctx.ensure_request_id();
    /// assert_eq!(ctx.request_id.as_deref(), Some(id.as_str()));
    /// ```
    pub fn ensure_request_id(&mut self) -> String {
        if self.request_id.is_none() {
            self.request_id = Some(uuid::Uuid::new_v4().to_string());
        }
        // Safe: just set above when absent.
        self.request_id.clone().unwrap_or_default()
    }

    /// Reconstruct a context from a header map, reading only the propagated set.
    ///
    /// Header values that are not valid UTF-8 are ignored.
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let read = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        Self {
            request_id: read(X_REQUEST_ID),
            trace_id: read(X_TRACE_ID),
            span_id: read(X_SPAN_ID),
            correlation_id: read(X_CORRELATION_ID),
            client_id: read(X_CLIENT_ID),
        }
    }

    /// Build a header map containing only the populated fields.
    ///
    /// Values that cannot be encoded as header values are skipped.
    #[must_use]
    pub fn to_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        self.apply_to(&mut headers);
        headers
    }

    /// Insert this context's populated fields into an existing header map.
    pub fn apply_to(&self, headers: &mut HeaderMap) {
        let pairs = [
            (X_REQUEST_ID, self.request_id.as_deref()),
            (X_TRACE_ID, self.trace_id.as_deref()),
            (X_SPAN_ID, self.span_id.as_deref()),
            (X_CORRELATION_ID, self.correlation_id.as_deref()),
            (X_CLIENT_ID, self.client_id.as_deref()),
        ];
        for (name, value) in pairs {
            if let Some(value) = value
                && let (Ok(name), Ok(value)) = (
                    HeaderName::from_bytes(name.as_bytes()),
                    HeaderValue::from_str(value),
                )
            {
                headers.insert(name, value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrips_through_headers() {
        let ctx = RequestContext::new()
            .with_request_id("r")
            .with_trace_id("t")
            .with_span_id("s")
            .with_correlation_id("c")
            .with_client_id("cl");
        let headers = ctx.to_headers();
        assert_eq!(headers.len(), 5);
        let back = RequestContext::from_headers(&headers);
        assert_eq!(ctx, back);
    }

    #[test]
    fn only_populated_fields_emitted() {
        let ctx = RequestContext::new().with_trace_id("t");
        let headers = ctx.to_headers();
        assert_eq!(headers.len(), 1);
        assert!(headers.contains_key(X_TRACE_ID));
        assert!(!headers.contains_key(X_REQUEST_ID));
    }

    #[test]
    fn ensure_request_id_generates_uuid_when_absent() {
        let mut ctx = RequestContext::new();
        let id = ctx.ensure_request_id();
        assert_eq!(id.len(), 36);
        // Idempotent: a second call keeps the same value.
        let again = ctx.ensure_request_id();
        assert_eq!(id, again);
    }

    #[test]
    fn ensure_request_id_preserves_existing() {
        let mut ctx = RequestContext::new().with_request_id("keep-me");
        assert_eq!(ctx.ensure_request_id(), "keep-me");
    }

    #[test]
    fn from_headers_ignores_unrelated() {
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_static("abc"));
        headers.insert("authorization", HeaderValue::from_static("Bearer x"));
        let ctx = RequestContext::from_headers(&headers);
        assert_eq!(ctx.request_id.as_deref(), Some("abc"));
        assert_eq!(ctx.client_id, None);
    }

    #[test]
    fn propagated_header_set_is_exact() {
        assert_eq!(
            PROPAGATED_HEADERS,
            [
                "x-request-id",
                "x-trace-id",
                "x-span-id",
                "x-correlation-id",
                "x-client-id"
            ]
        );
    }
}
