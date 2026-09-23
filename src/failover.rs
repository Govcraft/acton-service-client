//! Endpoint sets and failover under one deadline.
//!
//! A client can be given an ordered set of endpoints: the builder's base URL
//! first, then any number of [`failover_endpoint`](crate::ServiceClientBuilder::failover_endpoint)s.
//! Every endpoint serves the same API, so each one reuses the base URL's path
//! and differs only in its origin (scheme, host and port).
//!
//! When retries apply to a request (a [`RetryPolicy`] is configured and the
//! method is idempotent or the request is marked
//! [`retriable(true)`](crate::RequestBuilder::retriable)), an attempt that
//! ends in a **rotation outcome** moves the next attempt to the next endpoint
//! in the set, at once and without a pause:
//!
//! - a status listed with
//!   [`retry_on_status`](crate::RequestBuilder::retry_on_status), such as
//!   `421 Misdirected Request`;
//! - a connect failure (reqwest's `is_connect()`: the connection could not be
//!   established, so no byte of the request was written);
//! - an attempt timeout.
//!
//! Every other outcome behaves exactly as with a single endpoint: a status
//! that is retriable by default (`429`, `502`, `503`, `504`, and `423` with
//! `Retry-After`) is retried on the **same** endpoint with the policy's
//! backoff, and anything else is returned at once. That includes a transport
//! failure after the request was written (a connection reset mid-body, for
//! example): it is neither a connect failure nor a timeout, so it is returned
//! as [`ClientError::Transport`] and never retried, since the endpoint may
//! have processed the request.
//!
//! After a full cycle of rotations (every endpoint in the set answered with a
//! rotation outcome in a row) the client pauses before starting the next
//! cycle, with the policy's [`pause`](RetryPolicy::pause), or with the
//! smallest server `Retry-After` when every rotation in the cycle carried one.
//! One deadline covers every attempt on every endpoint, and `max_attempts`
//! counts every send. The client never sends to an origin outside the set.
//! A [`RetryObserver`] hears of every re-send: each rotation, and each retry
//! on the same endpoint.
//!
//! Every decision (set validation, the classification of an attempt's
//! outcome, and whether to stop, retry the same endpoint, or rotate) is a pure
//! function of the attempt history, the clock reading and the jitter draw, so
//! `spec/fixtures/endpoint-failover-v1.json` pins it for every port.

use std::fmt;
use std::time::Duration;

use reqwest::StatusCode;

use crate::error::{ClientError, status_is_retriable};
use crate::retry::{RetryPolicy, fits_before};

/// One endpoint to add to a client's failover set.
///
/// Built from a bare origin (`scheme://host[:port]`, optionally with a
/// trailing `/`). The path, query and version prefix come from the client's
/// base URL, so they are the same on every endpoint. A `&str` or `String`
/// converts into an `Endpoint` that shares the client's HTTP client; use
/// [`with_http_client`](Self::with_http_client) to give one endpoint its own,
/// for example a client carrying a different TLS configuration.
///
/// # Examples
///
/// ```
/// use acton_service_client::{Endpoint, ServiceClient};
///
/// let client = ServiceClient::builder("https://a.example.com")
///     .failover_endpoint("https://b.example.com")
///     .failover_endpoint(Endpoint::new("https://c.example.com:8443"))
///     .build()
///     .expect("a valid endpoint set");
/// assert_eq!(client.endpoints().len(), 3);
/// ```
#[derive(Clone, Debug)]
pub struct Endpoint {
    pub(crate) url: String,
    pub(crate) http: Option<reqwest::Client>,
}

impl Endpoint {
    /// An endpoint at the origin `url`, sharing the client's HTTP client.
    ///
    /// The URL is validated when the client is built; see
    /// [`EndpointSetError`] for what is refused.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::Endpoint;
    ///
    /// let endpoint = Endpoint::new("https://b.example.com");
    /// assert_eq!(endpoint.url(), "https://b.example.com");
    /// ```
    #[must_use]
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            http: None,
        }
    }

    /// Send this endpoint's attempts through `client` instead of the client
    /// shared by the rest of the set.
    ///
    /// Use it when one endpoint needs its own TLS configuration (a different
    /// root store or client certificate). It is treated like a client passed
    /// to [`with_http_client`](crate::ServiceClientBuilder::with_http_client):
    /// the builder's [`timeout`](crate::ServiceClientBuilder::timeout) does
    /// not apply to it, so bound its attempts with
    /// [`attempt_timeout`](crate::ServiceClientBuilder::attempt_timeout). Build
    /// it with [`reqwest::redirect::Policy::none`] (or a policy limited to the
    /// set): a redirect that leaves the set is refused with
    /// [`ClientError::Config`].
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{Endpoint, ServiceClient, reqwest};
    ///
    /// let backup_tls = reqwest::Client::builder()
    ///     .redirect(reqwest::redirect::Policy::none())
    ///     .build()
    ///     .expect("a client");
    /// let client = ServiceClient::builder("https://a.example.com")
    ///     .failover_endpoint(Endpoint::new("https://b.example.com").with_http_client(backup_tls))
    ///     .build()
    ///     .expect("a valid endpoint set");
    /// # let _ = client;
    /// ```
    #[must_use]
    pub fn with_http_client(mut self, client: reqwest::Client) -> Self {
        self.http = Some(client);
        self
    }

    /// The URL this endpoint was created from, as given.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::Endpoint;
    ///
    /// assert_eq!(Endpoint::from("https://b.example.com").url(), "https://b.example.com");
    /// ```
    #[must_use]
    pub fn url(&self) -> &str {
        &self.url
    }
}

impl From<&str> for Endpoint {
    fn from(url: &str) -> Self {
        Self::new(url)
    }
}

impl From<String> for Endpoint {
    fn from(url: String) -> Self {
        Self::new(url)
    }
}

/// The normalized origin of one endpoint: scheme, host and port.
///
/// Normalization lower-cases the scheme and host and fills in the scheme's
/// default port, so `https://A.example.com/` and `https://a.example.com:443`
/// are the same origin. It displays as `scheme://host:port`.
///
/// # Examples
///
/// ```
/// use acton_service_client::ServiceClient;
///
/// let client = ServiceClient::builder("https://API.example.com/gateway")
///     .failover_endpoint("https://backup.example.com:8443/")
///     .build()
///     .expect("a valid endpoint set");
/// let origins: Vec<String> = client.endpoints().iter().map(ToString::to_string).collect();
/// assert_eq!(origins, ["https://api.example.com:443", "https://backup.example.com:8443"]);
/// assert_eq!(client.endpoints()[1].port(), 8443);
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EndpointOrigin {
    scheme: String,
    host: String,
    port: u16,
}

impl EndpointOrigin {
    /// The origin of `url`, or `None` when it has no host or no known port.
    pub(crate) fn from_url(url: &url::Url) -> Option<Self> {
        Some(Self {
            scheme: url.scheme().to_string(),
            host: url.host_str()?.to_string(),
            port: url.port_or_known_default()?,
        })
    }

    /// The scheme, `http` or `https`.
    ///
    /// # Examples
    ///
    /// ```
    /// # let client = acton_service_client::ServiceClient::builder("https://a.example.com").build().unwrap();
    /// assert_eq!(client.endpoints()[0].scheme(), "https");
    /// ```
    #[must_use]
    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// The host, lower-cased; an IPv6 address keeps its brackets.
    ///
    /// # Examples
    ///
    /// ```
    /// # let client = acton_service_client::ServiceClient::builder("http://[::1]:8080").build().unwrap();
    /// assert_eq!(client.endpoints()[0].host(), "[::1]");
    /// ```
    #[must_use]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port, with the scheme's default filled in.
    ///
    /// # Examples
    ///
    /// ```
    /// # let client = acton_service_client::ServiceClient::builder("http://a.example.com").build().unwrap();
    /// assert_eq!(client.endpoints()[0].port(), 80);
    /// ```
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// `base` with its origin replaced by this one (path, query and user info
    /// kept).
    pub(crate) fn apply_to(&self, base: &url::Url) -> Result<url::Url, ClientError> {
        let mut url = base.clone();
        let rejected = |what: &str| {
            ClientError::Config(format!(
                "cannot address endpoint {self}: {what} was rejected for {base}"
            ))
        };
        url.set_host(Some(&self.host))
            .map_err(|_| rejected("the host"))?;
        url.set_port(Some(self.port))
            .map_err(|()| rejected("the port"))?;
        Ok(url)
    }
}

impl fmt::Display for EndpointOrigin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}:{}", self.scheme, self.host, self.port)
    }
}

/// Why an attempt asked for another try: on the next endpoint (a rotation) or
/// on the same one (a retry). A [`RetryObserver`] receives it for each.
///
/// It is also the outcome recorded for every attempt in a [`FailoverTrace`],
/// since an attempt that ends a failover call early (a success, or an answer
/// that is not retried) never appears in one.
///
/// # What each reason proves
///
/// | Reason | Did the endpoint process the request? |
/// |--------|---------------------------------------|
/// | [`Connect`](Self::Connect) | **No.** The connection could not be established, so no byte of the request was written. |
/// | [`Status(421)`](Self::Status) | **No.** The endpoint refused it as misdirected. |
/// | [`Timeout`](Self::Timeout) | Unknown: it may have been processed. |
/// | any other [`Status`](Self::Status) | Unknown: it may have been processed. |
///
/// [`proves_not_processed`](Self::proves_not_processed) encodes this table.
///
/// # A failure after the request was written has no reason
///
/// A connection reset (or any other I/O error) after the request was written
/// is neither [`Connect`](Self::Connect) nor [`Timeout`](Self::Timeout), and
/// it is deliberately not retried or rotated: the call returns it at once as
/// [`ClientError::Transport`], exactly as 0.2.0 did. The endpoint may have
/// processed the request, and a patch release must not change what an
/// existing single-endpoint caller sees. Treat that error as ambiguous: re-send
/// the operation with the same idempotency identity, as a caller that owns
/// the operation's semantics (for example the Axorum SDK) already does.
///
/// # Examples
///
/// ```
/// use acton_service_client::{RetryReason, StatusCode};
///
/// let reason = RetryReason::Status(StatusCode::MISDIRECTED_REQUEST);
/// assert_eq!(reason.label(), "421");
/// assert!(reason.proves_not_processed());
/// assert!(!RetryReason::Timeout.proves_not_processed());
/// ```
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RetryReason {
    /// The endpoint answered with this status.
    Status(StatusCode),
    /// The connection to the endpoint could not be established (reqwest's
    /// `is_connect()`, a connect timeout included), so no byte of the request
    /// was written. A failure after the request was written is never
    /// `Connect`: it is not retried at all, and is returned as
    /// [`ClientError::Transport`].
    Connect,
    /// The attempt ran out of time before an answer arrived.
    Timeout,
}

impl RetryReason {
    /// A short, stable label for metrics: the status code (`"421"`),
    /// `"connect"` or `"timeout"`.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{RetryReason, StatusCode};
    ///
    /// assert_eq!(RetryReason::Status(StatusCode::SERVICE_UNAVAILABLE).label(), "503");
    /// assert_eq!(RetryReason::Connect.label(), "connect");
    /// assert_eq!(RetryReason::Timeout.label(), "timeout");
    /// ```
    #[must_use]
    pub fn label(&self) -> &str {
        match self {
            Self::Status(status) => status.as_str(),
            Self::Connect => "connect",
            Self::Timeout => "timeout",
        }
    }

    /// Whether this outcome proves the endpoint did **not** process the
    /// request: true only for [`Connect`](Self::Connect) and
    /// `Status(421 Misdirected Request)`.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{RetryReason, StatusCode};
    ///
    /// assert!(RetryReason::Connect.proves_not_processed());
    /// assert!(RetryReason::Status(StatusCode::MISDIRECTED_REQUEST).proves_not_processed());
    /// assert!(!RetryReason::Status(StatusCode::SERVICE_UNAVAILABLE).proves_not_processed());
    /// assert!(!RetryReason::Timeout.proves_not_processed());
    /// ```
    #[must_use]
    pub fn proves_not_processed(&self) -> bool {
        matches!(
            self,
            Self::Connect | Self::Status(StatusCode::MISDIRECTED_REQUEST)
        )
    }
}

impl fmt::Display for RetryReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Status(status) => write!(f, "status {}", status.as_u16()),
            Self::Connect => f.write_str("connect failure"),
            Self::Timeout => f.write_str("attempt timeout"),
        }
    }
}

/// Notified of every re-send a call makes: each rotation to the next
/// endpoint, and each retry on the same endpoint.
///
/// This is the crate's metrics seam: it carries no metrics dependency, so the
/// application wires it to whatever it exports. Both methods default to doing
/// nothing, so implement only the ones you need.
///
/// Each method is called once per re-send actually made, just before that
/// attempt is sent (a re-send the deadline or `max_attempts` stops is never
/// reported). The two never overlap: a re-send to another endpoint is a
/// rotation, a re-send to the same endpoint is a retry. A client with a single
/// endpoint therefore only ever calls [`on_retry`](Self::on_retry). Both run
/// inside the send loop, so keep them cheap and non-blocking.
///
/// # Guarantees
///
/// These hold for every call, on a single endpoint or a set, whatever it
/// returns, and `spec/fixtures/endpoint-failover-v1.json` pins them:
///
/// - **One call per attempt but the last, in send order.** Every attempt the
///   call sends except the last is reported exactly once, by
///   [`on_rotation`](Self::on_rotation) or [`on_retry`](Self::on_retry), with
///   that attempt's endpoint and outcome. The last attempt is never reported:
///   its outcome is what the call returns (the response, or the error).
/// - **Synchronous, on the caller's task.** Both run inside the future that
///   `send` returns, on the task that awaits it; the crate spawns no task for
///   them. A `tokio::task_local!` scoped around the call therefore sees every
///   report for that call and no other.
///
/// So the complete per-attempt record of a call is its reports followed by
/// its result, on the success path too. For example, a call that meets a
/// `421`, then a connect failure, then returns an accepted `421` response
/// reports two reasons (`421`, `connect`) and returns the third outcome; since
/// all three [prove it](RetryReason::proves_not_processed), no endpoint
/// processed the request.
///
/// # Recommended wiring for an `acton-service` application
///
/// Count both on the service's own meter provider, labelled by reason, as two
/// counters: `acton_service_client.endpoint.rotations` (the call changed
/// endpoint) and `acton_service_client.endpoint.retries` (it re-sent to the
/// same one). A single-endpoint deployment answering `421` shows up in the
/// second.
///
/// ```ignore
/// use acton_service::observability::get_meter;
/// use acton_service_client::{EndpointOrigin, RetryObserver, RetryReason};
/// use opentelemetry::KeyValue;
/// use opentelemetry::metrics::Counter;
///
/// struct Metrics {
///     rotations: Counter<u64>,
///     retries: Counter<u64>,
/// }
///
/// impl RetryObserver for Metrics {
///     fn on_rotation(&self, left: &EndpointOrigin, reason: RetryReason) {
///         tracing::warn!(%left, %reason, "left endpoint");
///         self.rotations.add(1, &[KeyValue::new("reason", reason.label().to_owned())]);
///     }
///
///     fn on_retry(&self, endpoint: &EndpointOrigin, reason: RetryReason, attempt: u32) {
///         tracing::warn!(%endpoint, %reason, attempt, "re-sending to the same endpoint");
///         self.retries.add(1, &[KeyValue::new("reason", reason.label().to_owned())]);
///     }
/// }
///
/// let meter = get_meter();
/// let client = ServiceClient::builder("https://a.example.com")
///     .failover_endpoint("https://b.example.com")
///     .retry_observer(Metrics {
///         rotations: meter.u64_counter("acton_service_client.endpoint.rotations").build(),
///         retries: meter.u64_counter("acton_service_client.endpoint.retries").build(),
///     })
///     .build()?;
/// ```
///
/// # Examples
///
/// ```
/// use acton_service_client::{EndpointOrigin, RetryObserver, RetryReason, ServiceClient};
/// use std::sync::atomic::{AtomicU64, Ordering};
///
/// #[derive(Default)]
/// struct Rotations(AtomicU64);
///
/// impl RetryObserver for Rotations {
///     fn on_rotation(&self, _left: &EndpointOrigin, _reason: RetryReason) {
///         self.0.fetch_add(1, Ordering::Relaxed);
///     }
/// }
///
/// let client = ServiceClient::builder("https://a.example.com")
///     .failover_endpoint("https://b.example.com")
///     .retry_observer(Rotations::default())
///     .build()
///     .expect("a valid endpoint set");
/// # let _ = client;
/// ```
pub trait RetryObserver: Send + Sync + 'static {
    /// The call is leaving endpoint `left` for the next one in the set because
    /// of `reason`.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{EndpointOrigin, RetryObserver, RetryReason};
    ///
    /// struct Log;
    ///
    /// impl RetryObserver for Log {
    ///     fn on_rotation(&self, left: &EndpointOrigin, reason: RetryReason) {
    ///         eprintln!("left {left}: {reason}");
    ///     }
    /// }
    /// ```
    fn on_rotation(&self, left: &EndpointOrigin, reason: RetryReason) {
        let _ = (left, reason);
    }

    /// The call is re-sending to `endpoint`, the endpoint that just answered
    /// with `reason`; `attempt` is the number of the attempt about to be sent
    /// (`2` for the first re-send), counted over the whole call.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{EndpointOrigin, RetryObserver, RetryReason};
    ///
    /// struct Log;
    ///
    /// impl RetryObserver for Log {
    ///     fn on_retry(&self, endpoint: &EndpointOrigin, reason: RetryReason, attempt: u32) {
    ///         eprintln!("attempt {attempt} to {endpoint} after {reason}");
    ///     }
    /// }
    /// ```
    fn on_retry(&self, endpoint: &EndpointOrigin, reason: RetryReason, attempt: u32) {
        let _ = (endpoint, reason, attempt);
    }
}

/// Why an endpoint set was refused when the client was built.
///
/// Endpoints are numbered in configured order: `0` is the builder's base URL,
/// `1` the first failover endpoint, and so on.
///
/// # Examples
///
/// ```
/// use acton_service_client::{ClientError, ServiceClient};
/// use acton_service_client::failover::EndpointSetError;
///
/// let err = ServiceClient::builder("https://a.example.com")
///     .failover_endpoint("http://b.example.com")
///     .build()
///     .unwrap_err();
/// assert!(matches!(
///     err,
///     ClientError::InvalidEndpoints(EndpointSetError::MixedScheme { index: 1, .. })
/// ));
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum EndpointSetError {
    /// Two endpoints have the same origin once normalized.
    #[error(
        "endpoint {duplicate} is the same origin as endpoint {first} ({origin}) once normalized; \
         list each origin once"
    )]
    DuplicateOrigin {
        /// The shared origin.
        origin: EndpointOrigin,
        /// The index of its first appearance.
        first: usize,
        /// The index of the repeat.
        duplicate: usize,
    },
    /// An endpoint's scheme differs from the base URL's.
    #[error(
        "endpoint {index} uses {found} but the base URL uses {expected}; every endpoint in a set \
         must use the same scheme, so give endpoint {index} as {expected}://host[:port]"
    )]
    MixedScheme {
        /// The index of the endpoint with the other scheme.
        index: usize,
        /// The base URL's scheme.
        expected: String,
        /// The scheme the endpoint used.
        found: String,
    },
    /// A failover endpoint is not a bare `scheme://host[:port]` origin.
    #[error(
        "endpoint {index} ({url:?}) is not a bare origin: {reason}; give it as \
         scheme://host[:port], since every endpoint reuses the base URL's path"
    )]
    NotAnOrigin {
        /// The index of the endpoint.
        index: usize,
        /// The URL as given.
        url: String,
        /// What is wrong with it.
        reason: &'static str,
    },
}

/// What happened on each attempt of a failover call that ran out of budget.
///
/// Carried by [`ClientError::EndpointsExhausted`] and returned by
/// [`ClientError::failover_trace`].
///
/// # Examples
///
/// ```no_run
/// use acton_service_client::{ClientError, Method, ServiceClient};
/// # async fn run(client: ServiceClient) {
/// match client.request(Method::GET, "orders/7").send().await {
///     Err(ClientError::EndpointsExhausted(trace)) => {
///         for attempt in &trace.attempts {
///             eprintln!("{} -> {} (rotated: {})", attempt.endpoint, attempt.outcome, attempt.rotated);
///         }
///         if trace.proves_not_processed() {
///             eprintln!("no endpoint processed it: safe to send again later");
///         }
///     }
///     other => { let _ = other; }
/// }
/// # }
/// ```
#[non_exhaustive]
#[derive(Debug)]
pub struct FailoverTrace {
    /// Every attempt, in the order sent, the final one included.
    pub attempts: Vec<TracedAttempt>,
    /// The final attempt's own error, exactly as a single-endpoint client
    /// would have returned it.
    pub last: ClientError,
    /// Time spent inside the call.
    pub elapsed: Duration,
}

impl FailoverTrace {
    /// Whether every attempt's outcome proves its endpoint did not process the
    /// request (see [`RetryReason::proves_not_processed`]). When true,
    /// nothing was applied anywhere in the set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use acton_service_client::ClientError;
    /// # fn check(err: &ClientError) {
    /// if let Some(trace) = err.failover_trace() {
    ///     let safe_to_resend = trace.proves_not_processed();
    ///     # let _ = safe_to_resend;
    /// }
    /// # }
    /// ```
    #[must_use]
    pub fn proves_not_processed(&self) -> bool {
        self.attempts
            .iter()
            .all(|attempt| attempt.outcome.proves_not_processed())
    }
}

impl fmt::Display for FailoverTrace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let advice = if self.proves_not_processed() {
            "no endpoint processed the request, so it is safe to send again once capacity returns, \
             or widen the deadline"
        } else {
            "an attempt may have been applied, so widen the deadline, or re-drive the operation \
             with the same idempotency identity"
        };
        write!(
            f,
            "every endpoint failed before the budget ran out ({} attempts in {:?}; last: {}); {advice}",
            self.attempts.len(),
            self.elapsed,
            self.last
        )
    }
}

impl std::error::Error for FailoverTrace {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.last)
    }
}

/// One attempt of a failover call.
///
/// # Examples
///
/// ```no_run
/// # use acton_service_client::ClientError;
/// # fn show(err: &ClientError) {
/// if let Some(trace) = err.failover_trace() {
///     let tried: Vec<String> = trace.attempts.iter().map(|a| a.endpoint.to_string()).collect();
///     eprintln!("tried {tried:?}");
/// }
/// # }
/// ```
#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TracedAttempt {
    /// The endpoint the attempt was sent to.
    pub endpoint: EndpointOrigin,
    /// How the attempt ended.
    pub outcome: RetryReason,
    /// Whether the next attempt went to the next endpoint because of this
    /// outcome (`false` for a retry on the same endpoint, and for the final
    /// attempt).
    pub rotated: bool,
}

/// Validate the failover endpoints against the base URL's `primary` origin,
/// returning every origin in order (the primary first).
///
/// Each endpoint is checked in order: that it is a bare `http(s)` origin, then
/// that it shares the primary's scheme, then that its origin is new.
pub(crate) fn validate_set(
    primary: EndpointOrigin,
    failovers: &[&str],
) -> Result<Vec<EndpointOrigin>, EndpointSetError> {
    let mut origins = vec![primary];
    for (offset, raw) in failovers.iter().enumerate() {
        let index = offset + 1;
        let origin = bare_origin(index, raw)?;
        if origin.scheme != origins[0].scheme {
            return Err(EndpointSetError::MixedScheme {
                index,
                expected: origins[0].scheme.clone(),
                found: origin.scheme,
            });
        }
        if let Some(first) = origins.iter().position(|seen| *seen == origin) {
            return Err(EndpointSetError::DuplicateOrigin {
                origin,
                first,
                duplicate: index,
            });
        }
        origins.push(origin);
    }
    Ok(origins)
}

/// Parse `raw` as a bare `http(s)://host[:port]` origin.
fn bare_origin(index: usize, raw: &str) -> Result<EndpointOrigin, EndpointSetError> {
    let refuse = |reason| EndpointSetError::NotAnOrigin {
        index,
        url: raw.to_string(),
        reason,
    };
    let url = url::Url::parse(raw).map_err(|_| refuse("it is not a valid absolute URL"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(refuse("its scheme must be http or https"));
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(refuse("it carries user info"));
    }
    if !matches!(url.path(), "" | "/") {
        return Err(refuse("it has a path"));
    }
    if url.query().is_some() {
        return Err(refuse("it has a query"));
    }
    if url.fragment().is_some() {
        return Err(refuse("it has a fragment"));
    }
    EndpointOrigin::from_url(&url).ok_or_else(|| refuse("it has no host"))
}

/// How one attempt that did not succeed bears on the next.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// Another try is wanted, and on a set it goes to the next endpoint.
    Rotate {
        reason: RetryReason,
        retry_after: Option<Duration>,
    },
    /// Another try is wanted, on the same endpoint (retriable by default).
    Retry {
        status: StatusCode,
        retry_after: Option<Duration>,
    },
    /// No other try: the outcome is returned as it is.
    Final,
}

impl Outcome {
    /// Classify a non-success response. A status listed in `retry_on` rotates
    /// (it is checked before `accepted`); otherwise an unaccepted status that
    /// is retriable by default retries in place. Together these are exactly
    /// the statuses 0.2.0 retried.
    pub(crate) fn of_status(
        status: StatusCode,
        retry_after: Option<Duration>,
        accepted: bool,
        retry_on: &[StatusCode],
    ) -> Self {
        if retry_on.contains(&status) {
            Self::Rotate {
                reason: RetryReason::Status(status),
                retry_after,
            }
        } else if !accepted && status_is_retriable(status, retry_after) {
            Self::Retry {
                status,
                retry_after,
            }
        } else {
            Self::Final
        }
    }

    /// Classify a transport failure. A connect failure (a connect timeout
    /// included, since the request never left) and an attempt timeout rotate,
    /// which are exactly the transport errors 0.2.0 retried.
    pub(crate) fn of_transport(connect: bool, timeout: bool) -> Self {
        let reason = if connect {
            RetryReason::Connect
        } else if timeout {
            RetryReason::Timeout
        } else {
            return Self::Final;
        };
        Self::Rotate {
            reason,
            retry_after: None,
        }
    }

    /// Whether this outcome rotates, and so leaves the sticky preference
    /// where it is.
    pub(crate) fn is_rotation(self) -> bool {
        matches!(self, Self::Rotate { .. })
    }

    fn traced(self) -> Option<(RetryReason, Option<Duration>)> {
        match self {
            Self::Rotate {
                reason,
                retry_after,
            } => Some((reason, retry_after)),
            Self::Retry {
                status,
                retry_after,
            } => Some((RetryReason::Status(status), retry_after)),
            Self::Final => None,
        }
    }
}

/// Why a call stopped without a success.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stop {
    /// The deadline had passed before the first attempt.
    NothingSent,
    /// Return the last outcome as it is.
    Raw,
    /// The budget ran out after at least one rotation: return the trace.
    Exhausted,
}

/// When the next attempt goes out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Next {
    /// At once (a rotation within a cycle).
    Now,
    /// After this pause.
    After(Duration),
}

/// The next attempt to send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Attempt {
    /// The endpoint to send it to.
    pub(crate) endpoint: usize,
    /// Its number in the call, from `1`.
    pub(crate) number: u32,
    /// How it re-sends the previous attempt, if it does (for the
    /// [`RetryObserver`]).
    pub(crate) resend: Option<Resend>,
}

/// A re-send, as the [`RetryObserver`] hears of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Resend {
    /// To the next endpoint, leaving `from`.
    Rotation { from: usize, reason: RetryReason },
    /// To the same endpoint.
    Retry { reason: RetryReason },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Entry {
    endpoint: usize,
    outcome: RetryReason,
    rotated: bool,
}

/// The failover state of one call: which endpoint is next, and the counts
/// every decision depends on. Pure: time and randomness are passed in.
///
/// With one endpoint every decision is exactly 0.2.0's: the pause after the
/// `n`th attempt is `next_pause(n, ..)`, since then every retry follows one
/// pause and `pauses + 1 == attempts`.
#[derive(Debug)]
pub(crate) struct Failover {
    len: usize,
    current: usize,
    attempts: u32,
    pauses: u32,
    /// Consecutive rotations since the last pause or same-endpoint retry.
    streak: usize,
    /// Over the rotations of the current cycle: `None` before the first,
    /// then the smallest `Retry-After` while every one carried one, and
    /// `Some(None)` once one did not.
    cycle_retry_after: Option<Option<Duration>>,
    /// A re-send decided but not yet made (made when the next attempt is
    /// sent).
    pending: Option<Resend>,
    trace: Vec<Entry>,
}

impl Failover {
    /// A call over `len` endpoints (at least one) starting at `start`.
    pub(crate) fn new(len: usize, start: usize) -> Self {
        let len = len.max(1);
        Self {
            len,
            current: start % len,
            attempts: 0,
            pauses: 0,
            streak: 0,
            cycle_retry_after: None,
            pending: None,
            trace: Vec::new(),
        }
    }

    /// Attempts sent so far.
    pub(crate) fn attempts(&self) -> u32 {
        self.attempts
    }

    /// Before each send, with the time `remaining` before the deadline: the
    /// attempt to send, or why to stop.
    pub(crate) fn begin(&mut self, remaining: Option<Duration>) -> Result<Attempt, Stop> {
        if remaining == Some(Duration::ZERO) {
            return Err(if self.attempts == 0 {
                Stop::NothingSent
            } else {
                self.spent()
            });
        }
        let resend = self.pending.take();
        if let (Some(Resend::Rotation { .. }), Some(left)) = (resend, self.trace.last_mut()) {
            left.rotated = true;
        }
        self.attempts = self.attempts.saturating_add(1);
        Ok(Attempt {
            endpoint: self.current,
            number: self.attempts,
            resend,
        })
    }

    /// After an attempt that did not succeed: when to send the next one, or
    /// why to stop.
    ///
    /// `policy` is `None` when retries do not apply to the request. `draw`
    /// supplies the jitter randomness, and is called only when a computed
    /// pause needs it.
    pub(crate) fn after(
        &mut self,
        outcome: Outcome,
        policy: Option<&RetryPolicy>,
        draw: impl FnOnce() -> f64,
        remaining: Option<Duration>,
    ) -> Result<Next, Stop> {
        let Some((reason, retry_after)) = outcome.traced() else {
            return Err(Stop::Raw);
        };
        self.trace.push(Entry {
            endpoint: self.current,
            outcome: reason,
            rotated: false,
        });
        let Some(policy) = policy else {
            return Err(Stop::Raw);
        };
        if !policy.should_retry(self.attempts) {
            return Err(self.spent());
        }
        if self.len > 1 && outcome.is_rotation() {
            self.rotate(policy, reason, retry_after, draw, remaining)
        } else {
            self.streak = 0;
            self.cycle_retry_after = None;
            let pause = self.pause(policy, retry_after, draw, remaining)?;
            self.pending = Some(Resend::Retry { reason });
            Ok(Next::After(pause))
        }
    }

    fn rotate(
        &mut self,
        policy: &RetryPolicy,
        reason: RetryReason,
        retry_after: Option<Duration>,
        draw: impl FnOnce() -> f64,
        remaining: Option<Duration>,
    ) -> Result<Next, Stop> {
        self.streak += 1;
        self.cycle_retry_after = Some(match self.cycle_retry_after {
            None => retry_after,
            Some(smallest) => smallest.zip(retry_after).map(|(a, b)| a.min(b)),
        });
        let next = if self.streak.is_multiple_of(self.len) {
            let server = self.cycle_retry_after.take().flatten();
            Next::After(self.pause(policy, server, draw, remaining)?)
        } else {
            Next::Now
        };
        self.pending = Some(Resend::Rotation {
            from: self.current,
            reason,
        });
        self.current = (self.current + 1) % self.len;
        Ok(next)
    }

    /// The server's `Retry-After` if given, else the policy's backoff, as
    /// long as it ends before the deadline.
    fn pause(
        &mut self,
        policy: &RetryPolicy,
        retry_after: Option<Duration>,
        draw: impl FnOnce() -> f64,
        remaining: Option<Duration>,
    ) -> Result<Duration, Stop> {
        let pause = match retry_after {
            Some(server) => fits_before(server, remaining),
            None => policy.pause(self.pauses.saturating_add(1), draw(), remaining),
        };
        let pause = pause.ok_or_else(|| self.spent())?;
        self.pauses = self.pauses.saturating_add(1);
        Ok(pause)
    }

    /// The budget is spent: a trace once the call has rotated, the last
    /// outcome as it is otherwise.
    fn spent(&self) -> Stop {
        if self.trace.iter().any(|entry| entry.rotated) {
            Stop::Exhausted
        } else {
            Stop::Raw
        }
    }

    /// The trace of this call, naming endpoints by `origins`.
    pub(crate) fn trace(
        &self,
        origins: &[EndpointOrigin],
        last: ClientError,
        elapsed: Duration,
    ) -> FailoverTrace {
        FailoverTrace {
            attempts: self
                .trace
                .iter()
                .filter_map(|entry| {
                    Some(TracedAttempt {
                        endpoint: origins.get(entry.endpoint)?.clone(),
                        outcome: entry.outcome,
                        rotated: entry.rotated,
                    })
                })
                .collect(),
            last,
            elapsed,
        }
    }
}

#[cfg(test)]
#[path = "../tests/support/failover_fixture.rs"]
mod fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::retry::{Jitter, attempt_timeout, is_idempotent};

    fn origin(raw: &str) -> EndpointOrigin {
        EndpointOrigin::from_url(&url::Url::parse(raw).unwrap()).unwrap()
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    const MISDIRECTED: Outcome = Outcome::Rotate {
        reason: RetryReason::Status(StatusCode::MISDIRECTED_REQUEST),
        retry_after: None,
    };

    #[test]
    fn origins_normalize_case_and_default_ports() {
        assert_eq!(
            origin("HTTPS://API.Example.COM/").to_string(),
            "https://api.example.com:443"
        );
        assert_eq!(origin("http://a.example").port(), 80);
        assert_eq!(origin("http://[::1]:9/").host(), "[::1]");
    }

    #[test]
    fn validation_accepts_a_clean_set_in_order() {
        let set = validate_set(
            origin("https://a.example/api"),
            &["https://b.example", "https://c.example:8443/"],
        )
        .unwrap();
        let shown: Vec<String> = set.iter().map(ToString::to_string).collect();
        assert_eq!(
            shown,
            [
                "https://a.example:443",
                "https://b.example:443",
                "https://c.example:8443"
            ]
        );
    }

    #[test]
    fn validation_refuses_duplicates_after_normalization() {
        let err = validate_set(
            origin("https://a.example"),
            &["https://b.example", "HTTPS://A.EXAMPLE:443/"],
        )
        .unwrap_err();
        assert_eq!(
            err,
            EndpointSetError::DuplicateOrigin {
                origin: origin("https://a.example"),
                first: 0,
                duplicate: 2,
            }
        );
        assert!(err.to_string().contains("list each origin once"), "{err}");
    }

    #[test]
    fn validation_refuses_mixed_schemes() {
        let err = validate_set(origin("https://a.example"), &["http://b.example"]).unwrap_err();
        assert_eq!(
            err,
            EndpointSetError::MixedScheme {
                index: 1,
                expected: "https".into(),
                found: "http".into(),
            }
        );
        assert!(err.to_string().contains("https://host[:port]"), "{err}");
    }

    #[test]
    fn validation_refuses_anything_but_a_bare_origin() {
        let cases = [
            ("not a url", "it is not a valid absolute URL"),
            ("ftp://b.example", "its scheme must be http or https"),
            ("https://user:pw@b.example", "it carries user info"),
            ("https://b.example/api", "it has a path"),
            ("https://b.example/?x=1", "it has a query"),
            ("https://b.example/#top", "it has a fragment"),
        ];
        for (raw, reason) in cases {
            let err = validate_set(origin("https://a.example"), &[raw]).unwrap_err();
            assert_eq!(
                err,
                EndpointSetError::NotAnOrigin {
                    index: 1,
                    url: raw.into(),
                    reason,
                },
                "{raw}"
            );
        }
    }

    #[test]
    fn classification_matches_what_0_2_0_retried() {
        let s421 = StatusCode::MISDIRECTED_REQUEST;
        let s503 = StatusCode::SERVICE_UNAVAILABLE;
        assert!(Outcome::of_status(s421, None, false, &[s421]).is_rotation());
        // retry_on is checked before accept_status.
        assert!(Outcome::of_status(s421, None, true, &[s421]).is_rotation());
        assert_eq!(
            Outcome::of_status(s503, None, false, &[s421]),
            Outcome::Retry {
                status: s503,
                retry_after: None
            }
        );
        assert_eq!(Outcome::of_status(s503, None, true, &[]), Outcome::Final);
        assert_eq!(
            Outcome::of_status(StatusCode::NOT_FOUND, None, false, &[]),
            Outcome::Final
        );
        assert_eq!(Outcome::of_transport(false, false), Outcome::Final);
        assert_eq!(
            Outcome::of_transport(true, true),
            Outcome::Rotate {
                reason: RetryReason::Connect,
                retry_after: None
            }
        );
        assert_eq!(
            Outcome::of_transport(false, true),
            Outcome::Rotate {
                reason: RetryReason::Timeout,
                retry_after: None
            }
        );
    }

    /// A status asks for another try (rotating or in place) exactly when
    /// 0.2.0's `wants_retry` said so, over every status and flag.
    #[test]
    fn classification_agrees_with_0_2_0_over_every_status() {
        let retry_on = [
            StatusCode::MISDIRECTED_REQUEST,
            StatusCode::SERVICE_UNAVAILABLE,
        ];
        for code in 100..600u16 {
            let status = StatusCode::from_u16(code).unwrap();
            for accepted in [false, true] {
                for retry_after in [None, Some(ms(1_000))] {
                    for listed in [&retry_on[..], &[]] {
                        let wanted =
                            crate::retry::wants_retry(status, retry_after, accepted, listed);
                        let outcome = Outcome::of_status(status, retry_after, accepted, listed);
                        assert_eq!(outcome != Outcome::Final, wanted, "{status} {accepted}");
                        assert_eq!(outcome.is_rotation(), listed.contains(&status), "{status}");
                    }
                }
            }
        }
    }

    /// A single endpoint takes exactly 0.2.0's decisions: the same pause
    /// (`next_pause(attempt, ..)`) after every attempt, and the same stop.
    #[test]
    fn a_single_endpoint_decides_exactly_as_0_2_0() {
        let outcomes = [
            MISDIRECTED,
            Outcome::of_transport(true, false),
            Outcome::of_transport(false, true),
            Outcome::of_status(StatusCode::BAD_GATEWAY, None, false, &[]),
            Outcome::of_status(StatusCode::LOCKED, Some(ms(700)), false, &[]),
            Outcome::of_status(
                StatusCode::MISDIRECTED_REQUEST,
                Some(Duration::from_secs(2)),
                false,
                &[StatusCode::MISDIRECTED_REQUEST],
            ),
        ];
        let policies = [
            RetryPolicy::default(),
            RetryPolicy::with_max_attempts(u32::MAX)
                .base_delay(ms(100))
                .max_delay(ms(1600))
                .jitter(Jitter::Full),
        ];
        let remainings = [None, Some(ms(5_000)), Some(ms(450)), Some(ms(1))];
        for policy in &policies {
            for remaining in remainings {
                for (seed, outcome) in outcomes.iter().enumerate() {
                    let mut failover = Failover::new(1, 0);
                    for attempt in 1..=12u32 {
                        failover.begin(remaining).unwrap();
                        let draw = f64::from(attempt + u32::try_from(seed).unwrap()) / 20.0;
                        let (_, retry_after) = outcome.traced().unwrap();
                        let expected = policy.next_pause(attempt, draw, retry_after, remaining);
                        let got = failover.after(*outcome, Some(policy), || draw, remaining);
                        match expected {
                            Some(pause) => assert_eq!(got, Ok(Next::After(pause))),
                            None => {
                                assert_eq!(got, Err(Stop::Raw), "{outcome:?} {remaining:?}");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn rotation_is_immediate_within_a_cycle_then_pauses() {
        let policy = RetryPolicy::with_max_attempts(u32::MAX).base_delay(ms(100));
        let mut failover = Failover::new(3, 1);
        let mut sent = Vec::new();
        let mut nexts = Vec::new();
        for _ in 0..7 {
            let attempt = failover.begin(None).unwrap();
            sent.push(attempt.endpoint);
            nexts.push(
                failover
                    .after(MISDIRECTED, Some(&policy), || 0.0, None)
                    .unwrap(),
            );
        }
        assert_eq!(sent, [1, 2, 0, 1, 2, 0, 1]);
        assert_eq!(
            nexts,
            [
                Next::Now,
                Next::Now,
                Next::After(ms(100)),
                Next::Now,
                Next::Now,
                Next::After(ms(200)),
                Next::Now
            ]
        );
    }

    #[test]
    fn a_rotation_is_made_only_when_the_next_attempt_is_sent() {
        let policy = RetryPolicy::with_max_attempts(10);
        let mut failover = Failover::new(2, 0);
        assert_eq!(failover.begin(None).unwrap().resend, None);
        failover
            .after(MISDIRECTED, Some(&policy), || 0.0, None)
            .unwrap();
        // The deadline passes before the rotation happens: nothing rotated,
        // so the last outcome comes back as it is.
        assert_eq!(failover.begin(Some(Duration::ZERO)), Err(Stop::Raw));

        let mut failover = Failover::new(2, 0);
        failover.begin(None).unwrap();
        failover
            .after(MISDIRECTED, Some(&policy), || 0.0, None)
            .unwrap();
        let second = failover.begin(None).unwrap();
        assert_eq!(second.endpoint, 1);
        assert_eq!(
            second.resend,
            Some(Resend::Rotation {
                from: 0,
                reason: RetryReason::Status(StatusCode::MISDIRECTED_REQUEST)
            })
        );
        assert_eq!(failover.begin(Some(Duration::ZERO)), Err(Stop::Exhausted));
    }

    #[test]
    fn nothing_sent_is_distinct_from_spent() {
        let mut failover = Failover::new(2, 0);
        assert_eq!(failover.begin(Some(Duration::ZERO)), Err(Stop::NothingSent));
        assert_eq!(failover.attempts(), 0);
    }

    #[test]
    fn without_retries_a_rotation_status_stops_on_the_first_endpoint() {
        let mut failover = Failover::new(2, 0);
        failover.begin(None).unwrap();
        assert_eq!(
            failover.after(MISDIRECTED, None, || 0.0, None),
            Err(Stop::Raw)
        );
    }

    #[test]
    fn a_final_answer_after_a_rotation_is_returned_as_it_is() {
        let policy = RetryPolicy::with_max_attempts(10);
        let mut failover = Failover::new(2, 0);
        failover.begin(None).unwrap();
        failover
            .after(MISDIRECTED, Some(&policy), || 0.0, None)
            .unwrap();
        failover.begin(None).unwrap();
        assert_eq!(
            failover.after(Outcome::Final, Some(&policy), || 0.0, None),
            Err(Stop::Raw)
        );
    }

    #[test]
    fn a_cycle_where_every_rotation_carried_retry_after_waits_the_smallest() {
        let policy = RetryPolicy::with_max_attempts(10).base_delay(ms(5));
        let with = |secs| Outcome::Rotate {
            reason: RetryReason::Status(StatusCode::SERVICE_UNAVAILABLE),
            retry_after: Some(Duration::from_secs(secs)),
        };
        let mut failover = Failover::new(2, 0);
        failover.begin(None).unwrap();
        assert_eq!(
            failover.after(with(3), Some(&policy), || 0.0, None),
            Ok(Next::Now)
        );
        failover.begin(None).unwrap();
        assert_eq!(
            failover.after(with(1), Some(&policy), || 0.0, None),
            Ok(Next::After(Duration::from_secs(1)))
        );
        // One rotation without Retry-After: the policy's backoff instead.
        failover.begin(None).unwrap();
        failover
            .after(with(1), Some(&policy), || 0.0, None)
            .unwrap();
        failover.begin(None).unwrap();
        assert_eq!(
            failover.after(MISDIRECTED, Some(&policy), || 0.0, None),
            Ok(Next::After(ms(10)))
        );
    }

    #[test]
    fn trace_names_endpoints_and_marks_rotations() {
        let policy = RetryPolicy::with_max_attempts(2);
        let origins = [origin("https://a.example"), origin("https://b.example")];
        let mut failover = Failover::new(2, 0);
        failover.begin(None).unwrap();
        failover
            .after(
                Outcome::of_transport(true, false),
                Some(&policy),
                || 0.0,
                None,
            )
            .unwrap();
        failover.begin(None).unwrap();
        assert_eq!(
            failover.after(MISDIRECTED, Some(&policy), || 0.0, None),
            Err(Stop::Exhausted)
        );
        let trace = failover.trace(&origins, ClientError::Config("last".into()), ms(3));
        assert_eq!(
            trace.attempts,
            [
                TracedAttempt {
                    endpoint: origins[0].clone(),
                    outcome: RetryReason::Connect,
                    rotated: true
                },
                TracedAttempt {
                    endpoint: origins[1].clone(),
                    outcome: RetryReason::Status(StatusCode::MISDIRECTED_REQUEST),
                    rotated: false
                },
            ]
        );
        assert!(trace.proves_not_processed());
        assert!(trace.to_string().contains("safe to send again"), "{trace}");
    }

    fn fixture_reason(reason: RetryReason) -> fixture::Reason {
        fixture::Reason::from_label(reason.label())
    }

    #[test]
    fn fixture_reasons_table() {
        for row in fixture::load().reasons {
            let reason = row.reason.to_reason();
            assert_eq!(reason.label(), row.label);
            assert_eq!(
                reason.proves_not_processed(),
                row.proves_not_processed,
                "{reason:?}"
            );
            assert_eq!(fixture_reason(reason), row.reason);
        }
    }

    #[test]
    fn fixture_validation_rows() {
        for row in fixture::load().validation {
            let base = url::Url::parse(&row.base).unwrap();
            let failovers: Vec<&str> = row.failover.iter().map(String::as_str).collect();
            let got = validate_set(EndpointOrigin::from_url(&base).unwrap(), &failovers);
            match (got, &row.endpoints, &row.error) {
                (Ok(origins), Some(expected), None) => {
                    let shown: Vec<String> = origins.iter().map(ToString::to_string).collect();
                    assert_eq!(&shown, expected, "{}", row.name);
                }
                (Err(err), None, Some(expected)) => {
                    let got = match err {
                        EndpointSetError::DuplicateOrigin {
                            origin,
                            first,
                            duplicate,
                        } => fixture::SetError::DuplicateOrigin {
                            origin: origin.to_string(),
                            first,
                            duplicate,
                        },
                        EndpointSetError::MixedScheme {
                            index,
                            expected,
                            found,
                        } => fixture::SetError::MixedScheme {
                            index,
                            expected,
                            found,
                        },
                        EndpointSetError::NotAnOrigin { index, reason, .. } => {
                            fixture::SetError::NotAnOrigin {
                                index,
                                reason: reason.to_string(),
                            }
                        }
                    };
                    assert_eq!(&got, expected, "{}", row.name);
                }
                (got, _, _) => panic!("{}: unexpected {got:?}", row.name),
            }
        }
    }

    /// What one simulated attempt returned.
    #[derive(Clone, Copy, Debug)]
    enum Sim {
        /// An accepted non-success status, returned as a success.
        Accepted(u16),
        Api(u16),
        Transport(fixture::Reason),
        Reset,
    }

    fn sim_outcome(sim: Sim) -> fixture::CallOutcome {
        match sim {
            Sim::Accepted(status) => fixture::CallOutcome::Ok { status },
            Sim::Api(status) => fixture::CallOutcome::Api { status },
            Sim::Transport(reason) => fixture::CallOutcome::Transport { reason },
            Sim::Reset => fixture::CallOutcome::Reset,
        }
    }

    /// Every fixture scenario, run through the real state machine on a
    /// virtual clock: the same decisions the send loop takes, with the
    /// scripted outcomes in place of the network.
    #[test]
    fn fixture_scenarios_on_a_virtual_clock() {
        let fixture = fixture::load();
        assert!(!fixture.scenarios.is_empty());
        let origins: Vec<EndpointOrigin> = (0..8)
            .map(|i| origin(&format!("https://e{i}.example")))
            .collect();
        for scenario in &fixture.scenarios {
            let policy = scenario.policy.as_ref().map(fixture::Policy::to_policy);
            let method = reqwest::Method::from_bytes(scenario.request.method.as_bytes()).unwrap();
            let allowed = is_idempotent(&method) || scenario.request.retriable;
            let policy = policy.filter(|_| allowed);
            let deadline = scenario.policy.as_ref().and_then(|p| p.deadline_ms).map(ms);
            let retry_on: Vec<StatusCode> = scenario
                .request
                .retry_on_status
                .iter()
                .map(|&code| fixture::status(code))
                .collect();
            let mut preferred = 0;
            for (n, call) in scenario.calls.iter().enumerate() {
                let ctx = format!("{} call {n}", scenario.name);
                assert_eq!(preferred, call.preferred_before, "{ctx}");
                let mut clock = Duration::ZERO;
                let left = |clock: Duration| deadline.map(|d| d.saturating_sub(clock));
                let mut draws = call.draws.iter().copied();
                let mut script = call.attempts.iter();
                let mut observed = Vec::new();
                let mut failover = Failover::new(scenario.endpoints, preferred);
                let mut last = None;
                let finish = |stop: Stop, last: Option<Sim>, failover: &Failover, clock| match (
                    stop, last,
                ) {
                    (Stop::Raw, Some(sim)) | (Stop::Exhausted, Some(sim @ Sim::Accepted(_))) => {
                        sim_outcome(sim)
                    }
                    (Stop::Exhausted, Some(sim)) => {
                        let trace =
                            failover.trace(&origins, ClientError::Config(String::new()), clock);
                        let attempts = trace
                            .attempts
                            .iter()
                            .map(|a| fixture::Traced {
                                endpoint: origins.iter().position(|o| *o == a.endpoint).unwrap(),
                                outcome: fixture_reason(a.outcome),
                                rotated: a.rotated,
                            })
                            .collect();
                        let last = match sim {
                            Sim::Api(status) => fixture::LastError::Api { status },
                            Sim::Transport(reason) => fixture::LastError::Transport { reason },
                            Sim::Accepted(_) | Sim::Reset => unreachable!(),
                        };
                        fixture::CallOutcome::EndpointsExhausted {
                            attempts,
                            last,
                            proves_not_processed: trace.proves_not_processed(),
                        }
                    }
                    (_, _) => fixture::CallOutcome::DeadlineExceeded {
                        attempts: failover.attempts(),
                    },
                };
                let outcome = loop {
                    let attempt = match failover.begin(left(clock)) {
                        Ok(attempt) => attempt,
                        Err(stop) => break finish(stop, last, &failover, clock),
                    };
                    observed.extend(attempt.resend.map(|resend| match resend {
                        Resend::Rotation { from, reason } => fixture::Observed::Rotation {
                            left: from,
                            reason: fixture_reason(reason),
                        },
                        Resend::Retry { reason } => fixture::Observed::Retry {
                            endpoint: attempt.endpoint,
                            reason: fixture_reason(reason),
                            attempt: attempt.number,
                        },
                    }));
                    let scripted = script
                        .next()
                        .unwrap_or_else(|| panic!("{ctx}: an attempt the fixture does not script"));
                    assert_eq!(attempt.endpoint, scripted.endpoint, "{ctx}");
                    let own = scenario.attempt_timeout_ms.map(ms);
                    let (sim, outcome) = match scripted.result {
                        fixture::ScriptedResult::Status {
                            status,
                            retry_after_s,
                        } => {
                            clock += ms(scripted.latency_ms);
                            let code = fixture::status(status);
                            if code.is_success() {
                                preferred = attempt.endpoint;
                                break fixture::CallOutcome::Ok { status };
                            }
                            let accepted = scenario.request.accept_status.contains(&status);
                            let retry_after = retry_after_s.map(Duration::from_secs);
                            let outcome =
                                Outcome::of_status(code, retry_after, accepted, &retry_on);
                            if !outcome.is_rotation() {
                                preferred = attempt.endpoint;
                            }
                            let sim = if accepted {
                                Sim::Accepted(status)
                            } else {
                                Sim::Api(status)
                            };
                            (sim, outcome)
                        }
                        fixture::ScriptedResult::Connect => {
                            clock += ms(scripted.latency_ms);
                            (
                                Sim::Transport(scripted.result.reason().unwrap()),
                                Outcome::of_transport(true, false),
                            )
                        }
                        fixture::ScriptedResult::Reset => {
                            clock += ms(scripted.latency_ms);
                            (Sim::Reset, Outcome::of_transport(false, false))
                        }
                        fixture::ScriptedResult::Stall => {
                            clock += attempt_timeout(None, own, None, left(clock))
                                .unwrap_or_else(|| panic!("{ctx}: a stall needs a bound"));
                            (
                                Sim::Transport(scripted.result.reason().unwrap()),
                                Outcome::of_transport(false, true),
                            )
                        }
                    };
                    // Jitter `none` ignores the draw, so those scenarios list none.
                    let draw = || {
                        if call.draws.is_empty() {
                            0.0
                        } else {
                            draws
                                .next()
                                .unwrap_or_else(|| panic!("{ctx}: out of draws"))
                        }
                    };
                    match failover.after(outcome, policy.as_ref(), draw, left(clock)) {
                        Ok(next) => {
                            let step = match next {
                                Next::Now => fixture::NextStep::Now,
                                Next::After(pause) => {
                                    clock += pause;
                                    fixture::NextStep::After {
                                        pause_ms: u64::try_from(pause.as_millis()).unwrap(),
                                    }
                                }
                            };
                            assert_eq!(Some(step), scripted.next, "{ctx}");
                            last = Some(sim);
                        }
                        Err(stop) => {
                            assert_eq!(scripted.next, None, "{ctx}: stopped early");
                            break finish(stop, Some(sim), &failover, clock);
                        }
                    }
                };
                assert!(
                    script.next().is_none(),
                    "{ctx}: scripted attempts left unsent"
                );
                assert_eq!(outcome, call.outcome, "{ctx}");
                assert_eq!(observed, call.observed, "{ctx}");
                assert_eq!(preferred, call.preferred_after, "{ctx}");
                assert_eq!(draws.next(), None, "{ctx}: draws left unused");
            }
        }
    }
}
