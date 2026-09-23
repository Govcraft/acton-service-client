//! The [`ServiceClient`] and its builder.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::ClientError;
use crate::failover::{Endpoint, EndpointOrigin, RetryObserver, validate_set};
use crate::health::{HealthResponse, ReadinessResponse};
use crate::request::RequestBuilder;
use crate::retry::RetryPolicy;
use crate::versioning::ApiVersion;

/// The builder's timeout until [`ServiceClientBuilder::timeout`] or
/// [`ServiceClientBuilder::no_timeout`] changes it.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How one endpoint of the set is reached.
pub(crate) struct Slot {
    pub(crate) http: reqwest::Client,
}

/// Shared, cheaply-cloneable client configuration.
pub(crate) struct Inner {
    /// The endpoint set in configured order, the base URL first. One entry
    /// for a client without failover endpoints.
    pub(crate) origins: Vec<EndpointOrigin>,
    /// How to reach each entry of `origins`, index for index.
    pub(crate) slots: Vec<Slot>,
    /// The endpoint the next call starts at (see
    /// [`ServiceClient::preferred_endpoint`]).
    pub(crate) preferred: AtomicUsize,
    pub(crate) observer: Option<Arc<dyn RetryObserver>>,
    pub(crate) base_url: String,
    pub(crate) base_path: String,
    pub(crate) version: ApiVersion,
    pub(crate) retry: Option<RetryPolicy>,
    /// The client-wide per-attempt timeout
    /// ([`ServiceClientBuilder::attempt_timeout`]), for any HTTP client.
    pub(crate) attempt_timeout: Option<Duration>,
    /// The builder's [`timeout`](ServiceClientBuilder::timeout), sent with
    /// every attempt on every endpoint, built or supplied client alike;
    /// `None` only after [`ServiceClientBuilder::no_timeout`].
    pub(crate) timeout: Option<Duration>,
    /// Headers applied to every request (bearer token plus any
    /// [`ServiceClientBuilder::default_header`]). Held here rather than baked
    /// into the [`reqwest::Client`] so they apply equally to a client supplied
    /// via [`ServiceClientBuilder::with_http_client`].
    pub(crate) default_headers: HeaderMap,
}

impl Inner {
    /// The endpoint the next call starts at.
    pub(crate) fn preferred(&self) -> usize {
        self.preferred.load(Ordering::Relaxed) % self.origins.len().max(1)
    }

    /// Start the next call at `endpoint`, which just gave a definitive
    /// answer.
    pub(crate) fn prefer(&self, endpoint: usize) {
        self.preferred.store(endpoint, Ordering::Relaxed);
    }
}

/// A typed async HTTP client for services built on `acton-service`.
///
/// Construct one with [`ServiceClient::builder`]. The client is cheap to clone
/// (it shares an internal [`reqwest::Client`] and configuration behind an
/// `Arc`), so a single instance can be shared across tasks.
///
/// # Examples
///
/// ```no_run
/// use acton_service_client::{ApiVersion, ServiceClient};
/// # async fn run() -> Result<(), acton_service_client::ClientError> {
/// let client = ServiceClient::builder("https://api.example.com")
///     .api_version(ApiVersion::V1)
///     .bearer_token("secret-token")
///     .build()?;
///
/// # #[derive(serde::Deserialize)]
/// # struct User { id: u64 }
/// let user: User = client.get("users/42").await?;
/// # let _ = user;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct ServiceClient {
    pub(crate) inner: Arc<Inner>,
}

impl std::fmt::Debug for ServiceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceClient")
            .field("base_url", &self.inner.base_url)
            .field("endpoints", &self.inner.origins)
            .field("base_path", &self.inner.base_path)
            .field("version", &self.inner.version)
            .field("retry", &self.inner.retry)
            .finish_non_exhaustive()
    }
}

impl ServiceClient {
    /// Start building a client for the given base URL (scheme + host, e.g.
    /// `https://api.example.com`).
    #[must_use]
    pub fn builder(base_url: impl Into<String>) -> ServiceClientBuilder {
        ServiceClientBuilder::new(base_url)
    }

    /// The configured API version.
    #[must_use]
    pub fn api_version(&self) -> ApiVersion {
        self.inner.version
    }

    /// The endpoint set, normalized, in configured order: the base URL's
    /// origin first, then each
    /// [`failover_endpoint`](ServiceClientBuilder::failover_endpoint). A
    /// client without failover endpoints has exactly one.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ServiceClient;
    ///
    /// let client = ServiceClient::builder("https://a.example.com")
    ///     .failover_endpoint("https://b.example.com")
    ///     .build()
    ///     .expect("a valid endpoint set");
    /// assert_eq!(client.endpoints()[1].to_string(), "https://b.example.com:443");
    /// ```
    #[must_use]
    pub fn endpoints(&self) -> &[EndpointOrigin] {
        &self.inner.origins
    }

    /// The endpoint the next call starts at.
    ///
    /// It starts as the base URL's, and it is **sticky**: after each call it
    /// moves to the last endpoint that gave a definitive answer, meaning any
    /// response whose status is not a rotation status for that request (a
    /// success, and also a `4xx` refusal). A connect failure, a timeout, and
    /// a rotation status such as `421` never move it. So once the primary is
    /// down, calls go straight to the backup that answered, instead of paying
    /// for the dead primary first on every call, and they move back only when
    /// the backup in turn fails over. Clones of a client share it.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ServiceClient;
    ///
    /// let client = ServiceClient::builder("https://a.example.com")
    ///     .failover_endpoint("https://b.example.com")
    ///     .build()
    ///     .expect("a valid endpoint set");
    /// assert_eq!(client.preferred_endpoint(), &client.endpoints()[0]);
    /// ```
    #[must_use]
    pub fn preferred_endpoint(&self) -> &EndpointOrigin {
        &self.inner.origins[self.inner.preferred()]
    }

    /// Begin a versioned request to `path` (relative to `{base_path}/{version}`).
    ///
    /// This is the escape hatch: attach query parameters, extra headers, a
    /// per-request [`crate::RequestContext`], a JSON body, or a retriable flag,
    /// then call one of the `send_*` methods.
    #[must_use]
    pub fn request(&self, method: Method, path: impl Into<String>) -> RequestBuilder {
        RequestBuilder::new(self.clone(), method, path.into(), true)
    }

    /// Begin an **unversioned** request to `path` (relative to the base URL).
    #[must_use]
    pub fn request_unversioned(&self, method: Method, path: impl Into<String>) -> RequestBuilder {
        RequestBuilder::new(self.clone(), method, path.into(), false)
    }

    /// `GET {base_path}/{version}/{path}`, decoding a JSON body into `T`.
    pub async fn get<T: DeserializeOwned>(
        &self,
        path: impl Into<String>,
    ) -> Result<T, ClientError> {
        self.request(Method::GET, path).send_json().await
    }

    /// `POST {base_path}/{version}/{path}` with a JSON `body`, decoding the
    /// `200`/`201` response body into `T`.
    pub async fn post<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: impl Into<String>,
        body: &B,
    ) -> Result<T, ClientError> {
        self.request(Method::POST, path)
            .json(body)?
            .send_json()
            .await
    }

    /// `PUT {base_path}/{version}/{path}` with a JSON `body`, decoding the
    /// response body into `T`.
    pub async fn put<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: impl Into<String>,
        body: &B,
    ) -> Result<T, ClientError> {
        self.request(Method::PUT, path)
            .json(body)?
            .send_json()
            .await
    }

    /// `PATCH {base_path}/{version}/{path}` with a JSON `body`, decoding the
    /// response body into `T`.
    pub async fn patch<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: impl Into<String>,
        body: &B,
    ) -> Result<T, ClientError> {
        self.request(Method::PATCH, path)
            .json(body)?
            .send_json()
            .await
    }

    /// `DELETE {base_path}/{version}/{path}`, accepting `200`/`201`/`204` and
    /// discarding any body.
    pub async fn delete(&self, path: impl Into<String>) -> Result<(), ClientError> {
        self.request(Method::DELETE, path).send_no_content().await
    }

    /// `GET /health` (unversioned).
    pub async fn health(&self) -> Result<HealthResponse, ClientError> {
        self.request_unversioned(Method::GET, "health")
            .send_json()
            .await
    }

    /// `GET /ready` (unversioned).
    ///
    /// A `503 Service Unavailable` readiness response is decoded into
    /// [`ReadinessResponse`] rather than raised as an error, since the body
    /// carries the same shape whether or not the service is ready.
    pub async fn ready(&self) -> Result<ReadinessResponse, ClientError> {
        self.request_unversioned(Method::GET, "ready")
            .accept_status(reqwest::StatusCode::SERVICE_UNAVAILABLE)
            .send_json()
            .await
    }
}

/// Builder for [`ServiceClient`].
///
/// # Examples
///
/// ```
/// use acton_service_client::{ApiVersion, RetryPolicy, ServiceClient};
/// use std::time::Duration;
///
/// let client = ServiceClient::builder("https://api.example.com")
///     .api_version(ApiVersion::V2)
///     .base_path("/api")
///     .timeout(Duration::from_secs(15))
///     .retry(RetryPolicy::default())
///     .build()
///     .expect("valid base url");
/// assert_eq!(client.api_version(), ApiVersion::V2);
/// ```
pub struct ServiceClientBuilder {
    base_url: String,
    base_path: String,
    version: ApiVersion,
    bearer_token: Option<String>,
    timeout: Option<Duration>,
    attempt_timeout: Option<Duration>,
    retry: Option<RetryPolicy>,
    default_headers: HeaderMap,
    http_client: Option<reqwest::Client>,
    failovers: Vec<Endpoint>,
    observer: Option<Arc<dyn RetryObserver>>,
}

impl ServiceClientBuilder {
    fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            base_path: "/api".to_string(),
            version: ApiVersion::V1,
            bearer_token: None,
            timeout: Some(DEFAULT_TIMEOUT),
            attempt_timeout: None,
            retry: None,
            default_headers: HeaderMap::new(),
            http_client: None,
            failovers: Vec::new(),
            observer: None,
        }
    }

    /// Set the API version for versioned routes (default [`ApiVersion::V1`]).
    #[must_use]
    pub fn api_version(mut self, version: ApiVersion) -> Self {
        self.version = version;
        self
    }

    /// Set the base path prefixing versioned routes (default `/api`).
    #[must_use]
    pub fn base_path(mut self, base_path: impl Into<String>) -> Self {
        self.base_path = base_path.into();
        self
    }

    /// Attach a bearer token, sent as `Authorization: Bearer <token>` on every
    /// request. The token is opaque to the client (JWT or PASETO).
    #[must_use]
    pub fn bearer_token(mut self, token: impl Into<String>) -> Self {
        self.bearer_token = Some(token.into());
        self
    }

    /// Bound every attempt of every request (default 30s).
    ///
    /// Each attempt, including each retry, is sent with this as its reqwest
    /// per-request timeout, so it holds whether the builder constructs the
    /// HTTP client or you supply one through
    /// [`with_http_client`](Self::with_http_client) or
    /// [`Endpoint::with_http_client`]: on a supplied client it replaces the
    /// client's own timeout. Under a deadline ([`RetryPolicy::deadline`] or
    /// [`RequestBuilder::deadline_at`](crate::RequestBuilder::deadline_at)) an
    /// attempt gets the smaller of this and the time remaining.
    /// [`attempt_timeout`](Self::attempt_timeout) and
    /// [`RequestBuilder::timeout`](crate::RequestBuilder::timeout) take
    /// precedence over it. To send attempts with no timeout from the builder,
    /// say so with [`no_timeout`](Self::no_timeout).
    ///
    /// When it fires, the call fails with a [`ClientError::Transport`] whose
    /// error reports `is_timeout()`.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ServiceClient;
    /// use std::time::Duration;
    ///
    /// let client = ServiceClient::builder("https://api.example.com")
    ///     .timeout(Duration::from_secs(5))
    ///     .build()
    ///     .expect("valid base url");
    /// # let _ = client;
    /// ```
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Send attempts without the builder's timeout: the explicit opt-out from
    /// the 30s default and from any [`timeout`](Self::timeout) set before.
    ///
    /// A client the builder constructs then has no timeout at all, and a
    /// supplied client (through [`with_http_client`](Self::with_http_client)
    /// or [`Endpoint::with_http_client`]) keeps its own, whatever it is
    /// (reqwest's default is none). An attempt can then wait forever on a
    /// server that accepts the connection and never answers, so prefer a
    /// longer [`timeout`](Self::timeout), or bound the call with a
    /// [`RetryPolicy::deadline`].
    ///
    /// Still in force: [`attempt_timeout`](Self::attempt_timeout),
    /// [`RequestBuilder::timeout`](crate::RequestBuilder::timeout), and a
    /// deadline, which bounds every attempt by the time remaining (on a
    /// supplied client, in place of its own timeout). A later
    /// [`timeout`](Self::timeout) call sets a timeout again.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ServiceClient;
    /// use std::time::Duration;
    ///
    /// // A long-poll client that relies on its own 10-minute timeout.
    /// let long_poll = acton_service_client::reqwest::Client::builder()
    ///     .timeout(Duration::from_secs(600))
    ///     .build()
    ///     .expect("a reqwest client");
    /// let client = ServiceClient::builder("https://api.example.com")
    ///     .with_http_client(long_poll)
    ///     .no_timeout()
    ///     .build()
    ///     .expect("valid base url");
    /// # let _ = client;
    /// ```
    #[must_use]
    pub fn no_timeout(mut self) -> Self {
        self.timeout = None;
        self
    }

    /// Set a client-wide per-attempt timeout, for a built **or supplied** HTTP
    /// client.
    ///
    /// Each attempt, including each retry, is sent with this as its reqwest
    /// per-request timeout, clamped to the time remaining under a deadline. It
    /// overrides the builder's [`timeout`](Self::timeout), and a
    /// [`no_timeout`](Self::no_timeout), for every request of the client.
    ///
    /// The timeout for one attempt is chosen in this order, and in every case
    /// clamped to the time remaining before the deadline, if there is one:
    ///
    /// 1. the request's [`RequestBuilder::timeout`](crate::RequestBuilder::timeout);
    /// 2. this client-wide `attempt_timeout`;
    /// 3. the builder's [`timeout`](Self::timeout) (30s unless changed), on a
    ///    built or supplied client alike;
    /// 4. after [`no_timeout`](Self::no_timeout), the remaining budget itself.
    ///
    /// Only after [`no_timeout`](Self::no_timeout), with no deadline and
    /// neither of the first two, is an attempt sent without a timeout of its
    /// own: a supplied client's own timeout then applies.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{RetryPolicy, ServiceClient};
    /// use std::time::Duration;
    /// # fn make_tls_client() -> acton_service_client::reqwest::Client { acton_service_client::reqwest::Client::new() }
    ///
    /// // A supplied mTLS client: 5s per attempt, 15s for the whole call.
    /// let client = ServiceClient::builder("https://api.example.com")
    ///     .with_http_client(make_tls_client())
    ///     .attempt_timeout(Duration::from_secs(5))
    ///     .retry(RetryPolicy::default().deadline(Duration::from_secs(15)))
    ///     .build()
    ///     .expect("valid base url");
    /// # let _ = client;
    /// ```
    #[must_use]
    pub fn attempt_timeout(mut self, timeout: Duration) -> Self {
        self.attempt_timeout = Some(timeout);
        self
    }

    /// Enable retries with the given policy (retries are off by default).
    #[must_use]
    pub fn retry(mut self, policy: RetryPolicy) -> Self {
        self.retry = Some(policy);
        self
    }

    /// Add a default header sent on every request.
    #[must_use]
    pub fn default_header(mut self, name: HeaderName, value: HeaderValue) -> Self {
        self.default_headers.insert(name, value);
        self
    }

    /// Supply a pre-configured [`reqwest::Client`] instead of letting the
    /// builder construct one.
    ///
    /// This is the escape hatch for any reqwest capability the builder does not
    /// surface: a client certificate for mutual TLS (`use_rustls_tls()` +
    /// [`Identity`](reqwest::Identity)), a custom root store, a proxy, a shared
    /// connection pool, or a custom DNS resolver. In particular it is how you
    /// pair this crate with an `acton-service` listener that verifies client
    /// certificates — build a reqwest client carrying the client identity and
    /// hand it in here.
    ///
    /// The [`bearer_token`](Self::bearer_token) and
    /// [`default_header`](Self::default_header) values still apply: they are sent
    /// per-request rather than baked into the client, so they work identically
    /// whether or not a client is supplied.
    ///
    /// # The builder's timeout replaces the supplied client's
    ///
    /// The builder's [`timeout`](Self::timeout) (30s unless changed) bounds
    /// every attempt on the supplied client too: it is sent as each request's
    /// reqwest timeout, which replaces the client's own. This crate cannot
    /// read a supplied client's timeout, so it never relies on one being set:
    /// a client with none can no longer hang a request forever. To bound
    /// attempts differently, set [`timeout`](Self::timeout) or
    /// [`attempt_timeout`](Self::attempt_timeout) here (or
    /// [`RequestBuilder::timeout`](crate::RequestBuilder::timeout) on one
    /// request). To keep the supplied client's own timeout instead, opt out
    /// with [`no_timeout`](Self::no_timeout); under a deadline
    /// ([`RetryPolicy::deadline`] or
    /// [`RequestBuilder::deadline_at`](crate::RequestBuilder::deadline_at))
    /// the time remaining still replaces it on every attempt.
    ///
    /// # With an endpoint set
    ///
    /// The supplied client serves every
    /// [`failover_endpoint`](Self::failover_endpoint) that was not given its
    /// own. Build it with [`reqwest::redirect::Policy::none`] (or a policy
    /// limited to the set): this crate cannot change a supplied client's
    /// redirect policy, so a redirect it follows out of the set fails the call
    /// with [`ClientError::Config`] rather than sending outside the set.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use acton_service_client::ServiceClient;
    /// # fn make_tls_client() -> reqwest::Client { unimplemented!() }
    /// # fn run() -> Result<(), acton_service_client::ClientError> {
    /// let mtls: reqwest::Client = make_tls_client();
    /// let client = ServiceClient::builder("https://api.example.com")
    ///     .bearer_token("token")
    ///     .with_http_client(mtls)
    ///     .build()?;
    /// # let _ = client;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn with_http_client(mut self, client: reqwest::Client) -> Self {
        self.http_client = Some(client);
        self
    }

    /// Add a failover endpoint after the base URL and any added before it.
    ///
    /// The base URL and every failover endpoint form one ordered **endpoint
    /// set** serving the same API. A failover endpoint is a bare origin
    /// (`scheme://host[:port]`): each request uses the base URL's path,
    /// version and query on whichever endpoint it is sent to. Pass an
    /// [`Endpoint`] built with [`Endpoint::with_http_client`] to give one
    /// endpoint its own HTTP client (for example its own TLS configuration).
    ///
    /// # How a call fails over
    ///
    /// Only when retries apply to the request: a [`RetryPolicy`] is set with
    /// [`retry`](Self::retry), and the method is idempotent or the request is
    /// marked [`retriable(true)`](crate::RequestBuilder::retriable). A request
    /// that is not retried is sent to one endpoint only, exactly as without a
    /// set. Then:
    ///
    /// - A status listed with
    ///   [`retry_on_status`](crate::RequestBuilder::retry_on_status), a connect
    ///   failure, or an attempt timeout moves the next attempt to the next
    ///   endpoint, at once.
    /// - After a full cycle of such outcomes, the client pauses with the
    ///   policy's backoff (or the smallest server `Retry-After`, when every
    ///   endpoint in the cycle sent one), then starts the next cycle.
    /// - A status retriable by default but not listed (`429`, `502`, `503`,
    ///   `504`) is retried on the same endpoint, and every other answer is
    ///   returned, exactly as without a set.
    /// - One deadline covers every attempt on every endpoint, and
    ///   `max_attempts` counts every send. A call that runs out of budget after
    ///   failing over returns [`ClientError::EndpointsExhausted`] with the
    ///   trace of what each endpoint answered.
    /// - The next call starts where this one got its answer (see
    ///   [`ServiceClient::preferred_endpoint`]).
    ///
    /// The client never sends outside the set. A client this builder
    /// constructs follows a redirect only to an origin in the set; a supplied
    /// client that follows one elsewhere fails the call with
    /// [`ClientError::Config`].
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{RetryPolicy, ServiceClient};
    /// use std::time::Duration;
    ///
    /// let client = ServiceClient::builder("https://a.example.com")
    ///     .failover_endpoint("https://b.example.com")
    ///     .attempt_timeout(Duration::from_secs(5))
    ///     .retry(RetryPolicy::default().deadline(Duration::from_secs(15)))
    ///     .build()
    ///     .expect("a valid endpoint set");
    /// assert_eq!(client.endpoints().len(), 2);
    /// ```
    #[must_use]
    pub fn failover_endpoint(mut self, endpoint: impl Into<Endpoint>) -> Self {
        self.failovers.push(endpoint.into());
        self
    }

    /// Add several failover endpoints, in order; see
    /// [`failover_endpoint`](Self::failover_endpoint).
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ServiceClient;
    ///
    /// let client = ServiceClient::builder("https://a.example.com")
    ///     .failover_endpoints(["https://b.example.com", "https://c.example.com"])
    ///     .build()
    ///     .expect("a valid endpoint set");
    /// assert_eq!(client.endpoints().len(), 3);
    /// ```
    #[must_use]
    pub fn failover_endpoints<E: Into<Endpoint>>(
        mut self,
        endpoints: impl IntoIterator<Item = E>,
    ) -> Self {
        self.failovers.extend(endpoints.into_iter().map(Into::into));
        self
    }

    /// Be notified of every re-send: each rotation to the next endpoint and
    /// each retry on the same one, on a single-endpoint client too. See
    /// [`RetryObserver`] for when each is called and how to count them as
    /// metrics.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::{EndpointOrigin, RetryObserver, RetryReason, ServiceClient};
    ///
    /// struct Log;
    ///
    /// impl RetryObserver for Log {
    ///     fn on_rotation(&self, left: &EndpointOrigin, reason: RetryReason) {
    ///         eprintln!("left {left}: {reason}");
    ///     }
    ///
    ///     fn on_retry(&self, endpoint: &EndpointOrigin, reason: RetryReason, attempt: u32) {
    ///         eprintln!("attempt {attempt} to {endpoint} after {reason}");
    ///     }
    /// }
    ///
    /// let client = ServiceClient::builder("https://a.example.com")
    ///     .failover_endpoint("https://b.example.com")
    ///     .retry_observer(Log)
    ///     .build()
    ///     .expect("a valid endpoint set");
    /// # let _ = client;
    /// ```
    #[must_use]
    pub fn retry_observer(mut self, observer: impl RetryObserver) -> Self {
        self.observer = Some(Arc::new(observer));
        self
    }

    /// Validate configuration and build the [`ServiceClient`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the base URL is not a valid absolute
    /// `http`/`https` URL, or if the bearer token cannot be encoded as a header
    /// value, or if the underlying HTTP client cannot be constructed. A client
    /// supplied via [`with_http_client`](Self::with_http_client) is used as-is
    /// (only its requests carry the builder's timeout), so the last case cannot
    /// arise on that path.
    ///
    /// Returns [`ClientError::InvalidEndpoints`] if a
    /// [`failover_endpoint`](Self::failover_endpoint) is not a bare origin,
    /// uses a different scheme from the base URL, or repeats an origin already
    /// in the set once normalized.
    pub fn build(mut self) -> Result<ServiceClient, ClientError> {
        let parsed = url::Url::parse(&self.base_url).map_err(|e| {
            ClientError::Config(format!("invalid base URL {:?}: {e}", self.base_url))
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(ClientError::Config(format!(
                "base URL scheme must be http or https, got {:?}",
                parsed.scheme()
            )));
        }
        if parsed.host().is_none() {
            return Err(ClientError::Config(format!(
                "base URL must have a host: {:?}",
                self.base_url
            )));
        }

        if let Some(token) = &self.bearer_token {
            let mut value = HeaderValue::from_str(&format!("Bearer {token}")).map_err(|e| {
                ClientError::Config(format!("bearer token is not a valid header value: {e}"))
            })?;
            value.set_sensitive(true);
            self.default_headers
                .insert(reqwest::header::AUTHORIZATION, value);
        }

        let primary = EndpointOrigin::from_url(&parsed).ok_or_else(|| {
            ClientError::Config(format!(
                "base URL has no known port for its scheme: {:?}",
                self.base_url
            ))
        })?;
        let failover_urls: Vec<&str> = self.failovers.iter().map(Endpoint::url).collect();
        let origins = validate_set(primary, &failover_urls)?;

        // The timeout is not baked into a built client: it is sent with every
        // attempt (see `attempt_timeout`), the one mechanism that also binds a
        // supplied client.
        let shared = match self.http_client {
            Some(client) => Slot { http: client },
            None => {
                let mut builder = reqwest::Client::builder();
                if origins.len() > 1 {
                    builder = builder.redirect(in_set_redirects(origins.clone()));
                }
                Slot {
                    http: builder.build().map_err(|e| {
                        ClientError::Config(format!("failed to build HTTP client: {e}"))
                    })?,
                }
            }
        };
        let mut slots = Vec::with_capacity(origins.len());
        for endpoint in self.failovers {
            slots.push(Slot {
                http: endpoint.http.unwrap_or_else(|| shared.http.clone()),
            });
        }
        slots.insert(0, shared);

        Ok(ServiceClient {
            inner: Arc::new(Inner {
                origins,
                slots,
                preferred: AtomicUsize::new(0),
                observer: self.observer,
                base_url: self.base_url,
                base_path: self.base_path,
                version: self.version,
                retry: self.retry,
                attempt_timeout: self.attempt_timeout,
                timeout: self.timeout,
                default_headers: self.default_headers,
            }),
        })
    }
}

/// The most redirects followed for one attempt, as reqwest's default policy.
const MAX_REDIRECTS: usize = 10;

/// A redirect policy that follows a redirect only to an origin in `set`.
fn in_set_redirects(set: Vec<EndpointOrigin>) -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(move |attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            return attempt.error(format!("stopped after {MAX_REDIRECTS} redirects"));
        }
        match redirect_verdict(&set, attempt.url()) {
            Ok(()) => attempt.follow(),
            Err(refusal) => attempt.error(refusal),
        }
    })
}

/// `Ok` when `target` is in `set`, else a refusal naming it.
pub(crate) fn redirect_verdict(set: &[EndpointOrigin], target: &url::Url) -> Result<(), String> {
    match EndpointOrigin::from_url(target) {
        Some(origin) if set.contains(&origin) => Ok(()),
        _ => Err(format!(
            "refused a redirect to {target}, outside the endpoint set; point the redirect at an \
             endpoint in the set, or add that origin with failover_endpoint"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_rejects_non_http_scheme() {
        let err = ServiceClient::builder("ftp://example.com")
            .build()
            .unwrap_err();
        assert!(matches!(err, ClientError::Config(_)));
    }

    #[test]
    fn build_rejects_garbage_url() {
        let err = ServiceClient::builder("not a url").build().unwrap_err();
        assert!(matches!(err, ClientError::Config(_)));
    }

    #[test]
    fn build_rejects_missing_host() {
        let err = ServiceClient::builder("http://").build().unwrap_err();
        assert!(matches!(err, ClientError::Config(_)));
    }

    #[test]
    fn build_accepts_valid_url_with_defaults() {
        let client = ServiceClient::builder("https://api.example.com")
            .build()
            .unwrap();
        assert_eq!(client.api_version(), ApiVersion::V1);
        assert_eq!(client.inner.base_path, "/api");
        assert!(client.inner.retry.is_none());
        assert_eq!(client.inner.timeout, Some(Duration::from_secs(30)));
        assert_eq!(client.endpoints().len(), 1);
        assert_eq!(client.inner.attempt_timeout, None);
    }

    #[test]
    fn build_rejects_control_char_token() {
        let err = ServiceClient::builder("https://api.example.com")
            .bearer_token("bad\ntoken")
            .build()
            .unwrap_err();
        assert!(matches!(err, ClientError::Config(_)));
    }

    #[test]
    fn bearer_token_is_stored_as_a_default_header() {
        let client = ServiceClient::builder("https://api.example.com")
            .bearer_token("secret")
            .build()
            .unwrap();
        let auth = client
            .inner
            .default_headers
            .get(reqwest::header::AUTHORIZATION)
            .unwrap();
        assert_eq!(auth, "Bearer secret");
        assert!(auth.is_sensitive());
    }

    #[test]
    fn attempt_timeout_is_kept_for_built_and_supplied_clients() {
        let built = ServiceClient::builder("https://api.example.com")
            .attempt_timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        assert_eq!(built.inner.attempt_timeout, Some(Duration::from_secs(2)));
        let supplied = ServiceClient::builder("https://api.example.com")
            .with_http_client(reqwest::Client::new())
            .attempt_timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        assert_eq!(supplied.inner.attempt_timeout, Some(Duration::from_secs(2)));
        assert_eq!(supplied.inner.timeout, Some(DEFAULT_TIMEOUT));
    }

    #[test]
    fn no_timeout_is_explicit_and_a_later_timeout_restores_one() {
        let none = ServiceClient::builder("https://api.example.com")
            .with_http_client(reqwest::Client::new())
            .no_timeout()
            .build()
            .unwrap();
        assert_eq!(none.inner.timeout, None);
        let again = ServiceClient::builder("https://api.example.com")
            .no_timeout()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap();
        assert_eq!(again.inner.timeout, Some(Duration::from_secs(5)));
    }

    #[test]
    fn with_http_client_builds_and_preserves_default_headers() {
        let supplied = reqwest::Client::new();
        let client = ServiceClient::builder("https://api.example.com")
            .bearer_token("secret")
            .with_http_client(supplied)
            .build()
            .unwrap();
        // The builder's default timeout binds a supplied client too.
        assert_eq!(client.inner.timeout, Some(DEFAULT_TIMEOUT));
        assert_eq!(client.inner.attempt_timeout, None);
        // The bearer token is carried per-request, not baked into the supplied
        // client, so it survives on the custom-client path.
        assert!(
            client
                .inner
                .default_headers
                .contains_key(reqwest::header::AUTHORIZATION)
        );
    }

    #[test]
    fn failover_endpoints_share_the_built_client_unless_given_their_own() {
        let client = ServiceClient::builder("https://a.example.com")
            .timeout(Duration::from_secs(7))
            .failover_endpoint("https://b.example.com")
            .failover_endpoint(
                Endpoint::new("https://c.example.com").with_http_client(reqwest::Client::new()),
            )
            .build()
            .unwrap();
        assert_eq!(client.inner.slots.len(), 3);
        // One timeout for every endpoint, the one with its own client included.
        assert_eq!(client.inner.timeout, Some(Duration::from_secs(7)));
        assert_eq!(client.preferred_endpoint(), &client.endpoints()[0]);
    }

    #[test]
    fn an_invalid_set_is_a_typed_build_error() {
        let err = ServiceClient::builder("https://a.example.com")
            .failover_endpoints(["https://b.example.com", "https://b.example.com:443/"])
            .build()
            .unwrap_err();
        assert!(
            matches!(
                err,
                ClientError::InvalidEndpoints(crate::EndpointSetError::DuplicateOrigin {
                    first: 1,
                    duplicate: 2,
                    ..
                })
            ),
            "{err:?}"
        );
    }

    #[test]
    fn preference_is_shared_by_clones_and_wraps_into_the_set() {
        let client = ServiceClient::builder("https://a.example.com")
            .failover_endpoint("https://b.example.com")
            .build()
            .unwrap();
        let clone = client.clone();
        client.inner.prefer(1);
        assert_eq!(clone.preferred_endpoint().host(), "b.example.com");
    }

    #[test]
    fn redirects_are_judged_by_origin() {
        let set = [
            EndpointOrigin::from_url(&url::Url::parse("https://a.example").unwrap()).unwrap(),
            EndpointOrigin::from_url(&url::Url::parse("https://b.example:8443").unwrap()).unwrap(),
        ];
        let target = |raw: &str| url::Url::parse(raw).unwrap();
        assert!(redirect_verdict(&set, &target("https://B.example:8443/x?y")).is_ok());
        assert!(redirect_verdict(&set, &target("https://a.example:443/")).is_ok());
        let refusal = redirect_verdict(&set, &target("https://b.example/x")).unwrap_err();
        assert!(refusal.contains("outside the endpoint set"), "{refusal}");
        assert!(redirect_verdict(&set, &target("http://a.example/")).is_err());
    }
}
