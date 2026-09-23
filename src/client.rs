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

/// How one endpoint of the set is reached.
pub(crate) struct Slot {
    pub(crate) http: reqwest::Client,
    /// The timeout the builder baked into `http`, or `None` when the client
    /// was supplied (via [`ServiceClientBuilder::with_http_client`] or
    /// [`Endpoint::with_http_client`]), whose own timeout cannot be observed.
    /// Consulted only under a deadline, as the last fallback before
    /// `remaining`.
    pub(crate) timeout: Option<Duration>,
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
    timeout: Duration,
    attempt_timeout: Option<Duration>,
    retry: Option<RetryPolicy>,
    default_headers: HeaderMap,
    http_client: Option<reqwest::Client>,
    failovers: Vec<Endpoint>,
    observer: Option<Arc<dyn RetryObserver>>,
    tls: TlsSettings,
}

/// TLS material for a client the builder constructs, held as PEM until
/// [`ServiceClientBuilder::build`] parses it.
#[derive(Default)]
struct TlsSettings {
    /// Extra trust anchors, one PEM bundle per call.
    roots: Vec<Vec<u8>>,
    /// The client certificate chain followed by its private key, as one PEM.
    identity: Option<Vec<u8>>,
}

impl TlsSettings {
    /// Whether any TLS material was configured.
    fn is_set(&self) -> bool {
        !self.roots.is_empty() || self.identity.is_some()
    }

    /// Applies the material to a reqwest builder. Pure apart from parsing.
    ///
    /// Errors name what failed to parse, never the bytes: an identity PEM
    /// carries a private key.
    fn apply(
        &self,
        mut builder: reqwest::ClientBuilder,
    ) -> Result<reqwest::ClientBuilder, ClientError> {
        for bundle in &self.roots {
            let certificates = reqwest::Certificate::from_pem_bundle(bundle).map_err(|e| {
                ClientError::Config(format!("root certificate PEM could not be used: {e}"))
            })?;
            if certificates.is_empty() {
                return Err(ClientError::Config(
                    "root certificate PEM holds no certificate".to_string(),
                ));
            }
            for certificate in certificates {
                builder = builder.add_root_certificate(certificate);
            }
        }
        if let Some(pem) = &self.identity {
            let identity = reqwest::Identity::from_pem(pem).map_err(|e| {
                ClientError::Config(format!(
                    "client identity PEM could not be used (expected a certificate chain and \
                     its private key): {e}"
                ))
            })?;
            builder = builder.identity(identity);
        }
        Ok(builder)
    }
}

impl ServiceClientBuilder {
    fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            base_path: "/api".to_string(),
            version: ApiVersion::V1,
            bearer_token: None,
            timeout: Duration::from_secs(30),
            attempt_timeout: None,
            retry: None,
            default_headers: HeaderMap::new(),
            http_client: None,
            failovers: Vec::new(),
            observer: None,
            tls: TlsSettings::default(),
        }
    }

    /// Trust the certificates in a PEM bundle, in addition to the built-in
    /// roots, for a client the builder constructs.
    ///
    /// This is the trust-only path: a deployment whose server certificate is
    /// issued by a private CA, reached without a client certificate. Call it
    /// once per bundle; every certificate in each is added. The builder's
    /// [`timeout`](Self::timeout) applies to the client it builds, so a
    /// TLS-configured client is bounded exactly as a plain one is.
    ///
    /// The PEM is parsed by [`build`](Self::build), which fails with
    /// [`ClientError::Config`] if it holds no usable certificate, and also if
    /// a client was supplied with [`with_http_client`](Self::with_http_client),
    /// whose TLS this builder cannot change.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use acton_service_client::ServiceClient;
    /// use std::time::Duration;
    ///
    /// let ca = std::fs::read("deployment-ca.pem").expect("the CA bundle");
    /// let client = ServiceClient::builder("https://ledger.internal:8443")
    ///     .root_certificate_pem(ca)
    ///     .timeout(Duration::from_secs(10))
    ///     .build()
    ///     .expect("a usable CA bundle");
    /// # let _ = client;
    /// ```
    #[must_use]
    pub fn root_certificate_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.tls.roots.push(pem.into());
        self
    }

    /// Present a client certificate, for a client the builder constructs:
    /// mutual TLS against a listener that verifies client certificates.
    ///
    /// `cert_pem` is the certificate chain, leaf first, and `key_pem` its
    /// private key (PKCS#8, PKCS#1 or SEC1). A second call replaces the first.
    /// Combine it with [`root_certificate_pem`](Self::root_certificate_pem)
    /// when the server's certificate is issued by a private CA. The builder's
    /// [`timeout`](Self::timeout) applies to the client it builds.
    ///
    /// The PEM is parsed by [`build`](Self::build), which fails with
    /// [`ClientError::Config`] if the pair cannot be used (the error names what
    /// failed, never the key), and also if a client was supplied with
    /// [`with_http_client`](Self::with_http_client).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use acton_service_client::ServiceClient;
    ///
    /// let cert = std::fs::read("operator.pem").expect("the certificate");
    /// let key = std::fs::read("operator.key").expect("the private key");
    /// let ca = std::fs::read("deployment-ca.pem").expect("the CA bundle");
    /// let client = ServiceClient::builder("https://ledger.internal:8443")
    ///     .identity_pem(cert, key)
    ///     .root_certificate_pem(ca)
    ///     .build()
    ///     .expect("a usable identity");
    /// # let _ = client;
    /// ```
    #[must_use]
    pub fn identity_pem(mut self, cert_pem: impl Into<Vec<u8>>, key_pem: impl AsRef<[u8]>) -> Self {
        let mut pem = cert_pem.into();
        if !pem.ends_with(b"\n") {
            pem.push(b'\n');
        }
        pem.extend_from_slice(key_pem.as_ref());
        self.tls.identity = Some(pem);
        self
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

    /// Set the timeout of the HTTP client the builder constructs (default 30s).
    ///
    /// Each attempt, including each retry, gets this long. Under a deadline
    /// ([`RetryPolicy::deadline`] or
    /// [`RequestBuilder::deadline_at`](crate::RequestBuilder::deadline_at)) an
    /// attempt gets the smaller of this and the time remaining.
    /// [`attempt_timeout`](Self::attempt_timeout) and
    /// [`RequestBuilder::timeout`](crate::RequestBuilder::timeout) take
    /// precedence over it.
    ///
    /// Ignored with a client supplied via
    /// [`with_http_client`](Self::with_http_client); use
    /// [`attempt_timeout`](Self::attempt_timeout) there.
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
        self.timeout = timeout;
        self
    }

    /// Set a client-wide per-attempt timeout, for a built **or supplied** HTTP
    /// client.
    ///
    /// Each attempt, including each retry, is sent with this as its reqwest
    /// per-request timeout, clamped to the time remaining under a deadline.
    /// This is the way to bound attempts on a client supplied via
    /// [`with_http_client`](Self::with_http_client), whose own timeout is
    /// otherwise replaced by the remaining budget under a deadline.
    ///
    /// The timeout for one attempt is chosen in this order, and in every case
    /// clamped to the time remaining before the deadline, if there is one:
    ///
    /// 1. the request's [`RequestBuilder::timeout`](crate::RequestBuilder::timeout);
    /// 2. this client-wide `attempt_timeout`;
    /// 3. the builder's [`timeout`](Self::timeout), for a client the builder
    ///    constructs (under a deadline only; otherwise it is already the
    ///    client's own timeout);
    /// 4. with none of these, the remaining budget itself.
    ///
    /// Without a deadline and without either of the first two, no per-request
    /// timeout is set at all, exactly as in 0.1.
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
    /// surface: a proxy, a shared connection pool, a custom DNS resolver, or a
    /// root store that replaces the built-in roots rather than adding to them.
    /// A client certificate for mutual TLS and extra trust anchors do not need
    /// it: [`identity_pem`](Self::identity_pem) and
    /// [`root_certificate_pem`](Self::root_certificate_pem) configure them on
    /// the client the builder constructs, where [`timeout`](Self::timeout)
    /// still applies.
    ///
    /// The [`bearer_token`](Self::bearer_token) and
    /// [`default_header`](Self::default_header) values still apply: they are sent
    /// per-request rather than baked into the client, so they work identically
    /// whether or not a client is supplied. Only [`timeout`](Self::timeout) is
    /// ignored with a supplied client — configure the timeout on the client you
    /// pass in, or use [`attempt_timeout`](Self::attempt_timeout).
    ///
    /// # Deadlines replace the supplied client's timeout
    ///
    /// Under a deadline ([`RetryPolicy::deadline`] or
    /// [`RequestBuilder::deadline_at`](crate::RequestBuilder::deadline_at)), the
    /// supplied client's own timeout is **replaced** on every attempt by the
    /// remaining budget. This crate cannot read that timeout, and reqwest
    /// applies one timeout per request, so a 5s client timeout under a 60s
    /// deadline lets a single attempt run for up to 60s. To keep a tighter
    /// per-attempt bound, set [`attempt_timeout`](Self::attempt_timeout) here
    /// (or [`RequestBuilder::timeout`](crate::RequestBuilder::timeout) on one
    /// request).
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
    /// value, or if the underlying HTTP client cannot be constructed, which
    /// includes TLS material from [`root_certificate_pem`](Self::root_certificate_pem)
    /// or [`identity_pem`](Self::identity_pem) that cannot be used. A client
    /// supplied via [`with_http_client`](Self::with_http_client) is used as-is,
    /// so that case cannot arise on that path; configuring TLS material
    /// alongside a supplied client is itself a [`ClientError::Config`].
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

        if self.http_client.is_some() && self.tls.is_set() {
            return Err(ClientError::Config(
                "root_certificate_pem and identity_pem configure a client this builder \
                 constructs, and a client was supplied with with_http_client; configure TLS \
                 on the supplied client, or drop it"
                    .to_string(),
            ));
        }

        let shared = match self.http_client {
            Some(client) => Slot {
                http: client,
                timeout: None,
            },
            None => {
                let mut builder = self
                    .tls
                    .apply(reqwest::Client::builder().timeout(self.timeout))?;
                if origins.len() > 1 {
                    builder = builder.redirect(in_set_redirects(origins.clone()));
                }
                Slot {
                    http: builder.build().map_err(|e| {
                        ClientError::Config(format!("failed to build HTTP client: {e}"))
                    })?,
                    timeout: Some(self.timeout),
                }
            }
        };
        let mut slots = Vec::with_capacity(origins.len());
        for endpoint in self.failovers {
            slots.push(match endpoint.http {
                Some(http) => Slot {
                    http,
                    timeout: None,
                },
                None => Slot {
                    http: shared.http.clone(),
                    timeout: shared.timeout,
                },
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
        assert_eq!(client.inner.slots[0].timeout, Some(Duration::from_secs(30)));
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
        assert_eq!(supplied.inner.slots[0].timeout, None);
    }

    #[test]
    fn with_http_client_builds_and_preserves_default_headers() {
        let supplied = reqwest::Client::new();
        let client = ServiceClient::builder("https://api.example.com")
            .bearer_token("secret")
            .with_http_client(supplied)
            .build()
            .unwrap();
        // A supplied client's timeout cannot be observed.
        assert_eq!(client.inner.slots[0].timeout, None);
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
        let timeouts: Vec<_> = client.inner.slots.iter().map(|s| s.timeout).collect();
        let seven = Some(Duration::from_secs(7));
        assert_eq!(timeouts, [seven, seven, None]);
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
