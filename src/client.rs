//! The [`ServiceClient`] and its builder.

use std::sync::Arc;
use std::time::Duration;

use reqwest::Method;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::ClientError;
use crate::health::{HealthResponse, ReadinessResponse};
use crate::request::RequestBuilder;
use crate::retry::RetryPolicy;
use crate::versioning::ApiVersion;

/// Shared, cheaply-cloneable client configuration.
pub(crate) struct Inner {
    pub(crate) http: reqwest::Client,
    pub(crate) base_url: String,
    pub(crate) base_path: String,
    pub(crate) version: ApiVersion,
    pub(crate) retry: Option<RetryPolicy>,
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
    retry: Option<RetryPolicy>,
    default_headers: HeaderMap,
}

impl ServiceClientBuilder {
    fn new(base_url: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            base_path: "/api".to_string(),
            version: ApiVersion::V1,
            bearer_token: None,
            timeout: Duration::from_secs(30),
            retry: None,
            default_headers: HeaderMap::new(),
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

    /// Set the per-request timeout (default 30s).
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
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

    /// Validate configuration and build the [`ServiceClient`].
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if the base URL is not a valid absolute
    /// `http`/`https` URL, or if the bearer token cannot be encoded as a header
    /// value, or if the underlying HTTP client cannot be constructed.
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

        let http = reqwest::Client::builder()
            .timeout(self.timeout)
            .default_headers(self.default_headers)
            .build()
            .map_err(|e| ClientError::Config(format!("failed to build HTTP client: {e}")))?;

        Ok(ServiceClient {
            inner: Arc::new(Inner {
                http,
                base_url: self.base_url,
                base_path: self.base_path,
                version: self.version,
                retry: self.retry,
            }),
        })
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
    }

    #[test]
    fn build_rejects_control_char_token() {
        let err = ServiceClient::builder("https://api.example.com")
            .bearer_token("bad\ntoken")
            .build()
            .unwrap_err();
        assert!(matches!(err, ClientError::Config(_)));
    }
}
