//! Per-request builder and the retry/execute loop.

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::client::ServiceClient;
use crate::context::RequestContext;
use crate::error::{ClientError, build_api_error, snippet};
use crate::retry::is_idempotent;
use crate::url::{build_url, join_segments};

/// A fluent builder for a single request.
///
/// Obtained from [`ServiceClient::request`](crate::ServiceClient::request) or
/// [`ServiceClient::request_unversioned`](crate::ServiceClient::request_unversioned).
/// Configure query parameters, headers, a [`RequestContext`], a JSON body, and
/// retry/acceptance behavior, then finish with [`send_json`](Self::send_json),
/// [`send_no_content`](Self::send_no_content), or [`send`](Self::send).
///
/// # Examples
///
/// ```no_run
/// use acton_service_client::{RequestContext, ServiceClient};
/// use reqwest::Method;
/// # async fn run(client: ServiceClient) -> Result<(), acton_service_client::ClientError> {
/// # #[derive(serde::Deserialize)]
/// # struct Page;
/// let page: Page = client
///     .request(Method::GET, "users")
///     .query("page", "2")
///     .query("limit", "50")
///     .context(RequestContext::new().with_correlation_id("abc"))
///     .send_json()
///     .await?;
/// # let _ = page;
/// # Ok(())
/// # }
/// ```
pub struct RequestBuilder {
    client: ServiceClient,
    method: Method,
    path: String,
    versioned: bool,
    query: Vec<(String, String)>,
    headers: HeaderMap,
    context: RequestContext,
    body: Option<Vec<u8>>,
    retriable_override: bool,
    accept_extra: Vec<StatusCode>,
}

impl RequestBuilder {
    pub(crate) fn new(
        client: ServiceClient,
        method: Method,
        path: String,
        versioned: bool,
    ) -> Self {
        Self {
            client,
            method,
            path,
            versioned,
            query: Vec::new(),
            headers: HeaderMap::new(),
            context: RequestContext::new(),
            body: None,
            retriable_override: false,
            accept_extra: Vec::new(),
        }
    }

    /// Append a query parameter.
    #[must_use]
    pub fn query(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.query.push((key.into(), value.into()));
        self
    }

    /// Add a request header.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `name` or `value` is not a valid HTTP
    /// header.
    pub fn header(
        mut self,
        name: impl AsRef<str>,
        value: impl AsRef<str>,
    ) -> Result<Self, ClientError> {
        let name = HeaderName::from_bytes(name.as_ref().as_bytes())
            .map_err(|e| ClientError::Config(format!("invalid header name: {e}")))?;
        let value = HeaderValue::from_str(value.as_ref())
            .map_err(|e| ClientError::Config(format!("invalid header value: {e}")))?;
        self.headers.insert(name, value);
        Ok(self)
    }

    /// Attach a full propagation [`RequestContext`] to this request.
    #[must_use]
    pub fn context(mut self, context: RequestContext) -> Self {
        self.context = context;
        self
    }

    /// Mark this request as retriable even if its method is not idempotent.
    ///
    /// Has no effect unless a retry policy is configured on the client.
    #[must_use]
    pub fn retriable(mut self, retriable: bool) -> Self {
        self.retriable_override = retriable;
        self
    }

    /// Treat an additional status code as a success (returned rather than raised).
    #[must_use]
    pub fn accept_status(mut self, status: StatusCode) -> Self {
        self.accept_extra.push(status);
        self
    }

    /// Serialize `body` as JSON and attach it, setting `Content-Type`.
    ///
    /// Note that a `&str` becomes a JSON *string literal* (quoted, and with newlines
    /// and quotes escaped), not the raw text. To send a body verbatim — plain text,
    /// CSV, bytes — use [`body`](Self::body). Both set the body, and the last call
    /// wins.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `body` cannot be serialized to JSON.
    pub fn json<B: Serialize + ?Sized>(mut self, body: &B) -> Result<Self, ClientError> {
        let bytes = serde_json::to_vec(body)
            .map_err(|e| ClientError::Config(format!("failed to serialize request body: {e}")))?;
        self.headers
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        self.body = Some(bytes);
        Ok(self)
    }

    /// Attach a raw body verbatim, with an explicit `Content-Type`.
    ///
    /// [`json`](Self::json) is the common case and should be preferred. This is for
    /// the endpoints JSON cannot express: one that takes a `text/plain` document, a
    /// `text/csv` upload, an `application/octet-stream` blob, an
    /// `application/x-www-form-urlencoded` form, a pre-rendered payload of any kind.
    ///
    /// Such a body is **not** a JSON document, and `json` cannot emit one: given a
    /// `&str` it produces a *JSON string literal* — quoted, with newlines and quotes
    /// escaped — which is a different sequence of bytes than the text the caller
    /// meant to send. Here the bytes go out exactly as given.
    ///
    /// # Precedence with [`json`](Self::json)
    ///
    /// Both set the body and overwrite `Content-Type`, and **the last call wins**.
    /// Neither panics and neither merges; calling `.json(&x).body(raw, ct)` sends
    /// `raw` with `ct`, and calling `.body(raw, ct).json(&x)` sends the JSON with
    /// `application/json`. A request carries at most one body, so the final call is
    /// simply the one that describes it.
    ///
    /// # Composition
    ///
    /// Chains with [`query`](Self::query), [`header`](Self::header),
    /// [`context`](Self::context), [`retriable`](Self::retriable), and
    /// [`accept_status`](Self::accept_status) in any order. An explicit
    /// `.header("content-type", …)` set *after* this call still wins, since it is
    /// applied to the same header map.
    ///
    /// ```no_run
    /// use acton_service_client::{Method, ServiceClient};
    /// # async fn run(client: ServiceClient) -> Result<(), acton_service_client::ClientError> {
    /// let response = client
    ///     .request(Method::POST, "documents")
    ///     .body("id,name\n1,Ada\n", "text/csv")?
    ///     .send()
    ///     .await?;
    /// # let _ = response;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Config`] if `content_type` is not a valid header
    /// value.
    pub fn body(
        mut self,
        body: impl Into<Vec<u8>>,
        content_type: impl AsRef<str>,
    ) -> Result<Self, ClientError> {
        let content_type = HeaderValue::from_str(content_type.as_ref())
            .map_err(|e| ClientError::Config(format!("invalid content type: {e}")))?;
        self.headers.insert(CONTENT_TYPE, content_type);
        self.body = Some(body.into());
        Ok(self)
    }

    /// Whether retries may apply to this request (policy present and the method
    /// is idempotent or the caller opted in).
    fn retry_allowed(&self) -> bool {
        self.client.inner.retry.is_some()
            && (is_idempotent(&self.method) || self.retriable_override)
    }

    /// Compute the effective absolute URL string for this request.
    fn url_string(&self) -> String {
        let inner = &self.client.inner;
        let path = if self.versioned {
            join_segments(&[
                &inner.base_path,
                inner.version.as_path_segment(),
                &self.path,
            ])
        } else {
            join_segments(&[&self.path])
        };
        build_url(&inner.base_url, &path)
    }

    /// Build the header map sent on every attempt: context (with an ensured
    /// `x-request-id`) first, then explicitly-set headers on top.
    fn effective_headers(&self) -> HeaderMap {
        let mut ctx = self.context.clone();
        ctx.ensure_request_id();
        let mut headers = ctx.to_headers();
        for (name, value) in &self.headers {
            headers.insert(name.clone(), value.clone());
        }
        headers
    }

    /// Execute the request, applying the retry policy, and return the raw
    /// response for any status treated as success.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Api`] for non-success statuses, or
    /// [`ClientError::Transport`] / [`ClientError::Config`] for lower-level
    /// failures.
    pub async fn send(self) -> Result<reqwest::Response, ClientError> {
        let url = url::Url::parse(&self.url_string())
            .map_err(|e| ClientError::Config(format!("invalid request URL: {e}")))?;
        let headers = self.effective_headers();
        let retry_allowed = self.retry_allowed();
        let policy = self.client.inner.retry.clone();
        let http = &self.client.inner.http;

        let mut attempt: u32 = 1;
        loop {
            let mut rb = http.request(self.method.clone(), url.clone());
            if !self.query.is_empty() {
                rb = rb.query(&self.query);
            }
            rb = rb.headers(headers.clone());
            if let Some(body) = &self.body {
                rb = rb.body(body.clone());
            }

            match rb.send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() || self.accept_extra.contains(&status) {
                        return Ok(resp);
                    }
                    let resp_headers = resp.headers().clone();
                    let text = resp.text().await.unwrap_or_default();
                    let api = build_api_error(status, &resp_headers, &text);
                    if let Some(policy) = &policy
                        && retry_allowed
                        && api.is_retriable()
                        && policy.should_retry(attempt)
                    {
                        let delay = api
                            .retry_after
                            .unwrap_or_else(|| policy.backoff_delay(attempt));
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                        continue;
                    }
                    return Err(ClientError::Api(Box::new(api)));
                }
                Err(e) => {
                    if let Some(policy) = &policy {
                        let transient = e.is_timeout() || e.is_connect();
                        if retry_allowed && transient && policy.should_retry(attempt) {
                            tokio::time::sleep(policy.backoff_delay(attempt)).await;
                            attempt += 1;
                            continue;
                        }
                    }
                    return Err(ClientError::Transport(e));
                }
            }
        }
    }

    /// Send the request and decode a JSON success body into `T`.
    ///
    /// # Errors
    ///
    /// In addition to the errors from [`send`](Self::send), returns
    /// [`ClientError::Decode`] if the success body is not valid JSON for `T`.
    pub async fn send_json<T: DeserializeOwned>(self) -> Result<T, ClientError> {
        let resp = self.send().await?;
        let status = resp.status();
        let text = resp.text().await.map_err(ClientError::Transport)?;
        serde_json::from_str(&text).map_err(|source| ClientError::Decode {
            status,
            snippet: snippet(&text),
            source,
        })
    }

    /// Send the request and discard the body (for `204 No Content` and similar).
    ///
    /// # Errors
    ///
    /// Returns the errors from [`send`](Self::send). A non-success status still
    /// yields [`ClientError::Api`].
    pub async fn send_no_content(self) -> Result<(), ClientError> {
        let _ = self.send().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::versioning::ApiVersion;

    fn client() -> ServiceClient {
        ServiceClient::builder("https://api.example.com")
            .api_version(ApiVersion::V1)
            .build()
            .unwrap()
    }

    #[test]
    fn versioned_url_is_built_correctly() {
        let rb = client().request(Method::GET, "users/42");
        assert_eq!(rb.url_string(), "https://api.example.com/api/v1/users/42");
    }

    #[test]
    fn unversioned_url_skips_base_path_and_version() {
        let rb = client().request_unversioned(Method::GET, "health");
        assert_eq!(rb.url_string(), "https://api.example.com/health");
    }

    #[test]
    fn leading_slash_in_path_is_normalized() {
        let rb = client().request(Method::GET, "/users/");
        assert_eq!(rb.url_string(), "https://api.example.com/api/v1/users");
    }

    #[test]
    fn effective_headers_generate_request_id() {
        let rb = client().request(Method::GET, "x");
        let h = rb.effective_headers();
        assert!(h.contains_key("x-request-id"));
    }

    #[test]
    fn explicit_header_overrides_context() {
        let rb = client()
            .request(Method::GET, "x")
            .context(RequestContext::new().with_request_id("from-ctx"))
            .header("x-request-id", "explicit")
            .unwrap();
        let h = rb.effective_headers();
        assert_eq!(h.get("x-request-id").unwrap(), "explicit");
    }

    #[test]
    fn retry_not_allowed_without_policy() {
        let rb = client().request(Method::GET, "x");
        assert!(!rb.retry_allowed());
    }

    #[test]
    fn retry_allowed_for_idempotent_with_policy() {
        let c = ServiceClient::builder("https://api.example.com")
            .retry(crate::retry::RetryPolicy::default())
            .build()
            .unwrap();
        assert!(c.request(Method::GET, "x").retry_allowed());
        assert!(!c.request(Method::POST, "x").retry_allowed());
        assert!(c.request(Method::POST, "x").retriable(true).retry_allowed());
    }
}
