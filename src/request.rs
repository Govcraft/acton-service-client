//! Per-request builder and the retry/execute loop.

use std::time::{Duration, Instant};

use reqwest::header::{CONTENT_TYPE, HeaderMap, HeaderName, HeaderValue};
use reqwest::{Method, StatusCode};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::client::ServiceClient;
use crate::context::RequestContext;
use crate::error::{ClientError, build_api_error, parse_retry_after, snippet};
use crate::retry::{attempt_timeout, is_idempotent, remaining_until, wants_retry};
use crate::url::{build_url, join_segments};

/// The error for a request whose deadline passed before its first attempt,
/// saying by how much and what to do about it.
fn deadline_passed(deadline: Option<Instant>, started: Instant) -> ClientError {
    let overdue = deadline.map_or(Duration::ZERO, |at| started.saturating_duration_since(at));
    ClientError::Config(format!(
        "request deadline had already passed {overdue:?} before the first attempt, so the \
         request was not sent; pass a later `deadline_at`, or start the operation's budget \
         earlier so each send gets time"
    ))
}

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
    retry_on: Vec<StatusCode>,
    timeout: Option<Duration>,
    deadline_at: Option<Instant>,
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
            retry_on: Vec::new(),
            timeout: None,
            deadline_at: None,
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
    ///
    /// A status that is also listed with
    /// [`retry_on_status`](Self::retry_on_status) is retried first, and only
    /// returned once retries are exhausted.
    #[must_use]
    pub fn accept_status(mut self, status: StatusCode) -> Self {
        self.accept_extra.push(status);
        self
    }

    /// Also retry this request when the server answers `status`.
    ///
    /// Extends the statuses retried by default (`429`, `502`, `503`, `504`, and
    /// `423` with `Retry-After`; see [`ApiError::is_retriable`](crate::ApiError::is_retriable)).
    /// Repeatable: call it once per extra status.
    ///
    /// **Only takes effect when retries apply to this request**: a
    /// [`RetryPolicy`](crate::RetryPolicy) is configured on the client *and* the
    /// method is idempotent or the request is marked
    /// [`retriable(true)`](Self::retriable). A `POST` answered `421` is not
    /// retried just because `421` is listed here; mark it retriable first.
    ///
    /// It is checked **before** [`accept_status`](Self::accept_status): a status
    /// in both lists is retried while attempts and the deadline allow (honouring
    /// any `Retry-After`), and once they run out the last response goes down
    /// the normal path, so an accepted status is still returned raw and
    /// decodable.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use acton_service_client::{Method, ServiceClient, StatusCode};
    /// # async fn run(client: ServiceClient) -> Result<(), acton_service_client::ClientError> {
    /// # #[derive(serde::Deserialize)] struct Answer;
    /// // A `421 Misdirected Request` means "try another replica": retry it.
    /// let answer: Answer = client
    ///     .request(Method::POST, "authorize")
    ///     .retriable(true)
    ///     .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
    ///     .send_json()
    ///     .await?;
    /// # let _ = answer;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn retry_on_status(mut self, status: StatusCode) -> Self {
        self.retry_on.push(status);
        self
    }

    /// Override the per-attempt timeout for this request.
    ///
    /// Takes precedence over the client's
    /// [`attempt_timeout`](crate::ServiceClientBuilder::attempt_timeout) and
    /// [`timeout`](crate::ServiceClientBuilder::timeout) for every attempt of
    /// this request, including with a client supplied via
    /// [`with_http_client`](crate::ServiceClientBuilder::with_http_client).
    /// Under a deadline an attempt gets the smaller of this and the time
    /// remaining.
    ///
    /// **Supplied client under a deadline:** a client passed to
    /// [`with_http_client`](crate::ServiceClientBuilder::with_http_client) does
    /// not expose its own timeout, and reqwest applies one timeout per request.
    /// So under a deadline, that client's own timeout is **replaced** on every
    /// attempt by the remaining budget. To keep a tighter per-attempt bound,
    /// set [`ServiceClientBuilder::attempt_timeout`](crate::ServiceClientBuilder::attempt_timeout)
    /// (client-wide) or this method (one request).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use acton_service_client::{Method, ServiceClient};
    /// use std::time::Duration;
    /// # async fn run(client: ServiceClient) -> Result<(), acton_service_client::ClientError> {
    /// client
    ///     .request(Method::GET, "slow-report")
    ///     .timeout(Duration::from_secs(120))
    ///     .send_no_content()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Bound this request, every attempt and pause included, by the absolute
    /// `deadline`.
    ///
    /// Overrides the policy's relative [`deadline`](crate::RetryPolicy::deadline)
    /// for this request, and applies whether or not a retry policy is
    /// configured. Because it is absolute, several requests that make up one
    /// logical operation can share one budget: compute the instant once and
    /// pass it to each, and each request gets only what the earlier ones left.
    ///
    /// A request whose deadline has already passed is not sent: it fails at
    /// once with a non-retriable [`ClientError::Config`] saying so. Once a
    /// request has been sent, running out of budget returns the last attempt's
    /// error or response instead (an attempt cut short by the deadline is a
    /// [`ClientError::Transport`] timeout).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use acton_service_client::{Method, ServiceClient};
    /// use std::time::{Duration, Instant};
    /// # async fn run(client: ServiceClient) -> Result<(), acton_service_client::ClientError> {
    /// let deadline = Instant::now() + Duration::from_secs(2);
    /// client
    ///     .request(Method::PUT, "reservations/7")
    ///     .deadline_at(deadline)
    ///     .send_no_content()
    ///     .await?;
    /// // The confirmation gets whatever the reservation left of the 2 seconds.
    /// client
    ///     .request(Method::PUT, "reservations/7/confirm")
    ///     .deadline_at(deadline)
    ///     .send_no_content()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn deadline_at(mut self, deadline: Instant) -> Self {
        self.deadline_at = Some(deadline);
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

    /// Build the header map sent on every attempt: the client's default headers
    /// (bearer token and any `default_header`) as the base, then context (with
    /// an ensured `x-request-id`), then explicitly-set per-request headers on
    /// top. Later layers override earlier ones by name.
    fn effective_headers(&self) -> HeaderMap {
        let mut headers = self.client.inner.default_headers.clone();
        let mut ctx = self.context.clone();
        ctx.ensure_request_id();
        for (name, value) in &ctx.to_headers() {
            headers.insert(name.clone(), value.clone());
        }
        for (name, value) in &self.headers {
            headers.insert(name.clone(), value.clone());
        }
        headers
    }

    /// Execute the request, applying the retry policy and any deadline, and
    /// return the raw response for any status treated as success.
    ///
    /// When retries are exhausted (by `max_attempts` or the deadline), the last
    /// error or response is returned exactly as a single attempt would return it.
    ///
    /// # Errors
    ///
    /// Returns [`ClientError::Api`] for non-success statuses, or
    /// [`ClientError::Transport`] / [`ClientError::Config`] for lower-level
    /// failures. An attempt cut short by the deadline is a
    /// [`ClientError::Transport`] timeout. A request whose deadline passed
    /// before its first attempt is not sent and fails with
    /// [`ClientError::Config`].
    pub async fn send(self) -> Result<reqwest::Response, ClientError> {
        self.execute(fastrand::f64).await
    }

    /// The send loop, with the jitter randomness injected so tests can pin it.
    async fn execute(
        self,
        mut draw: impl FnMut() -> f64,
    ) -> Result<reqwest::Response, ClientError> {
        let started = Instant::now();
        let url = url::Url::parse(&self.url_string())
            .map_err(|e| ClientError::Config(format!("invalid request URL: {e}")))?;
        let headers = self.effective_headers();
        let deadline = self.deadline(started);

        let mut attempt: u32 = 1;
        // The outcome of the previous attempt, returned instead of a new one
        // if the pause before this attempt used up the rest of the budget.
        let mut last: Option<Result<reqwest::Response, ClientError>> = None;
        loop {
            let remaining = remaining_until(deadline, Instant::now());
            if remaining == Some(Duration::ZERO) {
                return last.unwrap_or_else(|| Err(deadline_passed(deadline, started)));
            }
            let mut rb = self
                .client
                .inner
                .http
                .request(self.method.clone(), url.clone());
            if !self.query.is_empty() {
                rb = rb.query(&self.query);
            }
            rb = rb.headers(headers.clone());
            if let Some(body) = &self.body {
                rb = rb.body(body.clone());
            }
            if let Some(timeout) = attempt_timeout(
                self.timeout,
                self.client.inner.attempt_timeout,
                self.client.inner.timeout,
                remaining,
            ) {
                rb = rb.timeout(timeout);
            }

            let (outcome, pause) = match rb.send().await {
                Ok(resp) if resp.status().is_success() => return Ok(resp),
                Ok(resp) => {
                    let status = resp.status();
                    let retry_after = parse_retry_after(resp.headers());
                    let accepted = self.accept_extra.contains(&status);
                    let retry = wants_retry(status, retry_after, accepted, &self.retry_on);
                    if accepted {
                        // Kept unread, so that on exhaustion it is returned intact.
                        let pause = self.pause_before_retry(
                            retry,
                            attempt,
                            &mut draw,
                            retry_after,
                            deadline,
                        );
                        (Ok(resp), pause)
                    } else {
                        let resp_headers = resp.headers().clone();
                        let text = resp.text().await.unwrap_or_default();
                        let api = build_api_error(status, &resp_headers, &text);
                        let pause = self.pause_before_retry(
                            retry,
                            attempt,
                            &mut draw,
                            retry_after,
                            deadline,
                        );
                        (Err(ClientError::Api(Box::new(api))), pause)
                    }
                }
                Err(e) => {
                    let transient = e.is_timeout() || e.is_connect();
                    let pause =
                        self.pause_before_retry(transient, attempt, &mut draw, None, deadline);
                    (Err(ClientError::Transport(e)), pause)
                }
            };
            let Some(pause) = pause else {
                return outcome;
            };
            last = Some(outcome);
            tokio::time::sleep(pause).await;
            attempt += 1;
        }
    }

    /// The absolute deadline for this send: the per-request override, else the
    /// policy's relative deadline measured from `started`.
    fn deadline(&self, started: Instant) -> Option<Instant> {
        self.deadline_at.or_else(|| {
            let budget = self.client.inner.retry.as_ref()?.deadline?;
            started.checked_add(budget)
        })
    }

    /// The pause before another attempt, or `None` to stop and return the
    /// current outcome. `None` unless the outcome is `retriable`, retries apply
    /// to this request, attempts remain, and the pause ends before the deadline.
    fn pause_before_retry(
        &self,
        retriable: bool,
        attempt: u32,
        draw: &mut impl FnMut() -> f64,
        retry_after: Option<Duration>,
        deadline: Option<Instant>,
    ) -> Option<Duration> {
        if !(retriable && self.retry_allowed()) {
            return None;
        }
        let policy = self.client.inner.retry.as_ref()?;
        let remaining = remaining_until(deadline, Instant::now());
        policy.next_pause(attempt, draw(), retry_after, remaining)
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
    fn default_headers_are_applied_per_request() {
        let c = ServiceClient::builder("https://api.example.com")
            .bearer_token("secret")
            .build()
            .unwrap();
        let h = c.request(Method::GET, "x").effective_headers();
        assert_eq!(h.get("authorization").unwrap(), "Bearer secret");
        // The generated request id still lands alongside the default header.
        assert!(h.contains_key("x-request-id"));
    }

    #[test]
    fn per_request_header_overrides_default_header() {
        let c = ServiceClient::builder("https://api.example.com")
            .bearer_token("from-default")
            .build()
            .unwrap();
        let h = c
            .request(Method::GET, "x")
            .header("authorization", "Bearer explicit")
            .unwrap()
            .effective_headers();
        assert_eq!(h.get("authorization").unwrap(), "Bearer explicit");
    }

    #[test]
    fn retry_not_allowed_without_policy() {
        let rb = client().request(Method::GET, "x");
        assert!(!rb.retry_allowed());
    }

    /// Jitter floor, end to end: every draw forced to the minimum, an upstream
    /// that answers 503 forever, and `max_attempts = u32::MAX`. The floor keeps
    /// each pause at `base_delay`, so the deadline ends the loop after a bounded
    /// number of attempts, and the last 503 is what comes back.
    #[tokio::test]
    async fn jitter_floor_bounds_attempts_under_a_deadline_with_minimum_draws() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let hits = Arc::new(AtomicUsize::new(0));
        let counter = hits.clone();
        let app = axum::Router::new().route(
            "/api/v1/down",
            axum::routing::get(move || {
                counter.fetch_add(1, Ordering::SeqCst);
                async { (axum::http::StatusCode::SERVICE_UNAVAILABLE, "down") }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let client = ServiceClient::builder(format!("http://{addr}"))
            .retry(
                crate::retry::RetryPolicy::default()
                    .max_attempts(u32::MAX)
                    .base_delay(Duration::from_millis(20))
                    .max_delay(Duration::from_secs(1))
                    .jitter(crate::retry::Jitter::Full)
                    .deadline(Duration::from_millis(300)),
            )
            .build()
            .unwrap();

        let started = Instant::now();
        let err = client
            .request(Method::GET, "down")
            .execute(|| 0.0)
            .await
            .unwrap_err();
        let elapsed = started.elapsed();

        let api = err.as_api().expect("the last 503 is returned");
        assert_eq!(api.status(), StatusCode::SERVICE_UNAVAILABLE);
        let attempts = hits.load(Ordering::SeqCst);
        // 300ms / 20ms floor: at most 15 pauses, so at most 16 attempts.
        assert!((2..=16).contains(&attempts), "{attempts} attempts");
        assert!(elapsed < Duration::from_millis(450), "{elapsed:?}");
    }

    #[test]
    fn deadline_at_overrides_the_policy_deadline() {
        let c = ServiceClient::builder("https://api.example.com")
            .retry(crate::retry::RetryPolicy::default().deadline(Duration::from_secs(9)))
            .build()
            .unwrap();
        let started = Instant::now();
        assert_eq!(
            c.request(Method::GET, "x").deadline(started),
            Some(started + Duration::from_secs(9))
        );
        let at = started + Duration::from_secs(1);
        assert_eq!(
            c.request(Method::GET, "x")
                .deadline_at(at)
                .deadline(started),
            Some(at)
        );
        assert_eq!(client().request(Method::GET, "x").deadline(started), None);
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
