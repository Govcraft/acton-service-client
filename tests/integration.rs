//! Integration round-trip against a plain-axum server that reproduces the exact
//! `acton-service` 0.27 wire shapes (verified against the framework source:
//! `error.rs::ErrorResponse`, `responses.rs::Created`, `health.rs`, and the
//! `request_tracking` propagation header set).
//!
//! # Test-server strategy
//!
//! The task allowed spinning a genuine `acton-service` router as a dev-dependency
//! *or*, if its default features make the build unreasonably heavy, a plain-axum
//! fake reproducing the documented wire shapes. We use the fake: `acton-service`'s
//! default features pull `opentelemetry` and an `aws-lc-rs` C toolchain, and its
//! health/readiness handlers require a fully bootstrapped `AppState` (figment +
//! XDG config discovery), which is disproportionately heavy and
//! environment-sensitive for proving wire-shape mirroring. Every JSON body and
//! header below is byte-for-byte what `acton-service` emits.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use acton_service_client::{
    ApiVersion, ClientError, DependencyStatus, HealthResponse, ReadinessResponse, RequestContext,
    RetryPolicy, ServiceClient, StatusCode,
};
use axum::body::Bytes;
use axum::extract::Path;
use axum::http::{HeaderMap, StatusCode as AxumStatus, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::net::TcpListener;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct User {
    id: u64,
    name: String,
}

/// Build a router whose responses mirror acton-service byte-for-byte.
fn app() -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .route("/api/v1/users/{id}", get(get_user))
        .route("/api/v1/users/{id}", delete(delete_user))
        .route("/api/v1/users", post(create_user))
        .route("/api/v1/echo-headers", get(echo_headers))
        .route("/api/v1/echo-body", post(echo_body))
        .route("/api/v1/rate-limited", get(rate_limited))
        .route("/api/v1/locked", get(locked))
        .route("/api/v1/broken", get(broken))
        .route("/api/v1/missing", get(not_found))
}

/// Echoes the raw request body back, along with the `Content-Type` it arrived with.
///
/// Takes [`Bytes`], not a `Json<…>` and not even a `String`: the point is to observe
/// the bytes exactly as they were sent, without a body extractor getting a chance to
/// reinterpret — or reject — them. A `String` extractor would 400 on a body that is
/// not valid UTF-8, which is precisely the binary case worth proving.
///
/// `body` is the UTF-8 view (for the text cases) and `bytes` the raw octets (for the
/// binary one).
async fn echo_body(headers: HeaderMap, body: Bytes) -> Json<serde_json::Value> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    Json(json!({
        "body": String::from_utf8_lossy(&body),
        "bytes": body.to_vec(),
        "content_type": content_type,
    }))
}

async fn health() -> Json<serde_json::Value> {
    // acton-service health.rs: {status, service, version}
    Json(json!({"status": "healthy", "service": "test-svc", "version": "0.1.0"}))
}

async fn ready() -> Response {
    // acton-service returns 503 when a dependency is unhealthy, body = ReadinessResponse.
    let body = json!({
        "ready": false,
        "service": "test-svc",
        "dependencies": {
            "postgres": {"healthy": true, "message": "Connected"},
            "redis": {"healthy": false, "message": "Connection failed"}
        }
    });
    (AxumStatus::SERVICE_UNAVAILABLE, Json(body)).into_response()
}

async fn get_user(Path(id): Path<u64>) -> Json<User> {
    Json(User {
        id,
        name: "Ada".to_string(),
    })
}

async fn create_user(Json(mut user): Json<User>) -> Response {
    // acton-service responses.rs Created: 201 + Location header + JSON body.
    user.id = 100;
    let mut resp = (AxumStatus::CREATED, Json(user)).into_response();
    resp.headers_mut()
        .insert(header::LOCATION, "/api/v1/users/100".parse().unwrap());
    resp
}

async fn delete_user(Path(_id): Path<u64>) -> AxumStatus {
    // acton-service NoContent: 204 empty body.
    AxumStatus::NO_CONTENT
}

async fn echo_headers(headers: HeaderMap) -> Json<serde_json::Value> {
    let read = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    Json(json!({
        "x-request-id": read("x-request-id"),
        "x-correlation-id": read("x-correlation-id"),
        "x-client-id": read("x-client-id"),
        "authorization": read("authorization"),
    }))
}

async fn rate_limited() -> Response {
    // acton-service RateLimitExceeded: 429 + RATE_LIMIT_EXCEEDED, plus RateLimit-* headers.
    let body = json!({"error": "Too many requests", "code": "RATE_LIMIT_EXCEEDED", "status": 429});
    let mut resp = (AxumStatus::TOO_MANY_REQUESTS, Json(body)).into_response();
    let h = resp.headers_mut();
    h.insert("RateLimit-Limit", "100".parse().unwrap());
    h.insert("RateLimit-Remaining", "0".parse().unwrap());
    h.insert("RateLimit-Reset", "42".parse().unwrap());
    h.insert(header::RETRY_AFTER, "42".parse().unwrap());
    resp
}

async fn locked() -> Response {
    // acton-service AccountLocked: 423 LOCKED + ACCOUNT_LOCKED + Retry-After.
    let body = json!({"error": "Account locked", "code": "ACCOUNT_LOCKED", "status": 423});
    let mut resp = (AxumStatus::LOCKED, Json(body)).into_response();
    resp.headers_mut()
        .insert(header::RETRY_AFTER, "30".parse().unwrap());
    resp
}

async fn broken() -> Response {
    // A non-ErrorResponse body at an error status: must not panic, keep status.
    (AxumStatus::BAD_GATEWAY, "<html>502 upstream boom</html>").into_response()
}

async fn not_found() -> Response {
    let body = json!({"error": "User not found", "code": "NOT_FOUND", "status": 404});
    (AxumStatus::NOT_FOUND, Json(body)).into_response()
}

/// Spawn the fake server on an ephemeral port and return its base URL.
async fn spawn_server() -> String {
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app()).await.unwrap();
    });
    format!("http://{addr}")
}

/// Spawn a server whose `/api/v1/flaky` route returns `503` twice, then `200`.
async fn spawn_flaky_server() -> String {
    let counter = Arc::new(AtomicUsize::new(0));
    let app = Router::new().route(
        "/api/v1/flaky",
        get(move || {
            let counter = counter.clone();
            async move {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                if n < 2 {
                    let body =
                        json!({"error": "unavailable", "code": "SERVICE_UNAVAILABLE", "status": 503});
                    (AxumStatus::SERVICE_UNAVAILABLE, Json(body)).into_response()
                } else {
                    Json(json!({"id": 1, "name": "ok"})).into_response()
                }
            }
        }),
    );
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn client(base: &str) -> ServiceClient {
    ServiceClient::builder(base)
        .api_version(ApiVersion::V1)
        .bearer_token("test-token")
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap()
}

#[tokio::test]
async fn get_decodes_versioned_json() {
    let base = spawn_server().await;
    let user: User = client(&base).get("users/42").await.unwrap();
    assert_eq!(
        user,
        User {
            id: 42,
            name: "Ada".into()
        }
    );
}

#[tokio::test]
async fn post_handles_201_created() {
    let base = spawn_server().await;
    let created: User = client(&base)
        .post(
            "users",
            &User {
                id: 0,
                name: "Grace".into(),
            },
        )
        .await
        .unwrap();
    assert_eq!(created.id, 100);
    assert_eq!(created.name, "Grace");
}

#[tokio::test]
async fn delete_handles_204_no_content() {
    let base = spawn_server().await;
    let out: () = client(&base).delete("users/7").await.unwrap();
    assert_eq!(out, ());
}

#[tokio::test]
async fn health_hits_unversioned_route() {
    let base = spawn_server().await;
    let h: HealthResponse = client(&base).health().await.unwrap();
    assert_eq!(h.status, "healthy");
    assert_eq!(h.service, "test-svc");
    assert_eq!(h.version.as_deref(), Some("0.1.0"));
    assert!(h.is_healthy());
}

#[tokio::test]
async fn ready_decodes_503_body_as_readiness() {
    let base = spawn_server().await;
    let r: ReadinessResponse = client(&base).ready().await.unwrap();
    assert!(!r.ready);
    assert_eq!(r.service, "test-svc");
    assert_eq!(
        r.dependencies.get("postgres"),
        Some(&DependencyStatus {
            healthy: true,
            message: Some("Connected".into())
        })
    );
    assert!(!r.dependencies["redis"].healthy);
    let _: &HashMap<String, DependencyStatus> = &r.dependencies;
}

#[tokio::test]
async fn not_found_becomes_typed_api_error() {
    let base = spawn_server().await;
    let err = client(&base).get::<User>("missing").await.unwrap_err();
    let api = err.as_api().expect("api error");
    assert_eq!(api.status(), StatusCode::NOT_FOUND);
    assert_eq!(api.code(), Some("NOT_FOUND"));
    assert_eq!(api.message(), "User not found");
    assert!(!api.is_retriable());
}

#[tokio::test]
async fn rate_limit_headers_surface_on_error() {
    let base = spawn_server().await;
    let err = client(&base).get::<User>("rate-limited").await.unwrap_err();
    let api = err.as_api().expect("api error");
    assert_eq!(api.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(api.code(), Some("RATE_LIMIT_EXCEEDED"));
    let rl = api.rate_limit().expect("rate limit info");
    assert_eq!(rl.limit, Some(100));
    assert_eq!(rl.remaining, Some(0));
    assert_eq!(rl.reset, Some(42));
    assert_eq!(api.retry_after(), Some(Duration::from_secs(42)));
    assert!(api.is_retriable());
}

#[tokio::test]
async fn locked_carries_retry_after() {
    let base = spawn_server().await;
    let err = client(&base).get::<User>("locked").await.unwrap_err();
    let api = err.as_api().expect("api error");
    assert_eq!(api.status(), StatusCode::LOCKED);
    assert_eq!(api.code(), Some("ACCOUNT_LOCKED"));
    assert_eq!(api.retry_after(), Some(Duration::from_secs(30)));
    assert!(api.is_retriable()); // 423 + Retry-After
}

#[tokio::test]
async fn non_json_error_body_is_preserved() {
    let base = spawn_server().await;
    let err = client(&base).get::<User>("broken").await.unwrap_err();
    let api = err.as_api().expect("api error");
    assert_eq!(api.status(), StatusCode::BAD_GATEWAY);
    assert_eq!(api.code(), None);
    assert!(api.message().contains("upstream boom"));
    assert!(api.is_retriable());
}

#[tokio::test]
async fn request_context_and_bearer_propagate() {
    let base = spawn_server().await;
    let ctx = RequestContext::new()
        .with_request_id("req-abc")
        .with_correlation_id("corr-xyz")
        .with_client_id("client-1");
    let echoed: serde_json::Value = client(&base)
        .request(acton_service_client::Method::GET, "echo-headers")
        .context(ctx)
        .send_json()
        .await
        .unwrap();
    assert_eq!(echoed["x-request-id"], "req-abc");
    assert_eq!(echoed["x-correlation-id"], "corr-xyz");
    assert_eq!(echoed["x-client-id"], "client-1");
    assert_eq!(echoed["authorization"], "Bearer test-token");
}

#[tokio::test]
async fn auto_request_id_generated_when_absent() {
    let base = spawn_server().await;
    let echoed: serde_json::Value = client(&base)
        .request(acton_service_client::Method::GET, "echo-headers")
        .send_json()
        .await
        .unwrap();
    // Client always sends an x-request-id even when the caller supplies none.
    let id = echoed["x-request-id"].as_str().unwrap();
    assert_eq!(id.len(), 36);
}

#[tokio::test]
async fn retries_idempotent_get_until_success() {
    let base = spawn_flaky_server().await;
    let client = ServiceClient::builder(&base)
        .retry(
            RetryPolicy::default()
                .base_delay(Duration::from_millis(1))
                .max_delay(Duration::from_millis(5)),
        )
        .build()
        .unwrap();
    // Two 503s then a 200; default policy allows 3 attempts.
    let user: User = client.get("flaky").await.unwrap();
    assert_eq!(
        user,
        User {
            id: 1,
            name: "ok".into()
        }
    );
}

#[tokio::test]
async fn no_retry_without_policy_surfaces_first_error() {
    let base = spawn_flaky_server().await;
    // No retry policy configured: the first 503 is returned immediately.
    let err = client(&base).get::<User>("flaky").await.unwrap_err();
    let api = err.as_api().expect("api error");
    assert_eq!(api.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert!(api.is_retriable());
}

#[tokio::test]
async fn decode_error_when_body_mismatches_type() {
    let base = spawn_server().await;
    // /health returns a HealthResponse shape; decoding it as User must fail cleanly.
    let err = client(&base)
        .request_unversioned(acton_service_client::Method::GET, "health")
        .send_json::<User>()
        .await
        .unwrap_err();
    assert!(matches!(err, ClientError::Decode { .. }));
}

/// A CSV upload: newlines and quotes, exactly the characters JSON encoding mangles.
const CSV: &str = "id,name\n1,\"Lovelace, Ada\"\n2,Hopper\n";

/// A raw body goes out **verbatim**, with the caller's `Content-Type`.
///
/// The case `json` cannot express. An endpoint that takes a document — CSV, plain
/// text, a pre-rendered payload — needs the bytes it was handed, and `json` would
/// serialize a `&str` into a *quoted, escaped JSON string*, which is a different
/// document. Not a hypothetical: it silently turns a valid upload into one the far
/// end cannot parse.
#[tokio::test]
async fn body_sends_raw_bytes_verbatim_with_the_given_content_type() {
    let base = spawn_server().await;

    let echoed: serde_json::Value = client(&base)
        .request(acton_service_client::Method::POST, "echo-body")
        .body(CSV, "text/csv; charset=utf-8")
        .unwrap()
        .send_json()
        .await
        .unwrap();

    assert_eq!(
        echoed["body"], CSV,
        "the bytes must arrive exactly as handed over"
    );
    assert_eq!(echoed["content_type"], "text/csv; charset=utf-8");
}

/// Binary is bytes, not text: a body given as raw octets survives byte-for-byte,
/// including bytes that are not valid UTF-8 at all.
#[tokio::test]
async fn body_accepts_raw_bytes_not_just_text() {
    let base = spawn_server().await;
    // A PNG magic number: `0x89` is not valid UTF-8, so nothing may treat this as text.
    let bytes: Vec<u8> = vec![0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A];

    let echoed: serde_json::Value = client(&base)
        .request(acton_service_client::Method::POST, "echo-body")
        .body(bytes.clone(), "application/octet-stream")
        .unwrap()
        .send_json()
        .await
        .unwrap();

    assert_eq!(echoed["content_type"], "application/octet-stream");
    let received: Vec<u8> = serde_json::from_value(echoed["bytes"].clone()).unwrap();
    assert_eq!(received, bytes, "every octet survives, UTF-8 or not");
}

/// The contrast that justifies the method above: `json` on the *same* `&str` sends a
/// JSON string literal — quoted and escaped — not the document. Both are correct;
/// they are simply different bodies, and an endpoint taking a document needs the
/// other one.
#[tokio::test]
async fn json_on_a_str_sends_a_quoted_json_string_not_the_raw_document() {
    let base = spawn_server().await;

    let echoed: serde_json::Value = client(&base)
        .request(acton_service_client::Method::POST, "echo-body")
        .json(CSV)
        .unwrap()
        .send_json()
        .await
        .unwrap();

    assert_eq!(
        echoed["body"],
        serde_json::to_string(CSV).unwrap(),
        "json encodes the string; it does not pass it through"
    );
    assert_ne!(echoed["body"], CSV);
    assert_eq!(echoed["content_type"], "application/json");
}

/// The documented precedence: a request carries at most one body, so **the last call
/// wins** — in either order, and without panicking or merging.
#[tokio::test]
async fn the_last_of_json_and_body_to_be_called_wins() {
    let base = spawn_server().await;

    // body last: the raw document wins, with its content type.
    let raw_last: serde_json::Value = client(&base)
        .request(acton_service_client::Method::POST, "echo-body")
        .json(&User {
            id: 1,
            name: "Ada".into(),
        })
        .unwrap()
        .body(CSV, "text/csv")
        .unwrap()
        .send_json()
        .await
        .unwrap();
    assert_eq!(raw_last["body"], CSV);
    assert_eq!(raw_last["content_type"], "text/csv");

    // json last: the JSON wins, and reclaims `application/json`.
    let json_last: serde_json::Value = client(&base)
        .request(acton_service_client::Method::POST, "echo-body")
        .body(CSV, "text/csv")
        .unwrap()
        .json(&User {
            id: 1,
            name: "Ada".into(),
        })
        .unwrap()
        .send_json()
        .await
        .unwrap();
    assert_eq!(json_last["content_type"], "application/json");
    assert_eq!(
        json_last["body"],
        serde_json::to_string(&User {
            id: 1,
            name: "Ada".into()
        })
        .unwrap()
    );
}

/// It composes with the rest of the chain — query, headers, retriable, accept_status
/// — in any order, and an explicit `content-type` header set afterwards still wins.
#[tokio::test]
async fn body_composes_with_the_rest_of_the_builder_chain() {
    let base = spawn_server().await;

    let echoed: serde_json::Value = client(&base)
        .request(acton_service_client::Method::POST, "echo-body")
        .query("dry_run", "true")
        .body(CSV, "text/csv")
        .unwrap()
        .header("content-type", "text/plain")
        .unwrap()
        .retriable(true)
        .accept_status(StatusCode::CONFLICT)
        .send_json()
        .await
        .unwrap();

    assert_eq!(
        echoed["body"], CSV,
        "the body survives the rest of the chain"
    );
    assert_eq!(
        echoed["content_type"], "text/plain",
        "an explicit header set afterwards wins"
    );
}

// ---------------------------------------------------------------------------
// Retry deadline, jitter, and per-request retry controls (0.2.0).
// ---------------------------------------------------------------------------

/// What a scripted endpoint answers on every hit.
#[derive(Clone)]
struct Script {
    status: AxumStatus,
    retry_after: Option<&'static str>,
    delay: Duration,
}

impl Script {
    fn status(status: AxumStatus) -> Self {
        Self {
            status,
            retry_after: None,
            delay: Duration::ZERO,
        }
    }
}

/// When each request reached the scripted endpoint.
type Hits = Arc<std::sync::Mutex<Vec<std::time::Instant>>>;

/// Spawn a server whose `/api/v1/scripted` answers every method with `script`,
/// with an acton-service error body, recording the arrival time of each hit.
async fn spawn_scripted(script: Script) -> (String, Hits) {
    let hits: Hits = Arc::default();
    let seen = hits.clone();
    let app = Router::new().route(
        "/api/v1/scripted",
        axum::routing::any(move || {
            let script = script.clone();
            let seen = seen.clone();
            async move {
                seen.lock().unwrap().push(std::time::Instant::now());
                tokio::time::sleep(script.delay).await;
                let code = script.status.as_u16();
                let body = json!({"error": "scripted", "code": "SCRIPTED", "status": code});
                let mut resp = (script.status, Json(body)).into_response();
                if let Some(value) = script.retry_after {
                    resp.headers_mut()
                        .insert(header::RETRY_AFTER, value.parse().unwrap());
                }
                resp
            }
        }),
    );
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), hits)
}

fn hit_count(hits: &Hits) -> usize {
    hits.lock().unwrap().len()
}

/// A fast policy so retry tests do not sleep for long.
fn quick_policy() -> RetryPolicy {
    RetryPolicy::default()
        .base_delay(Duration::from_millis(1))
        .max_delay(Duration::from_millis(5))
}

fn retrying_client(base: &str, policy: RetryPolicy) -> ServiceClient {
    ServiceClient::builder(base)
        .timeout(Duration::from_secs(5))
        .retry(policy)
        .build()
        .unwrap()
}

fn assert_timeout(err: &ClientError) {
    match err {
        ClientError::Transport(e) => assert!(e.is_timeout(), "not a timeout: {e}"),
        other => panic!("expected a transport timeout, got {other:?}"),
    }
}

/// An accepted 503 that is also `retry_on_status` IS retried; on exhaustion
/// the last response still comes back raw and decodable.
#[tokio::test]
async fn accepted_status_listed_for_retry_is_retried_then_returned_raw() {
    let (base, hits) = spawn_scripted(Script::status(AxumStatus::SERVICE_UNAVAILABLE)).await;
    let client = retrying_client(&base, quick_policy().max_attempts(3));

    let resp = client
        .request(acton_service_client::Method::GET, "scripted")
        .accept_status(StatusCode::SERVICE_UNAVAILABLE)
        .retry_on_status(StatusCode::SERVICE_UNAVAILABLE)
        .send()
        .await
        .expect("an accepted status is returned, not raised");
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(hit_count(&hits), 3, "retried until attempts ran out");
    let body: acton_service_client::ErrorResponse = resp.json().await.unwrap();
    assert_eq!(body.code.as_deref(), Some("SCRIPTED"));

    // And send_json decodes it, as with any accepted status.
    let decoded: acton_service_client::ErrorResponse = client
        .request(acton_service_client::Method::GET, "scripted")
        .accept_status(StatusCode::SERVICE_UNAVAILABLE)
        .retry_on_status(StatusCode::SERVICE_UNAVAILABLE)
        .send_json()
        .await
        .unwrap();
    assert_eq!(decoded.status, 503);
    assert_eq!(hit_count(&hits), 6);
}

/// Without `retry_on_status`, an accepted 503 is returned on the first
/// attempt, exactly as in 0.1.2 (`ready()` depends on this).
#[tokio::test]
async fn accepted_status_alone_is_not_retried() {
    let (base, hits) = spawn_scripted(Script::status(AxumStatus::SERVICE_UNAVAILABLE)).await;
    let client = retrying_client(&base, quick_policy().max_attempts(3));
    let resp = client
        .request(acton_service_client::Method::GET, "scripted")
        .accept_status(StatusCode::SERVICE_UNAVAILABLE)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(hit_count(&hits), 1);
}

/// A POST without `.retriable(true)` is not retried on 421, even when 421 is
/// listed with `retry_on_status`; marking it retriable turns retries on.
#[tokio::test]
async fn post_is_not_retried_on_listed_status_unless_marked_retriable() {
    let (base, hits) = spawn_scripted(Script::status(AxumStatus::MISDIRECTED_REQUEST)).await;
    let client = retrying_client(&base, quick_policy().max_attempts(4));

    let err = client
        .request(acton_service_client::Method::POST, "scripted")
        .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_api().unwrap().status(),
        StatusCode::MISDIRECTED_REQUEST
    );
    assert_eq!(hit_count(&hits), 1, "a POST is not retried by default");

    let err = client
        .request(acton_service_client::Method::POST, "scripted")
        .retriable(true)
        .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.as_api().unwrap().status(),
        StatusCode::MISDIRECTED_REQUEST
    );
    assert_eq!(hit_count(&hits), 1 + 4, "opted in: all four attempts");
}

/// A status that is not retriable by default is retried when listed.
#[tokio::test]
async fn retry_on_status_extends_the_retriable_set() {
    let (base, hits) = spawn_scripted(Script::status(AxumStatus::MISDIRECTED_REQUEST)).await;
    let client = retrying_client(&base, quick_policy().max_attempts(3));

    let _ = client
        .get::<serde_json::Value>("scripted")
        .await
        .unwrap_err();
    assert_eq!(hit_count(&hits), 1, "421 is not retriable by default");

    let _ = client
        .request(acton_service_client::Method::GET, "scripted")
        .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
        .send()
        .await
        .unwrap_err();
    assert_eq!(hit_count(&hits), 1 + 3);
}

/// Two sends of one logical operation share one `deadline_at` budget: the
/// second gets only what the first left.
#[tokio::test]
async fn deadline_at_is_shared_across_sends() {
    let (base, _hits) = spawn_scripted(Script {
        status: AxumStatus::OK,
        retry_after: None,
        delay: Duration::from_millis(300),
    })
    .await;
    // No retry policy: `deadline_at` works on its own.
    let client = client(&base);
    let started = std::time::Instant::now();
    let deadline = started + Duration::from_millis(500);

    client
        .request(acton_service_client::Method::GET, "scripted")
        .deadline_at(deadline)
        .send()
        .await
        .expect("the first send fits in the budget");
    let second_started = std::time::Instant::now();
    let err = client
        .request(acton_service_client::Method::GET, "scripted")
        .deadline_at(deadline)
        .send()
        .await
        .unwrap_err();
    assert_timeout(&err);

    let second = second_started.elapsed();
    assert!(
        second < Duration::from_millis(290),
        "the second send had only the ~200ms the first left, took {second:?}"
    );
    let total = started.elapsed();
    assert!(total >= Duration::from_millis(480), "{total:?}");
    assert!(total < Duration::from_millis(650), "{total:?}");
}

/// A request whose deadline has already passed is not sent at all, and the
/// error says what to do about it.
#[tokio::test]
async fn expired_deadline_fails_without_sending() {
    let (base, hits) = spawn_scripted(Script::status(AxumStatus::OK)).await;
    let past = std::time::Instant::now();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let err = client(&base)
        .request(acton_service_client::Method::GET, "scripted")
        .deadline_at(past)
        .send()
        .await
        .unwrap_err();
    match &err {
        ClientError::Config(message) => {
            assert!(message.contains("deadline"), "{message}");
            assert!(message.contains("not sent"), "{message}");
        }
        other => panic!("expected a config error, got {other:?}"),
    }
    assert!(!err.is_retriable());
    assert_eq!(hit_count(&hits), 0);
}

/// Per-attempt timeout = min(client timeout, remaining): a 5s client timeout
/// under a 200ms deadline times out at the deadline, and the timeout is not
/// retried because no time remains for a pause.
#[tokio::test]
async fn attempt_timeout_is_clamped_to_the_remaining_budget() {
    let (base, hits) = spawn_scripted(Script {
        status: AxumStatus::OK,
        retry_after: None,
        delay: Duration::from_secs(2),
    })
    .await;
    let client = retrying_client(
        &base,
        quick_policy()
            .max_attempts(5)
            .deadline(Duration::from_millis(200)),
    );
    let started = std::time::Instant::now();
    let err = client
        .request(acton_service_client::Method::GET, "scripted")
        .send()
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert_timeout(&err);
    assert!(elapsed >= Duration::from_millis(190), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(400), "{elapsed:?}");
    assert_eq!(hit_count(&hits), 1);
}

/// Per-attempt timeout = min(request timeout, remaining): a 100ms request
/// timeout under a 700ms deadline times out each attempt at 100ms and retries
/// until the budget is spent.
#[tokio::test]
async fn request_timeout_bounds_each_attempt_under_a_deadline() {
    let (base, hits) = spawn_scripted(Script {
        status: AxumStatus::OK,
        retry_after: None,
        delay: Duration::from_secs(2),
    })
    .await;
    let client = retrying_client(
        &base,
        RetryPolicy::default()
            .max_attempts(u32::MAX)
            .base_delay(Duration::from_millis(10))
            .max_delay(Duration::from_millis(10))
            .deadline(Duration::from_millis(700)),
    );
    let started = std::time::Instant::now();
    let err = client
        .request(acton_service_client::Method::GET, "scripted")
        .timeout(Duration::from_millis(100))
        .send()
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert_timeout(&err);
    let attempts = hit_count(&hits);
    // ~110ms per round inside 700ms.
    assert!((4..=7).contains(&attempts), "{attempts} attempts");
    assert!(elapsed < Duration::from_millis(900), "{elapsed:?}");
}

/// Without a deadline, a request timeout replaces the client's timeout.
#[tokio::test]
async fn request_timeout_overrides_the_client_timeout() {
    let (base, _hits) = spawn_scripted(Script {
        status: AxumStatus::OK,
        retry_after: None,
        delay: Duration::from_secs(2),
    })
    .await;
    let started = std::time::Instant::now();
    let err = client(&base)
        .request(acton_service_client::Method::GET, "scripted")
        .timeout(Duration::from_millis(100))
        .send()
        .await
        .unwrap_err();
    assert_timeout(&err);
    assert!(started.elapsed() < Duration::from_millis(1000));
}

/// A `Retry-After` that would carry the next attempt past the deadline stops
/// the loop at once and returns that response's error.
#[tokio::test]
async fn retry_after_crossing_the_deadline_stops_the_loop() {
    let (base, hits) = spawn_scripted(Script {
        status: AxumStatus::SERVICE_UNAVAILABLE,
        retry_after: Some("2"),
        delay: Duration::ZERO,
    })
    .await;
    let client = retrying_client(
        &base,
        quick_policy()
            .max_attempts(5)
            .deadline(Duration::from_secs(1)),
    );
    let started = std::time::Instant::now();
    let err = client
        .get::<serde_json::Value>("scripted")
        .await
        .unwrap_err();
    let api = err.as_api().expect("the 503 is returned");
    assert_eq!(api.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(api.retry_after(), Some(Duration::from_secs(2)));
    assert_eq!(hit_count(&hits), 1);
    assert!(started.elapsed() < Duration::from_millis(500));
}

/// Defaults reproduce 0.1.2: three attempts, pauses of 100ms then 200ms, POST
/// not retried, and an accepted status not retried.
#[tokio::test]
async fn default_policy_reproduces_0_1_2_attempts_and_delays() {
    let (base, hits) = spawn_scripted(Script::status(AxumStatus::SERVICE_UNAVAILABLE)).await;
    let client = retrying_client(&base, RetryPolicy::default());

    let err = client
        .get::<serde_json::Value>("scripted")
        .await
        .unwrap_err();
    assert_eq!(
        err.as_api().unwrap().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let at = hits.lock().unwrap().clone();
    assert_eq!(at.len(), 3);
    let first_gap = at[1] - at[0];
    let second_gap = at[2] - at[1];
    assert!(
        first_gap >= Duration::from_millis(100) && first_gap < Duration::from_millis(195),
        "{first_gap:?}"
    );
    assert!(
        second_gap >= Duration::from_millis(200) && second_gap < Duration::from_millis(395),
        "{second_gap:?}"
    );

    let _ = client
        .post::<_, serde_json::Value>("scripted", &json!({}))
        .await
        .unwrap_err();
    assert_eq!(hit_count(&hits), 3 + 1, "POST is not retried");

    let resp = client
        .request(acton_service_client::Method::GET, "scripted")
        .accept_status(StatusCode::SERVICE_UNAVAILABLE)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        hit_count(&hits),
        3 + 1 + 1,
        "an accepted status is not retried"
    );
}

/// Defaults reproduce 0.1.2 for transport failures too: a refused connection
/// is retried three times with 100ms and 200ms pauses.
#[tokio::test]
async fn default_policy_retries_connect_failures_on_the_0_1_2_schedule() {
    // Bind then drop, so the port refuses connections.
    let listener = TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let client = retrying_client(&format!("http://{addr}"), RetryPolicy::default());
    let started = std::time::Instant::now();
    let err = client
        .get::<serde_json::Value>("anything")
        .await
        .unwrap_err();
    let elapsed = started.elapsed();
    assert!(
        matches!(&err, ClientError::Transport(e) if e.is_connect()),
        "{err:?}"
    );
    assert!(elapsed >= Duration::from_millis(300), "{elapsed:?}");
    assert!(elapsed < Duration::from_millis(600), "{elapsed:?}");
}

// ---------------------------------------------------------------------------
// Per-attempt timeout precedence: request > attempt_timeout > builder timeout
// (built client only) > remaining, always clamped to the deadline.
// ---------------------------------------------------------------------------

/// A server that holds every request for far longer than any test waits.
async fn spawn_stalled() -> (String, Hits) {
    spawn_scripted(Script {
        status: AxumStatus::OK,
        retry_after: None,
        delay: Duration::from_secs(60),
    })
    .await
}

/// One attempt only, so the elapsed time is the first attempt's timeout.
fn single_attempt(deadline: Duration) -> RetryPolicy {
    RetryPolicy::default().max_attempts(1).deadline(deadline)
}

/// Send a GET to `scripted`, expect a timeout, and return how long it took.
async fn time_to_timeout(request: acton_service_client::RequestBuilder) -> Duration {
    let started = std::time::Instant::now();
    let err = request.send().await.unwrap_err();
    let elapsed = started.elapsed();
    assert_timeout(&err);
    elapsed
}

fn assert_near(elapsed: Duration, expected: Duration) {
    let slack = Duration::from_millis(150);
    assert!(
        elapsed + Duration::from_millis(10) >= expected && elapsed < expected + slack,
        "expected ~{expected:?}, took {elapsed:?}"
    );
}

/// The trap, pinned: under a deadline, a supplied client's own timeout is
/// REPLACED by the remaining budget. Its 100ms timeout does not fire; the
/// 400ms deadline does. A request `.timeout()` restores a tighter bound.
#[tokio::test]
async fn supplied_client_timeout_is_replaced_by_the_remaining_budget() {
    let (base, _hits) = spawn_stalled().await;
    let supplied = acton_service_client::reqwest::Client::builder()
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let client = ServiceClient::builder(&base)
        .with_http_client(supplied)
        .retry(single_attempt(Duration::from_millis(400)))
        .build()
        .unwrap();

    let elapsed =
        time_to_timeout(client.request(acton_service_client::Method::GET, "scripted")).await;
    assert_near(elapsed, Duration::from_millis(400));

    let elapsed = time_to_timeout(
        client
            .request(acton_service_client::Method::GET, "scripted")
            .timeout(Duration::from_millis(150)),
    )
    .await;
    assert_near(elapsed, Duration::from_millis(150));
}

/// Without a deadline the supplied client's own timeout is left alone.
#[tokio::test]
async fn supplied_client_keeps_its_own_timeout_without_a_deadline() {
    let (base, _hits) = spawn_stalled().await;
    let supplied = acton_service_client::reqwest::Client::builder()
        .timeout(Duration::from_millis(150))
        .build()
        .unwrap();
    let client = ServiceClient::builder(&base)
        .with_http_client(supplied)
        .build()
        .unwrap();
    let elapsed =
        time_to_timeout(client.request(acton_service_client::Method::GET, "scripted")).await;
    assert_near(elapsed, Duration::from_millis(150));
}

/// The reviewer's case: a supplied client with a 5s `attempt_timeout` under a
/// 15s deadline against a stalled server times out at ~5s, not 15s.
#[tokio::test]
async fn supplied_client_attempt_timeout_bounds_each_attempt_under_a_long_deadline() {
    let (base, hits) = spawn_stalled().await;
    let client = ServiceClient::builder(&base)
        .with_http_client(acton_service_client::reqwest::Client::new())
        .attempt_timeout(Duration::from_secs(5))
        .retry(single_attempt(Duration::from_secs(15)))
        .build()
        .unwrap();
    let elapsed =
        time_to_timeout(client.request(acton_service_client::Method::GET, "scripted")).await;
    assert!(
        elapsed >= Duration::from_millis(4990) && elapsed < Duration::from_millis(5500),
        "expected ~5s, took {elapsed:?}"
    );
    assert_eq!(hit_count(&hits), 1);
}

/// Each precedence level winning in turn, and the deadline clamping them all.
#[tokio::test]
async fn attempt_timeout_precedence_each_level_wins_and_the_deadline_clamps() {
    let (base, _hits) = spawn_stalled().await;
    let ms = Duration::from_millis;
    let get = |c: &ServiceClient| c.request(acton_service_client::Method::GET, "scripted");

    // 1. The request override beats attempt_timeout and the builder timeout.
    let c = ServiceClient::builder(&base)
        .timeout(ms(900))
        .attempt_timeout(ms(600))
        .retry(single_attempt(Duration::from_secs(5)))
        .build()
        .unwrap();
    assert_near(time_to_timeout(get(&c).timeout(ms(150))).await, ms(150));

    // 2. attempt_timeout beats the builder timeout (under a deadline)...
    assert_near(time_to_timeout(get(&c)).await, ms(600));

    // ...and applies without a deadline too (opt-in per-request timeout).
    let c = ServiceClient::builder(&base)
        .timeout(Duration::from_secs(5))
        .attempt_timeout(ms(200))
        .build()
        .unwrap();
    assert_near(time_to_timeout(get(&c)).await, ms(200));

    // 3. The builder timeout, for a built client under a deadline.
    let c = ServiceClient::builder(&base)
        .timeout(ms(250))
        .retry(single_attempt(Duration::from_secs(5)))
        .build()
        .unwrap();
    assert_near(time_to_timeout(get(&c)).await, ms(250));

    // 4. The deadline clamps every level: 300ms remaining beats a 2s
    //    override, a 3s attempt_timeout, and a 5s builder timeout.
    let c = ServiceClient::builder(&base)
        .timeout(Duration::from_secs(5))
        .attempt_timeout(Duration::from_secs(3))
        .retry(single_attempt(ms(300)))
        .build()
        .unwrap();
    assert_near(
        time_to_timeout(get(&c).timeout(Duration::from_secs(2))).await,
        ms(300),
    );
    assert_near(time_to_timeout(get(&c)).await, ms(300));
}
