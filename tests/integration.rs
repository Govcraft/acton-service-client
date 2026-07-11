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
        .route("/api/v1/rate-limited", get(rate_limited))
        .route("/api/v1/locked", get(locked))
        .route("/api/v1/broken", get(broken))
        .route("/api/v1/missing", get(not_found))
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
