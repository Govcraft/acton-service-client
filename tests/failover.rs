//! Endpoint failover over real HTTP: every scenario of
//! `spec/fixtures/endpoint-failover-v1.json` against scripted axum servers,
//! plus the redirect, per-endpoint client, and expired-deadline guarantees.

#[path = "support/failover_fixture.rs"]
mod fixture;
#[path = "support/refused.rs"]
mod refused;

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use acton_service_client::{
    ClientError, Endpoint, EndpointOrigin, EndpointSetError, Method, RetryPolicy, RotationReason,
    ServiceClient, StatusCode, reqwest,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode as AxumStatus};
use axum::response::{IntoResponse, Redirect};

type Log = Arc<Mutex<Vec<usize>>>;

/// Serve `script` in order on a fresh port, logging each arrival as `index`.
async fn scripted_endpoint(
    index: usize,
    script: Vec<(fixture::ScriptedResult, u64)>,
    log: Log,
) -> String {
    let queue = Arc::new(Mutex::new(VecDeque::from(script)));
    let app = axum::Router::new().fallback(move || {
        let queue = queue.clone();
        let log = log.clone();
        async move {
            log.lock().unwrap().push(index);
            let next = queue.lock().unwrap().pop_front();
            match next {
                Some((
                    fixture::ScriptedResult::Status {
                        status,
                        retry_after_s,
                    },
                    latency,
                )) => {
                    tokio::time::sleep(Duration::from_millis(latency)).await;
                    let code = AxumStatus::from_u16(status).unwrap();
                    let body = format!(r#"{{"error":"scripted","status":{status}}}"#);
                    let mut response = (code, body).into_response();
                    if let Some(secs) = retry_after_s {
                        response
                            .headers_mut()
                            .insert("retry-after", HeaderValue::from(secs));
                    }
                    response
                }
                Some((fixture::ScriptedResult::Stall, _)) => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    AxumStatus::OK.into_response()
                }
                _ => (AxumStatus::IM_A_TEAPOT, "unscripted attempt").into_response(),
            }
        }
    });
    serve(app).await
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

fn index_of(client: &ServiceClient, origin: &EndpointOrigin) -> usize {
    client
        .endpoints()
        .iter()
        .position(|o| o == origin)
        .expect("an origin in the set")
}

fn preferred(client: &ServiceClient) -> usize {
    index_of(client, client.preferred_endpoint())
}

fn last_error(error: &ClientError) -> fixture::LastError {
    match error {
        ClientError::Api(api) => fixture::LastError::Api {
            status: api.status().as_u16(),
        },
        ClientError::Transport(e) if e.is_connect() => fixture::LastError::Transport {
            reason: fixture::Reason::Connect,
        },
        ClientError::Transport(e) if e.is_timeout() => fixture::LastError::Transport {
            reason: fixture::Reason::Timeout,
        },
        other => panic!("unexpected error {other:?}"),
    }
}

fn observed(
    client: &ServiceClient,
    result: Result<reqwest::Response, ClientError>,
) -> fixture::CallOutcome {
    match result {
        Ok(response) => fixture::CallOutcome::Ok {
            status: response.status().as_u16(),
        },
        Err(ClientError::DeadlineExceeded { attempts, .. }) => {
            fixture::CallOutcome::DeadlineExceeded { attempts }
        }
        Err(ClientError::EndpointsExhausted(trace)) => fixture::CallOutcome::EndpointsExhausted {
            attempts: trace
                .attempts
                .iter()
                .map(|a| fixture::Traced {
                    endpoint: index_of(client, &a.endpoint),
                    outcome: fixture::Reason::from_label(a.outcome.label()),
                    rotated: a.rotated,
                })
                .collect(),
            last: last_error(&trace.last),
            proves_not_processed: trace.proves_not_processed(),
        },
        Err(other) => match last_error(&other) {
            fixture::LastError::Api { status } => fixture::CallOutcome::Api { status },
            fixture::LastError::Transport { reason } => fixture::CallOutcome::Transport { reason },
        },
    }
}

type Rotations = Arc<Mutex<Vec<(EndpointOrigin, RotationReason)>>>;

async fn run_scenario(scenario: &fixture::Scenario) {
    let name = &scenario.name;
    let mut scripts = vec![Vec::new(); scenario.endpoints];
    for call in &scenario.calls {
        for attempt in &call.attempts {
            scripts[attempt.endpoint].push((attempt.result, attempt.latency_ms));
        }
    }
    let log: Log = Arc::default();
    let mut urls = Vec::new();
    let mut dead = Vec::new();
    for (index, script) in scripts.into_iter().enumerate() {
        let connects = script
            .iter()
            .filter(|(r, _)| *r == fixture::ScriptedResult::Connect)
            .count();
        urls.push(if connects == 0 {
            scripted_endpoint(index, script, log.clone()).await
        } else {
            assert_eq!(
                connects,
                script.len(),
                "{name}: a dead endpoint only refuses"
            );
            let refused = refused::Refused::bind();
            let url = refused.url();
            dead.push(refused);
            url
        });
    }

    let rotations: Rotations = Arc::default();
    let seen = rotations.clone();
    let mut builder = ServiceClient::builder(urls[0].clone())
        .failover_endpoints(urls[1..].iter().cloned())
        .rotation_observer(move |left: &EndpointOrigin, reason: RotationReason| {
            seen.lock().unwrap().push((left.clone(), reason));
        });
    if let Some(policy) = &scenario.policy {
        builder = builder.retry(policy.to_policy());
    }
    if let Some(ms) = scenario.attempt_timeout_ms {
        builder = builder.attempt_timeout(Duration::from_millis(ms));
    }
    let client = builder.build().unwrap();
    assert_eq!(client.endpoints().len(), scenario.endpoints, "{name}");

    for (n, call) in scenario.calls.iter().enumerate() {
        let ctx = format!("{name} call {n}");
        assert_eq!(preferred(&client), call.preferred_before, "{ctx}");
        log.lock().unwrap().clear();
        rotations.lock().unwrap().clear();

        let method = Method::from_bytes(scenario.request.method.as_bytes()).unwrap();
        let mut request = client
            .request(method, "fixture")
            .retriable(scenario.request.retriable);
        for &code in &scenario.request.retry_on_status {
            request = request.retry_on_status(fixture::status(code));
        }
        for &code in &scenario.request.accept_status {
            request = request.accept_status(fixture::status(code));
        }
        let started = Instant::now();
        let result = request.send().await;
        let elapsed = started.elapsed();

        assert_eq!(observed(&client, result), call.outcome, "{ctx}");
        let reached: Vec<usize> = call
            .attempts
            .iter()
            .filter(|a| a.result != fixture::ScriptedResult::Connect)
            .map(|a| a.endpoint)
            .collect();
        assert_eq!(
            *log.lock().unwrap(),
            reached,
            "{ctx}: requests that reached a server"
        );
        let rotated: Vec<fixture::Rotation> = rotations
            .lock()
            .unwrap()
            .iter()
            .map(|(left, reason)| fixture::Rotation {
                left: index_of(&client, left),
                reason: fixture::Reason::from_label(reason.label()),
            })
            .collect();
        assert_eq!(rotated, call.rotations, "{ctx}");
        assert_eq!(preferred(&client), call.preferred_after, "{ctx}");
        if call.draws.is_empty() {
            let paused: u64 = call
                .attempts
                .iter()
                .filter_map(|a| match a.next {
                    Some(fixture::NextStep::After { pause_ms }) => Some(pause_ms),
                    _ => None,
                })
                .sum();
            assert!(
                elapsed >= Duration::from_millis(paused),
                "{ctx}: {elapsed:?} < {paused}ms of pauses"
            );
        }
    }
}

#[tokio::test]
async fn fixture_scenarios_over_real_http() {
    let fixture = fixture::load();
    assert!(!fixture.scenarios.is_empty());
    for scenario in &fixture.scenarios {
        run_scenario(scenario).await;
    }
}

#[test]
fn fixture_reasons_table() {
    for row in fixture::load().reasons {
        let reason = row.reason.to_reason();
        assert_eq!(reason.label(), row.label);
        assert_eq!(reason.proves_not_processed(), row.proves_not_processed);
    }
}

#[test]
fn fixture_validation_rows_through_the_builder() {
    for row in fixture::load().validation {
        let built = ServiceClient::builder(row.base.clone())
            .failover_endpoints(row.failover.iter().cloned())
            .build();
        match (built, &row.endpoints, &row.error) {
            (Ok(client), Some(expected), None) => {
                let shown: Vec<String> =
                    client.endpoints().iter().map(ToString::to_string).collect();
                assert_eq!(&shown, expected, "{}", row.name);
            }
            (Err(ClientError::InvalidEndpoints(err)), None, Some(expected)) => {
                assert!(err.to_string().contains("endpoint"), "{}: {err}", row.name);
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
                    other => panic!("{}: unexpected {other:?}", row.name),
                };
                assert_eq!(&got, expected, "{}", row.name);
            }
            (got, _, _) => panic!("{}: unexpected {got:?}", row.name),
        }
    }
}

/// A server that answers every request with `status` and counts arrivals.
async fn constant(status: AxumStatus) -> (String, Arc<Mutex<u32>>) {
    let hits = Arc::new(Mutex::new(0));
    let counter = hits.clone();
    let app = axum::Router::new().fallback(move || {
        let counter = counter.clone();
        async move {
            *counter.lock().unwrap() += 1;
            (status, r#"{"error":"constant","status":0}"#)
        }
    });
    (serve(app).await, hits)
}

fn retrying_set(primary: &str, backup: &str) -> ServiceClient {
    ServiceClient::builder(primary)
        .failover_endpoint(backup)
        .retry(RetryPolicy::with_max_attempts(4).deadline(Duration::from_secs(5)))
        .build()
        .unwrap()
}

#[tokio::test]
async fn non_retriable_post_never_reaches_the_healthy_endpoint() {
    let (misdirected, first_hits) = constant(AxumStatus::MISDIRECTED_REQUEST).await;
    let (healthy, healthy_hits) = constant(AxumStatus::OK).await;
    let client = retrying_set(&misdirected, &healthy);

    let err = client
        .request(Method::POST, "authorize")
        .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
        .send()
        .await
        .unwrap_err();

    assert_eq!(
        err.as_api().map(|api| api.status()),
        Some(StatusCode::MISDIRECTED_REQUEST)
    );
    assert!(err.failover_trace().is_none());
    assert_eq!(*first_hits.lock().unwrap(), 1);
    assert_eq!(
        *healthy_hits.lock().unwrap(),
        0,
        "the healthy endpoint was touched"
    );
    assert_eq!(preferred(&client), 0);
}

#[tokio::test]
async fn past_deadline_sends_nothing_to_any_endpoint() {
    let (a, a_hits) = constant(AxumStatus::OK).await;
    let (b, b_hits) = constant(AxumStatus::OK).await;
    let client = retrying_set(&a, &b);
    let past = Instant::now()
        .checked_sub(Duration::from_millis(5))
        .expect("an instant in the past");

    let err = client
        .request(Method::GET, "orders/7")
        .deadline_at(past)
        .send()
        .await
        .unwrap_err();

    assert!(
        matches!(err, ClientError::DeadlineExceeded { attempts: 0, .. }),
        "{err:?}"
    );
    assert!(err.failover_trace().is_none());
    assert!(
        err.to_string().contains("before anything was sent"),
        "{err}"
    );
    assert_eq!(*a_hits.lock().unwrap() + *b_hits.lock().unwrap(), 0);
}

/// A server whose every request is redirected to `location` (same path).
async fn redirecting_to(location: String) -> String {
    let app = axum::Router::new().fallback(move |uri: axum::http::Uri| {
        let target = format!("{location}{}", uri.path());
        async move { Redirect::temporary(&target) }
    });
    serve(app).await
}

#[tokio::test]
async fn built_client_follows_redirects_only_within_the_set() {
    let (backup, backup_hits) = constant(AxumStatus::OK).await;
    let (outside, outside_hits) = constant(AxumStatus::OK).await;

    let to_backup = redirecting_to(backup.clone()).await;
    let client = retrying_set(&to_backup, &backup);
    let response = client.request(Method::GET, "x").send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*backup_hits.lock().unwrap(), 1);

    let to_outside = redirecting_to(outside.clone()).await;
    let client = retrying_set(&to_outside, &backup);
    let err = client.request(Method::GET, "x").send().await.unwrap_err();
    let ClientError::Transport(transport) = &err else {
        panic!("expected a refused redirect, got {err:?}");
    };
    assert!(transport.is_redirect(), "{transport:?}");
    assert_eq!(*outside_hits.lock().unwrap(), 0, "left the endpoint set");
}

#[tokio::test]
async fn supplied_client_redirected_off_the_set_is_a_config_error() {
    let (backup, _) = constant(AxumStatus::OK).await;
    let (outside, _) = constant(AxumStatus::OK).await;
    let to_outside = redirecting_to(outside).await;
    let client = ServiceClient::builder(to_outside)
        .with_http_client(reqwest::Client::new())
        .failover_endpoint(backup)
        .build()
        .unwrap();

    let err = client.request(Method::GET, "x").send().await.unwrap_err();
    let ClientError::Config(message) = &err else {
        panic!("expected a config error, got {err:?}");
    };
    assert!(message.contains("outside the endpoint set"), "{message}");
    assert!(message.contains("Policy::none()"), "{message}");
}

#[tokio::test]
async fn single_endpoint_client_still_follows_any_redirect() {
    let (outside, outside_hits) = constant(AxumStatus::OK).await;
    let client = ServiceClient::builder(redirecting_to(outside).await)
        .build()
        .unwrap();
    let response = client.request(Method::GET, "x").send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*outside_hits.lock().unwrap(), 1);
}

#[tokio::test]
async fn per_endpoint_client_carries_that_endpoints_requests() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let record = seen.clone();
    let app = axum::Router::new().fallback(move |headers: HeaderMap| {
        let record = record.clone();
        async move {
            let via = headers
                .get("x-via")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("shared")
                .to_string();
            record.lock().unwrap().push(via);
            AxumStatus::OK
        }
    });
    let backup = serve(app).await;
    let refused = refused::Refused::bind();
    let primary = refused.url();

    let mut own_headers = reqwest::header::HeaderMap::new();
    own_headers.insert("x-via", reqwest::header::HeaderValue::from_static("backup"));
    let own = reqwest::Client::builder()
        .default_headers(own_headers)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let client = ServiceClient::builder(primary)
        .failover_endpoint(Endpoint::new(backup).with_http_client(own))
        .retry(RetryPolicy::with_max_attempts(3))
        .build()
        .unwrap();

    let response = client.request(Method::GET, "x").send().await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*seen.lock().unwrap(), ["backup"]);
}
