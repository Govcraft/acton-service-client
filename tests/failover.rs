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
    ClientError, Endpoint, EndpointOrigin, EndpointSetError, Method, RetryObserver, RetryPolicy,
    RetryReason, ServiceClient, StatusCode, reqwest,
};
use axum::http::{HeaderMap, HeaderValue, StatusCode as AxumStatus};
use axum::response::{IntoResponse, Redirect};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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

/// Where a resetting endpoint breaks each connection.
#[derive(Clone, Copy, Debug)]
enum Break {
    /// Once the request head has arrived, while any body may still be on its
    /// way.
    AfterRequestHead,
    /// After answering with a `200` head and part of its body.
    MidResponseBody,
}

/// Accept every connection, read the request head, log the arrival as
/// `index`, then reset the connection (RST) at `at`. The request has been
/// written by then, so the client must never see a connect failure.
async fn resetting_endpoint(index: usize, at: Break, log: Log) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = log.clone();
            tokio::spawn(async move {
                let mut head = Vec::new();
                let mut buf = [0_u8; 4096];
                while !head.windows(4).any(|w| w == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => head.extend_from_slice(&buf[..n]),
                    }
                }
                log.lock().unwrap().push(index);
                if let Break::MidResponseBody = at {
                    let partial = b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                                    content-length: 1000\r\n\r\n{\"partial\":";
                    if stream.write_all(partial).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                stream.set_zero_linger().unwrap();
            });
        }
    });
    format!("http://{addr}")
}

/// One call to the [`RetryObserver`], as recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Seen {
    Rotation(EndpointOrigin, RetryReason),
    Retry(EndpointOrigin, RetryReason, u32),
}

/// Records every observer call, in order.
#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<Seen>>>);

impl RetryObserver for Recorder {
    fn on_rotation(&self, left: &EndpointOrigin, reason: RetryReason) {
        self.0
            .lock()
            .unwrap()
            .push(Seen::Rotation(left.clone(), reason));
    }

    fn on_retry(&self, endpoint: &EndpointOrigin, reason: RetryReason, attempt: u32) {
        self.0
            .lock()
            .unwrap()
            .push(Seen::Retry(endpoint.clone(), reason, attempt));
    }
}

impl Recorder {
    fn take(&self) -> Vec<Seen> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
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
        Err(ClientError::Transport(e)) if !e.is_connect() && !e.is_timeout() => {
            fixture::CallOutcome::Reset
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
        let count =
            |kind: fixture::ScriptedResult| script.iter().filter(|(r, _)| *r == kind).count();
        let (connects, resets) = (
            count(fixture::ScriptedResult::Connect),
            count(fixture::ScriptedResult::Reset),
        );
        urls.push(if resets > 0 {
            assert_eq!(
                resets,
                script.len(),
                "{name}: a resetting endpoint only resets"
            );
            resetting_endpoint(index, Break::AfterRequestHead, log.clone()).await
        } else if connects == 0 {
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

    let recorder = Recorder::default();
    let mut builder = ServiceClient::builder(urls[0].clone())
        .failover_endpoints(urls[1..].iter().cloned())
        .retry_observer(recorder.clone());
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
        recorder.take();

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
        let seen: Vec<fixture::Observed> = recorder
            .take()
            .into_iter()
            .map(|seen| match seen {
                Seen::Rotation(left, reason) => fixture::Observed::Rotation {
                    left: index_of(&client, &left),
                    reason: fixture::Reason::from_label(reason.label()),
                },
                Seen::Retry(endpoint, reason, attempt) => fixture::Observed::Retry {
                    endpoint: index_of(&client, &endpoint),
                    reason: fixture::Reason::from_label(reason.label()),
                    attempt,
                },
            })
            .collect();
        assert_eq!(seen, call.observed, "{ctx}");
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

/// The fixture keeps the observer's completeness guarantee: every attempt but
/// the last is reported exactly once, in order, with its own endpoint and
/// outcome. The runners check the implementation against these lists, so
/// together they pin the guarantee.
#[test]
fn fixture_observed_reports_every_attempt_but_the_last() {
    for scenario in fixture::load().scenarios {
        for (n, call) in scenario.calls.iter().enumerate() {
            let ctx = format!("{} call {n}", scenario.name);
            let sent = &call.attempts;
            assert_eq!(call.observed.len(), sent.len().saturating_sub(1), "{ctx}");
            for (i, (seen, attempt)) in call.observed.iter().zip(sent).enumerate() {
                let reason = attempt.result.reason().expect("a reported attempt");
                let next = sent[i + 1].endpoint;
                let expected = if next == attempt.endpoint {
                    fixture::Observed::Retry {
                        endpoint: attempt.endpoint,
                        reason,
                        attempt: u32::try_from(i + 2).unwrap(),
                    }
                } else {
                    fixture::Observed::Rotation {
                        left: attempt.endpoint,
                        reason,
                    }
                };
                assert_eq!(*seen, expected, "{ctx}: attempt {}", i + 1);
            }
        }
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

/// A reset set up so that it happens mid-body, against `[resetting, healthy]`
/// with a retriable POST that rotates on 421: the transport error must come
/// back as it is, never as a connect failure, and nothing is retried.
async fn reset_is_never_a_connect_failure(at: Break, body: Vec<u8>) {
    let log: Log = Arc::default();
    let resetting = resetting_endpoint(0, at, log.clone()).await;
    let (healthy, healthy_hits) = constant(AxumStatus::OK).await;
    let recorder = Recorder::default();
    let client = ServiceClient::builder(resetting)
        .failover_endpoint(healthy)
        .retry(RetryPolicy::with_max_attempts(4).deadline(Duration::from_secs(5)))
        .retry_observer(recorder.clone())
        .build()
        .unwrap();

    let err = client
        .request(Method::POST, "authorize")
        .retriable(true)
        .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
        .body(body, "application/octet-stream")
        .unwrap()
        .send_json::<serde_json::Value>()
        .await
        .unwrap_err();

    let ClientError::Transport(e) = &err else {
        panic!("{at:?}: expected a transport error, got {err:?}");
    };
    assert!(
        !e.is_connect(),
        "{at:?}: a reset reported as a connect failure: {e:?}"
    );
    assert!(!e.is_timeout(), "{at:?}: {e:?}");
    assert!(!err.is_retriable(), "{at:?}");
    assert!(err.failover_trace().is_none(), "{at:?}");
    assert_eq!(
        *log.lock().unwrap(),
        vec![0],
        "{at:?}: one attempt reached it"
    );
    assert_eq!(
        *healthy_hits.lock().unwrap(),
        0,
        "{at:?}: rotated after a reset"
    );
    assert_eq!(
        recorder.take(),
        Vec::new(),
        "{at:?}: the observer heard a re-send"
    );
    assert_eq!(preferred(&client), 0, "{at:?}");
}

#[tokio::test]
async fn reset_mid_request_body_is_never_a_connect_failure() {
    // A body far larger than the socket buffers, so the reset lands while the
    // client is still writing it.
    reset_is_never_a_connect_failure(Break::AfterRequestHead, vec![b'x'; 8 << 20]).await;
}

#[tokio::test]
async fn reset_mid_response_body_is_never_a_connect_failure() {
    reset_is_never_a_connect_failure(Break::MidResponseBody, Vec::new()).await;
}

#[tokio::test]
async fn single_endpoint_client_reports_every_resend_as_a_retry() {
    let status = |status| fixture::ScriptedResult::Status {
        status,
        retry_after_s: None,
    };
    let log: Log = Arc::default();
    let only = scripted_endpoint(
        0,
        vec![(status(503), 1), (status(421), 1), (status(200), 1)],
        log.clone(),
    )
    .await;
    let recorder = Recorder::default();
    let client = ServiceClient::builder(only)
        .retry(RetryPolicy::with_max_attempts(4).base_delay(Duration::from_millis(10)))
        .retry_observer(recorder.clone())
        .build()
        .unwrap();

    let response = client
        .request(Method::POST, "authorize")
        .retriable(true)
        .retry_on_status(StatusCode::MISDIRECTED_REQUEST)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(*log.lock().unwrap(), vec![0, 0, 0]);
    let origin = client.endpoints()[0].clone();
    assert_eq!(
        recorder.take(),
        vec![
            Seen::Retry(
                origin.clone(),
                RetryReason::Status(StatusCode::SERVICE_UNAVAILABLE),
                2
            ),
            Seen::Retry(
                origin,
                RetryReason::Status(StatusCode::MISDIRECTED_REQUEST),
                3
            ),
        ],
        "a single endpoint re-sends in place: retries only, never a rotation"
    );
}
