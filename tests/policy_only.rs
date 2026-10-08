//! The retry policy without the HTTP client.
//!
//! With `default-features = false` the crate is its `retry` module, which a
//! caller that does its own sending (a sans-IO state machine, for example one
//! built for wasm32-unknown-unknown) drives with its own clock. These tests run
//! under every feature set and reach the policy only through public paths.

use std::path::Path;
use std::time::Duration;

use acton_service_client::retry::{
    Jitter, RetryPolicy, is_idempotent, parse_retry_after_value, wants_retry,
};
use acton_service_client::{Method, StatusCode};

/// One scripted response: its status and `Retry-After` header, if any.
type Answer = (StatusCode, Option<&'static str>);

/// Drive `policy` over `answers` the way a sans-IO caller does, on a virtual
/// clock where each attempt takes `took`: classify each answer, then ask for
/// the pause. Returns the pauses taken and the attempt the call stopped at.
fn run(policy: &RetryPolicy, answers: &[Answer], took: Duration) -> (Vec<Duration>, u32) {
    let mut elapsed = Duration::ZERO;
    let mut pauses = Vec::new();
    for (attempt, (status, header)) in (1u32..).zip(answers) {
        elapsed += took;
        let retry_after = header.and_then(parse_retry_after_value);
        if status.is_success() || !wants_retry(*status, retry_after, false, &[]) {
            return (pauses, attempt);
        }
        let remaining = policy.deadline.map(|d| d.saturating_sub(elapsed));
        match policy.next_pause(attempt, 0.0, retry_after, remaining) {
            Some(pause) => {
                elapsed += pause;
                pauses.push(pause);
            }
            None => return (pauses, attempt),
        }
    }
    panic!("the script ran out before the call stopped");
}

#[test]
fn a_sans_io_caller_backs_off_honours_retry_after_and_stops_at_the_deadline() {
    let ms = Duration::from_millis;
    let policy = RetryPolicy::with_max_attempts(10)
        .base_delay(ms(100))
        .max_delay(ms(1000))
        .deadline(ms(2000));
    let answers = [
        (StatusCode::SERVICE_UNAVAILABLE, None),        // backoff 100
        (StatusCode::BAD_GATEWAY, Some("Wed, 21 Oct")), // a date is no Retry-After: 200
        (StatusCode::TOO_MANY_REQUESTS, Some("1")),     // Retry-After wins: 1000
        (StatusCode::SERVICE_UNAVAILABLE, None),        // 800 would reach the deadline
        (StatusCode::OK, None),
    ];
    // 10 ms per attempt: 1340 ms have passed when attempt 4 answers.
    assert_eq!(
        run(&policy, &answers, ms(10)),
        (vec![ms(100), ms(200), ms(1000)], 4)
    );
}

#[test]
fn a_sans_io_caller_returns_an_answer_that_is_not_retriable() {
    let policy = RetryPolicy::default().jitter(Jitter::Full);
    for status in [StatusCode::NOT_FOUND, StatusCode::LOCKED, StatusCode::OK] {
        assert_eq!(
            run(&policy, &[(status, None)], Duration::ZERO),
            (vec![], 1),
            "{status}"
        );
    }
    // 423 with a delay is retriable, and its Retry-After is the pause.
    assert_eq!(
        run(
            &policy,
            &[(StatusCode::LOCKED, Some("2")), (StatusCode::OK, None)],
            Duration::ZERO
        ),
        (vec![Duration::from_secs(2)], 2)
    );
}

#[test]
fn only_idempotent_methods_retry_without_an_opt_in() {
    for method in [Method::GET, Method::HEAD, Method::DELETE, Method::PUT] {
        assert!(is_idempotent(&method), "{method}");
    }
    for method in [Method::POST, Method::PATCH] {
        assert!(!is_idempotent(&method), "{method}");
    }
}

/// `Instant::now` panics on wasm32-unknown-unknown, and that panic would only
/// show when a pause is computed, so no build catches it. The policy takes the
/// time remaining as an argument instead; this keeps every clock read out of
/// the code the crate builds without `transport`.
#[test]
fn the_policy_reads_no_clock() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut found = Vec::new();
    for file in ["lib.rs", "retry.rs"] {
        let text = std::fs::read_to_string(src.join(file)).unwrap();
        // The unit tests at the end of a file are not built into the crate.
        let built = text.split("\n#[cfg(test)]\nmod tests {").next().unwrap();
        for (index, line) in built.lines().enumerate() {
            let code = line.split("//").next().unwrap_or_default();
            if ["Instant::now", "SystemTime::now"]
                .iter()
                .any(|read| code.contains(read))
            {
                found.push(format!("src/{file}:{}: {}", index + 1, line.trim()));
            }
        }
    }
    assert!(found.is_empty(), "{}", found.join("\n"));
}
