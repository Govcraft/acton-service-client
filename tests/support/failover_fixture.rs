//! The model of `spec/fixtures/endpoint-failover-v1.json`, shared by the
//! in-crate virtual-clock pass and the real-HTTP pass.

use std::time::Duration;

use serde::Deserialize;

/// The fixture, embedded at compile time.
pub const FIXTURE: &str = include_str!("../../spec/fixtures/endpoint-failover-v1.json");

/// Parse the fixture.
pub fn load() -> Fixture {
    serde_json::from_str(FIXTURE).expect("endpoint-failover-v1.json is valid")
}

#[derive(Debug, Deserialize)]
pub struct Fixture {
    pub reasons: Vec<ReasonRow>,
    pub validation: Vec<ValidationRow>,
    pub scenarios: Vec<Scenario>,
}

#[derive(Debug, Deserialize)]
pub struct ReasonRow {
    pub reason: Reason,
    pub label: String,
    pub proves_not_processed: bool,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Reason {
    Status { status: u16 },
    Connect,
    Timeout,
}

#[derive(Debug, Deserialize)]
pub struct ValidationRow {
    pub name: String,
    pub base: String,
    pub failover: Vec<String>,
    #[serde(default)]
    pub endpoints: Option<Vec<String>>,
    #[serde(default)]
    pub error: Option<SetError>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SetError {
    DuplicateOrigin {
        origin: String,
        first: usize,
        duplicate: usize,
    },
    MixedScheme {
        index: usize,
        expected: String,
        found: String,
    },
    NotAnOrigin {
        index: usize,
        reason: String,
    },
}

#[derive(Debug, Deserialize)]
pub struct Scenario {
    pub name: String,
    pub endpoints: usize,
    #[serde(default)]
    pub attempt_timeout_ms: Option<u64>,
    pub policy: Option<Policy>,
    pub request: Request,
    pub calls: Vec<Call>,
}

#[derive(Debug, Deserialize)]
pub struct Policy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    pub jitter: String,
    pub deadline_ms: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct Request {
    pub method: String,
    #[serde(default)]
    pub retriable: bool,
    pub retry_on_status: Vec<u16>,
    #[serde(default)]
    pub accept_status: Vec<u16>,
}

#[derive(Debug, Deserialize)]
pub struct Call {
    pub preferred_before: usize,
    #[serde(default)]
    pub draws: Vec<f64>,
    pub attempts: Vec<ScriptedAttempt>,
    pub observed: Vec<Observed>,
    pub outcome: CallOutcome,
    pub preferred_after: usize,
}

#[derive(Debug, Deserialize)]
pub struct ScriptedAttempt {
    pub endpoint: usize,
    pub result: ScriptedResult,
    #[serde(default = "one")]
    pub latency_ms: u64,
    #[serde(default)]
    pub next: Option<NextStep>,
}

fn one() -> u64 {
    1
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScriptedResult {
    Status {
        status: u16,
        #[serde(default)]
        retry_after_s: Option<u64>,
    },
    Connect,
    Stall,
    Reset,
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NextStep {
    Now,
    After { pause_ms: u64 },
}

/// One call to the retry observer, in order.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Observed {
    Rotation {
        left: usize,
        reason: Reason,
    },
    Retry {
        endpoint: usize,
        reason: Reason,
        attempt: u32,
    },
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CallOutcome {
    Ok {
        status: u16,
    },
    Api {
        status: u16,
    },
    Transport {
        reason: Reason,
    },
    /// A transport failure after the request was written: neither a connect
    /// failure nor a timeout, returned as it is.
    Reset,
    DeadlineExceeded {
        attempts: u32,
    },
    EndpointsExhausted {
        attempts: Vec<Traced>,
        last: LastError,
        proves_not_processed: bool,
    },
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Traced {
    pub endpoint: usize,
    pub outcome: Reason,
    pub rotated: bool,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LastError {
    Api { status: u16 },
    Transport { reason: Reason },
}

impl ScriptedResult {
    /// The outcome an observer reports for an attempt that ended this way,
    /// or `None` for a reset, which always ends the call.
    pub fn reason(self) -> Option<Reason> {
        match self {
            Self::Status { status, .. } => Some(Reason::Status { status }),
            Self::Connect => Some(Reason::Connect),
            Self::Stall => Some(Reason::Timeout),
            Self::Reset => None,
        }
    }
}

impl Policy {
    /// The policy as the crate's type.
    pub fn to_policy(&self) -> acton_service_client::RetryPolicy {
        let jitter = match self.jitter.as_str() {
            "none" => acton_service_client::Jitter::None,
            "full" => acton_service_client::Jitter::Full,
            other => panic!("unknown jitter {other:?}"),
        };
        let policy = acton_service_client::RetryPolicy::with_max_attempts(self.max_attempts)
            .base_delay(Duration::from_millis(self.base_delay_ms))
            .max_delay(Duration::from_millis(self.max_delay_ms))
            .jitter(jitter);
        match self.deadline_ms {
            Some(ms) => policy.deadline(Duration::from_millis(ms)),
            None => policy,
        }
    }
}

impl Reason {
    /// The fixture form of a crate reason, from its
    /// [`label`](acton_service_client::RetryReason::label).
    pub fn from_label(label: &str) -> Self {
        match label {
            "connect" => Self::Connect,
            "timeout" => Self::Timeout,
            code => Self::Status {
                status: code.parse().expect("a status label"),
            },
        }
    }

    /// The reason as the crate's type.
    pub fn to_reason(self) -> acton_service_client::RetryReason {
        match self {
            Self::Status { status } => acton_service_client::RetryReason::Status(
                acton_service_client::StatusCode::from_u16(status).expect("a valid status"),
            ),
            Self::Connect => acton_service_client::RetryReason::Connect,
            Self::Timeout => acton_service_client::RetryReason::Timeout,
        }
    }
}

/// Parse a status code from the fixture.
pub fn status(code: u16) -> acton_service_client::StatusCode {
    acton_service_client::StatusCode::from_u16(code).expect("a valid status")
}
