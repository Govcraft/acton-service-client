//! Health and readiness response types.
//!
//! These types mirror the unversioned `GET /health` and `GET /ready` endpoints
//! exposed by every `acton-service` deployment. They are `Serialize` +
//! `Deserialize` so they round-trip against the genuine framework structs.
//!
//! # Examples
//!
//! ```
//! use acton_service_client::{HealthResponse, ReadinessResponse};
//!
//! let json = r#"{"status":"healthy","service":"users","version":"1.2.3"}"#;
//! let health: HealthResponse = serde_json::from_str(json).unwrap();
//! assert_eq!(health.status, "healthy");
//! assert_eq!(health.service, "users");
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Response body of the unversioned `GET /health` endpoint.
///
/// Always returned with `200 OK` while the service process is running.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    /// Service status, e.g. `"healthy"`.
    pub status: String,
    /// Name of the service reporting health.
    pub service: String,
    /// Service version, when advertised.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
}

impl HealthResponse {
    /// Returns `true` when the reported status is exactly `"healthy"`.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.status == "healthy"
    }
}

/// Response body of the unversioned `GET /ready` endpoint.
///
/// Returned with `200 OK` when ready, or `503 Service Unavailable` when a
/// dependency is unhealthy — in the latter case the body still deserializes
/// into this type via [`crate::ApiError`] diagnostics on the caller side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadinessResponse {
    /// Overall readiness of the service.
    pub ready: bool,
    /// Name of the service reporting readiness.
    pub service: String,
    /// Per-dependency readiness, keyed by dependency name.
    pub dependencies: HashMap<String, DependencyStatus>,
}

/// Readiness of a single downstream dependency.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DependencyStatus {
    /// Whether the dependency is healthy.
    pub healthy: bool,
    /// Optional human-readable detail (e.g. `"Connected"`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_roundtrips_without_version() {
        let h = HealthResponse {
            status: "healthy".to_string(),
            service: "users".to_string(),
            version: None,
        };
        let json = serde_json::to_string(&h).unwrap();
        assert!(!json.contains("version"));
        let back: HealthResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(h, back);
        assert!(back.is_healthy());
    }

    #[test]
    fn readiness_deserializes_dependencies() {
        let json = r#"{
            "ready": false,
            "service": "orders",
            "dependencies": {
                "postgres": {"healthy": true, "message": "Connected"},
                "redis": {"healthy": false, "message": "Connection failed"}
            }
        }"#;
        let r: ReadinessResponse = serde_json::from_str(json).unwrap();
        assert!(!r.ready);
        assert_eq!(r.dependencies.len(), 2);
        assert!(r.dependencies["postgres"].healthy);
        assert!(!r.dependencies["redis"].healthy);
    }

    #[test]
    fn dependency_status_omits_none_message() {
        let d = DependencyStatus {
            healthy: true,
            message: None,
        };
        let json = serde_json::to_string(&d).unwrap();
        assert_eq!(json, r#"{"healthy":true}"#);
    }
}
