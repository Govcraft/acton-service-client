//! API version selection for versioned routes.
//!
//! Services built on `acton-service` mount versioned routes under
//! `{base_path}/{version}` — canonically `/api/v1`. This module mirrors the
//! `ApiVersion` enum from `acton-service` exactly so the two crates agree on
//! the path segment and parsing rules.
//!
//! # Examples
//!
//! ```
//! use acton_service_client::ApiVersion;
//!
//! assert_eq!(ApiVersion::V1.as_path_segment(), "v1");
//! assert_eq!(ApiVersion::V2.as_number(), 2);
//! assert_eq!(ApiVersion::parse("V3"), Some(ApiVersion::V3));
//! assert_eq!(ApiVersion::parse("1"), Some(ApiVersion::V1));
//! assert_eq!(ApiVersion::parse("v9"), None);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported API versions for versioned routes.
///
/// Mirrors `acton_service::versioning::ApiVersion` (variants `V1`..`V5`).
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum ApiVersion {
    /// Version 1 (`v1`).
    #[default]
    V1,
    /// Version 2 (`v2`).
    V2,
    /// Version 3 (`v3`).
    V3,
    /// Version 4 (`v4`).
    V4,
    /// Version 5 (`v5`).
    V5,
}

impl ApiVersion {
    /// Parse a version from a string such as `"v1"`, `"V1"`, or `"1"`.
    ///
    /// Returns `None` for unrecognized values.
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ApiVersion;
    ///
    /// assert_eq!(ApiVersion::parse("v2"), Some(ApiVersion::V2));
    /// assert_eq!(ApiVersion::parse("V2"), Some(ApiVersion::V2));
    /// assert_eq!(ApiVersion::parse("2"), Some(ApiVersion::V2));
    /// assert_eq!(ApiVersion::parse("nope"), None);
    /// ```
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "v1" | "V1" | "1" => Some(Self::V1),
            "v2" | "V2" | "2" => Some(Self::V2),
            "v3" | "V3" | "3" => Some(Self::V3),
            "v4" | "V4" | "4" => Some(Self::V4),
            "v5" | "V5" | "5" => Some(Self::V5),
            _ => None,
        }
    }

    /// Return the numeric form of the version (`1`..`5`).
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ApiVersion;
    ///
    /// assert_eq!(ApiVersion::V4.as_number(), 4);
    /// ```
    #[must_use]
    pub fn as_number(&self) -> u8 {
        match self {
            Self::V1 => 1,
            Self::V2 => 2,
            Self::V3 => 3,
            Self::V4 => 4,
            Self::V5 => 5,
        }
    }

    /// Return the path segment for this version (e.g. `"v1"`).
    ///
    /// # Examples
    ///
    /// ```
    /// use acton_service_client::ApiVersion;
    ///
    /// assert_eq!(ApiVersion::V5.as_path_segment(), "v5");
    /// ```
    #[must_use]
    pub fn as_path_segment(&self) -> &'static str {
        match self {
            Self::V1 => "v1",
            Self::V2 => "v2",
            Self::V3 => "v3",
            Self::V4 => "v4",
            Self::V5 => "v5",
        }
    }
}

impl fmt::Display for ApiVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_path_segment())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_accepts_all_documented_forms() {
        for (input, expected) in [
            ("v1", ApiVersion::V1),
            ("V1", ApiVersion::V1),
            ("1", ApiVersion::V1),
            ("v5", ApiVersion::V5),
            ("V5", ApiVersion::V5),
            ("5", ApiVersion::V5),
        ] {
            assert_eq!(ApiVersion::parse(input), Some(expected));
        }
    }

    #[test]
    fn parse_rejects_unknown() {
        assert_eq!(ApiVersion::parse(""), None);
        assert_eq!(ApiVersion::parse("v0"), None);
        assert_eq!(ApiVersion::parse("v6"), None);
        assert_eq!(ApiVersion::parse("latest"), None);
    }

    #[test]
    fn number_and_segment_agree() {
        for v in [
            ApiVersion::V1,
            ApiVersion::V2,
            ApiVersion::V3,
            ApiVersion::V4,
            ApiVersion::V5,
        ] {
            assert_eq!(v.as_path_segment(), format!("v{}", v.as_number()));
        }
    }

    #[test]
    fn default_is_v1() {
        assert_eq!(ApiVersion::default(), ApiVersion::V1);
    }

    #[test]
    fn display_matches_segment() {
        assert_eq!(ApiVersion::V3.to_string(), "v3");
    }
}
