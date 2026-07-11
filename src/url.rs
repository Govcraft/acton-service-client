//! Pure URL construction helpers.
//!
//! These functions build request URLs from a base URL and path components using
//! plain string normalization, so they can be unit-tested without a network.
//! The client layer parses the resulting string into a [`url::Url`].

/// Join path parts into a normalized absolute path.
///
/// Each part has leading and trailing slashes stripped; empty parts (and parts
/// that are only slashes) are dropped. The result always starts with `/` and
/// never contains duplicate separators. An empty input yields `"/"`.
///
/// # Examples
///
/// ```
/// use acton_service_client::url::join_segments;
///
/// assert_eq!(join_segments(&["/api/", "v1", "/users/42"]), "/api/v1/users/42");
/// assert_eq!(join_segments(&["", "health"]), "/health");
/// assert_eq!(join_segments(&[]), "/");
/// ```
#[must_use]
pub fn join_segments(parts: &[&str]) -> String {
    let mut out = String::from("/");
    for part in parts {
        let trimmed = part.trim_matches('/');
        if trimmed.is_empty() {
            continue;
        }
        if out.len() > 1 {
            out.push('/');
        }
        out.push_str(trimmed);
    }
    out
}

/// Build a full URL string from a base URL and already-joined absolute path.
///
/// The base URL's trailing slashes are stripped before the path (which must
/// begin with `/`) is appended. This preserves the base URL's own path prefix
/// if one is present (e.g. a base of `https://host/gateway`).
///
/// # Examples
///
/// ```
/// use acton_service_client::url::{build_url, join_segments};
///
/// let path = join_segments(&["/api", "v1", "users/42"]);
/// assert_eq!(
///     build_url("https://api.example.com/", &path),
///     "https://api.example.com/api/v1/users/42"
/// );
/// ```
#[must_use]
pub fn build_url(base_url: &str, absolute_path: &str) -> String {
    let base = base_url.trim_end_matches('/');
    format!("{base}{absolute_path}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_normalizes_slashes() {
        assert_eq!(
            join_segments(&["/api/", "/v1/", "/users/"]),
            "/api/v1/users"
        );
    }

    #[test]
    fn join_drops_empty_and_slash_only_parts() {
        assert_eq!(join_segments(&["", "/", "v1", "//", "x"]), "/v1/x");
    }

    #[test]
    fn join_empty_is_root() {
        assert_eq!(join_segments(&[]), "/");
        assert_eq!(join_segments(&["", "/"]), "/");
    }

    #[test]
    fn join_single_segment() {
        assert_eq!(join_segments(&["health"]), "/health");
        assert_eq!(join_segments(&["/health/"]), "/health");
    }

    #[test]
    fn build_strips_base_trailing_slash() {
        assert_eq!(
            build_url("https://api.example.com/", "/health"),
            "https://api.example.com/health"
        );
        assert_eq!(
            build_url("https://api.example.com", "/health"),
            "https://api.example.com/health"
        );
    }

    #[test]
    fn build_preserves_base_path_prefix() {
        let path = join_segments(&["/api", "v1", "users"]);
        assert_eq!(
            build_url("https://host/gateway", &path),
            "https://host/gateway/api/v1/users"
        );
    }
}
