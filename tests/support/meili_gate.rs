//! Shared gate for live Meilisearch integration tests.
//!
//! Included from integration-test crates with
//! `#[path = "support/meili_gate.rs"] mod meili_gate;`. Cargo only discovers
//! `tests/*.rs` as test targets, so this file is never a target by itself.
//!
//! Without `RAG_TEST_REQUIRE_MEILI`, a missing `RAG_TEST_MEILI_URL`, an
//! unreachable server, or a keyed server without `RAG_TEST_MEILI_API_KEY`
//! skips the test (it returns early and is reported as passed). With
//! `RAG_TEST_REQUIRE_MEILI=1` (set by CI), every one of those conditions, and
//! a missing `RAG_TEST_MEILI_API_KEY`, panics so a broken service container
//! cannot turn the Meili suite into silent green.

use std::{
    net::{TcpStream, ToSocketAddrs},
    time::Duration,
};

pub const URL_VAR: &str = "RAG_TEST_MEILI_URL";
pub const API_KEY_VAR: &str = "RAG_TEST_MEILI_API_KEY";
pub const REQUIRE_VAR: &str = "RAG_TEST_REQUIRE_MEILI";

/// Connection settings for a reachable live Meilisearch server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeiliTarget {
    pub url: String,
    pub api_key: Option<String>,
}

/// Result of probing the environment before a live test runs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateState {
    UrlUnset,
    Unreachable { url: String },
    KeyRequired { url: String },
    Ready(MeiliTarget),
}

impl GateState {
    fn reason(&self) -> String {
        match self {
            GateState::UrlUnset => format!("{URL_VAR} is not set"),
            GateState::Unreachable { url } => format!("{url} is not reachable"),
            GateState::KeyRequired { url } => format!("{url} requires a key; set {API_KEY_VAR}"),
            GateState::Ready(_) => "Meilisearch is ready".to_string(),
        }
    }
}

/// True for the accepted spellings of an enabled boolean switch.
pub fn is_truthy(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether the environment demands that live Meili tests actually run.
pub fn meili_required() -> bool {
    let value = std::env::var(REQUIRE_VAR).unwrap_or_default();
    is_truthy(&value)
}

/// Pure decision: `Ok(Some(target))` runs the test, `Ok(None)` skips it, and
/// `Err(message)` must fail it.
pub fn gate_outcome(required: bool, state: GateState) -> Result<Option<MeiliTarget>, String> {
    match state {
        GateState::Ready(target) if required && target.api_key.is_none() => Err(format!(
            "{REQUIRE_VAR} is set but {API_KEY_VAR} is not; required Meilisearch \
             integration runs must use an authenticated server"
        )),
        GateState::Ready(target) => Ok(Some(target)),
        other if required => Err(format!(
            "{REQUIRE_VAR} is set but {}; refusing to skip the Meilisearch integration test",
            other.reason()
        )),
        other => {
            let reason = other.reason();
            eprintln!("skipping Meilisearch integration test; {reason}");
            Ok(None)
        }
    }
}

/// TCP reachability probe for the Meili authority (default port 7700).
pub fn meili_available(url: &str) -> bool {
    let authority = url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or_default();
    let address = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:7700")
    };
    let Ok(addrs) = address.to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok())
}

async fn server_requires_key(url: &str) -> bool {
    reqwest::Client::new()
        .get(format!("{}/indexes", url.trim_end_matches('/')))
        .send()
        .await
        .is_ok_and(|response| response.status().as_u16() == 401)
}

fn non_empty_env(name: &str) -> Option<String> {
    let value = std::env::var(name).ok()?;
    (!value.trim().is_empty()).then_some(value)
}

/// Probe the environment and return the live target, `None` to skip, or
/// panic when `RAG_TEST_REQUIRE_MEILI` forbids skipping.
pub async fn meili_target() -> Option<MeiliTarget> {
    let api_key = non_empty_env(API_KEY_VAR);
    let state = match non_empty_env(URL_VAR) {
        None => GateState::UrlUnset,
        Some(url) if !meili_available(&url) => GateState::Unreachable { url },
        Some(url) if api_key.is_none() && server_requires_key(&url).await => {
            GateState::KeyRequired { url }
        }
        Some(url) => GateState::Ready(MeiliTarget { url, api_key }),
    };
    match gate_outcome(meili_required(), state) {
        Ok(target) => target,
        Err(message) => panic!("{message}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(api_key: Option<&str>) -> MeiliTarget {
        MeiliTarget {
            url: "http://127.0.0.1:7700".to_string(),
            api_key: api_key.map(str::to_string),
        }
    }

    #[test]
    fn meili_gate_truthy_values_are_explicit() {
        for value in ["1", "true", "TRUE", " yes ", "on"] {
            assert!(is_truthy(value), "{value}");
        }
        for value in ["", "0", "false", "no", "off", "required"] {
            assert!(!is_truthy(value), "{value}");
        }
    }

    #[test]
    fn meili_gate_skips_unavailable_server_when_not_required() {
        for state in [
            GateState::UrlUnset,
            GateState::Unreachable {
                url: "http://127.0.0.1:1".to_string(),
            },
            GateState::KeyRequired {
                url: "http://127.0.0.1:7700".to_string(),
            },
        ] {
            assert_eq!(gate_outcome(false, state), Ok(None));
        }
    }

    #[test]
    fn meili_gate_fails_unavailable_server_when_required() {
        for (state, expected) in [
            (GateState::UrlUnset, URL_VAR),
            (
                GateState::Unreachable {
                    url: "http://127.0.0.1:1".to_string(),
                },
                "not reachable",
            ),
            (
                GateState::KeyRequired {
                    url: "http://127.0.0.1:7700".to_string(),
                },
                API_KEY_VAR,
            ),
        ] {
            let error = gate_outcome(true, state).expect_err("required gate must fail");
            assert!(error.contains(REQUIRE_VAR), "{error}");
            assert!(error.contains(expected), "{error}");
        }
    }

    #[test]
    fn meili_gate_required_run_needs_an_api_key() {
        let error = gate_outcome(true, GateState::Ready(target(None)))
            .expect_err("required gate must demand a key");
        assert!(error.contains(API_KEY_VAR), "{error}");
        assert_eq!(
            gate_outcome(true, GateState::Ready(target(Some("ci-key")))),
            Ok(Some(target(Some("ci-key"))))
        );
    }

    #[test]
    fn meili_gate_optional_run_accepts_an_unkeyed_open_server() {
        assert_eq!(
            gate_outcome(false, GateState::Ready(target(None))),
            Ok(Some(target(None)))
        );
    }
}
