use axum::{
    http::{header::RETRY_AFTER, HeaderValue, StatusCode},
    response::IntoResponse,
    Json,
};
use serde::Serialize;
use serde_json::{json, Value};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SafeCauseDiagnostic {
    pub category: &'static str,
    pub fingerprint: String,
}

pub(crate) fn safe_cause_diagnostic(
    cause: &(impl std::fmt::Display + ?Sized),
) -> SafeCauseDiagnostic {
    let cause = cause.to_string();
    SafeCauseDiagnostic {
        category: safe_cause_category(&cause),
        fingerprint: safe_value_fingerprint("cause", &cause),
    }
}

pub(crate) fn safe_value_fingerprint(namespace: &str, value: &str) -> String {
    let namespaced = format!("{namespace}\0{value}");
    format!(
        "hmac:{}",
        crate::request_context::fingerprint_current(&namespaced)
    )
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub error: ErrorInfo,
}

#[derive(Debug, Serialize)]
pub struct ErrorInfo {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Value>,
}

#[derive(Debug, Error)]
pub enum ApiError {
    #[error("{0}")]
    BadRequest(String),
    #[error("{message}")]
    Validation { field: String, message: String },
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("request payload is too large")]
    PayloadTooLarge,
    #[error("too many requests")]
    TooManyRequests(u64),
    #[error("service unavailable")]
    ServiceUnavailable(u64),
    #[error("request timed out")]
    Timeout,
    #[error("{0}")]
    Upstream(String),
    #[error("{0}")]
    Internal(String),
}

impl ApiError {
    pub fn bad_request(message: impl Into<String>) -> Self {
        Self::BadRequest(message.into())
    }

    pub fn validation(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Validation {
            field: field.into(),
            message: message.into(),
        }
    }

    pub fn forbidden(message: impl Into<String>) -> Self {
        Self::Forbidden(message.into())
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    pub fn payload_too_large() -> Self {
        Self::PayloadTooLarge
    }

    pub fn too_many_requests(retry_after_seconds: u64) -> Self {
        Self::TooManyRequests(retry_after_seconds)
    }

    pub fn service_unavailable(retry_after_seconds: u64) -> Self {
        Self::ServiceUnavailable(retry_after_seconds)
    }

    pub fn timeout() -> Self {
        Self::Timeout
    }

    fn status_and_code(&self) -> (StatusCode, &'static str) {
        match self {
            Self::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request"),
            Self::Validation { .. } => (StatusCode::BAD_REQUEST, "validation_error"),
            Self::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
            Self::Forbidden(_) => (StatusCode::FORBIDDEN, "forbidden"),
            Self::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            Self::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            Self::PayloadTooLarge => (StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large"),
            Self::TooManyRequests(_) => (StatusCode::TOO_MANY_REQUESTS, "too_many_requests"),
            Self::ServiceUnavailable(_) => (StatusCode::SERVICE_UNAVAILABLE, "service_unavailable"),
            Self::Timeout => (StatusCode::GATEWAY_TIMEOUT, "timeout"),
            Self::Upstream(_) => (StatusCode::BAD_GATEWAY, "upstream_error"),
            Self::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, "internal_error"),
        }
    }

    fn public_message(&self) -> &str {
        match self {
            Self::BadRequest(message)
            | Self::Validation { message, .. }
            | Self::Unauthorized(message)
            | Self::Forbidden(message)
            | Self::NotFound(message)
            | Self::Conflict(message) => message,
            Self::PayloadTooLarge => "request payload is too large",
            Self::TooManyRequests(_) => "too many requests",
            Self::ServiceUnavailable(_) => "service unavailable",
            Self::Timeout => "request timed out",
            Self::Upstream(_) => "upstream service unavailable",
            Self::Internal(_) => "internal server error",
        }
    }

    fn private_cause(&self) -> Option<&str> {
        match self {
            Self::Upstream(cause) | Self::Internal(cause) => Some(cause),
            _ => None,
        }
    }

    fn retry_after_seconds(&self) -> Option<u64> {
        match self {
            Self::TooManyRequests(seconds) | Self::ServiceUnavailable(seconds) => Some(*seconds),
            _ => None,
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        let (status, code) = self.status_and_code();
        let request_id = self.private_cause().map(|cause| {
            let request_id = crate::request_context::current_or_new_id();
            let diagnostic = safe_cause_diagnostic(cause);
            match &self {
                Self::Upstream(_) => tracing::warn!(
                    target: "nowledge::error",
                    %request_id,
                    error_kind = code,
                    cause_category = diagnostic.category,
                    cause_fingerprint = %diagnostic.fingerprint,
                    "request failed"
                ),
                Self::Internal(_) => tracing::error!(
                    target: "nowledge::error",
                    %request_id,
                    error_kind = code,
                    cause_category = diagnostic.category,
                    cause_fingerprint = %diagnostic.fingerprint,
                    "request failed"
                ),
                _ => unreachable!("private causes are restricted to internal errors"),
            }
            request_id
        });
        let details = match (&self, request_id) {
            (Self::Validation { field, .. }, Some(request_id)) => json!({
                "status": status.as_u16(),
                "field": field,
                "request_id": request_id
            }),
            (Self::Validation { field, .. }, None) => json!({
                "status": status.as_u16(),
                "field": field
            }),
            (_, Some(request_id)) => json!({
                "status": status.as_u16(),
                "request_id": request_id
            }),
            (_, None) => json!({ "status": status.as_u16() }),
        };
        let retry_after_seconds = self.retry_after_seconds();
        let body = ErrorBody {
            error: ErrorInfo {
                code: code.to_string(),
                message: self.public_message().to_string(),
                details: Some(details),
            },
        };
        let mut response = (status, Json(body)).into_response();
        if let Some(seconds) = retry_after_seconds {
            if let Ok(value) = HeaderValue::from_str(&seconds.to_string()) {
                response.headers_mut().insert(RETRY_AFTER, value);
            }
        }
        response
    }
}

fn safe_cause_category(cause: &str) -> &'static str {
    let summary = cause
        .chars()
        .take(2_048)
        .collect::<String>()
        .to_ascii_lowercase();
    if summary.contains("timed out") || summary.contains("timeout") {
        "timeout"
    } else if summary.contains("rate limit") || summary.contains("status 429") {
        "rate_limited"
    } else if summary.contains("quota")
        || summary.contains("credit")
        || summary.contains("status 402")
    {
        "quota"
    } else if summary.contains("unauthorized")
        || summary.contains("authentication")
        || summary.contains("status 401")
        || summary.contains("status 403")
    {
        "authentication"
    } else if summary.contains("connect")
        || summary.contains("connection")
        || summary.contains("dns")
    {
        "connection"
    } else if summary.contains("parse")
        || summary.contains("decode")
        || summary.contains("encode")
        || summary.contains("json")
    {
        "invalid_data"
    } else if summary.contains("lock poisoned") {
        "state_lock"
    } else if summary.contains("invalid") || summary.contains("missing") {
        "invariant"
    } else {
        "unspecified"
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(value: anyhow::Error) -> Self {
        Self::Internal(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use axum::{body::to_bytes, http::header::RETRY_AFTER, response::IntoResponse};
    use serde_json::Value;

    use super::{safe_cause_diagnostic, safe_value_fingerprint, ApiError};

    #[test]
    fn safe_diagnostic_emits_only_category_and_keyed_fingerprint() {
        let private = "connection failed for /private/path with bearer secret-token";
        let diagnostic = safe_cause_diagnostic(private);

        assert_eq!(diagnostic.category, "connection");
        assert!(diagnostic.fingerprint.starts_with("hmac:"));
        assert_eq!(diagnostic.fingerprint.len(), "hmac:".len() + 16);
        assert!(!diagnostic.fingerprint.contains(private));
        assert!(!diagnostic.fingerprint.contains("secret-token"));
    }

    #[test]
    fn safe_fingerprints_are_stable_and_namespace_separated() {
        let first = safe_value_fingerprint("source_id", "private-id");
        let repeated = safe_value_fingerprint("source_id", "private-id");
        let other_namespace = safe_value_fingerprint("task_id", "private-id");

        assert_eq!(first, repeated);
        assert_ne!(first, other_namespace);
    }

    #[tokio::test]
    async fn validation_errors_include_the_field_in_the_stable_envelope() {
        let response =
            ApiError::validation("rows", "rows exceeds the configured maximum").into_response();
        assert_eq!(response.status(), axum::http::StatusCode::BAD_REQUEST);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "validation_error");
        assert_eq!(body["error"]["details"]["status"], 400);
        assert_eq!(body["error"]["details"]["field"], "rows");
    }

    #[test]
    fn pressure_errors_set_retry_after_without_changing_the_envelope_status() {
        for error in [
            ApiError::too_many_requests(17),
            ApiError::service_unavailable(3),
        ] {
            let response = error.into_response();
            assert!(matches!(response.status().as_u16(), 429 | 503));
            assert!(matches!(
                response
                    .headers()
                    .get(RETRY_AFTER)
                    .and_then(|value| value.to_str().ok()),
                Some("17" | "3")
            ));
        }
    }

    #[test]
    fn timeout_uses_gateway_timeout() {
        assert_eq!(
            ApiError::timeout().into_response().status(),
            axum::http::StatusCode::GATEWAY_TIMEOUT
        );
    }
}
