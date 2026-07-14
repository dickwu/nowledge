use axum::{body::to_bytes, http::StatusCode, response::IntoResponse};
use nowledge::error::ApiError;
use serde_json::{json, Value};

#[tokio::test]
async fn api_error_variants_keep_the_current_json_envelope() {
    let cases = [
        (
            ApiError::BadRequest("invalid input".to_string()),
            StatusCode::BAD_REQUEST,
            "bad_request",
            "invalid input",
        ),
        (
            ApiError::Unauthorized("missing token".to_string()),
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing token",
        ),
        (
            ApiError::Forbidden("wrong owner".to_string()),
            StatusCode::FORBIDDEN,
            "forbidden",
            "wrong owner",
        ),
        (
            ApiError::NotFound("missing record".to_string()),
            StatusCode::NOT_FOUND,
            "not_found",
            "missing record",
        ),
        (
            ApiError::Conflict("not ready".to_string()),
            StatusCode::CONFLICT,
            "conflict",
            "not ready",
        ),
        (
            ApiError::Upstream("provider failed".to_string()),
            StatusCode::BAD_GATEWAY,
            "upstream_error",
            "provider failed",
        ),
        (
            ApiError::Internal("unexpected failure".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "unexpected failure",
        ),
    ];

    for (error, expected_status, expected_code, expected_message) in cases {
        let response = error.into_response();
        assert_eq!(response.status(), expected_status);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let actual: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            actual,
            json!({
                "error": {
                    "code": expected_code,
                    "message": expected_message,
                    "details": { "status": expected_status.as_u16() }
                }
            })
        );
    }
}

#[test]
fn anyhow_conversion_currently_exposes_the_internal_message() {
    let error = ApiError::from(anyhow::anyhow!(
        "provider response referenced /private/runtime/auth.json"
    ));

    assert!(matches!(
        error,
        ApiError::Internal(ref message)
            if message == "provider response referenced /private/runtime/auth.json"
    ));
}
