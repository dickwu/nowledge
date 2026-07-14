use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
    middleware,
    response::IntoResponse,
    routing::get,
    Router,
};
use nowledge::{
    error::ApiError,
    request_context::{assign, RequestContextState, X_REQUEST_ID},
    Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

static ERROR_LOG_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Default)]
struct SharedLog(Arc<Mutex<Vec<u8>>>);

impl SharedLog {
    fn contents(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

struct SharedLogWriter(SharedLog);

impl Write for SharedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 .0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for SharedLog {
    type Writer = SharedLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedLogWriter(self.clone())
    }
}

#[tokio::test]
async fn api_error_variants_keep_the_current_json_envelope() {
    let _guard = ERROR_LOG_TEST_LOCK.lock().await;
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
            "upstream service unavailable",
        ),
        (
            ApiError::Internal("unexpected failure".to_string()),
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal_error",
            "internal server error",
        ),
    ];

    for (error, expected_status, expected_code, expected_message) in cases {
        let response = error.into_response();
        assert_eq!(response.status(), expected_status);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let actual: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(actual["error"]["code"], expected_code);
        assert_eq!(actual["error"]["message"], expected_message);
        assert_eq!(
            actual["error"]["details"]["status"],
            expected_status.as_u16()
        );
        if matches!(
            expected_status,
            StatusCode::BAD_GATEWAY | StatusCode::INTERNAL_SERVER_ERROR
        ) {
            let request_id = actual["error"]["details"]["request_id"]
                .as_str()
                .expect("500/502 responses carry a request correlation ID");
            assert!(uuid::Uuid::parse_str(request_id).is_ok(), "{request_id}");
        } else {
            assert!(actual["error"]["details"].get("request_id").is_none());
        }
    }
}

#[tokio::test]
async fn anyhow_internal_cause_is_not_serialized() {
    let _guard = ERROR_LOG_TEST_LOCK.lock().await;
    let private_message = "provider response referenced /private/runtime/auth.json";
    let error = ApiError::from(anyhow::anyhow!("{private_message}"));
    let response = error.into_response();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let actual: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(actual["error"]["code"], json!("internal_error"));
    assert_eq!(actual["error"]["message"], json!("internal server error"));
    assert_eq!(actual["error"]["details"]["status"], json!(500));
    let request_id = actual["error"]["details"]["request_id"]
        .as_str()
        .expect("internal errors carry a request correlation ID");
    assert!(uuid::Uuid::parse_str(request_id).is_ok(), "{request_id}");
    assert!(!actual.to_string().contains(private_message));
}

#[tokio::test]
async fn private_error_correlation_id_matches_the_response_header() {
    let _guard = ERROR_LOG_TEST_LOCK.lock().await;
    let mut config = Config::test();
    config.admin_token = Some("private-operator-token".to_string());
    let request_context = RequestContextState::from_config(&config);
    let app = Router::new()
        .route(
            "/internal",
            get(|| async {
                Err::<(), _>(ApiError::Internal(
                    "upstream echoed private-operator-token".to_string(),
                ))
            }),
        )
        .layer(middleware::from_fn_with_state(request_context, assign));

    let response = app
        .oneshot(
            Request::builder()
                .uri("/internal")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let header_request_id = response
        .headers()
        .get(X_REQUEST_ID)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let actual: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(actual["error"]["details"]["request_id"], header_request_id);
    assert!(!actual.to_string().contains("private-operator-token"));
}

#[tokio::test]
async fn private_error_logs_only_allowlisted_diagnostics() {
    let _guard = ERROR_LOG_TEST_LOCK.lock().await;
    let logs = SharedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::TRACE)
        .with_writer(logs.clone())
        .finish();
    let private_cause = concat!(
        "status 401 from /private/runtime/auth.json; provider body=",
        "private prompt excerpt; Bearer private-operator-token"
    );

    tracing::subscriber::with_default(subscriber, || {
        let _response = ApiError::Internal(private_cause.to_string()).into_response();
    });

    let rendered = logs.contents();
    for field in [
        "request_id",
        "error_kind",
        "cause_category",
        "cause_fingerprint",
    ] {
        assert!(rendered.contains(field), "missing {field}: {rendered}");
    }
    assert!(rendered.contains("authentication"), "{rendered}");
    for private in [
        "/private/runtime/auth.json",
        "private prompt excerpt",
        "private-operator-token",
        "provider body",
    ] {
        assert!(!rendered.contains(private), "leaked {private}: {rendered}");
    }
}
use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
};
