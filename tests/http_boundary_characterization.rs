use std::{
    collections::HashSet,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    extract::{Multipart, State},
    http::{
        header::{
            ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_REQUEST_METHOD, AUTHORIZATION,
            CONTENT_TYPE, ORIGIN, RETRY_AFTER,
        },
        HeaderMap, Method, Request, StatusCode,
    },
    response::Response,
    routing::post,
    Json, Router,
};
use nowledge::config::{AuthUserConfig, AuthUserScope};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::{sleep, timeout};
use tower::ServiceExt;

static MULTIPART_TEMP_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn raw_response(
    app: Router,
    method: Method,
    uri: &str,
    content_type: &str,
    body: Vec<u8>,
) -> Response {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap();
    app.oneshot(request).await.unwrap()
}

async fn raw_call(
    app: Router,
    method: Method,
    uri: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let response = raw_response(app, method, uri, content_type, body).await;
    let status = response.status();
    let headers = response.headers().clone();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, headers, body)
}

async fn json_call(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
) -> (StatusCode, HeaderMap, Value) {
    json_call_with_token(app, method, uri, body, None).await
}

async fn json_call_with_token(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
    token: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let mut request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        request = request.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    let response = app
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, headers, body)
}

fn assert_error(body: &Value, status: StatusCode, code: &str) {
    assert_eq!(body["error"]["code"], code, "{body}");
    assert_eq!(body["error"]["details"]["status"], status.as_u16());
}

fn multipart_body(file_bytes: &[u8]) -> (String, Vec<u8>) {
    multipart_body_with_parts(
        &[
            ("owner_user_id", "u1"),
            ("source_id", "implicit-multipart-limit"),
            ("title", "Implicit multipart limit"),
        ],
        &[("file", "large.txt", "text/plain", file_bytes)],
    )
}

fn multipart_body_with_parts(
    fields: &[(&str, &str)],
    files: &[(&str, &str, &str, &[u8])],
) -> (String, Vec<u8>) {
    let boundary = format!("nowledge-boundary-{}", uuid::Uuid::now_v7());
    let estimated_file_bytes: usize = files.iter().map(|(_, _, _, bytes)| bytes.len()).sum();
    let mut body = Vec::with_capacity(estimated_file_bytes + 1_024);
    for (name, value) in fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    for (name, file_name, content_type, bytes) in files {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{name}\"; filename=\"{file_name}\"\r\nContent-Type: {content_type}\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

fn multipart_body_with_file_and_trailing_fields(
    leading_fields: &[(&str, &str)],
    file_name: &str,
    file_content_type: Option<&str>,
    file_bytes: &[u8],
    trailing_fields: &[(&str, &str)],
) -> (String, Vec<u8>) {
    let boundary = format!("nowledge-boundary-{}", uuid::Uuid::now_v7());
    let mut body = Vec::with_capacity(file_bytes.len() + 1_024);
    for (name, value) in leading_fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }

    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        format!("Content-Disposition: form-data; name=\"file\"; filename=\"{file_name}\"\r\n")
            .as_bytes(),
    );
    if let Some(content_type) = file_content_type {
        body.extend_from_slice(format!("Content-Type: {content_type}\r\n").as_bytes());
    }
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(file_bytes);
    body.extend_from_slice(b"\r\n");

    for (name, value) in trailing_fields {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

fn staged_upload_paths() -> HashSet<PathBuf> {
    std::fs::read_dir(std::env::temp_dir())
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("nowledge-upload-"))
                .then(|| entry.path())
        })
        .collect()
}

fn assert_no_new_staged_uploads(before: &HashSet<PathBuf>) {
    let after = staged_upload_paths();
    let leaked = after.difference(before).collect::<Vec<_>>();
    assert!(leaked.is_empty(), "staged upload files leaked: {leaked:?}");
}

async fn assert_no_ingest_tasks(state: &AppState) {
    let (status, _, usage) = json_call(
        build_router(state.clone()),
        Method::GET,
        "/v1/usage",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["task_count"], 0, "{usage}");
}

#[tokio::test]
async fn oversized_json_returns_the_stable_413_envelope() {
    let mut config = Config::test();
    config.max_json_bytes = 1_024;
    let oversized_query = "x".repeat(1_025);
    let (status, headers, body) = raw_call(
        build_router(AppState::new(Arc::new(config))),
        Method::POST,
        "/v1/context/search",
        "application/json",
        json!({ "query": oversized_query }).to_string().into_bytes(),
    )
    .await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large");
    assert!(headers.contains_key("x-request-id"));
}

#[tokio::test]
async fn oversized_multipart_upload_returns_the_stable_413_envelope() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let mut config = Config::test();
    config.max_upload_bytes = 1_024;
    let (content_type, body) = multipart_body(&vec![b'x'; 1_025]);
    let (status, headers, body) = raw_call(
        build_router(AppState::new(Arc::new(config))),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large");
    assert!(headers.contains_key("x-request-id"));
}

#[tokio::test]
async fn exact_limit_upload_verifies_checksum_sanitizes_filename_and_cleans_temp_file() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let payload = b"exact-limit-body";
    let checksum = hex::encode(Sha256::digest(payload));
    let mut config = Config::test();
    config.max_upload_bytes = payload.len();
    let state = AppState::new(Arc::new(config));
    let (content_type, body) = multipart_body_with_parts(
        &[("owner_user_id", "u1"), ("checksum", &checksum)],
        &[("file", "../../exact.txt", "text/plain", payload)],
    );
    let (status, _, body) = raw_call(
        build_router(state.clone()),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["task"]["state"], "completed", "{body}");
    assert_eq!(body["source_id"], "ingest:exact-txt", "{body}");
    let source_id = body["source_id"].as_str().unwrap();
    assert!(!source_id.contains(['/', '\\']));
    assert!(!source_id.contains(".."));

    state.shutdown().await;
    assert_no_new_staged_uploads(&before);
}

#[tokio::test]
async fn checksum_mismatch_rejects_before_task_creation_and_cleans_temp_file() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let state = AppState::new(Arc::new(Config::test()));
    let checksum = "0".repeat(64);
    let (content_type, body) = multipart_body_with_parts(
        &[("owner_user_id", "u1"), ("checksum", &checksum)],
        &[("file", "checksum.txt", "text/plain", b"checksum mismatch")],
    );
    let (status, _, body) = raw_call(
        build_router(state.clone()),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "checksum");

    let (status, _, usage) = json_call(
        build_router(state.clone()),
        Method::GET,
        "/v1/usage",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["task_count"], 0);

    state.shutdown().await;
    assert_no_new_staged_uploads(&before);
}

#[tokio::test]
async fn staged_file_with_alternate_content_is_rejected_without_task_or_temp_leak() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let state = AppState::new(Arc::new(Config::test()));
    let (content_type, body) = multipart_body_with_file_and_trailing_fields(
        &[("owner_user_id", "u1")],
        "alternate.txt",
        Some("text/plain"),
        b"staged file content",
        &[("content", "alternate inline content")],
    );
    let (status, _, body) = raw_call(
        build_router(state.clone()),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "multipart", "{body}");
    assert_no_ingest_tasks(&state).await;

    state.shutdown().await;
    assert_no_new_staged_uploads(&before);
}

#[tokio::test]
async fn upload_without_file_part_content_type_is_rejected() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let state = AppState::new(Arc::new(Config::test()));
    let (content_type, body) = multipart_body_with_file_and_trailing_fields(
        &[("owner_user_id", "u1")],
        "missing-mime.txt",
        None,
        b"missing MIME declaration",
        &[],
    );
    let (status, _, body) = raw_call(
        build_router(state.clone()),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "content_type", "{body}");
    assert_no_ingest_tasks(&state).await;

    state.shutdown().await;
    assert_no_new_staged_uploads(&before);
}

#[tokio::test]
async fn trailing_metadata_content_type_must_match_staged_file_part() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let state = AppState::new(Arc::new(Config::test()));
    let (content_type, body) = multipart_body_with_file_and_trailing_fields(
        &[("owner_user_id", "u1")],
        "mismatch.txt",
        Some("text/plain"),
        b"staged before mismatched metadata",
        &[("content_type", "application/pdf")],
    );
    let (status, _, body) = raw_call(
        build_router(state.clone()),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "content_type", "{body}");
    assert_no_ingest_tasks(&state).await;

    state.shutdown().await;
    assert_no_new_staged_uploads(&before);
}

#[tokio::test]
async fn syntactically_valid_but_disallowed_upload_mime_is_rejected() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let mut config = Config::test();
    config.upload_allowed_mime_types = vec!["text/plain".to_string()];
    let state = AppState::new(Arc::new(config));
    let (content_type, body) = multipart_body_with_file_and_trailing_fields(
        &[("owner_user_id", "u1")],
        "disallowed.bin",
        Some("application/x-nowledge-adversarial"),
        b"valid MIME syntax outside policy",
        &[],
    );
    let (status, _, body) = raw_call(
        build_router(state.clone()),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "content_type", "{body}");
    assert_no_ingest_tasks(&state).await;

    state.shutdown().await;
    assert_no_new_staged_uploads(&before);
}

#[tokio::test]
async fn multipart_shape_and_metadata_limits_fail_with_stable_errors() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();

    let cases = [
        (
            Config::test(),
            multipart_body_with_parts(
                &[],
                &[
                    ("file", "first.txt", "text/plain", b"first"),
                    ("file", "second.txt", "text/plain", b"second"),
                ],
            ),
            StatusCode::BAD_REQUEST,
            "validation_error",
            Some("file"),
        ),
        (
            {
                let mut config = Config::test();
                config.max_multipart_fields = 1;
                config
            },
            multipart_body_with_parts(
                &[("owner_user_id", "u1")],
                &[("file", "fields.txt", "text/plain", b"bounded")],
            ),
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            None,
        ),
        (
            {
                let mut config = Config::test();
                config.max_json_bytes = 4;
                config
            },
            multipart_body_with_parts(
                &[("title", "12345")],
                &[("file", "metadata.txt", "text/plain", b"bounded")],
            ),
            StatusCode::PAYLOAD_TOO_LARGE,
            "payload_too_large",
            None,
        ),
        (
            Config::test(),
            multipart_body_with_parts(
                &[("content_type", "not a mime")],
                &[("file", "mime.txt", "text/plain", b"bounded")],
            ),
            StatusCode::BAD_REQUEST,
            "validation_error",
            Some("content_type"),
        ),
    ];

    for (config, (content_type, request_body), expected_status, code, field) in cases {
        let state = AppState::new(Arc::new(config));
        let (status, headers, body) = raw_call(
            build_router(state.clone()),
            Method::POST,
            "/v1/ingest/uploads:sync",
            &content_type,
            request_body,
        )
        .await;
        assert_eq!(
            status,
            expected_status,
            "{}",
            String::from_utf8_lossy(&body)
        );
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_error(&body, expected_status, code);
        if let Some(field) = field {
            assert_eq!(body["error"]["details"]["field"], field);
        }
        assert!(headers.contains_key("x-request-id"));
        state.shutdown().await;
    }

    assert_no_new_staged_uploads(&before);
}

#[tokio::test]
async fn disabled_worker_rejects_async_ingest_before_creating_a_task() {
    let mut config = Config::test();
    config.ingest_worker_enabled = false;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    let (status, headers, body) = json_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/tasks",
        json!({
            "owner_user_id": "u1",
            "source_id": "disabled-worker-characterization",
            "content": "no worker can claim this task"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_error(
        &body,
        StatusCode::SERVICE_UNAVAILABLE,
        "service_unavailable",
    );
    assert_eq!(headers.get(RETRY_AFTER).unwrap(), "1");
    state.shutdown().await;
}

#[derive(Clone)]
struct BlockingParserState {
    entered: Arc<AtomicUsize>,
    released: Arc<AtomicBool>,
}

async fn blocking_parse(
    State(state): State<BlockingParserState>,
    mut multipart: Multipart,
) -> Json<Value> {
    while let Some(field) = multipart.next_field().await.unwrap() {
        let _ = field.bytes().await.unwrap();
    }
    state.entered.fetch_add(1, Ordering::SeqCst);
    while !state.released.load(Ordering::SeqCst) {
        sleep(Duration::from_millis(5)).await;
    }
    Json(json!({
        "markdown": "blocking parser payload",
        "content_list_v2": [{
            "type": "paragraph",
            "text": "blocking parser payload",
            "page_idx": 0,
            "bbox": [0, 0, 1, 1],
            "reading_order": 0
        }],
        "parser_version": "blocking-test"
    }))
}

async fn spawn_blocking_parser() -> (String, BlockingParserState) {
    let state = BlockingParserState {
        entered: Arc::new(AtomicUsize::new(0)),
        released: Arc::new(AtomicBool::new(false)),
    };
    let app = Router::new()
        .route("/file_parse", post(blocking_parse))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

fn queued_ingest_body(sequence: usize) -> Value {
    json!({
        "owner_user_id": "u1",
        "source_id": format!("queue-pressure-{sequence}"),
        "revision_id": "v1",
        "content_type": "text/plain",
        "content": format!("queue pressure payload {sequence}")
    })
}

#[tokio::test]
async fn full_ingest_queue_rejects_immediately_without_a_phantom_task() {
    let (parser_url, parser) = spawn_blocking_parser().await;
    let mut config = Config::test();
    config.parser_provider = "mineru".to_string();
    config.mineru_api_url = parser_url;
    config.ingest_max_concurrent_tasks = 1;
    config.ingest_queue_capacity = 2;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    let (status, _, first) = json_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/tasks",
        queued_ingest_body(1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");

    timeout(Duration::from_secs(2), async {
        while parser.entered.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("first parser request did not start");

    for sequence in 2..=3 {
        let (status, _, task) = json_call(
            app.clone(),
            Method::POST,
            "/v1/ingest/tasks",
            queued_ingest_body(sequence),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "task {sequence}: {task}");
    }

    let rejected = timeout(
        Duration::from_millis(150),
        json_call(
            app.clone(),
            Method::POST,
            "/v1/ingest/tasks",
            queued_ingest_body(4),
        ),
    )
    .await
    .expect("queue pressure response blocked");
    assert_eq!(rejected.0, StatusCode::TOO_MANY_REQUESTS, "{}", rejected.2);
    assert_error(
        &rejected.2,
        StatusCode::TOO_MANY_REQUESTS,
        "too_many_requests",
    );
    assert_eq!(rejected.1.get(RETRY_AFTER).unwrap(), "1");
    let (status, _, usage) = json_call(app, Method::GET, "/v1/usage", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["task_count"], 3, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["queued"], 2, "{usage}");

    parser.released.store(true, Ordering::SeqCst);
    state.shutdown().await;
}

#[tokio::test]
async fn cors_allows_only_configured_origins() {
    let mut config = Config::test();
    config.cors_allowed_origins = vec!["https://allowed.example".to_string()];
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    for (origin, expected) in [
        ("https://allowed.example", Some("https://allowed.example")),
        ("https://denied.example", None),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/v1/context/search")
                    .header(ORIGIN, origin)
                    .header(ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(response.status().is_success());
        assert_eq!(
            response
                .headers()
                .get(ACCESS_CONTROL_ALLOW_ORIGIN)
                .and_then(|value| value.to_str().ok()),
            expected
        );
    }

    state.shutdown().await;
}

#[tokio::test]
async fn bulk_tag_and_search_limits_reject_before_mutation() {
    let mut config = Config::test();
    config.max_bulk_events = 1;
    config.max_bulk_rows = 1;
    config.max_tags_per_item = 1;
    config.max_search_limit = 1;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    let (status, _, body) = json_call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events:bulk",
        json!({ "events": [{ "text": "one" }, { "text": "two" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "events");

    let (status, _, body) = json_call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        json!({ "text": "tag pressure", "tags": ["one", "two"] }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");

    let (status, _, body) = json_call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "bounded", "limit": 2 }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "limit");

    let (status, _, snapshot) = json_call(
        app.clone(),
        Method::POST,
        "/v1/history/structured/snapshots",
        json!({
            "dataset_key": "bounded-rows",
            "owner_user_id": "u1",
            "period_key": "2026-W28",
            "period_start": "2026-07-06T00:00:00Z",
            "period_end": "2026-07-12T23:59:59Z",
            "granularity": "weekly",
            "source_ref": { "kind": "test", "id": "bounded" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{snapshot}");
    let snapshot_id = snapshot["snapshot"]["id"].as_str().unwrap();
    let (status, _, body) = json_call(
        app.clone(),
        Method::POST,
        &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
        json!({ "rows": [{ "id": "one" }, { "id": "two" }] }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_error(&body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], "rows");

    state.shutdown().await;
}

#[tokio::test]
async fn rate_limits_are_shared_by_logical_owner_not_raw_credential() {
    let mut config = Config::test();
    config.allow_unsafe_unauthenticated = false;
    config.rate_limit_requests_per_minute = 2;
    config.auth_users = vec![
        AuthUserConfig {
            token: "u1-token-alpha".to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            roles: vec!["user".to_string()],
        },
        AuthUserConfig {
            token: "u1-token-bravo".to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            roles: vec!["user".to_string()],
        },
        AuthUserConfig {
            token: "u2-token-alpha".to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u2".to_string(),
            },
            roles: vec!["user".to_string()],
        },
    ];
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    for token in ["u1-token-alpha", "u1-token-bravo"] {
        let (status, _, body) = json_call_with_token(
            app.clone(),
            Method::POST,
            "/v1/state/search",
            json!({}),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    let (status, headers, body) = json_call_with_token(
        app.clone(),
        Method::POST,
        "/v1/state/search",
        json!({}),
        Some("u1-token-alpha"),
    )
    .await;
    assert_eq!(status, StatusCode::TOO_MANY_REQUESTS, "{body}");
    assert_error(&body, StatusCode::TOO_MANY_REQUESTS, "too_many_requests");
    assert!(headers.contains_key(RETRY_AFTER));

    let (status, _, body) = json_call_with_token(
        app,
        Method::POST,
        "/v1/state/search",
        json!({}),
        Some("u2-token-alpha"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    state.shutdown().await;
}

#[tokio::test]
async fn global_capacity_load_sheds_but_liveness_remains_available() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let (parser_url, parser) = spawn_blocking_parser().await;
    let mut config = Config::test();
    config.parser_provider = "mineru".to_string();
    config.mineru_api_url = parser_url;
    config.max_in_flight_requests = 1;
    config.request_timeout_ms = 5_000;
    config.sync_ingest_timeout_ms = 5_000;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());
    let (content_type, body) = multipart_body(b"bounded parser input");
    let first_app = app.clone();
    let first = tokio::spawn(async move {
        raw_call(
            first_app,
            Method::POST,
            "/v1/ingest/uploads:sync",
            &content_type,
            body,
        )
        .await
    });

    timeout(Duration::from_secs(2), async {
        while parser.entered.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("parser request did not start");

    let (status, headers, body) =
        json_call(app.clone(), Method::POST, "/v1/state/search", json!({})).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_error(
        &body,
        StatusCode::SERVICE_UNAVAILABLE,
        "service_unavailable",
    );
    assert_eq!(headers.get(RETRY_AFTER).unwrap(), "1");

    let response = raw_response(app, Method::GET, "/livez", "text/plain", Vec::new()).await;
    assert_eq!(response.status(), StatusCode::OK);

    parser.released.store(true, Ordering::SeqCst);
    let first = first.await.unwrap();
    assert_eq!(
        first.0,
        StatusCode::OK,
        "{}",
        String::from_utf8_lossy(&first.2)
    );
    state.shutdown().await;
}

#[tokio::test]
async fn sync_ingest_timeout_uses_stable_504_and_releases_capacity() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let (parser_url, parser) = spawn_blocking_parser().await;
    let mut config = Config::test();
    config.parser_provider = "mineru".to_string();
    config.mineru_api_url = parser_url;
    config.max_in_flight_requests = 1;
    config.request_timeout_ms = 20;
    config.sync_ingest_timeout_ms = 50;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());
    let (content_type, body) = multipart_body(b"timeout parser input");
    let (status, headers, body) = raw_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;
    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_error(&body, StatusCode::GATEWAY_TIMEOUT, "timeout");
    assert!(headers.contains_key("x-request-id"));

    let (status, _, body) = json_call(app, Method::POST, "/v1/state/search", json!({})).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let usage = timeout(Duration::from_secs(1), async {
        loop {
            let (status, _, usage) = json_call(
                build_router(state.clone()),
                Method::GET,
                "/v1/usage",
                Value::Null,
            )
            .await;
            assert_eq!(status, StatusCode::OK, "{usage}");
            if usage["providers"]["ingest"]["failed"] == 1 {
                break usage;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed-out sync ingest did not reach a terminal task state");
    assert_eq!(usage["providers"]["ingest"]["task_count"], 1, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["failed"], 1, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["queued"], 0, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["parsing"], 0, "{usage}");
    timeout(Duration::from_secs(1), async {
        while staged_upload_paths() != before {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed-out sync upload did not release its staged file");
    assert_no_new_staged_uploads(&before);
    parser.released.store(true, Ordering::SeqCst);
    state.shutdown().await;
}

#[tokio::test]
async fn shutdown_rejects_new_jobs_and_interrupts_running_ingest() {
    let (parser_url, parser) = spawn_blocking_parser().await;
    let mut config = Config::test();
    config.parser_provider = "mineru".to_string();
    config.mineru_api_url = parser_url;
    config.ingest_max_concurrent_tasks = 1;
    config.shutdown_timeout_ms = 500;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    let (status, _, task) = json_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/tasks",
        queued_ingest_body(1),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{task}");
    let task_id = task["task_id"].as_str().unwrap().to_string();
    timeout(Duration::from_secs(2), async {
        while parser.entered.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("parser request did not start");

    state.begin_shutdown();
    let (status, _, body) = json_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/tasks",
        queued_ingest_body(2),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    state.shutdown().await;

    let (status, _, task) = json_call(
        app,
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}?owner_user_id=u1"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{task}");
    assert_eq!(task["state"], "failed");
    assert_eq!(task["error"], "ingest_interrupted");
    parser.released.store(true, Ordering::SeqCst);
}
