use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    extract::{Multipart, State},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    routing::post,
    Json, Router,
};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use tokio::time::{sleep, timeout};
use tower::ServiceExt;

async fn raw_call(
    app: Router,
    method: Method,
    uri: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, Vec<u8>) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, content_type)
        .body(Body::from(body))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap()
        .to_vec();
    (status, body)
}

async fn json_call(app: Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let (status, bytes) = raw_call(
        app,
        method,
        uri,
        "application/json",
        body.to_string().into_bytes(),
    )
    .await;
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, body)
}

fn multipart_body(file_bytes: &[u8]) -> (String, Vec<u8>) {
    let boundary = format!("nowledge-boundary-{}", uuid::Uuid::now_v7());
    let mut body = Vec::with_capacity(file_bytes.len() + 512);
    for (name, value) in [
        ("owner_user_id", "u1"),
        ("source_id", "implicit-multipart-limit"),
        ("title", "Implicit multipart limit"),
    ] {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"large.txt\"\r\nContent-Type: text/plain\r\n\r\n",
    );
    body.extend_from_slice(file_bytes);
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    (format!("multipart/form-data; boundary={boundary}"), body)
}

#[tokio::test]
async fn current_axum_json_limit_returns_a_non_envelope_413() {
    let oversized_query = "x".repeat(2 * 1024 * 1024);
    let (status, body) = raw_call(
        build_router(AppState::new(Arc::new(Config::test()))),
        Method::POST,
        "/v1/context/search",
        "application/json",
        json!({ "query": oversized_query }).to_string().into_bytes(),
    )
    .await;

    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert!(serde_json::from_slice::<Value>(&body).is_err());
    assert!(!String::from_utf8_lossy(&body).is_empty());
}

#[tokio::test]
async fn current_axum_multipart_limit_is_mapped_to_a_bad_request_envelope() {
    let (content_type, body) = multipart_body(&vec![b'x'; 2 * 1024 * 1024]);
    let (status, body) = raw_call(
        build_router(AppState::new(Arc::new(Config::test()))),
        Method::POST,
        "/v1/ingest/uploads:sync",
        &content_type,
        body,
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"]["code"], "bad_request");
    assert_eq!(body["error"]["details"], json!({ "status": 400 }));
    assert_eq!(
        body["error"]["message"],
        "invalid multipart body: Error parsing `multipart/form-data` request"
    );
}

#[tokio::test]
async fn current_disabled_worker_accepts_a_task_that_remains_queued() {
    let mut config = Config::test();
    config.ingest_worker_enabled = false;
    let app = build_router(AppState::new(Arc::new(config)));

    let (status, task) = json_call(
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
    assert_eq!(status, StatusCode::OK, "{task}");
    assert_eq!(task["state"], "queued");

    sleep(Duration::from_millis(50)).await;
    let task_id = task["task_id"].as_str().unwrap();
    let (status, still_queued) = json_call(
        app,
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}?owner_user_id=u1"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{still_queued}");
    assert_eq!(still_queued["state"], "queued");
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
async fn current_full_ingest_queue_blocks_the_next_request_instead_of_rejecting_it() {
    let (parser_url, parser) = spawn_blocking_parser().await;
    let mut config = Config::test();
    config.parser_provider = "mineru".to_string();
    config.mineru_api_url = parser_url;
    config.ingest_max_concurrent_tasks = 1;
    let app = build_router(AppState::new(Arc::new(config)));

    let (status, first) = json_call(
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

    for sequence in 2..=10 {
        let (status, task) = json_call(
            app.clone(),
            Method::POST,
            "/v1/ingest/tasks",
            queued_ingest_body(sequence),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "task {sequence}: {task}");
    }

    let blocked = timeout(
        Duration::from_millis(150),
        json_call(
            app,
            Method::POST,
            "/v1/ingest/tasks",
            queued_ingest_body(11),
        ),
    )
    .await;
    assert!(
        blocked.is_err(),
        "the full queue unexpectedly rejected or accepted immediately"
    );

    parser.released.store(true, Ordering::SeqCst);
}
