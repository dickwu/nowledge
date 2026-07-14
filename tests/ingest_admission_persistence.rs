use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    extract::{Request as AxumRequest, State},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use tokio::{sync::Notify, task::JoinHandle, time::sleep};
use tower::ServiceExt;

async fn call(app: Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = app.oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, body)
}

fn ingest_request(source_id: &str) -> Value {
    json!({
        "owner_user_id": "u1",
        "source_id": source_id,
        "revision_id": "v1",
        "content_type": "text/plain",
        "content": "repository admission must complete before the task is visible"
    })
}

fn assert_no_visible_ingest_task(usage: &Value) {
    assert_eq!(usage["providers"]["ingest"]["task_count"], 0, "{usage}");
    assert_eq!(usage["providers"]["ingest"]["queued"], 0, "{usage}");
}

async fn spawn_stub(app: Router) -> (String, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{addr}"), server)
}

async fn fail_repository_write(_request: AxumRequest) -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": "injected ingest-task persistence failure" })),
    )
        .into_response()
}

#[tokio::test]
async fn failed_repository_admission_does_not_publish_a_queued_task() {
    let (meili_url, server) = spawn_stub(Router::new().fallback(fail_repository_write)).await;
    let mut config = Config::test();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(meili_url);
    config.meili_wait_for_tasks = false;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    let (status, failure) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/tasks",
        ingest_request("failed-admission"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{failure}");
    assert_eq!(failure["error"]["code"], "upstream_error", "{failure}");

    let (status, usage) = call(app, Method::GET, "/v1/usage", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{usage}");
    assert_no_visible_ingest_task(&usage);

    state.shutdown().await;
    server.abort();
}

#[derive(Clone)]
struct HangingRepository {
    entered: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

async fn hang_repository_write(
    State(state): State<HangingRepository>,
    _request: AxumRequest,
) -> Response {
    state.entered.fetch_add(1, Ordering::SeqCst);
    state.release.notified().await;
    StatusCode::INTERNAL_SERVER_ERROR.into_response()
}

#[tokio::test]
async fn timed_out_repository_admissions_release_queue_capacity_without_phantom_tasks() {
    let repository = HangingRepository {
        entered: Arc::new(AtomicUsize::new(0)),
        release: Arc::new(Notify::new()),
    };
    let stub = Router::new()
        .fallback(hang_repository_write)
        .with_state(repository.clone());
    let (meili_url, server) = spawn_stub(stub).await;

    let mut config = Config::test();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(meili_url);
    config.meili_wait_for_tasks = false;
    config.request_timeout_ms = 75;
    config.sync_ingest_timeout_ms = 100;
    config.ingest_queue_capacity = 1;
    config.ingest_max_concurrent_tasks = 1;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());

    for attempt in 1..=2 {
        let (status, failure) = call(
            app.clone(),
            Method::POST,
            "/v1/ingest/tasks",
            ingest_request(&format!("timed-out-admission-{attempt}")),
        )
        .await;
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT, "{failure}");
        assert_eq!(failure["error"]["code"], "timeout", "{failure}");

        // The admission future has the same absolute deadline as the request
        // boundary. Give its cancellation path a scheduling turn before the
        // next request attempts to reserve the single queue slot.
        sleep(Duration::from_millis(10)).await;
    }

    assert_eq!(repository.entered.load(Ordering::SeqCst), 2);
    let (status, usage) = call(app, Method::GET, "/v1/usage", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{usage}");
    assert_no_visible_ingest_task(&usage);

    repository.release.notify_waiters();
    state.shutdown().await;
    server.abort();
}
