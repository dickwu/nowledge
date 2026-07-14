use std::{
    future::Future,
    sync::{Arc, Mutex},
};

use axum::{
    body::{to_bytes, Body},
    extract::{Request as AxumRequest, State},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use nowledge::{
    build_router,
    meili::MeiliAdmin,
    repository::{KnowledgeRepository, MeiliRepository},
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn spawn_meili_stub(
    handler: fn(AxumRequest) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>>,
) -> String {
    let app = Router::new().fallback(handler);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

fn eventual_meili(
    request: AxumRequest,
) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>> {
    Box::pin(async move {
        let path = request.uri().path();
        let method = request.method();
        if method == Method::GET && path.starts_with("/indexes/") {
            return (StatusCode::OK, Json(json!({ "uid": "stub-index" }))).into_response();
        }
        if method == Method::POST && path.ends_with("/search") {
            return (
                StatusCode::OK,
                Json(json!({ "hits": [], "processingTimeMs": 0 })),
            )
                .into_response();
        }
        if (method == Method::POST && path.ends_with("/documents"))
            || (method == Method::PATCH && path.ends_with("/settings"))
        {
            return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
        }
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("unexpected stub request: {method} {path}") })),
        )
            .into_response()
    })
}

fn failing_meili(
    _request: AxumRequest,
) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>> {
    Box::pin(async {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": "injected persistence failure" })),
        )
            .into_response()
    })
}

#[derive(Clone, Default)]
struct ResetRecorder {
    requests: Arc<Mutex<Vec<String>>>,
}

async fn reset_meili_stub(State(recorder): State<ResetRecorder>, request: AxumRequest) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::DELETE && path.starts_with("/indexes/") {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 10 }))).into_response();
    }
    if method == Method::GET && path.starts_with("/indexes/") {
        return (StatusCode::NOT_FOUND, Json(json!({}))).into_response();
    }
    if method == Method::POST && path == "/indexes" {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 11 }))).into_response();
    }
    if method == Method::PATCH && path.ends_with("/settings") {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 12 }))).into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected reset request: {method} {path}") })),
    )
        .into_response()
}

#[derive(Clone)]
struct SourceRevisionSearchRecorder {
    requests: Arc<Mutex<Vec<Value>>>,
    corpus_size: usize,
}

async fn source_revision_search_stub(
    State(recorder): State<SourceRevisionSearchRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    if method != Method::POST || path != "/indexes/rag_source_revisions/search" {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({
                "message": format!("unexpected source revision request: {method} {path}")
            })),
        )
            .into_response();
    }

    let body = to_bytes(request.into_body(), usize::MAX).await.unwrap();
    let search: Value = serde_json::from_slice(&body).unwrap();
    recorder.requests.lock().unwrap().push(search.clone());

    let limit = search["limit"].as_u64().unwrap() as usize;
    let hits = (0..recorder.corpus_size.min(limit))
        .map(|index| {
            json!({
                "id": format!("revision-{index}"),
                "source_id": "source-with-many-revisions",
                "title": format!("Revision {index}"),
                "source_uri": "ctx://company/sources/many-revisions",
                "checksum": format!("checksum-{index}"),
                "content": format!("content {index}"),
                "status": "historical",
                "created_at": "2026-07-13T00:00:00Z"
            })
        })
        .collect::<Vec<_>>();

    (
        StatusCode::OK,
        Json(json!({
            "hits": hits,
            "estimatedTotalHits": recorder.corpus_size,
            "processingTimeMs": 0
        })),
    )
        .into_response()
}

fn meili_backed_app(url: String) -> Router {
    let mut config = Config::test();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.meili_wait_for_tasks = false;
    build_router(AppState::new(Arc::new(config)))
}

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

#[tokio::test]
async fn current_eventual_meili_read_can_hide_an_accepted_local_write() {
    let url = spawn_meili_stub(eventual_meili).await;
    let app = meili_backed_app(url);

    let (status, event) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "read-after-write-gap",
            "owner_user_id": "u1",
            "occurred_at": "2026-07-13T00:00:00Z",
            "observed_at": "2026-07-13T00:00:01Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "read-after-write-gap" },
            "text": "accepted locally but not visible in repository search",
            "privacy": "private"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");
    assert_eq!(event["meili_task_uid"], "1");

    let (status, search) = call(
        app,
        Method::POST,
        "/v1/history/users/u1/search",
        json!({ "query": "accepted locally", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"], json!([]), "{search}");
}

#[tokio::test]
async fn current_persistence_failure_leaves_the_failed_state_write_visible_in_memory() {
    let url = spawn_meili_stub(failing_meili).await;
    let app = meili_backed_app(url);

    let (status, failed) = call(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/persistence-gap",
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "title": "Persistence gap",
            "statement": "this mutation remains visible after the request fails",
            "source_refs": [{ "kind": "test", "id": "failure-injection" }]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{failed}");
    assert_eq!(failed["error"]["code"], "upstream_error");
    assert_eq!(failed["error"]["details"]["status"], 502);
    let request_id = failed["error"]["details"]["request_id"]
        .as_str()
        .expect("upstream errors include a request correlation ID");
    assert!(uuid::Uuid::parse_str(request_id).is_ok(), "{request_id}");
    assert_eq!(failed["error"]["message"], "upstream service unavailable");
    assert!(!failed.to_string().contains("rag_state_items"), "{failed}");

    let (status, visible) = call(
        app,
        Method::GET,
        "/v1/state/profile/facts/persistence-gap?owner_user_id=u1",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{visible}");
    assert_eq!(
        visible["item"]["statement"],
        "this mutation remains visible after the request fails"
    );
}

#[tokio::test]
async fn current_bootstrap_reset_recreates_indexes_without_waiting_for_delete_tasks() {
    let recorder = ResetRecorder::default();
    let app = Router::new()
        .fallback(reset_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.meili_url = Some(format!("http://{addr}"));
    config.meili_wait_for_tasks = false;
    let result = MeiliAdmin::from_config(&config)
        .bootstrap(true)
        .await
        .expect("reset bootstrap should be accepted by the stub");
    assert!(!result.tasks.is_empty());

    let requests = recorder.requests.lock().unwrap().clone();
    assert_eq!(requests[0], "DELETE /indexes/rag_company_context");
    assert_eq!(requests[1], "GET /indexes/rag_company_context");
    assert_eq!(requests[2], "POST /indexes");
    assert_eq!(requests[3], "PATCH /indexes/rag_company_context/settings");
    assert!(
        requests.iter().all(|request| !request.contains("/tasks/")),
        "reset unexpectedly waited for a Meilisearch task: {requests:?}"
    );
}

#[tokio::test]
async fn current_source_revision_rehydration_requests_at_most_two_thousand_rows() {
    let recorder = SourceRevisionSearchRecorder {
        requests: Arc::new(Mutex::new(Vec::new())),
        corpus_size: 2001,
    };
    let app = Router::new()
        .fallback(source_revision_search_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.meili_url = Some(format!("http://{addr}"));
    let repository = MeiliRepository::new(MeiliAdmin::from_config(&config), false);

    let revisions = repository
        .list_source_revisions("tenant-a")
        .await
        .expect("source revision search should succeed")
        .expect("Meilisearch repository should return persisted revisions");

    let requests = recorder.requests.lock().unwrap().clone();
    assert_eq!(requests, vec![json!({ "q": "", "limit": 2000 })]);
    assert_eq!(recorder.corpus_size, 2001);
    assert_eq!(revisions.len(), 2000);
    assert_eq!(revisions.first().unwrap().id, "revision-0");
    assert_eq!(revisions.last().unwrap().id, "revision-1999");
    assert!(revisions
        .iter()
        .all(|revision| revision.id != "revision-2000"));
}
