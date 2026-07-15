use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use axum::{
    body::{to_bytes, Body},
    extract::{Request as AxumRequest, State},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use chrono::Utc;
use nowledge::{
    build_router,
    config::WriteConsistency,
    meili::{settings_for, MeiliAdmin, FIXED_INDEXES},
    models::{
        ContextNode, HistoryEvent, OperationActor, OperationActorScope, OperationPlan,
        OperationResource, OperationStep, OperationStepRole, SourceRef, UserEventIndex,
    },
    mutation::{
        operation_record_from_plan, operation_step_completed, OPERATION_PLAN_SCHEMA_VERSION,
    },
    repository::{KnowledgeRepository, MeiliRepository},
    resolver::EventIndexResolver,
    tenant_scope::tenant_document,
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

static EVENTUAL_TASK_UID: AtomicU64 = AtomicU64::new(1);

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
        if method == Method::GET && path.starts_with("/tasks/") {
            return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
        }
        if (method == Method::POST && path.ends_with("/documents"))
            || (method == Method::PATCH && path.ends_with("/settings"))
        {
            let task_uid = EVENTUAL_TASK_UID.fetch_add(1, Ordering::Relaxed);
            return (StatusCode::ACCEPTED, Json(json!({ "taskUid": task_uid }))).into_response();
        }
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("unexpected stub request: {method} {path}") })),
        )
            .into_response()
    })
}

fn primary_failure_meili(
    request: AxumRequest,
) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>> {
    Box::pin(async move {
        let path = request.uri().path();
        let method = request.method();
        if method == Method::POST && path.ends_with("/search") {
            return (
                StatusCode::OK,
                Json(json!({ "hits": [], "processingTimeMs": 0 })),
            )
                .into_response();
        }
        if method == Method::GET && path == "/tasks/1" {
            return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
        }
        if method == Method::POST && path == "/indexes/rag_state_items/documents" {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": "injected persistence failure" })),
            )
                .into_response();
        }
        if method == Method::POST && path.ends_with("/documents") {
            return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
        }
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("unexpected stub request: {method} {path}") })),
        )
            .into_response()
    })
}

fn missing_task_uid_meili(
    request: AxumRequest,
) -> std::pin::Pin<Box<dyn Future<Output = Response> + Send>> {
    Box::pin(async move {
        let path = request.uri().path();
        let method = request.method();
        if method == Method::GET && path == "/indexes/missing-task-index" {
            return (StatusCode::NOT_FOUND, Json(json!({}))).into_response();
        }
        if method == Method::GET && path == "/indexes/rag_state_items/settings" {
            return (StatusCode::OK, Json(json!({}))).into_response();
        }
        if method == Method::POST || method == Method::PATCH || method == Method::DELETE {
            return (
                StatusCode::ACCEPTED,
                Json(json!({ "status": "enqueued", "note": "task UID omitted" })),
            )
                .into_response();
        }
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": format!("unexpected missing-task request: {method} {path}") })),
        )
            .into_response()
    })
}

#[derive(Clone)]
struct TamperedJournalRecorder {
    operation: Value,
    mutation_requests: Arc<Mutex<Vec<String>>>,
}

async fn tampered_journal_meili_stub(
    State(recorder): State<TamperedJournalRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    if method == Method::POST && path.ends_with("/documents/fetch") {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let offset = request["offset"].as_u64().unwrap() as usize;
        let limit = request["limit"].as_u64().unwrap() as usize;
        let results = if path == "/indexes/rag_operations/documents/fetch" {
            vec![recorder.operation]
        } else if path == "/indexes/rag_user_event_indexes/documents/fetch" {
            Vec::new()
        } else {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": format!("unexpected hydration scan: {path}") })),
            )
                .into_response();
        };
        let total = results.len();
        return (
            StatusCode::OK,
            Json(json!({
                "results": results,
                "offset": offset,
                "limit": limit,
                "total": total
            })),
        )
            .into_response();
    }
    if (method == Method::POST || method == Method::PATCH || method == Method::DELETE)
        && !path.ends_with("/search")
    {
        recorder
            .mutation_requests
            .lock()
            .unwrap()
            .push(format!("{method} {path}"));
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected tampered-journal request: {method} {path}") })),
    )
        .into_response()
}

async fn assert_startup_rejects_tampered_operation_before_mutation(
    tenant_id: &str,
    persisted_operation: Value,
    expected_error: &str,
) {
    let recorder = TamperedJournalRecorder {
        operation: persisted_operation,
        mutation_requests: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .fallback(tampered_journal_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.tenant_id = tenant_id.to_string();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(format!("http://{addr}"));
    let state = AppState::new(Arc::new(config));
    let error = state
        .store
        .hydrate_from_repository(tenant_id)
        .await
        .expect_err("tampered operation must fail startup hydration");

    assert!(error.to_string().contains(expected_error), "{error}");
    assert!(
        recorder.mutation_requests.lock().unwrap().is_empty(),
        "tampered replay reached a mutation endpoint"
    );
    state.shutdown().await;
}

#[derive(Clone)]
struct StartupRegistryRecorder {
    operation: Value,
    registry_row: Option<Value>,
    operation_available: Arc<AtomicBool>,
    registry_visible: Arc<AtomicBool>,
    registry_scans: Arc<AtomicU64>,
    requests: Arc<Mutex<Vec<String>>>,
}

impl StartupRegistryRecorder {
    fn new(operation: Value, registry_row: Option<Value>) -> Self {
        Self {
            operation,
            registry_row,
            operation_available: Arc::new(AtomicBool::new(true)),
            registry_visible: Arc::new(AtomicBool::new(false)),
            registry_scans: Arc::new(AtomicU64::new(0)),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

async fn spawn_startup_registry_stub(recorder: StartupRegistryRecorder) -> String {
    let app = Router::new()
        .fallback(startup_registry_meili_stub)
        .with_state(recorder);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn startup_registry_meili_stub(
    State(recorder): State<StartupRegistryRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::POST && path.ends_with("/documents/fetch") {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let offset = request["offset"].as_u64().unwrap() as usize;
        let limit = request["limit"].as_u64().unwrap() as usize;
        let results = if path == "/indexes/rag_operations/documents/fetch" {
            if recorder.operation_available.swap(false, Ordering::SeqCst) {
                vec![recorder.operation.clone()]
            } else {
                Vec::new()
            }
        } else if path == "/indexes/rag_user_event_indexes/documents/fetch" {
            recorder.registry_scans.fetch_add(1, Ordering::SeqCst);
            if recorder.registry_visible.load(Ordering::SeqCst) {
                recorder.registry_row.clone().into_iter().collect()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        let total = results.len();
        return (
            StatusCode::OK,
            Json(json!({
                "results": results,
                "offset": offset,
                "limit": limit,
                "total": total
            })),
        )
            .into_response();
    }
    if method == Method::POST && path.ends_with("/search") {
        return (
            StatusCode::OK,
            Json(json!({ "hits": [], "processingTimeMs": 0 })),
        )
            .into_response();
    }
    if method == Method::GET && path.starts_with("/tasks/") {
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }
    if method == Method::GET && path.ends_with("/settings") {
        let index_uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/settings"))
            .unwrap();
        return (StatusCode::OK, Json(settings_for(index_uid))).into_response();
    }
    if method == Method::GET && path.starts_with("/indexes/") {
        let index_uid = path.strip_prefix("/indexes/").unwrap();
        return (
            StatusCode::OK,
            Json(json!({ "uid": index_uid, "primaryKey": "id" })),
        )
            .into_response();
    }
    if method == Method::POST && path == "/indexes/rag_user_event_indexes/documents" {
        recorder.registry_visible.store(true, Ordering::SeqCst);
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
    }
    if method == Method::POST && path.ends_with("/documents") {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
    }
    if method == Method::POST && path == "/indexes" {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
    }
    if method == Method::PATCH && path.ends_with("/settings") {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected startup-registry request: {method} {path}") })),
    )
        .into_response()
}

#[derive(Clone, Default)]
struct ResetRecorder {
    requests: Arc<Mutex<Vec<String>>>,
}

#[derive(Clone, Default)]
struct ConcurrentCreateRecorder {
    requests: Arc<Mutex<Vec<String>>>,
    index_checks: Arc<Mutex<usize>>,
}

#[derive(Clone, Default)]
struct ExistingThenMissingRecorder {
    requests: Arc<Mutex<Vec<String>>>,
    index_checks: Arc<Mutex<std::collections::HashMap<String, usize>>>,
}

async fn partial_fixed_set_meili_stub(
    State(recorder): State<ResetRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path.starts_with("/indexes/") {
        let present = path == format!("/indexes/{}", FIXED_INDEXES[0]);
        return (
            if present {
                StatusCode::OK
            } else {
                StatusCode::NOT_FOUND
            },
            Json(json!({})),
        )
            .into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected partial-set request: {method} {path}") })),
    )
        .into_response()
}

async fn existing_then_missing_meili_stub(
    State(recorder): State<ExistingThenMissingRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path.starts_with("/indexes/") {
        let mut checks = recorder.index_checks.lock().unwrap();
        let count = checks.entry(path.clone()).or_default();
        let allowed_checks = if path == "/indexes/rag_operations" {
            2
        } else {
            1
        };
        let status = if *count < allowed_checks {
            StatusCode::OK
        } else {
            StatusCode::NOT_FOUND
        };
        *count += 1;
        return (status, Json(json!({ "primaryKey": "id" }))).into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected recovery request: {method} {path}") })),
    )
        .into_response()
}

async fn concurrent_create_meili_stub(
    State(recorder): State<ConcurrentCreateRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path == "/indexes/concurrent-index" {
        let mut checks = recorder.index_checks.lock().unwrap();
        let status = if *checks == 0 {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::OK
        };
        *checks += 1;
        return (status, Json(json!({ "uid": "concurrent-index" }))).into_response();
    }
    if method == Method::GET && path == "/indexes/concurrent-index/settings" {
        return (StatusCode::OK, Json(json!({}))).into_response();
    }
    if method == Method::POST && path == "/indexes" {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 21 }))).into_response();
    }
    if method == Method::GET && path == "/tasks/21" {
        return (
            StatusCode::OK,
            Json(json!({
                "status": "failed",
                "error": { "code": "index_already_exists" }
            })),
        )
            .into_response();
    }
    if method == Method::PATCH && path == "/indexes/concurrent-index/settings" {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 22 }))).into_response();
    }
    if method == Method::GET && path == "/tasks/22" {
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(
            json!({ "message": format!("unexpected concurrent-create request: {method} {path}") }),
        ),
    )
        .into_response()
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
    if method == Method::GET && path.ends_with("/settings") {
        return (StatusCode::OK, Json(json!({}))).into_response();
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
    if method == Method::GET && path.starts_with("/tasks/") {
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected reset request: {method} {path}") })),
    )
        .into_response()
}

async fn matching_settings_meili_stub(
    State(recorder): State<ResetRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path == "/indexes/rag_sessions" {
        return (StatusCode::OK, Json(json!({ "uid": "rag_sessions" }))).into_response();
    }
    if method == Method::GET && path == "/indexes/rag_sessions/settings" {
        let mut settings = settings_for("rag_sessions");
        settings["filterableAttributes"] = json!([
            "logical_id",
            "revision_id",
            "source_id",
            "privacy",
            "status",
            "dataset_key",
            "snapshot_id",
            "owner_user_id",
            "tenant_id",
            "id"
        ]);
        settings["sortableAttributes"] = json!(["created_at", "id", "occurred_at", "updated_at"]);
        return (StatusCode::OK, Json(settings)).into_response();
    }
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(
            json!({ "message": format!("unexpected matching-settings request: {method} {path}") }),
        ),
    )
        .into_response()
}

fn meili_backed_app(url: String) -> Router {
    let mut config = Config::test();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.write_consistency = WriteConsistency::ReadYourWrites;
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
async fn read_your_writes_exposes_an_accepted_local_event_and_its_context() {
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
    assert!(
        event["meili_task_uid"]
            .as_str()
            .is_some_and(|task_uid| task_uid.parse::<u64>().is_ok()),
        "{event}"
    );

    let (status, search) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/search",
        json!({ "query": "accepted locally", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"].as_array().unwrap().len(), 1, "{search}");
    assert_eq!(search["hits"][0]["id"], event["event"]["id"], "{search}");

    let (status, context) = call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": "u1",
            "query": "accepted locally",
            "limit": 10
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{context}");
    assert!(
        context["hits"].as_array().unwrap().iter().any(|hit| {
            hit["snippet"]
                .as_str()
                .is_some_and(|snippet| snippet.contains("accepted locally"))
        }),
        "{context}"
    );
}

#[tokio::test]
async fn primary_persistence_failure_does_not_publish_a_live_only_state_write() {
    let url = spawn_meili_stub(primary_failure_meili).await;
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

    let (status, absent) = call(
        app,
        Method::GET,
        "/v1/state/profile/facts/persistence-gap?owner_user_id=u1",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{absent}");
    assert_eq!(absent["error"]["code"], "not_found", "{absent}");
}

#[tokio::test]
async fn bootstrap_reset_waits_for_delete_create_and_settings_tasks() {
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
    assert_eq!(requests[1], "GET /tasks/10");
    assert_eq!(requests[2], "GET /indexes/rag_company_context");
    assert_eq!(requests[3], "POST /indexes");
    assert_eq!(requests[4], "GET /tasks/11");
    assert_eq!(requests[5], "GET /indexes/rag_company_context/settings");
    assert_eq!(requests[6], "PATCH /indexes/rag_company_context/settings");
    assert_eq!(
        requests[7], "GET /tasks/12",
        "ensure_index returned before its settings were usable: {requests:?}"
    );
    assert!(
        requests.iter().any(|request| request == "GET /tasks/12"),
        "bootstrap did not wait for settings: {requests:?}"
    );
}

#[tokio::test]
async fn bootstrap_provisions_all_fixed_indexes_only_when_the_managed_set_is_empty() {
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
    MeiliAdmin::from_config(&config)
        .bootstrap(false)
        .await
        .expect("an entirely fresh backend should be provisioned");

    let requests = recorder.requests.lock().unwrap().clone();
    assert_eq!(
        requests
            .iter()
            .filter(|request| request.as_str() == "POST /indexes")
            .count(),
        FIXED_INDEXES.len()
    );
    assert!(!requests
        .iter()
        .any(|request| request.starts_with("DELETE ")));
}

#[tokio::test]
async fn bootstrap_refuses_to_recreate_a_partial_fixed_index_set() {
    let recorder = ResetRecorder::default();
    let app = Router::new()
        .fallback(partial_fixed_set_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.meili_url = Some(format!("http://{addr}"));
    let error = MeiliAdmin::from_config(&config)
        .bootstrap(false)
        .await
        .expect_err("partial managed state must fail closed");

    assert!(error.to_string().contains("incomplete"), "{error}");
    let requests = recorder.requests.lock().unwrap().clone();
    assert!(!requests.iter().any(|request| request == "POST /indexes"));
    assert!(!requests.iter().any(|request| request.contains("/settings")));
}

#[tokio::test]
async fn bootstrap_recovery_rechecks_existing_indexes_without_recreating_them() {
    let recorder = ExistingThenMissingRecorder::default();
    let app = Router::new()
        .fallback(existing_then_missing_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.meili_url = Some(format!("http://{addr}"));
    let error = MeiliAdmin::from_config(&config)
        .bootstrap(false)
        .await
        .expect_err("an index lost after preflight must not be recreated empty");

    assert!(
        error.to_string().contains("refusing empty recreation"),
        "{error}"
    );
    let requests = recorder.requests.lock().unwrap().clone();
    assert!(!requests.iter().any(|request| request == "POST /indexes"));
}

#[tokio::test]
async fn registered_dynamic_index_reconciliation_refuses_empty_recreation() {
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
    let repository = MeiliRepository::new(MeiliAdmin::from_config(&config), false);
    let index = UserEventIndex {
        id: "registry-row".to_string(),
        tenant_id: "tenant-a".to_string(),
        tenant_hash: "tenant-hash".to_string(),
        owner_user_id_hash: "owner-hash".to_string(),
        event_index_uid: "missing-event-index".to_string(),
        personal_context_index_uid: "missing-context-index".to_string(),
        schema_version: 1,
        settings_hash: "events-v3".to_string(),
        status: "active".to_string(),
        created_at: Utc::now(),
        last_event_at: None,
        event_count_estimate: 0,
    };
    let error = repository
        .reconcile_registered_user_event_index(&index)
        .await
        .expect_err("registered indexes are authoritative durable state");

    assert!(
        error.to_string().contains("refusing empty recreation"),
        "{error}"
    );
    let requests = recorder.requests.lock().unwrap().clone();
    assert_eq!(requests, ["GET /indexes/missing-event-index"]);
}

#[tokio::test]
async fn settings_reconciliation_skips_an_identical_managed_configuration() {
    let recorder = ResetRecorder::default();
    let app = Router::new()
        .fallback(matching_settings_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.meili_url = Some(format!("http://{addr}"));
    let tasks = MeiliAdmin::from_config(&config)
        .reconcile_existing_index("rag_sessions", true)
        .await
        .expect("identical settings should require no Meilisearch mutation");

    assert!(tasks.is_empty());
    assert_eq!(
        recorder.requests.lock().unwrap().as_slice(),
        [
            "GET /indexes/rag_sessions",
            "GET /indexes/rag_sessions/settings"
        ]
    );
}

#[tokio::test]
async fn ensure_index_tolerates_a_concurrent_create_winner_and_still_waits_for_settings() {
    let recorder = ConcurrentCreateRecorder::default();
    let app = Router::new()
        .fallback(concurrent_create_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.meili_url = Some(format!("http://{addr}"));
    let tasks = MeiliAdmin::from_config(&config)
        .ensure_index("concurrent-index", "id", true)
        .await
        .expect("the other creator's finished index should satisfy provisioning");

    assert_eq!(tasks, vec!["22"]);
    assert_eq!(
        recorder.requests.lock().unwrap().as_slice(),
        [
            "GET /indexes/concurrent-index",
            "POST /indexes",
            "GET /tasks/21",
            "GET /indexes/concurrent-index",
            "GET /indexes/concurrent-index/settings",
            "PATCH /indexes/concurrent-index/settings",
            "GET /tasks/22",
        ]
    );
}

#[tokio::test]
async fn successful_meili_mutations_without_task_uids_fail_closed() {
    let url = spawn_meili_stub(missing_task_uid_meili).await;
    let mut config = Config::test();
    config.meili_url = Some(url);
    let admin = MeiliAdmin::from_config(&config);

    let errors = vec![
        admin
            .ensure_index("missing-task-index", "id", false)
            .await
            .expect_err("index creation without a task UID must fail"),
        admin
            .add_documents("rag_state_items", &json!([{ "id": "state-1" }]))
            .await
            .expect_err("document writes without a task UID must fail"),
        admin
            .delete_documents_by_filter("rag_state_items", "id = \"state-1\"")
            .await
            .expect_err("filter deletes without a task UID must fail"),
        admin
            .delete_documents_by_ids("rag_state_items", &["state-1".to_string()])
            .await
            .expect_err("batch deletes without a task UID must fail"),
        admin
            .apply_settings("rag_state_items")
            .await
            .expect_err("settings writes without a task UID must fail"),
        admin
            .bootstrap(true)
            .await
            .expect_err("index deletion without a task UID must fail"),
    ];

    for error in errors {
        assert!(
            error.to_string().contains("without returning a task UID"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn startup_rejects_tampered_persisted_dynamic_context_routing_before_mutation() {
    let tenant_id = "tenant-tampered-operation";
    let tampered_index_uid = "rag_context__t_wrong_tenant__u_wrong_owner";
    let at = Utc::now();
    let record = operation_record_from_plan(OperationPlan {
        schema_version: OPERATION_PLAN_SCHEMA_VERSION,
        id: "tampered-operation".to_string(),
        tenant_id: tenant_id.to_string(),
        operation_kind: "context.upsert".to_string(),
        actor: OperationActor {
            scope: OperationActorScope::TenantService,
            owner_user_id_hash: None,
            roles: vec!["writer".to_string()],
            request_id: Some("tampered-operation-request".to_string()),
        },
        idempotency_key_hash: None,
        primary: OperationStep {
            id: "primary".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::ContextNodes {
                index_uid: tampered_index_uid.to_string(),
                nodes: vec![ContextNode {
                    uri: "ctx://users/u1/tampered".to_string(),
                    title: "Tampered persisted context".to_string(),
                    layer: 1,
                    body: "must never be replayed".to_string(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: Some("u1".to_string()),
                    index_uid: tampered_index_uid.to_string(),
                    index_kind: "personal".to_string(),
                    ancestor_uris: vec!["ctx://users/u1".to_string()],
                    node_kind: "context".to_string(),
                    retrieval_role: "content".to_string(),
                    retrieval_enabled: true,
                    parent_uri: Some("ctx://users/u1".to_string()),
                    source_document_uri: None,
                    fragment_index: None,
                    char_start: None,
                    char_end: None,
                    token_estimate: None,
                    checksum: None,
                    source_id: None,
                    revision_id: None,
                    block_type: None,
                    page_idx: None,
                    bbox: None,
                    section_path: Vec::new(),
                    heading_level: None,
                    asset_refs: Vec::new(),
                    artifact_refs: Vec::new(),
                    status: "active".to_string(),
                    privacy: "private".to_string(),
                    updated_at: at,
                }],
            },
        },
        side_effects: Vec::new(),
        redacted_metadata: json!({ "fixture": "tampered dynamic routing" }),
        response_snapshot: json!({ "ok": true }),
        created_at: at,
    })
    .expect("the journal fixture is structurally valid before route derivation");
    let persisted_operation =
        tenant_document(tenant_id, "rag_operations", &record.id, &record).unwrap();
    let recorder = TamperedJournalRecorder {
        operation: persisted_operation,
        mutation_requests: Arc::new(Mutex::new(Vec::new())),
    };
    let app = Router::new()
        .fallback(tampered_journal_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.tenant_id = tenant_id.to_string();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(format!("http://{addr}"));
    let state = AppState::new(Arc::new(config));
    let error = state
        .store
        .hydrate_from_repository(tenant_id)
        .await
        .expect_err("tampered dynamic routing must fail startup hydration");

    assert!(
        error.to_string().contains("invalid personal-context route"),
        "{error}"
    );
    let report = serde_json::to_value(state.store.hydration_report().unwrap()).unwrap();
    assert_eq!(report["ready"], false, "{report}");
    assert_eq!(report["status"], "incomplete", "{report}");
    assert_eq!(
        report["domains"]["operations"]["status"], "incomplete",
        "{report}"
    );
    assert!(
        recorder.mutation_requests.lock().unwrap().is_empty(),
        "tampered replay reached a mutation endpoint"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn startup_rejects_tampered_user_index_metadata_before_mutation() {
    let tenant_id = "tenant-tampered-user-index-metadata";
    let owner_user_id = "owner-tampered-user-index-metadata";
    let mut config = Config::test();
    config.tenant_id = tenant_id.to_string();
    let resolver = EventIndexResolver::new(config.index_hash_secret.clone());
    let routing = resolver
        .resolve(tenant_id, owner_user_id, true, false)
        .unwrap();
    let tenant_hash = resolver.tenant_hash(tenant_id);
    let at = Utc::now();
    let current = UserEventIndex {
        id: format!("uei__t_{tenant_hash}__u_{}", routing.owner_user_id_hash),
        tenant_id: tenant_id.to_string(),
        tenant_hash,
        owner_user_id_hash: routing.owner_user_id_hash.clone(),
        event_index_uid: routing.event_index_uid.clone(),
        personal_context_index_uid: routing.personal_context_index_uid.clone(),
        schema_version: routing.schema_version,
        settings_hash: routing.settings_hash.clone(),
        status: "active".to_string(),
        created_at: at,
        last_event_at: None,
        event_count_estimate: 0,
    };
    let mut stale_schema = current.clone();
    stale_schema.schema_version = routing.schema_version.saturating_add(1);
    let mut stale_settings = current.clone();
    stale_settings.settings_hash = "events-stale".to_string();
    let mut inactive = current;
    inactive.status = "disabled".to_string();

    for (case, index) in [
        ("schema", stale_schema),
        ("settings", stale_settings),
        ("status", inactive),
    ] {
        let record = operation_record_from_plan(OperationPlan {
            schema_version: OPERATION_PLAN_SCHEMA_VERSION,
            id: format!("tampered-user-index-{case}"),
            tenant_id: tenant_id.to_string(),
            operation_kind: "user_event_index.ensure".to_string(),
            actor: OperationActor {
                scope: OperationActorScope::Owner,
                owner_user_id_hash: Some(routing.owner_user_id_hash.clone()),
                roles: vec!["owner".to_string()],
                request_id: Some(format!("tampered-user-index-{case}-request")),
            },
            idempotency_key_hash: None,
            primary: OperationStep {
                id: "primary".to_string(),
                role: OperationStepRole::Primary,
                resource: OperationResource::EnsureUserEventIndex { index },
            },
            side_effects: Vec::new(),
            redacted_metadata: json!({ "fixture": case }),
            response_snapshot: json!({ "ok": true }),
            created_at: at,
        })
        .expect("tampered metadata should remain structurally valid for journal replay");
        let persisted_operation =
            tenant_document(tenant_id, "rag_operations", &record.id, &record).unwrap();

        assert_startup_rejects_tampered_operation_before_mutation(
            tenant_id,
            persisted_operation,
            "invalid user-index route",
        )
        .await;
    }
}

#[tokio::test]
async fn startup_rejects_tampered_history_schema_before_mutation() {
    let tenant_id = "tenant-tampered-history-schema";
    let owner_user_id = "owner-tampered-history-schema";
    let mut config = Config::test();
    config.tenant_id = tenant_id.to_string();
    let routing = EventIndexResolver::new(config.index_hash_secret.clone())
        .resolve(tenant_id, owner_user_id, false, true)
        .unwrap();
    let at = Utc::now();
    let record = operation_record_from_plan(OperationPlan {
        schema_version: OPERATION_PLAN_SCHEMA_VERSION,
        id: "tampered-history-schema-operation".to_string(),
        tenant_id: tenant_id.to_string(),
        operation_kind: "history_event.append".to_string(),
        actor: OperationActor {
            scope: OperationActorScope::Owner,
            owner_user_id_hash: Some(routing.owner_user_id_hash.clone()),
            roles: vec!["owner".to_string()],
            request_id: Some("tampered-history-schema-request".to_string()),
        },
        idempotency_key_hash: None,
        primary: OperationStep {
            id: "primary".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::HistoryEvents {
                events: vec![HistoryEvent {
                    id: "tampered-history-schema-event".to_string(),
                    event_type: "note.created".to_string(),
                    entity_type: "note".to_string(),
                    entity_id: "tampered-history-schema-event".to_string(),
                    occurred_at: at,
                    observed_at: at,
                    source_kind: "test".to_string(),
                    source_ref: SourceRef {
                        kind: "test".to_string(),
                        id: "tampered-history-schema-event".to_string(),
                        uri: None,
                        meta: None,
                    },
                    text: "must not replay with stale dynamic-index metadata".to_string(),
                    payload: json!({}),
                    tags: Vec::new(),
                    privacy: "private".to_string(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: owner_user_id.to_string(),
                    owner_user_id_hash: routing.owner_user_id_hash.clone(),
                    event_index_uid: routing.event_index_uid.clone(),
                    event_index_schema_version: routing.schema_version.saturating_add(1),
                    idempotency_key_hash: None,
                }],
            },
        },
        side_effects: Vec::new(),
        redacted_metadata: json!({ "fixture": "tampered history schema" }),
        response_snapshot: json!({ "ok": true }),
        created_at: at,
    })
    .expect("the stale history schema should remain structurally valid for journal replay");
    let persisted_operation =
        tenant_document(tenant_id, "rag_operations", &record.id, &record).unwrap();

    assert_startup_rejects_tampered_operation_before_mutation(
        tenant_id,
        persisted_operation,
        "invalid history-event route",
    )
    .await;
}

#[tokio::test]
async fn startup_rejects_valid_history_replay_without_an_authoritative_registry_row() {
    let tenant_id = "tenant-unregistered-history-operation";
    let owner_user_id = "owner-unregistered-history-operation";
    let mut config = Config::test();
    config.tenant_id = tenant_id.to_string();
    let routing = EventIndexResolver::new(config.index_hash_secret.clone())
        .resolve(tenant_id, owner_user_id, false, true)
        .unwrap();
    let at = Utc::now();
    let record = operation_record_from_plan(OperationPlan {
        schema_version: OPERATION_PLAN_SCHEMA_VERSION,
        id: "unregistered-history-operation".to_string(),
        tenant_id: tenant_id.to_string(),
        operation_kind: "history_event.append".to_string(),
        actor: OperationActor {
            scope: OperationActorScope::Owner,
            owner_user_id_hash: Some(routing.owner_user_id_hash.clone()),
            roles: vec!["owner".to_string()],
            request_id: Some("unregistered-history-request".to_string()),
        },
        idempotency_key_hash: None,
        primary: OperationStep {
            id: "primary".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::HistoryEvents {
                events: vec![HistoryEvent {
                    id: "unregistered-history-event".to_string(),
                    event_type: "note.created".to_string(),
                    entity_type: "note".to_string(),
                    entity_id: "unregistered-history-event".to_string(),
                    occurred_at: at,
                    observed_at: at,
                    source_kind: "test".to_string(),
                    source_ref: SourceRef {
                        kind: "test".to_string(),
                        id: "unregistered-history-event".to_string(),
                        uri: None,
                        meta: None,
                    },
                    text: "must not auto-create a missing dynamic index".to_string(),
                    payload: json!({}),
                    tags: Vec::new(),
                    privacy: "private".to_string(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: owner_user_id.to_string(),
                    owner_user_id_hash: routing.owner_user_id_hash.clone(),
                    event_index_uid: routing.event_index_uid.clone(),
                    event_index_schema_version: routing.schema_version,
                    idempotency_key_hash: None,
                }],
            },
        },
        side_effects: Vec::new(),
        redacted_metadata: json!({ "fixture": "unregistered dynamic history replay" }),
        response_snapshot: json!({ "ok": true }),
        created_at: at,
    })
    .expect("the history operation fixture must be structurally valid");
    let persisted_operation =
        tenant_document(tenant_id, "rag_operations", &record.id, &record).unwrap();
    let recorder = StartupRegistryRecorder::new(persisted_operation, None);
    let url = spawn_startup_registry_stub(recorder.clone()).await;
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    let state = AppState::new(Arc::new(config));

    let error = state
        .store
        .hydrate_from_repository(tenant_id)
        .await
        .expect_err("history replay without a registry row must fail hydration");

    assert!(
        error.to_string().contains("unregistered dynamic index"),
        "startup failure should identify the missing authoritative registry row: {error}"
    );
    let requests = recorder.requests.lock().unwrap().clone();
    assert!(
        !requests.iter().any(|request| {
            request == &format!("POST /indexes/{}/documents", routing.event_index_uid)
        }),
        "unregistered history replay reached the dynamic document write: {requests:?}"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn startup_rejects_completed_ensure_without_authoritative_registry_before_dynamic_replay() {
    let tenant_id = "tenant-completed-ensure-without-registry";
    let owner_user_id = "owner-completed-ensure-without-registry";
    let mut config = Config::test();
    config.tenant_id = tenant_id.to_string();
    let resolver = EventIndexResolver::new(config.index_hash_secret.clone());
    let routing = resolver
        .resolve(tenant_id, owner_user_id, true, false)
        .unwrap();
    let tenant_hash = resolver.tenant_hash(tenant_id);
    let at = Utc::now();
    let index = UserEventIndex {
        id: format!("uei__t_{tenant_hash}__u_{}", routing.owner_user_id_hash),
        tenant_id: tenant_id.to_string(),
        tenant_hash,
        owner_user_id_hash: routing.owner_user_id_hash.clone(),
        event_index_uid: routing.event_index_uid.clone(),
        personal_context_index_uid: routing.personal_context_index_uid.clone(),
        schema_version: routing.schema_version,
        settings_hash: routing.settings_hash.clone(),
        status: "active".to_string(),
        created_at: at,
        last_event_at: None,
        event_count_estimate: 0,
    };
    let context_uri = format!("ctx://users/{owner_user_id}/completed-ensure");
    let record = operation_record_from_plan(OperationPlan {
        schema_version: OPERATION_PLAN_SCHEMA_VERSION,
        id: "completed-ensure-without-registry-operation".to_string(),
        tenant_id: tenant_id.to_string(),
        operation_kind: "user_event_index.ensure_with_context".to_string(),
        actor: OperationActor {
            scope: OperationActorScope::Owner,
            owner_user_id_hash: Some(routing.owner_user_id_hash.clone()),
            roles: vec!["owner".to_string()],
            request_id: Some("completed-ensure-without-registry-request".to_string()),
        },
        idempotency_key_hash: None,
        primary: OperationStep {
            id: "ensure-index".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::EnsureUserEventIndex {
                index: index.clone(),
            },
        },
        side_effects: vec![OperationStep {
            id: "write-personal-context".to_string(),
            role: OperationStepRole::SideEffect,
            resource: OperationResource::ContextNodes {
                index_uid: routing.personal_context_index_uid.clone(),
                nodes: vec![ContextNode {
                    uri: context_uri.clone(),
                    title: "Completed ensure without registry".to_string(),
                    layer: 1,
                    body: "must not be replayed through an unregistered dynamic index".to_string(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: Some(owner_user_id.to_string()),
                    index_uid: routing.personal_context_index_uid.clone(),
                    index_kind: "personal".to_string(),
                    ancestor_uris: vec![format!("ctx://users/{owner_user_id}")],
                    node_kind: "context".to_string(),
                    retrieval_role: "content".to_string(),
                    retrieval_enabled: true,
                    parent_uri: Some(format!("ctx://users/{owner_user_id}")),
                    source_document_uri: None,
                    fragment_index: None,
                    char_start: None,
                    char_end: None,
                    token_estimate: None,
                    checksum: None,
                    source_id: None,
                    revision_id: None,
                    block_type: None,
                    page_idx: None,
                    bbox: None,
                    section_path: Vec::new(),
                    heading_level: None,
                    asset_refs: Vec::new(),
                    artifact_refs: Vec::new(),
                    status: "active".to_string(),
                    privacy: "private".to_string(),
                    updated_at: at,
                }],
            },
        }],
        redacted_metadata: json!({ "fixture": "completed ensure missing registry" }),
        response_snapshot: json!({ "ok": true }),
        created_at: at,
    })
    .expect("the completed-ensure fixture must be structurally valid");
    let record =
        operation_step_completed(&record, "ensure-index", at + chrono::Duration::seconds(1))
            .expect("the ensure step should be completed before persistence");
    let persisted_operation =
        tenant_document(tenant_id, "rag_operations", &record.id, &record).unwrap();
    let recorder = StartupRegistryRecorder::new(persisted_operation, None);
    let url = spawn_startup_registry_stub(recorder.clone()).await;
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    let state = AppState::new(Arc::new(config));

    let error = state
        .store
        .hydrate_from_repository(tenant_id)
        .await
        .expect_err("a completed ensure without an authoritative row must fail hydration");

    assert!(
        error.to_string().contains("unregistered dynamic index"),
        "startup failure should identify the missing authoritative registry row: {error}"
    );
    let requests = recorder.requests.lock().unwrap().clone();
    assert!(
        !requests.iter().any(|request| {
            request
                == &format!(
                    "POST /indexes/{}/documents",
                    routing.personal_context_index_uid
                )
        }),
        "startup replay reached the pending personal-context write: {requests:?}"
    );
    assert!(
        !recorder.registry_visible.load(Ordering::SeqCst),
        "startup replay rewrote a registry row for an already-completed ensure"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn startup_refreshes_registry_after_replaying_an_ensure_operation() {
    let tenant_id = "tenant-replayed-user-index";
    let owner_user_id = "owner-replayed-user-index";
    let mut config = Config::test();
    config.tenant_id = tenant_id.to_string();
    let resolver = EventIndexResolver::new(config.index_hash_secret.clone());
    let routing = resolver
        .resolve(tenant_id, owner_user_id, true, false)
        .unwrap();
    let tenant_hash = resolver.tenant_hash(tenant_id);
    let at = Utc::now();
    let index = UserEventIndex {
        id: format!("uei__t_{tenant_hash}__u_{}", routing.owner_user_id_hash),
        tenant_id: tenant_id.to_string(),
        tenant_hash,
        owner_user_id_hash: routing.owner_user_id_hash.clone(),
        event_index_uid: routing.event_index_uid.clone(),
        personal_context_index_uid: routing.personal_context_index_uid.clone(),
        schema_version: routing.schema_version,
        settings_hash: routing.settings_hash.clone(),
        status: "active".to_string(),
        created_at: at,
        last_event_at: None,
        event_count_estimate: 0,
    };
    let record = operation_record_from_plan(OperationPlan {
        schema_version: OPERATION_PLAN_SCHEMA_VERSION,
        id: "replayed-user-index-operation".to_string(),
        tenant_id: tenant_id.to_string(),
        operation_kind: "user_event_index.ensure".to_string(),
        actor: OperationActor {
            scope: OperationActorScope::Owner,
            owner_user_id_hash: Some(routing.owner_user_id_hash.clone()),
            roles: vec!["owner".to_string()],
            request_id: Some("replayed-user-index-request".to_string()),
        },
        idempotency_key_hash: None,
        primary: OperationStep {
            id: "primary".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::EnsureUserEventIndex {
                index: index.clone(),
            },
        },
        side_effects: Vec::new(),
        redacted_metadata: json!({ "fixture": "replayed user-index registry row" }),
        response_snapshot: json!({ "ok": true }),
        created_at: at,
    })
    .expect("the ensure-index operation fixture must be structurally valid");
    let persisted_operation =
        tenant_document(tenant_id, "rag_operations", &record.id, &record).unwrap();
    let persisted_registry =
        tenant_document(tenant_id, "rag_user_event_indexes", &index.id, &index).unwrap();
    let recorder = StartupRegistryRecorder::new(persisted_operation, Some(persisted_registry));
    let url = spawn_startup_registry_stub(recorder.clone()).await;
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    let state = AppState::new(Arc::new(config));

    state
        .store
        .hydrate_from_repository(tenant_id)
        .await
        .expect("startup should replay and retain the missing registry row");

    let indexes = state.store.list_user_indexes(tenant_id).unwrap().indexes;
    assert_eq!(
        indexes.len(),
        1,
        "replayed registry row was lost: {indexes:?}"
    );
    assert_eq!(indexes[0].id, index.id);
    assert_eq!(indexes[0].event_index_uid, index.event_index_uid);
    assert!(
        recorder.registry_scans.load(Ordering::SeqCst) >= 2,
        "startup did not refresh the authoritative registry after replay"
    );
    assert!(
        recorder.registry_visible.load(Ordering::SeqCst),
        "ensure-index replay did not durably publish the registry row"
    );
    state.shutdown().await;
}
