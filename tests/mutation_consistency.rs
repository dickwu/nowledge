use std::{
    collections::HashMap,
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
use nowledge::{
    build_router, config::WriteConsistency, meili::settings_for,
    models::EnsureUserEventIndexRequest, AppState, Config,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tower::ServiceExt;

fn app() -> (AppState, Router) {
    let state = AppState::new(Arc::new(Config::test()));
    let app = build_router(state.clone());
    (state, app)
}

#[derive(Clone, Default)]
struct HistorySideEffectFault {
    next_task_uid: Arc<AtomicU64>,
    personal_context_attempts: Arc<AtomicU64>,
    event_primary_task_uid: Arc<AtomicU64>,
    event_confirmation_attempts: Arc<AtomicU64>,
    operation_journal: Arc<Mutex<HashMap<String, Value>>>,
    operation_fetch_requests: Arc<Mutex<Vec<Value>>>,
    operation_fetch_total_override: Arc<AtomicU64>,
    request_log: Arc<Mutex<Vec<String>>>,
    documents_by_index: Arc<Mutex<HashMap<String, HashMap<String, Value>>>>,
    freeze_domain_writes: Arc<AtomicBool>,
    fail_state_primary_once: Arc<AtomicBool>,
    state_primary_attempts: Arc<AtomicU64>,
    persist_documents: bool,
    allow_personal_context: bool,
    fail_personal_context_always: bool,
    fail_event_task_confirmation: bool,
}

impl HistorySideEffectFault {
    fn next_task_uid(&self) -> u64 {
        self.next_task_uid.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn accepted(&self) -> Response {
        let task_uid = self.next_task_uid();
        (StatusCode::ACCEPTED, Json(json!({ "taskUid": task_uid }))).into_response()
    }
}

async fn history_side_effect_failure_meili(
    State(fault): State<HistorySideEffectFault>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    fault
        .request_log
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path.ends_with("/settings") {
        let uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/settings"))
            .expect("settings request should contain an index UID");
        if uid == "rag_operations" {
            return (StatusCode::OK, Json(settings_for(uid))).into_response();
        }
        return (StatusCode::OK, Json(json!({}))).into_response();
    }
    if method == Method::GET && path.starts_with("/indexes/") {
        let uid = path.trim_start_matches("/indexes/");
        return (
            StatusCode::OK,
            Json(json!({
                "uid": uid,
                "primaryKey": "id",
                "createdAt": "2026-07-14T00:00:00Z"
            })),
        )
            .into_response();
    }
    if method == Method::GET && path.starts_with("/tasks/") {
        let task_uid = path.trim_start_matches("/tasks/").parse::<u64>().unwrap();
        if fault.fail_event_task_confirmation
            && task_uid == fault.event_primary_task_uid.load(Ordering::Relaxed)
        {
            fault
                .event_confirmation_attempts
                .fetch_add(1, Ordering::Relaxed);
            return (
                StatusCode::OK,
                Json(json!({
                    "status": "failed",
                    "error": { "message": "injected event task confirmation failure" }
                })),
            )
                .into_response();
        }
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }
    if method == Method::POST && path == "/indexes/rag_operations/search" {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let filter = request["filter"].as_str().unwrap_or_default();
        let tenant_id = meili_eq_filter_value(filter, "tenant_id");
        let operation_ids = meili_in_filter_values(filter, "logical_id").or_else(|| {
            meili_eq_filter_value(filter, "logical_id").map(|operation_id| vec![operation_id])
        });
        let statuses = meili_in_filter_values(filter, "status");
        let limit = request["limit"].as_u64().unwrap_or(20) as usize;
        let mut hits = fault
            .operation_journal
            .lock()
            .unwrap()
            .values()
            .filter(|operation| {
                tenant_id.as_ref().is_none_or(|tenant_id| {
                    operation["tenant_id"].as_str() == Some(tenant_id.as_str())
                }) && operation_ids.as_ref().is_none_or(|operation_ids| {
                    operation["logical_id"]
                        .as_str()
                        .is_some_and(|operation_id| {
                            operation_ids
                                .iter()
                                .any(|candidate| candidate == operation_id)
                        })
                }) && statuses.as_ref().is_none_or(|statuses| {
                    operation["status"]
                        .as_str()
                        .is_some_and(|status| statuses.iter().any(|candidate| candidate == status))
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            left["created_at"]
                .as_str()
                .cmp(&right["created_at"].as_str())
                .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()))
        });
        hits.truncate(limit);
        return (
            StatusCode::OK,
            Json(json!({ "hits": hits, "processingTimeMs": 0 })),
        )
            .into_response();
    }
    if method == Method::POST && path.ends_with("/search") {
        if fault.persist_documents {
            let index_uid = path
                .strip_prefix("/indexes/")
                .and_then(|path| path.strip_suffix("/search"))
                .expect("test search path has an index uid");
            let hits = fault
                .documents_by_index
                .lock()
                .unwrap()
                .get(index_uid)
                .into_iter()
                .flat_map(|documents| documents.values().cloned())
                .collect::<Vec<_>>();
            return (
                StatusCode::OK,
                Json(json!({ "hits": hits, "processingTimeMs": 0 })),
            )
                .into_response();
        }
        return (
            StatusCode::OK,
            Json(json!({ "hits": [], "processingTimeMs": 0 })),
        )
            .into_response();
    }
    if method == Method::POST && path == "/indexes/rag_operations/documents/fetch" {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        fault
            .operation_fetch_requests
            .lock()
            .unwrap()
            .push(request.clone());
        let offset = request["offset"].as_u64().unwrap() as usize;
        let limit = request["limit"].as_u64().unwrap() as usize;
        let filter = request["filter"].as_str().unwrap();
        let tenant_id = meili_eq_filter_value(filter, "tenant_id");
        let operation_kinds = meili_in_filter_values(filter, "operation_kind");
        let statuses = meili_in_filter_values(filter, "status");
        let reconcilable_only = filter.contains("status != \"completed\"")
            && filter.contains("indexing_state != \"completed\"");
        let mut operations = fault
            .operation_journal
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect::<Vec<_>>();
        operations.retain(|operation| {
            tenant_id
                .as_ref()
                .is_none_or(|tenant_id| operation["tenant_id"].as_str() == Some(tenant_id.as_str()))
                && operation_kinds.as_ref().is_none_or(|operation_kinds| {
                    operation["operation_kind"].as_str().is_some_and(|kind| {
                        operation_kinds.iter().any(|candidate| candidate == kind)
                    })
                })
                && statuses.as_ref().is_none_or(|statuses| {
                    operation["status"]
                        .as_str()
                        .is_some_and(|status| statuses.iter().any(|candidate| candidate == status))
                })
                && (!reconcilable_only
                    || operation["status"] != "completed"
                    || operation["indexing_state"] != "completed")
        });
        let ascending = request["sort"]
            .as_array()
            .and_then(|sort| sort.first())
            .and_then(Value::as_str)
            .is_some_and(|sort| sort.ends_with(":asc"));
        operations.sort_by(|left, right| {
            let ordering = left["created_at"]
                .as_str()
                .cmp(&right["created_at"].as_str())
                .then_with(|| left["id"].as_str().cmp(&right["id"].as_str()));
            if ascending {
                ordering
            } else {
                ordering.reverse()
            }
        });
        let total_override = fault.operation_fetch_total_override.load(Ordering::Relaxed);
        let total = if total_override == 0 || operations.is_empty() {
            operations.len()
        } else {
            total_override as usize
        };
        let results = operations
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
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
    if method == Method::POST && path.ends_with("/documents/fetch") {
        let index_uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/documents/fetch"))
            .expect("test document-fetch path has an index uid");
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let offset = request["offset"].as_u64().unwrap_or_default() as usize;
        let limit = request["limit"].as_u64().unwrap_or(100) as usize;
        let mut documents = if fault.persist_documents {
            fault
                .documents_by_index
                .lock()
                .unwrap()
                .get(index_uid)
                .into_iter()
                .flat_map(|documents| documents.values().cloned())
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        documents.sort_by(|left, right| left["id"].as_str().cmp(&right["id"].as_str()));
        let total = documents.len();
        let results = documents
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect::<Vec<_>>();
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
    if method == Method::POST && path == "/indexes/rag_operations/documents" {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let documents: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
        let mut journal = fault.operation_journal.lock().unwrap();
        for document in documents {
            let operation_id = document["logical_id"]
                .as_str()
                .expect("operation journal document has a logical id")
                .to_string();
            journal.insert(operation_id, document);
        }
        drop(journal);
        return fault.accepted();
    }
    if method == Method::POST
        && path.starts_with("/indexes/rag_events__")
        && path.ends_with("/documents")
    {
        let task_uid = fault.next_task_uid();
        fault
            .event_primary_task_uid
            .store(task_uid, Ordering::Relaxed);
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": task_uid }))).into_response();
    }
    if method == Method::POST && path == "/indexes/rag_state_items/documents" {
        fault.state_primary_attempts.fetch_add(1, Ordering::Relaxed);
        if fault.fail_state_primary_once.swap(false, Ordering::Relaxed) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": "injected state primary failure" })),
            )
                .into_response();
        }
    }
    if method == Method::POST
        && path.starts_with("/indexes/rag_context__")
        && path.ends_with("/documents")
        && !fault.allow_personal_context
    {
        let attempt = fault
            .personal_context_attempts
            .fetch_add(1, Ordering::Relaxed);
        if attempt == 0 || fault.fail_personal_context_always {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": "injected personal-context side-effect failure" })),
            )
                .into_response();
        }
    }
    if (method == Method::POST && path.ends_with("/documents"))
        || (method == Method::PATCH && path.ends_with("/settings"))
    {
        if method == Method::POST
            && fault.persist_documents
            && !fault.freeze_domain_writes.load(Ordering::Relaxed)
        {
            let index_uid = path
                .strip_prefix("/indexes/")
                .and_then(|path| path.strip_suffix("/documents"))
                .expect("test document path has an index uid");
            let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
            let documents: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
            let mut indexes = fault.documents_by_index.lock().unwrap();
            let index = indexes.entry(index_uid.to_string()).or_default();
            for document in documents {
                let id = document["id"]
                    .as_str()
                    .expect("persisted test document has a physical id")
                    .to_string();
                index.insert(id, document);
            }
        }
        return fault.accepted();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "message": format!("unexpected side-effect stub request: {method} {path}")
        })),
    )
        .into_response()
}

async fn side_effect_fault_app() -> (AppState, Router, HistorySideEffectFault) {
    fault_app(HistorySideEffectFault::default()).await
}

async fn task_confirmation_fault_app() -> (AppState, Router, HistorySideEffectFault) {
    fault_app_with_consistency(
        HistorySideEffectFault {
            allow_personal_context: true,
            fail_event_task_confirmation: true,
            ..HistorySideEffectFault::default()
        },
        WriteConsistency::ReadYourWrites,
    )
    .await
}

async fn fault_app(fault: HistorySideEffectFault) -> (AppState, Router, HistorySideEffectFault) {
    fault_app_with_consistency(fault, WriteConsistency::ReadYourWrites).await
}

async fn fault_app_with_consistency(
    fault: HistorySideEffectFault,
    write_consistency: WriteConsistency,
) -> (AppState, Router, HistorySideEffectFault) {
    let meili = Router::new()
        .fallback(history_side_effect_failure_meili)
        .with_state(fault.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, meili).await.unwrap();
    });

    let mut config = Config::test();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(format!("http://{addr}"));
    config.meili_wait_for_tasks = false;
    config.write_consistency = write_consistency;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());
    (state, app, fault)
}

async fn call(app: Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, body)
}

fn event(owner_user_id: &str, entity_id: &str, text: &str) -> Value {
    json!({
        "event_type": "note.created",
        "entity_type": "note",
        "entity_id": entity_id,
        "owner_user_id": owner_user_id,
        "occurred_at": "2026-07-14T00:00:00Z",
        "observed_at": "2026-07-14T00:00:01Z",
        "source_kind": "test",
        "source_ref": { "kind": "test", "id": entity_id },
        "text": text,
        "privacy": "private"
    })
}

fn query_encode(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace(':', "%3A")
        .replace('/', "%2F")
        .replace(' ', "%20")
}

fn meili_in_filter_values(filter: &str, field: &str) -> Option<Vec<String>> {
    let tail = filter.split_once(&format!("{field} IN "))?.1;
    let end = tail.find(']')?;
    serde_json::from_str(&tail[..=end]).ok()
}

fn meili_eq_filter_value(filter: &str, field: &str) -> Option<String> {
    let tail = filter.split_once(&format!("{field} = "))?.1;
    String::deserialize(&mut serde_json::Deserializer::from_str(tail)).ok()
}

#[test]
fn ensure_user_index_default_keeps_companion_index_enabled() {
    assert!(EnsureUserEventIndexRequest::default().create_personal_context_index);
}

#[tokio::test]
async fn ensure_user_index_rejects_non_current_or_incomplete_routing_shapes() {
    let (state, app) = app();

    for request in [
        json!({ "schema_version": 999 }),
        json!({ "create_personal_context_index": false }),
    ] {
        let (status, rejected) = call(
            app.clone(),
            Method::PUT,
            "/v1/history/users/u1/event-index",
            request,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
        assert_eq!(rejected["error"]["code"], "bad_request", "{rejected}");
    }

    assert!(
        state
            .store
            .list_user_indexes(state.tenant_id())
            .unwrap()
            .indexes
            .is_empty(),
        "invalid ensure request created a registry row"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn invalid_bulk_is_prevalidated_without_partial_local_writes() {
    let (state, app) = app();
    let mut invalid_second = event("u1", "invalid-second", "missing required field");
    invalid_second["event_type"] = Value::Null;
    let (status, rejected) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events:bulk",
        json!({
            "events": [
                event("u1", "valid-first", "must not be inserted"),
                invalid_second
            ]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    assert_eq!(rejected["error"]["code"], "bad_request");

    let mut nested_key = event("u1", "nested-key", "must use the batch key");
    nested_key["idempotency_key"] = json!("nested-event-key");
    let (status, rejected) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events:bulk",
        json!({ "events": [nested_key], "idempotency_key": "batch-key" }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    assert_eq!(rejected["error"]["code"], "bad_request", "{rejected}");

    for owner in ["u1", "u2"] {
        let (status, search) = call(
            app.clone(),
            Method::POST,
            &format!("/v1/history/users/{owner}/search"),
            json!({ "query": "batch", "limit": 10 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{search}");
        assert_eq!(search["hits"], json!([]), "{search}");
    }

    let usage = state
        .store
        .usage_snapshot(state.tenant_id(), None, true)
        .unwrap();
    assert_eq!(usage["providers"]["history_events"]["event_count"], 0);
    assert_eq!(
        usage["providers"]["history_events"]["user_event_index_count"],
        0
    );
    state.shutdown().await;
}

#[tokio::test]
async fn personal_ingest_applies_dynamic_index_settings_before_fragment_documents() {
    let (state, app, fault) = fault_app(HistorySideEffectFault {
        allow_personal_context: true,
        ..HistorySideEffectFault::default()
    })
    .await;
    let owner = "dynamic-ingest-owner";
    let routing = state
        .store
        .resolver()
        .resolve(state.tenant_id(), owner, false, true)
        .unwrap();

    let (status, response) = call(
        app,
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": owner,
            "source_id": "dynamic-ingest-source",
            "revision_id": "v1",
            "title": "Dynamic ingest",
            "content": "filterable fragment content"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let settings_request = format!(
        "PATCH /indexes/{}/settings",
        routing.personal_context_index_uid
    );
    let documents_request = format!(
        "POST /indexes/{}/documents",
        routing.personal_context_index_uid
    );
    {
        let requests = fault.request_log.lock().unwrap();
        let settings_position = requests
            .iter()
            .position(|request| request == &settings_request)
            .unwrap_or_else(|| panic!("dynamic context settings were never applied: {requests:?}"));
        let documents_position = requests
            .iter()
            .position(|request| request == &documents_request)
            .unwrap_or_else(|| panic!("personal fragments were never persisted: {requests:?}"));
        assert!(
            settings_position < documents_position,
            "personal fragment documents were submitted before dynamic settings: {requests:?}"
        );
    }
    state.shutdown().await;
}

#[tokio::test]
async fn ingest_idempotency_key_is_rejected_instead_of_silently_ignored() {
    let (state, app) = app();
    let (status, rejected) = call(
        app,
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": "u1",
            "source_id": "unsupported-ingest-key",
            "revision_id": "v1",
            "content": "the server must not pretend this key is effective",
            "idempotency_key": "unsupported-ingest-idempotency"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{rejected}");
    assert_eq!(rejected["error"]["code"], "bad_request", "{rejected}");
    assert!(
        state
            .store
            .usage_snapshot(state.tenant_id(), Some("u1"), false)
            .unwrap()["providers"]["ingest"]["task_count"]
            == 0,
        "rejected ingest idempotency key created a task"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn idempotent_retry_and_repeated_reconcile_do_not_duplicate_records() {
    let (state, app) = app();
    let mut request = event("u1", "stable-retry", "exactly once event");
    request["idempotency_key"] = json!("stable-retry-key");

    let (status, first) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");
    assert_eq!(first["duplicate"], false);
    let first_operation_id = first["persistence"]["operation_id"]
        .as_str()
        .expect("initial response includes its durable operation id")
        .to_string();
    assert_eq!(first["persistence"]["status"], "completed", "{first}");

    let (status, retry) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{retry}");
    assert_eq!(retry["duplicate"], true, "{retry}");
    assert_eq!(retry["event"], first["event"], "{retry}");
    assert_eq!(retry["routing"], first["routing"], "{retry}");
    assert_eq!(
        retry["materialization_job_id"], first["materialization_job_id"],
        "{retry}"
    );
    let retry_operation_id = retry["persistence"]["operation_id"]
        .as_str()
        .expect("retry response includes its durable operation id");
    assert_eq!(retry_operation_id, first_operation_id, "{retry}");
    assert_eq!(retry["persistence"]["status"], "completed", "{retry}");

    let mut mismatched = event("u1", "stable-retry", "different payload");
    mismatched["idempotency_key"] = json!("stable-retry-key");
    let (status, rejected) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        mismatched,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
    assert_eq!(rejected["error"]["code"], "conflict", "{rejected}");

    let (status, journal) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations/search",
        json!({
            "operation_kinds": ["history_event.append"],
            "include_plan": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{journal}");
    assert_eq!(
        journal["operations"].as_array().unwrap().len(),
        1,
        "{journal}"
    );
    let operation = &journal["operations"][0];
    assert_eq!(operation["status"], "completed", "{operation}");
    assert_eq!(operation["indexing_state"], "completed", "{operation}");
    assert!(operation.get("plan").is_none(), "{operation}");
    let operation_id = operation["id"].as_str().unwrap();
    assert_eq!(operation_id, first_operation_id, "{operation}");

    for _ in 0..2 {
        let (status, reconciled) = call(
            app.clone(),
            Method::POST,
            "/v1/admin/operations:reconcile",
            json!({ "operation_ids": [operation_id] }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{reconciled}");
        assert_eq!(reconciled["checked"], 1, "{reconciled}");
        assert_eq!(reconciled["reconciled"], 0, "{reconciled}");
        assert_eq!(reconciled["skipped"], 1, "{reconciled}");
        assert_eq!(reconciled["failed"], 0, "{reconciled}");
    }

    let (status, search) = call(
        app,
        Method::POST,
        "/v1/history/users/u1/search",
        json!({ "query": "exactly once", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"].as_array().unwrap().len(), 1, "{search}");
    assert_eq!(search["hits"][0]["id"], first["event"]["id"]);
    state.shutdown().await;
}

#[tokio::test]
async fn meili_operation_pages_filter_project_and_validate_the_cursor() {
    let (state, app, fault) = fault_app(HistorySideEffectFault {
        allow_personal_context: true,
        ..HistorySideEffectFault::default()
    })
    .await;

    for (entity_id, idempotency_key) in [
        ("paged-operation-a", "paged-operation-key-a"),
        ("paged-operation-b", "paged-operation-key-b"),
    ] {
        let mut request = event("u1", entity_id, "paged operation");
        request["idempotency_key"] = json!(idempotency_key);
        let (status, response) = call(
            app.clone(),
            Method::POST,
            "/v1/history/users/u1/events",
            request,
        )
        .await;
        assert!(status.is_success(), "{status}: {response}");
    }

    let list_request = json!({
        "operation_kinds": ["history_event.append"],
        "limit": 1,
        "include_plan": false
    });
    let (status, first_page) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations/search",
        list_request.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first_page}");
    assert_eq!(first_page["operations"].as_array().unwrap().len(), 1);
    assert!(first_page["operations"][0].get("plan").is_none());
    let cursor = first_page["next_cursor"]
        .as_str()
        .expect("the first bounded page has an opaque continuation cursor")
        .to_string();

    let first_fetch = fault
        .operation_fetch_requests
        .lock()
        .unwrap()
        .last()
        .cloned()
        .unwrap();
    assert_eq!(first_fetch["offset"], 0);
    assert_eq!(first_fetch["limit"], 1);
    assert!(first_fetch["filter"]
        .as_str()
        .is_some_and(|filter| filter.contains("tenant_id")
            && filter.contains("operation_kind")
            && filter.contains("history_event.append")));
    let summary_fields = first_fetch["fields"].as_array().unwrap();
    assert!(summary_fields.iter().any(|field| field == "progress"));
    assert!(!summary_fields.iter().any(|field| field == "plan"));

    let (status, second_page) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations/search",
        json!({
            "operation_kinds": ["history_event.append"],
            "limit": 1,
            "cursor": cursor,
            "include_plan": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second_page}");
    assert_eq!(second_page["operations"].as_array().unwrap().len(), 1);
    assert!(second_page["operations"][0].get("plan").is_none());
    assert!(second_page.get("next_cursor").is_none());
    assert_ne!(
        first_page["operations"][0]["id"],
        second_page["operations"][0]["id"]
    );

    let fetches = fault.operation_fetch_requests.lock().unwrap().clone();
    let cursor_check = &fetches[fetches.len() - 2];
    assert_eq!(cursor_check["offset"], 0);
    assert_eq!(cursor_check["limit"], 1);
    assert_eq!(
        cursor_check["fields"],
        json!(["id", "logical_id", "tenant_id"])
    );
    let second_fetch = fetches.last().unwrap();
    assert_eq!(second_fetch["offset"], 1);
    assert!(!second_fetch["fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "plan"));

    let fetch_count = fetches.len();
    let (status, invalid_cursor) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations/search",
        json!({
            "operation_kinds": ["different.kind"],
            "limit": 1,
            "cursor": first_page["next_cursor"]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid_cursor}");
    assert_eq!(
        fault.operation_fetch_requests.lock().unwrap().len(),
        fetch_count,
        "a cursor from another filter scope must be rejected before querying Meili"
    );

    let (status, with_plan) = call(
        app,
        Method::POST,
        "/v1/admin/operations/search",
        json!({
            "operation_kinds": ["history_event.append"],
            "limit": 1,
            "include_plan": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{with_plan}");
    assert!(with_plan["operations"][0]["plan"].is_object());
    let plan_fetch = fault
        .operation_fetch_requests
        .lock()
        .unwrap()
        .last()
        .cloned()
        .unwrap();
    assert!(plan_fetch["fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|field| field == "plan"));
    state.shutdown().await;
}

#[tokio::test]
async fn targeted_reconciliation_does_not_scan_retained_operation_history() {
    let (state, app, fault) = side_effect_fault_app().await;
    let mut request = event(
        "u1",
        "targeted-reconcile",
        "targeted reconciliation avoids retained history",
    );
    request["idempotency_key"] = json!("targeted-reconcile-key");

    let (status, partially_failed) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request,
    )
    .await;
    assert!(status.is_success(), "{status}: {partially_failed}");
    assert_eq!(
        partially_failed["persistence"]["status"], "partially_failed",
        "{partially_failed}"
    );
    let operation_id = partially_failed["persistence"]["operation_id"]
        .as_str()
        .expect("partial response includes its durable operation id")
        .to_string();

    fault
        .operation_fetch_total_override
        .store(100_001, Ordering::Relaxed);
    fault.operation_fetch_requests.lock().unwrap().clear();
    let (status, reconciled) = call(
        app,
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [operation_id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reconciled}");
    assert_eq!(reconciled["checked"], 1, "{reconciled}");
    assert_eq!(reconciled["completed"], 1, "{reconciled}");
    assert_eq!(reconciled["failed"], 0, "{reconciled}");
    assert!(
        fault.operation_fetch_requests.lock().unwrap().is_empty(),
        "targeted reconciliation must not use the bounded full-history fetch path"
    );
    assert!(
        fault
            .request_log
            .lock()
            .unwrap()
            .iter()
            .any(|request| request == "POST /indexes/rag_operations/search"),
        "targeted reconciliation should fetch the exact operation"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn untargeted_reconciliation_fetches_only_a_bounded_active_page() {
    let (state, app, fault) = side_effect_fault_app().await;
    let mut request = event(
        "u1",
        "bounded-active-reconcile",
        "bounded active reconciliation",
    );
    request["idempotency_key"] = json!("bounded-active-reconcile-key");

    let (status, partially_failed) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request,
    )
    .await;
    assert!(status.is_success(), "{status}: {partially_failed}");
    assert_eq!(
        partially_failed["persistence"]["status"], "partially_failed",
        "{partially_failed}"
    );

    fault
        .operation_fetch_total_override
        .store(100_001, Ordering::Relaxed);
    fault.operation_fetch_requests.lock().unwrap().clear();
    let (status, reconciled) = call(
        app,
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "limit": 1 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reconciled}");
    assert_eq!(reconciled["checked"], 1, "{reconciled}");
    assert_eq!(reconciled["completed"], 1, "{reconciled}");
    assert_eq!(reconciled["failed"], 0, "{reconciled}");

    {
        let fetches = fault.operation_fetch_requests.lock().unwrap();
        assert_eq!(fetches.len(), 1, "{fetches:?}");
        assert_eq!(fetches[0]["offset"], 0, "{}", fetches[0]);
        assert_eq!(fetches[0]["limit"], 1, "{}", fetches[0]);
        assert!(
            fetches[0]["filter"]
                .as_str()
                .is_some_and(|filter| filter.contains("status != \"completed\"")
                    && filter.contains("indexing_state != \"completed\"")),
            "{}",
            fetches[0]
        );
    }
    state.shutdown().await;
}

#[tokio::test]
async fn retry_reconciles_a_failed_history_side_effect_without_losing_the_committed_response() {
    let (state, app, fault) = side_effect_fault_app().await;
    let mut request = event("u1", "deploying", "deploying");
    request["idempotency_key"] = json!("partial-history-side-effect-key");

    let (status, partially_failed) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request.clone(),
    )
    .await;
    assert!(status.is_success(), "{status}: {partially_failed}");
    assert_eq!(partially_failed["duplicate"], false, "{partially_failed}");
    assert_eq!(
        partially_failed["persistence"]["status"], "partially_failed",
        "{partially_failed}"
    );
    assert_eq!(
        partially_failed["persistence"]["indexing_state"], "failed",
        "{partially_failed}"
    );
    let operation_id = partially_failed["persistence"]["operation_id"]
        .as_str()
        .expect("partial response includes its durable operation id")
        .to_string();
    let event_id = partially_failed["event"]["id"]
        .as_str()
        .expect("primary response includes the committed event id")
        .to_string();
    assert_eq!(fault.personal_context_attempts.load(Ordering::Relaxed), 1);

    let (status, reconciled) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request,
    )
    .await;
    assert!(status.is_success(), "{status}: {reconciled}");
    assert_eq!(reconciled["duplicate"], true, "{reconciled}");
    assert_eq!(reconciled["event"]["id"], event_id, "{reconciled}");
    assert_eq!(
        reconciled["persistence"]["operation_id"], operation_id,
        "{reconciled}"
    );
    assert_eq!(
        reconciled["persistence"]["status"], "completed",
        "{reconciled}"
    );
    assert_eq!(
        reconciled["persistence"]["indexing_state"], "completed",
        "{reconciled}"
    );
    assert_eq!(
        fault.personal_context_attempts.load(Ordering::Relaxed),
        2,
        "retry must replay only the failed personal-context step"
    );

    let (status, context) = call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": "u1",
            "query": "deployment",
            "limit": 10
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{context}");
    assert!(
        context["hits"].as_array().unwrap().iter().any(|hit| {
            hit["uri"]
                .as_str()
                .is_some_and(|uri| uri.contains(&event_id))
                && hit["snippet"]
                    .as_str()
                    .is_some_and(|snippet| snippet.contains("deploying"))
        }),
        "vector-only RYW overlay should recover the local `deploying` context for `deployment`: {context}"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn wait_for_index_history_retry_replays_the_same_partial_response_when_repair_still_fails() {
    let (state, app, fault) = fault_app_with_consistency(
        HistorySideEffectFault {
            fail_personal_context_always: true,
            ..HistorySideEffectFault::default()
        },
        WriteConsistency::WaitForIndex,
    )
    .await;
    let mut request = event("u1", "wfi-stable-partial", "stable partial response");
    request["idempotency_key"] = json!("wfi-stable-partial-key");

    let (status, first) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request.clone(),
    )
    .await;
    assert!(status.is_success(), "{status}: {first}");
    assert_eq!(first["duplicate"], false, "{first}");
    assert_eq!(
        first["persistence"]["status"], "partially_failed",
        "{first}"
    );

    let (status, replayed) = call(app, Method::POST, "/v1/history/users/u1/events", request).await;
    assert!(status.is_success(), "{status}: {replayed}");
    assert_eq!(replayed["duplicate"], true, "{replayed}");
    assert_eq!(replayed["event"], first["event"], "{replayed}");
    assert_eq!(
        replayed["persistence"]["operation_id"], first["persistence"]["operation_id"],
        "{replayed}"
    );
    assert_eq!(
        replayed["persistence"]["status"], "partially_failed",
        "{replayed}"
    );
    assert_eq!(fault.personal_context_attempts.load(Ordering::Relaxed), 2);
    state.shutdown().await;
}

#[tokio::test]
async fn retry_replays_an_accepted_history_response_when_primary_task_confirmation_fails() {
    let (state, app, fault) = task_confirmation_fault_app().await;
    let mut request = event(
        "u1",
        "primary-task-confirmation",
        "accepted before task confirmation",
    );
    request["idempotency_key"] = json!("primary-task-confirmation-key");

    let (status, accepted) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{accepted}");
    assert_eq!(accepted["duplicate"], false, "{accepted}");
    assert_eq!(accepted["persistence"]["status"], "completed", "{accepted}");
    assert_eq!(
        accepted["persistence"]["indexing_state"], "pending",
        "{accepted}"
    );
    let operation_id = accepted["persistence"]["operation_id"]
        .as_str()
        .expect("accepted response includes its durable operation id")
        .to_string();
    let primary_task_uid = accepted["persistence"]["primary_task_uids"][0]
        .as_str()
        .expect("accepted response includes its primary task UID")
        .to_string();
    assert_eq!(
        primary_task_uid.parse::<u64>().unwrap(),
        fault.event_primary_task_uid.load(Ordering::Relaxed)
    );
    assert_eq!(fault.event_confirmation_attempts.load(Ordering::Relaxed), 0);

    let (status, replayed) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        request.clone(),
    )
    .await;
    assert!(status.is_success(), "{status}: {replayed}");
    assert_eq!(replayed["duplicate"], true, "{replayed}");
    assert_eq!(replayed["event"], accepted["event"], "{replayed}");
    assert_eq!(replayed["routing"], accepted["routing"], "{replayed}");
    assert_eq!(
        replayed["materialization_job_id"], accepted["materialization_job_id"],
        "{replayed}"
    );
    assert_eq!(
        replayed["persistence"]["operation_id"], operation_id,
        "{replayed}"
    );
    assert_eq!(replayed["persistence"]["status"], "failed", "{replayed}");
    assert_eq!(
        replayed["persistence"]["indexing_state"], "failed",
        "{replayed}"
    );
    assert_eq!(replayed["meili_task_uid"], primary_task_uid, "{replayed}");
    assert_eq!(fault.event_confirmation_attempts.load(Ordering::Relaxed), 1);

    let (status, replayed_again) =
        call(app, Method::POST, "/v1/history/users/u1/events", request).await;
    assert!(status.is_success(), "{status}: {replayed_again}");
    assert_eq!(replayed_again["duplicate"], true, "{replayed_again}");
    assert_eq!(replayed_again["event"], accepted["event"]);
    assert_eq!(replayed_again["persistence"]["operation_id"], operation_id);
    assert_eq!(replayed_again["persistence"]["status"], "failed");
    assert_eq!(
        fault.event_confirmation_attempts.load(Ordering::Relaxed),
        2,
        "each retry may confirm the retained task again but must keep replaying the original response"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn wait_for_index_rejects_a_definitively_failed_primary_task() {
    let fault = HistorySideEffectFault {
        allow_personal_context: true,
        fail_event_task_confirmation: true,
        ..HistorySideEffectFault::default()
    };
    let (state, app, fault) =
        fault_app_with_consistency(fault, WriteConsistency::WaitForIndex).await;
    let request = event(
        "u1",
        "wait-for-index-confirmation",
        "accepted before failed index confirmation",
    );

    let (status, failed) = call(app, Method::POST, "/v1/history/users/u1/events", request).await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{failed}");
    assert_eq!(failed["error"]["code"], "upstream_error", "{failed}");
    assert!(failed.get("event").is_none(), "{failed}");
    let task_uid = fault.event_primary_task_uid.load(Ordering::Relaxed);
    assert_eq!(fault.event_confirmation_attempts.load(Ordering::Relaxed), 1);
    {
        let journal = fault.operation_journal.lock().unwrap();
        let operation = journal
            .values()
            .find(|document| document["operation_kind"] == "history_event.append")
            .expect("failed primary confirmation must remain discoverable in the journal");
        assert_eq!(operation["status"], "failed", "{operation}");
        assert_eq!(operation["indexing_state"], "failed", "{operation}");
        assert_eq!(
            operation["progress"]["steps"]["primary"]["task_uids"][0],
            task_uid.to_string(),
            "the failure checkpoint must retain the submitted Meilisearch task UID"
        );
    }
    state.shutdown().await;
}

#[tokio::test]
async fn wait_for_index_does_not_publish_state_before_all_side_effects_complete() {
    let (state, app, fault) = fault_app_with_consistency(
        HistorySideEffectFault::default(),
        WriteConsistency::WaitForIndex,
    )
    .await;
    let fact_key = "wait-for-index-publication-barrier";

    let (status, failed) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "title": "Wait-for-index publication barrier",
            "statement": "The live cache must remain unchanged until every side effect succeeds.",
            "source_refs": [{ "kind": "test", "id": fact_key }],
            "idempotency_key": "wait-for-index-publication-barrier-key"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{failed}");
    assert_eq!(failed["error"]["code"], "upstream_error", "{failed}");
    assert_eq!(fault.state_primary_attempts.load(Ordering::Relaxed), 1);
    assert_eq!(fault.personal_context_attempts.load(Ordering::Relaxed), 1);

    let (status, absent) = call(
        app,
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}?owner_user_id=u1"),
        Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "a failed wait-for-index composite write leaked its primary cache projection: {absent}"
    );
    assert_eq!(absent["error"]["code"], "not_found", "{absent}");
    state.shutdown().await;
}

#[tokio::test]
async fn composite_state_side_effect_failure_is_an_error_with_a_retryable_journal_record() {
    let (state, app, fault) = side_effect_fault_app().await;

    let (status, failed) = call(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/composite-side-effect-failure",
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "title": "Composite side-effect failure",
            "statement": "The primary state item commits before personal context indexing fails.",
            "source_refs": [{ "kind": "test", "id": "composite-side-effect-failure" }],
            "idempotency_key": "composite-side-effect-failure-key"
        }),
    )
    .await;
    assert!(!status.is_success(), "{status}: {failed}");
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{failed}");
    assert_eq!(failed["error"]["code"], "upstream_error", "{failed}");
    assert_eq!(
        failed["error"]["message"], "upstream service unavailable",
        "{failed}"
    );
    assert!(
        !failed.to_string().contains("injected personal-context"),
        "{failed}"
    );
    assert!(!failed.to_string().contains("rag_context__"), "{failed}");
    assert_eq!(fault.personal_context_attempts.load(Ordering::Relaxed), 1);

    let (status, operations) = call(
        app,
        Method::POST,
        "/v1/admin/operations/search",
        json!({ "operation_kinds": ["state_fact.upsert"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{operations}");
    let operations = operations["operations"].as_array().unwrap();
    assert_eq!(operations.len(), 1, "{operations:?}");
    let operation = &operations[0];
    assert_eq!(operation["status"], "partially_failed", "{operation}");
    assert_eq!(operation["indexing_state"], "failed", "{operation}");
    assert_eq!(operation["failed_steps"], 1, "{operation}");
    assert!(
        operation["attempt_count"].as_u64().unwrap() >= 2,
        "{operation}"
    );
    assert!(
        operation["idempotency_key_hash"].as_str().is_some(),
        "{operation}"
    );
    assert!(operation.get("plan").is_none(), "{operation}");
    state.shutdown().await;
}

#[tokio::test]
async fn newer_state_update_waits_for_partial_predecessor_reconciliation() {
    let (state, app, fault) = side_effect_fault_app().await;
    let fact_key = "state-generation-guard";

    let (status, failed) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "statement": "version one must finish before version two",
            "idempotency_key": "state-generation-v1"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{failed}");
    assert_eq!(fault.personal_context_attempts.load(Ordering::Relaxed), 1);

    let operation_id = fault
        .operation_journal
        .lock()
        .unwrap()
        .values()
        .find(|operation| operation["operation_kind"] == "state_fact.upsert")
        .and_then(|operation| operation["logical_id"].as_str())
        .expect("the partial state update remains journaled")
        .to_string();

    let (status, blocked) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "statement": "version two must not race replayed version one"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked}");
    assert_eq!(blocked["error"]["code"], "conflict", "{blocked}");
    assert_eq!(
        fault.personal_context_attempts.load(Ordering::Relaxed),
        1,
        "the newer update must be rejected before submitting another context write"
    );

    let (status, reconciled) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [operation_id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reconciled}");
    assert_eq!(reconciled["reconciled"], 1, "{reconciled}");

    let (status, updated) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "statement": "version two is now the current state"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["item"]["current_version"], 2, "{updated}");

    let (status, replayed_old) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [operation_id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replayed_old}");
    assert_eq!(replayed_old["reconciled"], 0, "{replayed_old}");
    assert_eq!(replayed_old["skipped"], 1, "{replayed_old}");

    let (status, current) = call(
        app,
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}?owner_user_id=u1"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    assert_eq!(
        current["item"]["statement"], "version two is now the current state",
        "a repeated predecessor reconcile must not overwrite the newer projection"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn failed_state_primary_blocks_a_type_change_until_the_prior_operation_is_reconciled() {
    let (state, app, fault) = fault_app(HistorySideEffectFault {
        fail_state_primary_once: Arc::new(AtomicBool::new(true)),
        allow_personal_context: true,
        ..HistorySideEffectFault::default()
    })
    .await;
    let fact_key = "failed-primary-type-guard";

    let (status, failed) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "profile",
            "statement": "the first state generation remains journaled",
            "idempotency_key": "failed-primary-type-guard-v1"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_GATEWAY, "{failed}");
    assert_eq!(fault.state_primary_attempts.load(Ordering::Relaxed), 1);

    let (status, blocked) = call(
        app,
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "statement": "a different type must not bypass the failed generation"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked}");
    assert_eq!(blocked["error"]["code"], "conflict", "{blocked}");
    assert_eq!(
        fault.state_primary_attempts.load(Ordering::Relaxed),
        1,
        "the type-changing request must be rejected before another primary write"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn immediate_state_document_update_after_restart_supersedes_the_read_through_aggregate() {
    let fault = HistorySideEffectFault {
        allow_personal_context: true,
        persist_documents: true,
        ..HistorySideEffectFault::default()
    };
    let (state, app, fault) = fault_app(fault).await;
    let config = state.config.clone();
    let tenant_id = state.tenant_id().to_string();
    let owner = "restart-state-owner";
    let fact_key = "restart-state-fact";
    let old_marker = "restart-state-old-marker";
    let new_marker = "restart-state-new-marker";

    let (status, created) = call(
        app,
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "preference",
            "statement": "the old state statement",
            "document": {
                "content": format!("{old_marker} persisted source body. ").repeat(20),
                "content_type": "text/plain"
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    let old_source_document_uri = created["item"]["source_refs"][0]["meta"]["source_document_uri"]
        .as_str()
        .expect("v1 response includes its source document uri")
        .to_string();
    state.shutdown().await;

    let fresh_state = AppState::new(config);
    let hydration = fresh_state
        .store
        .hydrate_from_repository(&tenant_id)
        .await
        .expect("fresh state hydrates its startup aggregates");
    assert_eq!(hydration["ready"], true, "{hydration}");
    let fresh_app = build_router(fresh_state.clone());
    fault.request_log.lock().unwrap().clear();
    fault.freeze_domain_writes.store(true, Ordering::Relaxed);

    // This is deliberately the first owner-context action after hydration: no
    // read endpoint primes personal context or source documents before v2.
    let (status, updated) = call(
        fresh_app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "preference",
            "statement": "the new state statement",
            "document": {
                "content": format!("{new_marker} replacement source body. ").repeat(20),
                "content_type": "text/plain"
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["item"]["current_version"], 2, "{updated}");
    let new_source_document_uri = updated["item"]["source_refs"][0]["meta"]["source_document_uri"]
        .as_str()
        .expect("v2 response includes its source document uri")
        .to_string();
    assert_ne!(old_source_document_uri, new_source_document_uri);

    let routing = fresh_state
        .store
        .resolver()
        .resolve(&tenant_id, owner, false, true)
        .unwrap();
    let requests = fault.request_log.lock().unwrap().clone();
    let context_read = requests
        .iter()
        .position(|request| {
            request
                == &format!(
                    "POST /indexes/{}/documents/fetch",
                    routing.personal_context_index_uid
                )
        })
        .unwrap_or_else(|| {
            panic!("state update did not read through personal context: {requests:?}")
        });
    let source_read = requests
        .iter()
        .position(|request| request == "POST /indexes/rag_source_documents/search")
        .unwrap_or_else(|| {
            panic!("state update did not read through source documents: {requests:?}")
        });
    let journal_write = requests
        .iter()
        .position(|request| request == "POST /indexes/rag_operations/documents")
        .unwrap_or_else(|| {
            panic!("state update did not journal its staged mutation: {requests:?}")
        });
    assert!(context_read < journal_write, "{requests:?}");
    assert!(source_read < journal_write, "{requests:?}");
    drop(requests);

    let backend_v1_is_still_active = fault
        .documents_by_index
        .lock()
        .unwrap()
        .get(&routing.personal_context_index_uid)
        .into_iter()
        .flat_map(|documents| documents.values())
        .any(|document| {
            document["revision_id"] == "v1"
                && document["status"] == "active"
                && document["source_document_uri"] == old_source_document_uri
        });
    assert!(
        backend_v1_is_still_active,
        "the fixture must keep v1 stale so the RYW overlay is actually exercised"
    );

    let (status, old_search) = call(
        fresh_app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "owner_user_id": owner, "query": old_marker, "limit": 20 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_search}");
    assert!(
        old_search["hits"].as_array().unwrap().iter().all(|hit| {
            hit["source_document_uri"].as_str() != Some(old_source_document_uri.as_str())
        }),
        "a stale repository v1 row leaked through the RYW merge: {old_search}"
    );

    let (status, new_search) = call(
        fresh_app,
        Method::POST,
        "/v1/context/search",
        json!({ "owner_user_id": owner, "query": new_marker, "limit": 20 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{new_search}");
    assert!(
        new_search["hits"].as_array().unwrap().iter().any(|hit| {
            hit["source_document_uri"].as_str() == Some(new_source_document_uri.as_str())
        }),
        "the local v2 document was not visible through RYW: {new_search}"
    );
    fresh_state.shutdown().await;
}

#[tokio::test]
async fn state_type_is_immutable_for_an_existing_fact() {
    let (state, app) = app();
    let fact_key = "immutable-state-type";
    let (status, created) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "statement": "the original aggregate remains authoritative",
            "document": { "content": "original preference document" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");

    let (status, rejected) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u1",
            "state_type": "status",
            "statement": "must not split the physical aggregate",
            "document": { "content": "conflicting status document" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
    assert_eq!(rejected["error"]["code"], "conflict", "{rejected}");

    let (status, current) = call(
        app,
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}?owner_user_id=u1"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    assert_eq!(current["item"]["state_type"], "preference", "{current}");
    assert_eq!(current["item"]["current_version"], 1, "{current}");
    state.shutdown().await;
}

#[tokio::test]
async fn canonical_state_path_collision_is_rejected_before_writing() {
    let (state, app) = app();
    let (status, created) = call(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/collision%20key",
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "statement": "space key owns the canonical path",
            "document": { "content": "space-key document" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");

    let (status, rejected) = call(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/collision-key",
        json!({
            "owner_user_id": "u1",
            "state_type": "preference",
            "statement": "hyphen key must not alias the space key",
            "document": { "content": "hyphen-key document" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
    assert_eq!(rejected["error"]["code"], "conflict", "{rejected}");

    let (status, missing) = call(
        app,
        Method::GET,
        "/v1/state/profile/facts/collision-key?owner_user_id=u1",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    state.shutdown().await;
}

#[tokio::test]
async fn state_idempotency_key_is_bound_to_target_and_payload() {
    let (state, app) = app();
    let first_request = json!({
        "owner_user_id": "u1",
        "state_type": "preference",
        "statement": "first payload",
        "idempotency_key": "state-request-binding"
    });
    let (status, created) = call(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/fact-a",
        first_request.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");

    let (status, replayed) = call(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/fact-a",
        first_request,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{replayed}");
    assert_eq!(replayed["item"]["natural_key"], "fact-a", "{replayed}");

    for (path, statement) in [
        ("/v1/state/profile/facts/fact-b", "first payload"),
        ("/v1/state/profile/facts/fact-a", "different payload"),
    ] {
        let (status, rejected) = call(
            app.clone(),
            Method::PUT,
            path,
            json!({
                "owner_user_id": "u1",
                "state_type": "preference",
                "statement": statement,
                "idempotency_key": "state-request-binding"
            }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{rejected}");
        assert_eq!(rejected["error"]["code"], "conflict", "{rejected}");
    }

    let (status, missing) = call(
        app,
        Method::GET,
        "/v1/state/profile/facts/fact-b?owner_user_id=u1",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    state.shutdown().await;
}

#[tokio::test]
async fn identical_idempotency_keys_are_scoped_by_owner() {
    let (state, app) = app();
    for owner in ["u1", "u2"] {
        let mut request = event(owner, &format!("entity-{owner}"), "owner scoped event");
        request["idempotency_key"] = json!("shared-client-key");
        let (status, response) = call(
            app.clone(),
            Method::POST,
            &format!("/v1/history/users/{owner}/events"),
            request,
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{response}");
        assert_eq!(response["duplicate"], false, "{response}");
    }

    let (status, journal) = call(
        app,
        Method::POST,
        "/v1/admin/operations/search",
        json!({ "operation_kinds": ["history_event.append"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{journal}");
    assert_eq!(
        journal["operations"].as_array().unwrap().len(),
        2,
        "{journal}"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn company_delete_immediately_removes_source_document_and_context_surfaces() {
    let (state, app) = app();
    let source_id = "delete-visibility-regression";
    let marker = "company-delete-visibility-marker";

    let (status, revision) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Delete visibility regression",
            "source_uri": "https://example.test/delete-visibility-regression",
            "content": format!("{marker} is present in the active company document. ").repeat(40),
            "checksum": "delete-visibility-regression-v1"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{revision}");
    let revision_id = revision["revision_id"].as_str().unwrap();

    let (status, activated) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": "delete visibility regression" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{activated}");
    let source_document_uri = activated["source_document_uri"].as_str().unwrap();
    let context_uris = activated["context_uris"]
        .as_array()
        .unwrap()
        .iter()
        .map(|uri| uri.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert!(!context_uris.is_empty(), "{activated}");

    let (status, source_document) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(source_document_uri)),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source_document}");
    assert_eq!(source_document["source_id"], source_id);

    let (status, before_search) = call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker, "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{before_search}");
    assert!(
        before_search["hits"]
            .as_array()
            .unwrap()
            .iter()
            .any(|hit| hit["source_id"] == source_id),
        "{before_search}"
    );

    let (status, deleted) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["deleted"], true, "{deleted}");

    let (status, missing_source) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing_source}");

    let (status, missing_document) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(source_document_uri)),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing_document}");

    for uri in context_uris {
        let (status, missing_context) = call(
            app.clone(),
            Method::GET,
            &format!("/v1/fs/read?uri={}", query_encode(&uri)),
            Value::Null,
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}: {missing_context}");
    }

    let (status, after_search) = call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker, "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{after_search}");
    assert!(
        after_search["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|hit| hit["source_id"] != source_id),
        "{after_search}"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn unchanged_index_settings_are_reapplied_only_when_explicitly_requested() {
    let (state, app) = app();
    let owner = "settings-journal-owner";
    let index_uri = format!("/v1/history/users/{owner}/event-index");

    let (status, created) = call(app.clone(), Method::PUT, &index_uri, json!({})).await;
    assert_eq!(status, StatusCode::OK, "{created}");

    let (status, forced) = call(
        app.clone(),
        Method::PUT,
        &index_uri,
        json!({ "force_reapply_settings": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{forced}");

    let (status, force_operations) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations/search",
        json!({ "operation_kinds": ["user_event_index.settings_reapply"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{force_operations}");
    let force_operations = force_operations["operations"].as_array().unwrap();
    assert_eq!(force_operations.len(), 1, "{force_operations:?}");
    assert_eq!(force_operations[0]["status"], "completed");
    assert_eq!(force_operations[0]["indexing_state"], "completed");

    let (status, unchanged) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/history/user-event-indexes:reconcile",
        json!({
            "owner_user_ids": [owner],
            "reapply_settings": false
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{unchanged}");
    assert_eq!(unchanged["updated_settings"], 0, "{unchanged}");

    let (status, no_reapply_operations) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations/search",
        json!({ "operation_kinds": ["user_event_indexes.settings_reapply"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{no_reapply_operations}");
    assert_eq!(
        no_reapply_operations["operations"]
            .as_array()
            .unwrap()
            .len(),
        0,
        "{no_reapply_operations}"
    );

    let (status, reapplied) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/history/user-event-indexes:reconcile",
        json!({
            "owner_user_ids": [owner],
            "reapply_settings": true
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reapplied}");
    assert_eq!(reapplied["updated_settings"], 1, "{reapplied}");

    let (status, reapply_operations) = call(
        app,
        Method::POST,
        "/v1/admin/operations/search",
        json!({ "operation_kinds": ["user_event_indexes.settings_reapply"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reapply_operations}");
    let reapply_operations = reapply_operations["operations"].as_array().unwrap();
    assert_eq!(reapply_operations.len(), 1, "{reapply_operations:?}");
    assert_eq!(reapply_operations[0]["status"], "completed");
    assert_eq!(reapply_operations[0]["indexing_state"], "completed");
    state.shutdown().await;
}
