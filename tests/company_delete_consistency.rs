use std::{
    cmp::Ordering as CmpOrdering,
    collections::{BTreeMap, HashMap, HashSet},
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
    build_router,
    config::WriteConsistency,
    models::{ActivateRevisionRequest, CreateRevisionRequest, LinkUpsertRequest},
    tenant_scope::tenant_document,
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

const REQUIRED_DELETE_INDEXES: [&str; 8] = [
    "rag_company_context",
    "rag_source_revisions",
    "rag_sources",
    "rag_source_documents",
    "rag_parse_artifacts",
    "rag_ingest_tasks",
    "rag_ingest_results",
    "rag_links",
];

#[derive(Clone, Default)]
struct CompanyDeleteFault {
    next_task_uid: Arc<AtomicU64>,
    operation_journal: Arc<Mutex<Vec<Value>>>,
    delete_attempts: Arc<Mutex<Vec<String>>>,
    delete_filters: Arc<Mutex<Vec<(String, String)>>>,
    accepted_deletes: Arc<Mutex<Vec<(String, String)>>>,
    documents: Arc<Mutex<HashMap<String, BTreeMap<String, Value>>>>,
    missing_index: Option<String>,
    fail_once_index: Arc<Mutex<Option<String>>>,
    fail_once_write_index: Arc<Mutex<Option<String>>>,
    fail_completed_delete_checkpoint_once: Arc<AtomicBool>,
    failed_task_uid: Arc<AtomicU64>,
}

impl CompanyDeleteFault {
    fn accepted(&self) -> Response {
        let task_uid = self.next_task_uid.fetch_add(1, Ordering::Relaxed) + 1;
        (StatusCode::ACCEPTED, Json(json!({ "taskUid": task_uid }))).into_response()
    }

    fn latest_operation(&self, operation_id: &str) -> Value {
        self.operation_journal
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find(|operation| operation["logical_id"] == operation_id)
            .cloned()
            .unwrap_or_else(|| panic!("operation {operation_id} was not journaled"))
    }
}

fn operation_logical_id(operation: &Value) -> Option<&str> {
    operation["logical_id"]
        .as_str()
        .or_else(|| operation["id"].as_str())
}

fn filter_value_after(filter: &str, marker: &str) -> Option<Value> {
    let remainder = filter.split_once(marker)?.1.trim_start();
    serde_json::Deserializer::from_str(remainder)
        .into_iter::<Value>()
        .next()?
        .ok()
}

fn filter_string_after(filter: &str, marker: &str) -> Option<String> {
    filter_value_after(filter, marker)?
        .as_str()
        .map(ToString::to_string)
}

fn filter_strings_after(filter: &str, marker: &str) -> Option<HashSet<String>> {
    filter_value_after(filter, marker)?
        .as_array()?
        .iter()
        .map(|value| value.as_str().map(ToString::to_string))
        .collect::<Option<HashSet<_>>>()
}

fn operation_matches_filter(operation: &Value, filter: &str) -> bool {
    if filter_string_after(filter, "tenant_id = ")
        .is_some_and(|tenant_id| operation["tenant_id"] != tenant_id)
    {
        return false;
    }

    let logical_ids = filter_strings_after(filter, "logical_id IN ");
    let logical_id = filter_string_after(filter, "logical_id = ");
    if logical_ids
        .as_ref()
        .is_some_and(|ids| operation_logical_id(operation).is_none_or(|id| !ids.contains(id)))
        || logical_id
            .as_deref()
            .is_some_and(|expected| operation_logical_id(operation) != Some(expected))
    {
        return false;
    }

    let statuses = filter_strings_after(filter, "status IN ");
    let status = filter_string_after(filter, "status = ");
    if statuses.as_ref().is_some_and(|statuses| {
        operation["status"]
            .as_str()
            .is_none_or(|status| !statuses.contains(status))
    }) || status
        .as_deref()
        .is_some_and(|expected| operation["status"] != expected)
    {
        return false;
    }

    if filter.contains("status != \"completed\"")
        && filter.contains("indexing_state != \"completed\"")
        && operation["status"] == "completed"
        && operation["indexing_state"] == "completed"
    {
        return false;
    }

    true
}

fn compare_operation_field(left: &Value, right: &Value, field: &str) -> CmpOrdering {
    match (left[field].as_str(), right[field].as_str()) {
        (Some(left), Some(right)) => left.cmp(right),
        (None, Some(_)) => CmpOrdering::Less,
        (Some(_), None) => CmpOrdering::Greater,
        (None, None) => CmpOrdering::Equal,
    }
}

fn sort_operations(operations: &mut [Value], sort: Option<&Vec<Value>>) {
    let Some(sort) = sort else {
        return;
    };
    operations.sort_by(|left, right| {
        for rule in sort.iter().filter_map(Value::as_str) {
            let (field, direction) = rule.rsplit_once(':').unwrap_or((rule, "asc"));
            let ordering = compare_operation_field(left, right, field);
            let ordering = if direction == "desc" {
                ordering.reverse()
            } else {
                ordering
            };
            if ordering != CmpOrdering::Equal {
                return ordering;
            }
        }
        CmpOrdering::Equal
    });
}

fn latest_filtered_operations(fault: &CompanyDeleteFault, request: &Value) -> Vec<Value> {
    let filter = request["filter"].as_str().unwrap_or_default();
    let mut seen = HashSet::new();
    let mut operations = fault
        .operation_journal
        .lock()
        .unwrap()
        .iter()
        .rev()
        .filter(|operation| {
            operation_logical_id(operation).is_some_and(|id| seen.insert(id.to_string()))
        })
        .cloned()
        .collect::<Vec<_>>();
    operations.reverse();
    operations.retain(|operation| operation_matches_filter(operation, filter));
    sort_operations(&mut operations, request["sort"].as_array());
    operations
}

async fn company_delete_meili(
    State(fault): State<CompanyDeleteFault>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    if method == Method::GET && path.ends_with("/settings") {
        return (StatusCode::OK, Json(json!({}))).into_response();
    }
    if method == Method::GET && path.starts_with("/indexes/") {
        let uid = path.trim_start_matches("/indexes/");
        return (
            StatusCode::OK,
            Json(json!({ "uid": uid, "primaryKey": "id" })),
        )
            .into_response();
    }
    if method == Method::GET && path.starts_with("/tasks/") {
        let task_uid = path.trim_start_matches("/tasks/").parse::<u64>().unwrap();
        if task_uid == fault.failed_task_uid.load(Ordering::Relaxed) {
            return (
                StatusCode::OK,
                Json(json!({
                    "status": "failed",
                    "error": { "message": "injected operation checkpoint failure" }
                })),
            )
                .into_response();
        }
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }
    if method == Method::POST && path == "/indexes/rag_operations/search" {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let query: Value = serde_json::from_slice(&bytes).unwrap();
        let limit = query["limit"].as_u64().unwrap_or(20) as usize;
        let operations = latest_filtered_operations(&fault, &query)
            .into_iter()
            .take(limit)
            .collect::<Vec<_>>();
        return (
            StatusCode::OK,
            Json(json!({ "hits": operations, "processingTimeMs": 0 })),
        )
            .into_response();
    }
    if method == Method::POST && path.ends_with("/search") {
        let index_uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/search"))
            .unwrap();
        let hits = fault
            .documents
            .lock()
            .unwrap()
            .get(index_uid)
            .map(|documents| documents.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
        return (
            StatusCode::OK,
            Json(json!({ "hits": hits, "processingTimeMs": 0 })),
        )
            .into_response();
    }
    if method == Method::POST && path == "/indexes/rag_operations/documents/fetch" {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let offset = request["offset"].as_u64().unwrap_or_default() as usize;
        let limit = request["limit"].as_u64().unwrap_or(20) as usize;
        let operations = latest_filtered_operations(&fault, &request);
        let total = operations.len();
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
            .unwrap();
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let offset = request["offset"].as_u64().unwrap_or_default() as usize;
        let limit = request["limit"].as_u64().unwrap_or(20) as usize;
        let documents = fault
            .documents
            .lock()
            .unwrap()
            .get(index_uid)
            .map(|documents| documents.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();
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
        let fail_checkpoint = fault
            .fail_completed_delete_checkpoint_once
            .load(Ordering::Relaxed)
            && documents.iter().any(|document| {
                document["operation_kind"] == "company_doc.delete"
                    && document["status"] == "completed"
                    && document["indexing_state"] == "completed"
            });
        if fail_checkpoint
            && fault
                .fail_completed_delete_checkpoint_once
                .swap(false, Ordering::Relaxed)
        {
            let task_uid = fault.next_task_uid.fetch_add(1, Ordering::Relaxed) + 1;
            fault.failed_task_uid.store(task_uid, Ordering::Relaxed);
            return (StatusCode::ACCEPTED, Json(json!({ "taskUid": task_uid }))).into_response();
        }
        fault.operation_journal.lock().unwrap().extend(documents);
        return fault.accepted();
    }
    if method == Method::POST {
        if let Some(index_uid) = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/documents/delete"))
        {
            let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
            let delete: Value = serde_json::from_slice(&bytes).unwrap();
            fault.delete_filters.lock().unwrap().push((
                index_uid.to_string(),
                delete["filter"].as_str().unwrap_or_default().to_string(),
            ));
            fault
                .delete_attempts
                .lock()
                .unwrap()
                .push(index_uid.to_string());
            if fault.missing_index.as_deref() == Some(index_uid) {
                return (
                    StatusCode::NOT_FOUND,
                    Json(json!({ "message": "missing index" })),
                )
                    .into_response();
            }
            let fail_once = {
                let mut configured = fault.fail_once_index.lock().unwrap();
                if configured.as_deref() == Some(index_uid) {
                    configured.take();
                    true
                } else {
                    false
                }
            };
            if fail_once {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "message": "injected later deletion failure" })),
                )
                    .into_response();
            }
            let task_uid = fault.next_task_uid.fetch_add(1, Ordering::Relaxed) + 1;
            fault
                .accepted_deletes
                .lock()
                .unwrap()
                .push((index_uid.to_string(), task_uid.to_string()));
            fault
                .documents
                .lock()
                .unwrap()
                .entry(index_uid.to_string())
                .or_default()
                .clear();
            return (StatusCode::ACCEPTED, Json(json!({ "taskUid": task_uid }))).into_response();
        }
    }
    if method == Method::POST && path.ends_with("/documents") {
        let index_uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/documents"))
            .unwrap();
        let fail_once = {
            let mut configured = fault.fail_once_write_index.lock().unwrap();
            if configured.as_deref() == Some(index_uid) {
                configured.take();
                true
            } else {
                false
            }
        };
        if fail_once {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": "injected operation write failure" })),
            )
                .into_response();
        }
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let documents: Vec<Value> = serde_json::from_slice(&bytes).unwrap();
        let mut persisted = fault.documents.lock().unwrap();
        let index = persisted.entry(index_uid.to_string()).or_default();
        for document in documents {
            let id = document["id"].as_str().unwrap().to_string();
            index.insert(id, document);
        }
        return fault.accepted();
    }
    if method == Method::PATCH && path.ends_with("/settings") {
        return fault.accepted();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "message": format!("unexpected company-delete stub request: {method} {path}")
        })),
    )
        .into_response()
}

async fn company_delete_app(fault: CompanyDeleteFault) -> (AppState, Router, CompanyDeleteFault) {
    let meili = Router::new()
        .fallback(company_delete_meili)
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
    config.write_consistency = WriteConsistency::ReadYourWrites;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());
    (state, app, fault)
}

fn seed_company_source(state: &AppState, source_id: &str) {
    state
        .store
        .create_revision(
            state.tenant_id(),
            source_id,
            CreateRevisionRequest {
                title: Some("Company delete consistency".to_string()),
                content: Some("content retained until every delete step commits".to_string()),
                ingest: false,
                ..CreateRevisionRequest::default()
            },
        )
        .unwrap();
}

fn seed_active_company_source(state: &AppState, source_id: &str) -> String {
    let revision = state
        .store
        .create_revision(
            state.tenant_id(),
            source_id,
            CreateRevisionRequest {
                title: Some("Active company delete consistency".to_string()),
                content: Some("active content retained until its delete step commits".to_string()),
                ingest: false,
                ..CreateRevisionRequest::default()
            },
        )
        .unwrap();
    state
        .store
        .activate_revision(
            state.tenant_id(),
            source_id,
            &revision.revision_id,
            ActivateRevisionRequest::default(),
        )
        .unwrap()
        .source_document_uri
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

#[tokio::test]
async fn operation_stub_filters_latest_checkpoints_before_bounding_recovery() {
    let fault = CompanyDeleteFault::default();
    fault.operation_journal.lock().unwrap().extend([
        json!({
            "id": "physical-completed-old",
            "logical_id": "operation-completed",
            "tenant_id": "test-tenant",
            "status": "pending",
            "indexing_state": "pending",
            "created_at": "2026-07-14T00:00:00Z"
        }),
        json!({
            "id": "physical-completed-latest",
            "logical_id": "operation-completed",
            "tenant_id": "test-tenant",
            "status": "completed",
            "indexing_state": "completed",
            "created_at": "2026-07-14T00:00:00Z"
        }),
        json!({
            "id": "physical-older",
            "logical_id": "operation-older",
            "tenant_id": "test-tenant",
            "status": "pending",
            "indexing_state": "pending",
            "created_at": "2026-07-14T01:00:00Z"
        }),
        json!({
            "id": "physical-newer",
            "logical_id": "operation-newer",
            "tenant_id": "test-tenant",
            "status": "pending",
            "indexing_state": "pending",
            "created_at": "2026-07-14T02:00:00Z"
        }),
        json!({
            "id": "physical-other-tenant",
            "logical_id": "operation-other-tenant",
            "tenant_id": "other-tenant",
            "status": "pending",
            "indexing_state": "pending",
            "created_at": "2026-07-14T03:00:00Z"
        }),
        json!({
            "id": "physical-failed",
            "logical_id": "operation-failed",
            "tenant_id": "test-tenant",
            "status": "failed",
            "indexing_state": "failed",
            "created_at": "2026-07-14T04:00:00Z"
        }),
    ]);
    let query = json!({
        "filter": "tenant_id = \"test-tenant\" AND (status != \"completed\" OR indexing_state != \"completed\") AND status IN [\"pending\"]",
        "sort": ["created_at:desc", "id:asc"],
        "limit": 1
    });
    let response = company_delete_meili(
        State(fault.clone()),
        Request::builder()
            .method(Method::POST)
            .uri("/indexes/rag_operations/documents/fetch")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(query.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let page: Value = serde_json::from_slice(&body).unwrap();

    assert_eq!(page["total"], 2, "{page}");
    assert_eq!(page["results"].as_array().unwrap().len(), 1, "{page}");
    assert_eq!(page["results"][0]["logical_id"], "operation-newer");

    let search = json!({
        "filter": "tenant_id = \"test-tenant\" AND (logical_id IN [\"operation-older\",\"operation-newer\",\"operation-failed\"] OR ((logical_id NOT EXISTS OR logical_id IS NULL) AND id IN [\"operation-older\",\"operation-newer\",\"operation-failed\"])) AND status IN [\"pending\"]",
        "sort": ["created_at:asc", "id:asc"],
        "limit": 1
    });
    let response = company_delete_meili(
        State(fault),
        Request::builder()
            .method(Method::POST)
            .uri("/indexes/rag_operations/search")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(search.to_string()))
            .unwrap(),
    )
    .await;
    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let search: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(search["hits"].as_array().unwrap().len(), 1, "{search}");
    assert_eq!(search["hits"][0]["logical_id"], "operation-older");
}

#[tokio::test]
async fn restart_delete_removes_links_without_source_document_pre_read() {
    let (state, app, fault) = company_delete_app(CompanyDeleteFault::default()).await;
    let source_id = "restart-delete-links";

    let (status, revision) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Restart delete link closure",
            "content": "durable company content",
            "checksum": "restart-delete-links-v1"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{revision}");
    let revision_id = revision["revision_id"].as_str().unwrap();
    let (status, activated) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": "restart deletion regression" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{activated}");
    let source_document_uri = activated["source_document_uri"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, ingested) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "source_id": source_id,
            "revision_id": revision_id,
            "source_document_uri": source_document_uri,
            "title": "Restart delete link closure",
            "content": "Durable parsed content whose fragment and parse-artifact links must be deleted after restart.",
            "parser_provider": "builtin"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ingested}");
    let fragment_uri = ingested["fragment_uris"][0]
        .as_str()
        .expect("sync ingest should persist a fragment")
        .to_string();
    let artifact_uri = ingested["parse_artifacts"][0]["uri"]
        .as_str()
        .expect("sync ingest should persist a parse artifact")
        .to_string();

    let (status, source_link) = call(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "source_uri": source_document_uri,
            "target_uri": "ctx://company/restart-delete-links/related",
            "relation": "supports",
            "rationale": "must be removed without a source-document cache warmup"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source_link}");
    let (status, fragment_link) = call(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "source_uri": fragment_uri,
            "target_uri": "ctx://company/restart-delete-links/fragment-related",
            "relation": "supports",
            "rationale": "fragment links belong to the source deletion closure"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{fragment_link}");
    let (status, artifact_link) = call(
        app,
        Method::POST,
        "/v1/links",
        json!({
            "source_uri": artifact_uri,
            "target_uri": "ctx://company/restart-delete-links/artifact-related",
            "relation": "supports",
            "rationale": "parse-artifact links belong to the source deletion closure"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{artifact_link}");
    let mut link_ids = [source_link, fragment_link, artifact_link]
        .into_iter()
        .map(|response| response["link"]["id"].as_str().unwrap().to_string())
        .collect::<HashSet<_>>();

    // Persist an otherwise unreferenced company SourceDocument and a link to
    // its custom URI without warming either row into the live cache. Startup
    // deliberately does not hydrate source-document bodies, so DELETE must
    // read this aggregate from the repository before freezing its replay plan.
    let custom_source_document_id = "restart-delete-custom-document";
    let custom_source_document_uri =
        "ctx://company/restart-delete-links/custom-source-document".to_string();
    let custom_source_document = tenant_document(
        state.tenant_id(),
        "rag_source_documents",
        custom_source_document_id,
        &json!({
            "id": custom_source_document_id,
            "tenant_id": state.tenant_id(),
            "owner_user_id": null,
            "source_kind": "company_document",
            "source_id": source_id,
            "revision_id": revision_id,
            "uri": custom_source_document_uri.clone(),
            "title": "Read-through-only source document",
            "content": "This document intentionally has no derivative context or artifact row.",
            "checksum": "restart-delete-custom-document-checksum",
            "status": "active",
            "retrieval_enabled": true,
            "created_at": "2026-07-14T00:00:00Z",
            "updated_at": "2026-07-14T00:00:00Z"
        }),
    )
    .unwrap();
    let custom_source_document_physical_id =
        custom_source_document["id"].as_str().unwrap().to_string();
    fault
        .documents
        .lock()
        .unwrap()
        .entry("rag_source_documents".to_string())
        .or_default()
        .insert(custom_source_document_physical_id, custom_source_document);

    let custom_link_id = "restart-delete-custom-document-link";
    let mut custom_link = tenant_document(
        state.tenant_id(),
        "rag_links",
        custom_link_id,
        &json!({
            "id": custom_link_id,
            "tenant_id": state.tenant_id(),
            "owner_user_id": null,
            "source_uri": custom_source_document_uri.clone(),
            "target_uri": "ctx://company/restart-delete-links/custom-related",
            "relation": "supports",
            "rationale": "must be deleted from the read-through URI closure",
            "confidence": 1.0,
            "created_by": "test",
            "status": "active",
            "tags": [],
            "created_at": "2026-07-14T00:00:00Z",
            "updated_at": "2026-07-14T00:00:00Z"
        }),
    )
    .unwrap();
    // Pre-tenant-scope link mirrors may have only `id`, not `logical_id`.
    // URI closure must still delete them, while the filter retains both ID
    // selectors for current and legacy rows.
    custom_link
        .as_object_mut()
        .expect("tenant link wrapper is an object")
        .remove("logical_id");
    let custom_link_physical_id = custom_link["id"].as_str().unwrap().to_string();
    link_ids.insert(custom_link_physical_id.clone());
    fault
        .documents
        .lock()
        .unwrap()
        .entry("rag_links".to_string())
        .or_default()
        .insert(custom_link_physical_id, custom_link);

    let config = state.config.clone();
    state.shutdown().await;
    let restarted = AppState::new(config);
    restarted
        .store
        .hydrate_from_repository(restarted.tenant_id())
        .await
        .expect("restart hydration should succeed");
    let restarted_app = build_router(restarted.clone());

    // No ContextFS/source-document read occurs between hydration and DELETE.
    let (status, deleted) = call(
        restarted_app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["deleted"], true, "{deleted}");
    let operation_id = deleted["persistence"]["operation_id"].as_str().unwrap();
    let journal = fault.latest_operation(operation_id);
    assert!(
        fault
            .delete_attempts
            .lock()
            .unwrap()
            .iter()
            .any(|index_uid| index_uid == "rag_links"),
        "{journal}"
    );
    let link_step = journal["plan"]["side_effects"]
        .as_array()
        .unwrap()
        .iter()
        .find(|step| {
            step["resource"]["kind"] == "delete_company_source_index"
                && step["resource"]["payload"]["target"]["kind"] == "links"
        })
        .unwrap_or_else(|| panic!("delete plan omitted the link-closure step: {journal}"));
    let planned_link_ids = link_step["resource"]["payload"]["target"]["payload"]["link_ids"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect::<HashSet<_>>();
    assert!(
        link_ids.is_subset(&planned_link_ids),
        "delete plan omitted one of the explicitly persisted source/fragment/artifact links: {journal}"
    );
    let related_uris = link_step["resource"]["payload"]["target"]["payload"]["related_uris"]
        .as_array()
        .unwrap();
    for related_uri in [
        &source_document_uri,
        &fragment_uri,
        &artifact_uri,
        &custom_source_document_uri,
    ] {
        assert!(
            related_uris.iter().any(|uri| uri == related_uri),
            "delete plan omitted related URI {related_uri}: {journal}"
        );
    }
    let link_delete_filter = fault
        .delete_filters
        .lock()
        .unwrap()
        .iter()
        .find(|(index_uid, _)| index_uid == "rag_links")
        .map(|(_, filter)| filter.clone())
        .unwrap_or_else(|| panic!("link deletion did not submit a filter: {journal}"));
    assert!(
        link_delete_filter.contains("id IN")
            && link_delete_filter.contains("source_uri IN")
            && link_delete_filter.contains("target_uri IN")
            && link_delete_filter.contains(&custom_source_document_uri),
        "link deletion was not guarded by the complete durable URI closure: {link_delete_filter}"
    );
    assert!(
        fault
            .documents
            .lock()
            .unwrap()
            .get("rag_links")
            .is_none_or(BTreeMap::is_empty),
        "durable related links survived company deletion"
    );
    restarted.shutdown().await;
}

#[tokio::test]
async fn company_delete_returns_distinct_task_uids_in_typed_index_order() {
    let (state, app, fault) = company_delete_app(CompanyDeleteFault::default()).await;
    let source_id = "ordered-delete";
    seed_company_source(&state, source_id);

    let (status, deleted) = call(
        app,
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["persistence"]["status"], "completed", "{deleted}");
    assert_eq!(
        fault.delete_attempts.lock().unwrap().as_slice(),
        REQUIRED_DELETE_INDEXES
    );

    let accepted = fault.accepted_deletes.lock().unwrap().clone();
    assert_eq!(accepted.len(), REQUIRED_DELETE_INDEXES.len());
    let accepted_task_uids = accepted
        .iter()
        .map(|(_, task_uid)| task_uid.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        deleted["persistence"]["task_uids"],
        json!(accepted_task_uids),
        "{deleted}"
    );
    assert_eq!(deleted["fragments_task"], json!(accepted[0].1));
    assert_eq!(deleted["revisions_task"], json!(accepted[1].1));
    assert_eq!(deleted["source_task"], json!(accepted[2].1));
    assert_eq!(
        deleted["auxiliary_tasks"],
        json!(accepted[3..]
            .iter()
            .map(|(_, task_uid)| task_uid)
            .collect::<Vec<_>>())
    );
    state.shutdown().await;
}

#[tokio::test]
async fn missing_required_delete_index_stops_without_shifting_task_identity() {
    let fault = CompanyDeleteFault {
        missing_index: Some("rag_source_revisions".to_string()),
        ..CompanyDeleteFault::default()
    };
    let (state, app, fault) = company_delete_app(fault).await;
    let source_id = "missing-delete-index";
    seed_company_source(&state, source_id);

    let (status, partial) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{partial}");
    assert_eq!(
        partial["persistence"]["status"], "partially_failed",
        "{partial}"
    );
    assert_eq!(partial["deleted"], false, "{partial}");
    assert_eq!(
        fault.delete_attempts.lock().unwrap().as_slice(),
        ["rag_company_context", "rag_source_revisions"]
    );
    let accepted = fault.accepted_deletes.lock().unwrap().clone();
    assert_eq!(accepted.len(), 1);
    assert_eq!(partial["fragments_task"], accepted[0].1);
    assert!(partial["revisions_task"].is_null(), "{partial}");
    assert!(partial["source_task"].is_null(), "{partial}");
    assert_eq!(partial["auxiliary_tasks"], json!([]), "{partial}");

    let operation_id = partial["persistence"]["operation_id"].as_str().unwrap();
    let journal = fault.latest_operation(operation_id);
    assert_eq!(
        journal["progress"]["steps"]["primary"]["status"],
        "completed"
    );
    assert_eq!(
        journal["progress"]["steps"]["primary"]["task_uids"],
        json!([accepted[0].1])
    );
    assert_eq!(
        journal["progress"]["steps"]["effect-0001"]["status"],
        "failed"
    );
    assert_eq!(
        journal["progress"]["steps"]["effect-0002"]["status"],
        "pending"
    );

    let delete_attempt_count = fault.delete_attempts.lock().unwrap().len();
    let (status, blocked_retry) = call(
        app,
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked_retry}");
    assert_eq!(
        blocked_retry["error"]["code"], "conflict",
        "{blocked_retry}"
    );
    assert_eq!(
        fault.delete_attempts.lock().unwrap().len(),
        delete_attempt_count,
        "a repeated DELETE must not create a second operation or submit another index deletion"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn reconciliation_publishes_each_confirmed_step_before_a_later_retry_failure() {
    let fault = CompanyDeleteFault {
        fail_once_index: Arc::new(Mutex::new(Some("rag_source_documents".to_string()))),
        ..CompanyDeleteFault::default()
    };
    let (state, app, fault) = company_delete_app(fault).await;
    let source_id = "reconcile-step-publication";
    let source_document_uri = seed_active_company_source(&state, source_id);

    let (status, partial) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{partial}");
    assert_eq!(partial["deleted"], false, "{partial}");
    assert!(
        state
            .store
            .fs_read_async(state.tenant_id(), &source_document_uri, None, false)
            .await
            .is_ok(),
        "the unconfirmed source-document step was published too early"
    );

    *fault.fail_once_index.lock().unwrap() = Some("rag_parse_artifacts".to_string());
    let operation_id = partial["persistence"]["operation_id"].as_str().unwrap();
    let (status, reconciled) = call(
        app,
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [operation_id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reconciled}");
    assert_eq!(reconciled["failed"], 1, "{reconciled}");
    assert!(
        state
            .store
            .fs_read_async(state.tenant_id(), &source_document_uri, None, false)
            .await
            .is_err(),
        "the confirmed source-document retry remained live after a later parse-artifact failure"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn later_delete_failure_preserves_prior_checkpoints_and_retries_from_failed_step() {
    let fault = CompanyDeleteFault {
        fail_once_index: Arc::new(Mutex::new(Some("rag_parse_artifacts".to_string()))),
        ..CompanyDeleteFault::default()
    };
    let (state, app, fault) = company_delete_app(fault).await;
    let source_id = "retry-delete-step";
    let related_uri = seed_active_company_source(&state, source_id);

    let (status, partial) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{partial}");
    assert_eq!(
        partial["persistence"]["status"], "partially_failed",
        "{partial}"
    );
    assert_eq!(partial["deleted"], false, "{partial}");
    let operation_id = partial["persistence"]["operation_id"]
        .as_str()
        .unwrap()
        .to_string();
    let journal = fault.latest_operation(&operation_id);
    for step_id in ["primary", "effect-0001", "effect-0002", "effect-0003"] {
        assert_eq!(
            journal["progress"]["steps"][step_id]["status"], "completed",
            "{step_id}: {journal}"
        );
        assert_eq!(
            journal["progress"]["steps"][step_id]["task_uids"]
                .as_array()
                .unwrap()
                .len(),
            1,
            "{step_id}: {journal}"
        );
    }
    assert_eq!(
        journal["progress"]["steps"]["effect-0004"]["status"],
        "failed"
    );
    assert_eq!(
        journal["progress"]["steps"]["effect-0005"]["status"],
        "pending"
    );

    let (status, missing_before_reconcile) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "confirmed source deletion remained visible after a later-step failure: {missing_before_reconcile}"
    );

    let (status, blocked_recreate) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({ "title": "must wait for delete reconciliation", "content": "new data" }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked_recreate}");

    let (status, blocked_ingest) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/tasks",
        json!({
            "source_id": source_id,
            "revision_id": "new-ingest-generation",
            "title": "must also wait for delete reconciliation",
            "content": "new company ingest data"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked_ingest}");

    let (status, blocked_link) = call(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "source_uri": related_uri,
            "target_uri": "ctx://company/retry-delete-step/new-related-target",
            "relation": "supports",
            "rationale": "must not race an incomplete source deletion"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked_link}");

    let (status, reconciled) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [operation_id] }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "{reconciled}; attempts={:?}; journal={}",
        fault.delete_attempts.lock().unwrap(),
        fault.latest_operation(&operation_id)
    );
    assert_eq!(reconciled["reconciled"], 1, "{reconciled}");
    assert_eq!(reconciled["failed"], 0, "{reconciled}");

    let attempts = fault.delete_attempts.lock().unwrap().clone();
    for index_uid in REQUIRED_DELETE_INDEXES {
        let expected = if index_uid == "rag_parse_artifacts" {
            2
        } else {
            1
        };
        assert_eq!(
            attempts
                .iter()
                .filter(|attempt| *attempt == index_uid)
                .count(),
            expected,
            "{index_uid}: {attempts:?}"
        );
    }
    let journal = fault.latest_operation(&operation_id);
    assert_eq!(journal["status"], "completed", "{journal}");
    assert_eq!(journal["indexing_state"], "completed", "{journal}");

    let (status, recreated) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({ "title": "safe recreated source", "content": "new generation" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{recreated}");

    let (status, repeated) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [operation_id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{repeated}");
    assert_eq!(repeated["reconciled"], 0, "{repeated}");
    assert_eq!(repeated["skipped"], 1, "{repeated}");

    let (status, surviving) = call(
        app,
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{surviving}");
    assert_eq!(surviving["revision_id"], recreated["revision_id"]);
    state.shutdown().await;
}

#[tokio::test]
async fn failed_final_completion_checkpoint_never_reports_or_publishes_full_deletion() {
    let fault = CompanyDeleteFault {
        fail_completed_delete_checkpoint_once: Arc::new(AtomicBool::new(true)),
        ..CompanyDeleteFault::default()
    };
    let (state, app, fault) = company_delete_app(fault).await;
    let source_id = "failed-final-delete-checkpoint";
    let source_document_uri = seed_active_company_source(&state, source_id);
    let link = state
        .store
        .upsert_link(
            state.tenant_id(),
            LinkUpsertRequest {
                source_uri: Some(source_document_uri.clone()),
                target_uri: Some(
                    "ctx://company/failed-final-delete-checkpoint/related".to_string(),
                ),
                relation: "supports".to_string(),
                rationale: Some("the final cache projection must wait for its journal".to_string()),
                ..LinkUpsertRequest::default()
            },
        )
        .unwrap()
        .link;

    let (status, partial) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{partial}");
    assert_eq!(partial["deleted"], false, "{partial}");
    assert_eq!(
        partial["persistence"]["indexing_state"], "pending",
        "{partial}"
    );

    let (status, visible_link) = call(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "uri": source_document_uri, "direction": "both" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{visible_link}");
    assert!(
        visible_link["links"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| candidate["id"] == link.id),
        "the failed completion checkpoint published the final link deletion: {visible_link}"
    );

    let operation_id = partial["persistence"]["operation_id"]
        .as_str()
        .unwrap()
        .to_string();
    let journal = fault.latest_operation(&operation_id);
    assert_ne!(journal["indexing_state"], "completed", "{journal}");

    let (status, reconciled) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [operation_id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reconciled}");
    assert_eq!(reconciled["reconciled"], 1, "{reconciled}");

    let (status, removed_link) = call(
        app,
        Method::POST,
        "/v1/links/search",
        json!({ "uri": source_document_uri, "direction": "both" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{removed_link}");
    assert!(removed_link["links"].as_array().unwrap().is_empty());
    state.shutdown().await;
}

#[tokio::test]
async fn delete_conflicts_with_an_older_nonterminal_company_mutation() {
    let (state, app, fault) = company_delete_app(CompanyDeleteFault::default()).await;
    let source_id = "delete-after-partial-activation";

    let (status, revision) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Older company mutation",
            "content": "the activation will fail after its primary write",
            "checksum": "older-company-mutation-v1"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{revision}");
    let revision_id = revision["revision_id"].as_str().unwrap();

    *fault.fail_once_write_index.lock().unwrap() = Some("rag_company_context".to_string());
    let (status, activation_failure) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": "inject a nonterminal predecessor" }),
    )
    .await;
    assert!(
        status.is_server_error(),
        "activation unexpectedly completed instead of leaving a nonterminal operation: {activation_failure}"
    );
    let older_operation = fault
        .operation_journal
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|operation| operation["operation_kind"] == "company_revision.activate")
        .cloned()
        .expect("failed activation should remain journaled");
    assert_ne!(older_operation["status"], "completed", "{older_operation}");
    let older_operation_id = operation_logical_id(&older_operation).unwrap().to_string();
    let older_operation_status = older_operation["status"].as_str().unwrap().to_string();

    let (status, blocked_delete) = call(
        app,
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CONFLICT,
        "delete started while older operation {older_operation_id} remained {older_operation_status}: {blocked_delete}"
    );
    assert_eq!(blocked_delete["error"]["code"], "conflict");
    assert!(
        fault.delete_attempts.lock().unwrap().is_empty(),
        "delete reached Meilisearch despite the older nonterminal operation"
    );
    state.shutdown().await;
}

#[tokio::test]
async fn delete_conflicts_with_an_older_nonterminal_related_link_write() {
    let (state, app, fault) = company_delete_app(CompanyDeleteFault::default()).await;
    let source_id = "delete-after-partial-link";
    let source_document_uri = seed_active_company_source(&state, source_id);

    *fault.fail_once_write_index.lock().unwrap() = Some("rag_links".to_string());
    let (status, link_failure) = call(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "source_uri": source_document_uri,
            "target_uri": "ctx://company/delete-after-partial-link/related",
            "relation": "supports",
            "rationale": "the failed link write is still an older source generation"
        }),
    )
    .await;
    assert!(status.is_server_error(), "{status}: {link_failure}");
    let link_operation = fault
        .operation_journal
        .lock()
        .unwrap()
        .iter()
        .rev()
        .find(|operation| operation["operation_kind"] == "link.upsert")
        .cloned()
        .expect("failed link upsert should remain journaled");
    assert_ne!(link_operation["status"], "completed", "{link_operation}");
    let link_operation_id = operation_logical_id(&link_operation).unwrap().to_string();

    let (status, blocked_delete) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT, "{blocked_delete}");
    assert!(fault.delete_attempts.lock().unwrap().is_empty());

    let (status, reconciled) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/operations:reconcile",
        json!({ "operation_ids": [link_operation_id] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{reconciled}");
    assert_eq!(reconciled["reconciled"], 1, "{reconciled}");

    let (status, deleted) = call(
        app,
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["deleted"], true, "{deleted}");
    assert_eq!(
        fault
            .delete_attempts
            .lock()
            .unwrap()
            .last()
            .map(String::as_str),
        Some("rag_links")
    );
    state.shutdown().await;
}
