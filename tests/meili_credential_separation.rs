use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use axum::{
    body::to_bytes,
    extract::{Request as AxumRequest, State},
    http::{header::AUTHORIZATION, Method, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use chrono::Utc;
use nowledge::{
    meili::{settings_for, MeiliAdmin},
    models::{
        OperationActor, OperationActorScope, OperationPlan, OperationResource, OperationStep,
        OperationStepRole,
    },
    mutation::{operation_record_from_plan, OPERATION_PLAN_SCHEMA_VERSION},
    repository::repository_from_config,
    Config,
};
use serde_json::{json, Value};

const RUNTIME_KEY: &str = "runtime-test-key";
const ADMIN_KEY: &str = "index-admin-test-key";
const RUNTIME_AUTHORIZATION: &str = "Bearer runtime-test-key";
const ADMIN_AUTHORIZATION: &str = "Bearer index-admin-test-key";

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: Method,
    path: String,
    authorization: Option<String>,
}

#[derive(Clone, Default)]
struct CredentialRecorder {
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    indexes: Arc<Mutex<HashSet<String>>>,
    configured_settings: Arc<Mutex<HashSet<String>>>,
    next_task_uid: Arc<AtomicU64>,
}

impl CredentialRecorder {
    fn accepted_task(&self) -> Response {
        let task_uid = self.next_task_uid.fetch_add(1, Ordering::Relaxed) + 1;
        (StatusCode::ACCEPTED, Json(json!({ "taskUid": task_uid }))).into_response()
    }
}

async fn credential_meili_stub(
    State(recorder): State<CredentialRecorder>,
    request: AxumRequest,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let authorization = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    recorder.requests.lock().unwrap().push(RecordedRequest {
        method: method.clone(),
        path: path.clone(),
        authorization,
    });

    if method == Method::GET && path.starts_with("/tasks/") {
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }

    if method == Method::GET && path.ends_with("/settings") {
        let index_uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/settings"))
            .unwrap();
        if !recorder.indexes.lock().unwrap().contains(index_uid) {
            return (StatusCode::NOT_FOUND, Json(json!({}))).into_response();
        }
        if recorder
            .configured_settings
            .lock()
            .unwrap()
            .contains(index_uid)
        {
            return (StatusCode::OK, Json(settings_for(index_uid))).into_response();
        }
        return (StatusCode::OK, Json(json!({}))).into_response();
    }

    if method == Method::GET && path.starts_with("/indexes/") {
        let index_uid = path.strip_prefix("/indexes/").unwrap();
        if !recorder.indexes.lock().unwrap().contains(index_uid) {
            return (StatusCode::NOT_FOUND, Json(json!({}))).into_response();
        }
        return (
            StatusCode::OK,
            Json(json!({
                "uid": index_uid,
                "primaryKey": "id",
                "createdAt": "2026-07-15T00:00:00Z"
            })),
        )
            .into_response();
    }

    if method == Method::POST && path == "/indexes" {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let index_uid = body["uid"].as_str().unwrap();
        assert_eq!(body["primaryKey"], "id");
        recorder
            .indexes
            .lock()
            .unwrap()
            .insert(index_uid.to_string());
        return recorder.accepted_task();
    }

    if method == Method::PATCH && path.ends_with("/settings") {
        let index_uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/settings"))
            .unwrap();
        recorder
            .configured_settings
            .lock()
            .unwrap()
            .insert(index_uid.to_string());
        return recorder.accepted_task();
    }

    if method == Method::POST && path.ends_with("/search") {
        return (
            StatusCode::OK,
            Json(json!({ "hits": [], "processingTimeMs": 0 })),
        )
            .into_response();
    }

    if method == Method::POST && path.ends_with("/documents") {
        return recorder.accepted_task();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected stub request: {method} {path}") })),
    )
        .into_response()
}

async fn spawn_credential_meili_stub(recorder: CredentialRecorder) -> String {
    let app = Router::new()
        .fallback(credential_meili_stub)
        .with_state(recorder);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

#[tokio::test]
async fn runtime_and_index_admin_credentials_are_separated_end_to_end() {
    let recorder = CredentialRecorder::default();
    let url = spawn_credential_meili_stub(recorder.clone()).await;
    let mut config = Config::test();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.meili_api_key = Some(RUNTIME_KEY.to_string());
    config.meili_admin_api_key = Some(ADMIN_KEY.to_string());
    config.meili_wait_for_tasks = false;

    MeiliAdmin::from_admin_config(&config)
        .bootstrap(false)
        .await
        .expect("the distinct index-admin credential should provision managed indexes");

    let bootstrap_request_count = recorder.requests.lock().unwrap().len();
    let bootstrap_requests = recorder.requests.lock().unwrap().clone();
    assert!(
        bootstrap_requests
            .iter()
            .any(|request| request.method == Method::POST && request.path == "/indexes"),
        "bootstrap did not exercise index creation: {bootstrap_requests:?}"
    );
    assert!(
        bootstrap_requests.iter().any(|request| {
            request.method == Method::PATCH && request.path.ends_with("/settings")
        }),
        "bootstrap did not exercise settings mutation: {bootstrap_requests:?}"
    );
    assert!(
        bootstrap_requests
            .iter()
            .all(|request| request.authorization.as_deref() == Some(ADMIN_AUTHORIZATION)),
        "bootstrap sent a request without the index-admin credential: {bootstrap_requests:?}"
    );

    let repository = repository_from_config(&config);
    let operation = operation_record_from_plan(OperationPlan {
        schema_version: OPERATION_PLAN_SCHEMA_VERSION,
        id: format!("credential-separation-{}", uuid::Uuid::now_v7().simple()),
        tenant_id: "credential-separation-tenant".to_string(),
        operation_kind: "structured_rows.credential_test".to_string(),
        actor: OperationActor {
            scope: OperationActorScope::TenantService,
            owner_user_id_hash: None,
            roles: vec!["writer".to_string()],
            request_id: Some("credential-separation-request".to_string()),
        },
        idempotency_key_hash: None,
        primary: OperationStep {
            id: "primary".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::StructuredRows {
                rows: vec![json!({ "id": "credential-separation-row" })],
            },
        },
        side_effects: Vec::new(),
        redacted_metadata: json!({ "fixture": "credential separation" }),
        response_snapshot: json!({ "ok": true }),
        created_at: Utc::now(),
    })
    .expect("credential-separation operation fixture must be valid");

    repository
        .upsert_operation(&operation)
        .await
        .expect("the runtime credential should persist a durable operation");
    let found = repository
        .get_operation(&operation.tenant_id, &operation.id)
        .await
        .expect("the runtime credential should execute ordinary search");
    assert!(found.is_none(), "the empty stub should return no operation");

    let all_requests = recorder.requests.lock().unwrap().clone();
    let runtime_requests = &all_requests[bootstrap_request_count..];
    assert!(
        runtime_requests.iter().any(|request| {
            request.method == Method::POST && request.path == "/indexes/rag_operations/documents"
        }),
        "runtime flow did not exercise document persistence: {runtime_requests:?}"
    );
    assert!(
        runtime_requests.iter().any(|request| {
            request.method == Method::POST && request.path == "/indexes/rag_operations/search"
        }),
        "runtime flow did not exercise search: {runtime_requests:?}"
    );
    assert!(
        runtime_requests
            .iter()
            .any(|request| request.method == Method::GET && request.path.starts_with("/tasks/")),
        "durable runtime flow did not read task status: {runtime_requests:?}"
    );
    assert!(
        runtime_requests.iter().any(|request| {
            request.method == Method::GET && request.path == "/indexes/rag_operations"
        }) && runtime_requests.iter().any(|request| {
            request.method == Method::GET && request.path == "/indexes/rag_operations/settings"
        }),
        "durable runtime flow did not perform continuity reads: {runtime_requests:?}"
    );
    assert!(
        runtime_requests
            .iter()
            .all(|request| request.authorization.as_deref() == Some(RUNTIME_AUTHORIZATION)),
        "runtime repository path leaked or substituted the index-admin credential: {runtime_requests:?}"
    );
    assert!(
        runtime_requests
            .iter()
            .all(|request| request.authorization.as_deref() != Some(ADMIN_AUTHORIZATION)),
        "runtime repository path sent the index-admin credential: {runtime_requests:?}"
    );
}
