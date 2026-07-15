use std::sync::{Arc, Mutex};

use axum::{
    extract::{Request, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use nowledge::{meili::MeiliAdmin, Config};
use serde_json::json;

const ACTUAL_CREATED_AT: &str = "2026-07-15T00:00:00Z";

#[derive(Clone, Default)]
struct RequestRecorder {
    requests: Arc<Mutex<Vec<String>>>,
}

async fn fully_provisioned_meili_stub(
    State(recorder): State<RequestRecorder>,
    request: Request,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path.ends_with("/settings") {
        // Deliberate drift: a bootstrap that runs before pin validation will
        // attempt to reconcile this response with a PATCH.
        return (StatusCode::OK, Json(json!({}))).into_response();
    }
    if method == Method::GET && path.starts_with("/indexes/") {
        let uid = path.strip_prefix("/indexes/").unwrap();
        return (
            StatusCode::OK,
            Json(json!({
                "uid": uid,
                "primaryKey": "id",
                "createdAt": ACTUAL_CREATED_AT
            })),
        )
            .into_response();
    }
    if method == Method::GET && path.starts_with("/tasks/") {
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }
    if matches!(
        method,
        Method::POST | Method::PUT | Method::PATCH | Method::DELETE
    ) {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected request: {method} {path}") })),
    )
        .into_response()
}

async fn empty_meili_stub(State(recorder): State<RequestRecorder>, request: Request) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path == "/indexes" {
        return (
            StatusCode::OK,
            Json(json!({ "results": [], "offset": 0, "limit": 1, "total": 0 })),
        )
            .into_response();
    }
    if method == Method::GET {
        return (StatusCode::NOT_FOUND, Json(json!({ "message": "missing" }))).into_response();
    }
    (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response()
}

async fn dynamic_only_meili_stub(
    State(recorder): State<RequestRecorder>,
    request: Request,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    recorder
        .requests
        .lock()
        .unwrap()
        .push(format!("{method} {path}"));

    if method == Method::GET && path == "/indexes" {
        return (
            StatusCode::OK,
            Json(json!({
                "results": [{ "uid": "rag_events__t_existing__u_existing" }],
                "offset": 0,
                "limit": 1,
                "total": 1
            })),
        )
            .into_response();
    }
    if method == Method::GET {
        return (StatusCode::NOT_FOUND, Json(json!({ "message": "missing" }))).into_response();
    }
    (StatusCode::ACCEPTED, Json(json!({ "taskUid": 1 }))).into_response()
}

#[tokio::test]
async fn startup_pin_mismatch_fails_before_any_meilisearch_mutation() {
    let recorder = RequestRecorder::default();
    let app = Router::new()
        .fallback(fully_provisioned_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.run_mode = "production".to_string();
    config.meili_url = Some(format!("http://{address}"));
    config.meili_allow_initial_provision = false;
    config.meili_operations_index_created_at = Some("2026-07-15T00:00:01Z".to_string());
    config.meili_audit_index_created_at = Some(ACTUAL_CREATED_AT.to_string());
    let admin = MeiliAdmin::from_admin_config(&config);

    let error = admin
        .prepare_for_startup()
        .await
        .expect_err("a mismatched deployment identity must stop startup");

    assert!(
        error.to_string().contains("pinned deployment identity"),
        "{error}"
    );
    let requests = recorder.requests.lock().unwrap().clone();
    assert!(
        requests
            .iter()
            .any(|request| request == "GET /indexes/rag_operations"),
        "the durable pin was not inspected: {requests:?}"
    );
    assert!(
        requests.iter().all(|request| request.starts_with("GET ")),
        "pin mismatch reached a mutating Meilisearch request: {requests:?}"
    );
}

#[tokio::test]
async fn initial_provision_never_bypasses_configured_durable_pins() {
    let recorder = RequestRecorder::default();
    let app = Router::new()
        .fallback(empty_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.run_mode = "production".to_string();
    config.meili_url = Some(format!("http://{address}"));
    config.meili_allow_initial_provision = true;
    config.meili_operations_index_created_at = Some(ACTUAL_CREATED_AT.to_string());
    config.meili_audit_index_created_at = Some(ACTUAL_CREATED_AT.to_string());

    let error = MeiliAdmin::from_admin_config(&config)
        .prepare_for_startup()
        .await
        .expect_err("configured pins must reject an empty replacement backend");
    assert!(error.to_string().contains("unavailable"), "{error}");

    let requests = recorder.requests.lock().unwrap().clone();
    assert!(
        requests
            .iter()
            .any(|request| request == "GET /indexes/rag_operations"),
        "the configured durable pin was not inspected: {requests:?}"
    );
    assert!(
        requests.iter().all(|request| request.starts_with("GET ")),
        "initial-provision mode mutated an empty pinned backend: {requests:?}"
    );
}

#[tokio::test]
async fn initial_provision_rejects_a_dynamic_only_meilisearch_instance() {
    let recorder = RequestRecorder::default();
    let app = Router::new()
        .fallback(dynamic_only_meili_stub)
        .with_state(recorder.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let mut config = Config::test();
    config.run_mode = "production".to_string();
    config.meili_url = Some(format!("http://{address}"));
    config.meili_allow_initial_provision = true;

    MeiliAdmin::from_admin_config(&config)
        .prepare_for_startup()
        .await
        .expect_err("dynamic indexes prove that the backend is not a new empty instance");

    let requests = recorder.requests.lock().unwrap().clone();
    assert!(
        requests.iter().any(|request| request == "GET /indexes"),
        "the complete instance was not inspected: {requests:?}"
    );
    assert!(
        requests.iter().all(|request| request.starts_with("GET ")),
        "initial provision mutated a dynamic-only backend: {requests:?}"
    );
}
