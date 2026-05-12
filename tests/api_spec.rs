use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use tower::ServiceExt;

fn app() -> Router {
    build_router(AppState::new(Arc::new(Config::test())))
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
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, value)
}

fn event_body(owner: &str, entity: &str, text: &str) -> Value {
    json!({
        "event_type": "note.created",
        "entity_type": "note",
        "entity_id": entity,
        "owner_user_id": owner,
        "occurred_at": "2026-05-12T00:00:00Z",
        "observed_at": "2026-05-12T00:01:00Z",
        "source_kind": "test",
        "source_ref": { "kind": "test", "id": entity },
        "text": text,
        "privacy": "private"
    })
}

#[tokio::test]
async fn user_history_events_are_index_isolated() {
    let app = app();

    let (status, u1) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        event_body("u1", "n1", "shared-keyword alpha-private"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{u1}");

    let (status, u2) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u2/events",
        event_body("u2", "n2", "shared-keyword beta-private"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{u2}");

    let uid1 = u1["routing"]["event_index_uid"].as_str().unwrap();
    let uid2 = u2["routing"]["event_index_uid"].as_str().unwrap();
    assert_ne!(uid1, uid2);
    assert!(uid1
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    assert!(!uid1.contains("u1"));

    let (status, search) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/search",
        json!({ "query": "shared-keyword", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"].as_array().unwrap().len(), 1);
    assert_eq!(search["hits"][0]["owner_user_id"], "u1");

    let (status, cross_search) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/search",
        json!({ "query": "beta-private", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cross_search}");
    assert_eq!(cross_search["hits"].as_array().unwrap().len(), 0);

    let (status, context_without_owner) = call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "alpha-private", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{context_without_owner}");
    assert_eq!(context_without_owner["hits"].as_array().unwrap().len(), 0);

    let (status, context_with_owner) = call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "alpha-private", "owner_user_id": "u1", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{context_with_owner}");
    assert!(!context_with_owner["hits"].as_array().unwrap().is_empty());
    assert!(!context_with_owner.to_string().contains(uid2));

    let (status, company_debug) = call(
        app.clone(),
        Method::POST,
        "/v1/debug/meili/search",
        json!({ "index_uid": "rag_company_context", "query": "alpha-private" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{company_debug}");
    assert_eq!(company_debug["hits"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn owner_path_body_mismatch_and_index_hint_are_rejected() {
    let app = app();

    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        event_body("u2", "n1", "mismatch"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

    let mut hinted = event_body("u1", "n2", "hinted");
    hinted["event_index_hint"] = json!("rag_events_global");
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        hinted,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
}

#[tokio::test]
async fn state_upsert_keeps_one_active_item_and_writes_history() {
    let app = app();

    for statement in [
        "Prefers Rust backend work",
        "Prefers Rust and axum backend work",
    ] {
        let (status, body) = call(
            app.clone(),
            Method::PUT,
            "/v1/state/profile/facts/backend-preference",
            json!({
                "owner_user_id": "u1",
                "state_type": "preference",
                "title": "Backend preference",
                "statement": statement,
                "source_refs": [{ "kind": "test", "id": "state" }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    let (status, state_search) = call(
        app.clone(),
        Method::POST,
        "/v1/state/search",
        json!({ "owner_user_id": "u1", "query": "Rust", "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state_search}");
    assert_eq!(state_search["hits"].as_array().unwrap().len(), 1);
    assert_eq!(state_search["hits"][0]["current_version"], 2);

    let (status, history) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/search",
        json!({ "event_types": ["state.changed"], "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_eq!(history["hits"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn company_doc_preflight_prefers_revision_for_similar_docs() {
    let app = app();

    let (status, revision) = call(
        app.clone(),
        Method::POST,
        "/v1/state/company-docs/hr-leave/revisions",
        json!({
            "title": "HR Leave Policy",
            "source_uri": "https://example.test/hr/leave",
            "content": "Employees can request annual leave through the HR portal.",
            "checksum": "c1"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{revision}");
    let revision_id = revision["revision_id"].as_str().unwrap();

    let (status, activated) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/hr-leave/revisions/{revision_id}/activate"),
        json!({ "reason": "initial" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{activated}");

    let (status, preflight) = call(
        app.clone(),
        Method::POST,
        "/v1/state/company-docs/preflight",
        json!({
            "title": "HR Leave Policy",
            "source_uri": "https://example.test/hr/leave",
            "text_preview": "Annual leave requests go through the HR portal.",
            "similarity_threshold": 0.8
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preflight}");
    assert_eq!(preflight["recommended_action"], "update_revision");
}

#[tokio::test]
async fn structured_rows_are_idempotent_by_row_id() {
    let app = app();

    let (status, snapshot) = call(
        app.clone(),
        Method::POST,
        "/v1/history/structured/snapshots",
        json!({
            "dataset_key": "weekly-status",
            "owner_user_id": "u1",
            "period_key": "2026-W19",
            "period_start": "2026-05-04T00:00:00Z",
            "period_end": "2026-05-10T23:59:59Z",
            "granularity": "weekly",
            "source_ref": { "kind": "test", "id": "sheet" }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{snapshot}");
    let snapshot_id = snapshot["snapshot"]["id"].as_str().unwrap();

    for expected in [(1, 0), (0, 1)] {
        let (status, rows) = call(
            app.clone(),
            Method::POST,
            &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
            json!({ "rows": [{ "id": "row-1", "stress_score": 7.0 }] }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        assert_eq!(rows["inserted"], expected.0);
        assert_eq!(rows["duplicates"], expected.1);
    }
}

#[tokio::test]
async fn prompt_preview_redacts_tokens() {
    let app = app();
    let (status, preview) = call(
        app,
        Method::POST,
        "/v1/debug/prompt/preview",
        json!({ "question": "Please do not leak sk-test-secret-123456" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{preview}");
    assert!(!preview.to_string().contains("sk-test-secret"));
    assert!(preview.to_string().contains("[REDACTED]"));
}
