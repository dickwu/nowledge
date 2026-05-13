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

fn authed_app() -> Router {
    let mut config = Config::test();
    config.auth_users = vec![
        nowledge::config::AuthUserConfig {
            token: "u1-token".to_string(),
            owner_user_id: Some("u1".to_string()),
            roles: vec!["user".to_string()],
        },
        nowledge::config::AuthUserConfig {
            token: "u2-token".to_string(),
            owner_user_id: Some("u2".to_string()),
            roles: vec!["user".to_string()],
        },
        nowledge::config::AuthUserConfig {
            token: "admin-token".to_string(),
            owner_user_id: None,
            roles: vec!["admin".to_string()],
        },
    ];
    build_router(AppState::new(Arc::new(config)))
}

fn codex_import_app() -> Router {
    let mut config = Config::test();
    config.allow_codex_auth_import = true;
    config.auth_users = vec![nowledge::config::AuthUserConfig {
        token: "admin-token".to_string(),
        owner_user_id: None,
        roles: vec!["admin".to_string()],
    }];
    build_router(AppState::new(Arc::new(config)))
}

fn mock_llm_app() -> Router {
    let mut config = Config::test();
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("mock-model".to_string());
    build_router(AppState::new(Arc::new(config)))
}

async fn call(app: Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    call_with_token(app, method, uri, body, None).await
}

async fn call_with_token(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header("Authorization", format!("Bearer {token}"));
    }
    let request = builder.body(Body::from(body.to_string())).unwrap();
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

fn query_encode(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace(':', "%3A")
        .replace('/', "%2F")
        .replace(' ', "%20")
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

    let (status, index) = call(
        app.clone(),
        Method::GET,
        "/v1/history/users/u1/event-index",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{index}");
    let registry_id = index["index"]["id"].as_str().unwrap();
    assert!(registry_id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));

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

#[tokio::test]
async fn authenticated_user_is_bound_to_owner_user_id() {
    let app = authed_app();

    let (status, body) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/history/users/u2/events",
        event_body("u2", "n1", "cross-owner"),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/history/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "n2",
            "occurred_at": "2026-05-12T00:00:00Z",
            "observed_at": "2026-05-12T00:01:00Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "n2" },
            "text": "owner defaulted from token"
        }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["event"]["owner_user_id"], "u1");

    let (status, body) = call_with_token(
        app,
        Method::POST,
        "/v1/history/users/u2/events",
        event_body("u2", "n3", "admin cross-owner"),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
}

#[tokio::test]
async fn contextfs_private_acl_and_company_readability() {
    let app = authed_app();

    let (status, event) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        event_body("u1", "acl-note", "acl-private-keyword belongs to u1"),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");

    let (status, search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "acl-private-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    let private_uri = search["hits"][0]["uri"].as_str().unwrap();

    let (status, own_read) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(private_uri)),
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{own_read}");

    let (status, cross_read) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(private_uri)),
        Value::Null,
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{cross_read}");

    let (status, cross_reveal) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/reveal",
        json!({ "uri": private_uri, "next_layer": 1 }),
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{cross_reveal}");

    let (status, revision) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/state/company-docs/company-handbook/revisions",
        json!({
            "title": "Company Handbook",
            "source_uri": "https://example.test/company/handbook",
            "content": "company-visible-keyword is available to every employee.",
            "checksum": "company-1"
        }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{revision}");
    let revision_id = revision["revision_id"].as_str().unwrap();

    let (status, activated) = call_with_token(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/company-handbook/revisions/{revision_id}/activate"),
        json!({ "reason": "test" }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{activated}");
    let company_uri = activated["context_uris"][0].as_str().unwrap();

    let (status, company_read) = call_with_token(
        app,
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(company_uri)),
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{company_read}");
    assert_eq!(company_read["privacy"], "company");
}

#[tokio::test]
async fn structured_apply_snapshot_reports_weekly_trends() {
    let app = app();

    let mut snapshot_ids = Vec::new();
    for (period, score) in [("2026-W18", 4.0), ("2026-W19", 8.0)] {
        let (status, snapshot) = call(
            app.clone(),
            Method::POST,
            "/v1/history/structured/snapshots",
            json!({
                "dataset_key": "weekly-status",
                "owner_user_id": "u1",
                "period_key": period,
                "period_start": if period == "2026-W18" { "2026-04-27T00:00:00Z" } else { "2026-05-04T00:00:00Z" },
                "period_end": if period == "2026-W18" { "2026-05-03T23:59:59Z" } else { "2026-05-10T23:59:59Z" },
                "granularity": "weekly",
                "source_ref": { "kind": "test", "id": period }
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{snapshot}");
        let snapshot_id = snapshot["snapshot"]["id"].as_str().unwrap().to_string();
        let (status, rows) = call(
            app.clone(),
            Method::POST,
            &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
            json!({ "rows": [{ "id": format!("row-{period}"), "stress_score": score }] }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");
        snapshot_ids.push(snapshot_id);
    }

    let latest = snapshot_ids.last().unwrap();
    let (status, applied) = call(
        app.clone(),
        Method::POST,
        "/v1/state/structured/datasets/weekly-status/apply-snapshot",
        json!({ "snapshot_id": latest, "materialize_context": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{applied}");

    let (status, current) = call(
        app,
        Method::GET,
        "/v1/state/structured/current",
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    let metrics = current["summaries"][0]["stats"]["metrics"]
        .as_array()
        .unwrap();
    let stress = metrics
        .iter()
        .find(|metric| metric["metric"] == "stress_score")
        .unwrap();
    assert_eq!(stress["previous_mean"], 4.0);
    assert_eq!(stress["delta_vs_previous"], 4.0);
    assert_eq!(stress["trend_direction"], "up");
}

#[tokio::test]
async fn current_structured_is_owner_bound_for_users() {
    let app = authed_app();

    for (owner, token, score) in [("u1", "u1-token", 3.0), ("u2", "u2-token", 9.0)] {
        let (status, snapshot) = call_with_token(
            app.clone(),
            Method::POST,
            "/v1/history/structured/snapshots",
            json!({
                "dataset_key": "weekly-status",
                "owner_user_id": owner,
                "period_key": format!("2026-W20-{owner}"),
                "period_start": "2026-05-11T00:00:00Z",
                "period_end": "2026-05-17T23:59:59Z",
                "granularity": "weekly",
                "source_ref": { "kind": "test", "id": owner }
            }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{snapshot}");
        let snapshot_id = snapshot["snapshot"]["id"].as_str().unwrap();

        let (status, rows) = call_with_token(
            app.clone(),
            Method::POST,
            &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
            json!({ "rows": [{ "id": format!("row-{owner}"), "stress_score": score }] }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{rows}");

        let (status, applied) = call_with_token(
            app.clone(),
            Method::POST,
            "/v1/state/structured/datasets/weekly-status/apply-snapshot",
            json!({ "snapshot_id": snapshot_id, "materialize_context": true }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{applied}");
    }

    let (status, current) = call_with_token(
        app,
        Method::GET,
        "/v1/state/structured/current",
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{current}");
    let summaries = current["summaries"].as_array().unwrap();
    assert!(!summaries.is_empty());
    assert!(summaries
        .iter()
        .all(|summary| summary["owner_user_id"] == "u1"));
    assert!(!current.to_string().contains("\"owner_user_id\":\"u2\""));
}

#[tokio::test]
async fn llm_mock_provider_test_is_token_safe() {
    let app = mock_llm_app();
    let (status, body) = call(
        app,
        Method::POST,
        "/v1/llm/test",
        json!({ "prompt": "summarize without real provider" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["ok"], true);
    assert_eq!(body["model"], "mock-model");
    assert!(body["sample"].as_str().unwrap().contains("mock summary"));
}

#[tokio::test]
async fn rag_answer_uses_mock_llm_provider() {
    let app = mock_llm_app();
    let (status, event) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        event_body("u1", "rag-note", "rag-grounding-keyword from context"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");

    let (status, answer) = call(
        app,
        Method::POST,
        "/v1/rag/answer",
        json!({
            "owner_user_id": "u1",
            "question": "What does rag-grounding-keyword say?"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{answer}");
    assert!(answer["answer"].as_str().unwrap().contains("mock summary"));
    assert_ne!(answer["usage"]["provider"], "none");
}

#[tokio::test]
async fn codex_auth_import_is_token_safe() {
    let app = codex_import_app();
    let token = "codex-secret-token-should-not-leak";
    let path =
        std::env::temp_dir().join(format!("nowledge-codex-auth-{}.json", uuid::Uuid::now_v7()));
    std::fs::write(&path, json!({ "token": token }).to_string()).unwrap();

    let (status, body) = call_with_token(
        app,
        Method::POST,
        "/v1/llm/auth/import-codex",
        json!({
            "codex_auth_path": path.to_string_lossy(),
            "store_imported_token": false,
            "test_after_import": false
        }),
        Some("admin-token"),
    )
    .await;
    let _ = std::fs::remove_file(&path);
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "imported_in_memory");
    assert!(!body.to_string().contains(token));
}
