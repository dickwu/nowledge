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

fn llm_health_app(provider: &str) -> Router {
    let mut config = Config::test();
    config.llm_provider = provider.to_string();
    config.llm_model = Some("health-model".to_string());
    build_router(AppState::new(Arc::new(config)))
}

fn bearer_user_app() -> Router {
    let mut config = Config::test();
    config.bearer_token = Some("user-token".to_string());
    config.admin_token = Some("admin-token".to_string());
    build_router(AppState::new(Arc::new(config)))
}

fn stale_llm_health_app() -> Router {
    let mut config = Config::test();
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("health-model".to_string());
    config.health_llm_probe_interval_seconds = 999;
    config.health_llm_probe_ttl_seconds = 0;
    config.health_llm_max_stale_seconds = 0;
    build_router(AppState::new(Arc::new(config)))
}

fn mock_llm_app() -> Router {
    let mut config = Config::test();
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("mock-model".to_string());
    build_router(AppState::new(Arc::new(config)))
}

fn analysis_llm_app() -> Router {
    let mut config = Config::test();
    config.llm_provider = "none".to_string();
    config.llm_model = Some("main-rag-model".to_string());
    config.analysis_llm_provider = "mock".to_string();
    config.analysis_llm_model = Some("gpt-5.3-codex-spark".to_string());
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
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes).to_string() }))
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
async fn livez_is_minimal_process_liveness() {
    let app = llm_health_app("mock");
    let (status, body) = call(app, Method::GET, "/livez", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body, json!({ "status": "ok" }));
    assert!(body.get("llm").is_none());
    assert!(body.get("usage").is_none());
}

#[tokio::test]
async fn healthz_includes_llm_health_and_usage() {
    let app = llm_health_app("mock");
    let (status, body) = call(app, Method::GET, "/healthz", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "ok");
    assert_eq!(body["ready"], true);
    assert_eq!(body["llm"]["provider"], "mock");
    assert_eq!(body["llm"]["auth_valid"], true);
    assert_eq!(body["llm"]["quota_state"], "available");
    assert!(body.get("usage").is_some());
    assert!(body["usage"].get("history_events").is_some());
}

#[tokio::test]
async fn llm_auth_failure_makes_health_unhealthy() {
    let app = llm_health_app("mock_auth_failure");
    let (status, body) = call(app, Method::GET, "/healthz", Value::Null).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["ready"], false);
    assert_eq!(body["status"], "unhealthy");
    assert_eq!(body["llm"]["auth_valid"], false);
    assert_eq!(body["llm"]["error_kind"], "auth_failed");
}

#[tokio::test]
async fn llm_quota_exhaustion_makes_health_unhealthy() {
    let app = llm_health_app("mock_quota_exhausted");
    let (status, body) = call(app, Method::GET, "/healthz", Value::Null).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["ready"], false);
    assert_eq!(body["status"], "unhealthy");
    assert_eq!(body["llm"]["quota_state"], "exhausted");
}

#[tokio::test]
async fn llm_short_rate_limit_is_degraded_by_default() {
    let app = llm_health_app("mock_rate_limited");
    let (status, body) = call(app, Method::GET, "/healthz", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["ready"], true);
    assert_eq!(body["status"], "degraded");
    assert_eq!(body["llm"]["rate_limit_state"], "limited");
    assert_eq!(body["llm"]["rate_limits"]["remaining_requests"], "0");
}

#[tokio::test]
async fn stale_llm_probe_beyond_max_stale_makes_health_unhealthy() {
    let app = stale_llm_health_app();
    let (status, first) = call(app.clone(), Method::GET, "/healthz", Value::Null).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let (status, body) = call(app, Method::GET, "/readyz", Value::Null).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(body["status"], "unhealthy");
    assert_eq!(body["ready"], false);
    assert_eq!(body["llm"]["stale"], true);
    assert_eq!(body["llm"]["error_kind"], "stale_probe");
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
async fn company_doc_fragments_traceback_and_update_supersedes_old_content() {
    let app = authed_app();
    let source_id = "fragment-handbook";
    let v1_content = format!(
        "# Fragment Handbook\n\n{}\n\n## Legacy\n\n{}",
        "fragment-alpha-keyword describes active company guidance. ".repeat(35),
        "legacy-retention-keyword should disappear after update. ".repeat(35)
    );

    let (status, revision) = call_with_token(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Fragment Handbook",
            "source_uri": "https://example.test/company/fragments",
            "content": v1_content,
            "checksum": "company-fragment-v1"
        }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{revision}");
    let revision_id = revision["revision_id"].as_str().unwrap();

    let (status, activated) = call_with_token(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": "initial" }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{activated}");
    let source_document_uri = activated["source_document_uri"].as_str().unwrap();
    let fragment_uris = activated["fragment_uris"].as_array().unwrap();
    assert!(fragment_uris.len() > 1, "{activated}");

    let (status, source_doc) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(source_document_uri)),
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source_doc}");
    assert_eq!(source_doc["node_kind"], "source_doc");
    assert_eq!(source_doc["retrieval_enabled"], false);
    assert!(source_doc["body"]
        .as_str()
        .unwrap()
        .contains("legacy-retention-keyword"));

    let (status, search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "fragment-alpha-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    let first_hit = &search["hits"][0];
    let fragment_uri = first_hit["uri"].as_str().unwrap();
    assert_eq!(first_hit["node_kind"], "fragment");
    assert_ne!(fragment_uri, source_document_uri);
    assert!(search["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["node_kind"] == "fragment" && hit["uri"] != source_document_uri));
    assert!(fragment_uris
        .iter()
        .any(|uri| uri.as_str() == Some(fragment_uri)));

    let (status, traceback) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/traceback",
        json!({ "uri": fragment_uri }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{traceback}");
    assert_eq!(traceback["source_document_uri"], source_document_uri);
    assert_eq!(traceback["source_id"], source_id);

    let (status, old_link) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "uri": fragment_uri, "direction": "outbound", "relations": ["part_of"], "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_link}");
    assert_eq!(old_link["outbound"].as_array().unwrap().len(), 1);

    let v2_content = format!(
        "# Fragment Handbook\n\n{}",
        "new-fragment-keyword replaces the legacy wording. ".repeat(60)
    );
    let (status, revision) = call_with_token(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Fragment Handbook",
            "source_uri": "https://example.test/company/fragments",
            "content": v2_content,
            "checksum": "company-fragment-v2"
        }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{revision}");
    let revision_id = revision["revision_id"].as_str().unwrap();

    let (status, activated_v2) = call_with_token(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": "update" }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{activated_v2}");

    let (status, old_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "legacy-retention-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_search}");
    assert_eq!(old_search["hits"].as_array().unwrap().len(), 0);

    let (status, new_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "new-fragment-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{new_search}");
    assert_eq!(new_search["hits"][0]["node_kind"], "fragment");

    let (status, old_read) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(fragment_uri)),
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{old_read}");

    let (status, old_link_after_update) = call_with_token(
        app,
        Method::POST,
        "/v1/links/search",
        json!({ "uri": fragment_uri, "direction": "outbound", "relations": ["part_of"], "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_link_after_update}");
    assert_eq!(
        old_link_after_update["outbound"].as_array().unwrap().len(),
        0
    );
}

#[tokio::test]
async fn mineru_content_list_ingest_creates_block_fragments_and_traceback_artifacts() {
    let app = authed_app();
    let content_list_v2 = json!([
        {
            "type": "title",
            "text": "MinerU Fixture",
            "text_level": 1,
            "page_idx": 0,
            "bbox": [0, 0, 500, 40],
            "reading_order": 0
        },
        {
            "type": "table",
            "html": "<table><tr><td>table-block-keyword</td></tr></table>",
            "table_caption": ["Revenue table caption"],
            "page_idx": 1,
            "bbox": [10, 20, 300, 160],
            "reading_order": 1
        },
        {
            "type": "equation",
            "latex": "E = mc^2 + equation-block-keyword",
            "page_idx": 2,
            "bbox": [25, 50, 280, 90],
            "reading_order": 2
        },
        {
            "type": "image",
            "img_path": "images/figure-1.png",
            "caption": ["Architecture image-block-keyword"],
            "page_idx": 3,
            "bbox": [40, 80, 420, 260],
            "reading_order": 3
        }
    ]);

    let (status, result) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": "u1",
            "source_id": "mineru-fixture",
            "revision_id": "v1",
            "title": "MinerU Fixture",
            "file_name": "fixture.pdf",
            "content_type": "application/pdf",
            "content": "raw-source-only-keyword is present only in the stored source document.",
            "content_list_v2": content_list_v2
        }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["task"]["state"], "completed");
    assert!(result["parse_artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|artifact| artifact["artifact_kind"] == "content_list_v2"));
    let task_id = result["task"]["task_id"].as_str().unwrap();
    let source_document_uri = result["source_document_uri"].as_str().unwrap();

    let (status, task) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}"),
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{task}");
    assert_eq!(task["state"], "completed");

    let (status, task_result) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}/result"),
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{task_result}");
    assert_eq!(task_result["parsed_blocks"].as_array().unwrap().len(), 4);

    let (status, table_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "table-block-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{table_search}");
    let table_hit = &table_search["hits"][0];
    assert_eq!(table_hit["block_type"], "table");
    assert_eq!(table_hit["page_idx"], 1);
    assert_eq!(table_hit["bbox"], json!([10, 20, 300, 160]));
    assert_eq!(table_hit["source_document_uri"], source_document_uri);

    let (status, equation_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "equation-block-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{equation_search}");
    assert_eq!(equation_search["hits"][0]["block_type"], "equation");
    assert!(equation_search["hits"][0]["snippet"]
        .as_str()
        .unwrap()
        .contains("E = mc^2"));

    let (status, image_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "image-block-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{image_search}");
    assert_eq!(image_search["hits"][0]["block_type"], "image");
    assert_eq!(
        image_search["hits"][0]["asset_refs"][0],
        "images/figure-1.png"
    );

    let fragment_uri = table_hit["uri"].as_str().unwrap();
    let (status, traceback) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/traceback",
        json!({ "uri": fragment_uri }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{traceback}");
    assert_eq!(traceback["source_document_uri"], source_document_uri);
    assert_eq!(traceback["block_type"], "table");
    assert_eq!(traceback["page_idx"], 1);
    assert!(traceback["artifact_refs"]
        .as_array()
        .unwrap()
        .iter()
        .any(|artifact| artifact["artifact_kind"] == "content_list_v2"));

    let (status, source_only_search) = call_with_token(
        app,
        Method::POST,
        "/v1/context/search",
        json!({ "query": "raw-source-only-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source_only_search}");
    assert_eq!(source_only_search["hits"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn parsed_ingest_update_supersedes_old_fragments_and_part_of_links() {
    let app = authed_app();
    let source_id = "parsed-update-fixture";
    let v1_blocks = json!([
        {
            "type": "paragraph",
            "text": "old-ingest-keyword should be removed after the active revision changes.",
            "page_idx": 0,
            "bbox": [1, 2, 3, 4],
            "reading_order": 0
        }
    ]);
    let (status, first) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "source_id": source_id,
            "revision_id": "v1",
            "title": "Parsed Update Fixture",
            "content": "source v1",
            "content_list_v2": v1_blocks
        }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{first}");

    let (status, old_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "old-ingest-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_search}");
    let old_fragment_uri = old_search["hits"][0]["uri"].as_str().unwrap();

    let (status, old_link) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "uri": old_fragment_uri, "direction": "outbound", "relations": ["part_of"], "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_link}");
    assert_eq!(old_link["outbound"].as_array().unwrap().len(), 1);

    let v2_blocks = json!([
        {
            "type": "paragraph",
            "text": "new-ingest-keyword replaces the old parsed block.",
            "page_idx": 0,
            "bbox": [5, 6, 7, 8],
            "reading_order": 0
        }
    ]);
    let (status, second) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "source_id": source_id,
            "revision_id": "v2",
            "title": "Parsed Update Fixture",
            "content": "source v2",
            "content_list_v2": v2_blocks
        }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{second}");

    let (status, old_after_update) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "old-ingest-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_after_update}");
    assert_eq!(old_after_update["hits"].as_array().unwrap().len(), 0);

    let (status, new_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "new-ingest-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{new_search}");
    assert_eq!(new_search["hits"][0]["block_type"], "paragraph");

    let (status, old_read) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(old_fragment_uri)),
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{old_read}");

    let (status, old_link_after_update) = call_with_token(
        app,
        Method::POST,
        "/v1/links/search",
        json!({ "uri": old_fragment_uri, "direction": "outbound", "relations": ["part_of"], "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{old_link_after_update}");
    assert_eq!(
        old_link_after_update["outbound"].as_array().unwrap().len(),
        0
    );
}

#[tokio::test]
async fn parse_artifacts_and_fragments_are_owner_scoped() {
    let app = authed_app();
    let blocks = json!([
        {
            "type": "paragraph",
            "text": "private-parse-artifact-keyword belongs to owner u1 only.",
            "page_idx": 0,
            "bbox": [0, 0, 100, 100],
            "reading_order": 0
        }
    ]);
    let (status, result) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": "u1",
            "source_id": "private-parse-fixture",
            "revision_id": "v1",
            "title": "Private Parse Fixture",
            "content": "private source",
            "content_list_v2": blocks
        }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    let task_id = result["task"]["task_id"].as_str().unwrap();

    let (status, u1_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "private-parse-artifact-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{u1_search}");
    let fragment_uri = u1_search["hits"][0]["uri"].as_str().unwrap();

    let (status, u2_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "private-parse-artifact-keyword", "limit": 5 }),
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{u2_search}");
    assert_eq!(u2_search["hits"].as_array().unwrap().len(), 0);

    let (status, u2_result) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}/result"),
        Value::Null,
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{u2_result}");

    let (status, u2_traceback) = call_with_token(
        app,
        Method::POST,
        "/v1/context/traceback",
        json!({ "uri": fragment_uri }),
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{u2_traceback}");
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
async fn state_fact_document_creates_personal_fragments_and_enforces_traceback_acl() {
    let app = authed_app();
    let content = format!(
        "# Personal Status\n\n{}",
        "personal-fragment-keyword records detailed private status evidence. ".repeat(50)
    );

    let (status, state) = call_with_token(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/status-doc",
        json!({
            "state_type": "status",
            "title": "Status summary",
            "statement": "Current status summary only.",
            "document": {
                "content": content,
                "content_type": "text/markdown",
                "source_uri": "https://example.test/personal/status"
            }
        }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state}");
    assert_eq!(state["item"]["statement"], "Current status summary only.");
    assert!(!state["item"]["statement"]
        .as_str()
        .unwrap()
        .contains("personal-fragment-keyword"));
    let source_ref = &state["item"]["source_refs"][0];
    let source_document_uri = source_ref["meta"]["source_document_uri"].as_str().unwrap();
    assert!(
        source_ref["meta"]["fragment_uris"]
            .as_array()
            .unwrap()
            .len()
            > 1
    );

    let (status, search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "personal-fragment-keyword", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    let fragment_uri = search["hits"][0]["uri"].as_str().unwrap();
    assert_eq!(search["hits"][0]["node_kind"], "fragment");
    assert_eq!(
        search["hits"][0]["source_document_uri"].as_str().unwrap(),
        source_document_uri
    );

    let (status, traceback) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/traceback",
        json!({ "uri": fragment_uri }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{traceback}");
    assert_eq!(traceback["source_document_uri"], source_document_uri);

    let (status, cross_traceback) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/traceback",
        json!({ "uri": fragment_uri }),
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{cross_traceback}");

    let (status, cross_read) = call_with_token(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(source_document_uri)),
        Value::Null,
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{cross_read}");

    let (status, admin_traceback) = call_with_token(
        app,
        Method::POST,
        "/v1/context/traceback",
        json!({ "uri": fragment_uri }),
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{admin_traceback}");
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
async fn usage_returns_full_provider_snapshots_and_owner_scope() {
    let app = authed_app();
    for (owner, token, text) in [
        ("u1", "u1-token", "u1 scoped usage signal"),
        ("u2", "u2-token", "u2 scoped usage signal"),
    ] {
        let (status, body) = call_with_token(
            app.clone(),
            Method::POST,
            &format!("/v1/history/users/{owner}/events"),
            event_body(owner, &format!("usage-{owner}"), text),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    let (status, owner_usage) = call_with_token(
        app.clone(),
        Method::GET,
        "/v1/usage",
        Value::Null,
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{owner_usage}");
    assert_eq!(owner_usage["scope"]["owner_user_id"], "u1");
    assert_eq!(owner_usage["scope"]["global"], false);
    let providers = owner_usage["providers"].as_object().unwrap();
    for provider in [
        "nowledge_api",
        "meilisearch",
        "llm",
        "rag",
        "link_graph",
        "history_events",
        "contextfs",
        "structured_data",
        "sessions",
    ] {
        assert!(providers.contains_key(provider), "missing {provider}");
    }
    assert_eq!(owner_usage["providers"]["history_events"]["event_count"], 1);

    let (status, admin_usage) = call_with_token(
        app,
        Method::GET,
        "/v1/usage",
        Value::Null,
        Some("admin-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{admin_usage}");
    assert_eq!(admin_usage["scope"]["global"], true);
    assert_eq!(admin_usage["providers"]["history_events"]["event_count"], 2);
}

#[tokio::test]
async fn usage_rejects_unbound_non_admin_owner_selection() {
    let app = bearer_user_app();
    let (status, body) = call_with_token(
        app,
        Method::GET,
        "/v1/usage?owner_user_id=u1",
        Value::Null,
        Some("user-token"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
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
async fn link_graph_records_bidirectional_backlinks_and_owner_scope() {
    let app = authed_app();
    for (entity, text) in [
        ("link-a", "alpha-link-a customer onboarding note"),
        ("link-b", "alpha-link-b onboarding risk note"),
    ] {
        let (status, event) = call_with_token(
            app.clone(),
            Method::POST,
            "/v1/history/users/u1/events",
            event_body("u1", entity, text),
            Some("u1-token"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{event}");
    }

    let (status, source_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "alpha-link-a", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source_search}");
    let source_uri = source_search["hits"][0]["uri"].as_str().unwrap();

    let (status, target_search) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "alpha-link-b", "limit": 5 }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{target_search}");
    let target_uri = target_search["hits"][0]["uri"].as_str().unwrap();

    let (status, created) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "source_uri": source_uri,
            "target_uri": target_uri,
            "relation": "supports",
            "rationale": "manual backlink regression"
        }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["decision"], "created");
    assert!(created.get("history_event_id").is_some());
    let canonical_source = created["link"]["source_uri"].as_str().unwrap();
    let canonical_target = created["link"]["target_uri"].as_str().unwrap();
    assert!(!canonical_source.ends_with("/.abstract"));

    let (status, backlinks) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "uri": target_uri, "direction": "backlinks" }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{backlinks}");
    assert_eq!(backlinks["backlinks"].as_array().unwrap().len(), 1);
    assert_eq!(backlinks["backlinks"][0]["source_uri"], canonical_source);

    let (status, outbound) = call_with_token(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "uri": source_uri, "direction": "outbound" }),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{outbound}");
    assert_eq!(outbound["outbound"].as_array().unwrap().len(), 1);
    assert_eq!(outbound["outbound"][0]["target_uri"], canonical_target);

    let (status, cross_owner) = call_with_token(
        app,
        Method::POST,
        "/v1/links/search",
        json!({ "query": "manual backlink regression" }),
        Some("u2-token"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cross_owner}");
    assert_eq!(cross_owner["links"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn analysis_api_uses_independent_model_and_materializes_links_and_insights() {
    let app = analysis_llm_app();
    for (entity, text) in [
        (
            "analysis-a",
            "analysis-key launch plan depends on API readiness",
        ),
        (
            "analysis-b",
            "analysis-key API readiness depends on support staffing",
        ),
    ] {
        let (status, event) = call(
            app.clone(),
            Method::POST,
            "/v1/history/users/u1/events",
            event_body("u1", entity, text),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{event}");
    }

    let (status, analysis) = call(
        app.clone(),
        Method::POST,
        "/v1/analysis/insights",
        json!({
            "owner_user_id": "u1",
            "query": "analysis-key API readiness",
            "create_links": true,
            "upsert_insights": true,
            "context_limit": 8
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{analysis}");
    assert_eq!(analysis["usage"]["provider"], "mock");
    assert_eq!(analysis["usage"]["model"], "gpt-5.3-codex-spark");
    assert!(!analysis["link_candidates"].as_array().unwrap().is_empty());
    assert!(!analysis["created_links"].as_array().unwrap().is_empty());
    assert!(!analysis["insights"].as_array().unwrap().is_empty());

    let (status, links) = call(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "owner_user_id": "u1", "query": "analysis-key API readiness" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{links}");
    assert!(!links["links"].as_array().unwrap().is_empty());

    let (status, insights) = call(
        app,
        Method::POST,
        "/v1/state/insights/search",
        json!({ "owner_user_id": "u1", "query": "analysis-key API readiness" }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{insights}");
    assert!(!insights["hits"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn history_event_analysis_is_constrained_to_same_event_index() {
    let app = analysis_llm_app();
    let (status, selected) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        event_body(
            "u1",
            "history-scope-a",
            "same-index-insight selected user one event",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{selected}");
    let selected_id = selected["event"]["id"].as_str().unwrap();
    let u1_index = selected["event"]["event_index_uid"].as_str().unwrap();

    let (status, related) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        event_body(
            "u1",
            "history-scope-b",
            "same-index-insight related user one event",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{related}");

    let (status, cross_owner) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u2/events",
        event_body(
            "u2",
            "history-scope-c",
            "same-index-insight cross-index-secret user two event",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{cross_owner}");
    let u2_index = cross_owner["event"]["event_index_uid"].as_str().unwrap();
    assert_ne!(u1_index, u2_index);

    let (status, analysis) = call(
        app,
        Method::POST,
        "/v1/analysis/insights",
        json!({
            "owner_user_id": "u1",
            "history_event_id": selected_id,
            "query": "same-index-insight",
            "create_links": true,
            "upsert_insights": true,
            "context_limit": 8
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{analysis}");
    assert_eq!(analysis["history_event_id"], selected_id);
    assert_eq!(analysis["event_index_uid"], u1_index);
    assert_eq!(analysis["usage"]["history_scope"]["mode"], "same_index");
    assert_eq!(
        analysis["usage"]["history_scope"]["event_index_uid"],
        u1_index
    );
    assert!(!analysis["context_hits"].as_array().unwrap().is_empty());
    assert!(analysis["context_hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["uri"].as_str().unwrap().contains("/history/")));
    let rendered = analysis.to_string();
    assert!(!rendered.contains("cross-index-secret"));
    assert!(!rendered.contains(u2_index));
}

#[tokio::test]
async fn codex_auth_import_route_is_not_exposed_or_token_safe() {
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
    assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
    assert!(!body.to_string().contains(token));
}

#[tokio::test]
async fn codex_auth_reader_accepts_cli_openai_api_key_shape() {
    let token = "sk-codex-cli-shape-token";
    let path =
        std::env::temp_dir().join(format!("nowledge-codex-auth-{}.json", uuid::Uuid::now_v7()));
    std::fs::write(
        &path,
        json!({
            "auth_mode": "apikey",
            "OPENAI_API_KEY": token,
            "tokens": { "access_token": "nested-token" }
        })
        .to_string(),
    )
    .unwrap();

    let read = nowledge::llm::read_codex_auth_token(&path.to_string_lossy());
    let _ = std::fs::remove_file(&path);
    assert_eq!(read.as_deref(), Some(token));
}
