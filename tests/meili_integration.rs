use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use tower::ServiceExt;

fn meili_app() -> Option<Router> {
    let url = std::env::var("RAG_TEST_MEILI_URL").ok()?;
    let mut config = Config::test();
    config.tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.meili_api_key = std::env::var("RAG_TEST_MEILI_API_KEY").ok();
    config.meili_wait_for_tasks = true;
    Some(build_router(AppState::new(Arc::new(config))))
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

#[tokio::test]
async fn meili_backend_creates_dynamic_user_indexes_and_searches_events() {
    let Some(app) = meili_app() else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };

    let (status, bootstrap) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bootstrap}");

    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let (status, ensured) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/history/users/{owner}/event-index"),
        json!({ "create_personal_context_index": true }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ensured}");
    assert!(!ensured["routing"]["event_index_uid"]
        .as_str()
        .unwrap()
        .contains(&owner));
    assert!(!ensured["meili_task_uids"].as_array().unwrap().is_empty());
    assert!(ensured["meili_task_uids"]
        .as_array()
        .unwrap()
        .iter()
        .all(|uid| uid.as_str().unwrap().chars().all(|c| c.is_ascii_digit())));

    let text = format!("meili-only-keyword-{}", uuid::Uuid::now_v7());
    let (status, event) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/history/users/{owner}/events"),
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "n1",
            "owner_user_id": owner,
            "occurred_at": "2026-05-12T00:00:00Z",
            "observed_at": "2026-05-12T00:01:00Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "n1" },
            "text": text,
            "privacy": "private"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");
    assert!(event["meili_task_uid"].as_str().is_some());
    assert!(event["meili_task_uid"]
        .as_str()
        .unwrap()
        .chars()
        .all(|c| c.is_ascii_digit()));

    let (status, search) = call(
        app,
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "query": text, "limit": 5 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn meili_backend_indexes_internal_state_events() {
    let Some(app) = meili_app() else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };

    let (status, bootstrap) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bootstrap}");

    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let fact_key = format!("state-{}", uuid::Uuid::now_v7());
    let (status, state) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "preference",
            "title": "Meili state event",
            "statement": "state changed should be indexed in Meili"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state}");

    let (status, history) = call(
        app,
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "event_types": ["state.changed"], "limit": 10 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_eq!(history["hits"].as_array().unwrap().len(), 1);
    assert_eq!(history["hits"][0]["event_type"], "state.changed");
}

#[tokio::test]
async fn meili_backend_context_search_retrieves_fragments_only() {
    let Some(app) = meili_app() else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };

    let (status, bootstrap) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bootstrap}");

    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let fact_key = format!("doc-{}", uuid::Uuid::now_v7());
    let keyword = format!("meili-fragment-keyword-{}", uuid::Uuid::now_v7());
    let content = format!("# Meili Source\n\n{}", format!("{keyword} ").repeat(180));
    let (status, state) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "status",
            "title": "Meili source document",
            "statement": "Short current-state summary",
            "document": {
                "content": content,
                "content_type": "text/markdown",
                "source_uri": "https://example.test/meili/source"
            }
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state}");
    let source_document_uri = state["item"]["source_refs"][0]["meta"]["source_document_uri"]
        .as_str()
        .unwrap();

    let (status, search) = call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({ "owner_user_id": owner, "query": keyword, "limit": 5 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert!(!search["hits"].as_array().unwrap().is_empty(), "{search}");
    assert!(search["hits"].as_array().unwrap().iter().all(|hit| {
        hit["node_kind"] == "fragment"
            && hit["retrieval_role"] == "fragment"
            && hit["uri"].as_str() != Some(source_document_uri)
    }));
}
