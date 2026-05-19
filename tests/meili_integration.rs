use std::{
    net::{TcpStream, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use tower::ServiceExt;

async fn meili_app() -> Option<Router> {
    let url = std::env::var("RAG_TEST_MEILI_URL").ok()?;
    if !meili_available(&url) {
        eprintln!("skipping Meilisearch integration test; {url} is not reachable");
        return None;
    }
    let api_key = std::env::var("RAG_TEST_MEILI_API_KEY").ok();
    if api_key.is_none() {
        let response = reqwest::Client::new()
            .get(format!("{}/indexes", url.trim_end_matches('/')))
            .send()
            .await;
        if response
            .as_ref()
            .is_ok_and(|response| response.status().as_u16() == 401)
        {
            eprintln!(
                "skipping Meilisearch integration test; server requires a key, set RAG_TEST_MEILI_API_KEY"
            );
            return None;
        }
    }
    let mut config = Config::test();
    config.tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.meili_api_key = api_key;
    config.meili_wait_for_tasks = true;
    Some(build_router(AppState::new(Arc::new(config))))
}

fn meili_available(url: &str) -> bool {
    let authority = url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or_default();
    let address = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:7700")
    };
    let Ok(addrs) = address.to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok())
}

async fn meili_config_with_tenant(tenant_id: String) -> Option<Config> {
    let url = std::env::var("RAG_TEST_MEILI_URL").ok()?;
    if !meili_available(&url) {
        eprintln!("skipping Meilisearch integration test; {url} is not reachable");
        return None;
    }
    let api_key = std::env::var("RAG_TEST_MEILI_API_KEY").ok();
    if api_key.is_none() {
        let response = reqwest::Client::new()
            .get(format!("{}/indexes", url.trim_end_matches('/')))
            .send()
            .await;
        if response
            .as_ref()
            .is_ok_and(|response| response.status().as_u16() == 401)
        {
            eprintln!(
                "skipping Meilisearch integration test; server requires a key, set RAG_TEST_MEILI_API_KEY"
            );
            return None;
        }
    }
    let mut config = Config::test();
    config.tenant_id = tenant_id;
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.meili_api_key = api_key;
    config.meili_wait_for_tasks = true;
    Some(config)
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
    let Some(app) = meili_app().await else {
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
    let Some(app) = meili_app().await else {
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
    let Some(app) = meili_app().await else {
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

    let (status, source_docs) = call(
        app.clone(),
        Method::POST,
        "/v1/debug/meili/search",
        json!({ "index_uid": "rag_source_documents", "query": source_document_uri }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source_docs}");
    assert!(source_docs["hits"].as_array().unwrap().iter().any(|hit| {
        hit["uri"].as_str() == Some(source_document_uri) && hit["retrieval_enabled"] == false
    }));

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

#[tokio::test]
async fn meili_bootstrap_creates_harness_indexes_and_indexes_changes() {
    let Some(app) = meili_app().await else {
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
    let indexes = bootstrap["indexes"].as_array().unwrap();
    for uid in [
        "rag_harness_components",
        "rag_harness_changes",
        "rag_harness_verdicts",
    ] {
        assert!(indexes.iter().any(|index| index.as_str() == Some(uid)));
    }

    let (status, change) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/harness/evolution/changes",
        json!({
            "iteration": 1,
            "type": "improvement",
            "component_id": "retrieval.context_search",
            "files": ["src/store.rs"],
            "failure_pattern": "meili_harness_change_pattern",
            "root_cause": "test",
            "targeted_fix": "test",
            "predicted_fixes": ["meili_harness_change_pattern"],
            "risk_cases": [],
            "expected_metric_deltas": { "pass_rate": 1.0 },
            "why_this_component": "test",
            "created_by": "test"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{change}");

    let (status, search) = call(
        app,
        Method::POST,
        "/v1/debug/meili/search",
        json!({
            "index_uid": "rag_harness_changes",
            "query": "meili_harness_change_pattern"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert!(search["hits"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| { hit["id"].as_str() == change["id"].as_str() }));
}

#[tokio::test]
async fn meili_hydrates_harness_eval_and_ingest_metadata_into_fresh_app() {
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let app = build_router(AppState::new(Arc::new(config.clone())));
    let (status, bootstrap) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{bootstrap}");

    let (status, change) = call(
        app.clone(),
        Method::POST,
        "/v1/admin/harness/evolution/changes",
        json!({
            "iteration": 1,
            "type": "improvement",
            "component_id": "retrieval.context_search",
            "files": ["src/store.rs"],
            "failure_pattern": "hydration_failure_pattern",
            "root_cause": "test",
            "targeted_fix": "test",
            "predicted_fixes": ["hydration-eval-case"],
            "risk_cases": [],
            "expected_metric_deltas": { "pass_rate": 1.0 },
            "why_this_component": "test",
            "created_by": "test"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{change}");
    let change_id = change["id"].as_str().unwrap();

    let (status, eval_case) = call(
        app.clone(),
        Method::POST,
        "/v1/eval/cases",
        json!({
            "id": "hydration-eval-case",
            "question": "hydration eval should persist",
            "expected_answer_contains": ["unlikely-token"]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval_case}");

    let (status, eval_run) = call(
        app.clone(),
        Method::POST,
        "/v1/eval/runs",
        json!({ "case_ids": ["hydration-eval-case"], "change_id": change_id }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{eval_run}");
    let run_id = eval_run["id"].as_str().unwrap();

    let (status, ingest) = call(
        app,
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "source_id": "hydration-ingest-fixture",
            "revision_id": "v1",
            "title": "Hydration Ingest Fixture",
            "content": "hydration ingest metadata should persist"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ingest}");
    let task_id = ingest["task"]["task_id"].as_str().unwrap();

    let fresh = build_router(AppState::new(Arc::new(config)));
    let (status, hydrated) = call(
        fresh.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hydrated}");
    assert!(
        hydrated["hydrated"]["harness_changes"]
            .as_u64()
            .unwrap_or_default()
            >= 1
    );

    let (status, hydrated_change) = call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/admin/harness/evolution/changes/{change_id}"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hydrated_change}");
    assert_eq!(hydrated_change["id"], change["id"]);

    let (status, hydrated_report) = call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/eval/runs/{run_id}/report"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hydrated_report}");
    assert_eq!(hydrated_report["run"]["id"], eval_run["id"]);
    assert_eq!(
        hydrated_report["case_results"][0]["case_id"],
        "hydration-eval-case"
    );

    let (status, hydrated_result) = call(
        fresh,
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}/result"),
        Value::Null,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{hydrated_result}");
    assert_eq!(hydrated_result["task"]["task_id"], task_id);
}
