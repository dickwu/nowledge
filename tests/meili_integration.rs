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
use nowledge::{
    build_router,
    error::ApiError,
    meili::{task_uid, MeiliAdmin},
    models::ContextNode,
    repository::{KnowledgeRepository, MeiliRepository},
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

static LIVE_MEILI_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn live_meili_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    LIVE_MEILI_TEST_LOCK.lock().await
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

async fn bootstrapped_meili_admin(config: &Config) -> MeiliAdmin {
    let admin = MeiliAdmin::from_config(config);
    let bootstrap = admin
        .bootstrap(false)
        .await
        .expect("Meilisearch bootstrap should succeed");
    let task_uids = bootstrap
        .tasks
        .iter()
        .filter_map(task_uid)
        .collect::<Vec<_>>();
    admin
        .wait_for_tasks(&task_uids)
        .await
        .expect("Meilisearch bootstrap tasks should complete");
    admin
}

async fn meili_fixture() -> Option<(Config, MeiliAdmin, AppState, Router)> {
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let config = meili_config_with_tenant(tenant_id).await?;
    let admin = bootstrapped_meili_admin(&config).await;
    let state = AppState::new(Arc::new(config.clone()));
    let app = build_router(state.clone());
    Some((config, admin, state, app))
}

fn equality_filter(field: &str, value: &str) -> String {
    format!(
        "{field} = {}",
        serde_json::to_string(value).expect("filter value should serialize")
    )
}

async fn wait_for_optional_task(
    admin: &MeiliAdmin,
    task_uid: Option<String>,
) -> Result<(), String> {
    if let Some(task_uid) = task_uid {
        admin
            .wait_for_task(&task_uid)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn delete_by_filter_and_wait(
    admin: &MeiliAdmin,
    index_uid: &str,
    filter: &str,
) -> Result<(), String> {
    let task_uid = admin
        .delete_documents_by_filter(index_uid, filter)
        .await
        .map_err(|error| error.to_string())?;
    wait_for_optional_task(admin, task_uid).await
}

async fn delete_by_ids_and_wait(
    admin: &MeiliAdmin,
    index_uid: &str,
    ids: &[String],
) -> Result<(), String> {
    let task_uid = admin
        .delete_documents_by_ids(index_uid, ids)
        .await
        .map_err(|error| error.to_string())?;
    wait_for_optional_task(admin, task_uid).await
}

async fn delete_index_and_wait(
    config: &Config,
    admin: &MeiliAdmin,
    index_uid: &str,
) -> Result<(), String> {
    let Some(url) = config.meili_url.as_deref() else {
        return Ok(());
    };
    let client = reqwest::Client::new();
    let endpoint = format!("{}/indexes/{index_uid}", url.trim_end_matches('/'));
    let mut inspect = client.get(&endpoint);
    if let Some(api_key) = config.meili_api_key.as_deref() {
        inspect = inspect.bearer_auth(api_key);
    }
    let inspection = inspect.send().await.map_err(|error| error.to_string())?;
    if inspection.status().as_u16() == 404 {
        return Ok(());
    }
    if !inspection.status().is_success() {
        return Err(format!(
            "test-owned Meilisearch index cleanup inspection should succeed: {}",
            inspection.status()
        ));
    }

    let mut request = client.delete(endpoint);
    if let Some(api_key) = config.meili_api_key.as_deref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    if response.status().as_u16() == 404 {
        return Ok(());
    }
    if !response.status().is_success() {
        return Err(format!(
            "test-owned Meilisearch index cleanup should be accepted: {}",
            response.status()
        ));
    }
    let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
    wait_for_optional_task(admin, task_uid(&body)).await
}

fn assert_cleanup_results(results: Vec<(&'static str, Result<(), String>)>) {
    let failures = results
        .into_iter()
        .filter_map(|(scope, result)| result.err().map(|error| format!("{scope}: {error}")))
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "live Meilisearch fixture cleanup failed: {}",
        failures.join("; ")
    );
}

fn company_context_document(tenant_id: &str, run_id: &str, ordinal: usize) -> Value {
    let parent_uri = format!("ctx://company/pr1-cap-{run_id}");
    let uri = format!("{parent_uri}/node-{ordinal:04}");
    let node = ContextNode {
        uri: uri.clone(),
        title: format!("PR1 cap node {ordinal}"),
        layer: 2,
        body: format!("PR1 company-context cap characterization node {ordinal}"),
        tenant_id: tenant_id.to_string(),
        owner_user_id: None,
        index_uid: "rag_company_context".to_string(),
        index_kind: "company".to_string(),
        ancestor_uris: vec!["ctx://company".to_string(), parent_uri.clone()],
        node_kind: "fragment".to_string(),
        retrieval_role: "fragment".to_string(),
        retrieval_enabled: true,
        parent_uri: Some(parent_uri),
        source_document_uri: None,
        fragment_index: Some(ordinal as u32),
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
        privacy: "company".to_string(),
        updated_at: chrono::Utc::now(),
    };
    let mut document = serde_json::to_value(node).expect("ContextNode should serialize");
    document
        .as_object_mut()
        .expect("ContextNode should serialize as an object")
        .insert(
            "id".to_string(),
            json!(format!("pr1-cap-{run_id}-{ordinal}")),
        );
    document
}

async fn try_call(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
) -> Result<(StatusCode, Value), String> {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .map_err(|error| format!("request construction failed: {error}"))?;
    let response = app
        .oneshot(request)
        .await
        .map_err(|error| format!("router request failed: {error}"))?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|error| format!("response body failed: {error}"))?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).map_err(|error| format!("response JSON failed: {error}"))?
    };
    Ok((status, value))
}

#[tokio::test]
async fn meili_company_context_hydration_characterizes_silent_1000_cap() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let admin = bootstrapped_meili_admin(&config).await;
    let repository = MeiliRepository::new(admin.clone(), true);
    let run_id = uuid::Uuid::now_v7().to_string();
    let documents = (0..1001)
        .map(|ordinal| company_context_document(&tenant_id, &run_id, ordinal))
        .collect::<Vec<_>>();

    let add_task_result = admin.add_documents("rag_company_context", &documents).await;
    let wait_result = match &add_task_result {
        Ok(task_uid) => wait_for_optional_task(&admin, task_uid.clone()).await,
        Err(error) => Err(format!("write was not accepted: {error}")),
    };
    let loaded = if wait_result.is_ok() {
        Some(repository.list_company_context_nodes(&tenant_id).await)
    } else {
        None
    };
    let cleanup = delete_by_filter_and_wait(
        &admin,
        "rag_company_context",
        &equality_filter("tenant_id", &tenant_id),
    )
    .await;

    assert_cleanup_results(vec![("company context cap fixture", cleanup)]);
    add_task_result.expect("company ContextNode documents should be accepted");
    wait_result.expect("company ContextNode write should complete");

    let loaded = loaded
        .expect("company ContextNode scan should run after a completed write")
        .expect("company ContextNode scan should succeed")
        .expect("Meili repository should return persisted company ContextNodes");
    assert_eq!(
        loaded.len(),
        1000,
        "the current repository scan silently truncates the 1001st company ContextNode"
    );
    assert!(loaded.iter().all(|node| node.tenant_id == tenant_id));
}

#[tokio::test]
async fn meili_backend_creates_dynamic_user_indexes_and_searches_events() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");
    let ensured_result = try_call(
        app.clone(),
        Method::PUT,
        &format!("/v1/history/users/{owner}/event-index"),
        json!({ "create_personal_context_index": true }),
    )
    .await;

    let text = format!("meili-only-keyword-{}", uuid::Uuid::now_v7());
    let event_result = try_call(
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
    let search_result = try_call(
        app,
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "query": text, "limit": 5 }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (ensure_status, ensured) = ensured_result.expect("index ensure call should finish");
    let (event_status, event) = event_result.expect("event write call should finish");
    let (search_status, search) = search_result.expect("event search call should finish");
    assert_eq!(ensure_status, StatusCode::OK, "{ensured}");
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
    assert_eq!(event_status, StatusCode::OK, "{event}");
    assert!(event["meili_task_uid"]
        .as_str()
        .is_some_and(|uid| uid.chars().all(|character| character.is_ascii_digit())));
    assert_eq!(search_status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn meili_backend_indexes_internal_state_events() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let fact_key = format!("state-{}", uuid::Uuid::now_v7());
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");
    let state_result = try_call(
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
    let history_result = try_call(
        app,
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "event_types": ["state.changed"], "limit": 10 }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "state items",
            delete_by_filter_and_wait(&admin, "rag_state_items", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (state_status, state_body) = state_result.expect("state write call should finish");
    let (history_status, history) = history_result.expect("history search call should finish");
    assert_eq!(state_status, StatusCode::OK, "{state_body}");
    assert_eq!(history_status, StatusCode::OK, "{history}");
    assert_eq!(history["hits"].as_array().unwrap().len(), 1);
    assert_eq!(history["hits"][0]["event_type"], "state.changed");
}

#[tokio::test]
async fn meili_backend_context_search_retrieves_fragments_only() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let fact_key = format!("doc-{}", uuid::Uuid::now_v7());
    let keyword = format!("meili-fragment-keyword-{}", uuid::Uuid::now_v7());
    let content = format!("# Meili Source\n\n{}", format!("{keyword} ").repeat(180));
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");
    let state_result = try_call(
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
    let source_document_uri = state_result
        .as_ref()
        .ok()
        .and_then(|(_, state)| {
            state["item"]["source_refs"][0]["meta"]["source_document_uri"].as_str()
        })
        .unwrap_or("ctx://missing-source-document")
        .to_string();

    let source_docs_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/debug/meili/search",
        json!({ "index_uid": "rag_source_documents", "query": source_document_uri }),
    )
    .await;

    let search_result = try_call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({ "owner_user_id": owner, "query": keyword, "limit": 5 }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "state items",
            delete_by_filter_and_wait(&admin, "rag_state_items", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (state_status, state_body) = state_result.expect("state document call should finish");
    let (source_docs_status, source_docs) =
        source_docs_result.expect("source document search should finish");
    let (search_status, search) = search_result.expect("context search should finish");
    assert_eq!(state_status, StatusCode::OK, "{state_body}");
    assert_ne!(source_document_uri, "ctx://missing-source-document");
    assert_eq!(source_docs_status, StatusCode::OK, "{source_docs}");
    assert!(source_docs["hits"].as_array().unwrap().iter().any(|hit| {
        hit["uri"].as_str() == Some(source_document_uri.as_str())
            && hit["retrieval_enabled"] == false
    }));
    assert_eq!(search_status, StatusCode::OK, "{search}");
    assert!(!search["hits"].as_array().unwrap().is_empty(), "{search}");
    assert!(search["hits"].as_array().unwrap().iter().all(|hit| {
        hit["node_kind"] == "fragment"
            && hit["retrieval_role"] == "fragment"
            && hit["uri"].as_str() != Some(source_document_uri.as_str())
    }));
}

#[tokio::test]
async fn meili_backend_context_search_applies_structured_filters() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let source_a = format!("meili-filter-a-{}", uuid::Uuid::now_v7());
    let source_b = format!("meili-filter-b-{}", uuid::Uuid::now_v7());
    let keyword = format!("meili-filter-keyword-{}", uuid::Uuid::now_v7());
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");

    let ingest_a_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": owner,
            "source_id": source_a,
            "revision_id": "v1",
            "title": "Meili Filter A",
            "content": "source a",
            "content_list_v2": [
                {
                    "type": "table",
                    "html": format!("<table><tr><td>{keyword} table row</td></tr></table>"),
                    "page_idx": 1,
                    "bbox": [0, 0, 10, 10],
                    "reading_order": 0
                },
                {
                    "type": "paragraph",
                    "text": format!("{keyword} paragraph page three"),
                    "page_idx": 3,
                    "bbox": [1, 1, 11, 11],
                    "reading_order": 1
                }
            ]
        }),
    )
    .await;

    let ingest_b_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": owner,
            "source_id": source_b,
            "revision_id": "v1",
            "title": "Meili Filter B",
            "content": "source b",
            "content_list_v2": [
                {
                    "type": "table",
                    "html": format!("<table><tr><td>{keyword} other table</td></tr></table>"),
                    "page_idx": 1,
                    "bbox": [2, 2, 12, 12],
                    "reading_order": 0
                }
            ]
        }),
    )
    .await;

    let table_search_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": owner,
            "query": keyword,
            "filters": { "block_type": "table", "source_id": source_a },
            "limit": 10
        }),
    )
    .await;

    let page_search_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": owner,
            "query": keyword,
            "filters": { "page_idx_gte": 3, "page_idx_lte": 3 },
            "limit": 10
        }),
    )
    .await;

    let debug_search_result = try_call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": owner,
            "query": keyword,
            "debug": true,
            "limit": 5
        }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "ingest tasks",
            delete_by_filter_and_wait(&admin, "rag_ingest_tasks", &tenant_filter).await,
        ),
        (
            "ingest results",
            delete_by_filter_and_wait(&admin, "rag_ingest_results", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (ingest_a_status, ingest_a) = ingest_a_result.expect("first ingest call should finish");
    let (ingest_b_status, ingest_b) = ingest_b_result.expect("second ingest call should finish");
    let (table_status, table_search) =
        table_search_result.expect("table-filter search should finish");
    let (page_status, page_search) = page_search_result.expect("page-filter search should finish");
    let (debug_status, debug_search) =
        debug_search_result.expect("debug context search should finish");
    assert_eq!(ingest_a_status, StatusCode::OK, "{ingest_a}");
    assert_eq!(ingest_b_status, StatusCode::OK, "{ingest_b}");
    assert_eq!(table_status, StatusCode::OK, "{table_search}");
    assert_eq!(table_search["hits"].as_array().unwrap().len(), 1);
    assert_eq!(table_search["hits"][0]["block_type"], "table");
    assert_eq!(table_search["hits"][0]["source_id"], source_a);
    assert!(!table_search.to_string().contains("index_uid"));
    assert!(!table_search.to_string().contains("\"filter\""));
    assert_eq!(page_status, StatusCode::OK, "{page_search}");
    assert!(!page_search["hits"].as_array().unwrap().is_empty());
    assert!(page_search["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["page_idx"] == 3));
    assert_eq!(debug_status, StatusCode::OK, "{debug_search}");
    assert!(debug_search["stages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|stage| stage.get("raw_stage_debug").is_some()));
}

#[tokio::test]
async fn meili_bootstrap_creates_harness_indexes_and_indexes_changes() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, _state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();

    let bootstrap_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;

    let change_result = try_call(
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

    let search_result = try_call(
        app,
        Method::POST,
        "/v1/debug/meili/search",
        json!({
            "index_uid": "rag_harness_changes",
            "query": "meili_harness_change_pattern"
        }),
    )
    .await;

    let cleanup = delete_by_filter_and_wait(
        &admin,
        "rag_harness_changes",
        &equality_filter("tenant_id", &tenant_id),
    )
    .await;
    assert_cleanup_results(vec![("harness changes", cleanup)]);

    let (bootstrap_status, bootstrap) = bootstrap_result.expect("bootstrap call should finish");
    let (change_status, change) = change_result.expect("harness change call should finish");
    let (search_status, search) = search_result.expect("harness search call should finish");
    assert_eq!(bootstrap_status, StatusCode::OK, "{bootstrap}");
    let indexes = bootstrap["indexes"].as_array().unwrap();
    for uid in [
        "rag_harness_components",
        "rag_harness_changes",
        "rag_harness_verdicts",
    ] {
        assert!(indexes.iter().any(|index| index.as_str() == Some(uid)));
    }
    assert_eq!(change_status, StatusCode::OK, "{change}");
    assert_eq!(search_status, StatusCode::OK, "{search}");
    assert!(search["hits"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| { hit["id"].as_str() == change["id"].as_str() }));
}

#[tokio::test]
async fn meili_hydrates_harness_eval_and_ingest_metadata_into_fresh_app() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let fixture_id = uuid::Uuid::now_v7().to_string();
    let eval_case_id = format!("hydration-eval-case-{fixture_id}");
    let source_id = format!("hydration-ingest-fixture-{fixture_id}");
    let parsed_block_marker = format!("hydration-parsed-block-{fixture_id}");
    let company_routing = state
        .store
        .resolver()
        .resolve(&tenant_id, "company", false, true)
        .expect("company routing should resolve");

    let bootstrap_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;

    let change_result = try_call(
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
            "predicted_fixes": [eval_case_id],
            "risk_cases": [],
            "expected_metric_deltas": { "pass_rate": 1.0 },
            "why_this_component": "test",
            "created_by": "test"
        }),
    )
    .await;
    let change_id = change_result
        .as_ref()
        .ok()
        .and_then(|(_, change)| change["id"].as_str())
        .unwrap_or("missing-change")
        .to_string();

    let eval_case_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/eval/cases",
        json!({
            "id": eval_case_id,
            "question": "hydration eval should persist",
            "expected_answer_contains": ["unlikely-token"]
        }),
    )
    .await;

    let eval_run_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/eval/runs",
        json!({ "case_ids": [eval_case_id], "change_id": change_id }),
    )
    .await;
    let run_id = eval_run_result
        .as_ref()
        .ok()
        .and_then(|(_, run)| run["id"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("missing-run-{fixture_id}"));

    let ingest_result = try_call(
        app,
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "source_id": source_id,
            "revision_id": "v1",
            "title": "Hydration Ingest Fixture",
            "content": "hydration ingest metadata should persist",
            "content_list_v2": [{
                "type": "paragraph",
                "text": parsed_block_marker,
                "page_idx": 1,
                "reading_order": 0
            }]
        }),
    )
    .await;
    let task_id = ingest_result
        .as_ref()
        .ok()
        .and_then(|(_, ingest)| ingest["task"]["task_id"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("missing-task-{fixture_id}"));

    let fresh = build_router(AppState::new(Arc::new(config.clone())));
    let hydrated_result = try_call(
        fresh.clone(),
        Method::POST,
        "/v1/admin/bootstrap",
        json!({ "reset": false }),
    )
    .await;

    let hydrated_change_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/admin/harness/evolution/changes/{change_id}"),
        Value::Null,
    )
    .await;

    let hydrated_report_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/eval/runs/{run_id}/report"),
        Value::Null,
    )
    .await;

    let hydrated_ingest_result = try_call(
        fresh,
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}/result"),
        Value::Null,
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let persisted_run_inventory_result = admin
        .search::<Value>(
            "rag_eval_runs",
            json!({ "q": "", "filter": tenant_filter, "limit": 100 }),
        )
        .await;
    let case_filter = equality_filter("case_id", &eval_case_id);
    let persisted_case_result_inventory_result = admin
        .search::<Value>(
            "rag_eval_case_results",
            json!({ "q": "", "filter": case_filter, "limit": 100 }),
        )
        .await;
    let mut cleanup_run_ids = persisted_run_inventory_result
        .as_ref()
        .map(|response| {
            response
                .hits
                .iter()
                .filter_map(|run| run["id"].as_str().map(str::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Ok(response) = &persisted_case_result_inventory_result {
        cleanup_run_ids.extend(
            response
                .hits
                .iter()
                .filter_map(|result| result["run_id"].as_str().map(str::to_string)),
        );
    }
    if !cleanup_run_ids.contains(&run_id) {
        cleanup_run_ids.push(run_id.clone());
    }
    cleanup_run_ids.sort();
    cleanup_run_ids.dedup();

    let mut cleanup_results = vec![
        (
            "eval run inventory",
            persisted_run_inventory_result
                .as_ref()
                .map(|_| ())
                .map_err(ToString::to_string),
        ),
        (
            "eval case-result inventory",
            persisted_case_result_inventory_result
                .as_ref()
                .map(|_| ())
                .map_err(ToString::to_string),
        ),
        (
            "harness changes",
            delete_by_filter_and_wait(&admin, "rag_harness_changes", &tenant_filter).await,
        ),
        (
            "harness verdicts",
            delete_by_filter_and_wait(&admin, "rag_harness_verdicts", &tenant_filter).await,
        ),
        (
            "company context",
            delete_by_filter_and_wait(&admin, "rag_company_context", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "ingest tasks",
            delete_by_filter_and_wait(&admin, "rag_ingest_tasks", &tenant_filter).await,
        ),
        (
            "ingest results",
            delete_by_filter_and_wait(&admin, "rag_ingest_results", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "company event index",
            delete_index_and_wait(&config, &admin, &company_routing.event_index_uid).await,
        ),
        (
            "company context index",
            delete_index_and_wait(&config, &admin, &company_routing.personal_context_index_uid)
                .await,
        ),
    ];
    let inventory_is_recoverable = persisted_run_inventory_result.is_ok()
        || persisted_case_result_inventory_result.is_ok()
        || !run_id.starts_with("missing-run-");
    let mut eval_children_clean = inventory_is_recoverable;
    for cleanup_run_id in &cleanup_run_ids {
        let run_filter = equality_filter("run_id", cleanup_run_id);
        let overview_cleanup =
            delete_by_filter_and_wait(&admin, "rag_eval_overviews", &run_filter).await;
        eval_children_clean &= overview_cleanup.is_ok();
        cleanup_results.push(("eval overviews", overview_cleanup));
    }
    let case_results_cleanup =
        delete_by_filter_and_wait(&admin, "rag_eval_case_results", &case_filter).await;
    eval_children_clean &= case_results_cleanup.is_ok();
    cleanup_results.push(("eval case results", case_results_cleanup));

    let eval_runs_cleanup = if eval_children_clean {
        delete_by_filter_and_wait(&admin, "rag_eval_runs", &tenant_filter).await
    } else {
        Err("preserved parent eval runs because child cleanup or inventory failed".to_string())
    };
    let eval_runs_clean = eval_runs_cleanup.is_ok();
    cleanup_results.push(("eval runs", eval_runs_cleanup));

    let eval_cases_cleanup = if eval_runs_clean {
        delete_by_ids_and_wait(
            &admin,
            "rag_eval_cases",
            std::slice::from_ref(&eval_case_id),
        )
        .await
    } else {
        Err("preserved eval case because its run cleanup was incomplete".to_string())
    };
    cleanup_results.push(("eval cases", eval_cases_cleanup));
    assert_cleanup_results(cleanup_results);

    let (bootstrap_status, bootstrap) = bootstrap_result.expect("bootstrap call should finish");
    let (change_status, change) = change_result.expect("harness change call should finish");
    let (eval_case_status, eval_case) = eval_case_result.expect("eval case call should finish");
    let (eval_run_status, eval_run) = eval_run_result.expect("eval run call should finish");
    let (ingest_status, ingest) = ingest_result.expect("ingest call should finish");
    let (hydrated_status, hydrated) = hydrated_result.expect("hydration call should finish");
    let (hydrated_change_status, hydrated_change) =
        hydrated_change_result.expect("hydrated change call should finish");
    let (hydrated_report_status, hydrated_report) =
        hydrated_report_result.expect("hydrated report call should finish");
    let (hydrated_ingest_status, hydrated_ingest) =
        hydrated_ingest_result.expect("hydrated ingest result call should finish");

    assert_eq!(bootstrap_status, StatusCode::OK, "{bootstrap}");
    assert_eq!(change_status, StatusCode::OK, "{change}");
    assert_ne!(change_id, "missing-change");
    assert_eq!(eval_case_status, StatusCode::OK, "{eval_case}");
    assert_eq!(eval_run_status, StatusCode::OK, "{eval_run}");
    assert_eq!(ingest_status, StatusCode::OK, "{ingest}");
    assert_eq!(hydrated_status, StatusCode::OK, "{hydrated}");
    assert!(
        hydrated["hydrated"]["harness_changes"]
            .as_u64()
            .unwrap_or_default()
            >= 1
    );
    assert_eq!(hydrated_change_status, StatusCode::OK, "{hydrated_change}");
    assert_eq!(hydrated_change["id"], change["id"]);
    assert_eq!(hydrated_report_status, StatusCode::OK, "{hydrated_report}");
    assert_eq!(hydrated_report["run"]["id"], eval_run["id"]);
    assert_eq!(hydrated_report["case_results"][0]["case_id"], eval_case_id);
    assert_eq!(hydrated_ingest_status, StatusCode::OK, "{hydrated_ingest}");
    assert_eq!(hydrated_ingest["task"]["task_id"], task_id);
    assert!(hydrated_ingest["parse_artifacts"]
        .as_array()
        .is_some_and(|artifacts| artifacts.iter().any(|artifact| {
            artifact["artifact_kind"] == "content_list_v2" && artifact["source_id"] == source_id
        })));
    assert!(hydrated_ingest["parsed_blocks"]
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(|block| {
            block["type"] == "paragraph" && block["text"] == parsed_block_marker
        })));
}

#[tokio::test]
async fn meili_restart_hydrates_company_context_sources_and_revisions() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let admin = bootstrapped_meili_admin(&config).await;
    let state = AppState::new(Arc::new(config.clone()));
    let app = build_router(state.clone());
    let run_id = uuid::Uuid::now_v7().to_string();
    let source_id = format!("pr1-company-restart-{run_id}");
    let marker = format!("pr1-company-restart-marker-{run_id}");
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, "company", false, true)
        .expect("company history routing should resolve");

    let revision_result = try_call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "PR1 company restart fixture",
            "source_uri": format!("https://example.test/pr1-company-restart/{run_id}"),
            "content": format!("# Restart fixture\n\n{marker}"),
            "checksum": format!("pr1-company-restart-checksum-{run_id}")
        }),
    )
    .await;
    let revision_id = revision_result
        .as_ref()
        .ok()
        .and_then(|(_, revision)| revision["revision_id"].as_str())
        .unwrap_or("missing-revision")
        .to_string();

    let activation_result = try_call(
        app,
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": "PR1 restart characterization" }),
    )
    .await;
    let source_document_uri = activation_result
        .as_ref()
        .ok()
        .and_then(|(_, activation)| activation["source_document_uri"].as_str())
        .unwrap_or("ctx://missing-source-document")
        .to_string();

    let fresh_state = AppState::new(Arc::new(config.clone()));
    let hydration_result = fresh_state.store.hydrate_from_repository(&tenant_id).await;
    let source_document_result = fresh_state
        .store
        .fs_read_async(&tenant_id, &source_document_uri, None, false)
        .await;
    let fresh = build_router(fresh_state);
    let document_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let revisions_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/history/company-docs/{source_id}/revisions"),
        Value::Null,
    )
    .await;
    let context_result = try_call(
        fresh.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker, "limit": 10 }),
    )
    .await;
    // Teardown is independent of the behavior under test. Every operation is
    // attempted before any assertion so a failed hydration/read contract does
    // not strand shared-backend fixtures.
    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let source_filter = equality_filter("source_id", &source_id);
    let cleanup_results = vec![
        (
            "company context",
            delete_by_filter_and_wait(&admin, "rag_company_context", &source_filter).await,
        ),
        (
            "source revisions",
            delete_by_filter_and_wait(&admin, "rag_source_revisions", &source_filter).await,
        ),
        (
            "source pointer",
            delete_by_ids_and_wait(&admin, "rag_sources", std::slice::from_ref(&source_id)).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &source_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &source_filter).await,
        ),
        (
            "ingest tasks",
            delete_by_filter_and_wait(&admin, "rag_ingest_tasks", &source_filter).await,
        ),
        (
            "ingest results",
            delete_by_filter_and_wait(&admin, "rag_ingest_results", &source_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "company event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "company context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (revision_status, revision) = revision_result.expect("company revision call should finish");
    assert_eq!(revision_status, StatusCode::OK, "{revision}");
    assert_ne!(revision_id, "missing-revision");
    let (activation_status, activation) =
        activation_result.expect("company activation call should finish");
    assert_eq!(activation_status, StatusCode::OK, "{activation}");
    assert_ne!(source_document_uri, "ctx://missing-source-document");
    let hydration = hydration_result.expect("fresh AppState hydration should succeed");
    let (document_status, document) = document_result.expect("company document call should finish");
    let (revisions_status, revisions) =
        revisions_result.expect("company revisions call should finish");
    let (context_status, context) = context_result.expect("company context search should finish");
    for key in [
        "company_context_nodes",
        "company_sources",
        "source_revisions",
    ] {
        assert!(
            hydration[key].as_u64().is_some_and(|count| count >= 1),
            "current company hydration should report {key}: {hydration}"
        );
    }
    assert_eq!(document_status, StatusCode::OK, "{document}");
    assert_eq!(document["source_id"], source_id);
    assert_eq!(document["revision_id"], revision_id);
    assert_eq!(revisions_status, StatusCode::OK, "{revisions}");
    assert!(revisions["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|candidate| candidate["id"] == revision_id));
    assert_eq!(context_status, StatusCode::OK, "{context}");
    assert!(context["hits"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| hit["source_id"] == source_id));
    match source_document_result {
        Err(ApiError::NotFound(message)) => assert_eq!(message, "context uri not found"),
        other => panic!(
            "company source documents currently fail ownerless read-through with not_found: {other:?}"
        ),
    }
}

#[tokio::test]
async fn meili_restart_characterizes_state_link_and_session_durability_gaps() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let admin = bootstrapped_meili_admin(&config).await;
    let state = AppState::new(Arc::new(config.clone()));
    let app = build_router(state.clone());
    let run_id = uuid::Uuid::now_v7().to_string();
    let owner = format!("pr1-restart-owner-{run_id}");
    let fact_key = format!("pr1-restart-fact-{run_id}");
    let marker = format!("pr1-restart-marker-{run_id}");
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");

    let state_created_result = try_call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "status",
            "title": "PR1 restart state fact",
            "statement": marker,
            "document": {
                "content": format!("# PR1 restart source\n\n{marker}"),
                "content_type": "text/markdown",
                "source_uri": format!("https://example.test/pr1-state-restart/{run_id}")
            }
        }),
    )
    .await;
    let state_id = state_created_result
        .as_ref()
        .ok()
        .and_then(|(_, state)| state["item"]["id"].as_str())
        .unwrap_or("missing-state")
        .to_string();
    let personal_source_document_uri = state_created_result
        .as_ref()
        .ok()
        .and_then(|(_, state)| {
            state["item"]["source_refs"][0]["meta"]["source_document_uri"].as_str()
        })
        .unwrap_or("ctx://missing-personal-source-document")
        .to_string();

    let source_uri = format!("ctx://company/pr1-restart/{run_id}/source");
    let target_uri = format!("ctx://company/pr1-restart/{run_id}/target");
    let link_created_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "owner_user_id": owner,
            "source_uri": source_uri,
            "target_uri": target_uri,
            "relation": "supports",
            "rationale": marker
        }),
    )
    .await;
    let link_id = link_created_result
        .as_ref()
        .ok()
        .and_then(|(_, link)| link["link"]["id"].as_str())
        .unwrap_or("missing-link")
        .to_string();

    let session_created_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/sessions",
        json!({ "owner_user_id": owner, "title": "PR1 restart session" }),
    )
    .await;
    let session_id = session_created_result
        .as_ref()
        .ok()
        .and_then(|(_, session)| session["session_id"].as_str())
        .unwrap_or("missing-session")
        .to_string();

    let live_state_result = try_call(
        app.clone(),
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}?owner_user_id={owner}"),
        Value::Null,
    )
    .await;
    let live_links_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "owner_user_id": owner, "query": marker }),
    )
    .await;
    let live_session_result = try_call(
        app,
        Method::POST,
        &format!("/v1/sessions/{session_id}/messages"),
        json!({
            "role": "user",
            "content": marker,
            "write_history_event": false
        }),
    )
    .await;

    let state_filter = format!(
        "{} AND {}",
        equality_filter("id", &state_id),
        equality_filter("tenant_id", &tenant_id)
    );
    let persisted_state_result = admin
        .search::<Value>(
            "rag_state_items",
            json!({ "q": "", "filter": state_filter, "limit": 10 }),
        )
        .await;
    let link_filter = format!(
        "{} AND {}",
        equality_filter("id", &link_id),
        equality_filter("tenant_id", &tenant_id)
    );
    let persisted_link_result = admin
        .search::<Value>(
            "rag_links",
            json!({ "q": "", "filter": link_filter, "limit": 10 }),
        )
        .await;
    let persisted_session_result = admin
        .search::<Value>("rag_sessions", json!({ "q": session_id, "limit": 10 }))
        .await;

    let fresh_state = AppState::new(Arc::new(config.clone()));
    let hydration_result = fresh_state.store.hydrate_from_repository(&tenant_id).await;
    let personal_source_document_result = fresh_state
        .store
        .fs_read_async(
            &tenant_id,
            &personal_source_document_uri,
            Some(&owner),
            false,
        )
        .await;
    let fresh = build_router(fresh_state);
    let fresh_registry_result = try_call(
        fresh.clone(),
        Method::GET,
        "/v1/admin/history/user-event-indexes",
        Value::Null,
    )
    .await;
    let fresh_events_result = try_call(
        fresh.clone(),
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "event_types": ["state.changed"], "limit": 10 }),
    )
    .await;
    let fresh_personal_context_result = try_call(
        fresh.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "owner_user_id": owner, "query": marker, "limit": 10 }),
    )
    .await;
    let fresh_state_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}?owner_user_id={owner}"),
        Value::Null,
    )
    .await;
    let fresh_links_result = try_call(
        fresh.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "owner_user_id": owner, "query": marker }),
    )
    .await;
    let fresh_session_result = try_call(
        fresh,
        Method::POST,
        &format!("/v1/sessions/{session_id}/messages"),
        json!({
            "role": "user",
            "content": "message after restart",
            "write_history_event": false
        }),
    )
    .await;

    // Attempt every scoped cleanup before asserting any observed behavior.
    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "state items",
            delete_by_filter_and_wait(&admin, "rag_state_items", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "sessions",
            delete_by_filter_and_wait(&admin, "rag_sessions", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (state_created_status, state_created) =
        state_created_result.expect("state creation call should finish");
    assert_eq!(state_created_status, StatusCode::OK, "{state_created}");
    assert_ne!(state_id, "missing-state");
    let (link_created_status, link_created) =
        link_created_result.expect("link creation call should finish");
    assert_eq!(link_created_status, StatusCode::OK, "{link_created}");
    assert_ne!(link_id, "missing-link");
    let (session_created_status, session_created) =
        session_created_result.expect("session creation call should finish");
    assert_eq!(session_created_status, StatusCode::OK, "{session_created}");
    assert_ne!(session_id, "missing-session");
    let (live_state_status, live_state) = live_state_result.expect("live state read should finish");
    let (live_link_status, live_links) = live_links_result.expect("live link search should finish");
    let (live_session_status, live_session) =
        live_session_result.expect("live session write should finish");
    let persisted_state_count = persisted_state_result
        .expect("persisted state item lookup should succeed")
        .hits
        .len();
    let persisted_link_count = persisted_link_result
        .expect("persisted link lookup should succeed")
        .hits
        .len();
    let persisted_session_count = persisted_session_result
        .expect("persisted session lookup should succeed")
        .hits
        .len();
    let hydration = hydration_result.expect("fresh AppState hydration should succeed");
    let personal_source_document = personal_source_document_result
        .expect("personal source document should read through after restart");
    let (fresh_registry_status, fresh_registry) =
        fresh_registry_result.expect("fresh registry call should finish");
    let (fresh_event_status, fresh_events) =
        fresh_events_result.expect("fresh event search should finish");
    let (fresh_personal_context_status, fresh_personal_context) =
        fresh_personal_context_result.expect("fresh context search should finish");
    let (fresh_state_status, fresh_state_body) =
        fresh_state_result.expect("fresh state read should finish");
    let (fresh_link_status, fresh_links) =
        fresh_links_result.expect("fresh link search should finish");
    let (fresh_session_status, fresh_session_body) =
        fresh_session_result.expect("fresh session write should finish");

    assert_eq!(live_state_status, StatusCode::OK, "{live_state}");
    assert_eq!(live_link_status, StatusCode::OK, "{live_links}");
    assert_eq!(live_links["links"].as_array().unwrap().len(), 1);
    assert_eq!(live_session_status, StatusCode::OK, "{live_session}");

    // Current per-user restart behavior is mixed: the registry and personal
    // context are not startup-hydrated, while deterministic routing still lets
    // event and context searches read through to their physical indexes.
    assert_eq!(fresh_registry_status, StatusCode::OK, "{fresh_registry}");
    assert!(fresh_registry["indexes"].as_array().unwrap().is_empty());
    assert_eq!(fresh_event_status, StatusCode::OK, "{fresh_events}");
    assert_eq!(fresh_events["hits"].as_array().unwrap().len(), 1);
    assert_eq!(
        fresh_personal_context_status,
        StatusCode::OK,
        "{fresh_personal_context}"
    );
    assert!(
        !fresh_personal_context["hits"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{fresh_personal_context}"
    );
    assert_eq!(personal_source_document.uri, personal_source_document_uri);
    assert_eq!(personal_source_document.node_kind, "source_doc");

    // State and links reach Meili but are not hydrated; sessions never reach
    // Meili and are likewise absent after a fresh AppState. PR5 is expected to
    // invert these characterizations.
    assert_eq!(persisted_state_count, 1);
    assert_eq!(persisted_link_count, 1);
    assert_eq!(persisted_session_count, 0);
    for missing_domain in [
        "user_event_indexes",
        "personal_context_nodes",
        "state_items",
        "links",
        "sessions",
    ] {
        assert!(
            hydration.get(missing_domain).is_none(),
            "current hydration unexpectedly reported {missing_domain}: {hydration}"
        );
    }
    assert_eq!(
        fresh_state_status,
        StatusCode::NOT_FOUND,
        "{fresh_state_body}"
    );
    assert_eq!(fresh_link_status, StatusCode::OK, "{fresh_links}");
    assert!(fresh_links["links"].as_array().unwrap().is_empty());
    assert_eq!(
        fresh_session_status,
        StatusCode::NOT_FOUND,
        "{fresh_session_body}"
    );
}
