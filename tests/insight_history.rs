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
    config::{AuthUserConfig, AuthUserScope},
    meili::{task_uid, MeiliAdmin},
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

const OWNER_U1_TOKEN: &str = "owner-u1-token";
const OWNER_U2_TOKEN: &str = "owner-u2-token";
const TENANT_SERVICE_TOKEN: &str = "tenant-service-token";

fn authenticated_config() -> Config {
    let mut config = Config::test();
    config.max_search_limit = 10;
    config.auth_users = vec![
        AuthUserConfig {
            token: OWNER_U1_TOKEN.to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            roles: vec!["user".to_string()],
        },
        AuthUserConfig {
            token: OWNER_U2_TOKEN.to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u2".to_string(),
            },
            roles: vec!["user".to_string()],
        },
        AuthUserConfig {
            token: TENANT_SERVICE_TOKEN.to_string(),
            scope: AuthUserScope::TenantService,
            roles: vec!["user".to_string()],
        },
    ];
    config
}

fn app() -> Router {
    build_router(AppState::new(Arc::new(authenticated_config())))
}

async fn call(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
    token: &str,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, "application/json")
                .header("Authorization", format!("Bearer {token}"))
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

async fn create_and_patch_insight(app: &Router) -> (String, String, String) {
    let (status, created) = call(
        app.clone(),
        Method::POST,
        "/v1/state/insights",
        json!({
            "insight_type": "delivery",
            "title": "History endpoint coverage",
            "statement": "The endpoint starts as a placeholder",
            "evidence_text": "The insight was created"
        }),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");

    let insight_id = created["insight"]["id"].as_str().unwrap().to_string();
    let upsert_event_id = created["history_event_id"].as_str().unwrap().to_string();
    let (status, patched) = call(
        app.clone(),
        Method::PATCH,
        &format!("/v1/state/insights/{insight_id}"),
        json!({
            "statement": "The endpoint returns real audit history",
            "patch_reason": "History endpoint implemented"
        }),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{patched}");
    let patch_event_id = patched["history_event_id"].as_str().unwrap().to_string();

    (insight_id, upsert_event_id, patch_event_id)
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

async fn delete_meili_index(config: &Config, admin: &MeiliAdmin, index_uid: &str) {
    let endpoint = format!(
        "{}/indexes/{index_uid}",
        config.meili_url.as_deref().unwrap().trim_end_matches('/')
    );
    let client = reqwest::Client::new();
    let mut request = client.delete(endpoint);
    if let Some(api_key) = config.meili_api_key.as_deref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.unwrap();
    if response.status().as_u16() == 404 {
        return;
    }
    assert!(response.status().is_success(), "{}", response.status());
    let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
    if let Some(task_uid) = task_uid(&body) {
        admin.wait_for_task(&task_uid).await.unwrap();
    }
}

#[tokio::test]
async fn insight_history_returns_exact_newest_first_events_with_bounded_limit() {
    let app = app();
    let (insight_id, upsert_event_id, patch_event_id) = create_and_patch_insight(&app).await;
    let (status, unrelated) = call(
        app.clone(),
        Method::POST,
        "/v1/history/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": insight_id,
            "occurred_at": "2026-07-15T12:00:00Z",
            "observed_at": "2026-07-15T12:00:01Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "same-entity-id" },
            "text": "A non-insight event sharing the entity ID must not leak into the response"
        }),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{unrelated}");

    let (status, history) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/history/insights/{insight_id}/events"),
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history}");
    assert_eq!(history["insight_id"], insight_id);
    let events = history["events"].as_array().unwrap();
    assert_eq!(events.len(), 2, "{history}");
    assert_eq!(events[0]["id"], patch_event_id);
    assert_eq!(events[0]["event_type"], "insight.patched");
    assert_eq!(events[1]["id"], upsert_event_id);
    assert_eq!(events[1]["event_type"], "insight.upserted");
    assert!(events
        .iter()
        .all(|event| { event["entity_type"] == "insight" && event["entity_id"] == insight_id }));

    let (status, limited) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/history/insights/{insight_id}/events?limit=1"),
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{limited}");
    assert_eq!(limited["events"].as_array().unwrap().len(), 1);
    assert_eq!(limited["events"][0]["id"], patch_event_id);

    let (status, invalid) = call(
        app,
        Method::GET,
        &format!("/v1/history/insights/{insight_id}/events?limit=11"),
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{invalid}");
    assert_eq!(invalid["error"]["code"], "validation_error");
    assert_eq!(invalid["error"]["details"]["field"], "limit");
}

#[tokio::test]
async fn insight_history_enforces_owner_scope_and_tenant_service_path_scope() {
    let app = app();
    let (insight_id, _, _) = create_and_patch_insight(&app).await;

    let (status, denied) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/history/insights/{insight_id}/events"),
        Value::Null,
        OWNER_U2_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    assert_eq!(denied["error"]["code"], "forbidden");

    let (status, service_history) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/history/insights/{insight_id}/events"),
        Value::Null,
        TENANT_SERVICE_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{service_history}");
    assert_eq!(service_history["insight_id"], insight_id);
    assert_eq!(service_history["events"].as_array().unwrap().len(), 2);
    assert!(service_history["events"]
        .as_array()
        .unwrap()
        .iter()
        .all(|event| event["owner_user_id"] == "u1"));

    let unknown_id = "insight_00000000-0000-7000-8000-000000000000";
    let (status, missing) = call(
        app,
        Method::GET,
        &format!("/v1/history/insights/{unknown_id}/events"),
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND, "{missing}");
    assert_eq!(missing["error"]["code"], "not_found");
}

#[tokio::test]
async fn insight_history_survives_a_meili_backed_restart() {
    let Ok(meili_url) = std::env::var("RAG_TEST_MEILI_URL") else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    if !meili_available(&meili_url) {
        eprintln!("skipping Meilisearch integration test; {meili_url} is not reachable");
        return;
    }
    let meili_api_key = std::env::var("RAG_TEST_MEILI_API_KEY").ok();
    if meili_api_key.is_none() {
        let indexes = reqwest::Client::new()
            .get(format!("{}/indexes", meili_url.trim_end_matches('/')))
            .send()
            .await;
        if indexes
            .as_ref()
            .is_ok_and(|response| response.status() == StatusCode::UNAUTHORIZED)
        {
            eprintln!(
                "skipping Meilisearch integration test; server requires RAG_TEST_MEILI_API_KEY"
            );
            return;
        }
    }

    let mut config = authenticated_config();
    config.tenant_id = format!("insight-history-test-{}", uuid::Uuid::now_v7());
    config.store_backend = "meili".to_string();
    config.meili_url = Some(meili_url);
    config.meili_api_key = meili_api_key;
    config.meili_wait_for_tasks = true;
    config.request_timeout_ms = 180_000;

    let initial_state = AppState::new(Arc::new(config.clone()));
    initial_state.meili.bootstrap(false).await.unwrap();
    initial_state
        .store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .unwrap();
    let admin = initial_state.meili.clone();
    let routing = initial_state
        .store
        .resolver()
        .resolve(&config.tenant_id, "u1", false, true)
        .unwrap();
    let initial_app = build_router(initial_state.clone());
    let (insight_id, upsert_event_id, patch_event_id) =
        create_and_patch_insight(&initial_app).await;
    drop(initial_app);
    initial_state.shutdown().await;

    let restarted_state = AppState::new(Arc::new(config.clone()));
    restarted_state.meili.bootstrap(false).await.unwrap();
    let hydration = restarted_state
        .store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .unwrap();
    let restarted_app = build_router(restarted_state.clone());
    let history_result = call(
        restarted_app,
        Method::GET,
        &format!("/v1/history/insights/{insight_id}/events"),
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    restarted_state.shutdown().await;

    let tenant_filter = format!(
        "tenant_id = {}",
        serde_json::to_string(&config.tenant_id).unwrap()
    );
    for index_uid in ["rag_insights", "rag_user_event_indexes"] {
        if let Some(task_uid) = admin
            .delete_documents_by_filter(index_uid, &tenant_filter)
            .await
            .unwrap()
        {
            admin.wait_for_task(&task_uid).await.unwrap();
        }
    }
    delete_meili_index(&config, &admin, &routing.event_index_uid).await;
    delete_meili_index(&config, &admin, &routing.personal_context_index_uid).await;

    assert!(hydration["insights"].as_u64().unwrap_or_default() >= 1);
    let (status, history) = history_result;
    assert_eq!(status, StatusCode::OK, "{history}");
    let events = history["events"].as_array().unwrap();
    assert_eq!(events.len(), 2, "{history}");
    assert_eq!(events[0]["id"], patch_event_id);
    assert_eq!(events[1]["id"], upsert_event_id);
}
