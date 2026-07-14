use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{build_router, config::AuthUserConfig, AppState, Config};
use serde::Deserialize;
use serde_json::{json, Value};
use tower::ServiceExt;

const OWNER_U1_TOKEN: &str = "owner-u1-token";
const OWNER_U2_TOKEN: &str = "owner-u2-token";
const TENANT_SERVICE_LEGACY_TOKEN: &str = "tenant-service-legacy-token";
const COMPANY_WRITER_TOKEN: &str = "company-writer-token";
const ADMIN_TOKEN: &str = "admin-token";

fn characterized_config() -> Config {
    let mut config = Config::test();
    config.bearer_token = Some(TENANT_SERVICE_LEGACY_TOKEN.to_string());
    config.auth_users = vec![
        AuthUserConfig {
            token: OWNER_U1_TOKEN.to_string(),
            owner_user_id: Some("u1".to_string()),
            roles: vec!["user".to_string()],
        },
        AuthUserConfig {
            token: OWNER_U2_TOKEN.to_string(),
            owner_user_id: Some("u2".to_string()),
            roles: vec!["user".to_string()],
        },
        // No route currently interprets this role. Merely having a configured
        // principal is enough to pass UserGuard, including for shared writes.
        AuthUserConfig {
            token: COMPANY_WRITER_TOKEN.to_string(),
            owner_user_id: None,
            roles: vec!["company_writer".to_string()],
        },
        AuthUserConfig {
            token: ADMIN_TOKEN.to_string(),
            owner_user_id: None,
            roles: vec!["admin".to_string()],
        },
    ];
    config
}

fn characterized_app() -> Router {
    build_router(AppState::new(Arc::new(characterized_config())))
}

async fn call(
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
    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }))
    };
    (status, body)
}

#[tokio::test]
async fn legacy_tenant_service_bearer_can_access_multiple_explicit_owners() {
    let app = characterized_app();

    for owner_user_id in ["u1", "u2"] {
        let (status, body) = call(
            app.clone(),
            Method::PUT,
            &format!("/v1/history/users/{owner_user_id}/event-index"),
            json!({}),
            Some(TENANT_SERVICE_LEGACY_TOKEN),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "owner={owner_user_id}: {body}");
        assert_eq!(body["index"]["status"], "active");
        assert!(body["routing"]["event_index_uid"].is_string());
    }
}

async fn exercise_shared_mutations(app: &Router, token: &str, fixture: &str) {
    let source_id = format!("characterized-company-source-{fixture}");
    let dataset_key = format!("characterized_dataset_{fixture}");

    let (status, preflight) = call(
        app.clone(),
        Method::POST,
        "/v1/state/company-docs/preflight",
        json!({
            "title": format!("Characterization {fixture}"),
            "text_preview": format!("shared mutation fixture {fixture}"),
            "checksum": format!("checksum-{fixture}")
        }),
        Some(token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "preflight ({fixture}): {preflight}");

    let (status, revision) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "preflight_decision_id": preflight["decision_id"],
            "title": format!("Characterization {fixture}"),
            "content": format!("shared company content written by {fixture}"),
            "ingest": false,
            "force_create": true
        }),
        Some(token),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "create revision ({fixture}): {revision}"
    );
    let revision_id = revision["revision_id"]
        .as_str()
        .expect("revision id")
        .to_string();

    let (status, activation) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": format!("characterize {fixture}") }),
        Some(token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "activate ({fixture}): {activation}");
    assert_eq!(activation["active_revision_id"], revision_id);

    let (status, dataset) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/structured/datasets/{dataset_key}"),
        json!({
            "title": format!("Dataset {fixture}"),
            "columns": [{ "name": "value", "kind": "number" }]
        }),
        Some(token),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "dataset upsert ({fixture}): {dataset}"
    );
    assert_eq!(dataset["dataset"]["dataset_key"], dataset_key);

    let (status, deleted) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
        Some(token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "delete ({fixture}): {deleted}");
    assert_eq!(deleted["deleted"], true);
}

#[tokio::test]
// Expected to change: auth hardening will stop an ordinary owner from writing shared state.
async fn ordinary_owner_can_currently_mutate_shared_company_state_expected_to_change() {
    let app = characterized_app();

    exercise_shared_mutations(&app, OWNER_U1_TOKEN, "owner_u1").await;
}

#[tokio::test]
// Expected to remain allowed: the canonical company_writer role owns shared-state writes.
async fn company_writer_can_mutate_shared_company_state_expected_to_remain_allowed() {
    let app = characterized_app();

    exercise_shared_mutations(&app, COMPANY_WRITER_TOKEN, "company_writer").await;
}

#[tokio::test]
async fn unauthenticated_llm_status_exposes_the_configured_codex_auth_path() {
    let auth_path = format!(
        "/tmp/nowledge-characterization-{}/codex-auth.json",
        uuid::Uuid::now_v7()
    );
    let mut config = characterized_config();
    config.llm_provider = "codex_auth".to_string();
    config.llm_model = Some("characterization-model".to_string());
    config.codex_auth_path = Some(auth_path.clone());
    let app = build_router(AppState::new(Arc::new(config)));

    let (status, body) = call(app, Method::GET, "/v1/llm/status", Value::Null, None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["provider"], "codex_auth");
    assert_eq!(body["auth_source"], auth_path);
}

#[tokio::test]
async fn representative_runtime_routes_enforce_their_current_guard_classes() {
    let app = characterized_app();

    let (status, body) = call(app.clone(), Method::GET, "/livez", Value::Null, None).await;
    assert_eq!(status, StatusCode::OK, "public route: {body}");

    let (status, body) = call(
        app.clone(),
        Method::GET,
        "/v1/state/company-docs",
        Value::Null,
        None,
    )
    .await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "unguarded user route: {body}"
    );
    let (status, body) = call(
        app.clone(),
        Method::GET,
        "/v1/state/company-docs",
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "authenticated user route: {body}");

    let admin_uri = "/v1/admin/history/user-event-indexes";
    let (status, body) = call(app.clone(), Method::GET, admin_uri, Value::Null, None).await;
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "unguarded admin route: {body}"
    );
    let (status, body) = call(
        app.clone(),
        Method::GET,
        admin_uri,
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "owner on admin route: {body}"
    );
    let (status, body) = call(app, Method::GET, admin_uri, Value::Null, Some(ADMIN_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "admin route: {body}");
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardPolicy {
    Public,
    User,
    Admin,
}

#[derive(Debug, Deserialize)]
struct ManifestEntry {
    method: String,
    path: String,
    handler: String,
    group: String,
    file: String,
}

fn expected_policy(entry: &ManifestEntry) -> GuardPolicy {
    match entry.group.as_str() {
        "Health" => GuardPolicy::Public,
        "Admin" | "Harness" | "Debug" | "Eval" => GuardPolicy::Admin,
        "LLM" => match entry.handler.as_str() {
            "llm_status" => GuardPolicy::Public,
            "llm_test" => GuardPolicy::Admin,
            "llm_title" => GuardPolicy::User,
            other => panic!("unclassified LLM handler: {other}"),
        },
        "Analysis"
        | "Context"
        | "Context FS"
        | "Company Docs"
        | "History Alias"
        | "Insights"
        | "Structured History"
        | "History User Indexes"
        | "History Events"
        | "Links"
        | "Ingest"
        | "RAG"
        | "Sessions"
        | "State"
        | "Structured State"
        | "Usage" => GuardPolicy::User,
        other => panic!("unclassified route group: {other}"),
    }
}

fn handler_signature<'a>(source: &'a str, handler: &str) -> &'a str {
    let marker = format!("async fn {handler}");
    let start = source
        .find(&marker)
        .unwrap_or_else(|| panic!("handler {handler} is missing from routes.rs"));
    let open = source[start + marker.len()..]
        .find('(')
        .map(|offset| start + marker.len() + offset)
        .unwrap_or_else(|| panic!("handler {handler} has no parameter list"));
    let bytes = source.as_bytes();
    let mut depth = 0usize;
    for index in open..bytes.len() {
        match bytes[index] {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return &source[open + 1..index];
                }
            }
            _ => {}
        }
    }
    panic!("handler {handler} has an unterminated parameter list");
}

fn actual_policy(source: &str, handler: &str) -> GuardPolicy {
    let signature = handler_signature(source, handler);
    let has_user = signature.contains("UserGuard");
    let has_admin = signature.contains("AdminGuard");
    assert!(
        !(has_user && has_admin),
        "handler {handler} unexpectedly declares both guard types"
    );
    if has_admin {
        GuardPolicy::Admin
    } else if has_user {
        GuardPolicy::User
    } else {
        GuardPolicy::Public
    }
}

#[test]
fn route_policy_matrix_covers_every_manifest_handler_and_group() {
    let manifest: Vec<ManifestEntry> =
        serde_json::from_str(include_str!("../doc/api_manifest.json")).unwrap();
    let routes = include_str!("../src/routes.rs");
    assert_eq!(
        manifest.len(),
        87,
        "the characterized matrix must cover all routes"
    );

    for entry in &manifest {
        let expected = expected_policy(entry);
        let actual = actual_policy(routes, &entry.handler);
        assert_eq!(
            actual, expected,
            "{} {} ({}, {}, {})",
            entry.method, entry.path, entry.handler, entry.group, entry.file
        );
    }

    let policy_for = |handler: &str| {
        let entry = manifest
            .iter()
            .find(|entry| entry.handler == handler)
            .unwrap_or_else(|| panic!("manifest is missing {handler}"));
        expected_policy(entry)
    };
    assert_eq!(policy_for("llm_status"), GuardPolicy::Public);
    assert_eq!(policy_for("llm_test"), GuardPolicy::Admin);
    assert_eq!(policy_for("llm_title"), GuardPolicy::User);
    assert_eq!(policy_for("rag_debug"), GuardPolicy::User);
    assert!(manifest
        .iter()
        .filter(|entry| entry.group == "Debug")
        .all(|entry| expected_policy(entry) == GuardPolicy::Admin));
}
