use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{
    build_router,
    config::{AuthUserConfig, AuthUserScope, BearerTokenScope},
    AppState, Config,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tower::ServiceExt;

const OWNER_U1_TOKEN: &str = "owner-u1-token";
const OWNER_U2_TOKEN: &str = "owner-u2-token";
const TENANT_SERVICE_TOKEN: &str = "tenant-service-token";
const COMPANY_WRITER_TOKEN: &str = "company-writer-token";
const ADMIN_TOKEN: &str = "admin-token";
const LEGACY_BEARER_TOKEN: &str = "legacy-bearer-token";
const STRONG_INDEX_HASH_SECRET: &[u8] = b"7Qv!n2$La9@Xm4#Rp8%Wd3&Ks6*Hy1+Tz5";
const DIAGNOSTIC_INDEX_HASH_SECRET: &str = "zxqv-boundary-index-hash-secret-private-value";

fn auth_user(token: &str, scope: AuthUserScope, roles: &[&str]) -> AuthUserConfig {
    AuthUserConfig {
        token: token.to_string(),
        scope,
        roles: roles.iter().map(|role| (*role).to_string()).collect(),
    }
}

fn characterized_config() -> Config {
    let mut config = Config::test();
    config.auth_users = vec![
        auth_user(
            OWNER_U1_TOKEN,
            AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            &["user"],
        ),
        auth_user(
            OWNER_U2_TOKEN,
            AuthUserScope::Owner {
                owner_user_id: "u2".to_string(),
            },
            &["user"],
        ),
        auth_user(
            TENANT_SERVICE_TOKEN,
            AuthUserScope::TenantService,
            &["user"],
        ),
        auth_user(
            COMPANY_WRITER_TOKEN,
            AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            &["user", "company_writer"],
        ),
        auth_user(ADMIN_TOKEN, AuthUserScope::Admin, &["admin"]),
    ];
    config
}

fn characterized_app() -> Router {
    build_router(AppState::new(Arc::new(characterized_config())))
}

fn diagnostic_secret_app() -> Router {
    let mut config = characterized_config();
    config.index_hash_secret = DIAGNOSTIC_INDEX_HASH_SECRET.as_bytes().to_vec();
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("diagnostic-mock".to_string());
    config.analysis_llm_provider = "mock".to_string();
    config.analysis_llm_model = Some("diagnostic-analysis-mock".to_string());
    build_router(AppState::new(Arc::new(config)))
}

fn legacy_bearer_config(
    scope: Option<BearerTokenScope>,
    owner_user_id: Option<&str>,
    allow_legacy: bool,
) -> Config {
    let mut config = Config::test();
    config.bearer_token = Some(LEGACY_BEARER_TOKEN.to_string());
    config.bearer_token_scope = scope;
    config.bearer_token_owner_user_id = owner_user_id.map(ToString::to_string);
    config.allow_legacy_tenant_service_bearer = allow_legacy;
    config
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

fn assert_error(body: &Value, status: StatusCode, code: &str) {
    assert_eq!(body["error"]["code"], code, "{body}");
    assert_eq!(body["error"]["details"]["status"], status.as_u16());
    assert!(body["error"]["message"].is_string(), "{body}");
}

fn query_encode(value: &str) -> String {
    value
        .replace('%', "%25")
        .replace(':', "%3A")
        .replace('/', "%2F")
        .replace(' ', "%20")
}

async fn assert_owner_route(app: &Router, token: &str, owner_user_id: &str, expected: StatusCode) {
    let (status, body) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/history/users/{owner_user_id}/event-index"),
        json!({}),
        Some(token),
    )
    .await;
    assert_eq!(status, expected, "owner={owner_user_id}: {body}");
    if expected == StatusCode::OK {
        assert_eq!(body["index"]["status"], "active");
        assert!(body["routing"]["event_index_uid"].is_string());
    } else {
        assert_error(&body, expected, "forbidden");
    }
}

#[tokio::test]
async fn owner_scope_allows_own_owner_and_forbids_other_owner() {
    let app = characterized_app();
    assert_owner_route(&app, OWNER_U1_TOKEN, "u1", StatusCode::OK).await;
    assert_owner_route(&app, OWNER_U1_TOKEN, "u2", StatusCode::FORBIDDEN).await;
}

#[tokio::test]
async fn owner_scope_forbids_cross_owner_event_index_reads() {
    let app = characterized_app();

    let (status, body) = call(
        app.clone(),
        Method::GET,
        "/v1/history/users/u1/event-index",
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = call(
        app,
        Method::GET,
        "/v1/history/users/u2/event-index",
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_error(&body, StatusCode::FORBIDDEN, "forbidden");
}

#[tokio::test]
async fn owner_mismatch_precedes_history_boundary_validation() {
    let app = characterized_app();
    let oversized_tag = "x".repeat(129);
    let cases = [
        (
            "/v1/history/users/u2/events",
            json!({ "tags": [oversized_tag.clone()] }),
        ),
        (
            "/v1/history/users/u2/events:bulk",
            json!({ "events": [{ "tags": [oversized_tag.clone()] }] }),
        ),
        ("/v1/history/users/u2/search", json!({ "limit": 101 })),
        ("/v1/history/users/u2/timeline", json!({ "limit": 101 })),
        (
            "/v1/history/events",
            json!({ "owner_user_id": "u2", "tags": [oversized_tag.clone()] }),
        ),
        (
            "/v1/history/events:bulk",
            json!({
                "events": [{
                    "owner_user_id": "u2",
                    "tags": [oversized_tag]
                }]
            }),
        ),
        (
            "/v1/history/search",
            json!({ "owner_user_id": "u2", "limit": 101 }),
        ),
        (
            "/v1/history/timeline",
            json!({ "owner_user_id": "u2", "limit": 101 }),
        ),
    ];

    for (uri, body) in cases {
        let (status, response) =
            call(app.clone(), Method::POST, uri, body, Some(OWNER_U1_TOKEN)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {response}");
        assert_error(&response, StatusCode::FORBIDDEN, "forbidden");
    }
}

#[tokio::test]
async fn tenant_service_scope_can_access_multiple_explicit_owners() {
    let app = characterized_app();
    for owner_user_id in ["u1", "u2"] {
        assert_owner_route(&app, TENANT_SERVICE_TOKEN, owner_user_id, StatusCode::OK).await;
    }
}

#[tokio::test]
async fn admin_scope_can_access_multiple_explicit_owners() {
    let app = characterized_app();
    for owner_user_id in ["u1", "u2"] {
        assert_owner_route(&app, ADMIN_TOKEN, owner_user_id, StatusCode::OK).await;
    }
}

#[tokio::test]
async fn company_writer_role_does_not_expand_owner_scope() {
    let app = characterized_app();
    assert_owner_route(&app, COMPANY_WRITER_TOKEN, "u1", StatusCode::OK).await;
    assert_owner_route(&app, COMPANY_WRITER_TOKEN, "u2", StatusCode::FORBIDDEN).await;
}

#[tokio::test]
async fn tenant_service_usage_requires_an_explicit_owner_and_never_becomes_global() {
    let app = characterized_app();
    let (status, body) = call(
        app.clone(),
        Method::GET,
        "/v1/usage",
        Value::Null,
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = call(
        app,
        Method::GET,
        "/v1/usage?owner_user_id=u1",
        Value::Null,
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["scope"]["owner_user_id"], "u1");
    assert_eq!(body["scope"]["global"], false);
}

#[tokio::test]
async fn tenant_service_alias_write_requires_an_explicit_owner() {
    let (status, body) = call(
        characterized_app(),
        Method::POST,
        "/v1/history/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "tenant-service-missing-owner",
            "occurred_at": "2026-07-13T00:00:00Z",
            "observed_at": "2026-07-13T00:00:01Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "tenant-service-missing-owner" },
            "text": "tenant services must select an owner before writing"
        }),
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_error(&body, StatusCode::FORBIDDEN, "forbidden");
}

#[tokio::test]
async fn tenant_service_state_patch_requires_an_explicit_owner_selection() {
    let app = characterized_app();
    let fact_key = "tenant-service-owner-selection";
    let (status, created) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "state_type": "profile",
            "statement": "u2 original statement"
        }),
        Some(OWNER_U2_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{created}");
    assert_eq!(created["item"]["owner_user_id"], "u2");

    let (status, denied) = call(
        app.clone(),
        Method::PATCH,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({ "statement": "ambient cross-owner mutation" }),
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
    assert_error(&denied, StatusCode::FORBIDDEN, "forbidden");

    let (status, unchanged) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}"),
        Value::Null,
        Some(OWNER_U2_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{unchanged}");
    assert_eq!(unchanged["item"]["statement"], "u2 original statement");

    let (status, updated) = call(
        app,
        Method::PATCH,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": "u2",
            "statement": "explicitly selected service mutation"
        }),
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(
        updated["item"]["statement"],
        "explicitly selected service mutation"
    );
}

#[tokio::test]
async fn tenant_service_private_reads_require_an_explicit_owner_selection() {
    let app = characterized_app();

    for (token, owner, fact_key) in [
        (OWNER_U1_TOKEN, "u1", "service-read-u1"),
        (OWNER_U2_TOKEN, "u2", "service-read-u2"),
    ] {
        let (status, body) = call(
            app.clone(),
            Method::PUT,
            &format!("/v1/state/profile/facts/{fact_key}"),
            json!({
                "state_type": "profile",
                "title": format!("{owner} state"),
                "statement": format!("private state for {owner}")
            }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, body) = call(
            app.clone(),
            Method::POST,
            "/v1/state/insights",
            json!({
                "insight_type": "private",
                "title": format!("{owner} insight"),
                "statement": format!("private insight for {owner}")
            }),
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }

    for (method, uri, body) in [
        (
            Method::GET,
            "/v1/state/profile/facts/service-read-u1",
            Value::Null,
        ),
        (Method::POST, "/v1/state/search", json!({})),
        (Method::POST, "/v1/state/insights/search", json!({})),
    ] {
        let (status, denied) =
            call(app.clone(), method, uri, body, Some(TENANT_SERVICE_TOKEN)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{denied}");
        assert_error(&denied, StatusCode::FORBIDDEN, "forbidden");
    }

    let (status, state) = call(
        app.clone(),
        Method::GET,
        "/v1/state/profile/facts/service-read-u1?owner_user_id=u1",
        Value::Null,
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state}");
    assert_eq!(state["item"]["owner_user_id"], "u1");

    let (status, states) = call(
        app.clone(),
        Method::POST,
        "/v1/state/search",
        json!({ "owner_user_id": "u1" }),
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{states}");
    assert!(!states["hits"].as_array().unwrap().is_empty());
    assert!(states["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["owner_user_id"] == "u1"));

    let (status, insights) = call(
        app,
        Method::POST,
        "/v1/state/insights/search",
        json!({ "owner_user_id": "u1" }),
        Some(TENANT_SERVICE_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{insights}");
    assert!(!insights["hits"].as_array().unwrap().is_empty());
    assert!(insights["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|item| item["owner_user_id"] == "u1"));
}

#[tokio::test]
async fn owner_scoped_legacy_bearer_defaults_owner_and_rejects_cross_owner() {
    let app = build_router(AppState::new(Arc::new(legacy_bearer_config(
        Some(BearerTokenScope::Owner),
        Some("u1"),
        false,
    ))));
    assert_owner_route(&app, LEGACY_BEARER_TOKEN, "u1", StatusCode::OK).await;
    assert_owner_route(&app, LEGACY_BEARER_TOKEN, "u2", StatusCode::FORBIDDEN).await;

    let (status, body) = call(
        app,
        Method::POST,
        "/v1/history/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "legacy-owner-default",
            "occurred_at": "2026-07-13T00:00:00Z",
            "observed_at": "2026-07-13T00:00:01Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "legacy-owner-default" },
            "text": "owner comes from the explicit bearer scope"
        }),
        Some(LEGACY_BEARER_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["event"]["owner_user_id"], "u1");
}

#[tokio::test]
async fn explicit_tenant_service_legacy_bearer_can_access_multiple_owners() {
    let app = build_router(AppState::new(Arc::new(legacy_bearer_config(
        Some(BearerTokenScope::TenantService),
        None,
        false,
    ))));
    for owner_user_id in ["u1", "u2"] {
        assert_owner_route(&app, LEGACY_BEARER_TOKEN, owner_user_id, StatusCode::OK).await;
    }
}

#[tokio::test]
async fn temporary_legacy_compatibility_mode_preserves_tenant_service_access() {
    let app = build_router(AppState::new(Arc::new(legacy_bearer_config(
        None, None, true,
    ))));
    for owner_user_id in ["u1", "u2"] {
        assert_owner_route(&app, LEGACY_BEARER_TOKEN, owner_user_id, StatusCode::OK).await;
    }
}

#[test]
fn production_rejects_ambiguous_or_incomplete_legacy_bearer_scope() {
    let mut ambiguous = legacy_bearer_config(None, None, false);
    ambiguous.run_mode = "production".to_string();
    ambiguous.index_hash_secret = STRONG_INDEX_HASH_SECRET.to_vec();
    ambiguous.allow_unsafe_unauthenticated = false;
    let error = ambiguous.validate_startup().unwrap_err().to_string();
    assert!(error.contains("RAG_BEARER_TOKEN requires"), "{error}");

    let mut owner_without_id = legacy_bearer_config(Some(BearerTokenScope::Owner), None, false);
    owner_without_id.run_mode = "production".to_string();
    owner_without_id.index_hash_secret = STRONG_INDEX_HASH_SECRET.to_vec();
    owner_without_id.allow_unsafe_unauthenticated = false;
    let error = owner_without_id.validate_startup().unwrap_err().to_string();
    assert!(
        error.contains("requires RAG_BEARER_TOKEN_OWNER_USER_ID"),
        "{error}"
    );

    let mut explicit_service =
        legacy_bearer_config(Some(BearerTokenScope::TenantService), None, false);
    explicit_service.run_mode = "production".to_string();
    explicit_service.index_hash_secret = STRONG_INDEX_HASH_SECRET.to_vec();
    explicit_service.allow_unsafe_unauthenticated = false;
    explicit_service.validate_startup().unwrap();
}

async fn assert_shared_mutations_forbidden(app: &Router, token: &str, fixture: &str) {
    let source_id = format!("forbidden-company-source-{fixture}");
    let revision_id = format!("forbidden-revision-{fixture}");
    let dataset_key = format!("forbidden_dataset_{fixture}");
    let cases = [
        (
            Method::POST,
            "/v1/state/company-docs/preflight".to_string(),
            json!({
                "title": format!("Forbidden {fixture}"),
                "text_preview": "authorization must run before shared mutation",
                "checksum": format!("checksum-{fixture}")
            }),
        ),
        (
            Method::POST,
            format!("/v1/state/company-docs/{source_id}/revisions"),
            json!({
                "title": format!("Forbidden {fixture}"),
                "content": "must not be persisted",
                "ingest": false,
                "force_create": true
            }),
        ),
        (
            Method::POST,
            format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
            json!({ "reason": "must not activate" }),
        ),
        (
            Method::PUT,
            format!("/v1/state/structured/datasets/{dataset_key}"),
            json!({
                "title": format!("Forbidden dataset {fixture}"),
                "columns": [{ "name": "value", "kind": "number" }]
            }),
        ),
        (
            Method::DELETE,
            format!("/v1/state/company-docs/{source_id}"),
            Value::Null,
        ),
    ];

    for (method, uri, body) in cases {
        let (status, response) = call(app.clone(), method, &uri, body, Some(token)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {response}");
        assert_error(&response, StatusCode::FORBIDDEN, "forbidden");
    }
}

async fn exercise_company_writer_mutations(app: &Router, token: &str, fixture: &str) -> String {
    let source_id = format!("authorized-company-source-{fixture}");
    let dataset_key = format!("authorized_dataset_{fixture}");

    let (status, preflight) = call(
        app.clone(),
        Method::POST,
        "/v1/state/company-docs/preflight",
        json!({
            "title": format!("Authorized {fixture}"),
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
            "title": format!("Authorized {fixture}"),
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
        json!({ "reason": format!("authorize {fixture}") }),
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

    source_id
}

#[tokio::test]
async fn ordinary_owner_is_denied_every_shared_mutation() {
    assert_shared_mutations_forbidden(&characterized_app(), OWNER_U1_TOKEN, "owner_u1").await;
}

#[tokio::test]
async fn denied_shared_mutation_returns_a_request_id() {
    let response = characterized_app()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/state/company-docs/preflight")
                .header(CONTENT_TYPE, "application/json")
                .header("Authorization", format!("Bearer {OWNER_U1_TOKEN}"))
                .header("x-request-id", "untrusted-client-request-id")
                .body(Body::from(
                    json!({
                        "title": "Denied request ID fixture",
                        "text_preview": "authorization fails before mutation",
                        "checksum": "denied-request-id-fixture"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    let request_id = response
        .headers()
        .get("x-request-id")
        .expect("denied responses carry x-request-id")
        .to_str()
        .expect("request ID is valid ASCII");
    assert_ne!(request_id, "untrusted-client-request-id");
    assert!(uuid::Uuid::parse_str(request_id).is_ok(), "{request_id}");
}

#[tokio::test]
async fn temporary_legacy_shared_writer_mode_preserves_preflight_only_when_enabled() {
    let preflight = json!({
        "title": "Legacy shared writer compatibility",
        "text_preview": "temporary compatibility remains explicit",
        "checksum": "legacy-shared-writer-compatibility"
    });

    let (status, body) = call(
        characterized_app(),
        Method::POST,
        "/v1/state/company-docs/preflight",
        preflight.clone(),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let mut config = characterized_config();
    config.allow_legacy_shared_writer = true;
    let app = build_router(AppState::new(Arc::new(config)));
    let (status, body) = call(
        app,
        Method::POST,
        "/v1/state/company-docs/preflight",
        preflight,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["decision_id"].is_string(), "{body}");
}

#[tokio::test]
async fn tenant_service_without_company_writer_is_denied_every_shared_mutation() {
    assert_shared_mutations_forbidden(&characterized_app(), TENANT_SERVICE_TOKEN, "tenant_service")
        .await;
}

#[tokio::test]
async fn company_writer_can_preflight_create_activate_and_upsert_dataset() {
    exercise_company_writer_mutations(&characterized_app(), COMPANY_WRITER_TOKEN, "company_writer")
        .await;
}

#[tokio::test]
async fn company_writer_cannot_delete_company_documents() {
    let app = characterized_app();
    let source_id =
        exercise_company_writer_mutations(&app, COMPANY_WRITER_TOKEN, "writer_delete").await;

    let (status, body) = call(
        app.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
        Some(COMPANY_WRITER_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_error(&body, StatusCode::FORBIDDEN, "forbidden");

    let (status, body) = call(
        app,
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["deleted"], true);
}

#[tokio::test]
async fn admin_can_preflight_create_activate_upsert_and_delete() {
    let app = characterized_app();
    let source_id = exercise_company_writer_mutations(&app, ADMIN_TOKEN, "admin").await;
    let (status, body) = call(
        app,
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["deleted"], true);
}

#[tokio::test]
async fn shared_company_reads_allow_each_authenticated_scope() {
    let app = characterized_app();
    let (status, body) = call(
        app.clone(),
        Method::GET,
        "/v1/state/company-docs",
        Value::Null,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    for token in [
        OWNER_U1_TOKEN,
        TENANT_SERVICE_TOKEN,
        COMPANY_WRITER_TOKEN,
        ADMIN_TOKEN,
    ] {
        let (status, body) = call(
            app.clone(),
            Method::GET,
            "/v1/state/company-docs",
            Value::Null,
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "token={token}: {body}");
    }
}

fn llm_status_app(auth_path: &str) -> Router {
    let mut config = characterized_config();
    config.llm_provider = "codex_auth".to_string();
    config.llm_model = Some("characterization-model".to_string());
    config.codex_auth_path = Some(auth_path.to_string());
    build_router(AppState::new(Arc::new(config)))
}

#[tokio::test]
async fn llm_status_requires_authentication_and_sanitizes_codex_auth_source() {
    let auth_path = format!(
        "/tmp/nowledge-characterization-{}/codex-auth.json",
        uuid::Uuid::now_v7()
    );
    let app = llm_status_app(&auth_path);

    let (status, body) = call(
        app.clone(),
        Method::GET,
        "/v1/llm/status",
        Value::Null,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_error(&body, StatusCode::UNAUTHORIZED, "unauthorized");

    for token in [OWNER_U1_TOKEN, TENANT_SERVICE_TOKEN, ADMIN_TOKEN] {
        let (status, body) = call(
            app.clone(),
            Method::GET,
            "/v1/llm/status",
            Value::Null,
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "token={token}: {body}");
        assert_eq!(body["provider"], "codex_auth");
        assert_eq!(body["auth_source"], "codex_file");
        assert!(!body.to_string().contains(&auth_path), "{body}");
    }
}

#[tokio::test]
async fn public_livez_and_readyz_do_not_expose_detailed_diagnostics() {
    let auth_path = format!(
        "/tmp/nowledge-readyz-{}/codex-auth.json",
        uuid::Uuid::now_v7()
    );
    let app = llm_status_app(&auth_path);

    let (status, livez) = call(app.clone(), Method::GET, "/livez", Value::Null, None).await;
    assert_eq!(status, StatusCode::OK, "{livez}");
    assert_eq!(livez["status"], "ok");
    assert!(livez.get("llm").is_none());
    assert!(livez.get("usage").is_none());

    let (status, readyz) = call(app, Method::GET, "/readyz", Value::Null, None).await;
    assert!(
        matches!(status, StatusCode::OK | StatusCode::SERVICE_UNAVAILABLE),
        "{readyz}"
    );
    assert!(readyz["ready"].is_boolean(), "{readyz}");
    for forbidden in [
        "store_backend",
        "meilisearch",
        "llm",
        "parser",
        "usage",
        "credits",
        "balance",
        "plan_type",
    ] {
        assert!(readyz.get(forbidden).is_none(), "{forbidden}: {readyz}");
    }
    let rendered = readyz.to_string();
    assert!(!rendered.contains(&auth_path), "{readyz}");
    assert!(!rendered.contains("event_count"), "{readyz}");
    assert!(!rendered.contains("document_count"), "{readyz}");
}

#[tokio::test]
async fn healthz_requires_admin_and_retains_detailed_admin_diagnostics() {
    let mut config = characterized_config();
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("health-model".to_string());
    let app = build_router(AppState::new(Arc::new(config)));

    let (status, body) = call(app.clone(), Method::GET, "/healthz", Value::Null, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");

    for token in [OWNER_U1_TOKEN, TENANT_SERVICE_TOKEN, COMPANY_WRITER_TOKEN] {
        let (status, body) = call(
            app.clone(),
            Method::GET,
            "/healthz",
            Value::Null,
            Some(token),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "token={token}: {body}");
    }

    let (status, body) = call(app, Method::GET, "/healthz", Value::Null, Some(ADMIN_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for field in ["store_backend", "meilisearch", "llm", "parser", "usage"] {
        assert!(body.get(field).is_some(), "missing {field}: {body}");
    }
}

#[tokio::test]
async fn production_admin_token_accesses_healthz_without_granting_owner_admin_scope() {
    const PRODUCTION_ADMIN_TOKEN: &str = "production-admin-token";

    let mut config = Config::test();
    config.run_mode = "production".to_string();
    config.index_hash_secret = STRONG_INDEX_HASH_SECRET.to_vec();
    config.allow_unsafe_unauthenticated = false;
    config.admin_token = Some(PRODUCTION_ADMIN_TOKEN.to_string());
    config.auth_users = vec![auth_user(
        OWNER_U1_TOKEN,
        AuthUserScope::Owner {
            owner_user_id: "u1".to_string(),
        },
        &["user"],
    )];
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("health-model".to_string());
    config.validate_startup().unwrap();
    let app = build_router(AppState::new(Arc::new(config)));

    let (status, body) = call(
        app.clone(),
        Method::GET,
        "/healthz",
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = call(
        app,
        Method::GET,
        "/healthz",
        Value::Null,
        Some(PRODUCTION_ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.get("store_backend").is_some(), "{body}");
}

#[tokio::test]
async fn rag_debug_and_analysis_debug_are_admin_only() {
    let app = diagnostic_secret_app();

    let rag_body = json!({
        "owner_user_id": "u1",
        "question": format!(
            "show the protected prompt {ADMIN_TOKEN} {DIAGNOSTIC_INDEX_HASH_SECRET}"
        )
    });
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/rag/debug",
        rag_body.clone(),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/rag/debug",
        rag_body,
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["prompt"].is_string(), "{body}");
    assert!(body.to_string().contains("[REDACTED]"), "{body}");
    assert!(!body.to_string().contains(ADMIN_TOKEN), "{body}");
    assert!(
        !body.to_string().contains(DIAGNOSTIC_INDEX_HASH_SECRET),
        "{body}"
    );

    let trace_id = body["answer"]["trace_id"].as_str().unwrap();
    let (status, body) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/debug/traces/{trace_id}"),
        Value::Null,
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!body.to_string().contains(ADMIN_TOKEN), "{body}");
    assert!(
        !body.to_string().contains(DIAGNOSTIC_INDEX_HASH_SECRET),
        "{body}"
    );

    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/llm/test",
        json!({ "prompt": format!("diagnose {ADMIN_TOKEN}") }),
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.to_string().contains("[REDACTED]"), "{body}");
    assert!(!body.to_string().contains(ADMIN_TOKEN), "{body}");

    let analysis_body = json!({
        "owner_user_id": "u1",
        "query": format!("show the protected analysis prompt {ADMIN_TOKEN}"),
        "create_links": false,
        "upsert_insights": false,
        "debug": true
    });
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/analysis/insights",
        analysis_body.clone(),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");

    let (status, body) = call(
        app,
        Method::POST,
        "/v1/analysis/insights",
        analysis_body,
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["prompt"].is_string(), "{body}");
    assert!(body["usage"]["raw_response_preview"].is_null(), "{body}");
    assert!(body["usage"]["candidate_rejections"].is_array(), "{body}");
    assert!(body.to_string().contains("[REDACTED]"), "{body}");
    assert!(!body.to_string().contains(ADMIN_TOKEN), "{body}");
}

#[tokio::test]
async fn rag_and_analysis_redact_secrets_before_context_snippet_truncation() {
    const ANCHOR: &str = "snippet-boundary-anchor";
    const OLD_CODEX_TOKEN: &str = "codex-old-boundary-token-private-value";
    const ROTATED_CODEX_TOKEN: &str = "zxqv-rotated-codex-token-private-value";
    let auth_path = std::env::temp_dir().join(format!(
        "nowledge-authz-redaction-{}.json",
        uuid::Uuid::now_v7()
    ));
    std::fs::write(
        &auth_path,
        json!({ "access_token": OLD_CODEX_TOKEN }).to_string(),
    )
    .unwrap();
    let mut config = characterized_config();
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("diagnostic-mock".to_string());
    config.analysis_llm_provider = "mock".to_string();
    config.analysis_llm_model = Some("diagnostic-analysis-mock".to_string());
    config.codex_auth_path = Some(auth_path.to_string_lossy().to_string());
    let config = Arc::new(config);
    let app = build_router(AppState::new(config.clone()));

    let (status, configured_ingest) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": "u1",
            "source_id": "configured-response-secret-source",
            "revision_id": "v1",
            "title": "Configured Response Secret Source",
            "file_name": "configured-response.pdf",
            "content_type": "application/pdf",
            "content": format!("sync response contains {OLD_CODEX_TOKEN}"),
            "content_list_v2": [{
                "type": "paragraph",
                "text": format!("parsed response contains {OLD_CODEX_TOKEN}"),
                "reading_order": 0
            }]
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{configured_ingest}");
    assert!(
        configured_ingest.to_string().contains("[REDACTED]"),
        "{configured_ingest}"
    );
    assert!(
        !configured_ingest.to_string().contains(OLD_CODEX_TOKEN),
        "{configured_ingest}"
    );
    let configured_source_uri = configured_ingest["source_document_uri"].as_str().unwrap();
    let (status, configured_source) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(configured_source_uri)),
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{configured_source}");
    assert!(
        configured_source.to_string().contains('*'),
        "{configured_source}"
    );
    assert!(
        !configured_source.to_string().contains(OLD_CODEX_TOKEN),
        "{configured_source}"
    );

    let prefix = format!("{ANCHOR} ");
    let padding = "x".repeat(229 - prefix.chars().count());
    let boundary_content = format!("{prefix}{padding}{ROTATED_CODEX_TOKEN}");

    let (status, ingest) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": "u1",
            "source_id": "snippet-boundary-source",
            "revision_id": "v1",
            "title": "Snippet Boundary Source",
            "content": boundary_content,
            "fragment_policy": {
                "chunk_size_chars": 240,
                "overlap_chars": 0,
                "min_chunk_chars": 240
            }
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ingest}");
    std::fs::write(
        &auth_path,
        json!({ "access_token": ROTATED_CODEX_TOKEN }).to_string(),
    )
    .unwrap();
    let _ = config.refresh_configured_secret_values();
    let fragment_uris = ingest["fragment_uris"].as_array().unwrap();
    assert_eq!(fragment_uris.len(), 2, "{ingest}");

    let source_document_uri = ingest["source_document_uri"].as_str().unwrap();
    let (status, source_document) = call(
        app.clone(),
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(source_document_uri)),
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{source_document}");
    assert!(
        source_document.to_string().contains('*'),
        "{source_document}"
    );
    assert!(
        !source_document.to_string().contains(ROTATED_CODEX_TOKEN),
        "{source_document}"
    );
    assert!(
        !source_document.to_string().contains("zxqv-"),
        "{source_document}"
    );

    let mut fragment_bodies = String::new();
    for uri in fragment_uris {
        let uri = uri.as_str().unwrap();
        let (status, fragment) = call(
            app.clone(),
            Method::GET,
            &format!("/v1/fs/read?uri={}", query_encode(uri)),
            Value::Null,
            Some(OWNER_U1_TOKEN),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{fragment}");
        fragment_bodies.push_str(fragment["body"].as_str().unwrap());
    }
    assert!(fragment_bodies.contains('*'), "{fragment_bodies}");
    assert!(!fragment_bodies.contains("zxqv-"), "{fragment_bodies}");
    assert!(
        !fragment_bodies.contains(ROTATED_CODEX_TOKEN),
        "{fragment_bodies}"
    );

    let (status, rag) = call(
        app.clone(),
        Method::POST,
        "/v1/rag/debug",
        json!({ "owner_user_id": "u1", "question": ANCHOR }),
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rag}");
    let rag_prompt = rag["prompt"].as_str().unwrap();
    assert!(rag_prompt.contains('*'), "{rag_prompt}");
    assert!(!rag_prompt.contains("zxqv-"), "{rag_prompt}");
    assert!(!rag.to_string().contains(ROTATED_CODEX_TOKEN), "{rag}");

    let (status, analysis) = call(
        app.clone(),
        Method::POST,
        "/v1/analysis/insights",
        json!({
            "owner_user_id": "u1",
            "query": ANCHOR,
            "create_links": false,
            "upsert_insights": false,
            "debug": true
        }),
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{analysis}");
    let analysis_prompt = analysis["prompt"].as_str().unwrap();
    assert!(analysis_prompt.contains('*'), "{analysis_prompt}");
    assert!(!analysis_prompt.contains("zxqv-"), "{analysis_prompt}");

    let (status, event) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "snippet-boundary-event",
            "owner_user_id": "u1",
            "occurred_at": "2026-07-13T00:00:00Z",
            "observed_at": "2026-07-13T00:00:01Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "snippet-boundary-event" },
            "text": format!("{prefix}{padding}{ROTATED_CODEX_TOKEN}"),
            "privacy": "private"
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");
    assert!(event.to_string().contains("[REDACTED]"), "{event}");
    assert!(!event.to_string().contains(ROTATED_CODEX_TOKEN), "{event}");
    let event_id = event["event"]["id"].as_str().unwrap();

    let (status, history_analysis) = call(
        app,
        Method::POST,
        "/v1/analysis/insights",
        json!({
            "owner_user_id": "u1",
            "history_event_id": event_id,
            "query": ANCHOR,
            "create_links": false,
            "upsert_insights": false,
            "debug": true
        }),
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{history_analysis}");
    let history_prompt = history_analysis["prompt"].as_str().unwrap();
    assert!(history_prompt.contains("[REDACTED]"), "{history_prompt}");
    assert!(!history_prompt.contains("zxqv-"), "{history_prompt}");

    let _ = std::fs::remove_file(auth_path);
}

#[tokio::test]
async fn rotated_secret_split_across_three_fragments_cannot_be_reconstructed() {
    const OLD_CODEX_TOKEN: &str = "codex-old-q7x9p2m4v8-token";
    const ROTATED_CODEX_TOKEN: &str = "abcdefghij";
    let auth_path = std::env::temp_dir().join(format!(
        "nowledge-authz-three-fragment-redaction-{}.json",
        uuid::Uuid::now_v7()
    ));
    std::fs::write(
        &auth_path,
        json!({ "access_token": OLD_CODEX_TOKEN }).to_string(),
    )
    .unwrap();
    let mut config = characterized_config();
    config.llm_provider = "mock".to_string();
    config.llm_model = Some("diagnostic-mock".to_string());
    config.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
    let config = Arc::new(config);
    let app = build_router(AppState::new(config.clone()));

    let (status, ingest) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": "u1",
            "source_id": "three-fragment-rotation",
            "revision_id": "v1",
            "title": "Three fragment rotation",
            "content": format!("X{ROTATED_CODEX_TOKEN}"),
            "fragment_policy": {
                "chunk_size_chars": 4,
                "overlap_chars": 0,
                "min_chunk_chars": 4
            }
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{ingest}");
    assert!(ingest["fragment_uris"].is_array(), "{ingest}");
    assert_eq!(ingest["fragment_uris"].as_array().unwrap().len(), 3);

    std::fs::write(
        &auth_path,
        json!({ "access_token": ROTATED_CODEX_TOKEN }).to_string(),
    )
    .unwrap();
    let _ = config.refresh_configured_secret_values();

    let mut projected = String::new();
    for uri in ingest["fragment_uris"].as_array().unwrap() {
        let (status, fragment) = call(
            app.clone(),
            Method::GET,
            &format!("/v1/fs/read?uri={}", query_encode(uri.as_str().unwrap())),
            Value::Null,
            Some(OWNER_U1_TOKEN),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{fragment}");
        projected.push_str(fragment["body"].as_str().unwrap());
    }
    assert!(projected.contains("****"), "{projected}");
    assert!(!projected.contains(ROTATED_CODEX_TOKEN), "{projected}");

    let (status, rag) = call(
        app,
        Method::POST,
        "/v1/rag/debug",
        json!({ "owner_user_id": "u1", "question": "defg" }),
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{rag}");
    assert!(!rag["prompt"]
        .as_str()
        .unwrap()
        .contains(ROTATED_CODEX_TOKEN));

    let _ = std::fs::remove_file(auth_path);
}

#[tokio::test]
async fn configured_credentials_never_escape_typed_json_responses() {
    let app = characterized_app();
    let secret_text = format!("typed response contains {OWNER_U1_TOKEN}");

    let (status, state_item) = call(
        app.clone(),
        Method::PUT,
        "/v1/state/profile/facts/response-redaction",
        json!({
            "state_type": "profile",
            "statement": secret_text
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{state_item}");
    assert!(
        state_item.to_string().contains("[REDACTED]"),
        "{state_item}"
    );
    assert!(
        !state_item.to_string().contains(OWNER_U1_TOKEN),
        "{state_item}"
    );

    let (status, insight) = call(
        app.clone(),
        Method::POST,
        "/v1/state/insights",
        json!({
            "insight_type": "security",
            "title": "Typed response redaction",
            "statement": secret_text,
            "evidence_text": secret_text
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{insight}");
    assert!(insight.to_string().contains("[REDACTED]"), "{insight}");
    assert!(!insight.to_string().contains(OWNER_U1_TOKEN), "{insight}");

    let (status, link) = call(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "source_uri": "ctx://user/typed-response/source",
            "target_uri": "ctx://user/typed-response/target",
            "relation": "supports",
            "rationale": secret_text,
            "evidence_text": secret_text
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{link}");
    assert!(link.to_string().contains("[REDACTED]"), "{link}");
    assert!(!link.to_string().contains(OWNER_U1_TOKEN), "{link}");

    let (status, event) = call(
        app,
        Method::POST,
        "/v1/history/users/u1/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "typed-response-redaction",
            "owner_user_id": "u1",
            "occurred_at": "2026-07-13T00:00:00Z",
            "observed_at": "2026-07-13T00:00:01Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "typed-response-redaction" },
            "text": secret_text,
            "privacy": "private"
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");
    assert!(event.to_string().contains("[REDACTED]"), "{event}");
    assert!(!event.to_string().contains(OWNER_U1_TOKEN), "{event}");
}

#[tokio::test]
async fn rotated_secret_truncated_inside_history_abstract_is_masked() {
    const OLD_CODEX_TOKEN: &str = "codex-old-abstract-token-private-value";
    const ROTATED_CODEX_TOKEN: &str = "zxqv-super-secret-admin-token-private-value";
    let auth_path = std::env::temp_dir().join(format!(
        "nowledge-authz-abstract-redaction-{}.json",
        uuid::Uuid::now_v7()
    ));
    std::fs::write(
        &auth_path,
        json!({ "access_token": OLD_CODEX_TOKEN }).to_string(),
    )
    .unwrap();
    let mut config = characterized_config();
    config.codex_auth_path = Some(auth_path.to_string_lossy().to_string());
    let config = Arc::new(config);
    let app = build_router(AppState::new(config.clone()));
    let event_text = format!("{}{}", "x".repeat(490), ROTATED_CODEX_TOKEN);

    let (status, event) = call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "rotated-abstract-redaction",
            "owner_user_id": "u1",
            "occurred_at": "2026-07-13T00:00:00Z",
            "observed_at": "2026-07-13T00:00:01Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "rotated-abstract-redaction" },
            "text": event_text,
            "privacy": "private"
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");
    let event_id = event["event"]["id"].as_str().unwrap();

    std::fs::write(
        &auth_path,
        json!({ "access_token": ROTATED_CODEX_TOKEN }).to_string(),
    )
    .unwrap();
    let _ = config.refresh_configured_secret_values();
    let uri = format!("ctx://user/history/note-created/{event_id}/.abstract");
    let (status, abstract_node) = call(
        app,
        Method::GET,
        &format!("/v1/fs/read?uri={}", query_encode(&uri)),
        Value::Null,
        Some(OWNER_U1_TOKEN),
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{abstract_node}");
    let body = abstract_node["body"].as_str().unwrap();
    assert!(!body.contains("zxqv-su"), "{body}");
    assert!(!body.contains(ROTATED_CODEX_TOKEN), "{body}");
    assert!(body.ends_with("*******..."), "{body}");

    let _ = std::fs::remove_file(auth_path);
}

#[tokio::test]
async fn ordinary_owner_analysis_without_debug_succeeds_and_omits_prompt() {
    let (status, body) = call(
        characterized_app(),
        Method::POST,
        "/v1/analysis/insights",
        json!({
            "owner_user_id": "u1",
            "query": "summarize the owner's private context",
            "create_links": false,
            "upsert_insights": false,
            "debug": false
        }),
        Some(OWNER_U1_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["query"], "summarize the owner's private context");
    assert!(body.get("prompt").is_none(), "{body}");
}

#[tokio::test]
async fn llm_backed_responses_redact_configured_secrets_for_ordinary_callers() {
    let app = diagnostic_secret_app();

    for (path, body) in [
        (
            "/v1/rag/answer",
            json!({
                "owner_user_id": "u1",
                "question": format!("summarize {OWNER_U1_TOKEN}")
            }),
        ),
        (
            "/v1/llm/title",
            json!({ "content": format!("document containing {OWNER_U1_TOKEN}") }),
        ),
        (
            "/v1/analysis/insights",
            json!({
                "owner_user_id": "u1",
                "query": format!("analyze {OWNER_U1_TOKEN}"),
                "create_links": false,
                "upsert_insights": false,
                "debug": false
            }),
        ),
    ] {
        let (status, response) =
            call(app.clone(), Method::POST, path, body, Some(OWNER_U1_TOKEN)).await;
        assert_eq!(status, StatusCode::OK, "{path}: {response}");
        assert!(!response.to_string().contains(OWNER_U1_TOKEN), "{response}");
    }
}

#[tokio::test]
async fn representative_runtime_routes_enforce_their_guard_classes() {
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

#[tokio::test]
async fn operation_journal_routes_require_admin_scope() {
    let app = characterized_app();

    for (uri, body) in [
        ("/v1/admin/operations/search", json!({})),
        ("/v1/admin/operations:reconcile", json!({ "dry_run": true })),
    ] {
        let (status, response) = call(app.clone(), Method::POST, uri, body.clone(), None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{uri}: {response}");

        let (status, response) = call(
            app.clone(),
            Method::POST,
            uri,
            body.clone(),
            Some(OWNER_U1_TOKEN),
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{uri}: {response}");

        let (status, response) =
            call(app.clone(), Method::POST, uri, body, Some(ADMIN_TOKEN)).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {response}");
        assert!(response["operations"].is_array(), "{uri}: {response}");
        if uri.ends_with(":reconcile") {
            assert_eq!(response["checked"], 0, "{uri}: {response}");
            assert!(response["errors"].is_array(), "{uri}: {response}");
        }
    }
}

async fn index_mutation_plan(token: &str, owner_user_id: &str) -> Value {
    let app = characterized_app();
    let (status, response) = call(
        app.clone(),
        Method::PUT,
        &format!("/v1/history/users/{owner_user_id}/event-index"),
        json!({}),
        Some(token),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{response}");

    let (status, journal) = call(
        app,
        Method::POST,
        "/v1/admin/operations/search",
        json!({
            "operation_kinds": ["user_event_index.ensure"],
            "include_plan": true
        }),
        Some(ADMIN_TOKEN),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{journal}");
    let operations = journal["operations"].as_array().unwrap();
    assert_eq!(operations.len(), 1, "{journal}");
    operations[0]["plan"].clone()
}

#[tokio::test]
async fn operation_journal_actor_uses_authenticated_principal_scope_and_roles() {
    let owner_plan = index_mutation_plan(OWNER_U1_TOKEN, "u1").await;
    assert_eq!(owner_plan["actor"]["scope"], "owner", "{owner_plan}");
    assert_eq!(owner_plan["actor"]["roles"], json!(["user"]));
    let owner_hash = owner_plan["actor"]["owner_user_id_hash"]
        .as_str()
        .expect("owner actors retain only an HMAC-derived owner identity");
    assert!(!owner_hash.is_empty(), "{owner_plan}");
    assert_ne!(owner_hash, "u1", "{owner_plan}");
    assert!(
        owner_plan["actor"]["request_id"].as_str().is_some(),
        "{owner_plan}"
    );

    let service_plan = index_mutation_plan(TENANT_SERVICE_TOKEN, "service-owner").await;
    assert_eq!(
        service_plan["actor"]["scope"], "tenant_service",
        "{service_plan}"
    );
    assert_eq!(service_plan["actor"]["roles"], json!(["user"]));
    assert!(
        service_plan["actor"].get("owner_user_id_hash").is_none(),
        "{service_plan}"
    );

    let admin_plan = index_mutation_plan(ADMIN_TOKEN, "admin-owner").await;
    assert_eq!(admin_plan["actor"]["scope"], "admin", "{admin_plan}");
    assert_eq!(admin_plan["actor"]["roles"], json!(["admin"]));
    assert!(
        admin_plan["actor"].get("owner_user_id_hash").is_none(),
        "{admin_plan}"
    );
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GuardPolicy {
    Public,
    User,
    CompanyWriter,
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
        "Health" => match entry.handler.as_str() {
            "livez" | "readyz" => GuardPolicy::Public,
            "healthz" => GuardPolicy::Admin,
            other => panic!("unclassified health handler: {other}"),
        },
        "Admin" | "Harness" | "Debug" | "Eval" => GuardPolicy::Admin,
        "LLM" => match entry.handler.as_str() {
            "llm_status" | "llm_title" => GuardPolicy::User,
            "llm_test" => GuardPolicy::Admin,
            other => panic!("unclassified LLM handler: {other}"),
        },
        "Company Docs" => match entry.handler.as_str() {
            "preflight_doc" | "create_revision" | "activate_revision" => GuardPolicy::CompanyWriter,
            "delete_company_doc" => GuardPolicy::Admin,
            "list_company_docs" | "get_company_doc" | "list_revisions" => GuardPolicy::User,
            other => panic!("unclassified company-doc handler: {other}"),
        },
        "Structured State" if entry.handler == "upsert_dataset" => GuardPolicy::CompanyWriter,
        "RAG" if entry.handler == "rag_debug" => GuardPolicy::Admin,
        "Analysis"
        | "Context"
        | "Context FS"
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
    let policies = [
        (signature.contains("UserGuard"), GuardPolicy::User),
        (
            signature.contains("CompanyWriterGuard"),
            GuardPolicy::CompanyWriter,
        ),
        (signature.contains("AdminGuard"), GuardPolicy::Admin),
    ];
    let present: Vec<_> = policies
        .into_iter()
        .filter_map(|(present, policy)| present.then_some(policy))
        .collect();
    assert!(
        present.len() <= 1,
        "handler {handler} unexpectedly declares multiple guard types: {signature}"
    );
    present.first().copied().unwrap_or(GuardPolicy::Public)
}

#[test]
fn route_policy_matrix_covers_every_manifest_handler_and_group() {
    let manifest: Vec<ManifestEntry> =
        serde_json::from_str(include_str!("../doc/api_manifest.json")).unwrap();
    let routes = include_str!("../src/routes.rs");
    assert_eq!(
        manifest.len(),
        89,
        "the policy matrix must cover all routes"
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
    assert_eq!(policy_for("livez"), GuardPolicy::Public);
    assert_eq!(policy_for("readyz"), GuardPolicy::Public);
    assert_eq!(policy_for("healthz"), GuardPolicy::Admin);
    assert_eq!(policy_for("llm_status"), GuardPolicy::User);
    assert_eq!(policy_for("llm_test"), GuardPolicy::Admin);
    assert_eq!(policy_for("llm_title"), GuardPolicy::User);
    assert_eq!(policy_for("preflight_doc"), GuardPolicy::CompanyWriter);
    assert_eq!(policy_for("delete_company_doc"), GuardPolicy::Admin);
    assert_eq!(policy_for("upsert_dataset"), GuardPolicy::CompanyWriter);
    assert_eq!(policy_for("rag_debug"), GuardPolicy::Admin);
    assert_eq!(policy_for("search_operations"), GuardPolicy::Admin);
    assert_eq!(policy_for("reconcile_operations"), GuardPolicy::Admin);
    assert!(manifest
        .iter()
        .filter(|entry| entry.group == "Debug")
        .all(|entry| expected_policy(entry) == GuardPolicy::Admin));
}
