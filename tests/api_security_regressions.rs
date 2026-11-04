//! Regressions for review findings M8 (ingest URI/parser authorization),
//! M7 (structured row pagination), L5 (apply-snapshot owner check), and L4
//! (case-insensitive Bearer scheme).

use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{
    build_router,
    config::{AuthUserConfig, AuthUserScope},
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

const OWNER_U1_TOKEN: &str = "owner-u1-token";
const OWNER_U2_TOKEN: &str = "owner-u2-token";
const TENANT_SERVICE_TOKEN: &str = "tenant-service-token";
const ADMIN_TOKEN: &str = "admin-token";
const MAX_ROWS_PAGE: usize = 10;

fn auth_user(token: &str, scope: AuthUserScope, roles: &[&str]) -> AuthUserConfig {
    AuthUserConfig {
        token: token.to_string(),
        scope,
        roles: roles.iter().map(|role| (*role).to_string()).collect(),
    }
}

fn config() -> Config {
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
        auth_user(ADMIN_TOKEN, AuthUserScope::Admin, &["admin"]),
    ];
    config.parser_provider = "builtin".to_string();
    config.max_bulk_rows = MAX_ROWS_PAGE;
    config
}

fn app() -> Router {
    build_router(AppState::new(Arc::new(config())))
}

async fn read(response: axum::response::Response) -> (StatusCode, Value) {
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

async fn call_with_authorization(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
    authorization: Option<String>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json");
    if let Some(authorization) = authorization {
        builder = builder.header("Authorization", authorization);
    }
    let request_body = if body.is_null() {
        Body::empty()
    } else {
        Body::from(body.to_string())
    };
    read(
        app.oneshot(builder.body(request_body).unwrap())
            .await
            .unwrap(),
    )
    .await
}

async fn call(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
    token: &str,
) -> (StatusCode, Value) {
    call_with_authorization(app, method, uri, body, Some(format!("Bearer {token}"))).await
}

async fn call_multipart(
    app: Router,
    uri: &str,
    fields: &[(&str, &str)],
    token: &str,
) -> (StatusCode, Value) {
    let boundary = "nowledge-security-boundary";
    let mut body = Vec::new();
    for (name, value) in fields {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"note.md\"\r\nContent-Type: text/markdown\r\n\r\n# Note\n\nmultipart security body\r\n--{boundary}--\r\n"
        )
        .as_bytes(),
    );
    let request = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body))
        .unwrap();
    read(app.oneshot(request).await.unwrap()).await
}

fn assert_error(body: &Value, status: StatusCode, code: &str) {
    assert_eq!(body["error"]["code"], code, "{body}");
    assert_eq!(
        body["error"]["details"]["status"],
        status.as_u16(),
        "{body}"
    );
}

fn assert_validation(status: StatusCode, body: &Value, field: &str) {
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_error(body, StatusCode::BAD_REQUEST, "validation_error");
    assert_eq!(body["error"]["details"]["field"], field, "{body}");
}

async fn ingest_task_count(app: Router) -> Value {
    let (status, usage) = call(app, Method::GET, "/v1/usage", Value::Null, ADMIN_TOKEN).await;
    assert_eq!(status, StatusCode::OK, "{usage}");
    usage["providers"]["ingest"]["task_count"].clone()
}

fn ingest_body(extra: Value) -> Value {
    let mut body = json!({
        "owner_user_id": "u1",
        "source_id": "security-source",
        "revision_id": "v1",
        "title": "Security Source",
        "content": "security regression body content"
    });
    for (key, value) in extra.as_object().unwrap() {
        body[key] = value.clone();
    }
    body
}

#[tokio::test]
async fn non_admin_ingest_cannot_claim_non_personal_source_document_uris() {
    let app = app();
    for (token, uri) in [
        (OWNER_U1_TOKEN, "ctx://company/ingest/security/source/v1"),
        (OWNER_U1_TOKEN, "ctx://users/u1/source"),
        (OWNER_U1_TOKEN, "ctx://user/"),
        (OWNER_U1_TOKEN, "https://example.com/doc"),
        (
            TENANT_SERVICE_TOKEN,
            "ctx://company/ingest/security/source/v1",
        ),
    ] {
        for route in ["/v1/ingest/files:sync", "/v1/ingest/tasks"] {
            let (status, body) = call(
                app.clone(),
                Method::POST,
                route,
                ingest_body(json!({ "source_document_uri": uri })),
                token,
            )
            .await;
            assert_validation(status, &body, "source_document_uri");
        }
    }
    let (status, body) = call_multipart(
        app.clone(),
        "/v1/ingest/uploads:sync",
        &[
            ("owner_user_id", "u1"),
            ("source_document_uri", "ctx://company/spoofed/source"),
        ],
        OWNER_U1_TOKEN,
    )
    .await;
    assert_validation(status, &body, "source_document_uri");
    assert_eq!(ingest_task_count(app.clone()).await, 0);

    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        ingest_body(json!({
            "source_id": "security-custom-uri",
            "source_document_uri": "ctx://user/notes/security/v1"
        })),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["source_document_uri"], "ctx://user/notes/security/v1");

    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        ingest_body(json!({ "source_id": "security-default-uri" })),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(
        body["source_document_uri"]
            .as_str()
            .unwrap()
            .starts_with("ctx://user/"),
        "{body}"
    );
}

#[tokio::test]
async fn admin_ingest_keeps_company_and_custom_source_document_uris() {
    let app = app();
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "source_id": "admin-company-source",
            "revision_id": "v1",
            "source_document_uri": "ctx://company/ingest/admin-company-source/source/v1",
            "content": "company ingest content written by an administrator"
        }),
        ADMIN_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(
        body["source_document_uri"],
        "ctx://company/ingest/admin-company-source/source/v1"
    );
}

#[tokio::test]
async fn non_admin_ingest_cannot_override_parser_selection() {
    let app = app();
    for extra in [
        json!({ "parser_provider": "mineru" }),
        json!({ "parser_backend": "pipeline" }),
    ] {
        for route in ["/v1/ingest/files:sync", "/v1/ingest/tasks"] {
            let (status, body) = call(
                app.clone(),
                Method::POST,
                route,
                ingest_body(extra.clone()),
                OWNER_U1_TOKEN,
            )
            .await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert_error(&body, StatusCode::FORBIDDEN, "forbidden");
        }
    }
    for route in ["/v1/ingest/uploads", "/v1/ingest/uploads:sync"] {
        let (status, body) = call_multipart(
            app.clone(),
            route,
            &[("owner_user_id", "u1"), ("parser_provider", "mineru")],
            OWNER_U1_TOKEN,
        )
        .await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
        assert_error(&body, StatusCode::FORBIDDEN, "forbidden");
    }
    assert_eq!(ingest_task_count(app.clone()).await, 0);

    // Echoing the configured default remains allowed.
    let (status, body) = call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        ingest_body(json!({ "parser_provider": "builtin" })),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["task"]["parser_provider"], "builtin");
}

#[tokio::test]
async fn admin_ingest_may_override_parser_selection() {
    let mut config = config();
    config.mineru_api_url = "http://127.0.0.1:9/unreachable".to_string();
    let app = build_router(AppState::new(Arc::new(config)));
    let (status, body) = call(
        app,
        Method::POST,
        "/v1/ingest/tasks",
        ingest_body(json!({ "parser_provider": "mineru" })),
        ADMIN_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["parser_provider"], "mineru");
}

async fn create_snapshot(app: Router, token: &str) -> String {
    let (status, body) = call(
        app,
        Method::POST,
        "/v1/history/structured/snapshots",
        json!({
            "dataset_key": "security-rows",
            "owner_user_id": "u1",
            "period_key": "2026-W45",
            "period_start": "2026-11-02T00:00:00Z",
            "period_end": "2026-11-08T23:59:59Z",
            "granularity": "weekly",
            "source_ref": { "kind": "test", "id": "api-security-regressions" }
        }),
        token,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body["snapshot"]["id"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn apply_snapshot_requires_snapshot_id_and_enforces_owner() {
    let app = app();
    let snapshot_id = create_snapshot(app.clone(), OWNER_U1_TOKEN).await;
    let route = "/v1/state/structured/datasets/security-rows/apply-snapshot";

    for token in [OWNER_U1_TOKEN, ADMIN_TOKEN] {
        let (status, body) = call(
            app.clone(),
            Method::POST,
            route,
            json!({ "llm_mode": "none" }),
            token,
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert_error(&body, StatusCode::BAD_REQUEST, "bad_request");
        assert_eq!(body["error"]["message"], "snapshot_id is required");
    }

    let (status, body) = call(
        app.clone(),
        Method::POST,
        route,
        json!({ "snapshot_id": snapshot_id }),
        OWNER_U2_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert_error(&body, StatusCode::FORBIDDEN, "forbidden");

    let (status, body) = call(
        app,
        Method::POST,
        route,
        json!({ "snapshot_id": snapshot_id }),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["snapshot_id"], snapshot_id);
}

fn row_ids(body: &Value) -> Vec<String> {
    body["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["id"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn structured_rows_support_optional_keyset_pagination() {
    let app = app();
    let snapshot_id = create_snapshot(app.clone(), OWNER_U1_TOKEN).await;
    let (status, body) = call(
        app.clone(),
        Method::POST,
        &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
        json!({ "rows": [
            { "id": "r3", "value": 3 },
            { "id": "r1", "value": 1 },
            { "id": "r5", "value": 5 },
            { "id": "r2", "value": 2 },
            { "id": "r4", "value": 4 }
        ] }),
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let rows_uri = format!("/v1/history/structured/snapshots/{snapshot_id}/rows");

    // Unpaginated shape and order are unchanged.
    let (status, body) = call(
        app.clone(),
        Method::GET,
        &rows_uri,
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_ids(&body), ["r3", "r1", "r5", "r2", "r4"]);
    assert_eq!(body["snapshot_id"], snapshot_id);
    assert!(body.get("next_cursor").is_none(), "{body}");

    let mut collected = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let uri = match &cursor {
            Some(cursor) => format!("{rows_uri}?limit=2&cursor={cursor}"),
            None => format!("{rows_uri}?limit=2"),
        };
        let (status, body) =
            call(app.clone(), Method::GET, &uri, Value::Null, OWNER_U1_TOKEN).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(body.get("next_cursor").is_some(), "{body}");
        let page = row_ids(&body);
        assert!(page.len() <= 2, "{body}");
        collected.extend(page);
        pages += 1;
        match body["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => break,
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(collected, ["r1", "r2", "r3", "r4", "r5"]);

    // Cursor without limit uses the default page size (capped by the limit).
    let (status, body) = call(
        app.clone(),
        Method::GET,
        &format!("{rows_uri}?limit=1"),
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let first_cursor = body["next_cursor"].as_str().unwrap().to_string();
    let (status, body) = call(
        app.clone(),
        Method::GET,
        &format!("{rows_uri}?cursor={first_cursor}"),
        Value::Null,
        OWNER_U1_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(row_ids(&body), ["r2", "r3", "r4", "r5"]);
    assert!(body["next_cursor"].is_null(), "{body}");

    for (query, field) in [
        ("limit=0".to_string(), "limit"),
        (format!("limit={}", MAX_ROWS_PAGE + 1), "limit"),
        ("cursor=not-a-cursor".to_string(), "cursor"),
        ("cursor=rows1.zz".to_string(), "cursor"),
        ("cursor=rows1.".to_string(), "cursor"),
    ] {
        let (status, body) = call(
            app.clone(),
            Method::GET,
            &format!("{rows_uri}?{query}"),
            Value::Null,
            OWNER_U1_TOKEN,
        )
        .await;
        assert_validation(status, &body, field);
    }

    let (status, body) = call(
        app,
        Method::GET,
        &format!("{rows_uri}?limit=2"),
        Value::Null,
        OWNER_U2_TOKEN,
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
}

#[tokio::test]
async fn bearer_auth_scheme_is_case_insensitive() {
    let app = app();
    for header in [
        format!("Bearer {OWNER_U1_TOKEN}"),
        format!("bearer {OWNER_U1_TOKEN}"),
        format!("BEARER {OWNER_U1_TOKEN}"),
    ] {
        let (status, body) = call_with_authorization(
            app.clone(),
            Method::GET,
            "/v1/usage",
            Value::Null,
            Some(header),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    for header in [
        format!("Basic {OWNER_U1_TOKEN}"),
        format!("Bearer{OWNER_U1_TOKEN}"),
        format!("Bearer {}", OWNER_U1_TOKEN.to_uppercase()),
        "Bearer ".to_string(),
    ] {
        let (status, body) = call_with_authorization(
            app.clone(),
            Method::GET,
            "/v1/usage",
            Value::Null,
            Some(header),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
    }
}
