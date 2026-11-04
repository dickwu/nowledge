use std::sync::Arc;

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use tower::ServiceExt;

const MAX_SEARCH_LIMIT: usize = 2;
const MAX_TAG_BYTES: usize = 4;
const MAX_TAGS_PER_ITEM: usize = 1;
const MAX_BULK_ITEMS: usize = 1;

struct JsonResponse {
    status: StatusCode,
    request_id: String,
    body: Value,
}

fn bounded_app() -> (AppState, Router) {
    let mut config = Config::test();
    config.max_bulk_events = MAX_BULK_ITEMS;
    config.max_bulk_rows = MAX_BULK_ITEMS;
    config.max_search_limit = MAX_SEARCH_LIMIT;
    config.max_tags_per_item = MAX_TAGS_PER_ITEM;
    config.max_tag_bytes = MAX_TAG_BYTES;
    config.rate_limit_requests_per_minute = 10_000;
    let state = AppState::new(Arc::new(config));
    let app = build_router(state.clone());
    (state, app)
}

async fn json_call(app: Router, method: Method, uri: &str, body: Value) -> JsonResponse {
    let response = app
        .oneshot(
            Request::builder()
                .method(method)
                .uri(uri)
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    JsonResponse {
        status,
        request_id,
        body,
    }
}

fn assert_validation(response: &JsonResponse, field: &str, message: &str) {
    assert_eq!(
        response.status,
        StatusCode::BAD_REQUEST,
        "{}",
        response.body
    );
    assert!(
        uuid::Uuid::parse_str(&response.request_id).is_ok(),
        "missing or invalid x-request-id: {:?}",
        response.request_id
    );
    assert_eq!(
        response.body,
        json!({
            "error": {
                "code": "validation_error",
                "message": message,
                "details": {
                    "status": 400,
                    "field": field
                }
            }
        })
    );
}

#[tokio::test]
async fn canonical_and_alias_history_routes_share_bounded_validation_contracts() {
    let (state, app) = bounded_app();
    let oversized_tag = "ééé";
    let tag_bytes_message = format!("must be at most {MAX_TAG_BYTES} UTF-8 bytes");
    let tag_count_message = format!("must contain at most {MAX_TAGS_PER_ITEM} items");
    let bulk_count_message = format!("must contain at most {MAX_BULK_ITEMS} items");
    let search_limit_message = format!("must be at most {MAX_SEARCH_LIMIT}");

    for uri in ["/v1/history/users/u1/events", "/v1/history/events"] {
        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({
                "owner_user_id": "u1",
                "text": "oversized UTF-8 tag",
                "tags": [oversized_tag]
            }),
        )
        .await;
        assert_validation(&response, "tags[0]", &tag_bytes_message);

        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({
                "owner_user_id": "u1",
                "text": "too many tags",
                "tags": ["a", "b"]
            }),
        )
        .await;
        assert_validation(&response, "tags", &tag_count_message);
    }

    for uri in [
        "/v1/history/users/u1/events:bulk",
        "/v1/history/events:bulk",
    ] {
        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({
                "events": [
                    { "owner_user_id": "u1", "text": "one" },
                    { "owner_user_id": "u1", "text": "two" }
                ]
            }),
        )
        .await;
        assert_validation(&response, "events", &bulk_count_message);

        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({
                "events": [{
                    "owner_user_id": "u1",
                    "text": "nested oversized UTF-8 tag",
                    "tags": [oversized_tag]
                }]
            }),
        )
        .await;
        assert_validation(&response, "events[0].tags[0]", &tag_bytes_message);
    }

    for uri in [
        "/v1/history/users/u1/search",
        "/v1/history/search",
        "/v1/history/users/u1/timeline",
        "/v1/history/timeline",
    ] {
        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({ "owner_user_id": "u1", "limit": MAX_SEARCH_LIMIT + 1 }),
        )
        .await;
        assert_validation(&response, "limit", &search_limit_message);
    }

    let usage = state
        .store
        .usage_snapshot(state.tenant_id(), None, true)
        .unwrap();
    assert_eq!(usage["providers"]["history_events"]["event_count"], 0);
    assert_eq!(
        usage["providers"]["history_events"]["user_event_index_count"],
        0
    );

    state.shutdown().await;
}

#[tokio::test]
async fn bounded_state_insight_link_context_analysis_preflight_and_eval_requests_are_stable() {
    let (state, app) = bounded_app();
    let oversized_tag = "ééé";
    let tag_bytes_message = format!("must be at most {MAX_TAG_BYTES} UTF-8 bytes");
    let search_limit_message = format!("must be at most {MAX_SEARCH_LIMIT}");

    for uri in [
        "/v1/state/search",
        "/v1/state/insights/search",
        "/v1/links/search",
        "/v1/context/search",
    ] {
        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({ "owner_user_id": "u1", "limit": MAX_SEARCH_LIMIT + 1 }),
        )
        .await;
        assert_validation(&response, "limit", &search_limit_message);
    }

    for (field, body) in [
        (
            "context_limit",
            json!({
                "owner_user_id": "u1",
                "query": "bounded analysis",
                "context_limit": MAX_SEARCH_LIMIT + 1,
                "link_limit": MAX_SEARCH_LIMIT,
                "create_links": false,
                "upsert_insights": false
            }),
        ),
        (
            "link_limit",
            json!({
                "owner_user_id": "u1",
                "query": "bounded analysis",
                "context_limit": MAX_SEARCH_LIMIT,
                "link_limit": MAX_SEARCH_LIMIT + 1,
                "create_links": false,
                "upsert_insights": false
            }),
        ),
    ] {
        let response = json_call(app.clone(), Method::POST, "/v1/analysis/insights", body).await;
        assert_validation(&response, field, &search_limit_message);
    }

    for (uri, body, field) in [
        (
            "/v1/links",
            json!({
                "owner_user_id": "u1",
                "source_uri": "ctx://user/source",
                "target_uri": "ctx://user/target",
                "tags": [oversized_tag]
            }),
            "tags[0]",
        ),
        (
            "/v1/state/company-docs/preflight",
            json!({ "title": "bounded preflight", "tags": [oversized_tag] }),
            "tags[0]",
        ),
        (
            "/v1/eval/cases",
            json!({ "question": "bounded eval", "tags": [oversized_tag] }),
            "tags[0]",
        ),
    ] {
        let response = json_call(app.clone(), Method::POST, uri, body).await;
        assert_validation(&response, field, &tag_bytes_message);
    }

    let usage = state
        .store
        .usage_snapshot(state.tenant_id(), None, true)
        .unwrap();
    assert_eq!(usage["providers"]["history_events"]["event_count"], 0);
    assert_eq!(usage["providers"]["link_graph"]["link_count"], 0);
    assert_eq!(usage["providers"]["rag"]["trace_count"], 0);
    assert!(state.store.list_eval_cases().unwrap().is_empty());

    state.shutdown().await;
}

#[tokio::test]
async fn structured_row_bulk_limit_rejects_before_rows_or_history_are_mutated() {
    let (state, app) = bounded_app();
    let snapshot = json_call(
        app.clone(),
        Method::POST,
        "/v1/history/structured/snapshots",
        json!({
            "dataset_key": "bounded-rows",
            "owner_user_id": "u1",
            "period_key": "2026-W29",
            "period_start": "2026-07-13T00:00:00Z",
            "period_end": "2026-07-19T23:59:59Z",
            "granularity": "weekly",
            "source_ref": { "kind": "test", "id": "validation-characterization" }
        }),
    )
    .await;
    assert_eq!(snapshot.status, StatusCode::OK, "{}", snapshot.body);
    let snapshot_id = snapshot.body["snapshot"]["id"].as_str().unwrap();

    let response = json_call(
        app.clone(),
        Method::POST,
        &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
        json!({ "rows": [{ "id": "one" }, { "id": "two" }] }),
    )
    .await;
    assert_validation(
        &response,
        "rows",
        &format!("must contain at most {MAX_BULK_ITEMS} items"),
    );

    let rows = state
        .store
        .list_rows_async(state.tenant_id(), snapshot_id)
        .await
        .unwrap();
    assert_eq!(rows["rows"], json!([]));
    let stored_snapshot = state
        .store
        .get_snapshot_async(state.tenant_id(), snapshot_id)
        .await
        .unwrap();
    assert_eq!(stored_snapshot.row_count, 0);
    let usage = state
        .store
        .usage_snapshot(state.tenant_id(), None, true)
        .unwrap();
    assert_eq!(usage["providers"]["structured_data"]["snapshot_count"], 1);
    assert_eq!(usage["providers"]["structured_data"]["row_count"], 0);
    assert_eq!(usage["providers"]["history_events"]["event_count"], 1);

    state.shutdown().await;
}

#[tokio::test]
async fn fragment_policies_outside_the_supported_envelope_are_rejected_before_ingest() {
    let (_state, app) = bounded_app();
    let chunk_message = "must be between 1 and 8000";
    let overlap_message = "must be less than chunk_size_chars";
    let min_message = "must be between 1 and chunk_size_chars";
    for (policy, field, message) in [
        (
            json!({ "chunk_size_chars": 0 }),
            "fragment_policy.chunk_size_chars",
            chunk_message,
        ),
        (
            json!({ "chunk_size_chars": 8001 }),
            "fragment_policy.chunk_size_chars",
            chunk_message,
        ),
        (
            json!({ "chunk_size_chars": 400, "overlap_chars": 400 }),
            "fragment_policy.overlap_chars",
            overlap_message,
        ),
        (
            json!({ "overlap_chars": 1200 }),
            "fragment_policy.overlap_chars",
            overlap_message,
        ),
        (
            json!({ "chunk_size_chars": 400, "min_chunk_chars": 401 }),
            "fragment_policy.min_chunk_chars",
            min_message,
        ),
        (
            json!({ "min_chunk_chars": 0 }),
            "fragment_policy.min_chunk_chars",
            min_message,
        ),
    ] {
        for uri in ["/v1/ingest/files:sync", "/v1/ingest/tasks"] {
            let response = json_call(
                app.clone(),
                Method::POST,
                uri,
                json!({
                    "owner_user_id": "u1",
                    "source_id": "bounded-policy",
                    "content": "bounded policy content",
                    "fragment_policy": policy.clone()
                }),
            )
            .await;
            assert_validation(&response, field, message);
        }
    }

    for (uri, body, field) in [
        (
            "/v1/state/profile/facts/bounded-policy",
            json!({
                "owner_user_id": "u1",
                "state_type": "profile",
                "statement": "bounded policy",
                "fragment_policy": { "chunk_size_chars": 8001 }
            }),
            "fragment_policy.chunk_size_chars",
        ),
        (
            "/v1/state/profile/facts/bounded-policy",
            json!({
                "owner_user_id": "u1",
                "state_type": "profile",
                "statement": "bounded policy",
                "document": {
                    "content": "bounded policy document",
                    "fragment_policy": { "chunk_size_chars": 8001 }
                }
            }),
            "document.fragment_policy.chunk_size_chars",
        ),
    ] {
        let response = json_call(app.clone(), Method::PUT, uri, body).await;
        assert_validation(&response, field, chunk_message);
    }

    let boundary = "nowledge-fragment-policy";
    let multipart = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"owner_user_id\"\r\n\r\nu1\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"source_id\"\r\n\r\nbounded-upload\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"fragment_policy.chunk_size_chars\"\r\n\r\n8001\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"note.md\"\r\nContent-Type: text/markdown\r\n\r\n# Note\r\n\
         --{boundary}--\r\n"
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/v1/ingest/uploads:sync")
                .header(
                    CONTENT_TYPE,
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(multipart))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(
        body["error"]["details"]["field"],
        "fragment_policy.chunk_size_chars"
    );

    // Tiny policies remain valid: the fragmenter is linear and caps the
    // fragment count per document, so they cannot amplify a large document.
    let response = json_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": "u1",
            "source_id": "tiny-policy",
            "content": "Xabcdefghij",
            "fragment_policy": {
                "chunk_size_chars": 4,
                "overlap_chars": 0,
                "min_chunk_chars": 4
            }
        }),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.body);
    assert_eq!(
        response.body["fragment_uris"].as_array().map(Vec::len),
        Some(3)
    );

    let response = json_call(
        app,
        Method::GET,
        "/v1/state/profile/facts/bounded-policy?owner_user_id=u1",
        Value::Null,
    )
    .await;
    assert_eq!(response.status, StatusCode::NOT_FOUND, "{}", response.body);
}

#[tokio::test]
async fn search_queries_and_questions_are_capped_before_retrieval() {
    assert_eq!(Config::test().max_query_chars, 8192);
    let mut config = Config::test();
    config.max_query_chars = 32;
    config.rate_limit_requests_per_minute = 10_000;
    let app = build_router(AppState::new(Arc::new(config)));
    let message = "must contain at most 32 characters";
    // Characters, not bytes: 32 two-byte characters fit, 33 do not.
    let at_cap = "é".repeat(32);
    let over_cap = "é".repeat(33);

    for (uri, field) in [
        ("/v1/context/search", "query"),
        ("/v1/rag/answer", "question"),
        ("/v1/rag/stream", "question"),
        ("/v1/rag/stream?format=json", "question"),
        ("/v1/rag/debug", "question"),
        ("/v1/debug/prompt/preview", "question"),
        ("/v1/history/users/u1/search", "query"),
        ("/v1/history/search", "query"),
        ("/v1/state/search", "query"),
        ("/v1/state/insights/search", "query"),
        ("/v1/links/search", "query"),
        ("/v1/analysis/insights", "query"),
        ("/v1/eval/cases", "question"),
    ] {
        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({ "owner_user_id": "u1", field: over_cap.clone() }),
        )
        .await;
        assert_validation(&response, field, message);
    }

    for (uri, field) in [
        ("/v1/context/search", "query"),
        ("/v1/history/search", "query"),
        ("/v1/state/search", "query"),
    ] {
        let response = json_call(
            app.clone(),
            Method::POST,
            uri,
            json!({ "owner_user_id": "u1", field: at_cap.clone() }),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{uri}: {}", response.body);
    }
}
