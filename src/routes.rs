use std::{sync::Arc, time::Duration};

#[cfg(test)]
use axum::http::StatusCode;
use axum::{
    body::{to_bytes, Body},
    extract::{DefaultBodyLimit, MatchedPath, Request, State},
    http::header::{CONTENT_LENGTH, CONTENT_TYPE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, patch, post, put},
    Router,
};
#[cfg(test)]
use serde_json::json;
use serde_json::Value;
use tower_http::{
    compression::CompressionLayer,
    trace::{OnResponse, TraceLayer},
};

pub use crate::app::{AppState, IngestTaskManager};

use crate::{
    config::Config,
    error::ApiError,
    http_boundary,
    request_context::{self, RequestContextState, RequestId},
    route_analysis::analyze_insights,
    route_company_docs::{
        activate_revision, create_revision, delete_company_doc, get_company_doc, list_company_docs,
        list_revisions, preflight_doc,
    },
    route_context::{
        context_reveal, context_search, context_traceback, fs_abstract, fs_ls, fs_overview,
        fs_read, fs_tree,
    },
    route_eval::{
        create_eval_case, create_eval_run, get_eval_case_analysis, get_eval_overview, get_eval_run,
        get_eval_run_report, list_eval_cases,
    },
    route_harness::{
        compare_harness_change, create_harness_change, create_harness_component_revision,
        create_harness_verdict, get_harness_change, get_harness_change_delta,
        get_harness_component, list_harness_changes, list_harness_components,
        rollback_harness_component,
    },
    route_health::{
        bootstrap, debug_meili_search, get_trace, healthz, livez, llm_status, llm_test, readyz,
        usage,
    },
    route_history::{
        append_event_alias, append_events_bulk_alias, append_user_event, append_user_events_bulk,
        ensure_user_event_index, get_event_alias, get_user_event, get_user_event_index,
        list_user_event_indexes, reconcile_operations, reconcile_user_event_indexes,
        search_events_alias, search_operations, search_user_events, timeline_alias, user_timeline,
    },
    route_ingest::{
        create_ingest_task, create_ingest_upload, enforce_sync_ingest_timeout, get_ingest_task,
        get_ingest_task_result, ingest_file_sync, ingest_upload_sync, SyncIngestTimeoutState,
    },
    route_llm::llm_title,
    route_metrics::metrics,
    route_rag::{prompt_preview, rag_answer, rag_debug, rag_stream},
    route_registry::declare_routes,
    route_sessions::{add_session_message, commit_session, create_session},
    route_state::{
        get_state_fact, insight_events, patch_insight, patch_state_fact, search_insights,
        search_links, search_state, upsert_insight, upsert_link, upsert_state_fact,
    },
    route_structured::{
        apply_snapshot, bulk_rows, create_snapshot, current_structured, get_snapshot, list_rows,
        upsert_dataset,
    },
    util::redact_secrets,
};

#[derive(Clone, Copy, Debug, Default)]
struct ExplicitParentOnResponse;

impl<B> OnResponse<B> for ExplicitParentOnResponse {
    fn on_response(
        self,
        response: &axum::http::Response<B>,
        latency: Duration,
        span: &tracing::Span,
    ) {
        let latency = format!("{} ms", latency.as_millis());
        tracing::event!(
            target: "tower_http::trace::on_response",
            parent: span,
            tracing::Level::INFO,
            latency = %latency,
            status = response.status().as_u16(),
            "finished processing request"
        );
    }
}

declare_routes! {
    "/livez" => get(livez, Public);
    "/healthz" => get(healthz, Admin);
    "/readyz" => get(readyz, Public);
    "/v1/usage" => get(usage, User);
    "/v1/admin/bootstrap" => post(bootstrap, Admin);
    "/v1/admin/metrics" => get(metrics, Admin);
    "/v1/admin/harness/components" => get(list_harness_components, Admin);
    "/v1/admin/harness/components/{component_id}" => get(get_harness_component, Admin);
    "/v1/admin/harness/components/{component_id}/revisions" => post(create_harness_component_revision, Admin);
    "/v1/admin/harness/components/{component_id}/rollback" => post(rollback_harness_component, Admin);
    "/v1/admin/harness/evolution/changes" => post(create_harness_change, Admin).get(list_harness_changes, Admin);
    "/v1/admin/harness/evolution/changes/{change_id}" => get(get_harness_change, Admin);
    "/v1/admin/harness/evolution/changes/{change_id}/verdict" => post(create_harness_verdict, Admin);
    "/v1/admin/harness/evolution/changes/{change_id}/compare" => post(compare_harness_change, Admin);
    "/v1/admin/harness/evolution/changes/{change_id}/delta" => get(get_harness_change_delta, Admin);
    "/v1/state/profile/facts/{fact_key}" => put(upsert_state_fact, User).patch(patch_state_fact, User).get(get_state_fact, User);
    "/v1/state/search" => post(search_state, User);
    "/v1/state/structured/datasets/{dataset_key}" => put(upsert_dataset, CompanyWriter);
    "/v1/state/structured/datasets/{dataset_key}/apply-snapshot" => post(apply_snapshot, User);
    "/v1/state/structured/current" => get(current_structured, User);
    "/v1/state/insights" => post(upsert_insight, User);
    "/v1/state/insights/{insight_id}" => patch(patch_insight, User);
    "/v1/state/insights/search" => post(search_insights, User);
    "/v1/links" => post(upsert_link, User);
    "/v1/links/search" => post(search_links, User);
    "/v1/analysis/insights" => post(analyze_insights, User);
    "/v1/state/company-docs" => get(list_company_docs, User);
    "/v1/state/company-docs/{source_id}" => get(get_company_doc, User).delete(delete_company_doc, Admin);
    "/v1/state/company-docs/preflight" => post(preflight_doc, CompanyWriter);
    "/v1/state/company-docs/{source_id}/revisions" => post(create_revision, CompanyWriter);
    "/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate" => post(activate_revision, CompanyWriter);
    "/v1/history/users/{owner_user_id}/event-index" => put(ensure_user_event_index, User).get(get_user_event_index, User);
    "/v1/history/users/{owner_user_id}/events" => post(append_user_event, User);
    "/v1/history/users/{owner_user_id}/events:bulk" => post(append_user_events_bulk, User);
    "/v1/history/users/{owner_user_id}/search" => post(search_user_events, User);
    "/v1/history/users/{owner_user_id}/events/{event_id}" => get(get_user_event, User);
    "/v1/history/users/{owner_user_id}/timeline" => post(user_timeline, User);
    "/v1/admin/history/user-event-indexes" => get(list_user_event_indexes, Admin);
    "/v1/admin/history/user-event-indexes:reconcile" => post(reconcile_user_event_indexes, Admin);
    "/v1/admin/operations/search" => post(search_operations, Admin);
    "/v1/admin/operations:reconcile" => post(reconcile_operations, Admin);
    "/v1/history/events" => post(append_event_alias, User);
    "/v1/history/events:bulk" => post(append_events_bulk_alias, User);
    "/v1/history/search" => post(search_events_alias, User);
    "/v1/history/events/{event_id}" => get(get_event_alias, User);
    "/v1/history/timeline" => post(timeline_alias, User);
    "/v1/history/structured/snapshots" => post(create_snapshot, User);
    "/v1/history/structured/snapshots/{snapshot_id}" => get(get_snapshot, User);
    "/v1/history/structured/snapshots/{snapshot_id}/rows:bulk" => post(bulk_rows, User);
    "/v1/history/structured/snapshots/{snapshot_id}/rows" => get(list_rows, User);
    "/v1/history/company-docs/{source_id}/revisions" => get(list_revisions, User);
    "/v1/history/insights/{insight_id}/events" => get(insight_events, User);
    "/v1/fs/ls" => get(fs_ls, User);
    "/v1/fs/tree" => get(fs_tree, User);
    "/v1/fs/read" => get(fs_read, User);
    "/v1/fs/abstract" => get(fs_abstract, User);
    "/v1/fs/overview" => get(fs_overview, User);
    "/v1/context/search" => post(context_search, User);
    "/v1/context/reveal" => post(context_reveal, User);
    "/v1/context/traceback" => post(context_traceback, User);
    "/v1/ingest/tasks" => post(create_ingest_task, User);
    "/v1/ingest/tasks/{task_id}" => get(get_ingest_task, User);
    "/v1/ingest/tasks/{task_id}/result" => get(get_ingest_task_result, User);
    "/v1/ingest/uploads" => post(create_ingest_upload, User);
    "/v1/ingest/uploads:sync" => post(ingest_upload_sync, User);
    "/v1/ingest/files:sync" => post(ingest_file_sync, User);
    "/v1/rag/answer" => post(rag_answer, User);
    "/v1/rag/stream" => post(rag_stream, User);
    "/v1/rag/debug" => post(rag_debug, Admin);
    "/v1/eval/cases" => post(create_eval_case, Admin).get(list_eval_cases, Admin);
    "/v1/eval/runs" => post(create_eval_run, Admin);
    "/v1/eval/runs/{run_id}" => get(get_eval_run, Admin);
    "/v1/eval/runs/{run_id}/report" => get(get_eval_run_report, Admin);
    "/v1/eval/runs/{run_id}/analysis/overview" => get(get_eval_overview, Admin);
    "/v1/eval/runs/{run_id}/analysis/cases/{case_id}" => get(get_eval_case_analysis, Admin);
    "/v1/sessions" => post(create_session, User);
    "/v1/sessions/{session_id}/messages" => post(add_session_message, User);
    "/v1/sessions/{session_id}/commit" => post(commit_session, User);
    "/v1/llm/status" => get(llm_status, User);
    "/v1/llm/test" => post(llm_test, Admin);
    "/v1/llm/title" => post(llm_title, User);
    "/v1/debug/traces/{trace_id}" => get(get_trace, Admin);
    "/v1/debug/meili/search" => post(debug_meili_search, Admin);
    "/v1/debug/prompt/preview" => post(prompt_preview, Admin);
}

pub fn build_router(state: AppState) -> Router {
    registered_router()
        .layer(DefaultBodyLimit::max(
            state
                .config
                .max_multipart_body_bytes()
                .expect("validated multipart body limit"),
        ))
        .layer(middleware::from_fn_with_state(
            state.http_boundary.clone(),
            http_boundary::enforce_non_multipart_body,
        ))
        .layer(middleware::from_fn_with_state(
            SyncIngestTimeoutState::new(&state),
            enforce_sync_ingest_timeout,
        ))
        .layer(middleware::from_fn_with_state(
            state.http_boundary.clone(),
            http_boundary::enforce_timeout,
        ))
        .layer(middleware::from_fn_with_state(
            state.config.clone(),
            redact_json_response,
        ))
        .layer(CompressionLayer::new())
        .layer(middleware::from_fn_with_state(
            state.http_boundary.clone(),
            http_boundary::load_shed,
        ))
        .layer(
            http_boundary::build_cors_layer(&state.config).expect("validated CORS configuration"),
        )
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &axum::http::Request<axum::body::Body>| {
                    let request_id = request
                        .extensions()
                        .get::<RequestId>()
                        .map(RequestId::as_str)
                        .unwrap_or("missing");
                    let route = request
                        .extensions()
                        .get::<MatchedPath>()
                        .map(MatchedPath::as_str)
                        .unwrap_or("unmatched");
                    tracing::info_span!(
                        "http_request",
                        %request_id,
                        method = %request.method(),
                        route
                    )
                })
                .on_response(ExplicitParentOnResponse),
        )
        .layer(middleware::from_fn_with_state(
            state.metrics.clone(),
            http_boundary::record_metrics,
        ))
        .layer(middleware::from_fn_with_state(
            RequestContextState::from_shared_config(state.config.clone()),
            request_context::assign,
        ))
        .with_state(state)
}

async fn redact_json_response(
    State(config): State<Arc<Config>>,
    request: Request,
    next: Next,
) -> Response {
    let response = next.run(request).await;
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .is_some_and(|media_type| {
            media_type.eq_ignore_ascii_case("application/json")
                || media_type.to_ascii_lowercase().ends_with("+json")
        });
    if !is_json {
        return response;
    }

    sanitize_json_response(response, &config, MAX_REDACTABLE_JSON_RESPONSE_BYTES).await
}

const MAX_REDACTABLE_JSON_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

async fn sanitize_json_response(response: Response, config: &Config, limit: usize) -> Response {
    let (mut parts, body) = response.into_parts();
    let bytes = match to_bytes(body, limit).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return ApiError::Internal("JSON response exceeded the redaction limit".to_string())
                .into_response();
        }
    };
    let value = match serde_json::from_slice::<Value>(&bytes) {
        Ok(value) => value,
        Err(_) => {
            return ApiError::Internal("failed to parse JSON response for redaction".to_string())
                .into_response();
        }
    };
    let sanitized = redact_secrets(&value, &config.configured_secret_values());
    let bytes = match serde_json::to_vec(&sanitized) {
        Ok(bytes) => bytes,
        Err(_) => {
            return ApiError::Internal("failed to sanitize JSON response".to_string())
                .into_response();
        }
    };
    parts.headers.remove(CONTENT_LENGTH);
    Response::from_parts(parts, Body::from(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn json_response_redaction_fails_closed_for_oversized_or_malformed_bodies() {
        let config = Config::test();
        let oversized = (
            [(CONTENT_TYPE, "application/json")],
            json!({ "body": "x".repeat(128) }).to_string(),
        )
            .into_response();
        let oversized = sanitize_json_response(oversized, &config, 64).await;
        assert_eq!(oversized.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let oversized_body = to_bytes(oversized.into_body(), 1_024).await.unwrap();
        let oversized_body: Value = serde_json::from_slice(&oversized_body).unwrap();
        assert_eq!(oversized_body["error"]["code"], "internal_error");
        assert_eq!(oversized_body["error"]["message"], "internal server error");

        let malformed = ([(CONTENT_TYPE, "application/json")], "{not-json").into_response();
        let malformed = sanitize_json_response(malformed, &config, 1_024).await;
        assert_eq!(malformed.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let malformed_body = to_bytes(malformed.into_body(), 1_024).await.unwrap();
        assert!(!String::from_utf8_lossy(&malformed_body).contains("not-json"));
    }
}
