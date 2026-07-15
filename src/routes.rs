use std::{sync::Arc, time::Duration};

#[cfg(test)]
use axum::http::StatusCode;
use axum::{
    body::{to_bytes, Body},
    extract::{DefaultBodyLimit, MatchedPath, Path, Query, Request, State},
    http::header::{CONTENT_LENGTH, CONTENT_TYPE},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, patch, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::{
    compression::CompressionLayer,
    trace::{OnResponse, TraceLayer},
};

pub use crate::app::{AppState, IngestTaskManager};

use crate::{
    auth::{AdminGuard, CompanyWriterGuard, UserGuard},
    config::Config,
    error::ApiError,
    health_service::llm_health_false_ready,
    http_boundary,
    models::*,
    request_context::{self, RequestContextState, RequestId},
    request_validation::{
        validate_history_bulk, validate_max_items, validate_search_limit, validate_tags,
    },
    route_analysis::analyze_insights,
    route_health::{
        bootstrap, debug_meili_search, get_trace, healthz, livez, llm_status, llm_test, readyz,
        usage,
    },
    route_ingest::{
        create_ingest_task, create_ingest_upload, enforce_sync_ingest_timeout, get_ingest_task,
        get_ingest_task_result, ingest_file_sync, ingest_upload_sync, SyncIngestTimeoutState,
    },
    route_llm::llm_title,
    route_rag::{prompt_preview, rag_answer, rag_debug, rag_stream},
    route_registry::declare_routes,
    shared_audit::audit_shared_write,
    util::{redact_secrets, require_string},
};

#[derive(Debug, Deserialize)]
struct OwnerQuery {
    owner_user_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FsQuery {
    uri: Option<String>,
    depth: Option<usize>,
    owner_user_id: Option<String>,
}

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

async fn list_harness_components(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<Vec<HarnessComponent>>, ApiError> {
    Ok(Json(state.store.list_harness_components()?))
}

async fn get_harness_component(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(component_id): Path<String>,
) -> Result<Json<HarnessComponentDetail>, ApiError> {
    Ok(Json(state.store.harness_component_detail(&component_id)?))
}

async fn create_harness_component_revision(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(component_id): Path<String>,
    Json(req): Json<CreateHarnessComponentRevisionRequest>,
) -> Result<Json<HarnessComponentRevision>, ApiError> {
    Ok(Json(
        state
            .store
            .create_harness_component_revision_async(state.tenant_id(), &component_id, req)
            .await?,
    ))
}

async fn rollback_harness_component(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(component_id): Path<String>,
    Json(req): Json<RollbackHarnessComponentRequest>,
) -> Result<Json<HarnessRollbackResponse>, ApiError> {
    Ok(Json(
        state
            .store
            .rollback_harness_component_async(state.tenant_id(), &component_id, req)
            .await?,
    ))
}

async fn create_harness_change(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<CreateHarnessChangeManifestRequest>,
) -> Result<Json<HarnessChangeManifest>, ApiError> {
    Ok(Json(
        state
            .store
            .create_harness_change_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn list_harness_changes(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<Vec<HarnessChangeManifest>>, ApiError> {
    Ok(Json(state.store.list_harness_changes()?))
}

async fn get_harness_change(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
) -> Result<Json<HarnessChangeManifest>, ApiError> {
    Ok(Json(state.store.harness_change(&change_id)?))
}

async fn create_harness_verdict(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
    Json(req): Json<CreateHarnessChangeVerdictRequest>,
) -> Result<Json<HarnessChangeVerdict>, ApiError> {
    Ok(Json(
        state
            .store
            .create_harness_verdict_async(state.tenant_id(), &change_id, req)
            .await?,
    ))
}

async fn compare_harness_change(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
    Json(req): Json<Value>,
) -> Result<Json<EvalDeltaReport>, ApiError> {
    let baseline = req
        .get("baseline_eval_run_id")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let candidate = req
        .get("candidate_eval_run_id")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    Ok(Json(
        state
            .store
            .compare_harness_change(&change_id, baseline, candidate)?,
    ))
}

async fn get_harness_change_delta(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
) -> Result<Json<EvalDeltaReport>, ApiError> {
    Ok(Json(
        state.store.compare_harness_change(&change_id, None, None)?,
    ))
}

async fn ensure_user_event_index(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<EnsureUserEventIndexRequest>,
) -> Result<Json<UserEventIndexResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    Ok(Json(
        state
            .store
            .ensure_user_index_async(state.tenant_id(), &owner_user_id, req)
            .await?,
    ))
}

async fn get_user_event_index(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
) -> Result<Json<UserEventIndexResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    Ok(Json(
        state
            .store
            .ensure_user_index_async(
                state.tenant_id(),
                &owner_user_id,
                EnsureUserEventIndexRequest::default(),
            )
            .await?,
    ))
}

async fn list_user_event_indexes(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<ListUserEventIndexesResponse>, ApiError> {
    Ok(Json(state.store.list_user_indexes(state.tenant_id())?))
}

async fn reconcile_user_event_indexes(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<ReconcileUserEventIndexesRequest>,
) -> Result<Json<ReconcileUserEventIndexesResponse>, ApiError> {
    Ok(Json(
        state
            .store
            .reconcile_user_indexes_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn search_operations(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<OperationListRequest>,
) -> Result<Json<OperationListResponse>, ApiError> {
    Ok(Json(
        state.store.list_operations(state.tenant_id(), req).await?,
    ))
}

async fn reconcile_operations(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<ReconcileOperationsRequest>,
) -> Result<Json<ReconcileOperationsResponse>, ApiError> {
    Ok(Json(
        state
            .store
            .reconcile_operations_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn append_user_event(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_tags("tags", &req.tags, &state.config)?;
    Ok(Json(
        state
            .store
            .append_event_async(state.tenant_id(), Some(&owner_user_id), req)
            .await?,
    ))
}

async fn append_user_events_bulk(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<BulkHistoryEventsRequest>,
) -> Result<Json<BulkHistoryEventsResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_history_bulk(&req, &state.config)?;
    Ok(Json(
        state
            .store
            .append_bulk_events_async(state.tenant_id(), Some(&owner_user_id), req)
            .await?,
    ))
}

async fn search_user_events(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(mut req): Json<HistorySearchRequest>,
) -> Result<Json<HistorySearchResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_search_limit("limit", req.limit, &state.config)?;
    req.owner_user_id = Some(owner_user_id.clone());
    Ok(Json(
        state
            .store
            .search_events_async(state.tenant_id(), Some(&owner_user_id), req)
            .await?,
    ))
}

async fn get_user_event(
    user: UserGuard,
    State(state): State<AppState>,
    Path((owner_user_id, event_id)): Path<(String, String)>,
) -> Result<Json<HistoryEvent>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    Ok(Json(
        state
            .store
            .get_event_async(state.tenant_id(), &owner_user_id, &event_id)
            .await?,
    ))
}

async fn user_timeline(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<TimelineQueryRequest>,
) -> Result<Json<TimelineResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_search_limit("limit", req.limit, &state.config)?;
    Ok(Json(
        state
            .store
            .timeline_async(state.tenant_id(), Some(&owner_user_id), req)
            .await?,
    ))
}

async fn append_event_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    validate_tags("tags", &req.tags, &state.config)?;
    Ok(Json(
        state
            .store
            .append_event_async(state.tenant_id(), None, req)
            .await?,
    ))
}

async fn append_events_bulk_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<BulkHistoryEventsRequest>,
) -> Result<Json<BulkHistoryEventsResponse>, ApiError> {
    if let Some(first) = req.events.first_mut() {
        user.apply_owner_default(&mut first.owner_user_id)?;
    }
    if let Some(owner) = req
        .events
        .first()
        .and_then(|event| event.owner_user_id.clone())
    {
        user.require_owner_access(&owner)?;
    }
    validate_history_bulk(&req, &state.config)?;
    Ok(Json(
        state
            .store
            .append_bulk_events_async(state.tenant_id(), None, req)
            .await?,
    ))
}

async fn search_events_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<HistorySearchRequest>,
) -> Result<Json<HistorySearchResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    validate_search_limit("limit", req.limit, &state.config)?;
    Ok(Json(
        state
            .store
            .search_events_async(state.tenant_id(), None, req)
            .await?,
    ))
}

async fn get_event_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Path(event_id): Path<String>,
    Query(query): Query<OwnerQuery>,
) -> Result<Json<HistoryEvent>, ApiError> {
    let owner = require_string(query.owner_user_id, "owner_user_id")?;
    user.require_owner_access(&owner)?;
    Ok(Json(
        state
            .store
            .get_event_async(state.tenant_id(), &owner, &event_id)
            .await?,
    ))
}

async fn timeline_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<TimelineQueryRequest>,
) -> Result<Json<TimelineResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    validate_search_limit("limit", req.limit, &state.config)?;
    Ok(Json(
        state
            .store
            .timeline_async(state.tenant_id(), None, req)
            .await?,
    ))
}

async fn upsert_state_fact(
    user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Json(mut req): Json<UpsertStateFactRequest>,
) -> Result<Json<StateItemResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    Ok(Json(
        state
            .store
            .upsert_state_fact_async(state.tenant_id(), &fact_key, req)
            .await?,
    ))
}

async fn patch_state_fact(
    user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Json(mut req): Json<PatchStateFactRequest>,
) -> Result<Json<StateItemResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    Ok(Json(
        state
            .store
            .patch_state_fact_async(state.tenant_id(), &fact_key, req)
            .await?,
    ))
}

async fn get_state_fact(
    user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<StateItemResponse>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    require_explicit_owner_for_unbound_private_read(&user, query.owner_user_id.as_deref())?;
    Ok(Json(state.store.get_state_fact(
        state.tenant_id(),
        &fact_key,
        query.owner_user_id.as_deref(),
    )?))
}

async fn search_state(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<StateSearchRequest>,
) -> Result<Json<StateSearchResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_explicit_owner_for_unbound_private_read(&user, req.owner_user_id.as_deref())?;
    validate_search_limit("limit", req.limit, &state.config)?;
    Ok(Json(state.store.search_state(state.tenant_id(), req)?))
}

async fn upsert_insight(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<InsightUpsertRequest>,
) -> Result<Json<InsightResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    Ok(Json(
        state
            .store
            .upsert_insight_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn patch_insight(
    user: UserGuard,
    State(state): State<AppState>,
    Path(insight_id): Path<String>,
    Json(req): Json<InsightPatchRequest>,
) -> Result<Json<InsightResponse>, ApiError> {
    let owner = state.store.insight_owner(&insight_id)?;
    user.require_owner_access(&owner)?;
    Ok(Json(
        state
            .store
            .patch_insight_async(state.tenant_id(), &insight_id, req)
            .await?,
    ))
}

async fn search_insights(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<InsightSearchRequest>,
) -> Result<Json<InsightSearchResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_explicit_owner_for_unbound_private_read(&user, req.owner_user_id.as_deref())?;
    validate_search_limit("limit", req.limit, &state.config)?;
    Ok(Json(state.store.search_insights(req)?))
}

async fn upsert_link(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<LinkUpsertRequest>,
) -> Result<Json<LinkResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    validate_tags("tags", &req.tags, &state.config)?;
    Ok(Json(
        state
            .store
            .upsert_link_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn search_links(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<LinkSearchRequest>,
) -> Result<Json<LinkSearchResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    validate_search_limit("limit", req.limit, &state.config)?;
    Ok(Json(state.store.search_links(
        state.tenant_id(),
        req,
        user.principal.is_admin(),
    )?))
}

async fn preflight_doc(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Json(req): Json<CompanyDocPreflightRequest>,
) -> Result<Json<CompanyDocPreflightResponse>, ApiError> {
    validate_tags("tags", &req.tags, &state.config)?;
    let result = state.store.preflight_company_doc(req);
    Ok(Json(audit_shared_write(
        result,
        &user.principal,
        &state,
        "company_doc.preflight",
        "company-doc:preflight",
        "preflight_requested",
    )?))
}

async fn create_revision(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Json(req): Json<CreateRevisionRequest>,
) -> Result<Json<CreateRevisionResponse>, ApiError> {
    let result = state
        .store
        .create_revision_async(state.tenant_id(), &source_id, req)
        .await;
    Ok(Json(audit_shared_write(
        result,
        &user.principal,
        &state,
        "company_doc.create_revision",
        &source_id,
        "revision_create_requested",
    )?))
}

async fn activate_revision(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Path((source_id, revision_id)): Path<(String, String)>,
    Json(req): Json<ActivateRevisionRequest>,
) -> Result<Json<ActivateRevisionResponse>, ApiError> {
    let audit_reason = req
        .reason
        .as_deref()
        .unwrap_or("activation_reason_unspecified")
        .to_string();
    let result = state
        .store
        .activate_revision_async(state.tenant_id(), &source_id, &revision_id, req)
        .await;
    Ok(Json(audit_shared_write(
        result,
        &user.principal,
        &state,
        "company_doc.activate_revision",
        &format!("{source_id}:{revision_id}"),
        &audit_reason,
    )?))
}

async fn list_company_docs(
    _user: UserGuard,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.store.list_company_docs()?))
}

async fn get_company_doc(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.store.get_company_doc(&source_id)?))
}

async fn delete_company_doc(
    admin: AdminGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let result = state
        .store
        .delete_company_doc(state.tenant_id(), &source_id)
        .await;
    Ok(Json(audit_shared_write(
        result,
        &admin.principal,
        &state,
        "company_doc.delete",
        &source_id,
        "admin_delete",
    )?))
}

async fn list_revisions(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.store.list_revisions(&source_id)?))
}

async fn upsert_dataset(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Path(dataset_key): Path<String>,
    Json(req): Json<DatasetSchemaUpsertRequest>,
) -> Result<Json<DatasetSchemaResponse>, ApiError> {
    let result = state
        .store
        .upsert_dataset_async(state.tenant_id(), &dataset_key, req)
        .await;
    Ok(Json(audit_shared_write(
        result,
        &user.principal,
        &state,
        "dataset.upsert_schema",
        &dataset_key,
        "schema_upsert",
    )?))
}

async fn apply_snapshot(
    user: UserGuard,
    State(state): State<AppState>,
    Path(dataset_key): Path<String>,
    Json(req): Json<ApplySnapshotRequest>,
) -> Result<Json<ApplySnapshotResponse>, ApiError> {
    if let Some(snapshot_id) = req.snapshot_id.as_deref() {
        let owner = state
            .store
            .snapshot_owner_async(state.tenant_id(), snapshot_id)
            .await?;
        user.require_owner_access(&owner)?;
    }
    Ok(Json(
        state
            .store
            .apply_snapshot_async(state.tenant_id(), &dataset_key, req)
            .await?,
    ))
}

async fn current_structured(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<CurrentStructuredStateResponse>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    Ok(Json(state.store.current_structured_state(
        state.tenant_id(),
        query.owner_user_id.as_deref(),
        user.principal.is_admin(),
    )?))
}

async fn create_snapshot(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<CreateStructuredSnapshotRequest>,
) -> Result<Json<StructuredSnapshotResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    Ok(Json(
        state
            .store
            .create_snapshot_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn get_snapshot(
    user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<StructuredSnapshot>, ApiError> {
    let owner = state
        .store
        .snapshot_owner_async(state.tenant_id(), &snapshot_id)
        .await?;
    user.require_owner_access(&owner)?;
    Ok(Json(
        state
            .store
            .get_snapshot_async(state.tenant_id(), &snapshot_id)
            .await?,
    ))
}

async fn bulk_rows(
    user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
    Json(req): Json<BulkStructuredRowsRequest>,
) -> Result<Json<BulkStructuredRowsResponse>, ApiError> {
    let owner = state
        .store
        .snapshot_owner_async(state.tenant_id(), &snapshot_id)
        .await?;
    user.require_owner_access(&owner)?;
    validate_max_items("rows", req.rows.len(), state.config.max_bulk_rows)?;
    Ok(Json(
        state
            .store
            .bulk_rows_async(state.tenant_id(), &snapshot_id, req)
            .await?,
    ))
}

async fn list_rows(
    user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let owner = state
        .store
        .snapshot_owner_async(state.tenant_id(), &snapshot_id)
        .await?;
    user.require_owner_access(&owner)?;
    Ok(Json(
        state
            .store
            .list_rows_async(state.tenant_id(), &snapshot_id)
            .await?,
    ))
}

async fn insight_events(_user: UserGuard, Path(insight_id): Path<String>) -> Json<Value> {
    Json(json!({ "insight_id": insight_id, "events": [] }))
}

async fn fs_ls(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    Ok(Json(
        state
            .store
            .fs_ls_async(
                state.tenant_id(),
                query.uri.as_deref(),
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

async fn fs_tree(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    Ok(Json(
        state
            .store
            .fs_tree_async(
                state.tenant_id(),
                query.uri.as_deref(),
                query.depth,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

async fn fs_read(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(
        state
            .store
            .fs_read_async(
                state.tenant_id(),
                &uri,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

async fn fs_abstract(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(
        state
            .store
            .fs_layer_async(
                state.tenant_id(),
                &uri,
                0,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

async fn fs_overview(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(
        state
            .store
            .fs_layer_async(
                state.tenant_id(),
                &uri,
                1,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

async fn context_search(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<ContextSearchRequest>,
) -> Result<Json<ContextSearchResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    validate_search_limit("limit", req.limit, &state.config)?;
    Ok(Json(
        state
            .store
            .search_context_async(state.tenant_id(), req, user.principal.is_admin())
            .await?
            .response,
    ))
}

async fn context_reveal(
    user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<ContextRevealRequest>,
) -> Result<Json<ContextRevealResponse>, ApiError> {
    let owner = if let Some(trace_id) = req.trace_id.as_deref() {
        state
            .store
            .trace_owner_id_async(state.tenant_id(), trace_id)
            .await?
    } else {
        None
    };
    if let Some(owner) = &owner {
        user.require_owner_access(owner)?;
    }
    let owner_scope = owner.or_else(|| user.principal.owner_user_id().map(ToString::to_string));
    Ok(Json(
        state
            .store
            .reveal_context_async(
                state.tenant_id(),
                req,
                owner_scope.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

async fn context_traceback(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<ContextTracebackRequest>,
) -> Result<Json<ContextTracebackResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    Ok(Json(
        state
            .store
            .traceback_async(state.tenant_id(), req, user.principal.is_admin())
            .await?,
    ))
}

async fn create_eval_case(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<CreateRagEvalCaseRequest>,
) -> Result<Json<RagEvalCase>, ApiError> {
    validate_tags("tags", &req.tags, &state.config)?;
    Ok(Json(
        state
            .store
            .create_eval_case_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn list_eval_cases(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<Vec<RagEvalCase>>, ApiError> {
    Ok(Json(state.store.list_eval_cases()?))
}

async fn create_eval_run(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<CreateRagEvalRunRequest>,
) -> Result<Json<RagEvalRun>, ApiError> {
    let llm_false_ready = llm_health_false_ready(&state).await;
    Ok(Json(
        state
            .store
            .create_eval_run_async(state.tenant_id(), req, llm_false_ready)
            .await?,
    ))
}

async fn get_eval_run(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<RagEvalRun>, ApiError> {
    Ok(Json(state.store.get_eval_run(&run_id)?))
}

async fn get_eval_run_report(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.store.eval_run_report(&run_id)?))
}

async fn get_eval_overview(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<RagEvalOverview>, ApiError> {
    Ok(Json(state.store.eval_overview(&run_id)?))
}

async fn get_eval_case_analysis(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path((run_id, case_id)): Path<(String, String)>,
) -> Result<Json<RagEvalCaseResult>, ApiError> {
    Ok(Json(state.store.eval_case_result(&run_id, &case_id)?))
}

async fn create_session(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<SessionCreateRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    Ok(Json(
        state
            .store
            .create_session_async(state.tenant_id(), req)
            .await?,
    ))
}

async fn add_session_message(
    user: UserGuard,
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(req): Json<SessionMessageRequest>,
) -> Result<Json<Value>, ApiError> {
    let owner = state.store.session_owner_id(&session_id)?;
    user.require_owner_access(&owner)?;
    Ok(Json(
        state
            .store
            .add_session_message_async(state.tenant_id(), &session_id, req)
            .await?,
    ))
}

async fn commit_session(
    user: UserGuard,
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(req): Json<SessionCommitRequest>,
) -> Result<Json<SessionCommitResponse>, ApiError> {
    let owner = state.store.session_owner_id(&session_id)?;
    user.require_owner_access(&owner)?;
    Ok(Json(
        state
            .store
            .commit_session_async(state.tenant_id(), &session_id, req)
            .await?,
    ))
}

fn require_owner_for_write(user: &UserGuard, owner_user_id: Option<&str>) -> Result<(), ApiError> {
    if user.principal.is_admin() || owner_user_id.is_some() {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "owner_user_id is required for non-admin writes",
        ))
    }
}

fn require_explicit_owner_for_unbound_private_read(
    user: &UserGuard,
    owner_user_id: Option<&str>,
) -> Result<(), ApiError> {
    let is_tenant_service = !user.principal.is_admin() && user.principal.owner_user_id().is_none();
    if is_tenant_service && owner_user_id.is_none() {
        Err(ApiError::forbidden(
            "owner_user_id is required for tenant-service private access",
        ))
    } else {
        Ok(())
    }
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
