use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    routing::{get, patch, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use tower_http::{compression::CompressionLayer, cors::CorsLayer, trace::TraceLayer};

use crate::{
    auth::{AdminGuard, UserGuard},
    config::Config,
    error::ApiError,
    meili::MeiliAdmin,
    models::*,
    store::Store,
    util::{redact_secrets, require_string},
};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Store,
    pub meili: MeiliAdmin,
}

impl AppState {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            store: Store::new(&config),
            meili: MeiliAdmin::from_config(&config),
            config,
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.config.tenant_id
    }
}

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

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/admin/bootstrap", post(bootstrap))
        .route(
            "/v1/state/profile/facts/{fact_key}",
            put(upsert_state_fact)
                .patch(patch_state_fact)
                .get(get_state_fact),
        )
        .route("/v1/state/search", post(search_state))
        .route(
            "/v1/state/structured/datasets/{dataset_key}",
            put(upsert_dataset),
        )
        .route(
            "/v1/state/structured/datasets/{dataset_key}/apply-snapshot",
            post(apply_snapshot),
        )
        .route("/v1/state/structured/current", get(current_structured))
        .route("/v1/state/insights", post(upsert_insight))
        .route("/v1/state/insights/{insight_id}", patch(patch_insight))
        .route("/v1/state/insights/search", post(search_insights))
        .route("/v1/state/company-docs/preflight", post(preflight_doc))
        .route(
            "/v1/state/company-docs/{source_id}/revisions",
            post(create_revision),
        )
        .route(
            "/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate",
            post(activate_revision),
        )
        .route(
            "/v1/history/users/{owner_user_id}/event-index",
            put(ensure_user_event_index).get(get_user_event_index),
        )
        .route(
            "/v1/history/users/{owner_user_id}/events",
            post(append_user_event),
        )
        .route(
            "/v1/history/users/{owner_user_id}/events:bulk",
            post(append_user_events_bulk),
        )
        .route(
            "/v1/history/users/{owner_user_id}/search",
            post(search_user_events),
        )
        .route(
            "/v1/history/users/{owner_user_id}/events/{event_id}",
            get(get_user_event),
        )
        .route(
            "/v1/history/users/{owner_user_id}/timeline",
            post(user_timeline),
        )
        .route(
            "/v1/admin/history/user-event-indexes",
            get(list_user_event_indexes),
        )
        .route(
            "/v1/admin/history/user-event-indexes:reconcile",
            post(reconcile_user_event_indexes),
        )
        .route("/v1/history/events", post(append_event_alias))
        .route("/v1/history/events:bulk", post(append_events_bulk_alias))
        .route("/v1/history/search", post(search_events_alias))
        .route("/v1/history/events/{event_id}", get(get_event_alias))
        .route("/v1/history/timeline", post(timeline_alias))
        .route("/v1/history/structured/snapshots", post(create_snapshot))
        .route(
            "/v1/history/structured/snapshots/{snapshot_id}",
            get(get_snapshot),
        )
        .route(
            "/v1/history/structured/snapshots/{snapshot_id}/rows:bulk",
            post(bulk_rows),
        )
        .route(
            "/v1/history/structured/snapshots/{snapshot_id}/rows",
            get(list_rows),
        )
        .route(
            "/v1/history/company-docs/{source_id}/revisions",
            get(list_revisions),
        )
        .route(
            "/v1/history/insights/{insight_id}/events",
            get(insight_events),
        )
        .route("/v1/fs/ls", get(fs_ls))
        .route("/v1/fs/tree", get(fs_tree))
        .route("/v1/fs/read", get(fs_read))
        .route("/v1/fs/abstract", get(fs_abstract))
        .route("/v1/fs/overview", get(fs_overview))
        .route("/v1/context/search", post(context_search))
        .route("/v1/context/reveal", post(context_reveal))
        .route("/v1/rag/answer", post(rag_answer))
        .route("/v1/rag/stream", post(rag_stream))
        .route("/v1/rag/debug", post(rag_debug))
        .route("/v1/sessions", post(create_session))
        .route(
            "/v1/sessions/{session_id}/messages",
            post(add_session_message),
        )
        .route("/v1/sessions/{session_id}/commit", post(commit_session))
        .route("/v1/llm/status", get(llm_status))
        .route("/v1/llm/auth/import-codex", post(import_codex_auth))
        .route("/v1/llm/test", post(llm_test))
        .route("/v1/debug/traces/{trace_id}", get(get_trace))
        .route("/v1/debug/meili/search", post(debug_meili_search))
        .route("/v1/debug/prompt/preview", post(prompt_preview))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn healthz() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

async fn readyz(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "status": "ready",
        "meili": if state.config.meili_url.is_some() { "configured" } else { "memory" },
        "llm": state.config.llm_provider
    }))
}

async fn bootstrap(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let reset = req.get("reset").and_then(Value::as_bool).unwrap_or(false);
    let result = state.meili.bootstrap(reset).await?;
    Ok(Json(json!({
        "indexes": result.indexes,
        "tasks": result.tasks,
        "dry_run": result.dry_run
    })))
}

async fn ensure_user_event_index(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<EnsureUserEventIndexRequest>,
) -> Result<Json<UserEventIndexResponse>, ApiError> {
    Ok(Json(state.store.ensure_user_index(
        state.tenant_id(),
        &owner_user_id,
        req,
    )?))
}

async fn get_user_event_index(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
) -> Result<Json<UserEventIndexResponse>, ApiError> {
    Ok(Json(
        state
            .store
            .get_user_index(state.tenant_id(), &owner_user_id)?,
    ))
}

async fn list_user_event_indexes(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<ListUserEventIndexesResponse>, ApiError> {
    Ok(Json(state.store.list_user_indexes()?))
}

async fn reconcile_user_event_indexes(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<ReconcileUserEventIndexesRequest>,
) -> Result<Json<ReconcileUserEventIndexesResponse>, ApiError> {
    Ok(Json(
        state.store.reconcile_user_indexes(state.tenant_id(), req)?,
    ))
}

async fn append_user_event(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    Ok(Json(state.store.append_event(
        state.tenant_id(),
        Some(&owner_user_id),
        req,
    )?))
}

async fn append_user_events_bulk(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<BulkHistoryEventsRequest>,
) -> Result<Json<BulkHistoryEventsResponse>, ApiError> {
    Ok(Json(state.store.append_bulk_events(
        state.tenant_id(),
        Some(&owner_user_id),
        req,
    )?))
}

async fn search_user_events(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(mut req): Json<HistorySearchRequest>,
) -> Result<Json<HistorySearchResponse>, ApiError> {
    req.owner_user_id = Some(owner_user_id.clone());
    Ok(Json(state.store.search_events(
        state.tenant_id(),
        Some(&owner_user_id),
        req,
    )?))
}

async fn get_user_event(
    _user: UserGuard,
    State(state): State<AppState>,
    Path((owner_user_id, event_id)): Path<(String, String)>,
) -> Result<Json<HistoryEvent>, ApiError> {
    Ok(Json(state.store.get_event(
        state.tenant_id(),
        &owner_user_id,
        &event_id,
    )?))
}

async fn user_timeline(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<TimelineQueryRequest>,
) -> Result<Json<TimelineResponse>, ApiError> {
    Ok(Json(state.store.timeline(
        state.tenant_id(),
        Some(&owner_user_id),
        req,
    )?))
}

async fn append_event_alias(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    Ok(Json(state.store.append_event(
        state.tenant_id(),
        None,
        req,
    )?))
}

async fn append_events_bulk_alias(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<BulkHistoryEventsRequest>,
) -> Result<Json<BulkHistoryEventsResponse>, ApiError> {
    Ok(Json(state.store.append_bulk_events(
        state.tenant_id(),
        None,
        req,
    )?))
}

async fn search_events_alias(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<HistorySearchRequest>,
) -> Result<Json<HistorySearchResponse>, ApiError> {
    Ok(Json(state.store.search_events(
        state.tenant_id(),
        None,
        req,
    )?))
}

async fn get_event_alias(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(event_id): Path<String>,
    Query(query): Query<OwnerQuery>,
) -> Result<Json<HistoryEvent>, ApiError> {
    let owner = require_string(query.owner_user_id, "owner_user_id")?;
    Ok(Json(state.store.get_event(
        state.tenant_id(),
        &owner,
        &event_id,
    )?))
}

async fn timeline_alias(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<TimelineQueryRequest>,
) -> Result<Json<TimelineResponse>, ApiError> {
    Ok(Json(state.store.timeline(state.tenant_id(), None, req)?))
}

async fn upsert_state_fact(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Json(req): Json<UpsertStateFactRequest>,
) -> Result<Json<StateItemResponse>, ApiError> {
    Ok(Json(state.store.upsert_state_fact(
        state.tenant_id(),
        &fact_key,
        req,
    )?))
}

async fn patch_state_fact(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Json(req): Json<PatchStateFactRequest>,
) -> Result<Json<StateItemResponse>, ApiError> {
    Ok(Json(state.store.patch_state_fact(
        state.tenant_id(),
        &fact_key,
        req,
    )?))
}

async fn get_state_fact(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Query(query): Query<OwnerQuery>,
) -> Result<Json<StateItemResponse>, ApiError> {
    Ok(Json(state.store.get_state_fact(
        state.tenant_id(),
        &fact_key,
        query.owner_user_id.as_deref(),
    )?))
}

async fn search_state(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<StateSearchRequest>,
) -> Result<Json<StateSearchResponse>, ApiError> {
    Ok(Json(state.store.search_state(state.tenant_id(), req)?))
}

async fn upsert_insight(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<InsightUpsertRequest>,
) -> Result<Json<InsightResponse>, ApiError> {
    Ok(Json(state.store.upsert_insight(state.tenant_id(), req)?))
}

async fn patch_insight(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(insight_id): Path<String>,
    Json(req): Json<InsightPatchRequest>,
) -> Result<Json<InsightResponse>, ApiError> {
    Ok(Json(state.store.patch_insight(
        state.tenant_id(),
        &insight_id,
        req,
    )?))
}

async fn search_insights(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<InsightSearchRequest>,
) -> Result<Json<InsightSearchResponse>, ApiError> {
    Ok(Json(state.store.search_insights(req)?))
}

async fn preflight_doc(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<CompanyDocPreflightRequest>,
) -> Result<Json<CompanyDocPreflightResponse>, ApiError> {
    Ok(Json(state.store.preflight_company_doc(req)?))
}

async fn create_revision(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Json(req): Json<CreateRevisionRequest>,
) -> Result<Json<CreateRevisionResponse>, ApiError> {
    Ok(Json(state.store.create_revision(
        state.tenant_id(),
        &source_id,
        req,
    )?))
}

async fn activate_revision(
    _user: UserGuard,
    State(state): State<AppState>,
    Path((source_id, revision_id)): Path<(String, String)>,
    Json(req): Json<ActivateRevisionRequest>,
) -> Result<Json<ActivateRevisionResponse>, ApiError> {
    Ok(Json(state.store.activate_revision(
        &source_id,
        &revision_id,
        req,
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
    _user: UserGuard,
    State(state): State<AppState>,
    Path(dataset_key): Path<String>,
    Json(req): Json<DatasetSchemaUpsertRequest>,
) -> Result<Json<DatasetSchemaResponse>, ApiError> {
    Ok(Json(state.store.upsert_dataset(&dataset_key, req)?))
}

async fn apply_snapshot(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(dataset_key): Path<String>,
    Json(req): Json<ApplySnapshotRequest>,
) -> Result<Json<ApplySnapshotResponse>, ApiError> {
    Ok(Json(state.store.apply_snapshot(
        state.tenant_id(),
        &dataset_key,
        req,
    )?))
}

async fn current_structured(
    _user: UserGuard,
    State(state): State<AppState>,
) -> Result<Json<CurrentStructuredStateResponse>, ApiError> {
    Ok(Json(state.store.current_structured_state()?))
}

async fn create_snapshot(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<CreateStructuredSnapshotRequest>,
) -> Result<Json<StructuredSnapshotResponse>, ApiError> {
    Ok(Json(state.store.create_snapshot(state.tenant_id(), req)?))
}

async fn get_snapshot(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<StructuredSnapshot>, ApiError> {
    Ok(Json(state.store.get_snapshot(&snapshot_id)?))
}

async fn bulk_rows(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
    Json(req): Json<BulkStructuredRowsRequest>,
) -> Result<Json<BulkStructuredRowsResponse>, ApiError> {
    Ok(Json(state.store.bulk_rows(
        state.tenant_id(),
        &snapshot_id,
        req,
    )?))
}

async fn list_rows(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.store.list_rows(&snapshot_id)?))
}

async fn insight_events(_user: UserGuard, Path(insight_id): Path<String>) -> Json<Value> {
    Json(json!({ "insight_id": insight_id, "events": [] }))
}

async fn fs_ls(
    _user: UserGuard,
    State(state): State<AppState>,
    Query(query): Query<FsQuery>,
) -> Result<Json<Value>, ApiError> {
    let _ = query.owner_user_id;
    Ok(Json(state.store.fs_ls(query.uri.as_deref())?))
}

async fn fs_tree(
    _user: UserGuard,
    State(state): State<AppState>,
    Query(query): Query<FsQuery>,
) -> Result<Json<Value>, ApiError> {
    let _ = query.owner_user_id;
    Ok(Json(
        state.store.fs_tree(query.uri.as_deref(), query.depth)?,
    ))
}

async fn fs_read(
    _user: UserGuard,
    State(state): State<AppState>,
    Query(query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(state.store.fs_read(&uri)?))
}

async fn fs_abstract(
    _user: UserGuard,
    State(state): State<AppState>,
    Query(query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(state.store.fs_layer(&uri, 0)?))
}

async fn fs_overview(
    _user: UserGuard,
    State(state): State<AppState>,
    Query(query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(state.store.fs_layer(&uri, 1)?))
}

async fn context_search(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<ContextSearchRequest>,
) -> Result<Json<ContextSearchResponse>, ApiError> {
    Ok(Json(
        state.store.search_context(state.tenant_id(), req)?.response,
    ))
}

async fn context_reveal(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<ContextRevealRequest>,
) -> Result<Json<ContextRevealResponse>, ApiError> {
    Ok(Json(state.store.reveal_context(req)?))
}

async fn rag_answer(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<RagAnswerRequest>,
) -> Result<Json<RagAnswerResponse>, ApiError> {
    Ok(Json(state.store.answer_rag(state.tenant_id(), req)?))
}

async fn rag_stream(
    user: UserGuard,
    state: State<AppState>,
    req: Json<RagAnswerRequest>,
) -> Result<Json<RagAnswerResponse>, ApiError> {
    rag_answer(user, state, req).await
}

async fn rag_debug(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    let answer = state.store.answer_rag(state.tenant_id(), req.clone())?;
    let trace = state.store.get_trace(&answer.trace_id)?;
    Ok(Json(json!({
        "answer": answer,
        "trace": trace,
        "prompt": build_prompt(&req.question.unwrap_or_default(), &answer.citations)
    })))
}

async fn create_session(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<SessionCreateRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    Ok(Json(state.store.create_session(req)?))
}

async fn add_session_message(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(req): Json<SessionMessageRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(state.store.add_session_message(
        state.tenant_id(),
        &session_id,
        req,
    )?))
}

async fn commit_session(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(req): Json<SessionCommitRequest>,
) -> Result<Json<SessionCommitResponse>, ApiError> {
    Ok(Json(state.store.commit_session(
        state.tenant_id(),
        &session_id,
        req,
    )?))
}

async fn llm_status(State(state): State<AppState>) -> Json<LlmStatusResponse> {
    Json(LlmStatusResponse {
        provider: state.config.llm_provider.clone(),
        model: state
            .config
            .llm_model
            .clone()
            .unwrap_or_else(|| "none".to_string()),
        auth_source: if state.config.llm_provider == "codex_auth" {
            "codex_auth"
        } else {
            "none"
        }
        .to_string(),
        healthy: state.config.llm_provider == "none" || state.config.llm_model.is_some(),
    })
}

async fn import_codex_auth(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<ImportCodexAuthRequest>,
) -> Result<Json<ImportCodexAuthResponse>, ApiError> {
    let _ = req.codex_auth_path;
    let _ = req.store_imported_token;
    if !state.config.allow_codex_auth_import {
        return Ok(Json(ImportCodexAuthResponse {
            status: "disabled".to_string(),
            auth_source: "none".to_string(),
            test_ok: false,
        }));
    }
    Ok(Json(ImportCodexAuthResponse {
        status: "imported_in_memory".to_string(),
        auth_source: "codex_auth".to_string(),
        test_ok: req.test_after_import,
    }))
}

async fn llm_test(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<LlmTestRequest>,
) -> Json<LlmTestResponse> {
    Json(LlmTestResponse {
        ok: state.config.llm_provider == "none",
        model: state
            .config
            .llm_model
            .clone()
            .unwrap_or_else(|| "none".to_string()),
        latency_ms: 0,
        sample: req
            .prompt
            .map(|prompt| {
                format!(
                    "provider=none echo: {}",
                    prompt.chars().take(80).collect::<String>()
                )
            })
            .unwrap_or_else(|| "provider=none".to_string()),
    })
}

async fn get_trace(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<TraceRecord>, ApiError> {
    Ok(Json(state.store.get_trace(&trace_id)?))
}

async fn debug_meili_search(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let index_uid = require_string(
        req.get("index_uid")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        "index_uid",
    )?;
    let query = req.get("query").and_then(Value::as_str).unwrap_or("");
    let raw = state.store.debug_meili_search(&index_uid, query)?;
    Ok(Json(redact_for_state(&state, raw)))
}

async fn prompt_preview(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    let answer = state.store.answer_rag(state.tenant_id(), req.clone())?;
    let prompt = build_prompt(&req.question.unwrap_or_default(), &answer.citations);
    Ok(Json(redact_for_state(
        &state,
        json!({
            "prompt": prompt,
            "trace_id": answer.trace_id,
            "citations": answer.citations
        }),
    )))
}

fn build_prompt(question: &str, citations: &[Citation]) -> String {
    let context = citations
        .iter()
        .map(|citation| format!("[{}] {}", citation.uri, citation.quote))
        .collect::<Vec<_>>()
        .join("\n");
    format!("Question:\n{question}\n\nContextFS staged context:\n{context}")
}

fn redact_for_state(state: &AppState, value: Value) -> Value {
    let mut secrets = Vec::new();
    if let Some(token) = &state.config.bearer_token {
        secrets.push(token.clone());
    }
    if let Some(token) = &state.config.admin_token {
        secrets.push(token.clone());
    }
    if let Some(key) = &state.config.meili_api_key {
        secrets.push(key.clone());
    }
    redact_secrets(&value, &secrets)
}
