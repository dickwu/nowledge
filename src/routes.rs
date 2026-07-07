use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use axum::{
    extract::{Multipart, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
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
    llm::{llm_client_from_config, LlmHealthProbe, LlmHealthProbeResult, LlmRequest},
    meili::MeiliAdmin,
    models::*,
    parser::parser_health_status,
    store::Store,
    util::{
        redact_secrets, redact_string, require_string, sanitize_slug, text_score, truncate_chars,
    },
};

#[derive(Clone)]
pub struct IngestTaskManager {
    queue: Option<tokio::sync::mpsc::Sender<QueuedIngestJob>>,
    queued_depth: Arc<AtomicUsize>,
}

struct QueuedIngestJob {
    tenant_id: String,
    task_id: String,
    req: IngestTaskRequest,
    config: Config,
}

impl IngestTaskManager {
    fn new(store: Store, config: Arc<Config>) -> Self {
        let queued_depth = Arc::new(AtomicUsize::new(0));
        if !config.ingest_worker_enabled {
            return Self {
                queue: None,
                queued_depth,
            };
        }

        let (tx, mut rx) = tokio::sync::mpsc::channel::<QueuedIngestJob>(
            config.ingest_max_concurrent_tasks.max(1) * 8,
        );
        let depth = queued_depth.clone();
        let max_concurrent = config.ingest_max_concurrent_tasks.max(1);
        tokio::spawn(async move {
            let semaphore = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
            while let Some(job) = rx.recv().await {
                depth.fetch_sub(1, Ordering::SeqCst);
                let Ok(permit) = semaphore.clone().acquire_owned().await else {
                    break;
                };
                let store = store.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(err) = store
                        .run_ingest_task_async(&job.tenant_id, &job.task_id, job.req, &job.config)
                        .await
                    {
                        tracing::warn!(task_id = %job.task_id, error = %err, "ingest task failed");
                    }
                });
            }
        });

        Self {
            queue: Some(tx),
            queued_depth,
        }
    }

    fn queued_ahead(&self) -> usize {
        self.queued_depth.load(Ordering::SeqCst)
    }

    async fn enqueue(&self, job: QueuedIngestJob) -> Result<(), ApiError> {
        let Some(queue) = &self.queue else {
            return Ok(());
        };
        self.queued_depth.fetch_add(1, Ordering::SeqCst);
        if queue.send(job).await.is_err() {
            self.queued_depth.fetch_sub(1, Ordering::SeqCst);
            return Err(ApiError::Internal(
                "ingest worker queue is unavailable".to_string(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Store,
    pub meili: MeiliAdmin,
    pub llm_health: LlmHealthProbe,
    pub ingest_manager: IngestTaskManager,
}

impl AppState {
    pub fn new(config: Arc<Config>) -> Self {
        let store = Store::new(&config);
        let ingest_manager = IngestTaskManager::new(store.clone(), config.clone());
        spawn_ingest_task_cleanup(store.clone(), &config);
        Self {
            store,
            meili: MeiliAdmin::from_config(&config),
            llm_health: LlmHealthProbe::new(),
            ingest_manager,
            config,
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.config.tenant_id
    }

    fn effective_config(&self) -> Config {
        (*self.config).clone()
    }
}

/// Periodically prune terminal ingest tasks past their retention window:
/// `RAG_INGEST_TASK_RETENTION_SECONDS` (0 disables pruning entirely), swept
/// every `RAG_INGEST_CLEANUP_INTERVAL_SECONDS`.
fn spawn_ingest_task_cleanup(store: Store, config: &Arc<Config>) {
    let retention_seconds = config.ingest_task_retention_seconds;
    if retention_seconds == 0 {
        return;
    }
    let interval_seconds = config.ingest_cleanup_interval_seconds.max(1);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // interval() completes its first tick immediately; skip it so a
        // fresh process doesn't sweep while it is still reloading state.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            match store.cleanup_ingest_tasks_async(retention_seconds).await {
                Ok(pruned) if !pruned.is_empty() => {
                    tracing::info!(count = pruned.len(), "pruned expired ingest tasks");
                }
                Ok(_) => {}
                Err(err) => {
                    tracing::warn!(error = %err, "ingest task cleanup pass failed");
                }
            }
        }
    });
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
        .route("/livez", get(livez))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/v1/usage", get(usage))
        .route("/v1/admin/bootstrap", post(bootstrap))
        .route("/v1/admin/harness/components", get(list_harness_components))
        .route(
            "/v1/admin/harness/components/{component_id}",
            get(get_harness_component),
        )
        .route(
            "/v1/admin/harness/components/{component_id}/revisions",
            post(create_harness_component_revision),
        )
        .route(
            "/v1/admin/harness/components/{component_id}/rollback",
            post(rollback_harness_component),
        )
        .route(
            "/v1/admin/harness/evolution/changes",
            post(create_harness_change).get(list_harness_changes),
        )
        .route(
            "/v1/admin/harness/evolution/changes/{change_id}",
            get(get_harness_change),
        )
        .route(
            "/v1/admin/harness/evolution/changes/{change_id}/verdict",
            post(create_harness_verdict),
        )
        .route(
            "/v1/admin/harness/evolution/changes/{change_id}/compare",
            post(compare_harness_change),
        )
        .route(
            "/v1/admin/harness/evolution/changes/{change_id}/delta",
            get(get_harness_change_delta),
        )
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
        .route("/v1/links", post(upsert_link))
        .route("/v1/links/search", post(search_links))
        .route("/v1/analysis/insights", post(analyze_insights))
        .route("/v1/state/company-docs", get(list_company_docs))
        .route(
            "/v1/state/company-docs/{source_id}",
            get(get_company_doc).delete(delete_company_doc),
        )
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
        .route("/v1/context/traceback", post(context_traceback))
        .route("/v1/ingest/tasks", post(create_ingest_task))
        .route("/v1/ingest/tasks/{task_id}", get(get_ingest_task))
        .route(
            "/v1/ingest/tasks/{task_id}/result",
            get(get_ingest_task_result),
        )
        .route("/v1/ingest/uploads", post(create_ingest_upload))
        .route("/v1/ingest/uploads:sync", post(ingest_upload_sync))
        .route("/v1/ingest/files:sync", post(ingest_file_sync))
        .route("/v1/rag/answer", post(rag_answer))
        .route("/v1/rag/stream", post(rag_stream))
        .route("/v1/rag/debug", post(rag_debug))
        .route(
            "/v1/eval/cases",
            post(create_eval_case).get(list_eval_cases),
        )
        .route("/v1/eval/runs", post(create_eval_run))
        .route("/v1/eval/runs/{run_id}", get(get_eval_run))
        .route("/v1/eval/runs/{run_id}/report", get(get_eval_run_report))
        .route(
            "/v1/eval/runs/{run_id}/analysis/overview",
            get(get_eval_overview),
        )
        .route(
            "/v1/eval/runs/{run_id}/analysis/cases/{case_id}",
            get(get_eval_case_analysis),
        )
        .route("/v1/sessions", post(create_session))
        .route(
            "/v1/sessions/{session_id}/messages",
            post(add_session_message),
        )
        .route("/v1/sessions/{session_id}/commit", post(commit_session))
        .route("/v1/llm/status", get(llm_status))
        .route("/v1/llm/test", post(llm_test))
        .route("/v1/llm/title", post(llm_title))
        .route("/v1/debug/traces/{trace_id}", get(get_trace))
        .route("/v1/debug/meili/search", post(debug_meili_search))
        .route("/v1/debug/prompt/preview", post(prompt_preview))
        .layer(CompressionLayer::new())
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Crate version and git revision baked in at compile time so every health
/// surface can answer "which build is actually running?" on a deploy host.
const SERVICE_VERSION: &str = env!("CARGO_PKG_VERSION");
const SERVICE_GIT_REV: &str = env!("NOWLEDGE_GIT_REV");

async fn livez() -> Json<Value> {
    Json(json!({
        "status": "ok",
        "version": SERVICE_VERSION,
        "git_rev": SERVICE_GIT_REV
    }))
}

async fn healthz(State(state): State<AppState>) -> impl IntoResponse {
    operational_health(state).await
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    operational_health(state).await
}

async fn operational_health(state: AppState) -> impl IntoResponse {
    let config = state.effective_config();
    let meili = state.meili.health_status().await;
    let llm = state.llm_health.check(&config).await;
    let parser = parser_health_status(&config).await;
    let usage = compact_usage_summary(
        state
            .store
            .usage_snapshot(state.tenant_id(), None, true)
            .unwrap_or_else(|err| json!({ "error": err.to_string() })),
    );
    let meili_healthy = meili
        .get("healthy")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let llm_unhealthy = llm.status == "unhealthy"
        || llm.quota_state == "exhausted"
        || (!llm.auth_valid && config.health_require_llm);
    let parser_unhealthy = config.parser_provider == "mineru"
        && !parser
            .get("healthy")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let degraded = llm.status == "degraded" || llm.stale;
    let ready = meili_healthy && !llm_unhealthy && !parser_unhealthy;
    let status = if !ready {
        "unhealthy"
    } else if degraded {
        "degraded"
    } else {
        "ok"
    };
    let body = json!({
        "status": status,
        "ready": ready,
        "version": SERVICE_VERSION,
        "git_rev": SERVICE_GIT_REV,
        "store_backend": state.store.backend_name(),
        "meilisearch": meili,
        "llm": llm_health_json(&llm),
        "parser": parser,
        "usage": usage
    });
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(body),
    )
}

async fn llm_health_false_ready(state: &AppState) -> bool {
    let config = state.effective_config();
    let meili = state.meili.health_status().await;
    let llm = state.llm_health.check(&config).await;
    let parser = parser_health_status(&config).await;
    let meili_healthy = meili
        .get("healthy")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let llm_unhealthy = llm.status == "unhealthy"
        || llm.quota_state == "exhausted"
        || (!llm.auth_valid && config.health_require_llm);
    let parser_unhealthy = config.parser_provider == "mineru"
        && !parser
            .get("healthy")
            .and_then(Value::as_bool)
            .unwrap_or(false);
    let ready = meili_healthy && !llm_unhealthy && !parser_unhealthy;
    llm_unhealthy && ready
}

fn llm_health_json(llm: &LlmHealthProbeResult) -> Value {
    json!({
        "provider": &llm.provider,
        "model": &llm.model,
        "reasoning_effort": &llm.reasoning_effort,
        "status": &llm.status,
        "can_call": llm.can_call,
        "auth_valid": llm.auth_valid,
        "quota_state": &llm.quota_state,
        "rate_limit_state": &llm.rate_limit_state,
        // Freshest live snapshot for this provider (real calls update it
        // between probe intervals), falling back to the probe's own capture.
        "rate_limits": crate::llm::effective_rate_limits(llm),
        "checked_at": llm.checked_at,
        "latency_ms": llm.latency_ms,
        "stale": llm.stale,
        "age_seconds": llm.age_seconds,
        "consecutive_failures": llm.consecutive_failures,
        "error_kind": &llm.error_kind,
        "message": &llm.message
    })
}

/// Flatten provider-reported token counts into an API `usage` JSON block.
fn merge_token_usage(usage: &mut Value, tokens: &crate::llm::LlmTokenUsage) {
    let Ok(token_value) = serde_json::to_value(tokens) else {
        return;
    };
    if let (Some(target), Some(source)) = (usage.as_object_mut(), token_value.as_object()) {
        for (key, value) in source {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn compact_usage_summary(usage: Value) -> Value {
    let providers = usage.get("providers").cloned().unwrap_or_else(|| json!({}));
    json!({
        "generated_at": usage.get("generated_at").cloned().unwrap_or(Value::Null),
        "history_events": providers.get("history_events").cloned().unwrap_or(Value::Null),
        "contextfs": providers.get("contextfs").cloned().unwrap_or(Value::Null),
        "rag": providers.get("rag").cloned().unwrap_or(Value::Null),
        "link_graph": providers.get("link_graph").cloned().unwrap_or(Value::Null),
        "ingest": providers.get("ingest").cloned().unwrap_or(Value::Null),
        "structured_data": providers.get("structured_data").cloned().unwrap_or(Value::Null),
        "sessions": providers.get("sessions").cloned().unwrap_or(Value::Null)
    })
}

async fn usage(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    if !user.principal.is_admin() && user.principal.owner_user_id.is_none() {
        return Err(ApiError::forbidden(
            "owner-bound auth is required for usage",
        ));
    }
    let include_global = user.principal.is_admin() && query.owner_user_id.is_none();
    if !include_global && query.owner_user_id.is_none() {
        return Err(ApiError::forbidden(
            "owner_user_id is required for non-admin usage",
        ));
    }
    let config = state.effective_config();
    let llm =
        state
            .llm_health
            .cached(&config)
            .unwrap_or_else(|| crate::llm::LlmHealthProbeResult {
                provider: config.llm_provider.clone(),
                model: config
                    .llm_model
                    .clone()
                    .unwrap_or_else(|| "none".to_string()),
                reasoning_effort: config.llm_reasoning_effort.clone(),
                status: "unknown".to_string(),
                can_call: false,
                auth_valid: false,
                quota_state: "unknown".to_string(),
                rate_limit_state: "unknown".to_string(),
                checked_at: chrono::Utc::now(),
                latency_ms: 0,
                stale: true,
                age_seconds: 0,
                consecutive_failures: 0,
                rate_limits: crate::llm::RateLimitSnapshot::default(),
                error_kind: Some("not_probed".to_string()),
                message: Some("LLM health has not been probed yet".to_string()),
            });
    let mut snapshot = state.store.usage_snapshot(
        state.tenant_id(),
        query.owner_user_id.as_deref(),
        include_global,
    )?;
    if let Some(providers) = snapshot.get_mut("providers").and_then(Value::as_object_mut) {
        providers.insert(
            "meilisearch".to_string(),
            json!({
                "configured": state.meili.configured(),
                "store_backend": state.store.backend_name()
            }),
        );
        providers.insert(
            "parser".to_string(),
            json!({
                "provider": &config.parser_provider,
                "mineru_api_url": if config.parser_provider == "mineru" {
                    Some(config.mineru_api_url.clone())
                } else {
                    None
                },
                "backend": if config.parser_provider == "mineru" {
                    config.mineru_backend.clone()
                } else {
                    "text".to_string()
                }
            }),
        );
        providers.insert("llm".to_string(), llm_health_json(&llm));
    }
    Ok(Json(snapshot))
}

async fn bootstrap(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    let reset = req.get("reset").and_then(Value::as_bool).unwrap_or(false);
    let result = state.meili.bootstrap(reset).await?;
    let hydrated = if reset {
        json!({})
    } else {
        state
            .store
            .hydrate_from_repository(state.tenant_id())
            .await?
    };
    Ok(Json(json!({
        "indexes": result.indexes,
        "tasks": result.tasks,
        "dry_run": result.dry_run,
        "hydrated": hydrated
    })))
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
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(req): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
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
    Ok(Json(state.store.timeline(
        state.tenant_id(),
        Some(&owner_user_id),
        req,
    )?))
}

async fn append_event_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
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
    Ok(Json(state.store.timeline(state.tenant_id(), None, req)?))
}

async fn upsert_state_fact(
    user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Json(mut req): Json<UpsertStateFactRequest>,
) -> Result<Json<StateItemResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
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
    Ok(Json(state.store.search_insights(req)?))
}

async fn upsert_link(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<LinkUpsertRequest>,
) -> Result<Json<LinkResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
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
    Ok(Json(state.store.search_links(
        state.tenant_id(),
        req,
        user.principal.is_admin(),
    )?))
}

async fn analyze_insights(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<AnalysisInsightRequest>,
) -> Result<Json<AnalysisInsightResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    if req.history_event_id.is_some() && req.owner_user_id.is_none() {
        return Err(ApiError::bad_request(
            "owner_user_id is required for history_event_id analysis",
        ));
    }
    Ok(Json(
        run_analysis_insights(&state, req, user.principal.is_admin()).await?,
    ))
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
    Ok(Json(
        state
            .store
            .create_revision_async(state.tenant_id(), &source_id, req)
            .await?,
    ))
}

async fn activate_revision(
    _user: UserGuard,
    State(state): State<AppState>,
    Path((source_id, revision_id)): Path<(String, String)>,
    Json(req): Json<ActivateRevisionRequest>,
) -> Result<Json<ActivateRevisionResponse>, ApiError> {
    Ok(Json(
        state
            .store
            .activate_revision_async(state.tenant_id(), &source_id, &revision_id, req)
            .await?,
    ))
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
    _user: UserGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        state
            .store
            .delete_company_doc(state.tenant_id(), &source_id)
            .await?,
    ))
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
    user: UserGuard,
    State(state): State<AppState>,
    Path(dataset_key): Path<String>,
    Json(req): Json<ApplySnapshotRequest>,
) -> Result<Json<ApplySnapshotResponse>, ApiError> {
    if let Some(snapshot_id) = req.snapshot_id.as_deref() {
        let owner = state.store.snapshot_owner_async(snapshot_id).await?;
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
    let owner = state.store.snapshot_owner_async(&snapshot_id).await?;
    user.require_owner_access(&owner)?;
    Ok(Json(state.store.get_snapshot_async(&snapshot_id).await?))
}

async fn bulk_rows(
    user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
    Json(req): Json<BulkStructuredRowsRequest>,
) -> Result<Json<BulkStructuredRowsResponse>, ApiError> {
    let owner = state.store.snapshot_owner_async(&snapshot_id).await?;
    user.require_owner_access(&owner)?;
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
    let owner = state.store.snapshot_owner_async(&snapshot_id).await?;
    user.require_owner_access(&owner)?;
    Ok(Json(state.store.list_rows_async(&snapshot_id).await?))
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
    Ok(Json(state.store.fs_ls(
        state.tenant_id(),
        query.uri.as_deref(),
        query.owner_user_id.as_deref(),
        user.principal.is_admin(),
    )?))
}

async fn fs_tree(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    Ok(Json(state.store.fs_tree(
        state.tenant_id(),
        query.uri.as_deref(),
        query.depth,
        query.owner_user_id.as_deref(),
        user.principal.is_admin(),
    )?))
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
    let owner = req
        .trace_id
        .as_ref()
        .and_then(|trace_id| state.store.trace_owner_id(trace_id).ok().flatten());
    if let Some(owner) = &owner {
        user.require_owner_access(owner)?;
    }
    let owner_scope = owner.or_else(|| user.principal.owner_user_id.clone());
    Ok(Json(state.store.reveal_context(
        state.tenant_id(),
        req,
        owner_scope.as_deref(),
        user.principal.is_admin(),
    )?))
}

async fn context_traceback(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<ContextTracebackRequest>,
) -> Result<Json<ContextTracebackResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    Ok(Json(state.store.traceback(
        state.tenant_id(),
        req,
        user.principal.is_admin(),
    )?))
}

async fn create_ingest_task(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<IngestTaskRequest>,
) -> Result<Json<IngestTask>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    let config = state.effective_config();
    let task = state
        .store
        .create_ingest_task_record_async(
            state.tenant_id(),
            &req,
            &config,
            state.ingest_manager.queued_ahead(),
        )
        .await?;
    state
        .ingest_manager
        .enqueue(QueuedIngestJob {
            tenant_id: state.tenant_id().to_string(),
            task_id: task.task_id.clone(),
            req,
            config,
        })
        .await?;
    Ok(Json(task))
}

async fn get_ingest_task(
    user: UserGuard,
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<IngestTask>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let include_all_private = user.principal.is_admin() && query.owner_user_id.is_none();
    Ok(Json(state.store.get_ingest_task(
        &task_id,
        query.owner_user_id.as_deref(),
        include_all_private,
    )?))
}

async fn get_ingest_task_result(
    user: UserGuard,
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<IngestTaskResult>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let include_all_private = user.principal.is_admin() && query.owner_user_id.is_none();
    Ok(Json(state.store.get_ingest_task_result(
        &task_id,
        query.owner_user_id.as_deref(),
        include_all_private,
    )?))
}

async fn ingest_file_sync(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<IngestTaskRequest>,
) -> Result<Json<IngestTaskResult>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    Ok(Json(
        state
            .store
            .ingest_file_sync_async(state.tenant_id(), req, &state.effective_config())
            .await?,
    ))
}

async fn create_ingest_upload(
    user: UserGuard,
    State(state): State<AppState>,
    multipart: Multipart,
) -> Result<Json<IngestTask>, ApiError> {
    let mut req = ingest_request_from_multipart(multipart).await?;
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    let config = state.effective_config();
    let task = state
        .store
        .create_ingest_task_record_async(
            state.tenant_id(),
            &req,
            &config,
            state.ingest_manager.queued_ahead(),
        )
        .await?;
    state
        .ingest_manager
        .enqueue(QueuedIngestJob {
            tenant_id: state.tenant_id().to_string(),
            task_id: task.task_id.clone(),
            req,
            config,
        })
        .await?;
    Ok(Json(task))
}

async fn ingest_upload_sync(
    user: UserGuard,
    State(state): State<AppState>,
    multipart: Multipart,
) -> Result<Json<IngestTaskResult>, ApiError> {
    let mut req = ingest_request_from_multipart(multipart).await?;
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    Ok(Json(
        state
            .store
            .ingest_file_sync_async(state.tenant_id(), req, &state.effective_config())
            .await?,
    ))
}

async fn ingest_request_from_multipart(
    mut multipart: Multipart,
) -> Result<IngestTaskRequest, ApiError> {
    let mut req = IngestTaskRequest::default();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|err| ApiError::bad_request(format!("invalid multipart body: {err}")))?
    {
        let name = field.name().map(ToString::to_string).unwrap_or_default();
        if matches!(name.as_str(), "file" | "document" | "upload") {
            if req.file_name.is_none() {
                req.file_name = field.file_name().map(ToString::to_string);
            }
            if req.content_type.is_none() {
                req.content_type = field.content_type().map(ToString::to_string);
            }
            let bytes = field
                .bytes()
                .await
                .map_err(|err| ApiError::bad_request(format!("invalid uploaded file: {err}")))?;
            req.bytes = Some(bytes.to_vec());
            continue;
        }

        let text = field.text().await.map_err(|err| {
            ApiError::bad_request(format!("invalid multipart field {name}: {err}"))
        })?;
        apply_ingest_multipart_field(&mut req, &name, text)?;
    }
    Ok(req)
}

fn apply_ingest_multipart_field(
    req: &mut IngestTaskRequest,
    name: &str,
    value: String,
) -> Result<(), ApiError> {
    match name {
        "owner_user_id" => req.owner_user_id = non_empty(value),
        "source_id" => req.source_id = non_empty(value),
        "revision_id" => req.revision_id = non_empty(value),
        "title" => req.title = non_empty(value),
        "source_uri" => req.source_uri = non_empty(value),
        "source_document_uri" => req.source_document_uri = non_empty(value),
        "content" => req.content = Some(value),
        "content_type" => req.content_type = non_empty(validate_multipart_content_type(&value)?),
        "file_name" => req.file_name = non_empty(value),
        "checksum" => req.checksum = non_empty(value),
        "parser_provider" => req.parser_provider = non_empty(value),
        "parser_backend" => req.parser_backend = non_empty(value),
        "content_list" => req.content_list = Some(parse_json_field(name, &value)?),
        "content_list_v2" => req.content_list_v2 = Some(parse_json_field(name, &value)?),
        "middle_json" => req.middle_json = Some(parse_json_field(name, &value)?),
        "model_json" => req.model_json = Some(parse_json_field(name, &value)?),
        "fragment_policy" => {
            req.fragment_policy = Some(parse_json_field::<FragmentPolicy>(name, &value)?)
        }
        "fragment_policy.chunk_size_chars" => {
            req.fragment_policy
                .get_or_insert_with(FragmentPolicy::default)
                .chunk_size_chars = Some(parse_usize_field(name, &value)?);
        }
        "fragment_policy.overlap_chars" => {
            req.fragment_policy
                .get_or_insert_with(FragmentPolicy::default)
                .overlap_chars = Some(parse_usize_field(name, &value)?);
        }
        "fragment_policy.min_chunk_chars" => {
            req.fragment_policy
                .get_or_insert_with(FragmentPolicy::default)
                .min_chunk_chars = Some(parse_usize_field(name, &value)?);
        }
        "idempotency_key" => req.idempotency_key = non_empty(value),
        _ => {}
    }
    Ok(())
}

fn non_empty(value: String) -> Option<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn validate_multipart_content_type(value: &str) -> Result<String, ApiError> {
    reqwest::multipart::Part::bytes(Vec::new())
        .mime_str(value)
        .map_err(|err| ApiError::bad_request(format!("invalid content_type: {err}")))?;
    Ok(value.to_string())
}

fn parse_json_field<T: serde::de::DeserializeOwned>(
    name: &str,
    value: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(value)
        .map_err(|err| ApiError::bad_request(format!("{name} must be valid JSON: {err}")))
}

fn parse_usize_field(name: &str, value: &str) -> Result<usize, ApiError> {
    value
        .parse::<usize>()
        .map_err(|err| ApiError::bad_request(format!("{name} must be a positive integer: {err}")))
}

async fn rag_answer(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<RagAnswerRequest>,
) -> Result<Json<RagAnswerResponse>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    Ok(Json(
        answer_rag_with_llm(&state, req, user.principal.is_admin()).await?,
    ))
}

async fn rag_stream(
    user: UserGuard,
    state: State<AppState>,
    req: Json<RagAnswerRequest>,
) -> Result<Json<RagAnswerResponse>, ApiError> {
    rag_answer(user, state, req).await
}

async fn rag_debug(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    let answer = answer_rag_with_llm(&state, req.clone(), user.principal.is_admin()).await?;
    let trace = state.store.get_trace_async(&answer.trace_id).await?;
    Ok(Json(json!({
        "answer": answer,
        "trace": trace,
        "prompt": build_prompt(&req.question.unwrap_or_default(), &answer.citations)
    })))
}

async fn create_eval_case(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<CreateRagEvalCaseRequest>,
) -> Result<Json<RagEvalCase>, ApiError> {
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
    Ok(Json(state.store.create_session(req)?))
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

async fn llm_status(State(state): State<AppState>) -> Json<LlmStatusResponse> {
    let config = state.effective_config();
    let status = llm_client_from_config(&config).status().await;
    Json(LlmStatusResponse {
        provider: status.provider,
        model: status.model,
        auth_source: status.auth_source,
        healthy: status.healthy,
    })
}

async fn llm_test(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<LlmTestRequest>,
) -> Result<Json<LlmTestResponse>, ApiError> {
    let config = state.effective_config();
    let client = llm_client_from_config(&config);
    let status = client.status().await;
    let response = client
        .complete_text(LlmRequest {
            prompt: req.prompt.unwrap_or_else(|| "ping".to_string()),
        })
        .await?;
    Ok(Json(LlmTestResponse {
        ok: true,
        model: status.model,
        latency_ms: response.latency_ms,
        usage: response.usage,
        sample: response.text,
    }))
}

/// Summarize `content` into a short title via the configured LLM. Available
/// to any authenticated user (UserGuard); the LLM call is governed by the
/// service-level config the same way RAG answers are.
async fn llm_title(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<LlmTitleRequest>,
) -> Result<Json<LlmTitleResponse>, ApiError> {
    let content = req.content.trim();
    if content.is_empty() {
        return Err(ApiError::bad_request("content is required"));
    }
    let max_chars = req.max_chars.unwrap_or(80).clamp(20, 200);
    let language_hint = req
        .language
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| format!(" in {s}"))
        .unwrap_or_else(|| " in the same language as the content".to_string());
    let hint_line = req
        .hint
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| format!("\nThe user proposed this draft to refine: \"{s}\""))
        .unwrap_or_default();

    // Keep prompt size bounded — only the first ~2000 chars are needed to
    // pick a good title, and longer prompts inflate latency / cost.
    let truncated: String = content.chars().take(2000).collect();

    let prompt = format!(
        "You are a precise editor. Produce a single concise title{language_hint} \
that captures the main topic of the document below. Constraints: max {max_chars} \
characters; no surrounding quotes; no trailing period; no leading numbering or \
emoji; do NOT wrap in markdown. Return ONLY the title text on one line.{hint_line}\n\n\
Document:\n{truncated}"
    );

    let config = state.effective_config();
    let client = llm_client_from_config(&config);
    let status = client.status().await;
    let response = client.complete_text(LlmRequest { prompt }).await?;

    // Clean up common LLM artifacts: surrounding quotes, leading "Title:",
    // trailing punctuation, newlines, and enforce the soft length cap.
    let mut title = response.text.trim().to_string();
    title = title.trim_start_matches('#').trim().to_string();
    for prefix in ["Title:", "title:", "TITLE:", "Title -", "title -"] {
        if let Some(rest) = title.strip_prefix(prefix) {
            title = rest.trim().to_string();
        }
    }
    title = title
        .trim_matches(|c: char| c == '"' || c == '\'' || c == '`')
        .to_string();
    if let Some(first_line) = title.lines().next() {
        title = first_line.to_string();
    }
    title = title.trim().trim_end_matches('.').trim().to_string();
    if title.chars().count() > max_chars {
        title = title.chars().take(max_chars).collect();
    }
    if title.is_empty() {
        title = "Untitled".to_string();
    }

    Ok(Json(LlmTitleResponse {
        title,
        model: status.model,
        latency_ms: response.latency_ms,
        usage: response.usage,
    }))
}

async fn get_trace(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<TraceRecord>, ApiError> {
    Ok(Json(state.store.get_trace_async(&trace_id).await?))
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
    let raw = state
        .store
        .debug_meili_search_async(&index_uid, query)
        .await?;
    Ok(Json(redact_for_state(&state, raw)))
}

async fn prompt_preview(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    let answer = answer_rag_with_llm(&state, req.clone(), true).await?;
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

async fn answer_rag_with_llm(
    state: &AppState,
    req: RagAnswerRequest,
    is_admin: bool,
) -> Result<RagAnswerResponse, ApiError> {
    let mut answer = state
        .store
        .answer_rag_async(state.tenant_id(), req.clone(), is_admin)
        .await?;
    let config = state.effective_config();
    if config.llm_provider != "none" {
        let client = llm_client_from_config(&config);
        let status = client.status().await;
        let prompt = build_prompt(&req.question.unwrap_or_default(), &answer.citations);
        let prompt = redact_string(
            &prompt,
            &[
                config.openai_api_key.clone().unwrap_or_default(),
                config
                    .codex_auth_path
                    .as_deref()
                    .and_then(crate::llm::read_codex_auth_token)
                    .unwrap_or_default(),
            ],
        );
        let llm = client.complete_text(LlmRequest { prompt }).await?;
        answer.answer = llm.text;
        let mut usage = json!({
            "provider": status.provider,
            "model": status.model,
            "latency_ms": llm.latency_ms,
            "backend": state.store.backend_name(),
            "grounded": true
        });
        if let Some(tokens) = llm.usage.as_ref() {
            merge_token_usage(&mut usage, tokens);
        }
        answer.usage = usage;
    }
    Ok(answer)
}

async fn run_analysis_insights(
    state: &AppState,
    req: AnalysisInsightRequest,
    is_admin: bool,
) -> Result<AnalysisInsightResponse, ApiError> {
    let query = require_string(req.query.clone(), "query")?;
    let owner_user_id = req.owner_user_id.clone();
    let (context_hits, existing_links, event_index_uid, seed_uris) =
        if let Some(history_event_id) = req.history_event_id.as_deref() {
            let owner = owner_user_id.as_deref().ok_or_else(|| {
                ApiError::bad_request("owner_user_id is required for history_event_id analysis")
            })?;
            let scope = history_analysis_scope(
                state,
                owner,
                history_event_id,
                &query,
                req.context_limit,
                req.link_limit,
            )
            .await?;
            (
                scope.context_hits,
                scope.existing_links,
                Some(scope.event_index_uid),
                scope.seed_uris,
            )
        } else {
            let context = state
                .store
                .search_context_async(
                    state.tenant_id(),
                    ContextSearchRequest {
                        query: Some(query.clone()),
                        owner_user_id: owner_user_id.clone(),
                        limit: req.context_limit.max(2),
                        debug: req.debug,
                        ..ContextSearchRequest::default()
                    },
                    is_admin,
                )
                .await?;
            let existing_links = state
                .store
                .search_links(
                    state.tenant_id(),
                    LinkSearchRequest {
                        owner_user_id: owner_user_id.clone(),
                        query: Some(query.clone()),
                        limit: req.link_limit,
                        ..LinkSearchRequest::default()
                    },
                    true,
                )?
                .links;
            (
                context.response.hits.clone(),
                existing_links,
                None,
                req.seed_uris.clone(),
            )
        };

    let prompt = build_analysis_prompt(&query, &context_hits, &existing_links, &seed_uris);
    let analysis_config = state.config.analysis_llm_config();
    let client = llm_client_from_config(&analysis_config);
    let status = client.status().await;
    let mut usage = json!({
        "provider": status.provider,
        "model": status.model,
        "backend": state.store.backend_name(),
        "grounded": true
    });
    if let Some(uid) = &event_index_uid {
        usage["history_scope"] = json!({
            "mode": "same_index",
            "event_index_uid": uid
        });
    }

    let mut draft = deterministic_analysis_draft(&query, &context_hits);
    if analysis_config.llm_provider != "none" {
        let prompt = redact_string(
            &prompt,
            &[
                analysis_config.openai_api_key.clone().unwrap_or_default(),
                analysis_config
                    .codex_auth_path
                    .as_deref()
                    .and_then(crate::llm::read_codex_auth_token)
                    .unwrap_or_default(),
            ],
        );
        let llm = client.complete_text(LlmRequest { prompt }).await?;
        if let Some(parsed) = parse_analysis_draft(&llm.text) {
            draft = merge_analysis_drafts(parsed, draft);
        }
        usage["latency_ms"] = json!(llm.latency_ms);
        usage["raw_response_preview"] = json!(truncate_for_json(&llm.text, 500));
        if let Some(tokens) = llm.usage.as_ref() {
            merge_token_usage(&mut usage, tokens);
        }
    }

    let title_by_uri = context_hits
        .iter()
        .map(|hit| (canonical_analysis_uri(&hit.uri), hit.title.clone()))
        .collect::<std::collections::HashMap<_, _>>();
    let mut created_links = Vec::new();
    if req.create_links {
        for candidate in &draft.links {
            let response = state
                .store
                .upsert_link_async(
                    state.tenant_id(),
                    LinkUpsertRequest {
                        owner_user_id: owner_user_id.clone(),
                        source_uri: Some(candidate.source_uri.clone()),
                        target_uri: Some(candidate.target_uri.clone()),
                        source_title: title_by_uri
                            .get(&canonical_analysis_uri(&candidate.source_uri))
                            .cloned(),
                        target_title: title_by_uri
                            .get(&canonical_analysis_uri(&candidate.target_uri))
                            .cloned(),
                        relation: candidate.relation.clone(),
                        rationale: candidate.rationale.clone(),
                        evidence_text: Some(query.clone()),
                        confidence: candidate.confidence,
                        created_by: "analysis_api".to_string(),
                        tags: vec!["analysis".to_string()],
                        idempotency_key: Some(format!(
                            "analysis:{}:{}:{}:{}",
                            query, candidate.source_uri, candidate.relation, candidate.target_uri
                        )),
                    },
                )
                .await?;
            created_links.push(response.link);
        }
    }

    let mut insights = Vec::new();
    if req.upsert_insights {
        for candidate in &draft.insights {
            let response = state
                .store
                .upsert_insight_async(
                    state.tenant_id(),
                    InsightUpsertRequest {
                        owner_user_id: owner_user_id.clone(),
                        insight_type: Some(candidate.insight_type.clone()),
                        title: Some(candidate.title.clone()),
                        statement: Some(candidate.statement.clone()),
                        evidence_text: Some(query.clone()),
                        source_refs: candidate
                            .source_uris
                            .iter()
                            .map(|uri| SourceRef {
                                kind: "context_uri".to_string(),
                                id: uri.clone(),
                                uri: Some(uri.clone()),
                                meta: None,
                            })
                            .collect(),
                        confidence: candidate.confidence,
                        salience: candidate.salience,
                        privacy: "private".to_string(),
                        merge_policy: "merge".to_string(),
                        idempotency_key: Some(format!("analysis:{}:{}", query, candidate.title)),
                    },
                )
                .await?;
            insights.push(response.insight);
        }
    }

    Ok(AnalysisInsightResponse {
        analysis_id: crate::util::new_id("analysis"),
        query,
        history_event_id: req.history_event_id,
        event_index_uid,
        context_hits,
        existing_links,
        link_candidates: draft.links,
        insight_candidates: draft.insights,
        created_links,
        insights,
        usage,
        prompt: req.debug.then_some(prompt),
    })
}

struct HistoryAnalysisScope {
    context_hits: Vec<ContextHit>,
    existing_links: Vec<KnowledgeLink>,
    event_index_uid: String,
    seed_uris: Vec<String>,
}

async fn history_analysis_scope(
    state: &AppState,
    owner_user_id: &str,
    history_event_id: &str,
    query: &str,
    context_limit: usize,
    link_limit: usize,
) -> Result<HistoryAnalysisScope, ApiError> {
    let selected = state
        .store
        .get_event_async(state.tenant_id(), owner_user_id, history_event_id)
        .await?;
    let same_index = state
        .store
        .search_events_async(
            state.tenant_id(),
            Some(owner_user_id),
            HistorySearchRequest {
                owner_user_id: Some(owner_user_id.to_string()),
                query: Some(query.to_string()),
                limit: context_limit.max(2),
                ..HistorySearchRequest::default()
            },
        )
        .await?;

    let mut events = vec![selected.clone()];
    for event in same_index.hits {
        if event.id != selected.id && event.event_index_uid == selected.event_index_uid {
            events.push(event);
        }
    }
    events.truncate(context_limit.max(1));

    let context_hits = events
        .iter()
        .map(|event| history_event_context_hit(event, query))
        .collect::<Vec<_>>();
    let allowed_uris = context_hits
        .iter()
        .map(|hit| canonical_analysis_uri(&hit.uri))
        .collect::<HashSet<_>>();
    let existing_links = state
        .store
        .search_links(
            state.tenant_id(),
            LinkSearchRequest {
                owner_user_id: Some(owner_user_id.to_string()),
                limit: link_limit.max(1),
                ..LinkSearchRequest::default()
            },
            true,
        )?
        .links
        .into_iter()
        .filter(|link| {
            allowed_uris.contains(&canonical_analysis_uri(&link.source_uri))
                || allowed_uris.contains(&canonical_analysis_uri(&link.target_uri))
        })
        .collect::<Vec<_>>();
    let seed_uris = context_hits
        .iter()
        .map(|hit| canonical_analysis_uri(&hit.uri))
        .collect::<Vec<_>>();

    Ok(HistoryAnalysisScope {
        context_hits,
        existing_links,
        event_index_uid: selected.event_index_uid,
        seed_uris,
    })
}

fn history_event_context_hit(event: &HistoryEvent, query: &str) -> ContextHit {
    let uri = format!(
        "ctx://user/history/{}/{}/detail",
        sanitize_slug(&event.event_type),
        sanitize_slug(&event.id)
    );
    let title = format!("{} {}", event.event_type, event.entity_id);
    ContextHit {
        uri,
        title,
        layer: 2,
        score: text_score(&event.text, query),
        node_kind: Some("fragment".to_string()),
        retrieval_role: Some("fragment".to_string()),
        source_id: Some(event.id.clone()),
        revision_id: None,
        source_document_uri: None,
        source_title: None,
        source_relation: None,
        fragment_index: None,
        char_start: None,
        char_end: None,
        block_type: None,
        page_idx: None,
        bbox: None,
        section_path: Vec::new(),
        heading_level: None,
        asset_refs: Vec::new(),
        artifact_refs: Vec::new(),
        checksum: None,
        source_summary: None,
        neighbor_fragments: Vec::new(),
        related_links: Vec::new(),
        score_breakdown: None,
        snippet: truncate_chars(&event.text, 240),
    }
}

fn build_prompt(question: &str, citations: &[Citation]) -> String {
    let context = citations
        .iter()
        .enumerate()
        .map(|(idx, citation)| {
            let source_title = citation
                .source_title
                .as_deref()
                .unwrap_or(citation.title.as_str());
            let mut location = Vec::new();
            if let Some(page_idx) = citation.page_idx {
                location.push(format!("page_idx={page_idx}"));
            }
            if let Some(block_type) = citation.block_type.as_deref() {
                location.push(format!("block_type={block_type}"));
            }
            if !citation.section_path.is_empty() {
                location.push(format!(
                    "section_path={}",
                    citation.section_path.join(" > ")
                ));
            }
            let location = if location.is_empty() {
                String::new()
            } else {
                format!(" ({})", location.join(", "))
            };
            format!(
                "[{}] {}{} uri={}\nquote: {}",
                idx + 1,
                source_title,
                location,
                citation.uri,
                citation.quote
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    format!("Question:\n{question}\n\nContextFS staged context:\n{context}")
}

#[derive(Debug, Deserialize, Default)]
struct AnalysisDraft {
    #[serde(default)]
    links: Vec<LinkCandidate>,
    #[serde(default)]
    insights: Vec<InsightCandidate>,
}

fn build_analysis_prompt(
    query: &str,
    hits: &[ContextHit],
    links: &[KnowledgeLink],
    seed_uris: &[String],
) -> String {
    let context = hits
        .iter()
        .map(|hit| {
            format!(
                "- uri: {}\n  title: {}\n  snippet: {}",
                hit.uri, hit.title, hit.snippet
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let existing_links = links
        .iter()
        .map(|link| {
            format!(
                "- {} --{}--> {} ({})",
                link.source_uri,
                link.relation,
                link.target_uri,
                link.rationale.as_deref().unwrap_or("no rationale")
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    format!(
        "Analyze ingested Nowledge context and propose Obsidian-style bidirectional associations plus durable insights.\n\
Return strict JSON with this shape: {{\"links\":[{{\"source_uri\":\"ctx://...\",\"target_uri\":\"ctx://...\",\"relation\":\"related\",\"rationale\":\"why\",\"confidence\":0.7}}],\"insights\":[{{\"insight_type\":\"analysis\",\"title\":\"short title\",\"statement\":\"grounded statement\",\"confidence\":0.7,\"salience\":0.5,\"source_uris\":[\"ctx://...\"]}}]}}.\n\
Query: {query}\n\
Seed URIs: {seed_uris:?}\n\
Context hits:\n{context}\n\
Existing links:\n{existing_links}"
    )
}

fn deterministic_analysis_draft(query: &str, hits: &[ContextHit]) -> AnalysisDraft {
    let distinct = distinct_canonical_hits(hits);
    let mut links = Vec::new();
    if distinct.len() >= 2 {
        links.push(LinkCandidate {
            source_uri: canonical_analysis_uri(&distinct[0].uri),
            target_uri: canonical_analysis_uri(&distinct[1].uri),
            relation: "related".to_string(),
            rationale: Some(format!(
                "Both ingested contexts match the analysis query: {query}"
            )),
            confidence: 0.65,
        });
    }

    let insights = distinct.first().map_or_else(Vec::new, |hit| {
        vec![InsightCandidate {
            insight_type: "analysis".to_string(),
            title: format!("Insight for {query}"),
            statement: format!(
                "The query '{query}' is grounded by ingested context '{}'.",
                hit.title
            ),
            confidence: 0.65,
            salience: 0.5,
            source_uris: distinct
                .iter()
                .take(3)
                .map(|hit| canonical_analysis_uri(&hit.uri))
                .collect(),
        }]
    });

    AnalysisDraft { links, insights }
}

fn distinct_canonical_hits(hits: &[ContextHit]) -> Vec<ContextHit> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for hit in hits {
        if seen.insert(canonical_analysis_uri(&hit.uri)) {
            out.push(hit.clone());
        }
    }
    out
}

fn parse_analysis_draft(text: &str) -> Option<AnalysisDraft> {
    serde_json::from_str::<AnalysisDraft>(text)
        .ok()
        .or_else(|| {
            let start = text.find('{')?;
            let end = text.rfind('}')?;
            serde_json::from_str::<AnalysisDraft>(&text[start..=end]).ok()
        })
        .filter(|draft| !draft.links.is_empty() || !draft.insights.is_empty())
}

fn merge_analysis_drafts(primary: AnalysisDraft, fallback: AnalysisDraft) -> AnalysisDraft {
    AnalysisDraft {
        links: if primary.links.is_empty() {
            fallback.links
        } else {
            primary.links
        },
        insights: if primary.insights.is_empty() {
            fallback.insights
        } else {
            primary.insights
        },
    }
}

fn canonical_analysis_uri(uri: &str) -> String {
    uri.trim()
        .strip_suffix("/.abstract")
        .or_else(|| uri.trim().strip_suffix("/.overview"))
        .or_else(|| uri.trim().strip_suffix("/detail"))
        .or_else(|| uri.trim().strip_suffix("/chunks/0001"))
        .unwrap_or_else(|| uri.trim())
        .to_string()
}

fn truncate_for_json(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out = text.chars().take(max.saturating_sub(3)).collect::<String>();
    out.push_str("...");
    out
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
    if let Some(key) = &state.config.openai_api_key {
        secrets.push(key.clone());
    }
    redact_secrets(&value, &secrets)
}
