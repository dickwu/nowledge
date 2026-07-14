use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    extract::{
        multipart::{Field, MultipartError},
        DefaultBodyLimit, Extension, MatchedPath, Multipart, Path, Query, Request, State,
    },
    http::{header::CONTENT_LENGTH, header::CONTENT_TYPE, Method, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, patch, post, put},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::{
    io::AsyncWriteExt,
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};
use tower_http::{
    compression::CompressionLayer,
    trace::{DefaultOnResponse, TraceLayer},
    LatencyUnit,
};

use crate::{
    auth::{AdminGuard, CompanyWriterGuard, Principal, UserGuard},
    config::Config,
    error::{safe_cause_diagnostic, safe_value_fingerprint, ApiError},
    http_boundary::{self, HttpBoundaryState, RequestDeadline},
    llm::{
        llm_client_from_config, llm_client_from_config_with_credentials, LlmHealthProbe,
        LlmHealthProbeResult, LlmRequest,
    },
    meili::MeiliAdmin,
    models::*,
    parser::{parser_health_status, StagedUpload},
    request_context::{self, RequestContextState, RequestId},
    runtime::RuntimeSupervisor,
    store::Store,
    util::{
        hmac_hex, redact_egress_text, redact_locator, redact_secrets, redact_string,
        require_string, sanitize_slug, text_score, validate_meili_uid,
    },
};

#[derive(Clone)]
pub struct IngestTaskManager {
    queue: Arc<Mutex<Option<mpsc::Sender<QueuedIngestJob>>>>,
    queued_depth: Arc<AtomicUsize>,
    accepting: Arc<AtomicBool>,
    enabled: bool,
    runtime: RuntimeSupervisor,
}

struct QueuedIngestJob {
    tenant_id: String,
    task_id: String,
    req: IngestTaskRequest,
    staged_upload: Option<StagedUpload>,
    config: Config,
    queue_depth: Option<QueueDepthLease>,
}

type IngestRunCompletion = (tokio::task::Id, String, Result<IngestTaskResult, ApiError>);
type IngestJoinCompletion = Result<IngestRunCompletion, tokio::task::JoinError>;

struct QueueDepthLease {
    queued_depth: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
struct SyncIngestTracker {
    task_id: Arc<Mutex<Option<String>>>,
}

impl SyncIngestTracker {
    fn set_task_id(&self, task_id: &str) {
        *self
            .task_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(task_id.to_string());
    }

    fn task_id(&self) -> Option<String> {
        self.task_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[derive(Clone)]
struct SyncIngestTimeoutState {
    timeout: Duration,
    store: Store,
    runtime: RuntimeSupervisor,
}

impl QueueDepthLease {
    fn acquire(queued_depth: Arc<AtomicUsize>) -> (Self, usize) {
        let queued_ahead = queued_depth.fetch_add(1, Ordering::SeqCst);
        (Self { queued_depth }, queued_ahead)
    }
}

impl Drop for QueueDepthLease {
    fn drop(&mut self) {
        self.queued_depth.fetch_sub(1, Ordering::SeqCst);
    }
}

impl IngestTaskManager {
    fn new(store: Store, config: Arc<Config>, runtime: RuntimeSupervisor) -> Self {
        let queued_depth = Arc::new(AtomicUsize::new(0));
        let accepting = Arc::new(AtomicBool::new(config.ingest_worker_enabled));
        if !config.ingest_worker_enabled {
            return Self {
                queue: Arc::new(Mutex::new(None)),
                queued_depth,
                accepting,
                enabled: false,
                runtime,
            };
        }

        let (tx, rx) = mpsc::channel::<QueuedIngestJob>(config.ingest_queue_capacity.max(1));
        let max_concurrent = config.ingest_max_concurrent_tasks.max(1);
        let shutdown = runtime.subscribe();
        let shutdown_grace = Duration::from_millis(config.shutdown_timeout_ms);
        let spawned = runtime.spawn(run_ingest_dispatcher(
            store,
            rx,
            max_concurrent,
            shutdown,
            shutdown_grace,
        ));
        debug_assert!(spawned, "fresh ingest runtime must accept its dispatcher");

        Self {
            queue: Arc::new(Mutex::new(Some(tx))),
            queued_depth,
            accepting,
            enabled: true,
            runtime,
        }
    }

    fn ensure_available(&self) -> Result<(), ApiError> {
        if !self.enabled {
            return Err(ApiError::service_unavailable(1));
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ApiError::service_unavailable(1));
        }
        Ok(())
    }

    async fn submit(
        &self,
        store: Store,
        tenant_id: String,
        req: IngestTaskRequest,
        staged_upload: Option<StagedUpload>,
        config: Config,
        deadline: RequestDeadline,
    ) -> Result<IngestTask, ApiError> {
        self.ensure_available()?;
        let queue = self
            .queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .cloned()
            .ok_or_else(|| ApiError::service_unavailable(1))?;
        let permit = match queue.try_reserve_owned() {
            Ok(permit) => permit,
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(ApiError::too_many_requests(1));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Err(ApiError::service_unavailable(1));
            }
        };
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ApiError::service_unavailable(1));
        }

        let (queue_depth, queued_ahead) = QueueDepthLease::acquire(self.queued_depth.clone());
        let has_staged_upload = staged_upload.is_some();
        let (reply, response) = oneshot::channel();
        let admission = async move {
            let result = match tokio::time::timeout_at(deadline.instant(), async {
                let task = store
                    .create_ingest_task_record_async(
                        &tenant_id,
                        &req,
                        &config,
                        has_staged_upload,
                        queued_ahead,
                    )
                    .await?;
                permit.send(QueuedIngestJob {
                    tenant_id,
                    task_id: task.task_id.clone(),
                    req,
                    staged_upload,
                    config,
                    queue_depth: Some(queue_depth),
                });
                Ok(task)
            })
            .await
            {
                Ok(result) => result,
                Err(_) => Err(ApiError::timeout()),
            };
            let _ = reply.send(result);
        };
        if !self.runtime.spawn(admission) {
            return Err(ApiError::service_unavailable(1));
        }
        response
            .await
            .unwrap_or_else(|_| Err(ApiError::service_unavailable(1)))
    }

    fn begin_shutdown(&self) {
        self.accepting.store(false, Ordering::Release);
        self.queue
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();
    }
}

async fn run_ingest_dispatcher(
    store: Store,
    mut queue: mpsc::Receiver<QueuedIngestJob>,
    max_concurrent: usize,
    mut shutdown: watch::Receiver<bool>,
    shutdown_grace: Duration,
) {
    let mut running = JoinSet::new();
    let mut active_task_ids = HashMap::new();

    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            joined = running.join_next(), if !running.is_empty() => {
                handle_ingest_completion(&store, joined, &mut active_task_ids).await;
            }
            job = queue.recv(), if running.len() < max_concurrent => {
                let Some(mut job) = job else {
                    break;
                };
                job.queue_depth.take();
                let tracked_task_id = job.task_id.clone();
                let task_id = tracked_task_id.clone();
                let task_store = store.clone();
                let handle = running.spawn(async move {
                    let join_id = tokio::task::id();
                    let result = task_store
                        .run_ingest_task_async(
                            &job.tenant_id,
                            &job.task_id,
                            job.req,
                            job.staged_upload,
                            &job.config,
                        )
                        .await;
                    (join_id, task_id, result)
                });
                active_task_ids.insert(handle.id(), tracked_task_id);
            }
        }
    }

    let deadline = tokio::time::Instant::now() + shutdown_grace;
    let mut interrupted_tasks = Vec::new();
    queue.close();
    while let Some(mut job) = queue.recv().await {
        job.queue_depth.take();
        match store.mark_ingest_task_interrupted_local(&job.task_id) {
            Ok(Some(task)) => interrupted_tasks.push(task),
            Ok(None) => {}
            Err(err) => {
                log_ingest_failure(&job.task_id, &err, "failed to interrupt queued ingest task")
            }
        }
    }

    while !running.is_empty() {
        match tokio::time::timeout_at(deadline, running.join_next()).await {
            Ok(joined) => {
                if tokio::time::timeout_at(
                    deadline,
                    handle_ingest_completion(&store, joined, &mut active_task_ids),
                )
                .await
                .is_err()
                {
                    break;
                }
            }
            Err(_) => break,
        }
    }

    if !running.is_empty() {
        running.abort_all();
        while let Ok(Some(joined)) = tokio::time::timeout_at(deadline, running.join_next()).await {
            if tokio::time::timeout_at(
                deadline,
                handle_ingest_completion(&store, Some(joined), &mut active_task_ids),
            )
            .await
            .is_err()
            {
                break;
            }
        }
    }
    for task_id in active_task_ids.into_values() {
        match store.mark_ingest_task_interrupted_local(&task_id) {
            Ok(Some(task)) => interrupted_tasks.push(task),
            Ok(None) => {}
            Err(err) => {
                log_ingest_failure(&task_id, &err, "failed to interrupt active ingest task")
            }
        }
    }
    if !interrupted_tasks.is_empty() {
        match tokio::time::timeout_at(
            deadline,
            store.persist_ingest_task_records(&interrupted_tasks),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(err)) => log_ingest_failure(
                &interrupted_tasks[0].task_id,
                &err,
                "failed to persist interrupted ingest tasks",
            ),
            Err(_) => tracing::warn!(
                interrupted_tasks = interrupted_tasks.len(),
                "ingest dispatcher deadline elapsed before task states were persisted"
            ),
        }
    }
}

async fn handle_ingest_completion(
    store: &Store,
    joined: Option<IngestJoinCompletion>,
    active_task_ids: &mut HashMap<tokio::task::Id, String>,
) {
    let Some(joined) = joined else {
        return;
    };
    match joined {
        Ok((join_id, task_id, result)) => {
            active_task_ids.remove(&join_id);
            if let Err(err) = result {
                log_ingest_failure(&task_id, &err, "ingest task failed");
                if let Err(mark_err) = store.mark_ingest_task_failed_async(&task_id).await {
                    log_ingest_failure(&task_id, &mark_err, "failed to finalize ingest task");
                }
            }
        }
        Err(join_error) => {
            let Some(task_id) = active_task_ids.remove(&join_error.id()) else {
                return;
            };
            let result = if join_error.is_cancelled() {
                store.mark_ingest_task_interrupted_async(&task_id).await
            } else {
                let task_fingerprint = safe_value_fingerprint("ingest_task_id", &task_id);
                tracing::warn!(
                    %task_fingerprint,
                    cause_category = "task_panic",
                    "ingest task terminated unexpectedly"
                );
                store.mark_ingest_task_failed_async(&task_id).await
            };
            if let Err(err) = result {
                log_ingest_failure(&task_id, &err, "failed to finalize terminated ingest task");
            }
        }
    }
}

async fn enforce_sync_ingest_timeout(
    State(state): State<SyncIngestTimeoutState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !http_boundary::store_owns_timeout(request.uri().path()) {
        return next.run(request).await;
    }

    let deadline = tokio::time::Instant::now() + state.timeout;
    let tracker = SyncIngestTracker::default();
    request
        .extensions_mut()
        .insert(RequestDeadline::new(deadline));
    request.extensions_mut().insert(tracker.clone());

    match tokio::time::timeout_at(deadline, next.run(request)).await {
        Ok(response) => {
            if response.status().is_client_error() || response.status().is_server_error() {
                if let Some(task_id) = tracker.task_id() {
                    match state.store.mark_ingest_task_failed_local(&task_id) {
                        Ok(Some(task)) => supervise_ingest_task_persistence(
                            &state,
                            task,
                            "failed to persist failed sync ingest task",
                        ),
                        Ok(None) => {}
                        Err(err) => log_ingest_failure(
                            &task_id,
                            &err,
                            "failed to finalize sync ingest task",
                        ),
                    }
                }
            }
            response
        }
        Err(_) => {
            if let Some(task_id) = tracker.task_id() {
                if let Ok(result) = state.store.get_ingest_task_result(&task_id, None, true) {
                    return Json(result).into_response();
                }
                match state.store.mark_ingest_task_interrupted_local(&task_id) {
                    Ok(Some(task)) => supervise_ingest_task_persistence(
                        &state,
                        task,
                        "failed to persist timed-out ingest task",
                    ),
                    Ok(None) => {}
                    Err(err) => {
                        log_ingest_failure(
                            &task_id,
                            &err,
                            "failed to interrupt timed-out ingest task",
                        );
                    }
                }
            }
            ApiError::timeout().into_response()
        }
    }
}

fn supervise_ingest_task_persistence(
    state: &SyncIngestTimeoutState,
    task: IngestTask,
    message: &'static str,
) {
    let store = state.store.clone();
    let task_id = task.task_id.clone();
    let rejected_task_id = task_id.clone();
    if !state.runtime.spawn(async move {
        if let Err(err) = store.persist_ingest_task_record(&task).await {
            log_ingest_failure(&task_id, &err, message);
        }
    }) {
        tracing::warn!(
            task_fingerprint = %safe_value_fingerprint("ingest_task_id", &rejected_task_id),
            "ingest runtime closed before terminal task persistence could be supervised"
        );
    }
}

fn log_ingest_failure(task_id: &str, err: &ApiError, message: &'static str) {
    let diagnostic = safe_cause_diagnostic(err);
    let task_fingerprint = safe_value_fingerprint("ingest_task_id", task_id);
    tracing::warn!(
        %task_fingerprint,
        cause_category = diagnostic.category,
        cause_fingerprint = %diagnostic.fingerprint,
        message
    );
}

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub store: Store,
    pub meili: MeiliAdmin,
    pub llm_health: LlmHealthProbe,
    pub ingest_manager: IngestTaskManager,
    pub(crate) http_boundary: HttpBoundaryState,
    runtime: RuntimeSupervisor,
}

impl AppState {
    pub fn new(config: Arc<Config>) -> Self {
        let store = Store::new(&config);
        let runtime = RuntimeSupervisor::new();
        let http_boundary = HttpBoundaryState::new(&config);
        config.start_codex_secret_refresh_task();
        let ingest_manager = IngestTaskManager::new(store.clone(), config.clone(), runtime.clone());
        spawn_ingest_task_cleanup(store.clone(), &config, &runtime);
        Self {
            store,
            meili: MeiliAdmin::from_config(&config),
            llm_health: LlmHealthProbe::new(),
            ingest_manager,
            http_boundary,
            runtime,
            config,
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.config.tenant_id
    }

    fn effective_config(&self) -> Config {
        (*self.config).clone()
    }

    pub fn begin_shutdown(&self) {
        self.ingest_manager.begin_shutdown();
        self.runtime.begin_shutdown();
    }

    pub async fn shutdown(&self) {
        let deadline =
            tokio::time::Instant::now() + Duration::from_millis(self.config.shutdown_timeout_ms);
        self.shutdown_until(deadline).await;
    }

    pub async fn shutdown_until(&self, deadline: tokio::time::Instant) {
        self.begin_shutdown();
        self.runtime.shutdown_until(deadline).await;
        match self
            .store
            .interrupt_nonterminal_ingest_tasks_local(self.tenant_id())
        {
            Ok(tasks) if !tasks.is_empty() => {
                match tokio::time::timeout_at(
                    deadline,
                    self.store.persist_ingest_task_records(&tasks),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => log_ingest_failure(
                        &tasks[0].task_id,
                        &err,
                        "failed to persist interrupted ingest tasks during shutdown",
                    ),
                    Err(_) => tracing::warn!(
                        interrupted_tasks = tasks.len(),
                        "shutdown deadline elapsed before interrupted task states were persisted"
                    ),
                }
            }
            Ok(_) => {}
            Err(err) => {
                let diagnostic = safe_cause_diagnostic(&err);
                tracing::warn!(
                    cause_category = diagnostic.category,
                    cause_fingerprint = %diagnostic.fingerprint,
                    "failed to finalize interrupted ingest tasks during shutdown"
                );
            }
        }
    }
}

/// Periodically prune terminal ingest tasks past their retention window:
/// `RAG_INGEST_TASK_RETENTION_SECONDS` (0 disables pruning entirely), swept
/// every `RAG_INGEST_CLEANUP_INTERVAL_SECONDS`.
fn spawn_ingest_task_cleanup(store: Store, config: &Arc<Config>, runtime: &RuntimeSupervisor) {
    let retention_seconds = config.ingest_task_retention_seconds;
    if retention_seconds == 0 {
        return;
    }
    let interval_seconds = config.ingest_cleanup_interval_seconds.max(1);
    let tenant_id = config.tenant_id.clone();
    let mut shutdown = runtime.subscribe();
    let _ = runtime.spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(interval_seconds));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // interval() completes its first tick immediately; skip it so a
        // fresh process doesn't sweep while it is still reloading state.
        ticker.tick().await;
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                    continue;
                }
                _ = ticker.tick() => {}
            }
            match store
                .cleanup_ingest_tasks_async(&tenant_id, retention_seconds)
                .await
            {
                Ok(pruned) if !pruned.is_empty() => {
                    tracing::info!(count = pruned.len(), "pruned expired ingest tasks");
                }
                Ok(_) => {}
                Err(err) => {
                    let diagnostic = safe_cause_diagnostic(&err);
                    tracing::warn!(
                        cause_category = diagnostic.category,
                        cause_fingerprint = %diagnostic.fingerprint,
                        "ingest task cleanup pass failed"
                    );
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

fn validate_max_items(field: &str, actual: usize, maximum: usize) -> Result<(), ApiError> {
    if actual > maximum {
        return Err(ApiError::validation(
            field,
            format!("must contain at most {maximum} items"),
        ));
    }
    Ok(())
}

fn validate_search_limit(field: &str, limit: usize, config: &Config) -> Result<(), ApiError> {
    if limit > config.max_search_limit {
        return Err(ApiError::validation(
            field,
            format!("must be at most {}", config.max_search_limit),
        ));
    }
    Ok(())
}

fn validate_tags(field: &str, tags: &[String], config: &Config) -> Result<(), ApiError> {
    validate_max_items(field, tags.len(), config.max_tags_per_item)?;
    for (index, tag) in tags.iter().enumerate() {
        if tag.len() > config.max_tag_bytes {
            return Err(ApiError::validation(
                format!("{field}[{index}]"),
                format!("must be at most {} UTF-8 bytes", config.max_tag_bytes),
            ));
        }
    }
    Ok(())
}

fn validate_history_event(
    field: &str,
    request: &AppendHistoryEventRequest,
    config: &Config,
) -> Result<(), ApiError> {
    validate_tags(&format!("{field}.tags"), &request.tags, config)
}

fn validate_history_bulk(
    request: &BulkHistoryEventsRequest,
    config: &Config,
) -> Result<(), ApiError> {
    validate_max_items("events", request.events.len(), config.max_bulk_events)?;
    for (index, event) in request.events.iter().enumerate() {
        validate_history_event(&format!("events[{index}]"), event, config)?;
    }
    Ok(())
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
            SyncIngestTimeoutState {
                timeout: Duration::from_millis(state.config.sync_ingest_timeout_ms),
                store: state.store.clone(),
                runtime: state.runtime.clone(),
            },
            enforce_sync_ingest_timeout,
        ))
        .layer(middleware::from_fn_with_state(
            state.http_boundary.clone(),
            http_boundary::enforce_timeout,
        ))
        .layer(middleware::from_fn_with_state(
            state.http_boundary.clone(),
            http_boundary::load_shed,
        ))
        .layer(middleware::from_fn_with_state(
            state.config.clone(),
            redact_json_response,
        ))
        .layer(CompressionLayer::new())
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
                .on_response(
                    DefaultOnResponse::new()
                        .level(tracing::Level::INFO)
                        .latency_unit(LatencyUnit::Millis),
                ),
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

async fn healthz(_admin: AdminGuard, State(state): State<AppState>) -> impl IntoResponse {
    let check = operational_check(&state).await;
    let usage = compact_usage_summary(
        state
            .store
            .usage_snapshot(state.tenant_id(), None, true)
            .unwrap_or_else(|_| json!({ "error": "usage snapshot unavailable" })),
    );
    let body = json!({
        "status": check.status,
        "ready": check.ready,
        "version": SERVICE_VERSION,
        "git_rev": SERVICE_GIT_REV,
        "store_backend": state.store.backend_name(),
        "meilisearch": sanitize_dependency_health(check.meili, "Meilisearch health check failed"),
        "hydration": check.hydration,
        "llm": llm_health_json(&check.llm),
        "parser": sanitize_dependency_health(check.parser, "parser health check failed"),
        "usage": usage
    });
    health_response(check.ready, redact_for_state(&state, body))
}

async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    let check = operational_check(&state).await;
    let meili_status = dependency_status(&check.meili);
    let parser_status = dependency_status(&check.parser);
    let llm_status = if check.llm.status == "unhealthy"
        || check.llm.quota_state == "exhausted"
        || (!check.llm.auth_valid && state.config.health_require_llm)
    {
        "unhealthy"
    } else if check.llm.status == "degraded" || check.llm.stale {
        "degraded"
    } else {
        "ok"
    };
    health_response(
        check.ready,
        json!({
            "status": check.status,
            "ready": check.ready,
            "version": SERVICE_VERSION,
            "git_rev": SERVICE_GIT_REV,
            "dependencies": {
                "meilisearch": meili_status,
                "hydration": check.hydration.status,
                "llm": llm_status,
                "parser": parser_status
            }
        }),
    )
}

struct OperationalCheck {
    meili: Value,
    hydration: HydrationReport,
    llm: LlmHealthProbeResult,
    parser: Value,
    ready: bool,
    status: &'static str,
}

async fn operational_check(state: &AppState) -> OperationalCheck {
    let config = state.effective_config();
    let meili = state.meili.health_status().await;
    let hydration = state
        .store
        .hydration_report()
        .unwrap_or_else(|_| HydrationReport {
            tenant_id: state.tenant_id().to_string(),
            backend: state.store.backend_name().to_string(),
            status: HydrationStatus::Incomplete,
            ready: false,
            started_at: chrono::Utc::now(),
            completed_at: None,
            domains: Default::default(),
        });
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
    let degraded = llm.status == "degraded" || llm.stale;
    let ready = meili_healthy && hydration.ready && !llm_unhealthy && !parser_unhealthy;
    let status = if !ready {
        "unhealthy"
    } else if degraded {
        "degraded"
    } else {
        "ok"
    };
    OperationalCheck {
        meili,
        hydration,
        llm,
        parser,
        ready,
        status,
    }
}

fn health_response(ready: bool, body: Value) -> impl IntoResponse {
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(body),
    )
}

fn dependency_status(value: &Value) -> &'static str {
    if value
        .get("healthy")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        "ok"
    } else {
        "unhealthy"
    }
}

fn sanitize_dependency_health(mut value: Value, failure_message: &str) -> Value {
    if let Some(object) = value.as_object_mut() {
        if object.contains_key("error") {
            object.insert("error".to_string(), json!(failure_message));
        }
        object.remove("mineru");
    }
    value
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
    let public_message = llm
        .message
        .as_ref()
        .map(|_| "LLM health probe reported a failure");
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
        "message": public_message
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
    let include_global = user.principal.is_admin() && query.owner_user_id.is_none();
    if !include_global && query.owner_user_id.is_none() {
        return Err(ApiError::forbidden(
            "owner_user_id is required for non-admin usage",
        ));
    }
    let mut snapshot = state.store.usage_snapshot(
        state.tenant_id(),
        query.owner_user_id.as_deref(),
        include_global,
    )?;
    if let Some(providers) = snapshot.get_mut("providers").and_then(Value::as_object_mut) {
        if user.principal.is_admin() {
            let config = state.effective_config();
            let llm = state.llm_health.cached(&config).unwrap_or_else(|| {
                crate::llm::LlmHealthProbeResult {
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
                }
            });
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
        } else {
            providers.remove("nowledge_api");
        }
    }
    Ok(Json(snapshot))
}

async fn bootstrap(_admin: AdminGuard, Json(_req): Json<Value>) -> Result<Json<Value>, ApiError> {
    Err(ApiError::bad_request(
        "managed-index bootstrap is unavailable over HTTP; startup reconciles settings automatically",
    ))
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

async fn analyze_insights(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<AnalysisInsightRequest>,
) -> Result<Json<Value>, ApiError> {
    if req.debug && !user.principal.is_admin() {
        return Err(ApiError::forbidden(
            "admin permission is required for analysis debug output",
        ));
    }
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    validate_search_limit("context_limit", req.context_limit, &state.config)?;
    validate_search_limit("link_limit", req.link_limit, &state.config)?;
    if req.history_event_id.is_some() && req.owner_user_id.is_none() {
        return Err(ApiError::bad_request(
            "owner_user_id is required for history_event_id analysis",
        ));
    }
    let response = run_analysis_insights(&state, req, user.principal.is_admin()).await?;
    let response =
        serde_json::to_value(response).map_err(|error| ApiError::Internal(error.to_string()))?;
    Ok(Json(redact_for_state(&state, response)))
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

async fn create_ingest_task(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(deadline): Extension<RequestDeadline>,
    Json(mut req): Json<IngestTaskRequest>,
) -> Result<Json<IngestTask>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    let config = state.effective_config();
    let task = state
        .ingest_manager
        .submit(
            state.store.clone(),
            state.tenant_id().to_string(),
            req,
            None,
            config,
            deadline,
        )
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
    Extension(tracker): Extension<SyncIngestTracker>,
    Json(mut req): Json<IngestTaskRequest>,
) -> Result<Json<IngestTaskResult>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    Ok(Json(
        state
            .store
            .ingest_file_sync_async(
                state.tenant_id(),
                req,
                None,
                &state.effective_config(),
                |task_id| tracker.set_task_id(task_id),
            )
            .await?,
    ))
}

async fn create_ingest_upload(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(deadline): Extension<RequestDeadline>,
    multipart: Multipart,
) -> Result<Json<IngestTask>, ApiError> {
    state.ingest_manager.ensure_available()?;
    let mut prepared = ingest_request_from_multipart(multipart, &state.config).await?;
    user.apply_owner_default(&mut prepared.request.owner_user_id)?;
    require_owner_for_write(&user, prepared.request.owner_user_id.as_deref())?;
    let config = state.effective_config();
    let task = state
        .ingest_manager
        .submit(
            state.store.clone(),
            state.tenant_id().to_string(),
            prepared.request,
            prepared.staged_upload,
            config,
            deadline,
        )
        .await?;
    Ok(Json(task))
}

async fn ingest_upload_sync(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(tracker): Extension<SyncIngestTracker>,
    multipart: Multipart,
) -> Result<Json<IngestTaskResult>, ApiError> {
    let mut prepared = ingest_request_from_multipart(multipart, &state.config).await?;
    user.apply_owner_default(&mut prepared.request.owner_user_id)?;
    require_owner_for_write(&user, prepared.request.owner_user_id.as_deref())?;
    Ok(Json(
        state
            .store
            .ingest_file_sync_async(
                state.tenant_id(),
                prepared.request,
                prepared.staged_upload,
                &state.effective_config(),
                |task_id| tracker.set_task_id(task_id),
            )
            .await?,
    ))
}

struct PreparedIngestRequest {
    request: IngestTaskRequest,
    staged_upload: Option<StagedUpload>,
}

struct TemporaryUploadPath {
    path: Option<PathBuf>,
}

impl TemporaryUploadPath {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn path(&self) -> &std::path::Path {
        self.path
            .as_deref()
            .expect("temporary upload path must exist until staged")
    }

    fn into_staged(mut self, byte_len: u64, sha256: String) -> StagedUpload {
        let path = self
            .path
            .take()
            .expect("temporary upload path must exist until staged");
        StagedUpload::new(path, byte_len, sha256)
    }
}

impl Drop for TemporaryUploadPath {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(err) = std::fs::remove_file(path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    error_kind = "temporary_upload_cleanup",
                    "failed to remove incomplete staged upload"
                );
            }
        }
    }
}

async fn ingest_request_from_multipart(
    mut multipart: Multipart,
    config: &Config,
) -> Result<PreparedIngestRequest, ApiError> {
    let mut req = IngestTaskRequest::default();
    let mut staged_upload = None;
    let mut file_part_content_type = None;
    let mut field_count = 0_usize;
    let mut metadata_bytes = 0_usize;
    while let Some(field) = multipart.next_field().await.map_err(map_multipart_error)? {
        field_count = field_count.saturating_add(1);
        if field_count > config.max_multipart_fields {
            return Err(ApiError::payload_too_large());
        }
        let name = field.name().map(ToString::to_string).unwrap_or_default();
        if matches!(name.as_str(), "file" | "document" | "upload") {
            if staged_upload.is_some() {
                return Err(ApiError::validation(
                    "file",
                    "only one upload file is allowed",
                ));
            }
            if req.file_name.is_none() {
                req.file_name = field.file_name().map(sanitize_upload_filename);
            }
            let part_content_type = field
                .content_type()
                .ok_or_else(|| ApiError::validation("content_type", "is required for uploads"))
                .and_then(validate_multipart_content_type)?;
            validate_upload_content_type_policy(&part_content_type, config)?;
            if req
                .content_type
                .as_deref()
                .is_some_and(|declared| !declared.eq_ignore_ascii_case(&part_content_type))
            {
                return Err(ApiError::validation(
                    "content_type",
                    "metadata must match the upload part Content-Type",
                ));
            }
            req.content_type = Some(part_content_type.clone());
            file_part_content_type = Some(part_content_type);
            staged_upload = Some(stage_multipart_upload(field, config.max_upload_bytes).await?);
            continue;
        }

        let text =
            read_multipart_metadata_field(field, &name, &mut metadata_bytes, config.max_json_bytes)
                .await?;
        apply_ingest_multipart_field(&mut req, &name, text)?;
    }

    if staged_upload.is_some()
        && (req.content.is_some()
            || req.bytes.is_some()
            || req.content_list.is_some()
            || req.content_list_v2.is_some()
            || req.middle_json.is_some()
            || req.model_json.is_some())
    {
        return Err(ApiError::validation(
            "multipart",
            "file uploads cannot be combined with alternate content or parser output fields",
        ));
    }

    if let (Some(part_content_type), Some(effective_content_type)) = (
        file_part_content_type.as_deref(),
        req.content_type.as_deref(),
    ) {
        if !part_content_type.eq_ignore_ascii_case(effective_content_type) {
            return Err(ApiError::validation(
                "content_type",
                "metadata must match the upload part Content-Type",
            ));
        }
    }

    if let Some(content_type) = req.content_type.as_deref() {
        validate_upload_content_type_policy(content_type, config)?;
    }

    if let (Some(checksum), Some(upload)) = (req.checksum.as_deref(), staged_upload.as_ref()) {
        verify_upload_checksum(checksum, &upload.sha256)?;
    }

    Ok(PreparedIngestRequest {
        request: req,
        staged_upload,
    })
}

async fn stage_multipart_upload(
    mut field: Field<'_>,
    max_upload_bytes: usize,
) -> Result<StagedUpload, ApiError> {
    let path =
        std::env::temp_dir().join(format!("nowledge-upload-{}", uuid::Uuid::now_v7().simple()));
    let temporary_path = TemporaryUploadPath::new(path);
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(temporary_path.path())
        .await
        .map_err(|err| ApiError::Internal(format!("failed to create temporary upload: {err}")))?;
    let mut byte_len = 0_u64;
    let max_upload_bytes = u64::try_from(max_upload_bytes).unwrap_or(u64::MAX);
    let mut hasher = Sha256::new();

    while let Some(chunk) = field.chunk().await.map_err(map_multipart_error)? {
        let next_len = byte_len
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(ApiError::payload_too_large)?;
        if next_len > max_upload_bytes {
            return Err(ApiError::payload_too_large());
        }
        file.write_all(&chunk).await.map_err(|err| {
            ApiError::Internal(format!("failed to write temporary upload: {err}"))
        })?;
        hasher.update(&chunk);
        byte_len = next_len;
    }
    file.flush()
        .await
        .map_err(|err| ApiError::Internal(format!("failed to flush temporary upload: {err}")))?;
    drop(file);

    if byte_len == 0 {
        return Err(ApiError::validation("file", "must not be empty"));
    }
    Ok(temporary_path.into_staged(byte_len, hex::encode(hasher.finalize())))
}

async fn read_multipart_metadata_field(
    mut field: Field<'_>,
    name: &str,
    metadata_bytes: &mut usize,
    max_json_bytes: usize,
) -> Result<String, ApiError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field.chunk().await.map_err(map_multipart_error)? {
        let next_total = metadata_bytes
            .checked_add(chunk.len())
            .ok_or_else(ApiError::payload_too_large)?;
        if next_total > max_json_bytes {
            return Err(ApiError::payload_too_large());
        }
        *metadata_bytes = next_total;
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|_| {
        ApiError::validation(
            if name.is_empty() { "multipart" } else { name },
            "must be valid UTF-8",
        )
    })
}

fn map_multipart_error(err: MultipartError) -> ApiError {
    if err.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large()
    } else {
        ApiError::bad_request("invalid multipart body")
    }
}

fn sanitize_upload_filename(value: &str) -> String {
    let leaf = value.rsplit(['/', '\\']).next().unwrap_or_default();
    let mut sanitized = leaf
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>()
        .trim()
        .to_string();
    while sanitized.len() > 255 {
        sanitized.pop();
    }
    if sanitized.is_empty() || matches!(sanitized.as_str(), "." | "..") {
        "upload.bin".to_string()
    } else {
        sanitized
    }
}

fn verify_upload_checksum(expected: &str, actual: &str) -> Result<(), ApiError> {
    let expected = expected.trim();
    if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::validation(
            "checksum",
            "must be exactly 64 hexadecimal SHA-256 characters",
        ));
    }
    if !expected.eq_ignore_ascii_case(actual) {
        return Err(ApiError::validation(
            "checksum",
            "does not match the uploaded file",
        ));
    }
    Ok(())
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
        "file_name" => req.file_name = non_empty(sanitize_upload_filename(&value)),
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
        .map_err(|_| ApiError::validation("content_type", "must be a valid MIME type"))?;
    Ok(value.trim().to_ascii_lowercase())
}

fn validate_upload_content_type_policy(value: &str, config: &Config) -> Result<(), ApiError> {
    if config
        .upload_allowed_mime_types
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(value))
    {
        Ok(())
    } else {
        Err(ApiError::validation(
            "content_type",
            "is not allowed by RAG_UPLOAD_ALLOWED_MIME_TYPES",
        ))
    }
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
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    let answer = answer_rag_with_llm(&state, req, user.principal.is_admin()).await?;
    let answer =
        serde_json::to_value(answer).map_err(|error| ApiError::Internal(error.to_string()))?;
    Ok(Json(redact_for_state(&state, answer)))
}

async fn rag_stream(
    user: UserGuard,
    state: State<AppState>,
    req: Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    rag_answer(user, state, req).await
}

async fn rag_debug(
    admin: AdminGuard,
    State(state): State<AppState>,
    Json(mut req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    admin
        .principal
        .apply_owner_default(&mut req.owner_user_id)?;
    let answer = answer_rag_with_llm(&state, req.clone(), true).await?;
    let trace = state
        .store
        .get_trace_async(state.tenant_id(), &answer.trace_id)
        .await?;
    Ok(Json(redact_for_state(
        &state,
        json!({
            "answer": answer,
            "trace": trace,
            "prompt": build_prompt(
                &req.question.unwrap_or_default(),
                &answer.citations,
                &known_secrets_for_state(&state),
            )
        }),
    )))
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

async fn llm_status(_user: UserGuard, State(state): State<AppState>) -> Json<LlmStatusResponse> {
    let config = state.effective_config();
    let status = llm_client_from_config(&config).status().await;
    Json(LlmStatusResponse {
        auth_source: sanitized_llm_auth_source(&status.provider, &status.auth_source),
        provider: status.provider,
        model: status.model,
        healthy: status.healthy,
    })
}

fn sanitized_llm_auth_source(provider: &str, auth_source: &str) -> String {
    match provider {
        "none" => "none",
        "mock" => "mock",
        "codex_auth" if auth_source == "explicit_path_missing" => "missing",
        "codex_auth" => "codex_file",
        _ if auth_source.is_empty() => "missing",
        _ => "environment",
    }
    .to_string()
}

async fn llm_test(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<LlmTestRequest>,
) -> Result<Json<Value>, ApiError> {
    let config = state.effective_config();
    let security = state.config.provider_security_snapshot();
    let client = llm_client_from_config_with_credentials(&config, security.credentials);
    let status = client.status().await;
    let response = client
        .complete_text(LlmRequest {
            prompt: redact_egress_text(
                &req.prompt.unwrap_or_else(|| "ping".to_string()),
                &security.secrets,
            ),
        })
        .await?;
    let response = LlmTestResponse {
        ok: true,
        model: status.model,
        latency_ms: response.latency_ms,
        usage: response.usage,
        sample: response.text,
    };
    let response =
        serde_json::to_value(response).map_err(|error| ApiError::Internal(error.to_string()))?;
    Ok(Json(redact_for_state(&state, response)))
}

/// Summarize `content` into a short title via the configured LLM. Available
/// to any authenticated user (UserGuard); the LLM call is governed by the
/// service-level config the same way RAG answers are.
async fn llm_title(
    _user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<LlmTitleRequest>,
) -> Result<Json<Value>, ApiError> {
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

    let config = state.effective_config();
    let security = state.config.provider_security_snapshot();
    let client = llm_client_from_config_with_credentials(&config, security.credentials.clone());

    // Redact before truncating so a credential crossing the 2,000-character
    // boundary cannot be sent upstream as an unrecognizable partial value.
    let truncated = redact_egress_text(content, &security.secrets)
        .chars()
        .take(2_000)
        .collect::<String>();

    let prompt = redact_egress_text(
        &format!(
            "You are a precise editor. Produce a single concise title{language_hint} \
that captures the main topic of the document below. Constraints: max {max_chars} \
characters; no surrounding quotes; no trailing period; no leading numbering or \
emoji; do NOT wrap in markdown. Return ONLY the title text on one line.{hint_line}\n\n\
Document:\n{truncated}"
        ),
        &security.secrets,
    );

    let status = client.status().await;
    let response = client.complete_text(LlmRequest { prompt }).await?;

    // Clean up common LLM artifacts: surrounding quotes, leading "Title:",
    // trailing punctuation, newlines, and enforce the soft length cap.
    let safe_response = redact_text_for_state(&state, &response.text);
    let mut title = safe_response.trim().to_string();
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

    let response = LlmTitleResponse {
        title,
        model: status.model,
        latency_ms: response.latency_ms,
        usage: response.usage,
    };
    let response =
        serde_json::to_value(response).map_err(|error| ApiError::Internal(error.to_string()))?;
    Ok(Json(redact_for_state(&state, response)))
}

async fn get_trace(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let trace = state
        .store
        .get_trace_async(state.tenant_id(), &trace_id)
        .await?;
    let trace =
        serde_json::to_value(trace).map_err(|error| ApiError::Internal(error.to_string()))?;
    Ok(Json(redact_for_state(&state, trace)))
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
    validate_meili_uid(&index_uid)
        .map_err(|_| ApiError::bad_request("index_uid contains invalid characters"))?;
    let query = req.get("query").and_then(Value::as_str).unwrap_or("");
    let raw = state
        .store
        .debug_meili_search_async(state.tenant_id(), &index_uid, query)
        .await?;
    Ok(Json(redact_for_state(&state, raw)))
}

async fn prompt_preview(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    let answer = answer_rag_with_llm(&state, req.clone(), true).await?;
    let prompt = build_prompt(
        &req.question.unwrap_or_default(),
        &answer.citations,
        &known_secrets_for_state(&state),
    );
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
        let security = state.config.provider_security_snapshot();
        let client = llm_client_from_config_with_credentials(&config, security.credentials.clone());
        let status = client.status().await;
        let prompt = build_prompt(
            &req.question.unwrap_or_default(),
            &answer.citations,
            &security.secrets,
        );
        let llm = client.complete_text(LlmRequest { prompt }).await?;
        answer.answer = redact_text_for_state(state, &llm.text);
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
                        limit: req.context_limit.max(2).min(state.config.max_search_limit),
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

    let analysis_config = state.config.analysis_llm_config();
    let security = state.config.provider_security_snapshot();
    let client =
        llm_client_from_config_with_credentials(&analysis_config, security.credentials.clone());
    let known_secrets = security.secrets;
    let prompt = build_analysis_prompt(
        &query,
        &context_hits,
        &existing_links,
        &seed_uris,
        &known_secrets,
    );
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

    let allowed_uris =
        analysis_allowed_uris(&context_hits, &existing_links, &seed_uris, &known_secrets);
    let mut draft = sanitize_analysis_draft(
        deterministic_analysis_draft(&query, &context_hits),
        &allowed_uris,
        &known_secrets,
    );
    if analysis_config.llm_provider != "none" {
        let llm = client
            .complete_text(LlmRequest {
                prompt: prompt.clone(),
            })
            .await?;
        if let Some(parsed) = parse_analysis_draft(&llm.text) {
            let parsed = sanitize_analysis_draft(parsed, &allowed_uris, &known_secrets);
            draft = merge_analysis_drafts(parsed, draft);
        }
        usage["latency_ms"] = json!(llm.latency_ms);
        if req.debug {
            usage["raw_response_preview"] = json!(truncate_for_json(
                &redact_text_for_state(state, &llm.text),
                500
            ));
        }
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
                limit: context_limit.max(2).min(state.config.max_search_limit),
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
        .map(|event| history_event_context_hit(state, event, query))
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
                limit: link_limit.max(1).min(state.config.max_search_limit),
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

fn history_event_context_hit(state: &AppState, event: &HistoryEvent, query: &str) -> ContextHit {
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
        snippet: redact_and_truncate_text_for_state(state, &event.text, 240),
    }
}

fn build_prompt(question: &str, citations: &[Citation], known_secrets: &[String]) -> String {
    let context = citations
        .iter()
        .enumerate()
        .map(|(idx, citation)| {
            let source_title = redact_egress_text(
                citation
                    .source_title
                    .as_deref()
                    .unwrap_or(citation.title.as_str()),
                known_secrets,
            );
            let mut location = Vec::new();
            if let Some(page_idx) = citation.page_idx {
                location.push(format!("page_idx={page_idx}"));
            }
            if let Some(block_type) = citation.block_type.as_deref() {
                location.push(format!(
                    "block_type={}",
                    redact_string(block_type, known_secrets)
                ));
            }
            if !citation.section_path.is_empty() {
                location.push(format!(
                    "section_path={}",
                    citation
                        .section_path
                        .iter()
                        .map(|part| redact_egress_text(part, known_secrets))
                        .collect::<Vec<_>>()
                        .join(" > ")
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
                redact_locator(&citation.uri, known_secrets),
                redact_egress_text(&citation.quote, known_secrets)
            )
        })
        .collect::<Vec<_>>()
        .join("\n\n");
    format!(
        "Question:\n{}\n\nContextFS staged context:\n{context}",
        redact_egress_text(question, known_secrets)
    )
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
    known_secrets: &[String],
) -> String {
    let context = hits
        .iter()
        .map(|hit| {
            format!(
                "- uri: {}\n  title: {}\n  snippet: {}",
                redact_locator(&hit.uri, known_secrets),
                redact_egress_text(&hit.title, known_secrets),
                redact_egress_text(&hit.snippet, known_secrets)
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    let existing_links = links
        .iter()
        .map(|link| {
            format!(
                "- {} --{}--> {} ({})",
                redact_locator(&link.source_uri, known_secrets),
                redact_string(&link.relation, known_secrets),
                redact_locator(&link.target_uri, known_secrets),
                redact_egress_text(
                    link.rationale.as_deref().unwrap_or("no rationale"),
                    known_secrets,
                )
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
    Existing links:\n{existing_links}",
        query = redact_egress_text(query, known_secrets),
        seed_uris = seed_uris
            .iter()
            .map(|uri| redact_locator(uri, known_secrets))
            .collect::<Vec<_>>(),
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

fn analysis_allowed_uris(
    hits: &[ContextHit],
    links: &[KnowledgeLink],
    seed_uris: &[String],
    known_secrets: &[String],
) -> HashSet<String> {
    hits.iter()
        .map(|hit| hit.uri.as_str())
        .chain(seed_uris.iter().map(String::as_str))
        .chain(
            links
                .iter()
                .flat_map(|link| [link.source_uri.as_str(), link.target_uri.as_str()].into_iter()),
        )
        .map(canonical_analysis_uri)
        .filter(|uri| redact_locator(uri, known_secrets) == *uri)
        .collect()
}

fn sanitize_analysis_draft(
    draft: AnalysisDraft,
    allowed_uris: &HashSet<String>,
    known_secrets: &[String],
) -> AnalysisDraft {
    let links = draft
        .links
        .into_iter()
        .filter_map(|candidate| {
            let source_uri = canonical_analysis_uri(&candidate.source_uri);
            let target_uri = canonical_analysis_uri(&candidate.target_uri);
            if !allowed_uris.contains(&source_uri) || !allowed_uris.contains(&target_uri) {
                return None;
            }
            Some(LinkCandidate {
                source_uri,
                target_uri,
                relation: redact_string(&candidate.relation, known_secrets),
                rationale: candidate
                    .rationale
                    .as_deref()
                    .map(|value| redact_egress_text(value, known_secrets)),
                confidence: candidate.confidence,
            })
        })
        .collect();
    let insights = draft
        .insights
        .into_iter()
        .map(|candidate| InsightCandidate {
            insight_type: redact_string(&candidate.insight_type, known_secrets),
            title: redact_egress_text(&candidate.title, known_secrets),
            statement: redact_egress_text(&candidate.statement, known_secrets),
            confidence: candidate.confidence,
            salience: candidate.salience,
            source_uris: candidate
                .source_uris
                .into_iter()
                .map(|uri| canonical_analysis_uri(&uri))
                .filter(|uri| allowed_uris.contains(uri))
                .collect(),
        })
        .collect();
    AnalysisDraft { links, insights }
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

fn audit_shared_write<T>(
    result: Result<T, ApiError>,
    principal: &Principal,
    state: &AppState,
    action: &str,
    resource_id: &str,
    reason: &str,
) -> Result<T, ApiError> {
    match &result {
        Ok(_) => emit_shared_mutation_audit(
            Some(principal),
            state,
            action,
            resource_id,
            reason,
            "success",
            None,
        ),
        Err(error) => emit_shared_mutation_audit(
            Some(principal),
            state,
            action,
            resource_id,
            reason,
            "failure",
            Some(api_error_kind(error)),
        ),
    }
    result
}

pub(crate) fn audit_shared_write_denial(
    principal: Option<&Principal>,
    state: &AppState,
    method: &Method,
    path: &str,
    reason: &str,
    error: &ApiError,
) {
    let Some((action, resource_id)) = shared_mutation_audit_target(method, path) else {
        return;
    };
    emit_shared_mutation_audit(
        principal,
        state,
        action,
        &resource_id,
        reason,
        "denied",
        Some(api_error_kind(error)),
    );
}

fn shared_mutation_audit_target(method: &Method, path: &str) -> Option<(&'static str, String)> {
    let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
    match (method, segments.as_slice()) {
        (&Method::POST, ["v1", "state", "company-docs", "preflight"]) => {
            Some(("company_doc.preflight", "company-doc:preflight".to_string()))
        }
        (&Method::POST, ["v1", "state", "company-docs", source_id, "revisions"]) => {
            Some(("company_doc.create_revision", (*source_id).to_string()))
        }
        (
            &Method::POST,
            ["v1", "state", "company-docs", source_id, "revisions", revision_id, "activate"],
        ) => Some((
            "company_doc.activate_revision",
            format!("{source_id}:{revision_id}"),
        )),
        (&Method::PUT, ["v1", "state", "structured", "datasets", dataset_key]) => {
            Some(("dataset.upsert_schema", (*dataset_key).to_string()))
        }
        (&Method::DELETE, ["v1", "state", "company-docs", source_id]) => {
            Some(("company_doc.delete", (*source_id).to_string()))
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_shared_mutation_audit(
    principal: Option<&Principal>,
    state: &AppState,
    action: &str,
    resource_id: &str,
    reason: &str,
    outcome: &str,
    error_kind: Option<&str>,
) {
    let request_id = request_context::current_or_new_id();
    let resource_id = audit_identifier(&state.config, "resource", resource_id);
    let tenant_id = audit_identifier(&state.config, "tenant", state.tenant_id());
    let principal_scope = principal
        .map(Principal::scope_label)
        .unwrap_or("unauthenticated");
    let owner_user_id = principal
        .and_then(Principal::owner_user_id)
        .map(|owner| audit_identifier(&state.config, "principal-owner", owner))
        .unwrap_or_else(|| "none".to_string());
    let (reason_code, reason_fingerprint) = audit_reason(&state.config, reason);
    if outcome == "success" {
        tracing::info!(
            target: "nowledge::audit",
            %request_id,
            %tenant_id,
            principal_scope,
            principal_owner_user_id = %owner_user_id,
            action,
            %resource_id,
            reason = reason_code,
            %reason_fingerprint,
            outcome,
            "shared knowledge mutation"
        );
    } else {
        tracing::warn!(
            target: "nowledge::audit",
            %request_id,
            %tenant_id,
            principal_scope,
            principal_owner_user_id = %owner_user_id,
            action,
            %resource_id,
            reason = reason_code,
            %reason_fingerprint,
            outcome,
            error_kind = error_kind.unwrap_or("unknown"),
            "shared knowledge mutation"
        );
    }
}

fn audit_identifier(config: &Config, namespace: &str, value: &str) -> String {
    format!(
        "hmac:{}",
        hmac_hex(&config.index_hash_secret, namespace, value, 16)
    )
}

fn audit_reason(config: &Config, reason: &str) -> (&'static str, String) {
    let reason_code = match reason {
        "authentication_failed" => "authentication_failed",
        "company_writer_required" => "company_writer_required",
        "admin_required" => "admin_required",
        "preflight_requested" => "preflight_requested",
        "revision_create_requested" => "revision_create_requested",
        "activation_reason_unspecified" => "activation_reason_unspecified",
        "admin_delete" => "admin_delete",
        "schema_upsert" => "schema_upsert",
        _ => "caller_supplied",
    };
    let reason_fingerprint = format!(
        "hmac:{}",
        hmac_hex(&config.index_hash_secret, "audit-reason", reason, 16,)
    );
    (reason_code, reason_fingerprint)
}

fn api_error_kind(error: &ApiError) -> &'static str {
    match error {
        ApiError::BadRequest(_) => "bad_request",
        ApiError::Validation { .. } => "validation_error",
        ApiError::Unauthorized(_) => "unauthorized",
        ApiError::Forbidden(_) => "forbidden",
        ApiError::NotFound(_) => "not_found",
        ApiError::Conflict(_) => "conflict",
        ApiError::PayloadTooLarge => "payload_too_large",
        ApiError::TooManyRequests(_) => "too_many_requests",
        ApiError::ServiceUnavailable(_) => "service_unavailable",
        ApiError::Timeout => "timeout",
        ApiError::Upstream(_) => "upstream_error",
        ApiError::Internal(_) => "internal_error",
    }
}

fn redact_for_state(state: &AppState, value: Value) -> Value {
    redact_secrets(&value, &known_secrets_for_state(state))
}

fn redact_text_for_state(state: &AppState, value: &str) -> String {
    redact_egress_text(value, &known_secrets_for_state(state))
}

fn redact_and_truncate_text_for_state(state: &AppState, value: &str, max: usize) -> String {
    redact_text_for_state(state, value)
        .chars()
        .take(max)
        .collect()
}

fn known_secrets_for_state(state: &AppState) -> Vec<String> {
    state.config.configured_secret_values()
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

    #[test]
    fn rag_prompt_sanitizes_secret_projections_in_each_citation_field() {
        let secret = "zxqv-provider-prompt-secret-private-value".to_string();
        let left = &secret[..12];
        let middle = &secret[12..27];
        let right = &secret[27..];
        let citation: Citation = serde_json::from_value(json!({
            "uri": "ctx://document/stable-source",
            "source_title": left,
            "title": left,
            "quote": right,
            "score": 1.0,
            "section_path": [middle]
        }))
        .unwrap();

        let prompt = build_prompt(left, &[citation], std::slice::from_ref(&secret));

        assert!(!prompt.contains(left), "{prompt}");
        assert!(!prompt.contains(middle), "{prompt}");
        assert!(!prompt.contains(right), "{prompt}");
        assert!(!prompt.contains(&secret), "{prompt}");
    }

    #[test]
    fn rag_prompt_preserves_short_words_that_overlap_human_readable_test_tokens() {
        let known_secrets = vec!["owner-u1-token".to_string()];
        let citation: Citation = serde_json::from_value(json!({
            "uri": "ctx://document/owner-guide",
            "title": "owner",
            "quote": "owner guidance",
            "score": 1.0
        }))
        .unwrap();

        let prompt = build_prompt("owner", &[citation], &known_secrets);

        assert!(prompt.contains("Question:\nowner"), "{prompt}");
        assert!(prompt.contains("owner guidance"), "{prompt}");
    }

    #[test]
    fn provider_prompts_preserve_locators_with_incidental_secret_windows() {
        let known_secrets = vec!["old-token-with-boundary-private-value".to_string()];
        let uri = "ctx://docs/snippet-boundary-source";
        let citation: Citation = serde_json::from_value(json!({
            "uri": uri,
            "title": "Stable locator",
            "quote": "ordinary context",
            "score": 1.0
        }))
        .unwrap();
        let hit: ContextHit = serde_json::from_value(json!({
            "uri": uri,
            "title": "Stable locator",
            "layer": 2,
            "score": 1.0,
            "snippet": "ordinary context"
        }))
        .unwrap();

        let rag_prompt = build_prompt("question", &[citation], &known_secrets);
        let analysis_prompt =
            build_analysis_prompt("query", &[hit], &[], &[uri.to_string()], &known_secrets);

        assert!(rag_prompt.contains(uri), "{rag_prompt}");
        assert!(
            analysis_prompt.matches(uri).count() >= 2,
            "{analysis_prompt}"
        );
    }

    #[test]
    fn analysis_prompt_sanitizes_fields_without_corrupting_structural_enums() {
        let secret = "zxqv-analysis-prompt-secret-private-value".to_string();
        let enum_secret = "related-service-token".to_string();
        let left = &secret[..13];
        let middle = &secret[13..28];
        let right = &secret[28..];
        let hit: ContextHit = serde_json::from_value(json!({
            "uri": "ctx://document/stable-source",
            "title": left,
            "layer": 2,
            "score": 1.0,
            "snippet": right
        }))
        .unwrap();
        let link: KnowledgeLink = serde_json::from_value(json!({
            "id": "link-test",
            "tenant_id": "test-tenant",
            "owner_user_id": "u1",
            "source_uri": "ctx://source/stable-left",
            "target_uri": "ctx://target/stable-right",
            "relation": "related",
            "rationale": format!("{middle} {right}"),
            "confidence": 1.0,
            "created_by": "test",
            "status": "active",
            "tags": [],
            "created_at": "2026-07-13T00:00:00Z",
            "updated_at": "2026-07-13T00:00:00Z"
        }))
        .unwrap();

        let prompt = build_analysis_prompt(
            left,
            &[hit],
            &[link],
            &["ctx://seed/stable".to_string()],
            &[secret.clone(), enum_secret],
        );

        assert!(prompt.contains("--related-->"), "{prompt}");
        assert!(!prompt.contains(left), "{prompt}");
        assert!(!prompt.contains(middle), "{prompt}");
        assert!(!prompt.contains(right), "{prompt}");
        assert!(!prompt.contains(&secret), "{prompt}");
    }

    #[test]
    fn analysis_model_output_preserves_allowed_locators_and_rejects_unknown_ones() {
        let known_secrets = vec!["old-token-with-boundary-private-value".to_string()];
        let allowed = "ctx://docs/snippet-boundary-source".to_string();
        let unknown = "ctx://docs/model-invented-source";
        let raw = json!({
            "links": [
                {
                    "source_uri": allowed,
                    "target_uri": "ctx://docs/second-source",
                    "relation": "related",
                    "rationale": "ordinary rationale",
                    "confidence": 0.8
                },
                {
                    "source_uri": allowed,
                    "target_uri": unknown,
                    "relation": "related",
                    "confidence": 0.5
                }
            ],
            "insights": [{
                "insight_type": "analysis",
                "title": "Stable result",
                "statement": "Grounded statement",
                "source_uris": [allowed, unknown]
            }]
        })
        .to_string();
        let parsed = parse_analysis_draft(&raw).unwrap();
        let allowed_uris = HashSet::from([allowed.clone(), "ctx://docs/second-source".to_string()]);

        let sanitized = sanitize_analysis_draft(parsed, &allowed_uris, &known_secrets);

        assert_eq!(sanitized.links.len(), 1);
        assert_eq!(sanitized.links[0].source_uri, allowed);
        assert_eq!(sanitized.links[0].target_uri, "ctx://docs/second-source");
        assert_eq!(sanitized.insights[0].source_uris, vec![allowed]);
    }

    #[test]
    fn rejected_model_links_do_not_discard_grounded_deterministic_fallbacks() {
        let known_secrets = Vec::new();
        let allowed_uris = HashSet::from([
            "ctx://docs/first".to_string(),
            "ctx://docs/second".to_string(),
        ]);
        let fallback = AnalysisDraft {
            links: vec![LinkCandidate {
                source_uri: "ctx://docs/first".to_string(),
                target_uri: "ctx://docs/second".to_string(),
                relation: "related".to_string(),
                rationale: None,
                confidence: 0.6,
            }],
            insights: Vec::new(),
        };
        let ungrounded_model = AnalysisDraft {
            links: vec![LinkCandidate {
                source_uri: "ctx://model/unknown-one".to_string(),
                target_uri: "ctx://model/unknown-two".to_string(),
                relation: "related".to_string(),
                rationale: None,
                confidence: 0.9,
            }],
            insights: Vec::new(),
        };

        let fallback = sanitize_analysis_draft(fallback, &allowed_uris, &known_secrets);
        let model = sanitize_analysis_draft(ungrounded_model, &allowed_uris, &known_secrets);
        let merged = merge_analysis_drafts(model, fallback);

        assert_eq!(merged.links.len(), 1);
        assert_eq!(merged.links[0].source_uri, "ctx://docs/first");
        assert_eq!(merged.links[0].target_uri, "ctx://docs/second");
    }

    #[tokio::test]
    async fn llm_title_redacts_configured_secrets_before_prompt_truncation() {
        let secret = "private-boundary-secret-value";
        let mut config = Config::test();
        config.admin_token = Some(secret.to_string());
        let state = AppState::new(Arc::new(config));
        let content = format!("{}{secret}", "x".repeat(1_992));

        let truncated = redact_and_truncate_text_for_state(&state, &content, 2_000);

        assert_eq!(truncated.chars().count(), 2_000);
        assert!(!truncated.contains("private-"));
        assert!(!truncated.contains(secret));
    }

    #[test]
    fn audit_identifiers_and_caller_reasons_are_never_logged_raw() {
        let config = Config::test();
        let raw_identifier = "tenant/private-owner/source-id";
        let identifier = audit_identifier(&config, "resource", raw_identifier);
        assert!(identifier.starts_with("hmac:"));
        assert!(!identifier.contains(raw_identifier));

        let raw_reason = "activate because /private/auth.json contains a provider token";
        let (reason_code, reason_fingerprint) = audit_reason(&config, raw_reason);
        assert_eq!(reason_code, "caller_supplied");
        assert!(reason_fingerprint.starts_with("hmac:"));
        assert!(!reason_fingerprint.contains(raw_reason));

        let (system_code, _) = audit_reason(&config, "company_writer_required");
        assert_eq!(system_code, "company_writer_required");
    }
}
