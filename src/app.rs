use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::extract::FromRef;
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};

use crate::{
    audit_service::AuditRecorder,
    config::Config,
    error::{safe_cause_diagnostic, safe_value_fingerprint, ApiError},
    http_boundary::{HttpBoundaryState, RequestDeadline},
    llm::{LlmHealthProbe, LlmProviderRegistry},
    meili::MeiliAdmin,
    metrics::{IngestRuntimeMetrics, Metrics},
    models::{IngestTask, IngestTaskRequest, IngestTaskResult},
    parser::StagedUpload,
    runtime::RuntimeSupervisor,
    store::Store,
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

    pub(crate) fn ensure_available(&self) -> Result<(), ApiError> {
        if !self.enabled {
            return Err(ApiError::service_unavailable(1));
        }
        if !self.accepting.load(Ordering::Acquire) {
            return Err(ApiError::service_unavailable(1));
        }
        Ok(())
    }

    pub(crate) async fn submit(
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

    pub(crate) fn metrics(&self) -> IngestRuntimeMetrics {
        IngestRuntimeMetrics {
            queue_depth: self.queued_depth.load(Ordering::Acquire),
            accepting: self.accepting.load(Ordering::Acquire),
        }
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
    queue.close();
    while let Some(mut job) = queue.recv().await {
        job.queue_depth.take();
        match tokio::time::timeout_at(
            deadline,
            store.mark_ingest_task_interrupted_async(&job.task_id),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                log_ingest_failure(&job.task_id, &err, "failed to interrupt queued ingest task")
            }
            Err(_) => break,
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
        match tokio::time::timeout_at(deadline, store.mark_ingest_task_interrupted_async(&task_id))
            .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                log_ingest_failure(&task_id, &err, "failed to interrupt active ingest task")
            }
            Err(_) => break,
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

pub(crate) fn log_ingest_failure(task_id: &str, err: &ApiError, message: &'static str) {
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
    pub(crate) runtime_meili: MeiliAdmin,
    pub meili: MeiliAdmin,
    pub llm_health: LlmHealthProbe,
    pub analysis_llm_health: LlmHealthProbe,
    pub llm_providers: LlmProviderRegistry,
    pub ingest_manager: IngestTaskManager,
    pub(crate) metrics: Metrics,
    pub(crate) audit_recorder: AuditRecorder,
    pub(crate) http_boundary: HttpBoundaryState,
    pub(crate) runtime: RuntimeSupervisor,
}

#[derive(Clone)]
pub(crate) struct AuthState {
    config: Arc<Config>,
    http_boundary: HttpBoundaryState,
    audit_recorder: AuditRecorder,
}

impl AuthState {
    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn http_boundary(&self) -> &HttpBoundaryState {
        &self.http_boundary
    }

    pub(crate) fn audit_recorder(&self) -> &AuditRecorder {
        &self.audit_recorder
    }
}

impl FromRef<AppState> for AuthState {
    fn from_ref(state: &AppState) -> Self {
        Self {
            config: state.config.clone(),
            http_boundary: state.http_boundary.clone(),
            audit_recorder: state.audit_recorder.clone(),
        }
    }
}

impl AppState {
    pub fn new(config: Arc<Config>) -> Self {
        let (runtime_meili, index_admin) = MeiliAdmin::pair_from_config(&config);
        let metrics = Metrics::new();
        let store = Store::new_with_meili_admins_and_metrics(
            &config,
            runtime_meili.clone(),
            index_admin.clone(),
            metrics.clone(),
        );
        let runtime = RuntimeSupervisor::new();
        let audit_recorder = AuditRecorder::new(
            config.clone(),
            store.clone(),
            runtime.clone(),
            metrics.clone(),
        );
        let http_boundary = HttpBoundaryState::new(&config);
        let llm_providers = LlmProviderRegistry::new_with_metrics(config.clone(), metrics.clone());
        config.start_codex_secret_refresh_task();
        let ingest_manager = IngestTaskManager::new(store.clone(), config.clone(), runtime.clone());
        spawn_ingest_task_cleanup(store.clone(), &config, &runtime);
        Self {
            store,
            runtime_meili,
            meili: index_admin,
            llm_health: LlmHealthProbe::new(),
            analysis_llm_health: LlmHealthProbe::new(),
            llm_providers,
            ingest_manager,
            metrics,
            audit_recorder,
            http_boundary,
            runtime,
            config,
        }
    }

    pub fn tenant_id(&self) -> &str {
        &self.config.tenant_id
    }

    pub(crate) fn effective_config(&self) -> Config {
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
        match tokio::time::timeout_at(
            deadline,
            self.store
                .interrupt_nonterminal_ingest_tasks_async(self.tenant_id()),
        )
        .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(err)) => {
                let diagnostic = safe_cause_diagnostic(&err);
                tracing::warn!(
                    cause_category = diagnostic.category,
                    cause_fingerprint = %diagnostic.fingerprint,
                    "failed to finalize interrupted ingest tasks during shutdown"
                );
            }
            Err(_) => tracing::warn!(
                "shutdown deadline elapsed before interrupted task states were journaled"
            ),
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
