use std::{
    collections::HashMap,
    future::{Future, IntoFuture},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use anyhow::Context as _;
use axum::{extract::FromRef, Router};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, watch},
    task::JoinSet,
};

use crate::{
    audit_service::AuditRecorder,
    auth::CredentialIndex,
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
    /// Stops the dispatcher when ingest admission closes. This is separate
    /// from the runtime shutdown signal so ingest winds down at the start of
    /// the HTTP drain while the runtime still accepts audit/trace work.
    drain: Arc<watch::Sender<bool>>,
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
        let (drain, shutdown) = watch::channel(false);
        let drain = Arc::new(drain);
        if !config.ingest_worker_enabled {
            return Self {
                queue: Arc::new(Mutex::new(None)),
                queued_depth,
                accepting,
                enabled: false,
                runtime,
                drain,
            };
        }

        let (tx, rx) = mpsc::channel::<QueuedIngestJob>(config.ingest_queue_capacity.max(1));
        let max_concurrent = config.ingest_max_concurrent_tasks.max(1);
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
            drain,
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
        self.drain.send_replace(true);
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
    pub(crate) credentials: Arc<CredentialIndex>,
}

#[derive(Clone)]
pub(crate) struct AuthState {
    config: Arc<Config>,
    http_boundary: HttpBoundaryState,
    audit_recorder: AuditRecorder,
    credentials: Arc<CredentialIndex>,
}

impl AuthState {
    pub(crate) fn config(&self) -> &Config {
        &self.config
    }

    pub(crate) fn credentials(&self) -> &CredentialIndex {
        &self.credentials
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
            credentials: state.credentials.clone(),
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
        spawn_trace_persister(store.clone(), &runtime);
        spawn_trace_retention(store.clone(), &config, &runtime);
        let credentials = Arc::new(CredentialIndex::from_config(&config));
        Self {
            credentials,
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

    /// Gives `/readyz`, which only reads the LLM health cache, a verdict to
    /// report: probes every configured LLM profile once in the background
    /// and then stops. Only an opt-in `RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS`
    /// keeps it probing every interval until the background runtime shuts
    /// down; otherwise later probes come only from live checks such as
    /// `/healthz`, so an idle service spends no provider quota on health.
    /// The binary starts it after building the state; [`AppState::new`]
    /// does not, so tests and embedders never make provider calls they did
    /// not ask for. Returns whether it was started (not when health probing
    /// is disabled).
    pub fn start_llm_health_refresh(&self) -> bool {
        if !self.config.health_llm_enabled {
            return false;
        }
        let shutdown = self.runtime.subscribe();
        self.runtime
            .spawn(run_llm_health_refresh(self.clone(), shutdown))
    }

    /// First shutdown phase: stop ingest admission and dispatch while
    /// in-flight HTTP requests drain. The background runtime stays open so
    /// the audit finalizations and traces of draining requests are still
    /// admitted; [`AppState::shutdown_until`] closes and flushes it afterwards.
    pub fn begin_http_drain(&self) {
        self.ingest_manager.begin_shutdown();
    }

    pub fn begin_shutdown(&self) {
        self.begin_http_drain();
        self.runtime.begin_shutdown();
    }

    /// Serves `app` until `signal` resolves, then shuts down in dependency
    /// order within one `RAG_SHUTDOWN_TIMEOUT_MS` deadline: ingest admission
    /// stops and in-flight HTTP requests drain first, and only after the
    /// drain (or its deadline) is the background runtime closed and flushed.
    pub async fn serve_until_shutdown<S>(
        &self,
        listener: TcpListener,
        app: Router,
        signal: S,
    ) -> anyhow::Result<()>
    where
        S: Future<Output = ()> + Send + 'static,
    {
        let shutdown_grace = Duration::from_millis(self.config.shutdown_timeout_ms);
        let (shutdown_started, shutdown_observed) = oneshot::channel();
        let draining = self.clone();
        let server = axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                signal.await;
                tracing::info!("shutdown signal received; draining requests and ingest tasks");
                let deadline = tokio::time::Instant::now() + shutdown_grace;
                draining.begin_http_drain();
                let _ = shutdown_started.send(deadline);
            })
            .into_future();
        tokio::pin!(server);
        let (result, shutdown_deadline) = tokio::select! {
            result = &mut server => (result.context("server failed"), None),
            observed = shutdown_observed => {
                let deadline = observed.unwrap_or_else(|_| tokio::time::Instant::now());
                let result = match tokio::time::timeout_at(deadline, &mut server).await {
                    Ok(result) => result.context("server failed"),
                    Err(_) => {
                        tracing::warn!("HTTP drain exceeded shutdown deadline; forcing close");
                        Ok(())
                    }
                };
                (result, Some(deadline))
            }
        };
        if let Some(deadline) = shutdown_deadline {
            self.shutdown_until(deadline).await;
        } else {
            self.shutdown().await;
        }
        result
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

/// The startup LLM health probe, repeated every opt-in
/// `RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS` until runtime shutdown. With the
/// default interval of 0 it returns after the startup probe.
async fn run_llm_health_refresh(state: AppState, mut shutdown: watch::Receiver<bool>) {
    let interval = state.config.health_llm_probe_interval_seconds;
    loop {
        tokio::select! {
            () = crate::health_service::refresh_llm_health(&state) => {}
            () = runtime_shutdown(&mut shutdown) => return,
        }
        if interval == 0 {
            return;
        }
        tokio::select! {
            () = tokio::time::sleep(Duration::from_secs(interval)) => {}
            () = runtime_shutdown(&mut shutdown) => return,
        }
    }
}

/// Resolves once the background runtime has begun shutting down.
async fn runtime_shutdown(shutdown: &mut watch::Receiver<bool>) {
    while !*shutdown.borrow_and_update() {
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

/// First trace retention sweep runs shortly after startup, once the managed
/// index settings (including the `created_at_ms` filter) have been reconciled.
const TRACE_RETENTION_INITIAL_DELAY: Duration = Duration::from_secs(60);
const TRACE_RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Run the batched background trace persister for the life of the runtime.
fn spawn_trace_persister(store: Store, runtime: &RuntimeSupervisor) {
    let shutdown = runtime.subscribe();
    let _ = runtime.spawn(async move { store.run_trace_persister(shutdown).await });
}

/// Periodically delete traces older than `RAG_TRACE_RETENTION_DAYS`
/// (0 keeps traces forever) from the cache and the durable backend.
fn spawn_trace_retention(store: Store, config: &Arc<Config>, runtime: &RuntimeSupervisor) {
    let retention_days = config.trace_retention_days;
    if retention_days == 0 {
        return;
    }
    let tenant_id = config.tenant_id.clone();
    let mut shutdown = runtime.subscribe();
    let _ = runtime.spawn(async move {
        let mut ticker = tokio::time::interval_at(
            tokio::time::Instant::now() + TRACE_RETENTION_INITIAL_DELAY,
            TRACE_RETENTION_INTERVAL,
        );
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
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
                .prune_expired_traces_async(&tenant_id, retention_days)
                .await
            {
                Ok((cached, task_uids)) => {
                    tracing::debug!(
                        cached_pruned = cached,
                        delete_tasks = task_uids.len(),
                        "trace retention sweep submitted"
                    );
                }
                Err(err) => {
                    let diagnostic = safe_cause_diagnostic(&err);
                    tracing::warn!(
                        cause_category = diagnostic.category,
                        cause_fingerprint = %diagnostic.fingerprint,
                        "trace retention sweep failed"
                    );
                }
            }
        }
    });
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
        time::{sleep, timeout},
    };

    use super::*;
    use crate::{
        metrics::StoreMetricsSnapshot,
        models::{AuditAction, AuditPrincipalScope},
    };

    fn rendered_metrics(state: &AppState) -> String {
        state
            .metrics
            .render(
                state.ingest_manager.metrics(),
                &StoreMetricsSnapshot::default(),
            )
            .expect("metrics render")
    }

    async fn wait_until(what: &str, mut condition: impl FnMut() -> bool) {
        timeout(Duration::from_secs(5), async {
            while !condition() {
                sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    #[tokio::test]
    async fn graceful_shutdown_drains_http_before_closing_the_background_runtime() {
        let mut config = Config::test();
        config.shutdown_timeout_ms = 5_000;
        let state = AppState::new(Arc::new(config));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (signal, signalled) = oneshot::channel::<()>();
        let server = tokio::spawn({
            let state = state.clone();
            let app = crate::build_router(state.clone());
            async move {
                state
                    .serve_until_shutdown(listener, app, async {
                        let _ = signalled.await;
                    })
                    .await
            }
        });

        // Hold one request in flight: its headers are complete, its body is not.
        let mut client = TcpStream::connect(addr).await.unwrap();
        client
            .write_all(
                b"POST /v1/context/search HTTP/1.1\r\nHost: localhost\r\n\
                  Connection: close\r\nContent-Type: application/json\r\n\
                  Content-Length: 2\r\n\r\n{",
            )
            .await
            .unwrap();
        wait_until("the request to enter the router", || {
            rendered_metrics(&state).contains("\nnowledge_http_in_flight 1\n")
        })
        .await;

        signal.send(()).unwrap();
        wait_until("ingest admission to close", || {
            !state.ingest_manager.metrics().accepting
        })
        .await;
        assert!(state.ingest_manager.ensure_available().is_err());

        // While HTTP drains, a request finishing its audited mutation must
        // still be able to hand the finalization to the background runtime.
        let result = state
            .audit_recorder
            .record_mutation(
                AuditPrincipalScope::Admin,
                None,
                AuditAction::CompanyDocDelete,
                "draining-resource",
                "admin_delete",
                None,
                || async { Ok::<_, ApiError>(()) },
            )
            .await;
        assert!(result.is_ok(), "{result:?}");
        let flushed = Arc::new(AtomicBool::new(false));
        assert!(
            state.runtime.spawn({
                let flushed = flushed.clone();
                async move {
                    sleep(Duration::from_millis(25)).await;
                    flushed.store(true, Ordering::SeqCst);
                }
            }),
            "the background runtime must stay open during the HTTP drain"
        );
        assert!(
            !server.is_finished(),
            "the in-flight request is still draining"
        );

        client.write_all(b"}").await.unwrap();
        let mut response = Vec::new();
        timeout(Duration::from_secs(5), client.read_to_end(&mut response))
            .await
            .expect("drained response")
            .unwrap();
        assert!(response.starts_with(b"HTTP/1.1 "), "{response:?}");

        timeout(Duration::from_secs(5), server)
            .await
            .expect("server shutdown")
            .unwrap()
            .unwrap();
        assert!(
            flushed.load(Ordering::SeqCst),
            "work admitted during the drain must be flushed before shutdown returns"
        );
        let metrics = rendered_metrics(&state);
        assert!(
            !metrics.contains("reason=\"shutdown\""),
            "no audit write may be dropped for shutdown: {metrics}"
        );
        assert!(
            !state.runtime.spawn(async {}),
            "the background runtime closes once the drain completes"
        );
    }

    /// A Codex Responses stub that answers every health probe with `ok` and
    /// counts the calls.
    async fn spawn_counting_codex_stub() -> (String, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let app = Router::new().route(
            "/backend-api/codex/responses",
            axum::routing::post(move || {
                let counted = counted.clone();
                async move {
                    counted.fetch_add(1, Ordering::SeqCst);
                    let response = serde_json::json!({
                        "status": "completed",
                        "output": [{"content": [{"type": "output_text", "text": "ok"}]}]
                    });
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        format!(
                            "event: response.output_text.done\r\ndata: {}\r\n\r\nevent: response.completed\r\ndata: {}\r\n\r\n",
                            serde_json::json!({"type": "response.output_text.done", "text": "ok"}),
                            serde_json::json!({"type": "response.completed", "response": response})
                        ),
                    )
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind stub");
        let address = listener.local_addr().expect("stub address");
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{address}/backend-api/codex"), calls)
    }

    #[tokio::test]
    async fn default_llm_health_refresh_probes_once_and_stops() {
        // A single LLM profile costs exactly one probe call; a distinct
        // analysis profile adds exactly one probe of its own. Either way the
        // refresher then stops instead of probing again.
        for (analysis_model, expected_probes) in [
            ("refresh-once-model", 1),
            ("refresh-once-analysis-model", 2),
        ] {
            let (codex_base_url, probes) = spawn_counting_codex_stub().await;
            let auth_path = std::env::temp_dir().join(format!(
                "nowledge-refresh-once-{}.json",
                uuid::Uuid::now_v7()
            ));
            std::fs::write(
                &auth_path,
                serde_json::json!({ "access_token": "header.payload.signature" }).to_string(),
            )
            .expect("write codex auth");
            let mut config = Config::test();
            assert_eq!(
                config.health_llm_probe_interval_seconds, 0,
                "periodic LLM health probing is opt-in"
            );
            config.llm_provider = "codex_auth".to_string();
            config.llm_model = Some("refresh-once-model".to_string());
            config.analysis_llm_provider = "codex_auth".to_string();
            config.analysis_llm_model = Some(analysis_model.to_string());
            config.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
            config.codex_base_url = codex_base_url;
            config.health_llm_timeout_ms = 2_000;
            config.refresh_configured_secret_values();
            let state = AppState::new(Arc::new(config));

            timeout(
                Duration::from_secs(5),
                run_llm_health_refresh(state.clone(), state.runtime.subscribe()),
            )
            .await
            .expect("by default the refresher stops after its startup probe");
            assert_eq!(
                probes.load(Ordering::SeqCst),
                expected_probes,
                "exactly one startup probe per LLM profile ({analysis_model})"
            );

            let ready = crate::health_service::HealthService::readiness(&state).await;
            assert!(ready.ready, "{}", ready.body);
            assert_eq!(ready.body["dependencies"]["llm"], "ok");
            assert_eq!(ready.body["dependencies"]["analysis_llm"], "ok");
            assert!(
                ready.body["llm_checked_at"]["llm"].is_string(),
                "{}",
                ready.body
            );
            assert!(
                ready.body["llm_checked_at"]["analysis_llm"].is_string(),
                "{}",
                ready.body
            );
            assert_eq!(
                probes.load(Ordering::SeqCst),
                expected_probes,
                "readiness reads the cached verdicts ({analysis_model})"
            );
            let _ = std::fs::remove_file(auth_path);
        }
    }

    #[tokio::test]
    async fn opt_in_llm_health_refresher_warms_the_cache_and_stops_on_shutdown() {
        let mut config = Config::test();
        config.llm_provider = "mock".to_string();
        config.llm_model = Some("refresher-model".to_string());
        config.analysis_llm_provider = "mock".to_string();
        config.analysis_llm_model = Some("refresher-model".to_string());
        config.health_llm_probe_interval_seconds = 300;
        config.shutdown_timeout_ms = 30_000;
        let state = AppState::new(Arc::new(config));
        let cold = crate::health_service::HealthService::readiness(&state).await;
        assert!(!cold.ready, "{}", cold.body);
        assert_eq!(cold.body["dependencies"]["llm"], "not_probed");

        assert!(state.start_llm_health_refresh());
        wait_until("the refresher probes at startup", || {
            state.llm_health.cached_verdict(&state.config).is_some()
        })
        .await;
        let warm = crate::health_service::HealthService::readiness(&state).await;
        assert!(warm.ready, "{}", warm.body);
        assert_eq!(warm.body["dependencies"]["llm"], "ok");
        assert_eq!(warm.body["dependencies"]["analysis_llm"], "ok");

        // The refresher now sleeps for its 300 s interval; shutdown must not
        // wait for it (a stuck task would hold shutdown for 30 s).
        timeout(Duration::from_secs(5), state.shutdown())
            .await
            .expect("the periodic refresher must stop with the background runtime");

        let mut disabled = Config::test();
        disabled.health_llm_enabled = false;
        assert!(!AppState::new(Arc::new(disabled)).start_llm_health_refresh());
    }
}
