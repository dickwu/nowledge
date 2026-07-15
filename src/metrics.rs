use std::{sync::Arc, time::Instant};

use prometheus_client::{
    encoding::{text::encode, EncodeLabelSet},
    metrics::{
        counter::Counter,
        family::{Family, MetricConstructor},
        gauge::Gauge,
        histogram::Histogram,
        info::Info,
    },
    registry::Registry,
};

use crate::error::ApiError;

pub(crate) const INGEST_STATES: [&str; 8] = [
    "queued",
    "parsing",
    "parsed",
    "fragmenting",
    "indexing",
    "completed",
    "failed",
    "other",
];
pub(crate) const OPERATION_STATUSES: [&str; 6] = [
    "pending",
    "primary_committed",
    "effects_submitted",
    "partially_failed",
    "completed",
    "failed",
];

const HTTP_DURATION_BUCKETS_SECONDS: [f64; 11] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];
const OPERATION_DURATION_BUCKETS_SECONDS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 10.0, 60.0,
];
const CANDIDATE_COUNT_BUCKETS: [f64; 10] =
    [0.0, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 250.0, 500.0];

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpRequestLabels {
    method: String,
    route: String,
    status_class: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpRouteLabels {
    method: String,
    route: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StageLabels {
    stage: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StageOutcomeLabels {
    stage: String,
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct LlmLabels {
    profile: String,
    provider: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct LlmOutcomeLabels {
    profile: String,
    provider: String,
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct LlmTokenLabels {
    profile: String,
    provider: String,
    kind: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct ResourceOutcomeLabels {
    resource: String,
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct DomainOutcomeLabels {
    domain: String,
    outcome: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StateLabels {
    state: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct StatusLabels {
    status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct AuditBackgroundDropLabels {
    stage: String,
    reason: String,
}

#[derive(Clone, Copy, Debug)]
struct HttpDurationHistogram;

impl MetricConstructor<Histogram> for HttpDurationHistogram {
    fn new_metric(&self) -> Histogram {
        Histogram::new(HTTP_DURATION_BUCKETS_SECONDS)
    }
}

#[derive(Clone, Copy, Debug)]
struct OperationDurationHistogram;

impl MetricConstructor<Histogram> for OperationDurationHistogram {
    fn new_metric(&self) -> Histogram {
        Histogram::new(OPERATION_DURATION_BUCKETS_SECONDS)
    }
}

#[derive(Clone, Copy, Debug)]
struct CandidateCountHistogram;

impl MetricConstructor<Histogram> for CandidateCountHistogram {
    fn new_metric(&self) -> Histogram {
        Histogram::new(CANDIDATE_COUNT_BUCKETS)
    }
}

#[derive(Clone)]
pub(crate) struct Metrics {
    registry: Arc<Registry>,
    http_requests: Family<HttpRequestLabels, Counter>,
    http_request_duration: Family<HttpRouteLabels, Histogram, HttpDurationHistogram>,
    http_request_bytes: Family<HttpRouteLabels, Counter>,
    http_response_bytes: Family<HttpRouteLabels, Counter>,
    http_in_flight: Gauge,
    ingest_queue_depth: Gauge,
    ingest_accepting: Gauge,
    ingest_tasks: Family<StateLabels, Gauge>,
    ingest_stage_duration: Family<StageOutcomeLabels, Histogram, OperationDurationHistogram>,
    ingest_stage_failures: Family<StageLabels, Counter>,
    meili_task_duration: Family<StageOutcomeLabels, Histogram, OperationDurationHistogram>,
    meili_task_failures: Family<StageOutcomeLabels, Counter>,
    rag_stage_duration: Family<StageOutcomeLabels, Histogram, OperationDurationHistogram>,
    rag_stage_candidates: Family<StageLabels, Histogram, CandidateCountHistogram>,
    llm_request_duration: Family<LlmOutcomeLabels, Histogram, OperationDurationHistogram>,
    llm_tokens: Family<LlmTokenLabels, Counter>,
    llm_retries: Family<LlmLabels, Counter>,
    llm_timeouts: Family<LlmLabels, Counter>,
    llm_rate_limit_state: Family<LlmLabels, Gauge>,
    cache_accesses: Family<ResourceOutcomeLabels, Counter>,
    read_through: Family<ResourceOutcomeLabels, Counter>,
    hydration_records: Family<DomainOutcomeLabels, Counter>,
    operations: Family<StatusLabels, Gauge>,
    audit_background_drops: Family<AuditBackgroundDropLabels, Counter>,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IngestRuntimeMetrics {
    pub(crate) queue_depth: usize,
    pub(crate) accepting: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct StoreMetricsSnapshot {
    pub(crate) ingest_tasks: [usize; INGEST_STATES.len()],
    pub(crate) operations: [usize; OPERATION_STATUSES.len()],
}

pub(crate) struct HttpRequestObservation {
    metrics: Metrics,
    method: String,
    route: String,
    request_bytes: u64,
    response_bytes: u64,
    status_class: Option<String>,
    started_at: Instant,
}

pub(crate) struct IngestStageObservation {
    metrics: Metrics,
    stage: &'static str,
    started_at: Instant,
    completed: bool,
}

impl Metrics {
    pub(crate) fn new() -> Self {
        let http_requests = Family::default();
        let http_request_duration = Family::new_with_constructor(HttpDurationHistogram);
        let http_request_bytes = Family::default();
        let http_response_bytes = Family::default();
        let http_in_flight = Gauge::default();
        let ingest_queue_depth = Gauge::default();
        let ingest_accepting = Gauge::default();
        let ingest_tasks = Family::default();
        let ingest_stage_duration = Family::new_with_constructor(OperationDurationHistogram);
        let ingest_stage_failures = Family::default();
        let meili_task_duration = Family::new_with_constructor(OperationDurationHistogram);
        let meili_task_failures = Family::default();
        let rag_stage_duration = Family::new_with_constructor(OperationDurationHistogram);
        let rag_stage_candidates = Family::new_with_constructor(CandidateCountHistogram);
        let llm_request_duration = Family::new_with_constructor(OperationDurationHistogram);
        let llm_tokens = Family::default();
        let llm_retries = Family::default();
        let llm_timeouts = Family::default();
        let llm_rate_limit_state = Family::default();
        let cache_accesses = Family::default();
        let read_through = Family::default();
        let hydration_records = Family::default();
        let operations = Family::default();
        let audit_background_drops = Family::default();

        let build_info = Info::new(vec![
            ("version".to_string(), env!("CARGO_PKG_VERSION").to_string()),
            ("git_revision".to_string(), build_revision().to_string()),
        ]);
        let mut registry = Registry::default();
        registry.register(
            "nowledge_build",
            "Build identity; values remain constant for the process lifetime",
            build_info,
        );
        registry.register(
            "nowledge_http_requests",
            "Completed HTTP requests grouped only by bounded protocol labels",
            http_requests.clone(),
        );
        registry.register(
            "nowledge_http_request_duration_seconds",
            "HTTP request duration through response-body completion or cancellation",
            http_request_duration.clone(),
        );
        registry.register(
            "nowledge_http_request_bytes",
            "HTTP request body bytes grouped only by bounded method and route template",
            http_request_bytes.clone(),
        );
        registry.register(
            "nowledge_http_response_bytes",
            "HTTP response body bytes emitted through completion or cancellation",
            http_response_bytes.clone(),
        );
        registry.register(
            "nowledge_http_in_flight",
            "HTTP requests whose response bodies have not completed or been cancelled",
            http_in_flight.clone(),
        );
        registry.register(
            "nowledge_ingest_queue_depth",
            "Ingest jobs admitted but not yet running",
            ingest_queue_depth.clone(),
        );
        registry.register(
            "nowledge_ingest_accepting",
            "Whether the ingest dispatcher currently accepts new jobs",
            ingest_accepting.clone(),
        );
        registry.register(
            "nowledge_ingest_tasks",
            "Current tenant ingest task records grouped by bounded state",
            ingest_tasks.clone(),
        );
        registry.register(
            "nowledge_ingest_stage_duration_seconds",
            "Ingest stage duration grouped by fixed stage and outcome vocabularies",
            ingest_stage_duration.clone(),
        );
        registry.register(
            "nowledge_ingest_stage_failures",
            "Failed or cancelled ingest stages grouped by a fixed stage vocabulary",
            ingest_stage_failures.clone(),
        );
        registry.register(
            "nowledge_meili_task_duration_seconds",
            "Time spent awaiting Meilisearch tasks grouped by fixed operation and outcome",
            meili_task_duration.clone(),
        );
        registry.register(
            "nowledge_meili_task_failures",
            "Failed Meilisearch task waits grouped by fixed operation and failure class",
            meili_task_failures.clone(),
        );
        registry.register(
            "nowledge_rag_stage_duration_seconds",
            "RAG stage duration grouped by fixed stage and outcome vocabularies",
            rag_stage_duration.clone(),
        );
        registry.register(
            "nowledge_rag_stage_candidates",
            "Candidate counts observed at fixed RAG pipeline stages",
            rag_stage_candidates.clone(),
        );
        registry.register(
            "nowledge_llm_request_duration_seconds",
            "LLM request duration grouped by bounded profile, provider, and outcome",
            llm_request_duration.clone(),
        );
        registry.register(
            "nowledge_llm_tokens",
            "Provider-reported LLM token totals grouped by bounded token kind",
            llm_tokens.clone(),
        );
        registry.register(
            "nowledge_llm_retries",
            "Observed upstream LLM retry attempts grouped by bounded profile and provider",
            llm_retries.clone(),
        );
        registry.register(
            "nowledge_llm_timeouts",
            "LLM requests ending in a timeout grouped by bounded profile and provider",
            llm_timeouts.clone(),
        );
        registry.register(
            "nowledge_llm_rate_limit_state",
            "Latest bounded LLM rate-limit state: unknown=0, ok=1, near_limit=2, limited=3",
            llm_rate_limit_state.clone(),
        );
        registry.register(
            "nowledge_cache_accesses",
            "In-process cache accesses grouped by fixed resource and outcome vocabularies",
            cache_accesses.clone(),
        );
        registry.register(
            "nowledge_read_through",
            "Repository read-through results grouped by fixed resource and outcome vocabularies",
            read_through.clone(),
        );
        registry.register(
            "nowledge_hydration_records",
            "Startup hydration record counts grouped by fixed domain and outcome vocabularies",
            hydration_records.clone(),
        );
        registry.register(
            "nowledge_operations",
            "Current tenant durable mutation operations grouped by bounded status",
            operations.clone(),
        );
        registry.register(
            "nowledge_audit_background_drops",
            "Best-effort audit writes not durably persisted, grouped by bounded stage and reason",
            audit_background_drops.clone(),
        );

        Self {
            registry: Arc::new(registry),
            http_requests,
            http_request_duration,
            http_request_bytes,
            http_response_bytes,
            http_in_flight,
            ingest_queue_depth,
            ingest_accepting,
            ingest_tasks,
            ingest_stage_duration,
            ingest_stage_failures,
            meili_task_duration,
            meili_task_failures,
            rag_stage_duration,
            rag_stage_candidates,
            llm_request_duration,
            llm_tokens,
            llm_retries,
            llm_timeouts,
            llm_rate_limit_state,
            cache_accesses,
            read_through,
            hydration_records,
            operations,
            audit_background_drops,
        }
    }

    pub(crate) fn record_audit_background_drop(&self, stage: &'static str, reason: &'static str) {
        debug_assert!(matches!(stage, "denial" | "finalization"));
        debug_assert!(matches!(
            reason,
            "rate_limit" | "capacity" | "shutdown" | "persistence"
        ));
        self.audit_background_drops
            .get_or_create(&AuditBackgroundDropLabels {
                stage: stage.to_string(),
                reason: reason.to_string(),
            })
            .inc();
    }

    pub(crate) fn begin_http_request(
        &self,
        method: &str,
        route: &str,
        request_bytes: u64,
    ) -> HttpRequestObservation {
        self.http_in_flight.inc();
        HttpRequestObservation {
            metrics: self.clone(),
            method: bounded_method(method).to_string(),
            route: route.to_string(),
            request_bytes,
            response_bytes: 0,
            status_class: None,
            started_at: Instant::now(),
        }
    }

    pub(crate) fn begin_ingest_stage(&self, stage: &'static str) -> IngestStageObservation {
        IngestStageObservation {
            metrics: self.clone(),
            stage: bounded_ingest_stage(stage),
            started_at: Instant::now(),
            completed: false,
        }
    }

    pub(crate) fn record_meili_task_wait(
        &self,
        operation: &str,
        elapsed_seconds: f64,
        result: &Result<(), ApiError>,
    ) {
        let stage = bounded_meili_operation(operation).to_string();
        let outcome = result_outcome(result).to_string();
        self.meili_task_duration
            .get_or_create(&StageOutcomeLabels {
                stage: stage.clone(),
                outcome: outcome.clone(),
            })
            .observe(elapsed_seconds);
        if let Err(error) = result {
            self.meili_task_failures
                .get_or_create(&StageOutcomeLabels {
                    stage,
                    outcome: bounded_error_class(error).to_string(),
                })
                .inc();
        }
    }

    pub(crate) fn record_rag_stage(&self, stage: &str, elapsed_seconds: f64, success: bool) {
        self.rag_stage_duration
            .get_or_create(&StageOutcomeLabels {
                stage: bounded_rag_stage(stage).to_string(),
                outcome: if success { "success" } else { "failure" }.to_string(),
            })
            .observe(elapsed_seconds);
    }

    pub(crate) fn observe_rag_candidates(&self, stage: &str, count: usize) {
        self.rag_stage_candidates
            .get_or_create(&StageLabels {
                stage: bounded_rag_stage(stage).to_string(),
            })
            .observe(count as f64);
    }

    pub(crate) fn record_llm_request(
        &self,
        profile: &str,
        provider: &str,
        elapsed_seconds: f64,
        result: &Result<(), &ApiError>,
    ) {
        let labels = bounded_llm_labels(profile, provider);
        let outcome = match result {
            Ok(()) => "success",
            Err(ApiError::Timeout) => "timeout",
            Err(ApiError::TooManyRequests(_)) => "rate_limited",
            Err(_) => "failure",
        };
        self.llm_request_duration
            .get_or_create(&LlmOutcomeLabels {
                profile: labels.profile.clone(),
                provider: labels.provider.clone(),
                outcome: outcome.to_string(),
            })
            .observe(elapsed_seconds);
        if matches!(result, Err(ApiError::Timeout)) {
            self.llm_timeouts.get_or_create(&labels).inc();
        }
        if matches!(result, Err(ApiError::TooManyRequests(_))) {
            self.set_llm_rate_limit_state(profile, provider, "limited");
        }
    }

    pub(crate) fn record_llm_tokens(
        &self,
        profile: &str,
        provider: &str,
        usage: &crate::llm::LlmTokenUsage,
    ) {
        for (kind, value) in [
            ("input", usage.input_tokens),
            ("cached_input", usage.cached_input_tokens),
            ("output", usage.output_tokens),
            ("reasoning_output", usage.reasoning_output_tokens),
            ("total", usage.total_tokens),
        ] {
            if let Some(value) = value {
                let labels = bounded_llm_labels(profile, provider);
                self.llm_tokens
                    .get_or_create(&LlmTokenLabels {
                        profile: labels.profile,
                        provider: labels.provider,
                        kind: kind.to_string(),
                    })
                    .inc_by(value);
            }
        }
    }

    pub(crate) fn record_llm_retries(&self, profile: &str, provider: &str, retries: u64) {
        if retries == 0 {
            return;
        }
        self.llm_retries
            .get_or_create(&bounded_llm_labels(profile, provider))
            .inc_by(retries);
    }

    pub(crate) fn set_llm_rate_limit_state(&self, profile: &str, provider: &str, state: &str) {
        let value = match state {
            "ok" => 1,
            "near_limit" => 2,
            "limited" => 3,
            _ => 0,
        };
        self.llm_rate_limit_state
            .get_or_create(&bounded_llm_labels(profile, provider))
            .set(value);
    }

    pub(crate) fn record_cache_access(&self, resource: &str, outcome: &str) {
        self.cache_accesses
            .get_or_create(&bounded_resource_outcome(resource, outcome))
            .inc();
    }

    pub(crate) fn record_read_through(&self, resource: &str, outcome: &str, count: usize) {
        let counter = self
            .read_through
            .get_or_create(&bounded_resource_outcome(resource, outcome));
        counter.inc_by(u64::try_from(count).unwrap_or(u64::MAX));
    }

    pub(crate) fn record_hydration(&self, domain: &str, outcome: &str, count: usize) {
        self.hydration_records
            .get_or_create(&DomainOutcomeLabels {
                domain: bounded_hydration_domain(domain),
                outcome: bounded_hydration_outcome(outcome).to_string(),
            })
            .inc_by(u64::try_from(count).unwrap_or(u64::MAX));
    }

    pub(crate) fn render(
        &self,
        runtime: IngestRuntimeMetrics,
        store: &StoreMetricsSnapshot,
    ) -> Result<String, ApiError> {
        self.ingest_queue_depth
            .set(metric_value(runtime.queue_depth));
        self.ingest_accepting.set(i64::from(runtime.accepting));
        for (index, state) in INGEST_STATES.iter().enumerate() {
            self.ingest_tasks
                .get_or_create(&StateLabels {
                    state: (*state).to_string(),
                })
                .set(metric_value(store.ingest_tasks[index]));
        }
        for (index, status) in OPERATION_STATUSES.iter().enumerate() {
            self.operations
                .get_or_create(&StatusLabels {
                    status: (*status).to_string(),
                })
                .set(metric_value(store.operations[index]));
        }

        let mut body = String::new();
        encode(&mut body, &self.registry)
            .map_err(|_| ApiError::Internal("failed to encode operational metrics".to_string()))?;
        Ok(body)
    }
}

impl HttpRequestObservation {
    pub(crate) fn complete(mut self, status: u16) -> Self {
        self.status_class = Some(status_class(status).to_string());
        self
    }

    pub(crate) fn add_response_bytes(&mut self, bytes: usize) {
        self.response_bytes = self
            .response_bytes
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
    }
}

impl Drop for HttpRequestObservation {
    fn drop(&mut self) {
        self.metrics.http_in_flight.dec();
        let Some(status_class) = self.status_class.take() else {
            return;
        };
        self.metrics
            .http_requests
            .get_or_create(&HttpRequestLabels {
                method: self.method.clone(),
                route: self.route.clone(),
                status_class,
            })
            .inc();
        self.metrics
            .http_request_duration
            .get_or_create(&HttpRouteLabels {
                method: self.method.clone(),
                route: self.route.clone(),
            })
            .observe(self.started_at.elapsed().as_secs_f64());
        let route_labels = HttpRouteLabels {
            method: self.method.clone(),
            route: self.route.clone(),
        };
        self.metrics
            .http_request_bytes
            .get_or_create(&route_labels)
            .inc_by(self.request_bytes);
        self.metrics
            .http_response_bytes
            .get_or_create(&route_labels)
            .inc_by(self.response_bytes);
    }
}

impl IngestStageObservation {
    pub(crate) fn complete(mut self) {
        self.metrics
            .ingest_stage_duration
            .get_or_create(&StageOutcomeLabels {
                stage: self.stage.to_string(),
                outcome: "success".to_string(),
            })
            .observe(self.started_at.elapsed().as_secs_f64());
        self.completed = true;
    }
}

impl Drop for IngestStageObservation {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        self.metrics
            .ingest_stage_duration
            .get_or_create(&StageOutcomeLabels {
                stage: self.stage.to_string(),
                outcome: "failure".to_string(),
            })
            .observe(self.started_at.elapsed().as_secs_f64());
        self.metrics
            .ingest_stage_failures
            .get_or_create(&StageLabels {
                stage: self.stage.to_string(),
            })
            .inc();
    }
}

fn bounded_method(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "PATCH" => "PATCH",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        "CONNECT" => "CONNECT",
        "TRACE" => "TRACE",
        _ => "OTHER",
    }
}

fn status_class(status: u16) -> &'static str {
    match status {
        100..=199 => "1xx",
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

fn metric_value(value: usize) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn build_revision() -> &'static str {
    env!("NOWLEDGE_GIT_REV")
}

fn result_outcome(result: &Result<(), ApiError>) -> &'static str {
    if result.is_ok() {
        "success"
    } else {
        "failure"
    }
}

fn bounded_error_class(error: &ApiError) -> &'static str {
    match error {
        ApiError::Timeout => "timeout",
        ApiError::TooManyRequests(_) => "rate_limited",
        ApiError::Unauthorized(_) | ApiError::Forbidden(_) => "authorization",
        ApiError::Upstream(_) => "upstream",
        ApiError::Internal(_) => "internal",
        _ => "other",
    }
}

fn bounded_ingest_stage(stage: &str) -> &'static str {
    match stage {
        "parsing" => "parsing",
        "fragmenting" => "fragmenting",
        "indexing" => "indexing",
        _ => "other",
    }
}

fn bounded_meili_operation(operation: &str) -> &'static str {
    match operation {
        "write" => "write",
        "durable_write" => "durable_write",
        "batch" => "batch",
        "hydration" => "hydration",
        _ => "other",
    }
}

fn bounded_rag_stage(stage: &str) -> &'static str {
    match stage {
        "retrieval" => "retrieval",
        "generation" => "generation",
        "materialization" => "materialization",
        _ => "other",
    }
}

fn bounded_llm_labels(profile: &str, provider: &str) -> LlmLabels {
    LlmLabels {
        profile: match profile {
            "primary" => "primary",
            "analysis" => "analysis",
            _ => "other",
        }
        .to_string(),
        provider: match provider {
            "none" => "none",
            "mock" => "mock",
            "openai_api_key" => "openai_api_key",
            "codex_auth" => "codex_auth",
            _ => "other",
        }
        .to_string(),
    }
}

fn bounded_resource_outcome(resource: &str, outcome: &str) -> ResourceOutcomeLabels {
    ResourceOutcomeLabels {
        resource: match resource {
            "personal_context" => "personal_context",
            "context_node" => "context_node",
            "source_document" => "source_document",
            "structured_rows" => "structured_rows",
            "trace" => "trace",
            _ => "other",
        }
        .to_string(),
        outcome: match outcome {
            "hit" => "hit",
            "miss" => "miss",
            "loaded" => "loaded",
            "stored" => "stored",
            "not_found" => "not_found",
            "failure" => "failure",
            _ => "other",
        }
        .to_string(),
    }
}

fn bounded_hydration_domain(domain: &str) -> String {
    match domain {
        "operations"
        | "user_event_indexes"
        | "user_events"
        | "personal_context"
        | "company_context_nodes"
        | "state_items"
        | "insights"
        | "links"
        | "company_sources"
        | "source_revisions"
        | "source_documents"
        | "parsed_blocks"
        | "datasets"
        | "structured_snapshots"
        | "structured_rows"
        | "structured_summaries"
        | "sessions"
        | "traces"
        | "harness_components"
        | "harness_revisions"
        | "harness_changes"
        | "harness_verdicts"
        | "eval_cases"
        | "eval_runs"
        | "eval_case_results"
        | "eval_overviews"
        | "ingest_tasks"
        | "ingest_results"
        | "parse_artifacts"
        | "preflight_decisions"
        | "vector_embeddings"
        | "queue_permits"
        | "provider_health" => domain.to_string(),
        _ => "other".to_string(),
    }
}

fn bounded_hydration_outcome(outcome: &str) -> &'static str {
    match outcome {
        "loaded" => "loaded",
        "quarantined" => "quarantined",
        "recovered" => "recovered",
        "failure" => "failure",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attacker_controlled_methods_and_statuses_are_bounded() {
        assert_eq!(bounded_method("GET"), "GET");
        assert_eq!(bounded_method("X-CUSTOM-METHOD"), "OTHER");
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(799), "other");
        assert!(
            build_revision() == "unknown"
                || build_revision().ends_with("-dirty")
                || build_revision().bytes().all(|b| b.is_ascii_hexdigit())
        );
    }

    #[test]
    fn rendering_emits_fixed_runtime_series_and_openmetrics_eof() {
        let metrics = Metrics::new();
        let mut request = metrics
            .begin_http_request("POST", "/v1/rag/answer", 128)
            .complete(200);
        request.add_response_bytes(256);
        drop(request);
        metrics.begin_ingest_stage("parsing").complete();
        drop(metrics.begin_ingest_stage("indexing"));
        metrics.record_meili_task_wait("write", 0.02, &Ok(()));
        metrics.record_meili_task_wait("batch", 0.02, &Err(ApiError::timeout()));
        metrics.record_rag_stage("retrieval", 0.01, true);
        metrics.observe_rag_candidates("retrieval", 4);
        let llm_success: Result<(), &ApiError> = Ok(());
        metrics.record_llm_request("primary", "mock", 0.03, &llm_success);
        let timeout = ApiError::timeout();
        let llm_timeout = Err(&timeout);
        metrics.record_llm_request("primary", "mock", 0.04, &llm_timeout);
        metrics.record_llm_tokens(
            "primary",
            "mock",
            &crate::llm::LlmTokenUsage {
                input_tokens: Some(10),
                output_tokens: Some(5),
                total_tokens: Some(15),
                ..crate::llm::LlmTokenUsage::default()
            },
        );
        metrics.record_llm_retries("primary", "mock", 1);
        metrics.set_llm_rate_limit_state("primary", "mock", "ok");
        metrics.record_cache_access("context_node", "hit");
        metrics.record_read_through("context_node", "loaded", 2);
        metrics.record_hydration("operations", "loaded", 3);
        metrics.record_audit_background_drop("denial", "capacity");
        let body = metrics
            .render(
                IngestRuntimeMetrics {
                    queue_depth: 3,
                    accepting: true,
                },
                &StoreMetricsSnapshot::default(),
            )
            .unwrap();
        assert!(body.contains("nowledge_build_info"));
        assert!(body.contains(&format!("git_revision=\"{}\"", env!("NOWLEDGE_GIT_REV"))));
        assert!(body.contains("nowledge_http_request_bytes_total"));
        assert!(body.contains("nowledge_http_response_bytes_total"));
        assert!(body.contains("nowledge_ingest_queue_depth 3"));
        assert!(body.contains("nowledge_ingest_accepting 1"));
        assert!(body.contains("nowledge_ingest_stage_duration_seconds"));
        assert!(body.contains("nowledge_ingest_stage_failures_total"));
        assert!(body.contains("nowledge_meili_task_duration_seconds"));
        assert!(body.contains("nowledge_meili_task_failures_total"));
        assert!(body.contains("nowledge_rag_stage_duration_seconds"));
        assert!(body.contains("nowledge_rag_stage_candidates"));
        assert!(body.contains("nowledge_llm_request_duration_seconds"));
        assert!(body.contains("nowledge_llm_tokens_total"));
        assert!(body.contains("nowledge_llm_retries_total"));
        assert!(body.contains("nowledge_llm_timeouts_total"));
        assert!(body.contains("nowledge_llm_rate_limit_state"));
        assert!(body.contains("nowledge_cache_accesses_total"));
        assert!(body.contains("nowledge_read_through_total"));
        assert!(body.contains("nowledge_hydration_records_total"));
        assert!(body.contains("nowledge_audit_background_drops_total"));
        assert!(body.ends_with("# EOF\n"));
    }

    #[test]
    fn new_metric_labels_collapse_untrusted_values_into_fixed_vocabularies() {
        let metrics = Metrics::new();
        metrics.record_rag_stage("ctx://owner-secret", 0.01, true);
        metrics.observe_rag_candidates("ctx://owner-secret", 3);
        metrics.record_cache_access("owner-secret", "uri-secret");
        metrics.record_read_through("owner-secret", "uri-secret", 1);
        metrics.set_llm_rate_limit_state("tenant-secret", "model-secret", "limited");
        let body = metrics
            .render(
                IngestRuntimeMetrics::default(),
                &StoreMetricsSnapshot::default(),
            )
            .unwrap();

        assert!(body.contains("stage=\"other\""), "{body}");
        assert!(
            body.contains("resource=\"other\",outcome=\"other\""),
            "{body}"
        );
        assert!(
            body.contains("profile=\"other\",provider=\"other\""),
            "{body}"
        );
        assert!(!body.contains("owner-secret"), "{body}");
        assert!(!body.contains("uri-secret"), "{body}");
        assert!(!body.contains("tenant-secret"), "{body}");
        assert!(!body.contains("model-secret"), "{body}");
    }
}
