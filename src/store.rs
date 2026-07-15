use std::{
    cmp::Reverse,
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, RwLock},
    time::Instant,
};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

use crate::{
    config::{Config, WriteConsistency},
    error::{safe_cause_diagnostic, ApiError},
    fragmenter::{BlockAwareFragmenter, FragmentChunk},
    meili::MeiliAdmin,
    metrics::Metrics,
    models::*,
    mutation::{
        operation_list_item, operation_record_from_plan, operation_step_accepted,
        operation_step_completed, operation_step_failed, operation_summary, persistence_metadata,
        validate_operation_record, OPERATION_PLAN_SCHEMA_VERSION,
    },
    parser::{
        validate_parser_config, validate_parser_output, ParserInput, ParserOutput, ParserRegistry,
        StagedUpload,
    },
    repository::{
        repository_from_config, repository_from_meili_admins, KnowledgeRepository,
        RepositoryContextSearchQuery, RepositoryOperationListQuery,
    },
    request_context::{self, RequestPrincipalScope},
    resolver::{EventIndexResolver, EVENT_INDEX_SCHEMA_VERSION, EVENT_SETTINGS_HASH},
    util::{
        ancestor_uris, hmac_hex, mask_secret_egress_projection_preserving_chars,
        mask_secret_fragment_projection_preserving_chars, new_id, now, require_string,
        sanitize_slug, text_score, truncate_chars,
    },
    vector_match::{VectorMatcher, VectorScoreMap},
};

#[path = "store_accessors.rs"]
mod accessors;
#[path = "store_company_docs.rs"]
mod company_docs;
#[path = "store_context.rs"]
mod context;
#[path = "store_harness_eval.rs"]
mod harness_eval;
#[path = "store_history.rs"]
mod history;
#[path = "store_ingest.rs"]
mod ingest;
#[path = "store_sessions.rs"]
mod sessions;
#[path = "store_state_insights.rs"]
mod state_insights;
#[path = "store_structured.rs"]
mod structured;

const INGEST_ERROR_PARSER_FAILED: &str = "parser_failed";
const INGEST_ERROR_INDEXING_FAILED: &str = "indexing_failed";
const INGEST_ERROR_FAILED: &str = "ingest_failed";
const INGEST_ERROR_INTERRUPTED: &str = "ingest_interrupted";
const OPERATION_LIST_CURSOR_PREFIX: &str = "op1";
const OPERATION_LIST_CURSOR_MAX_BYTES: usize = 4_096;
const OPERATION_STARTUP_RECONCILE_BATCH_SIZE: usize = 250;

#[derive(Debug, Clone)]
struct OperationListCursor {
    offset: usize,
    previous_operation_id: String,
}

#[derive(Clone)]
pub struct Store {
    inner: Arc<RwLock<StoreData>>,
    mutation_gate: Arc<tokio::sync::Mutex<()>>,
    resolver: EventIndexResolver,
    repository: Arc<dyn KnowledgeRepository>,
    vector: Arc<Mutex<VectorMatcher>>,
    redaction_config: Arc<Config>,
    parser_registry: ParserRegistry,
    metrics: Metrics,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct SourceDocumentKey {
    tenant_id: String,
    owner_user_id: Option<String>,
    uri: String,
}

impl SourceDocumentKey {
    fn new(tenant_id: &str, owner_user_id: Option<&str>, uri: &str) -> Self {
        Self {
            tenant_id: tenant_id.to_string(),
            owner_user_id: owner_user_id.map(ToString::to_string),
            uri: uri.to_string(),
        }
    }

    fn from_document(document: &SourceDocument) -> Self {
        Self::new(
            &document.tenant_id,
            document.owner_user_id.as_deref(),
            &document.uri,
        )
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ParseArtifactKey {
    tenant_id: String,
    owner_user_id: Option<String>,
    artifact_id: String,
}

impl ParseArtifactKey {
    fn from_artifact(artifact: &ParseArtifact) -> Self {
        Self {
            tenant_id: artifact.tenant_id.clone(),
            owner_user_id: artifact.owner_user_id.clone(),
            artifact_id: artifact.id.clone(),
        }
    }
}

#[derive(Clone, Default)]
struct StoreData {
    hydration_report: Option<HydrationReport>,
    audit_records: HashMap<String, AuditRecord>,
    operations: HashMap<String, OperationRecord>,
    user_indexes: HashMap<(String, String), UserEventIndex>,
    events_by_index: HashMap<String, Vec<HistoryEvent>>,
    event_by_id: HashMap<String, HistoryEvent>,
    event_idempotency: HashMap<(String, String), String>,
    personal_context: HashMap<String, Vec<ContextNode>>,
    personal_context_loaded: HashSet<String>,
    company_context: Vec<ContextNode>,
    state_items: HashMap<(String, String, String), StateItem>,
    insights: HashMap<String, InsightRecord>,
    insight_idempotency: HashMap<(String, String, String), String>,
    sources: HashMap<String, CompanySource>,
    source_revisions: HashMap<String, Vec<SourceRevision>>,
    source_documents: HashMap<SourceDocumentKey, SourceDocument>,
    parse_artifacts: HashMap<ParseArtifactKey, ParseArtifact>,
    parsed_blocks: HashMap<SourceDocumentKey, Vec<ParsedBlock>>,
    ingest_tasks: HashMap<String, IngestTask>,
    ingest_results: HashMap<String, IngestTaskResult>,
    preflight_decisions: HashMap<String, CompanyDocPreflightResponse>,
    datasets: HashMap<String, DatasetRecord>,
    snapshots: HashMap<String, StructuredSnapshot>,
    snapshot_idempotency: HashMap<(String, String), String>,
    rows_by_snapshot: HashMap<String, Vec<Value>>,
    row_idempotency: HashSet<(String, String)>,
    structured_summaries: HashMap<String, Value>,
    sessions: HashMap<String, SessionRecord>,
    traces: HashMap<String, TraceRecord>,
    links: HashMap<String, KnowledgeLink>,
    link_idempotency: HashMap<(String, String, String), String>,
    harness_components: HashMap<String, HarnessComponent>,
    harness_revisions: HashMap<String, Vec<HarnessComponentRevision>>,
    harness_changes: HashMap<String, HarnessChangeManifest>,
    harness_verdicts: HashMap<String, HarnessChangeVerdict>,
    eval_cases: HashMap<String, RagEvalCase>,
    eval_runs: HashMap<String, RagEvalRun>,
    eval_case_results: HashMap<String, RagEvalCaseResult>,
    eval_overviews: HashMap<String, RagEvalOverview>,
}

const MAX_IN_MEMORY_AUDIT_RECORDS: usize = 10_000;

#[derive(Default)]
struct HydrationStage {
    operations: Vec<OperationRecord>,
    user_indexes: Vec<UserEventIndex>,
    company_context: Vec<ContextNode>,
    state_items: Vec<StateItem>,
    insights: Vec<InsightRecord>,
    links: Vec<KnowledgeLink>,
    sources: Vec<CompanySource>,
    source_revisions: Vec<SourceRevision>,
    datasets: Vec<DatasetRecord>,
    snapshots: Vec<StructuredSnapshot>,
    structured_summaries: Vec<Value>,
    sessions: Vec<SessionRecord>,
    traces: Vec<TraceRecord>,
    harness_components: Vec<HarnessComponent>,
    harness_revisions: Vec<HarnessComponentRevision>,
    harness_changes: Vec<HarnessChangeManifest>,
    harness_verdicts: Vec<HarnessChangeVerdict>,
    eval_cases: Vec<RagEvalCase>,
    eval_runs: Vec<RagEvalRun>,
    eval_case_results: Vec<RagEvalCaseResult>,
    eval_overviews: Vec<RagEvalOverview>,
    ingest_tasks: Vec<IngestTask>,
    ingest_results: Vec<IngestTaskResult>,
    parse_artifacts: Vec<ParseArtifact>,
    recovered_ingest_tasks: usize,
    recovered_parse_artifacts: usize,
}

struct HydrationFailure {
    domain: &'static str,
    error: ApiError,
}

impl StoreData {
    fn seed_harness_components(&mut self, tenant_id: &str) {
        let created_at = now();
        for (component_id, display_name, component_kind, description) in
            default_harness_components()
        {
            let revision_id = bootstrap_harness_revision_id(component_id);
            self.harness_components
                .entry(component_id.to_string())
                .or_insert_with(|| HarnessComponent {
                    id: component_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    display_name: display_name.to_string(),
                    component_kind: component_kind.to_string(),
                    description: description.to_string(),
                    status: "active".to_string(),
                    current_revision_id: Some(revision_id.clone()),
                    created_at,
                    updated_at: created_at,
                });
            let revisions = self
                .harness_revisions
                .entry(component_id.to_string())
                .or_default();
            if revisions
                .iter()
                .any(|revision| revision.tenant_id == tenant_id && revision.id == revision_id)
            {
                continue;
            }
            revisions.push(HarnessComponentRevision {
                id: revision_id,
                tenant_id: tenant_id.to_string(),
                component_id: component_id.to_string(),
                iteration: 0,
                manifest_id: "bootstrap".to_string(),
                files: Vec::new(),
                content: json!({
                    "source": "built_in_registry",
                    "invariants": [
                        "preserve public API behavior",
                        "preserve fragment-first retrieval",
                        "preserve source-document traceback"
                    ]
                }),
                status: "active".to_string(),
                created_by: "system_bootstrap".to_string(),
                created_at,
            });
        }
    }
}

fn durability_contract() -> BTreeMap<&'static str, (DurabilityClass, HydrationStrategy, bool)> {
    use DurabilityClass::{DerivedDurable, DurableCanonical, Ephemeral};
    use HydrationStrategy::{
        Ephemeral as EphemeralStrategy, LazySnapshot, ReadThrough, Regenerate, Startup,
    };

    [
        ("operations", (DurableCanonical, Startup, true)),
        ("user_event_indexes", (DurableCanonical, Startup, true)),
        ("user_events", (DurableCanonical, ReadThrough, false)),
        ("personal_context", (DerivedDurable, ReadThrough, false)),
        ("company_context_nodes", (DerivedDurable, Startup, true)),
        ("state_items", (DurableCanonical, Startup, true)),
        ("insights", (DurableCanonical, Startup, true)),
        ("links", (DurableCanonical, Startup, true)),
        ("company_sources", (DurableCanonical, Startup, true)),
        ("source_revisions", (DurableCanonical, Startup, true)),
        ("source_documents", (DurableCanonical, ReadThrough, false)),
        ("parse_artifacts", (DerivedDurable, Startup, true)),
        ("parsed_blocks", (Ephemeral, EphemeralStrategy, false)),
        ("datasets", (DurableCanonical, Startup, true)),
        ("structured_snapshots", (DurableCanonical, Startup, true)),
        ("structured_rows", (DurableCanonical, LazySnapshot, false)),
        ("structured_summaries", (DerivedDurable, Startup, true)),
        ("sessions", (DurableCanonical, Startup, true)),
        ("traces", (DurableCanonical, Startup, true)),
        ("harness_components", (DurableCanonical, Startup, true)),
        ("harness_revisions", (DurableCanonical, Startup, true)),
        ("harness_changes", (DurableCanonical, Startup, true)),
        ("harness_verdicts", (DurableCanonical, Startup, true)),
        ("eval_cases", (DurableCanonical, Startup, true)),
        ("eval_runs", (DurableCanonical, Startup, true)),
        ("eval_case_results", (DurableCanonical, Startup, true)),
        ("eval_overviews", (DerivedDurable, Startup, true)),
        ("ingest_tasks", (DurableCanonical, Startup, true)),
        ("ingest_results", (DurableCanonical, Startup, true)),
        ("preflight_decisions", (Ephemeral, EphemeralStrategy, false)),
        ("vector_embeddings", (Ephemeral, Regenerate, false)),
        ("queue_permits", (Ephemeral, EphemeralStrategy, false)),
        ("provider_health", (Ephemeral, Regenerate, false)),
    ]
    .into_iter()
    .collect()
}

fn initial_hydration_report(config: &Config) -> HydrationReport {
    let required = config.store_backend == "meili";
    let domains = durability_contract()
        .into_iter()
        .map(|(name, (durability, strategy, mandatory))| {
            let status = if !required {
                "not_required"
            } else {
                match strategy {
                    HydrationStrategy::Startup => "pending",
                    HydrationStrategy::ReadThrough => "read_through",
                    HydrationStrategy::LazySnapshot => "lazy",
                    HydrationStrategy::Regenerate => "regenerable",
                    HydrationStrategy::Ephemeral => "ephemeral",
                }
            };
            (
                name.to_string(),
                HydrationDomainReport {
                    durability,
                    strategy,
                    mandatory,
                    status: status.to_string(),
                    expected: 0,
                    loaded: 0,
                    skipped: usize::from(!required),
                    quarantined: 0,
                    recovered: 0,
                    error_category: None,
                    error_fingerprint: None,
                },
            )
        })
        .collect();
    let timestamp = now();
    HydrationReport {
        tenant_id: config.tenant_id.clone(),
        backend: config.store_backend.clone(),
        status: if required {
            HydrationStatus::Pending
        } else {
            HydrationStatus::NotRequired
        },
        ready: !required,
        started_at: timestamp,
        completed_at: (!required).then_some(timestamp),
        domains,
    }
}

fn required_hydration_rows<T>(
    domain: &'static str,
    result: Result<Option<Vec<T>>, ApiError>,
) -> Result<Vec<T>, HydrationFailure> {
    match result {
        Ok(Some(rows)) => Ok(rows),
        Ok(None) => Err(HydrationFailure {
            domain,
            error: ApiError::Internal(format!(
                "mandatory hydration domain {domain} is unavailable for the configured backend"
            )),
        }),
        Err(error) => Err(HydrationFailure { domain, error }),
    }
}

fn hydration_result<T>(
    domain: &'static str,
    result: Result<T, ApiError>,
) -> Result<T, HydrationFailure> {
    result.map_err(|error| HydrationFailure { domain, error })
}

fn failed_ingest_recovery_domain(record: &OperationRecord) -> Option<&'static str> {
    std::iter::once(&record.plan.primary)
        .chain(record.plan.side_effects.iter())
        .find_map(|step| {
            let failed = record
                .progress
                .steps
                .get(&step.id)
                .is_some_and(|progress| progress.status == OperationStepStatus::Failed);
            if !failed {
                return None;
            }
            match &step.resource {
                OperationResource::IngestTask { .. } | OperationResource::IngestTasks { .. } => {
                    Some("ingest_tasks")
                }
                OperationResource::ParseArtifacts { .. } => Some("parse_artifacts"),
                OperationResource::IngestResult { .. } => Some("ingest_results"),
                _ => None,
            }
        })
}

fn completed_hydration_report(
    config: &Config,
    tenant_id: &str,
    stage: &HydrationStage,
) -> HydrationReport {
    let mut report = initial_hydration_report(config);
    report.tenant_id = tenant_id.to_string();
    report.status = HydrationStatus::Complete;
    report.ready = true;
    report.completed_at = Some(now());

    let counts = [
        ("operations", stage.operations.len(), 0),
        ("user_event_indexes", stage.user_indexes.len(), 0),
        ("company_context_nodes", stage.company_context.len(), 0),
        ("state_items", stage.state_items.len(), 0),
        ("insights", stage.insights.len(), 0),
        ("links", stage.links.len(), 0),
        ("company_sources", stage.sources.len(), 0),
        ("source_revisions", stage.source_revisions.len(), 0),
        ("datasets", stage.datasets.len(), 0),
        ("structured_snapshots", stage.snapshots.len(), 0),
        ("structured_summaries", stage.structured_summaries.len(), 0),
        ("sessions", stage.sessions.len(), 0),
        ("traces", stage.traces.len(), 0),
        ("harness_components", stage.harness_components.len(), 0),
        ("harness_revisions", stage.harness_revisions.len(), 0),
        ("harness_changes", stage.harness_changes.len(), 0),
        ("harness_verdicts", stage.harness_verdicts.len(), 0),
        ("eval_cases", stage.eval_cases.len(), 0),
        ("eval_runs", stage.eval_runs.len(), 0),
        ("eval_case_results", stage.eval_case_results.len(), 0),
        ("eval_overviews", stage.eval_overviews.len(), 0),
        (
            "ingest_tasks",
            stage.ingest_tasks.len(),
            stage.recovered_ingest_tasks,
        ),
        ("ingest_results", stage.ingest_results.len(), 0),
        (
            "parse_artifacts",
            stage.parse_artifacts.len(),
            stage.recovered_parse_artifacts,
        ),
    ];
    for (domain_name, count, recovered) in counts {
        if let Some(domain) = report.domains.get_mut(domain_name) {
            domain.status = "complete".to_string();
            domain.expected = count;
            domain.loaded = count;
            domain.recovered = recovered;
        }
    }
    report
}

fn hydration_response(report: &HydrationReport) -> Value {
    let mut response = serde_json::Map::new();
    for (domain_name, domain) in &report.domains {
        response.insert(domain_name.clone(), json!(domain.loaded));
    }
    let recovered = report
        .domains
        .get("ingest_tasks")
        .map(|domain| domain.recovered)
        .unwrap_or_default();
    response.insert("interrupted_ingest_tasks".to_string(), json!(recovered));
    response.insert("hydration".to_string(), json!(report));
    response.insert("tenant_id".to_string(), json!(report.tenant_id));
    response.insert("backend".to_string(), json!(report.backend));
    response.insert("status".to_string(), json!(report.status));
    response.insert("ready".to_string(), json!(report.ready));
    response.insert("started_at".to_string(), json!(report.started_at));
    response.insert("completed_at".to_string(), json!(report.completed_at));
    response.insert("domains".to_string(), json!(report.domains));
    Value::Object(response)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSearchOutcome {
    pub response: ContextSearchResponse,
    pub trace: TraceRecord,
    pub nodes: Vec<ContextNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DocumentIngestResult {
    source_id: String,
    source_document_uri: String,
    fragment_uris: Vec<String>,
}

#[derive(Clone, Copy)]
enum MutationPrimary {
    UserIndex,
    HistoryEvents,
    StateItem,
    Insight,
    Links,
    AnalysisMaterialization,
    DeleteCompanySource,
    SourceRevision,
    SourceDocuments,
    ParseArtifacts,
    StructuredSnapshot,
    Dataset,
    StructuredRows,
    StructuredSummary,
    Session,
    Trace,
    HarnessComponents,
    HarnessChanges,
    HarnessVerdicts,
    IngestTask,
    DeleteIngestTasks,
    IngestResult,
    EvalCase,
    EvalRun,
}

struct MutationPlanInput<'a> {
    tenant_id: &'a str,
    operation_kind: &'a str,
    owner_user_id: Option<&'a str>,
    idempotency_key: Option<&'a str>,
    primary_kind: MutationPrimary,
    resources: Vec<OperationResource>,
    response_snapshot: Value,
    request_fingerprint: Option<&'a str>,
}

#[derive(Clone, Copy)]
struct MutationIdempotency<'a> {
    key: Option<&'a str>,
    request_fingerprint: Option<&'a str>,
}

impl MutationPrimary {
    fn matches(self, resource: &OperationResource) -> bool {
        matches!(
            (self, resource),
            (
                Self::UserIndex,
                OperationResource::EnsureUserEventIndex { .. }
            ) | (Self::HistoryEvents, OperationResource::HistoryEvents { .. })
                | (Self::StateItem, OperationResource::StateItem { .. })
                | (Self::Insight, OperationResource::Insight { .. })
                | (Self::Links, OperationResource::Links { .. })
                | (
                    Self::AnalysisMaterialization,
                    OperationResource::Insight { .. } | OperationResource::Links { .. }
                )
                | (
                    Self::DeleteCompanySource,
                    OperationResource::DeleteCompanySourceIndex {
                        target: CompanySourceDeleteTarget::Fragments,
                        ..
                    }
                )
                | (
                    Self::SourceRevision,
                    OperationResource::SourceRevision { .. }
                )
                | (
                    Self::SourceDocuments,
                    OperationResource::SourceDocuments { .. }
                )
                | (
                    Self::ParseArtifacts,
                    OperationResource::ParseArtifacts { .. }
                )
                | (
                    Self::StructuredSnapshot,
                    OperationResource::StructuredSnapshot { .. }
                )
                | (Self::Dataset, OperationResource::Dataset { .. })
                | (
                    Self::StructuredRows,
                    OperationResource::StructuredRows { .. }
                )
                | (
                    Self::StructuredSummary,
                    OperationResource::StructuredSummary { .. }
                )
                | (Self::Session, OperationResource::Session { .. })
                | (Self::Trace, OperationResource::Trace { .. })
                | (
                    Self::HarnessComponents,
                    OperationResource::HarnessComponents { .. }
                )
                | (
                    Self::HarnessChanges,
                    OperationResource::HarnessChanges { .. }
                )
                | (
                    Self::HarnessVerdicts,
                    OperationResource::HarnessVerdicts { .. }
                )
                | (
                    Self::IngestTask,
                    OperationResource::IngestTask { .. } | OperationResource::IngestTasks { .. }
                )
                | (
                    Self::DeleteIngestTasks,
                    OperationResource::DeleteIngestTasks { .. }
                )
                | (Self::IngestResult, OperationResource::IngestResult { .. })
                | (Self::EvalCase, OperationResource::EvalCase { .. })
                | (Self::EvalRun, OperationResource::EvalRun { .. })
        )
    }

    const fn exposes_partial_persistence(self) -> bool {
        matches!(self, Self::HistoryEvents | Self::DeleteCompanySource)
    }

    const fn requires_index_confirmation(self) -> bool {
        matches!(
            self,
            Self::UserIndex
                | Self::AnalysisMaterialization
                | Self::DeleteCompanySource
                | Self::DeleteIngestTasks
        )
    }
}

fn serialized_changed<T: Serialize>(before: Option<&T>, after: &T) -> bool {
    before
        .is_none_or(|before| serde_json::to_value(before).ok() != serde_json::to_value(after).ok())
}

fn context_node_identity(node: &ContextNode) -> (&str, u8) {
    (&node.uri, node.layer)
}

fn changed_context_nodes(
    before: impl Iterator<Item = ContextNode>,
    after: impl Iterator<Item = ContextNode>,
) -> Vec<ContextNode> {
    let before = before
        .map(|node| ((node.uri.clone(), node.layer), node))
        .collect::<HashMap<_, _>>();
    let mut changed = after
        .filter(|node| serialized_changed(before.get(&(node.uri.clone(), node.layer)), node))
        .collect::<Vec<_>>();
    changed.sort_by(|left, right| context_node_identity(left).cmp(&context_node_identity(right)));
    changed
}

fn company_source_related_uris(
    data: &StoreData,
    tenant_id: &str,
    source_id: &str,
) -> HashSet<String> {
    let mut uris = data
        .source_documents
        .values()
        .filter(|document| {
            document.tenant_id == tenant_id
                && document.owner_user_id.is_none()
                && document.source_id == source_id
        })
        .map(|document| document.uri.clone())
        .collect::<HashSet<_>>();
    uris.extend(
        data.source_revisions
            .get(source_id)
            .into_iter()
            .flatten()
            .filter(|revision| revision.tenant_id == tenant_id)
            .map(|revision| company_revision_source_document_uri(source_id, &revision.id)),
    );
    uris.extend(
        data.company_context
            .iter()
            .filter(|node| {
                node.tenant_id == tenant_id && node.source_id.as_deref() == Some(source_id)
            })
            .flat_map(|node| {
                std::iter::once(node.uri.clone()).chain(node.source_document_uri.clone())
            }),
    );
    uris.extend(
        data.parse_artifacts
            .values()
            .filter(|artifact| {
                artifact.tenant_id == tenant_id
                    && artifact.owner_user_id.is_none()
                    && artifact.source_id == source_id
            })
            .flat_map(|artifact| {
                [artifact.source_document_uri.clone(), artifact.uri.clone()].into_iter()
            }),
    );
    uris.extend(
        data.ingest_tasks
            .values()
            .filter(|task| {
                task.tenant_id == tenant_id
                    && task.owner_user_id.is_none()
                    && task.source_id == source_id
            })
            .filter_map(|task| task.source_document_uri.clone()),
    );
    uris.extend(
        data.ingest_results
            .values()
            .filter(|result| {
                result.task.tenant_id == tenant_id
                    && result.task.owner_user_id.is_none()
                    && result.source_id == source_id
            })
            .map(|result| result.source_document_uri.clone()),
    );
    uris
}

fn unfinished_company_source_delete(data: &StoreData, tenant_id: &str, source_id: &str) -> bool {
    data.operations.values().any(|operation| {
        operation.tenant_id == tenant_id
            && operation.operation_kind == "company_doc.delete"
            && !(operation.status == OperationStatus::Completed
                && operation.indexing_state == OperationIndexingState::Completed)
            && std::iter::once(&operation.plan.primary)
                .chain(operation.plan.side_effects.iter())
                .any(|step| {
                    matches!(
                        &step.resource,
                        OperationResource::DeleteCompanySourceIndex {
                            source_id: deleting_source_id,
                            ..
                        } if deleting_source_id == source_id
                    )
                })
    })
}

fn ensure_company_source_not_deleting(
    data: &StoreData,
    tenant_id: &str,
    source_id: &str,
) -> Result<(), ApiError> {
    if unfinished_company_source_delete(data, tenant_id, source_id) {
        return Err(ApiError::conflict(
            "company source deletion is incomplete; reconcile it before recreating or updating the source",
        ));
    }
    Ok(())
}

fn operation_resource_targets_company_source(
    resource: &OperationResource,
    tenant_id: &str,
    source_id: &str,
    related_uris: &HashSet<String>,
) -> bool {
    match resource {
        OperationResource::CompanySource { source } => {
            source.tenant_id == tenant_id && source.id == source_id
        }
        OperationResource::SourceRevision { revision } => {
            revision.tenant_id == tenant_id && revision.source_id == source_id
        }
        OperationResource::ContextNodes { nodes, .. } => nodes.iter().any(|node| {
            node.tenant_id == tenant_id
                && node.owner_user_id.is_none()
                && node.source_id.as_deref() == Some(source_id)
        }),
        OperationResource::SourceDocuments { documents } => documents.iter().any(|document| {
            document.tenant_id == tenant_id
                && document.owner_user_id.is_none()
                && document.source_id == source_id
        }),
        OperationResource::ParseArtifacts { artifacts } => artifacts.iter().any(|artifact| {
            artifact.tenant_id == tenant_id
                && artifact.owner_user_id.is_none()
                && artifact.source_id == source_id
        }),
        OperationResource::IngestTask { task } => {
            task.tenant_id == tenant_id
                && task.owner_user_id.is_none()
                && task.source_id == source_id
        }
        OperationResource::IngestTasks { tasks } => tasks.iter().any(|task| {
            task.tenant_id == tenant_id
                && task.owner_user_id.is_none()
                && task.source_id == source_id
        }),
        OperationResource::IngestResult { result } => {
            result.task.tenant_id == tenant_id
                && result.task.owner_user_id.is_none()
                && result.source_id == source_id
        }
        OperationResource::Links { links } => links.iter().any(|link| {
            link.tenant_id == tenant_id
                && (related_uris.contains(&link.source_uri)
                    || related_uris.contains(&link.target_uri))
        }),
        OperationResource::DeleteCompanySourceIndex {
            source_id: deleting_source_id,
            ..
        } => deleting_source_id == source_id,
        _ => false,
    }
}

fn ensure_company_source_mutations_reconciled_before_delete(
    data: &StoreData,
    tenant_id: &str,
    source_id: &str,
    related_uris: &HashSet<String>,
) -> Result<(), ApiError> {
    let unfinished_predecessor = data.operations.values().any(|operation| {
        operation.tenant_id == tenant_id
            && operation.operation_kind != "company_doc.delete"
            && !(operation.status == OperationStatus::Completed
                && operation.indexing_state == OperationIndexingState::Completed)
            && std::iter::once(&operation.plan.primary)
                .chain(operation.plan.side_effects.iter())
                .any(|step| {
                    operation_resource_targets_company_source(
                        &step.resource,
                        tenant_id,
                        source_id,
                        related_uris,
                    )
                })
    });
    if unfinished_predecessor {
        return Err(ApiError::conflict(
            "a previous company source mutation must be reconciled before deleting the source",
        ));
    }
    Ok(())
}

fn ensure_link_not_pending_company_source_delete(
    data: &StoreData,
    tenant_id: &str,
    link_id: Option<&str>,
    source_uri: &str,
    target_uri: &str,
) -> Result<(), ApiError> {
    let pending_delete_targets_link = data.operations.values().any(|operation| {
        operation.tenant_id == tenant_id
            && operation.operation_kind == "company_doc.delete"
            && !(operation.status == OperationStatus::Completed
                && operation.indexing_state == OperationIndexingState::Completed)
            && std::iter::once(&operation.plan.primary)
                .chain(operation.plan.side_effects.iter())
                .any(|step| {
                    matches!(
                        &step.resource,
                        OperationResource::DeleteCompanySourceIndex {
                            target: CompanySourceDeleteTarget::Links {
                                link_ids,
                                related_uris,
                            },
                            ..
                        } if link_id.is_some_and(|link_id| {
                                link_ids.iter().any(|deleting_link_id| deleting_link_id == link_id)
                            })
                            || related_uris.iter().any(|uri| {
                                uri == source_uri || uri == target_uri
                            })
                    )
                })
    });
    if pending_delete_targets_link {
        return Err(ApiError::conflict(
            "link belongs to an incomplete company source deletion; reconcile it before updating the link",
        ));
    }
    Ok(())
}

fn operation_list_cursor_scope(
    tenant_id: &str,
    statuses: &[OperationStatus],
    operation_kinds: &[String],
) -> Result<String, ApiError> {
    let mut statuses = statuses
        .iter()
        .map(|status| status.as_str().to_string())
        .collect::<Vec<_>>();
    statuses.sort();
    statuses.dedup();
    let mut operation_kinds = operation_kinds.to_vec();
    operation_kinds.sort();
    operation_kinds.dedup();
    serde_json::to_string(&(tenant_id, statuses, operation_kinds))
        .map_err(|error| ApiError::Internal(error.to_string()))
}

fn encode_operation_list_cursor(
    secret: &[u8],
    tenant_id: &str,
    statuses: &[OperationStatus],
    operation_kinds: &[String],
    offset: usize,
    previous_operation_id: &str,
) -> Result<String, ApiError> {
    if offset == 0 || previous_operation_id.trim().is_empty() {
        return Err(ApiError::Internal(
            "operation cursor requires a non-empty prior page".to_string(),
        ));
    }
    let scope = operation_list_cursor_scope(tenant_id, statuses, operation_kinds)?;
    let payload = hex::encode(format!("{offset}\0{previous_operation_id}"));
    let signature = hmac_hex(
        secret,
        "operation-list-cursor",
        &format!("{scope}\0{payload}"),
        32,
    );
    Ok(format!(
        "{OPERATION_LIST_CURSOR_PREFIX}.{payload}.{signature}"
    ))
}

fn decode_operation_list_cursor(
    secret: &[u8],
    tenant_id: &str,
    statuses: &[OperationStatus],
    operation_kinds: &[String],
    cursor: Option<&str>,
) -> Result<Option<OperationListCursor>, ApiError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let invalid = || ApiError::bad_request("cursor is invalid or stale");
    if cursor.len() > OPERATION_LIST_CURSOR_MAX_BYTES {
        return Err(invalid());
    }
    let mut parts = cursor.split('.');
    let prefix = parts.next().ok_or_else(&invalid)?;
    let payload = parts.next().ok_or_else(&invalid)?;
    let signature = parts.next().ok_or_else(&invalid)?;
    if prefix != OPERATION_LIST_CURSOR_PREFIX || parts.next().is_some() {
        return Err(invalid());
    }
    let scope = operation_list_cursor_scope(tenant_id, statuses, operation_kinds)?;
    let expected_signature = hmac_hex(
        secret,
        "operation-list-cursor",
        &format!("{scope}\0{payload}"),
        32,
    );
    if signature != expected_signature {
        return Err(invalid());
    }
    let decoded = hex::decode(payload).map_err(|_| invalid())?;
    let decoded = String::from_utf8(decoded).map_err(|_| invalid())?;
    let (offset, previous_operation_id) = decoded.split_once('\0').ok_or_else(&invalid)?;
    let offset = offset.parse::<usize>().map_err(|_| invalid())?;
    if offset == 0
        || previous_operation_id.trim().is_empty()
        || previous_operation_id.contains('\0')
    {
        return Err(invalid());
    }
    Ok(Some(OperationListCursor {
        offset,
        previous_operation_id: previous_operation_id.to_string(),
    }))
}

impl Store {
    pub fn new(config: &Config) -> Self {
        Self::new_with_repository(config, repository_from_config(config), Metrics::new())
    }

    pub(crate) fn new_with_meili_admins_and_metrics(
        config: &Config,
        runtime: MeiliAdmin,
        index_admin: MeiliAdmin,
        metrics: Metrics,
    ) -> Self {
        Self::new_with_repository(
            config,
            repository_from_meili_admins(config, runtime, index_admin),
            metrics,
        )
    }

    fn new_with_repository(
        config: &Config,
        repository: Arc<dyn KnowledgeRepository>,
        metrics: Metrics,
    ) -> Self {
        // Capture the current dynamic Codex credential before any later
        // rotation so response and provider-boundary redaction retain it.
        let _ = config.refresh_configured_secret_values();
        let mut data = StoreData {
            hydration_report: Some(initial_hydration_report(config)),
            ..StoreData::default()
        };
        data.seed_harness_components(&config.tenant_id);
        Self {
            inner: Arc::new(RwLock::new(data)),
            mutation_gate: Arc::new(tokio::sync::Mutex::new(())),
            resolver: EventIndexResolver::new(config.index_hash_secret.clone()),
            repository,
            vector: Arc::new(Mutex::new(VectorMatcher::from_config(config))),
            redaction_config: Arc::new(config.clone()),
            parser_registry: ParserRegistry::new(config),
            metrics,
        }
    }

    async fn wait_for_repository_tasks(
        &self,
        task_uids: &[String],
        operation: &'static str,
    ) -> Result<(), ApiError> {
        let started_at = Instant::now();
        let result = self.repository.wait_for_tasks(task_uids).await;
        if self.repository.backend_name() == "meili" && !task_uids.is_empty() {
            self.metrics.record_meili_task_wait(
                operation,
                started_at.elapsed().as_secs_f64(),
                &result,
            );
        }
        result
    }

    /// Persist a complete audit state before publishing it to the process
    /// cache. The initial attempted state therefore fails closed, while a
    /// rejected finalization leaves the previously accepted attempt intact.
    pub(crate) async fn persist_audit_record(&self, record: &AuditRecord) -> Result<(), ApiError> {
        record
            .validate()
            .map_err(|error| ApiError::Internal(format!("invalid audit record: {error}")))?;
        let receipt = self.repository.upsert_audit_record(record).await?;
        self.wait_for_repository_tasks(&receipt.task_uids, "durable_write")
            .await?;
        if self.backend_name() == "memory" {
            let mut data = self.write()?;
            if !data.audit_records.contains_key(&record.id)
                && data.audit_records.len() >= MAX_IN_MEMORY_AUDIT_RECORDS
            {
                let oldest_id = data
                    .audit_records
                    .values()
                    .min_by(|left, right| {
                        left.occurred_at
                            .cmp(&right.occurred_at)
                            .then_with(|| left.id.cmp(&right.id))
                    })
                    .map(|oldest| oldest.id.clone());
                if let Some(oldest_id) = oldest_id {
                    data.audit_records.remove(&oldest_id);
                }
            }
            data.audit_records.insert(record.id.clone(), record.clone());
        }
        Ok(())
    }

    /// Reuse the same pooled parser client used by ingestion for health checks.
    pub async fn parser_health_status(&self, config: &Config) -> Value {
        self.parser_registry.health_status(config).await
    }

    /// Build an isolated copy used to validate and plan a mutation without
    /// publishing any cache changes before the durable primary write is
    /// accepted. The repository and immutable helpers are shared; only the
    /// mutable StoreData snapshot and mutation gate are private to the stage.
    fn staged_copy(&self) -> Result<Self, ApiError> {
        let mut staged = self.clone();
        staged.inner = Arc::new(RwLock::new(self.read()?.clone()));
        staged.mutation_gate = Arc::new(tokio::sync::Mutex::new(()));
        // Vector warming is an optional performance projection. A staged
        // mutation must not share it with the live store because a rejected
        // primary write must have no live side effects.
        staged.vector = Arc::new(Mutex::new(VectorMatcher::from_config(
            &self.redaction_config,
        )));
        Ok(staged)
    }

    fn mutation_resources(
        &self,
        tenant_id: &str,
        before: &StoreData,
        after: &StoreData,
    ) -> Vec<OperationResource> {
        let mut resources = Vec::new();

        let mut indexes = after
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .filter(|index| {
                serialized_changed(
                    before
                        .user_indexes
                        .get(&(index.tenant_id.clone(), index.owner_user_id_hash.clone())),
                    *index,
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        indexes.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            indexes
                .into_iter()
                .map(|index| OperationResource::EnsureUserEventIndex { index }),
        );

        let mut events_by_index = BTreeMap::<String, Vec<HistoryEvent>>::new();
        for event in after
            .event_by_id
            .values()
            .filter(|event| event.tenant_id == tenant_id)
            .filter(|event| serialized_changed(before.event_by_id.get(&event.id), *event))
        {
            events_by_index
                .entry(event.event_index_uid.clone())
                .or_default()
                .push(event.clone());
        }
        for events in events_by_index.values_mut() {
            events.sort_by(|left, right| left.id.cmp(&right.id));
        }
        resources.extend(
            events_by_index
                .into_values()
                .map(|events| OperationResource::HistoryEvents { events }),
        );

        let changed_company_nodes = changed_context_nodes(
            before
                .company_context
                .iter()
                .filter(|node| node.tenant_id == tenant_id)
                .cloned(),
            after
                .company_context
                .iter()
                .filter(|node| node.tenant_id == tenant_id)
                .cloned(),
        );
        if !changed_company_nodes.is_empty() {
            resources.push(OperationResource::ContextNodes {
                index_uid: "rag_company_context".to_string(),
                nodes: changed_company_nodes,
            });
        }
        let mut personal_index_uids = after
            .personal_context
            .keys()
            .chain(before.personal_context.keys())
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        personal_index_uids.sort();
        for index_uid in personal_index_uids {
            let changed = changed_context_nodes(
                before
                    .personal_context
                    .get(&index_uid)
                    .into_iter()
                    .flatten()
                    .filter(|node| node.tenant_id == tenant_id)
                    .cloned(),
                after
                    .personal_context
                    .get(&index_uid)
                    .into_iter()
                    .flatten()
                    .filter(|node| node.tenant_id == tenant_id)
                    .cloned(),
            );
            if !changed.is_empty() {
                resources.push(OperationResource::ContextNodes {
                    index_uid,
                    nodes: changed,
                });
            }
        }

        let mut state_items = after
            .state_items
            .values()
            .filter(|item| item.tenant_id == tenant_id)
            .filter(|item| {
                serialized_changed(
                    before.state_items.get(&(
                        item.tenant_id.clone(),
                        item.owner_user_id.clone(),
                        item.natural_key.clone(),
                    )),
                    *item,
                )
            })
            .cloned()
            .collect::<Vec<_>>();
        state_items.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            state_items
                .into_iter()
                .map(|item| OperationResource::StateItem { item }),
        );

        let mut insights = after
            .insights
            .values()
            .filter(|insight| insight.tenant_id == tenant_id)
            .filter(|insight| serialized_changed(before.insights.get(&insight.id), *insight))
            .cloned()
            .collect::<Vec<_>>();
        insights.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            insights
                .into_iter()
                .map(|insight| OperationResource::Insight { insight }),
        );

        let mut sources = after
            .sources
            .values()
            .filter(|source| source.tenant_id == tenant_id)
            .filter(|source| serialized_changed(before.sources.get(&source.id), *source))
            .cloned()
            .collect::<Vec<_>>();
        sources.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            sources
                .into_iter()
                .map(|source| OperationResource::CompanySource { source }),
        );
        let mut deleted_sources = before
            .sources
            .values()
            .filter(|source| source.tenant_id == tenant_id)
            .filter(|source| !after.sources.contains_key(&source.id))
            .map(|source| {
                let mut source_document_uris =
                    company_source_related_uris(before, tenant_id, &source.id)
                        .into_iter()
                        .collect::<Vec<_>>();
                source_document_uris.sort();
                source_document_uris.dedup();
                let source_document_uri_set = source_document_uris.iter().collect::<HashSet<_>>();
                let mut link_ids = before
                    .links
                    .values()
                    .filter(|link| {
                        link.tenant_id == tenant_id
                            && (source_document_uri_set.contains(&link.source_uri)
                                || source_document_uri_set.contains(&link.target_uri))
                    })
                    .map(|link| link.id.clone())
                    .collect::<Vec<_>>();
                link_ids.sort();
                link_ids.dedup();
                (source.id.clone(), link_ids, source_document_uris)
            })
            .collect::<Vec<_>>();
        deleted_sources.sort_by(|left, right| left.0.cmp(&right.0));
        for (source_id, link_ids, related_uris) in deleted_sources {
            resources.extend(
                [
                    CompanySourceDeleteTarget::Fragments,
                    CompanySourceDeleteTarget::Revisions,
                    CompanySourceDeleteTarget::Source,
                    CompanySourceDeleteTarget::SourceDocuments,
                    CompanySourceDeleteTarget::ParseArtifacts,
                    CompanySourceDeleteTarget::IngestTasks,
                    CompanySourceDeleteTarget::IngestResults,
                ]
                .into_iter()
                .map(|target| OperationResource::DeleteCompanySourceIndex {
                    source_id: source_id.clone(),
                    target,
                }),
            );
            resources.push(OperationResource::DeleteCompanySourceIndex {
                source_id,
                target: CompanySourceDeleteTarget::Links {
                    link_ids,
                    related_uris,
                },
            });
        }

        let before_revisions = before
            .source_revisions
            .values()
            .flatten()
            .filter(|revision| revision.tenant_id == tenant_id)
            .map(|revision| (revision.id.clone(), revision))
            .collect::<HashMap<_, _>>();
        let mut revisions = after
            .source_revisions
            .values()
            .flatten()
            .filter(|revision| revision.tenant_id == tenant_id)
            .filter(|revision| {
                serialized_changed(before_revisions.get(&revision.id).copied(), *revision)
            })
            .cloned()
            .collect::<Vec<_>>();
        revisions.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            revisions
                .into_iter()
                .map(|revision| OperationResource::SourceRevision { revision }),
        );

        let mut documents = after
            .source_documents
            .iter()
            .filter(|(_, document)| document.tenant_id == tenant_id)
            .filter(|(key, document)| {
                serialized_changed(before.source_documents.get(*key), *document)
            })
            .map(|(_, document)| document.clone())
            .collect::<Vec<_>>();
        documents.sort_by(|left, right| left.uri.cmp(&right.uri));
        if !documents.is_empty() {
            resources.push(OperationResource::SourceDocuments { documents });
        }

        let mut artifacts = after
            .parse_artifacts
            .iter()
            .filter(|(_, artifact)| artifact.tenant_id == tenant_id)
            .filter(|(key, artifact)| {
                serialized_changed(before.parse_artifacts.get(*key), *artifact)
            })
            .map(|(_, artifact)| artifact.clone())
            .collect::<Vec<_>>();
        artifacts.sort_by(|left, right| left.id.cmp(&right.id));
        if !artifacts.is_empty() {
            resources.push(OperationResource::ParseArtifacts { artifacts });
        }

        self.structured_mutation_resources(tenant_id, before, after, &mut resources);
        self.session_and_link_mutation_resources(tenant_id, before, after, &mut resources);
        self.operational_mutation_resources(tenant_id, before, after, &mut resources);
        resources
    }

    fn structured_mutation_resources(
        &self,
        tenant_id: &str,
        before: &StoreData,
        after: &StoreData,
        resources: &mut Vec<OperationResource>,
    ) {
        let mut datasets = after
            .datasets
            .values()
            .filter(|dataset| dataset.tenant_id == tenant_id)
            .filter(|dataset| {
                serialized_changed(before.datasets.get(&dataset.dataset_key), *dataset)
            })
            .cloned()
            .collect::<Vec<_>>();
        datasets.sort_by(|left, right| left.dataset_key.cmp(&right.dataset_key));
        resources.extend(
            datasets
                .into_iter()
                .map(|dataset| OperationResource::Dataset { dataset }),
        );

        let mut snapshots = after
            .snapshots
            .values()
            .filter(|snapshot| snapshot.tenant_id == tenant_id)
            .filter(|snapshot| serialized_changed(before.snapshots.get(&snapshot.id), *snapshot))
            .cloned()
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            snapshots
                .into_iter()
                .map(|snapshot| OperationResource::StructuredSnapshot { snapshot }),
        );

        let mut snapshot_ids = after
            .snapshots
            .values()
            .filter(|snapshot| snapshot.tenant_id == tenant_id)
            .map(|snapshot| snapshot.id.clone())
            .collect::<Vec<_>>();
        snapshot_ids.sort();
        for snapshot_id in snapshot_ids {
            let Some(rows) = after.rows_by_snapshot.get(&snapshot_id) else {
                continue;
            };
            if !rows.is_empty()
                && serialized_changed(before.rows_by_snapshot.get(&snapshot_id), rows)
            {
                resources.push(OperationResource::StructuredRows { rows: rows.clone() });
            }
        }

        let mut summaries = after
            .structured_summaries
            .iter()
            .filter(|(_, summary)| {
                summary.get("tenant_id").and_then(Value::as_str) == Some(tenant_id)
            })
            .filter(|(id, summary)| {
                serialized_changed(before.structured_summaries.get(*id), *summary)
            })
            .map(|(id, summary)| (id.clone(), summary.clone()))
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| left.0.cmp(&right.0));
        resources.extend(
            summaries
                .into_iter()
                .map(|(_, summary)| OperationResource::StructuredSummary { summary }),
        );
    }

    fn session_and_link_mutation_resources(
        &self,
        tenant_id: &str,
        before: &StoreData,
        after: &StoreData,
        resources: &mut Vec<OperationResource>,
    ) {
        let mut sessions = after
            .sessions
            .values()
            .filter(|session| session.tenant_id == tenant_id)
            .filter(|session| serialized_changed(before.sessions.get(&session.id), *session))
            .cloned()
            .collect::<Vec<_>>();
        sessions.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            sessions
                .into_iter()
                .map(|session| OperationResource::Session { session }),
        );

        let mut traces = after
            .traces
            .values()
            .filter(|trace| trace.tenant_id == tenant_id)
            .filter(|trace| serialized_changed(before.traces.get(&trace.id), *trace))
            .cloned()
            .collect::<Vec<_>>();
        traces.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            traces
                .into_iter()
                .map(|trace| OperationResource::Trace { trace }),
        );

        let mut links = after
            .links
            .values()
            .filter(|link| link.tenant_id == tenant_id)
            .filter(|link| serialized_changed(before.links.get(&link.id), *link))
            .cloned()
            .collect::<Vec<_>>();
        links.sort_by(|left, right| left.id.cmp(&right.id));
        if !links.is_empty() {
            resources.push(OperationResource::Links { links });
        }
    }

    fn operational_mutation_resources(
        &self,
        tenant_id: &str,
        before: &StoreData,
        after: &StoreData,
        resources: &mut Vec<OperationResource>,
    ) {
        let mut components = after
            .harness_components
            .values()
            .filter(|component| component.tenant_id == tenant_id)
            .filter(|component| {
                serialized_changed(before.harness_components.get(&component.id), *component)
            })
            .cloned()
            .collect::<Vec<_>>();
        components.sort_by(|left, right| left.id.cmp(&right.id));
        let before_revisions = before
            .harness_revisions
            .values()
            .flatten()
            .filter(|revision| revision.tenant_id == tenant_id)
            .map(|revision| (revision.id.clone(), revision))
            .collect::<HashMap<_, _>>();
        let mut revisions = after
            .harness_revisions
            .values()
            .flatten()
            .filter(|revision| revision.tenant_id == tenant_id)
            .filter(|revision| {
                serialized_changed(before_revisions.get(&revision.id).copied(), *revision)
            })
            .cloned()
            .collect::<Vec<_>>();
        revisions.sort_by(|left, right| left.id.cmp(&right.id));
        if !components.is_empty() || !revisions.is_empty() {
            resources.push(OperationResource::HarnessComponents {
                components,
                revisions,
            });
        }

        let mut changes = after
            .harness_changes
            .values()
            .filter(|change| change.tenant_id == tenant_id)
            .filter(|change| serialized_changed(before.harness_changes.get(&change.id), *change))
            .cloned()
            .collect::<Vec<_>>();
        changes.sort_by(|left, right| left.id.cmp(&right.id));
        if !changes.is_empty() {
            resources.push(OperationResource::HarnessChanges { changes });
        }

        let mut verdicts = after
            .harness_verdicts
            .values()
            .filter(|verdict| verdict.tenant_id == tenant_id)
            .filter(|verdict| {
                serialized_changed(before.harness_verdicts.get(&verdict.id), *verdict)
            })
            .cloned()
            .collect::<Vec<_>>();
        verdicts.sort_by(|left, right| left.id.cmp(&right.id));
        if !verdicts.is_empty() {
            resources.push(OperationResource::HarnessVerdicts { verdicts });
        }

        let mut tasks = after
            .ingest_tasks
            .values()
            .filter(|task| task.tenant_id == tenant_id)
            .filter(|task| serialized_changed(before.ingest_tasks.get(&task.task_id), *task))
            .cloned()
            .collect::<Vec<_>>();
        tasks.sort_by(|left, right| left.task_id.cmp(&right.task_id));
        if !tasks.is_empty() {
            resources.push(OperationResource::IngestTasks { tasks });
        }
        let mut deleted_task_ids = before
            .ingest_tasks
            .values()
            .filter(|task| task.tenant_id == tenant_id)
            .filter(|task| !after.ingest_tasks.contains_key(&task.task_id))
            .map(|task| task.task_id.clone())
            .collect::<Vec<_>>();
        deleted_task_ids.sort();
        if !deleted_task_ids.is_empty() {
            resources.push(OperationResource::DeleteIngestTasks {
                task_ids: deleted_task_ids,
            });
        }
        let mut results = after
            .ingest_results
            .values()
            .filter(|result| result.task.tenant_id == tenant_id)
            .filter(|result| {
                serialized_changed(before.ingest_results.get(&result.task.task_id), *result)
            })
            .cloned()
            .collect::<Vec<_>>();
        results.sort_by(|left, right| left.task.task_id.cmp(&right.task.task_id));
        resources.extend(
            results
                .into_iter()
                .map(|result| OperationResource::IngestResult { result }),
        );

        self.eval_mutation_resources(tenant_id, before, after, resources);
    }

    fn eval_mutation_resources(
        &self,
        tenant_id: &str,
        before: &StoreData,
        after: &StoreData,
        resources: &mut Vec<OperationResource>,
    ) {
        let mut cases = after
            .eval_cases
            .values()
            .filter(|case| case.tenant_id == tenant_id)
            .filter(|case| serialized_changed(before.eval_cases.get(&case.id), *case))
            .cloned()
            .collect::<Vec<_>>();
        cases.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            cases
                .into_iter()
                .map(|case| OperationResource::EvalCase { case }),
        );

        let mut runs = after
            .eval_runs
            .values()
            .filter(|run| run.tenant_id == tenant_id)
            .filter(|run| serialized_changed(before.eval_runs.get(&run.id), *run))
            .cloned()
            .collect::<Vec<_>>();
        runs.sort_by(|left, right| left.id.cmp(&right.id));
        resources.extend(
            runs.into_iter()
                .map(|run| OperationResource::EvalRun { run }),
        );

        let mut results = after
            .eval_case_results
            .values()
            .filter(|result| result.tenant_id == tenant_id)
            .filter(|result| serialized_changed(before.eval_case_results.get(&result.id), *result))
            .cloned()
            .collect::<Vec<_>>();
        results.sort_by(|left, right| left.id.cmp(&right.id));
        if !results.is_empty() {
            resources.push(OperationResource::EvalCaseResults { results });
        }

        let mut overviews = after
            .eval_overviews
            .values()
            .filter(|overview| overview.tenant_id == tenant_id)
            .filter(|overview| {
                serialized_changed(before.eval_overviews.get(&overview.run_id), *overview)
            })
            .cloned()
            .collect::<Vec<_>>();
        overviews.sort_by(|left, right| left.run_id.cmp(&right.run_id));
        resources.extend(
            overviews
                .into_iter()
                .map(|overview| OperationResource::EvalOverview { overview }),
        );
    }

    fn mutation_plan(&self, input: MutationPlanInput<'_>) -> Result<OperationPlan, ApiError> {
        let MutationPlanInput {
            tenant_id,
            operation_kind,
            owner_user_id,
            idempotency_key,
            primary_kind,
            mut resources,
            response_snapshot,
            request_fingerprint,
        } = input;
        let resource_count = resources.len();
        let primary_index = resources
            .iter()
            .position(|resource| primary_kind.matches(resource))
            .ok_or_else(|| {
                ApiError::Internal(format!(
                    "mutation {operation_kind} did not produce its declared primary resource"
                ))
            })?;
        let primary_resource = resources.remove(primary_index);
        let idempotency_key_hash = idempotency_key.map(|key| self.resolver.idempotency_hash(key));
        let target_owner_user_id_hash = owner_user_id.map(|owner| self.resolver.user_hash(owner));
        let operation_id = idempotency_key_hash.as_deref().map_or_else(
            || new_id("op"),
            |hash| {
                self.idempotent_operation_id(
                    tenant_id,
                    operation_kind,
                    target_owner_user_id_hash.as_deref(),
                    hash,
                )
            },
        );
        let actor = match request_context::current_principal() {
            Some(principal) => {
                let (scope, owner_user_id_hash) = match principal.scope {
                    RequestPrincipalScope::Owner { owner_user_id_hash } => {
                        (OperationActorScope::Owner, Some(owner_user_id_hash))
                    }
                    RequestPrincipalScope::TenantService => {
                        (OperationActorScope::TenantService, None)
                    }
                    RequestPrincipalScope::Admin => (OperationActorScope::Admin, None),
                };
                OperationActor {
                    scope,
                    owner_user_id_hash,
                    roles: principal.roles,
                    request_id: request_context::current_id(),
                }
            }
            None => OperationActor {
                scope: OperationActorScope::System,
                owner_user_id_hash: None,
                roles: Vec::new(),
                request_id: request_context::current_id(),
            },
        };
        let created_at = now();
        let wait_for_index = self.redaction_config.write_consistency
            == WriteConsistency::WaitForIndex
            || primary_kind.requires_index_confirmation();
        Ok(OperationPlan {
            schema_version: OPERATION_PLAN_SCHEMA_VERSION,
            id: operation_id,
            tenant_id: tenant_id.to_string(),
            operation_kind: operation_kind.to_string(),
            actor,
            idempotency_key_hash,
            primary: OperationStep {
                id: "primary".to_string(),
                role: OperationStepRole::Primary,
                resource: primary_resource,
            },
            side_effects: resources
                .into_iter()
                .enumerate()
                .map(|(index, resource)| OperationStep {
                    id: format!("effect-{:04}", index + 1),
                    role: OperationStepRole::SideEffect,
                    resource,
                })
                .collect(),
            redacted_metadata: json!({
                "resource_count": resource_count,
                "write_consistency": self.redaction_config.write_consistency.as_str(),
                "wait_for_index": wait_for_index,
                "request_fingerprint": request_fingerprint,
                "target_owner_user_id_hash": target_owner_user_id_hash
            }),
            response_snapshot,
            created_at,
        })
    }

    fn idempotent_operation_id(
        &self,
        tenant_id: &str,
        operation_kind: &str,
        owner_user_id_hash: Option<&str>,
        idempotency_key_hash: &str,
    ) -> String {
        let actor_scope = owner_user_id_hash
            .map(|owner_hash| format!("owner\0{owner_hash}"))
            .unwrap_or_else(|| "tenant_service".to_string());
        format!(
            "op_{}",
            hmac_hex(
                &self.redaction_config.index_hash_secret,
                "operation-id-v1",
                &format!("{tenant_id}\0{operation_kind}\0{actor_scope}\0{idempotency_key_hash}"),
                32,
            )
        )
    }

    async fn persist_operation_checkpoint(&self, record: &OperationRecord) -> Result<(), ApiError> {
        let receipt = self.repository.upsert_operation(record).await?;
        if let Err(error) = self
            .wait_for_repository_tasks(&receipt.task_uids, "durable_write")
            .await
        {
            // Preserve an already confirmed local checkpoint. In particular,
            // never publish a newly completed step locally when the journal
            // task that records that completion failed. If this is the first
            // checkpoint, retaining the pending immutable plan prevents a
            // same-process retry from constructing a different plan under the
            // deterministic operation ID without overstating durability.
            self.write()?
                .operations
                .entry(record.id.clone())
                .or_insert_with(|| record.clone());
            return Err(error);
        }
        self.write()?
            .operations
            .insert(record.id.clone(), record.clone());
        Ok(())
    }

    async fn record_operation_failure(
        &self,
        record: OperationRecord,
        step_id: &str,
        error: ApiError,
    ) -> Result<OperationRecord, ApiError> {
        let diagnostic = safe_cause_diagnostic(&error);
        let failed = operation_step_failed(
            &record,
            step_id,
            diagnostic.category,
            diagnostic.fingerprint,
            now(),
        )
        .map_err(|transition| {
            ApiError::Internal(format!(
                "invalid operation failure transition: {transition}"
            ))
        })?;
        if let Err(checkpoint_error) = self.persist_operation_checkpoint(&failed).await {
            let checkpoint_diagnostic = safe_cause_diagnostic(&checkpoint_error);
            tracing::error!(
                step_id,
                cause_category = checkpoint_diagnostic.category,
                cause_fingerprint = %checkpoint_diagnostic.fingerprint,
                "failed to persist operation failure checkpoint"
            );
            // Preserve the explicit failure locally even when the journal
            // checkpoint itself is unavailable, so the accepted primary is
            // never reported back as an unqualified generic error.
            self.write()?
                .operations
                .insert(failed.id.clone(), failed.clone());
        }
        Err(error)
    }

    async fn apply_operation_step_record(
        &self,
        mut record: OperationRecord,
        step_id: &str,
        wait_for_index: bool,
        dynamic_registry: Option<&HashMap<String, UserEventIndex>>,
    ) -> Result<OperationRecord, ApiError> {
        self.validate_operation_routing(&record)?;
        self.validate_operation_step_dynamic_membership(&record, step_id, dynamic_registry)?;
        let step = std::iter::once(&record.plan.primary)
            .chain(record.plan.side_effects.iter())
            .find(|step| step.id == step_id)
            .cloned()
            .ok_or_else(|| ApiError::Internal(format!("operation step {step_id} is missing")))?;
        let wait_for_index = wait_for_index
            || matches!(
                &step.resource,
                OperationResource::EnsureUserEventIndex { .. }
            );
        let progress =
            record.progress.steps.get(step_id).cloned().ok_or_else(|| {
                ApiError::Internal(format!("operation step {step_id} is missing"))
            })?;
        if progress.status == OperationStepStatus::Completed {
            return Ok(record);
        }
        if progress.status == OperationStepStatus::Submitted {
            if !wait_for_index {
                return Ok(record);
            }
            if let Err(error) = self
                .wait_for_repository_tasks(&progress.task_uids, "write")
                .await
            {
                return self.record_operation_failure(record, step_id, error).await;
            }
            record = operation_step_completed(&record, step_id, now()).map_err(|error| {
                ApiError::Internal(format!("invalid operation completion: {error}"))
            })?;
            self.persist_operation_checkpoint(&record).await?;
            return Ok(record);
        }
        let receipt = match self
            .repository
            .apply_operation_step(&record.tenant_id, &step)
            .await
        {
            Ok(receipt) => receipt,
            Err(error) => return self.record_operation_failure(record, step_id, error).await,
        };
        record = if receipt.task_uids.is_empty() {
            operation_step_completed(&record, step_id, now())
        } else {
            operation_step_accepted(&record, step_id, receipt.task_uids.clone(), now())
        }
        .map_err(|error| ApiError::Internal(format!("invalid operation transition: {error}")))?;
        self.persist_operation_checkpoint(&record).await?;

        if wait_for_index && !receipt.task_uids.is_empty() {
            if let Err(error) = self
                .wait_for_repository_tasks(&receipt.task_uids, "write")
                .await
            {
                return self.record_operation_failure(record, step_id, error).await;
            }
            record = operation_step_completed(&record, step_id, now()).map_err(|error| {
                ApiError::Internal(format!("invalid operation completion: {error}"))
            })?;
            self.persist_operation_checkpoint(&record).await?;
        }
        Ok(record)
    }

    fn publish_operation_step_cache(
        &self,
        staged: &StoreData,
        record: &OperationRecord,
        step_id: &str,
    ) -> Result<(), ApiError> {
        let step = std::iter::once(&record.plan.primary)
            .chain(record.plan.side_effects.iter())
            .find(|step| step.id == step_id)
            .ok_or_else(|| ApiError::Internal(format!("operation step {step_id} is missing")))?;
        let mut current = self.write()?;
        Self::publish_resource_cache(&mut current, staged, &record.tenant_id, &step.resource);
        current.operations.insert(record.id.clone(), record.clone());
        Ok(())
    }

    fn publish_operation_cache(
        &self,
        staged: &StoreData,
        record: &OperationRecord,
    ) -> Result<(), ApiError> {
        let mut current = self.write()?;
        for step in std::iter::once(&record.plan.primary).chain(record.plan.side_effects.iter()) {
            Self::publish_resource_cache(&mut current, staged, &record.tenant_id, &step.resource);
        }
        current.operations.insert(record.id.clone(), record.clone());
        Ok(())
    }

    /// Project one durably accepted operation resource into the live cache.
    ///
    /// The staged store is a planning snapshot and may be stale by the time a
    /// repository write returns. Publishing the complete snapshot would both
    /// erase concurrent read-through fills and expose side effects whose own
    /// writes were never accepted. Each arm below therefore owns only the
    /// cache keys represented by that operation resource.
    fn publish_resource_cache(
        current: &mut StoreData,
        staged: &StoreData,
        tenant_id: &str,
        resource: &OperationResource,
    ) {
        match resource {
            OperationResource::EnsureUserEventIndex { index } => {
                current.user_indexes.insert(
                    (index.tenant_id.clone(), index.owner_user_id_hash.clone()),
                    index.clone(),
                );
                current
                    .events_by_index
                    .entry(index.event_index_uid.clone())
                    .or_default();
                current
                    .personal_context
                    .entry(index.personal_context_index_uid.clone())
                    .or_default();
            }
            OperationResource::HistoryEvents { events } => {
                let event_ids = events
                    .iter()
                    .map(|event| event.id.clone())
                    .collect::<HashSet<_>>();
                copy_idempotency_entries(
                    &mut current.event_idempotency,
                    &staged.event_idempotency,
                    &event_ids,
                );
                for event in events {
                    if let Some(hash) = &event.idempotency_key_hash {
                        current.event_idempotency.insert(
                            (event.event_index_uid.clone(), hash.clone()),
                            event.id.clone(),
                        );
                    }
                    let index_events = current
                        .events_by_index
                        .entry(event.event_index_uid.clone())
                        .or_default();
                    if let Some(existing) = index_events
                        .iter_mut()
                        .find(|existing| existing.id == event.id)
                    {
                        *existing = event.clone();
                    } else {
                        index_events.push(event.clone());
                    }
                    current.event_by_id.insert(event.id.clone(), event.clone());
                }
            }
            OperationResource::ContextNodes { index_uid, nodes } => {
                if index_uid == "rag_company_context" {
                    upsert_context_nodes(&mut current.company_context, nodes.clone());
                } else {
                    upsert_context_nodes(
                        current
                            .personal_context
                            .entry(index_uid.clone())
                            .or_default(),
                        nodes.clone(),
                    );
                }
            }
            OperationResource::StateItem { item } => {
                let key = (
                    item.tenant_id.clone(),
                    item.owner_user_id.clone(),
                    item.natural_key.clone(),
                );
                if current
                    .state_items
                    .get(&key)
                    .is_none_or(|existing| existing.updated_at <= item.updated_at)
                {
                    current.state_items.insert(key, item.clone());
                }
            }
            OperationResource::Insight { insight } => {
                let insight_ids = HashSet::from([insight.id.clone()]);
                copy_idempotency_entries(
                    &mut current.insight_idempotency,
                    &staged.insight_idempotency,
                    &insight_ids,
                );
                if current
                    .insights
                    .get(&insight.id)
                    .is_none_or(|existing| existing.updated_at <= insight.updated_at)
                {
                    current.insights.insert(insight.id.clone(), insight.clone());
                }
            }
            OperationResource::CompanySource { source } => {
                current.sources.insert(source.id.clone(), source.clone());
            }
            OperationResource::SourceRevision { revision } => {
                let revisions = current
                    .source_revisions
                    .entry(revision.source_id.clone())
                    .or_default();
                if let Some(existing) = revisions
                    .iter_mut()
                    .find(|existing| existing.id == revision.id)
                {
                    *existing = revision.clone();
                } else {
                    revisions.push(revision.clone());
                }
                revisions.sort_by_key(|revision| revision.created_at);
            }
            OperationResource::DeleteCompanySourceIndex { source_id, target } => match target {
                CompanySourceDeleteTarget::Fragments => {
                    current.company_context.retain(|node| {
                        node.tenant_id != tenant_id || node.source_id.as_deref() != Some(source_id)
                    });
                }
                CompanySourceDeleteTarget::Revisions => {
                    let remove_entry =
                        if let Some(revisions) = current.source_revisions.get_mut(source_id) {
                            revisions.retain(|revision| revision.tenant_id != tenant_id);
                            revisions.is_empty()
                        } else {
                            false
                        };
                    if remove_entry {
                        current.source_revisions.remove(source_id);
                    }
                }
                CompanySourceDeleteTarget::Source => {
                    if current
                        .sources
                        .get(source_id)
                        .is_some_and(|source| source.tenant_id == tenant_id)
                    {
                        current.sources.remove(source_id);
                    }
                }
                CompanySourceDeleteTarget::SourceDocuments => {
                    let removed_document_uris = current
                        .source_documents
                        .values()
                        .filter(|document| {
                            document.tenant_id == tenant_id
                                && document.owner_user_id.is_none()
                                && document.source_id == *source_id
                        })
                        .map(|document| document.uri.clone())
                        .collect::<HashSet<_>>();
                    current.source_documents.retain(|_, document| {
                        document.tenant_id != tenant_id
                            || document.owner_user_id.is_some()
                            || document.source_id != *source_id
                    });
                    current.parsed_blocks.retain(|key, _| {
                        key.tenant_id != tenant_id
                            || key.owner_user_id.is_some()
                            || !removed_document_uris.contains(&key.uri)
                    });
                }
                CompanySourceDeleteTarget::ParseArtifacts => {
                    current.parse_artifacts.retain(|_, artifact| {
                        artifact.tenant_id != tenant_id
                            || artifact.owner_user_id.is_some()
                            || artifact.source_id != *source_id
                    });
                }
                CompanySourceDeleteTarget::IngestTasks => {
                    current.ingest_tasks.retain(|_, task| {
                        task.tenant_id != tenant_id
                            || task.owner_user_id.is_some()
                            || task.source_id != *source_id
                    });
                }
                CompanySourceDeleteTarget::IngestResults => {
                    current.ingest_results.retain(|_, result| {
                        result.task.tenant_id != tenant_id
                            || result.task.owner_user_id.is_some()
                            || result.source_id != *source_id
                    });
                }
                CompanySourceDeleteTarget::Links { link_ids, .. } => {
                    let link_ids = link_ids.iter().collect::<HashSet<_>>();
                    current.links.retain(|link_id, link| {
                        link.tenant_id != tenant_id || !link_ids.contains(link_id)
                    });
                    current
                        .link_idempotency
                        .retain(|_, link_id| !link_ids.contains(link_id));
                }
            },
            OperationResource::SourceDocuments { documents } => {
                for document in documents {
                    upsert_source_document_cache(current, document.clone());
                }
            }
            OperationResource::ParseArtifacts { artifacts } => {
                for artifact in artifacts {
                    current
                        .parse_artifacts
                        .insert(ParseArtifactKey::from_artifact(artifact), artifact.clone());
                }
            }
            OperationResource::StructuredSnapshot { snapshot } => {
                let snapshot_ids = HashSet::from([snapshot.id.clone()]);
                copy_idempotency_entries(
                    &mut current.snapshot_idempotency,
                    &staged.snapshot_idempotency,
                    &snapshot_ids,
                );
                current
                    .snapshots
                    .insert(snapshot.id.clone(), snapshot.clone());
            }
            OperationResource::Dataset { dataset } => {
                current
                    .datasets
                    .insert(dataset.dataset_key.clone(), dataset.clone());
            }
            OperationResource::StructuredRows { rows } => {
                for row in rows {
                    let Some(snapshot_id) = row.get("snapshot_id").and_then(Value::as_str) else {
                        continue;
                    };
                    let row_id = row.get("id").and_then(Value::as_str);
                    let cached_rows = current
                        .rows_by_snapshot
                        .entry(snapshot_id.to_string())
                        .or_default();
                    if let Some(row_id) = row_id {
                        if let Some(existing) = cached_rows.iter_mut().find(|existing| {
                            existing.get("id").and_then(Value::as_str) == Some(row_id)
                        }) {
                            *existing = row.clone();
                        } else {
                            cached_rows.push(row.clone());
                        }
                        current
                            .row_idempotency
                            .insert((snapshot_id.to_string(), row_id.to_string()));
                    } else if !cached_rows.contains(row) {
                        cached_rows.push(row.clone());
                    }
                }
            }
            OperationResource::StructuredSummary { summary } => {
                if let Some(id) = summary.get("id").and_then(Value::as_str) {
                    current
                        .structured_summaries
                        .insert(id.to_string(), summary.clone());
                }
            }
            OperationResource::Session { session } => {
                current.sessions.insert(session.id.clone(), session.clone());
            }
            OperationResource::Trace { trace } => {
                current
                    .traces
                    .entry(trace.id.clone())
                    .or_insert_with(|| trace.clone());
            }
            OperationResource::Links { links } => {
                let link_ids = links
                    .iter()
                    .map(|link| link.id.clone())
                    .collect::<HashSet<_>>();
                copy_idempotency_entries(
                    &mut current.link_idempotency,
                    &staged.link_idempotency,
                    &link_ids,
                );
                for link in links {
                    if current
                        .links
                        .get(&link.id)
                        .is_none_or(|existing| existing.updated_at <= link.updated_at)
                    {
                        current.links.insert(link.id.clone(), link.clone());
                    }
                }
            }
            OperationResource::HarnessComponents {
                components,
                revisions,
            } => {
                current.harness_components.extend(
                    components
                        .iter()
                        .cloned()
                        .map(|component| (component.id.clone(), component)),
                );
                for revision in revisions {
                    let component_revisions = current
                        .harness_revisions
                        .entry(revision.component_id.clone())
                        .or_default();
                    if let Some(existing) = component_revisions
                        .iter_mut()
                        .find(|existing| existing.id == revision.id)
                    {
                        *existing = revision.clone();
                    } else {
                        component_revisions.push(revision.clone());
                    }
                    component_revisions.sort_by_key(|revision| revision.iteration);
                }
            }
            OperationResource::HarnessChanges { changes } => {
                current.harness_changes.extend(
                    changes
                        .iter()
                        .cloned()
                        .map(|change| (change.id.clone(), change)),
                );
            }
            OperationResource::HarnessVerdicts { verdicts } => {
                current.harness_verdicts.extend(
                    verdicts
                        .iter()
                        .cloned()
                        .map(|verdict| (verdict.id.clone(), verdict)),
                );
            }
            OperationResource::IngestTask { task } => {
                upsert_ingest_task_cache(current, task.clone());
            }
            OperationResource::IngestTasks { tasks } => {
                for task in tasks {
                    upsert_ingest_task_cache(current, task.clone());
                }
            }
            OperationResource::DeleteIngestTasks { task_ids } => {
                for task_id in task_ids {
                    current.ingest_tasks.remove(task_id);
                    if let Some(result) = current.ingest_results.remove(task_id) {
                        current.parsed_blocks.remove(&SourceDocumentKey::new(
                            &result.task.tenant_id,
                            result.task.owner_user_id.as_deref(),
                            &result.source_document_uri,
                        ));
                    }
                }
            }
            OperationResource::IngestResult { result } => {
                let should_replace = current
                    .ingest_results
                    .get(&result.task.task_id)
                    .is_none_or(|existing| existing.task.updated_at <= result.task.updated_at);
                if should_replace {
                    current.parsed_blocks.insert(
                        SourceDocumentKey::new(
                            &result.task.tenant_id,
                            result.task.owner_user_id.as_deref(),
                            &result.source_document_uri,
                        ),
                        result.parsed_blocks.clone(),
                    );
                    current
                        .ingest_results
                        .insert(result.task.task_id.clone(), result.clone());
                }
            }
            OperationResource::EvalCase { case } => {
                current.eval_cases.insert(case.id.clone(), case.clone());
            }
            OperationResource::EvalRun { run } => {
                current.eval_runs.insert(run.id.clone(), run.clone());
            }
            OperationResource::EvalCaseResults { results } => {
                current.eval_case_results.extend(
                    results
                        .iter()
                        .cloned()
                        .map(|result| (result.id.clone(), result)),
                );
            }
            OperationResource::EvalOverview { overview } => {
                current
                    .eval_overviews
                    .insert(overview.run_id.clone(), overview.clone());
            }
        }
    }

    fn operation_primary_was_accepted(record: &OperationRecord) -> bool {
        record
            .progress
            .steps
            .get(&record.plan.primary.id)
            .is_some_and(|progress| {
                matches!(
                    progress.status,
                    OperationStepStatus::Submitted | OperationStepStatus::Completed
                )
            })
    }

    fn operation_primary_response_was_exposed(record: &OperationRecord) -> bool {
        record
            .plan
            .redacted_metadata
            .get("wait_for_index")
            .and_then(Value::as_bool)
            == Some(false)
            && record
                .progress
                .steps
                .get(&record.plan.primary.id)
                .is_some_and(|progress| {
                    !progress.task_uids.is_empty()
                        || progress.status == OperationStepStatus::Completed
                })
    }

    fn replay_operation_response<R>(record: &OperationRecord) -> Result<R, ApiError>
    where
        R: DeserializeOwned,
    {
        if record.plan.response_snapshot.is_null() {
            return Err(ApiError::conflict(
                "the existing operation predates idempotent response replay",
            ));
        }
        let mut snapshot = record.plan.response_snapshot.clone();
        if let Value::Object(fields) = &mut snapshot {
            if fields.contains_key("duplicate") {
                fields.insert("duplicate".to_string(), Value::Bool(true));
            }
        }
        serde_json::from_value(snapshot).map_err(|error| {
            ApiError::Internal(format!(
                "operation {} contains an invalid response snapshot: {error}",
                record.id
            ))
        })
    }

    async fn replay_existing_operation<R>(
        &self,
        operation: OperationRecord,
        expose_partial_persistence: bool,
    ) -> Result<(R, Option<PersistenceMetadata>), ApiError>
    where
        R: DeserializeOwned,
    {
        let operation_id = operation.id.clone();
        // A RYW response may already have been returned while its accepted
        // backend task was still pending. Preserve that historical commit
        // boundary even if confirmation now transitions the step to failed.
        let primary_response_was_exposed = Self::operation_primary_response_was_exposed(&operation);
        let operation = match self.reconcile_operation_record(operation).await {
            Ok(operation) => operation,
            Err(error) => {
                let latest = self.read()?.operations.get(&operation_id).cloned();
                match latest {
                    Some(latest)
                        if expose_partial_persistence
                            && (primary_response_was_exposed
                                || Self::operation_primary_response_was_exposed(&latest)
                                || Self::operation_primary_was_accepted(&latest)) =>
                    {
                        latest
                    }
                    _ => return Err(error),
                }
            }
        };
        let response = Self::replay_operation_response(&operation)?;
        Ok((response, Some(persistence_metadata(&operation))))
    }

    async fn execute_staged_mutation<R, F>(
        &self,
        tenant_id: &str,
        operation_kind: &str,
        owner_user_id: Option<&str>,
        idempotency_key: Option<&str>,
        primary_kind: MutationPrimary,
        mutate: F,
    ) -> Result<(R, Option<PersistenceMetadata>), ApiError>
    where
        R: Serialize + DeserializeOwned,
        F: FnOnce(&Store) -> Result<R, ApiError>,
    {
        Box::pin(self.execute_staged_mutation_with_idempotency(
            tenant_id,
            operation_kind,
            owner_user_id,
            MutationIdempotency {
                key: idempotency_key,
                request_fingerprint: None,
            },
            primary_kind,
            mutate,
        ))
        .await
    }

    async fn execute_staged_mutation_with_idempotency<R, F>(
        &self,
        tenant_id: &str,
        operation_kind: &str,
        owner_user_id: Option<&str>,
        idempotency: MutationIdempotency<'_>,
        primary_kind: MutationPrimary,
        mutate: F,
    ) -> Result<(R, Option<PersistenceMetadata>), ApiError>
    where
        R: Serialize + DeserializeOwned,
        F: FnOnce(&Store) -> Result<R, ApiError>,
    {
        let _mutation_guard = self.mutation_gate.lock().await;
        Box::pin(self.execute_staged_mutation_guarded(
            tenant_id,
            operation_kind,
            owner_user_id,
            idempotency,
            primary_kind,
            mutate,
        ))
        .await
    }

    fn ensure_operation_request_matches(
        operation: &OperationRecord,
        expected_fingerprint: Option<&str>,
    ) -> Result<(), ApiError> {
        let Some(expected_fingerprint) = expected_fingerprint else {
            return Ok(());
        };
        let actual_fingerprint = operation
            .plan
            .redacted_metadata
            .get("request_fingerprint")
            .and_then(Value::as_str);
        if actual_fingerprint != Some(expected_fingerprint) {
            return Err(ApiError::conflict(
                "idempotency key was already used for a different request",
            ));
        }
        Ok(())
    }

    async fn execute_staged_mutation_guarded<R, F>(
        &self,
        tenant_id: &str,
        operation_kind: &str,
        owner_user_id: Option<&str>,
        idempotency: MutationIdempotency<'_>,
        primary_kind: MutationPrimary,
        mutate: F,
    ) -> Result<(R, Option<PersistenceMetadata>), ApiError>
    where
        R: Serialize + DeserializeOwned,
        F: FnOnce(&Store) -> Result<R, ApiError>,
    {
        let MutationIdempotency {
            key: idempotency_key,
            request_fingerprint,
        } = idempotency;
        if let Some(idempotency_key) = idempotency_key {
            let idempotency_key_hash = self.resolver.idempotency_hash(idempotency_key);
            let owner_user_id_hash = owner_user_id.map(|owner| self.resolver.user_hash(owner));
            let operation_id = self.idempotent_operation_id(
                tenant_id,
                operation_kind,
                owner_user_id_hash.as_deref(),
                &idempotency_key_hash,
            );
            let existing_local = { self.read()?.operations.get(&operation_id).cloned() };
            let existing = match existing_local {
                Some(existing) => Some(existing),
                None => {
                    self.repository
                        .get_operation(tenant_id, &operation_id)
                        .await?
                }
            };
            if let Some(existing) = existing {
                Self::ensure_operation_request_matches(&existing, request_fingerprint)?;
                return self
                    .replay_existing_operation(existing, primary_kind.exposes_partial_persistence())
                    .await;
            }
        }

        let staged = self.staged_copy()?;
        let before = staged.read()?.clone();
        let response = mutate(&staged)?;
        let after = staged.read()?.clone();
        let resources = self.mutation_resources(tenant_id, &before, &after);
        if resources.is_empty() {
            return Ok((response, None));
        }

        let response_snapshot = match idempotency_key {
            Some(_) => serde_json::to_value(&response).map_err(|error| {
                ApiError::Internal(format!("failed to snapshot mutation response: {error}"))
            })?,
            None => Value::Null,
        };
        let plan = self.mutation_plan(MutationPlanInput {
            tenant_id,
            operation_kind,
            owner_user_id,
            idempotency_key,
            primary_kind,
            resources,
            response_snapshot,
            request_fingerprint,
        })?;
        let existing_local = { self.read()?.operations.get(&plan.id).cloned() };
        let existing = if existing_local.is_some() {
            existing_local
        } else {
            self.repository.get_operation(tenant_id, &plan.id).await?
        };
        if let Some(existing) = existing {
            Self::ensure_operation_request_matches(&existing, request_fingerprint)?;
            return self
                .replay_existing_operation(existing, primary_kind.exposes_partial_persistence())
                .await;
        }

        let mut record = operation_record_from_plan(plan)
            .map_err(|error| ApiError::Internal(format!("invalid mutation plan: {error}")))?;
        self.persist_operation_checkpoint(&record).await?;
        let wait_for_index = self.redaction_config.write_consistency
            == WriteConsistency::WaitForIndex
            || primary_kind.requires_index_confirmation();
        let buffer_publication =
            wait_for_index && !matches!(primary_kind, MutationPrimary::DeleteCompanySource);
        let mut dynamic_registry = self.current_user_index_registry(tenant_id)?;
        let primary_step_id = record.plan.primary.id.clone();
        record = match self
            .apply_operation_step_record(
                record.clone(),
                &primary_step_id,
                wait_for_index,
                Some(&dynamic_registry),
            )
            .await
        {
            Ok(record) => record,
            Err(error) => return Err(error),
        };
        Self::record_completed_registry_step(&record, &primary_step_id, &mut dynamic_registry)?;
        if !buffer_publication {
            self.publish_operation_step_cache(&after, &record, &primary_step_id)?;
        }

        let side_effect_ids = record
            .plan
            .side_effects
            .iter()
            .map(|step| step.id.clone())
            .collect::<Vec<_>>();
        for step_id in side_effect_ids {
            match self
                .apply_operation_step_record(
                    record.clone(),
                    &step_id,
                    wait_for_index,
                    Some(&dynamic_registry),
                )
                .await
            {
                Ok(updated) => {
                    record = updated;
                    Self::record_completed_registry_step(&record, &step_id, &mut dynamic_registry)?;
                    if !buffer_publication {
                        self.publish_operation_step_cache(&after, &record, &step_id)?;
                    }
                }
                Err(error) => {
                    let latest = self
                        .read()?
                        .operations
                        .get(&record.id)
                        .cloned()
                        .unwrap_or_else(|| record.clone());
                    if primary_kind.exposes_partial_persistence()
                        && Self::operation_primary_was_accepted(&latest)
                    {
                        return Ok((response, Some(persistence_metadata(&latest))));
                    }
                    return Err(error);
                }
            }
        }
        if buffer_publication {
            self.publish_operation_cache(&after, &record)?;
        }
        Ok((response, Some(persistence_metadata(&record))))
    }

    /// Journal a repository write whose desired resource already matches the
    /// cache, such as an explicit settings reapplication. These writes have
    /// no staged StoreData delta, but they still require the same durable
    /// operation record and task tracking as ordinary mutations.
    async fn execute_explicit_resource_operation(
        &self,
        tenant_id: &str,
        operation_kind: &str,
        owner_user_id: Option<&str>,
        primary_kind: MutationPrimary,
        resource: OperationResource,
    ) -> Result<PersistenceMetadata, ApiError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let cache_projection = self.read()?.clone();
        let plan = self.mutation_plan(MutationPlanInput {
            tenant_id,
            operation_kind,
            owner_user_id,
            idempotency_key: None,
            primary_kind,
            resources: vec![resource],
            response_snapshot: Value::Null,
            request_fingerprint: None,
        })?;
        let mut record = operation_record_from_plan(plan)
            .map_err(|error| ApiError::Internal(format!("invalid mutation plan: {error}")))?;
        self.persist_operation_checkpoint(&record).await?;
        let primary_step_id = record.plan.primary.id.clone();
        let wait_for_index =
            self.redaction_config.write_consistency == WriteConsistency::WaitForIndex;
        record = self
            .apply_operation_step_record(record, &primary_step_id, wait_for_index, None)
            .await?;
        self.publish_operation_step_cache(&cache_projection, &record, &primary_step_id)?;
        Ok(persistence_metadata(&record))
    }

    /// Acquire the vector matcher.
    ///
    /// The matcher mutex is leaf-level: no data lock is ever acquired while
    /// it is held (the matcher never touches `inner`), and every caller
    /// acquires data locks first when it needs both, so the order
    /// `data -> vector` is consistent and cannot deadlock. A poisoned lock
    /// recovers the matcher and keeps serving — vector scoring degrades,
    /// never the search itself.
    fn vector_matcher(&self) -> std::sync::MutexGuard<'_, VectorMatcher> {
        self.vector.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("vector matcher lock poisoned; recovering matcher state");
            poisoned.into_inner()
        })
    }

    /// Per-query turbovec scores for candidate fragments.
    fn vector_score_map(&self, query: &str, nodes: &[ContextNode]) -> VectorScoreMap {
        self.vector_matcher().score_map(
            query,
            nodes
                .iter()
                .map(|node| (vector_match_key(node), node_match_text(node))),
        )
    }

    /// Document-level vector scores for the source documents referenced by
    /// a candidate set; candidates come from [`doc_candidates_locked`].
    fn vector_doc_score_map(
        &self,
        query: &str,
        candidates: Vec<(String, String)>,
    ) -> VectorScoreMap {
        self.vector_matcher().doc_score_map(query, candidates)
    }

    /// Pre-embed saved documents and fragments. Best-effort warm-up: the
    /// scoring paths lazily embed anything this missed or that predates it.
    fn vector_warm(&self, entries: Vec<(String, String)>) {
        self.vector_matcher().warm(entries);
    }

    pub fn resolver(&self) -> &EventIndexResolver {
        &self.resolver
    }

    pub fn backend_name(&self) -> &'static str {
        self.repository.backend_name()
    }

    pub fn hydration_report(&self) -> Result<HydrationReport, ApiError> {
        self.read()?
            .hydration_report
            .clone()
            .ok_or_else(|| ApiError::Internal("hydration report is unavailable".to_string()))
    }

    pub fn hydration_ready(&self) -> bool {
        self.hydration_report()
            .map(|report| report.ready)
            .unwrap_or(false)
    }

    fn validate_operation_routing(&self, record: &OperationRecord) -> Result<(), ApiError> {
        validate_operation_record(record).map_err(|error| {
            ApiError::Internal(format!("invalid persisted operation record: {error}"))
        })?;
        let tenant_hash = self.resolver.tenant_hash(&record.tenant_id);
        let analysis_target_owner_hash = (record.operation_kind == "analysis.materialize")
            .then(|| {
                record
                    .plan
                    .redacted_metadata
                    .get("target_owner_user_id_hash")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ApiError::Internal(
                            "analysis operation is missing its target owner commitment".to_string(),
                        )
                    })
            })
            .transpose()?;
        for step in std::iter::once(&record.plan.primary).chain(record.plan.side_effects.iter()) {
            match &step.resource {
                OperationResource::EnsureUserEventIndex { index } => {
                    let expected_id = user_event_index_id(&tenant_hash, &index.owner_user_id_hash);
                    let expected_event_uid = format!(
                        "rag_events__t_{tenant_hash}__u_{}",
                        index.owner_user_id_hash
                    );
                    let expected_context_uid = format!(
                        "rag_context__t_{tenant_hash}__u_{}",
                        index.owner_user_id_hash
                    );
                    if index.tenant_id != record.tenant_id
                        || index.tenant_hash != tenant_hash
                        || index.id != expected_id
                        || index.event_index_uid != expected_event_uid
                        || index.personal_context_index_uid != expected_context_uid
                        || index.schema_version != EVENT_INDEX_SCHEMA_VERSION
                        || index.settings_hash != EVENT_SETTINGS_HASH
                        || index.status != "active"
                        || analysis_target_owner_hash
                            .is_some_and(|expected| index.owner_user_id_hash != expected)
                    {
                        return Err(ApiError::Internal(
                            "persisted operation contains an invalid user-index route".to_string(),
                        ));
                    }
                }
                OperationResource::HistoryEvents { events } => {
                    for event in events {
                        let routing = self.resolver.resolve(
                            &record.tenant_id,
                            &event.owner_user_id,
                            false,
                            true,
                        )?;
                        if event.tenant_id != record.tenant_id
                            || event.owner_user_id_hash != routing.owner_user_id_hash
                            || event.event_index_uid != routing.event_index_uid
                            || event.event_index_schema_version != EVENT_INDEX_SCHEMA_VERSION
                            || analysis_target_owner_hash
                                .is_some_and(|expected| routing.owner_user_id_hash != expected)
                        {
                            return Err(ApiError::Internal(
                                "persisted operation contains an invalid history-event route"
                                    .to_string(),
                            ));
                        }
                    }
                }
                OperationResource::ContextNodes { index_uid, nodes } => {
                    for node in nodes {
                        if node.tenant_id != record.tenant_id || node.index_uid != *index_uid {
                            return Err(ApiError::Internal(
                                "persisted operation contains an invalid context route".to_string(),
                            ));
                        }
                        match node.owner_user_id.as_deref() {
                            Some(owner) => {
                                let routing =
                                    self.resolver
                                        .resolve(&record.tenant_id, owner, false, true)?;
                                if *index_uid != routing.personal_context_index_uid
                                    || node.index_kind != "personal"
                                    || node.privacy != "private"
                                    || analysis_target_owner_hash.is_some_and(|expected| {
                                        routing.owner_user_id_hash != expected
                                    })
                                {
                                    return Err(ApiError::Internal(
                                        "persisted operation contains an invalid personal-context route"
                                            .to_string(),
                                    ));
                                }
                            }
                            None => {
                                if index_uid != "rag_company_context"
                                    || node.index_kind != "company"
                                    || node.privacy != "company"
                                {
                                    return Err(ApiError::Internal(
                                        "persisted operation contains an invalid company-context route"
                                            .to_string(),
                                    ));
                                }
                            }
                        }
                    }
                }
                OperationResource::Insight { insight } => {
                    if analysis_target_owner_hash.is_some_and(|expected| {
                        self.resolver.user_hash(&insight.owner_user_id) != expected
                    }) {
                        return Err(ApiError::Internal(
                            "analysis insight does not match its target owner commitment"
                                .to_string(),
                        ));
                    }
                }
                OperationResource::Links { links }
                    if analysis_target_owner_hash.is_some_and(|expected| {
                        links.iter().any(|link| {
                            link.owner_user_id
                                .as_deref()
                                .is_none_or(|owner| self.resolver.user_hash(owner) != expected)
                        })
                    }) =>
                {
                    return Err(ApiError::Internal(
                        "analysis link does not match its target owner commitment".to_string(),
                    ));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn validate_operation_step_dynamic_membership(
        &self,
        record: &OperationRecord,
        step_id: &str,
        dynamic_registry: Option<&HashMap<String, UserEventIndex>>,
    ) -> Result<(), ApiError> {
        let step = std::iter::once(&record.plan.primary)
            .chain(record.plan.side_effects.iter())
            .find(|step| step.id == step_id)
            .ok_or_else(|| ApiError::Internal(format!("operation step {step_id} is missing")))?;
        let mut requirements = Vec::new();
        match &step.resource {
            OperationResource::EnsureUserEventIndex { index } => {
                let progress = record.progress.steps.get(step_id).ok_or_else(|| {
                    ApiError::Internal(format!("operation step {step_id} is missing"))
                })?;
                if progress.status != OperationStepStatus::Completed {
                    return Ok(());
                }
                requirements.push((
                    index.owner_user_id_hash.clone(),
                    Some(index.event_index_uid.clone()),
                    Some(index.personal_context_index_uid.clone()),
                    Some(index),
                ));
            }
            OperationResource::HistoryEvents { events } => {
                for event in events {
                    requirements.push((
                        event.owner_user_id_hash.clone(),
                        Some(event.event_index_uid.clone()),
                        None,
                        None,
                    ));
                }
            }
            OperationResource::ContextNodes { index_uid, nodes } => {
                for node in nodes {
                    if let Some(owner_user_id) = node.owner_user_id.as_deref() {
                        let owner_hash = self.resolver.user_hash(owner_user_id);
                        requirements.push((owner_hash, None, Some(index_uid.clone()), None));
                    }
                }
            }
            _ => return Ok(()),
        }
        if requirements.is_empty() {
            return Ok(());
        }

        let live_registry = if dynamic_registry.is_none() {
            Some(self.read()?)
        } else {
            None
        };
        for (owner_hash, event_uid, context_uid, exact_index) in requirements {
            let index = match dynamic_registry {
                Some(registry) => registry.get(&owner_hash),
                None => live_registry.as_ref().and_then(|data| {
                    data.user_indexes
                        .get(&(record.tenant_id.clone(), owner_hash.clone()))
                }),
            };
            let Some(index) = index else {
                return Err(ApiError::Internal(
                    "persisted operation references an unregistered dynamic index".to_string(),
                ));
            };
            if index.tenant_id != record.tenant_id
                || index.owner_user_id_hash != owner_hash
                || index.status != "active"
                || index.schema_version != EVENT_INDEX_SCHEMA_VERSION
                || index.settings_hash != EVENT_SETTINGS_HASH
                || event_uid
                    .as_deref()
                    .is_some_and(|uid| index.event_index_uid != uid)
                || context_uid
                    .as_deref()
                    .is_some_and(|uid| index.personal_context_index_uid != uid)
                || exact_index
                    .is_some_and(|expected| !Self::same_user_index_identity(index, expected))
            {
                return Err(ApiError::Internal(
                    "persisted operation dynamic index is not in the reconciled registry"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    fn current_user_index_registry(
        &self,
        tenant_id: &str,
    ) -> Result<HashMap<String, UserEventIndex>, ApiError> {
        Ok(self
            .read()?
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .cloned()
            .map(|index| (index.owner_user_id_hash.clone(), index))
            .collect())
    }

    async fn reconcile_operation_record(
        &self,
        record: OperationRecord,
    ) -> Result<OperationRecord, ApiError> {
        let mut registry = self.current_user_index_registry(&record.tenant_id)?;
        self.reconcile_operation_record_with_startup_registry(record, Some(&mut registry))
            .await
    }

    async fn reconcile_operation_record_with_startup_registry(
        &self,
        mut record: OperationRecord,
        mut startup_registry: Option<&mut HashMap<String, UserEventIndex>>,
    ) -> Result<OperationRecord, ApiError> {
        let cache_projection = self.read()?.clone();
        let buffer_publication = record
            .plan
            .redacted_metadata
            .get("wait_for_index")
            .and_then(Value::as_bool)
            == Some(true)
            && !matches!(
                &record.plan.primary.resource,
                OperationResource::DeleteCompanySourceIndex { .. }
            );
        let primary_id = record.plan.primary.id.clone();
        record = self
            .apply_operation_step_record(record, &primary_id, true, startup_registry.as_deref())
            .await?;
        if let Some(registry) = startup_registry.as_deref_mut() {
            Self::record_completed_registry_step(&record, &primary_id, registry)?;
        }
        if !buffer_publication {
            self.publish_operation_step_cache(&cache_projection, &record, &primary_id)?;
        }
        let side_effect_ids = record
            .plan
            .side_effects
            .iter()
            .map(|step| step.id.clone())
            .collect::<Vec<_>>();
        for step_id in side_effect_ids {
            record = self
                .apply_operation_step_record(record, &step_id, true, startup_registry.as_deref())
                .await?;
            if let Some(registry) = startup_registry.as_deref_mut() {
                Self::record_completed_registry_step(&record, &step_id, registry)?;
            }
            if !buffer_publication {
                self.publish_operation_step_cache(&cache_projection, &record, &step_id)?;
            }
        }
        if buffer_publication {
            self.publish_operation_cache(&cache_projection, &record)?;
        }
        Ok(record)
    }

    fn record_completed_registry_step(
        record: &OperationRecord,
        step_id: &str,
        registry: &mut HashMap<String, UserEventIndex>,
    ) -> Result<(), ApiError> {
        let progress =
            record.progress.steps.get(step_id).ok_or_else(|| {
                ApiError::Internal(format!("operation step {step_id} is missing"))
            })?;
        if progress.status != OperationStepStatus::Completed {
            return Ok(());
        }
        let step = std::iter::once(&record.plan.primary)
            .chain(record.plan.side_effects.iter())
            .find(|step| step.id == step_id)
            .ok_or_else(|| ApiError::Internal(format!("operation step {step_id} is missing")))?;
        if let OperationResource::EnsureUserEventIndex { index } = &step.resource {
            registry.insert(index.owner_user_id_hash.clone(), index.clone());
        }
        Ok(())
    }

    async fn reconcile_repository_operations_startup(
        &self,
        tenant_id: &str,
        startup_registry: &mut HashMap<String, UserEventIndex>,
    ) -> Result<(), ApiError> {
        loop {
            let mut operations = self
                .repository
                .list_oldest_reconcilable_operations(
                    tenant_id,
                    &[],
                    OPERATION_STARTUP_RECONCILE_BATCH_SIZE,
                )
                .await?
                .ok_or_else(|| {
                    ApiError::Internal(
                        "operation journal is unavailable for the configured backend".to_string(),
                    )
                })?;
            if operations.is_empty() {
                return Ok(());
            }
            if operations.len() > OPERATION_STARTUP_RECONCILE_BATCH_SIZE {
                return Err(ApiError::Internal(
                    "operation journal returned more startup candidates than requested".to_string(),
                ));
            }
            operations.sort_by(|left, right| {
                left.created_at
                    .cmp(&right.created_at)
                    .then_with(|| left.id.cmp(&right.id))
            });
            for operation in operations {
                if operation.tenant_id != tenant_id {
                    return Err(ApiError::Internal(
                        "operation journal returned a cross-tenant record".to_string(),
                    ));
                }
                validate_operation_record(&operation).map_err(|error| {
                    ApiError::Internal(format!(
                        "operation journal returned an invalid record: {error}"
                    ))
                })?;
                if operation.status == OperationStatus::Completed
                    && operation.indexing_state == OperationIndexingState::Completed
                {
                    return Err(ApiError::Internal(
                        "operation journal returned a completed startup candidate".to_string(),
                    ));
                }
                self.reconcile_operation_record_with_startup_registry(
                    operation,
                    Some(&mut *startup_registry),
                )
                .await?;
            }
        }
    }

    pub async fn list_operations(
        &self,
        tenant_id: &str,
        req: OperationListRequest,
    ) -> Result<OperationListResponse, ApiError> {
        if req.limit == 0 || req.limit > 500 {
            return Err(ApiError::bad_request("limit must be between 1 and 500"));
        }
        let cursor = decode_operation_list_cursor(
            &self.redaction_config.index_hash_secret,
            tenant_id,
            &req.statuses,
            &req.operation_kinds,
            req.cursor.as_deref(),
        )?;
        let offset = cursor.as_ref().map_or(0, |cursor| cursor.offset);
        let previous_operation_id = cursor
            .as_ref()
            .map(|cursor| cursor.previous_operation_id.as_str());

        if let Some(page) = self
            .repository
            .list_operation_page(RepositoryOperationListQuery {
                tenant_id,
                statuses: &req.statuses,
                operation_kinds: &req.operation_kinds,
                offset,
                previous_operation_id,
                limit: req.limit,
                include_plan: req.include_plan,
            })
            .await?
        {
            if page.operations.iter().any(|operation| {
                operation.summary.tenant_id != tenant_id
                    || (!req.statuses.is_empty()
                        && !req.statuses.contains(&operation.summary.status))
                    || (!req.operation_kinds.is_empty()
                        && !req
                            .operation_kinds
                            .contains(&operation.summary.operation_kind))
                    || operation.plan.is_some() != req.include_plan
            }) {
                return Err(ApiError::Internal(
                    "operation repository returned a record outside the requested page scope"
                        .to_string(),
                ));
            }
            let next_cursor = if page.has_more {
                let last = page.operations.last().ok_or_else(|| {
                    ApiError::Internal(
                        "operation repository returned an empty page with more results".to_string(),
                    )
                })?;
                let next_offset = offset
                    .checked_add(page.operations.len())
                    .ok_or_else(|| ApiError::bad_request("cursor is invalid or stale"))?;
                Some(encode_operation_list_cursor(
                    &self.redaction_config.index_hash_secret,
                    tenant_id,
                    &req.statuses,
                    &req.operation_kinds,
                    next_offset,
                    &last.summary.id,
                )?)
            } else {
                None
            };
            return Ok(OperationListResponse {
                operations: page.operations,
                next_cursor,
            });
        }

        let repository_operations = self
            .repository
            .list_operations(tenant_id, &req.statuses)
            .await?;
        let mut operations = match repository_operations {
            Some(operations) => operations,
            None => self.read()?.operations.values().cloned().collect(),
        };
        operations.retain(|operation| operation.tenant_id == tenant_id);
        operations.retain(|operation| {
            req.statuses.is_empty() || req.statuses.contains(&operation.status)
        });
        operations.retain(|operation| {
            req.operation_kinds.is_empty()
                || req.operation_kinds.contains(&operation.operation_kind)
        });
        operations.sort_by(|left, right| {
            right
                .created_at
                .cmp(&left.created_at)
                .then_with(|| right.id.cmp(&left.id))
        });
        if let Some(cursor) = &cursor {
            if cursor.offset > operations.len()
                || operations
                    .get(cursor.offset - 1)
                    .is_none_or(|operation| operation.id != cursor.previous_operation_id)
            {
                return Err(ApiError::bad_request("cursor is invalid or stale"));
            }
            operations.drain(..cursor.offset);
        }
        let has_more = operations.len() > req.limit;
        operations.truncate(req.limit);
        let next_cursor = if has_more {
            let last = operations.last().ok_or_else(|| {
                ApiError::Internal(
                    "operation journal returned an empty page with more results".to_string(),
                )
            })?;
            let next_offset = offset
                .checked_add(operations.len())
                .ok_or_else(|| ApiError::bad_request("cursor is invalid or stale"))?;
            Some(encode_operation_list_cursor(
                &self.redaction_config.index_hash_secret,
                tenant_id,
                &req.statuses,
                &req.operation_kinds,
                next_offset,
                &last.id,
            )?)
        } else {
            None
        };
        Ok(OperationListResponse {
            operations: operations
                .iter()
                .map(|operation| operation_list_item(operation, req.include_plan))
                .collect(),
            next_cursor,
        })
    }

    pub async fn reconcile_operations_async(
        &self,
        tenant_id: &str,
        req: ReconcileOperationsRequest,
    ) -> Result<ReconcileOperationsResponse, ApiError> {
        if req.limit == 0 || req.limit > 1_000 {
            return Err(ApiError::bad_request("limit must be between 1 and 1000"));
        }
        let mut requested_operation_ids = Vec::new();
        let mut seen_operation_ids = HashSet::new();
        for operation_id in &req.operation_ids {
            let operation_id = operation_id.trim();
            if operation_id.is_empty() {
                return Err(ApiError::bad_request(
                    "operation_ids must not contain empty values",
                ));
            }
            if seen_operation_ids.insert(operation_id.to_string()) {
                requested_operation_ids.push(operation_id.to_string());
            }
        }
        if requested_operation_ids.len() > 1_000 {
            return Err(ApiError::bad_request(
                "operation_ids must contain at most 1000 unique values",
            ));
        }

        let _mutation_guard = self.mutation_gate.lock().await;
        let mut candidates = if requested_operation_ids.is_empty() {
            match self
                .repository
                .list_oldest_reconcilable_operations(tenant_id, &req.statuses, req.limit)
                .await?
            {
                Some(candidates) => candidates,
                None => {
                    let data = self.read()?;
                    let mut candidates = data
                        .operations
                        .values()
                        .filter(|operation| operation.tenant_id == tenant_id)
                        .filter(|operation| {
                            operation.status != OperationStatus::Completed
                                || operation.indexing_state != OperationIndexingState::Completed
                        })
                        .filter(|operation| {
                            req.statuses.is_empty() || req.statuses.contains(&operation.status)
                        })
                        .collect::<Vec<_>>();
                    candidates.sort_by(|left, right| {
                        left.created_at
                            .cmp(&right.created_at)
                            .then_with(|| left.id.cmp(&right.id))
                    });
                    candidates.into_iter().take(req.limit).cloned().collect()
                }
            }
        } else {
            match self
                .repository
                .list_operations_by_ids(
                    tenant_id,
                    &requested_operation_ids,
                    &req.statuses,
                    req.limit,
                )
                .await?
            {
                Some(candidates) => candidates,
                None => {
                    let data = self.read()?;
                    requested_operation_ids
                        .iter()
                        .filter_map(|operation_id| data.operations.get(operation_id))
                        .filter(|operation| operation.tenant_id == tenant_id)
                        .filter(|operation| {
                            req.statuses.is_empty() || req.statuses.contains(&operation.status)
                        })
                        .cloned()
                        .collect()
                }
            }
        };
        if candidates.iter().any(|operation| {
            operation.tenant_id != tenant_id
                || (!requested_operation_ids.is_empty()
                    && !seen_operation_ids.contains(&operation.id))
        }) {
            return Err(ApiError::Internal(
                "operation repository returned a record outside the requested reconcile scope"
                    .to_string(),
            ));
        }
        for operation in &candidates {
            validate_operation_record(operation).map_err(|error| {
                ApiError::Internal(format!(
                    "operation repository returned an invalid record: {error}"
                ))
            })?;
        }
        candidates.retain(|operation| {
            req.statuses.is_empty() || req.statuses.contains(&operation.status)
        });
        candidates.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });
        candidates.truncate(req.limit);

        let checked = candidates.len();
        let mut reconciled = 0;
        let mut completed = 0;
        let mut failed = 0;
        let mut skipped = 0;
        let mut errors = Vec::new();
        let mut operations = Vec::new();
        for candidate in candidates {
            let already_complete = candidate.status == OperationStatus::Completed
                && candidate.indexing_state == OperationIndexingState::Completed;
            if req.dry_run || already_complete {
                skipped += 1;
                operations.push(operation_summary(&candidate));
                continue;
            }
            reconciled += 1;
            match self.reconcile_operation_record(candidate.clone()).await {
                Ok(operation) => {
                    if operation.status == OperationStatus::Completed
                        && operation.indexing_state == OperationIndexingState::Completed
                    {
                        completed += 1;
                    }
                    operations.push(operation_summary(&operation));
                }
                Err(error) => {
                    failed += 1;
                    let diagnostic = safe_cause_diagnostic(&error);
                    errors.push(OperationReconcileError {
                        operation_id: candidate.id.clone(),
                        category: diagnostic.category.to_string(),
                        fingerprint: diagnostic.fingerprint,
                    });
                    let latest = self
                        .read()?
                        .operations
                        .get(&candidate.id)
                        .cloned()
                        .unwrap_or(candidate);
                    operations.push(operation_summary(&latest));
                }
            }
        }
        Ok(ReconcileOperationsResponse {
            checked,
            reconciled,
            completed,
            failed,
            skipped,
            errors,
            operations,
        })
    }

    pub async fn hydrate_from_repository(&self, tenant_id: &str) -> Result<Value, ApiError> {
        let mut pending = initial_hydration_report(&self.redaction_config);
        pending.tenant_id = tenant_id.to_string();
        pending.started_at = now();
        pending.completed_at = None;
        {
            let mut data = self.write()?;
            data.hydration_report = Some(pending.clone());
        }

        if pending.status == HydrationStatus::NotRequired {
            return Ok(hydration_response(&pending));
        }

        let initial_user_indexes = match self.load_reconciled_user_indexes(tenant_id).await {
            Ok(indexes) => indexes,
            Err(failure) => {
                self.publish_hydration_failure(tenant_id, &failure)?;
                return Err(failure.error);
            }
        };
        let mut startup_registry = match Self::startup_registry(&initial_user_indexes) {
            Ok(registry) => registry,
            Err(failure) => {
                self.publish_hydration_failure(tenant_id, &failure)?;
                return Err(failure.error);
            }
        };

        if let Err(error) = self
            .reconcile_repository_operations_startup(tenant_id, &mut startup_registry)
            .await
        {
            let failure = HydrationFailure {
                domain: "operations",
                error,
            };
            self.publish_hydration_failure(tenant_id, &failure)?;
            return Err(failure.error);
        }

        let user_indexes = match self.load_reconciled_user_indexes(tenant_id).await {
            Ok(indexes) => indexes,
            Err(failure) => {
                self.publish_hydration_failure(tenant_id, &failure)?;
                return Err(failure.error);
            }
        };
        if let Err(failure) =
            Self::verify_refreshed_startup_registry(tenant_id, &startup_registry, &user_indexes)
        {
            self.publish_hydration_failure(tenant_id, &failure)?;
            return Err(failure.error);
        }

        let stage = match self.load_hydration_stage(tenant_id, user_indexes).await {
            Ok(stage) => stage,
            Err(failure) => {
                self.publish_hydration_failure(tenant_id, &failure)?;
                return Err(failure.error);
            }
        };
        let report = completed_hydration_report(&self.redaction_config, tenant_id, &stage);
        for (domain_name, domain) in &report.domains {
            self.metrics
                .record_hydration(domain_name, "loaded", domain.loaded);
            self.metrics
                .record_hydration(domain_name, "quarantined", domain.quarantined);
            self.metrics
                .record_hydration(domain_name, "recovered", domain.recovered);
        }
        self.publish_hydration_stage(tenant_id, stage, report.clone())?;
        Ok(hydration_response(&report))
    }

    async fn load_reconciled_user_indexes(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<UserEventIndex>, HydrationFailure> {
        let mut user_indexes = required_hydration_rows(
            "user_event_indexes",
            self.repository.list_user_event_indexes(tenant_id).await,
        )?;
        let tenant_hash = self.resolver.tenant_hash(tenant_id);
        let mut reconciliation_task_uids = Vec::new();
        for index in &mut user_indexes {
            let expected_event_index_uid = format!(
                "rag_events__t_{tenant_hash}__u_{}",
                index.owner_user_id_hash
            );
            let expected_context_index_uid = format!(
                "rag_context__t_{tenant_hash}__u_{}",
                index.owner_user_id_hash
            );
            let expected_id = user_event_index_id(&tenant_hash, &index.owner_user_id_hash);
            if index.tenant_id != tenant_id
                || index.tenant_hash != tenant_hash
                || index.id != expected_id
                || index.event_index_uid != expected_event_index_uid
                || index.personal_context_index_uid != expected_context_index_uid
            {
                return Err(HydrationFailure {
                    domain: "user_event_indexes",
                    error: ApiError::Internal(
                        "user event-index registry identity does not match its tenant scope"
                            .to_string(),
                    ),
                });
            }
            index.schema_version = EVENT_INDEX_SCHEMA_VERSION;
            index.settings_hash = EVENT_SETTINGS_HASH.to_string();
            let mut task_uids = hydration_result(
                "user_event_indexes",
                self.repository
                    .reconcile_registered_user_event_index(index)
                    .await,
            )?;
            reconciliation_task_uids.append(&mut task_uids);
        }
        if !reconciliation_task_uids.is_empty() {
            hydration_result(
                "user_event_indexes",
                self.wait_for_repository_tasks(&reconciliation_task_uids, "hydration")
                    .await,
            )?;
        }

        Ok(user_indexes)
    }

    fn startup_registry(
        user_indexes: &[UserEventIndex],
    ) -> Result<HashMap<String, UserEventIndex>, HydrationFailure> {
        let mut registry = HashMap::new();
        for index in user_indexes {
            if registry
                .insert(index.owner_user_id_hash.clone(), index.clone())
                .is_some()
            {
                return Err(HydrationFailure {
                    domain: "user_event_indexes",
                    error: ApiError::Internal(
                        "duplicate owner hash in user event-index registry".to_string(),
                    ),
                });
            }
        }
        Ok(registry)
    }

    fn same_user_index_identity(left: &UserEventIndex, right: &UserEventIndex) -> bool {
        left.id == right.id
            && left.tenant_id == right.tenant_id
            && left.tenant_hash == right.tenant_hash
            && left.owner_user_id_hash == right.owner_user_id_hash
            && left.event_index_uid == right.event_index_uid
            && left.personal_context_index_uid == right.personal_context_index_uid
            && left.schema_version == right.schema_version
            && left.settings_hash == right.settings_hash
            && left.status == right.status
    }

    fn verify_refreshed_startup_registry(
        tenant_id: &str,
        replay_registry: &HashMap<String, UserEventIndex>,
        refreshed_indexes: &[UserEventIndex],
    ) -> Result<(), HydrationFailure> {
        let refreshed = Self::startup_registry(refreshed_indexes)?;
        for (owner_hash, expected) in replay_registry {
            let Some(actual) = refreshed.get(owner_hash) else {
                return Err(HydrationFailure {
                    domain: "user_event_indexes",
                    error: ApiError::Internal(
                        "replayed user event-index registry row is not durably visible".to_string(),
                    ),
                });
            };
            if actual.tenant_id != tenant_id || !Self::same_user_index_identity(actual, expected) {
                return Err(HydrationFailure {
                    domain: "user_event_indexes",
                    error: ApiError::Internal(
                        "replayed user event-index registry row changed identity".to_string(),
                    ),
                });
            }
        }
        Ok(())
    }

    async fn load_hydration_stage(
        &self,
        tenant_id: &str,
        user_indexes: Vec<UserEventIndex>,
    ) -> Result<HydrationStage, HydrationFailure> {
        let mut stage = HydrationStage {
            // Startup reconciliation drains the bounded active set before
            // loading the remaining mandatory domains. Completed history is
            // intentionally read through by the admin journal endpoints.
            operations: Vec::new(),
            user_indexes,
            company_context: required_hydration_rows(
                "company_context_nodes",
                self.repository.list_company_context_nodes(tenant_id).await,
            )?,
            state_items: required_hydration_rows(
                "state_items",
                self.repository.list_state_items(tenant_id).await,
            )?,
            insights: required_hydration_rows(
                "insights",
                self.repository.list_insights(tenant_id).await,
            )?,
            links: required_hydration_rows("links", self.repository.list_links(tenant_id).await)?,
            sources: required_hydration_rows(
                "company_sources",
                self.repository.list_company_sources(tenant_id).await,
            )?,
            source_revisions: required_hydration_rows(
                "source_revisions",
                self.repository.list_source_revisions(tenant_id).await,
            )?,
            datasets: required_hydration_rows(
                "datasets",
                self.repository.list_datasets(tenant_id).await,
            )?,
            snapshots: required_hydration_rows(
                "structured_snapshots",
                self.repository.list_structured_snapshots(tenant_id).await,
            )?,
            structured_summaries: required_hydration_rows(
                "structured_summaries",
                self.repository.list_structured_summaries(tenant_id).await,
            )?,
            sessions: required_hydration_rows(
                "sessions",
                self.repository.list_sessions(tenant_id).await,
            )?,
            traces: required_hydration_rows(
                "traces",
                self.repository.list_traces(tenant_id).await,
            )?,
            harness_components: required_hydration_rows(
                "harness_components",
                self.repository.list_harness_components(tenant_id).await,
            )?,
            harness_revisions: required_hydration_rows(
                "harness_revisions",
                self.repository
                    .list_harness_component_revisions(tenant_id, None)
                    .await,
            )?,
            harness_changes: required_hydration_rows(
                "harness_changes",
                self.repository.list_harness_changes(tenant_id).await,
            )?,
            harness_verdicts: required_hydration_rows(
                "harness_verdicts",
                self.repository.list_harness_verdicts(tenant_id, None).await,
            )?,
            eval_cases: required_hydration_rows(
                "eval_cases",
                self.repository.list_eval_cases(tenant_id).await,
            )?,
            eval_runs: required_hydration_rows(
                "eval_runs",
                self.repository.list_eval_runs(tenant_id).await,
            )?,
            ingest_tasks: required_hydration_rows(
                "ingest_tasks",
                self.repository.list_ingest_tasks(tenant_id).await,
            )?,
            ingest_results: required_hydration_rows(
                "ingest_results",
                self.repository.list_ingest_results(tenant_id).await,
            )?,
            parse_artifacts: required_hydration_rows(
                "parse_artifacts",
                self.repository.list_tenant_parse_artifacts(tenant_id).await,
            )?,
            ..HydrationStage::default()
        };

        if stage
            .parse_artifacts
            .iter()
            .any(|artifact| artifact.tenant_id != tenant_id)
        {
            return Err(HydrationFailure {
                domain: "parse_artifacts",
                error: ApiError::Internal(
                    "parse artifact row does not match its tenant scope".to_string(),
                ),
            });
        }
        for run in &stage.eval_runs {
            let mut results = required_hydration_rows(
                "eval_case_results",
                self.repository
                    .list_eval_case_results(tenant_id, &run.id)
                    .await,
            )?;
            stage.eval_case_results.append(&mut results);
            if let Some(overview) = hydration_result(
                "eval_overviews",
                self.repository.get_eval_overview(tenant_id, &run.id).await,
            )? {
                stage.eval_overviews.push(overview);
            }
        }

        let mut recovered_ids = HashSet::new();
        for task in &mut stage.ingest_tasks {
            if is_nonterminal_ingest_state(&task.state) {
                apply_ingest_task_transition(task, "failed", Some(INGEST_ERROR_INTERRUPTED));
                recovered_ids.insert(task.task_id.clone());
            }
        }
        stage.recovered_ingest_tasks = recovered_ids.len();
        let mut corrected_result_ids = HashSet::new();
        for result in &mut stage.ingest_results {
            if let Some(task) = stage
                .ingest_tasks
                .iter()
                .find(|task| task.task_id == result.task.task_id)
            {
                if result.task != *task {
                    result.task = task.clone();
                    corrected_result_ids.insert(result.task.task_id.clone());
                }
            }
        }

        let mut artifact_by_key = stage
            .parse_artifacts
            .drain(..)
            .map(|artifact| (ParseArtifactKey::from_artifact(&artifact), artifact))
            .collect::<HashMap<_, _>>();
        let mut recovered_artifacts = Vec::new();
        for result in &stage.ingest_results {
            for artifact in &result.parse_artifacts {
                if artifact.tenant_id != tenant_id
                    || artifact.owner_user_id != result.task.owner_user_id
                {
                    return Err(HydrationFailure {
                        domain: "parse_artifacts",
                        error: ApiError::Internal(
                            "ingest-result artifact does not match its task scope".to_string(),
                        ),
                    });
                }
                let key = ParseArtifactKey::from_artifact(artifact);
                artifact_by_key.entry(key).or_insert_with(|| {
                    recovered_artifacts.push(artifact.clone());
                    artifact.clone()
                });
            }
        }
        stage.recovered_parse_artifacts = recovered_artifacts.len();
        stage.parse_artifacts = artifact_by_key.into_values().collect();
        stage.parse_artifacts.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
                .then_with(|| left.owner_user_id.cmp(&right.owner_user_id))
        });
        let recovered_tasks = stage
            .ingest_tasks
            .iter()
            .filter(|task| recovered_ids.contains(&task.task_id))
            .cloned()
            .collect::<Vec<_>>();
        let corrected_results = stage
            .ingest_results
            .iter()
            .filter(|result| corrected_result_ids.contains(&result.task.task_id))
            .cloned()
            .collect::<Vec<_>>();

        // Recovery changes are durable mutations too. Journal the immutable
        // repair intent before submitting any corrected domain row so a crash
        // during hydration leaves an observable, replayable operation rather
        // than an unknown prefix of direct writes.
        let mut recovery_resources = Vec::new();
        if !recovered_tasks.is_empty() {
            recovery_resources.push(OperationResource::IngestTasks {
                tasks: recovered_tasks,
            });
        }
        if !recovered_artifacts.is_empty() {
            recovery_resources.push(OperationResource::ParseArtifacts {
                artifacts: recovered_artifacts,
            });
        }
        recovery_resources.extend(
            corrected_results
                .into_iter()
                .map(|result| OperationResource::IngestResult { result }),
        );
        if !recovery_resources.is_empty() {
            let (primary_kind, recovery_domain) = match recovery_resources.first() {
                Some(OperationResource::IngestTasks { .. }) => {
                    (MutationPrimary::IngestTask, "ingest_tasks")
                }
                Some(OperationResource::ParseArtifacts { .. }) => {
                    (MutationPrimary::ParseArtifacts, "parse_artifacts")
                }
                Some(OperationResource::IngestResult { .. }) => {
                    (MutationPrimary::IngestResult, "ingest_results")
                }
                _ => {
                    return Err(HydrationFailure {
                        domain: "operations",
                        error: ApiError::Internal(
                            "startup ingest recovery produced an invalid primary resource"
                                .to_string(),
                        ),
                    });
                }
            };
            let plan = hydration_result(
                recovery_domain,
                self.mutation_plan(MutationPlanInput {
                    tenant_id,
                    operation_kind: "startup.ingest_recovery",
                    owner_user_id: None,
                    idempotency_key: None,
                    primary_kind,
                    resources: recovery_resources,
                    response_snapshot: Value::Null,
                    request_fingerprint: None,
                }),
            )?;
            let record = hydration_result(
                recovery_domain,
                operation_record_from_plan(plan).map_err(|error| {
                    ApiError::Internal(format!("invalid startup ingest-recovery plan: {error}"))
                }),
            )?;
            hydration_result(
                recovery_domain,
                self.persist_operation_checkpoint(&record).await,
            )?;
            let operation_id = record.id.clone();
            let record = match self.reconcile_operation_record(record).await {
                Ok(record) => record,
                Err(error) => {
                    let failure_domain = self
                        .read()
                        .ok()
                        .and_then(|data| {
                            data.operations
                                .get(&operation_id)
                                .and_then(failed_ingest_recovery_domain)
                        })
                        .unwrap_or(recovery_domain);
                    return Err(HydrationFailure {
                        domain: failure_domain,
                        error,
                    });
                }
            };
            stage.operations.push(record);
        }

        Ok(stage)
    }

    fn publish_hydration_failure(
        &self,
        tenant_id: &str,
        failure: &HydrationFailure,
    ) -> Result<(), ApiError> {
        let diagnostic = safe_cause_diagnostic(&failure.error);
        let mut report = initial_hydration_report(&self.redaction_config);
        report.tenant_id = tenant_id.to_string();
        report.status = HydrationStatus::Incomplete;
        report.ready = false;
        report.completed_at = Some(now());
        if let Some(domain) = report.domains.get_mut(failure.domain) {
            domain.status = "incomplete".to_string();
            domain.error_category = Some(diagnostic.category.to_string());
            domain.error_fingerprint = Some(diagnostic.fingerprint.clone());
        }
        tracing::error!(
            domain = failure.domain,
            cause_category = diagnostic.category,
            cause_fingerprint = %diagnostic.fingerprint,
            "mandatory repository hydration failed"
        );
        self.metrics.record_hydration(failure.domain, "failure", 1);
        self.write()?.hydration_report = Some(report);
        Ok(())
    }

    fn publish_hydration_stage(
        &self,
        tenant_id: &str,
        stage: HydrationStage,
        report: HydrationReport,
    ) -> Result<(), ApiError> {
        let HydrationStage {
            operations,
            user_indexes,
            company_context,
            state_items,
            insights,
            links,
            sources,
            source_revisions,
            datasets,
            snapshots,
            structured_summaries,
            sessions,
            traces,
            harness_components,
            harness_revisions,
            harness_changes,
            harness_verdicts,
            eval_cases,
            eval_runs,
            eval_case_results,
            eval_overviews,
            ingest_tasks,
            ingest_results,
            parse_artifacts,
            recovered_ingest_tasks: _,
            recovered_parse_artifacts: _,
        } = stage;

        let mut revisions_by_source: HashMap<String, Vec<SourceRevision>> = HashMap::new();
        for revision in source_revisions {
            revisions_by_source
                .entry(revision.source_id.clone())
                .or_default()
                .push(revision);
        }
        for revisions in revisions_by_source.values_mut() {
            revisions.sort_by_key(|revision| revision.created_at);
        }

        let mut harness_revisions_by_component: HashMap<String, Vec<HarnessComponentRevision>> =
            HashMap::new();
        for revision in harness_revisions {
            harness_revisions_by_component
                .entry(revision.component_id.clone())
                .or_default()
                .push(revision);
        }
        for revisions in harness_revisions_by_component.values_mut() {
            revisions.sort_by_key(|revision| revision.iteration);
        }

        let mut data = self.write()?;
        data.operations
            .retain(|_, operation| operation.tenant_id != tenant_id);
        data.operations.extend(
            operations
                .into_iter()
                .map(|operation| (operation.id.clone(), operation)),
        );
        let stale_event_index_uids = data
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .map(|index| index.event_index_uid.clone())
            .collect::<HashSet<_>>();
        let stale_personal_index_uids = data
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .map(|index| index.personal_context_index_uid.clone())
            .collect::<Vec<_>>();
        data.event_by_id
            .retain(|_, event| event.tenant_id != tenant_id);
        data.event_idempotency
            .retain(|(index_uid, _), _| !stale_event_index_uids.contains(index_uid));
        for index_uid in stale_event_index_uids {
            data.events_by_index.remove(&index_uid);
        }
        for index_uid in stale_personal_index_uids {
            data.personal_context.remove(&index_uid);
            data.personal_context_loaded.remove(&index_uid);
        }
        data.user_indexes
            .retain(|(tenant, _), _| tenant != tenant_id);
        for index in user_indexes {
            data.events_by_index
                .entry(index.event_index_uid.clone())
                .or_default();
            data.user_indexes.insert(
                (index.tenant_id.clone(), index.owner_user_id_hash.clone()),
                index,
            );
        }

        data.company_context
            .retain(|node| node.tenant_id != tenant_id);
        data.company_context.extend(company_context);

        data.state_items
            .retain(|(tenant, _, _), _| tenant != tenant_id);
        for item in state_items {
            data.state_items.insert(
                (
                    item.tenant_id.clone(),
                    item.owner_user_id.clone(),
                    item.natural_key.clone(),
                ),
                item,
            );
        }
        data.insights
            .retain(|_, insight| insight.tenant_id != tenant_id);
        data.insight_idempotency
            .retain(|(tenant, _, _), _| tenant != tenant_id);
        data.insights
            .extend(insights.into_iter().map(|item| (item.id.clone(), item)));
        data.links.retain(|_, link| link.tenant_id != tenant_id);
        data.link_idempotency
            .retain(|(tenant, _, _), _| tenant != tenant_id);
        data.links
            .extend(links.into_iter().map(|link| (link.id.clone(), link)));

        data.sources
            .retain(|_, source| source.tenant_id != tenant_id);
        data.source_documents
            .retain(|_, document| document.tenant_id != tenant_id);
        data.sources.extend(
            sources
                .into_iter()
                .map(|source| (source.id.clone(), source)),
        );
        for revisions in data.source_revisions.values_mut() {
            revisions.retain(|revision| revision.tenant_id != tenant_id);
        }
        data.source_revisions
            .retain(|_, revisions| !revisions.is_empty());
        for (source_id, mut revisions) in revisions_by_source {
            data.source_revisions
                .entry(source_id)
                .or_default()
                .append(&mut revisions);
        }

        data.datasets
            .retain(|_, dataset| dataset.tenant_id != tenant_id);
        data.datasets.extend(
            datasets
                .into_iter()
                .map(|dataset| (dataset.dataset_key.clone(), dataset)),
        );
        let stale_snapshot_ids = data
            .snapshots
            .values()
            .filter(|snapshot| snapshot.tenant_id == tenant_id)
            .map(|snapshot| snapshot.id.clone())
            .collect::<HashSet<_>>();
        for snapshot_id in &stale_snapshot_ids {
            data.rows_by_snapshot.remove(snapshot_id);
        }
        data.row_idempotency
            .retain(|(snapshot_id, _)| !stale_snapshot_ids.contains(snapshot_id));
        data.snapshot_idempotency
            .retain(|(tenant, _), _| tenant != tenant_id);
        data.snapshots
            .retain(|_, snapshot| snapshot.tenant_id != tenant_id);
        data.snapshots.extend(
            snapshots
                .into_iter()
                .map(|snapshot| (snapshot.id.clone(), snapshot)),
        );
        data.structured_summaries.retain(|_, summary| {
            summary.get("tenant_id").and_then(Value::as_str) != Some(tenant_id)
        });
        for summary in structured_summaries {
            if let Some(id) = summary.get("id").and_then(Value::as_str) {
                data.structured_summaries.insert(id.to_string(), summary);
            }
        }
        data.sessions
            .retain(|_, session| session.tenant_id != tenant_id);
        data.sessions.extend(
            sessions
                .into_iter()
                .map(|session| (session.id.clone(), session)),
        );
        data.traces.retain(|_, trace| trace.tenant_id != tenant_id);
        data.traces
            .extend(traces.into_iter().map(|trace| (trace.id.clone(), trace)));

        let default_component_ids = default_harness_components()
            .into_iter()
            .map(|(component_id, _, _, _)| component_id.to_string())
            .collect::<HashSet<_>>();
        let persisted_component_ids = harness_components
            .iter()
            .map(|component| component.id.clone())
            .collect::<HashSet<_>>();
        data.harness_components.retain(|component_id, component| {
            component.tenant_id != tenant_id
                || default_component_ids.contains(component_id)
                || persisted_component_ids.contains(component_id)
        });

        let persisted_revision_ids = harness_revisions_by_component
            .values()
            .flatten()
            .map(|revision| revision.id.clone())
            .collect::<HashSet<_>>();
        for (component_id, revisions) in &mut data.harness_revisions {
            let bootstrap_id = bootstrap_harness_revision_id(component_id);
            revisions.retain(|revision| {
                revision.tenant_id != tenant_id
                    || (default_component_ids.contains(component_id) && revision.id == bootstrap_id)
                    || persisted_revision_ids.contains(&revision.id)
            });
        }
        data.harness_revisions
            .retain(|_, revisions| !revisions.is_empty());
        data.seed_harness_components(tenant_id);
        data.harness_components.extend(
            harness_components
                .into_iter()
                .map(|component| (component.id.clone(), component)),
        );
        for (component_id, revisions) in harness_revisions_by_component {
            let target = data.harness_revisions.entry(component_id).or_default();
            for revision in revisions {
                if let Some(existing) = target.iter_mut().find(|existing| {
                    existing.tenant_id == revision.tenant_id && existing.id == revision.id
                }) {
                    *existing = revision;
                } else {
                    target.push(revision);
                }
            }
            target.sort_by_key(|revision| revision.iteration);
        }
        data.harness_changes
            .retain(|_, change| change.tenant_id != tenant_id);
        data.harness_changes.extend(
            harness_changes
                .into_iter()
                .map(|change| (change.id.clone(), change)),
        );
        data.harness_verdicts
            .retain(|_, verdict| verdict.tenant_id != tenant_id);
        data.harness_verdicts.extend(
            harness_verdicts
                .into_iter()
                .map(|verdict| (verdict.id.clone(), verdict)),
        );

        data.eval_cases
            .retain(|_, case| case.tenant_id != tenant_id);
        data.eval_cases
            .extend(eval_cases.into_iter().map(|case| (case.id.clone(), case)));
        data.eval_runs.retain(|_, run| run.tenant_id != tenant_id);
        data.eval_runs
            .extend(eval_runs.into_iter().map(|run| (run.id.clone(), run)));
        data.eval_case_results
            .retain(|_, result| result.tenant_id != tenant_id);
        data.eval_case_results.extend(
            eval_case_results
                .into_iter()
                .map(|result| (result.id.clone(), result)),
        );
        data.eval_overviews
            .retain(|_, overview| overview.tenant_id != tenant_id);
        data.eval_overviews.extend(
            eval_overviews
                .into_iter()
                .map(|overview| (overview.run_id.clone(), overview)),
        );

        data.ingest_tasks
            .retain(|_, task| task.tenant_id != tenant_id);
        data.ingest_results
            .retain(|_, result| result.task.tenant_id != tenant_id);
        data.parse_artifacts
            .retain(|_, artifact| artifact.tenant_id != tenant_id);
        data.parsed_blocks
            .retain(|key, _| key.tenant_id != tenant_id);
        for artifact in parse_artifacts {
            data.parse_artifacts
                .insert(ParseArtifactKey::from_artifact(&artifact), artifact);
        }
        data.ingest_tasks.extend(
            ingest_tasks
                .into_iter()
                .map(|task| (task.task_id.clone(), task)),
        );
        for result in ingest_results {
            for artifact in &result.parse_artifacts {
                data.parse_artifacts
                    .insert(ParseArtifactKey::from_artifact(artifact), artifact.clone());
            }
            let source_document_key = SourceDocumentKey::new(
                &result.task.tenant_id,
                result.task.owner_user_id.as_deref(),
                &result.source_document_uri,
            );
            data.parsed_blocks
                .insert(source_document_key, result.parsed_blocks.clone());
            data.ingest_results
                .insert(result.task.task_id.clone(), result);
        }

        data.hydration_report = Some(report);
        Ok(())
    }

    pub fn usage_snapshot(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        include_global: bool,
    ) -> Result<Value, ApiError> {
        let owner_hash = owner_user_id.map(|owner| self.resolver.user_hash(owner));
        let personal_context_index_uid = owner_user_id
            .map(|owner| {
                self.resolver
                    .resolve(tenant_id, owner, false, true)
                    .map(|routing| routing.personal_context_index_uid)
            })
            .transpose()?;
        let data = self.read()?;
        let owner_matches =
            |owner: &str| include_global || owner_user_id.is_some_and(|target| target == owner);
        let tenant_matches = |value: &str| value == tenant_id;

        let event_count = data
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .filter(|index| {
                include_global
                    || owner_hash
                        .as_deref()
                        .is_some_and(|hash| hash == index.owner_user_id_hash)
            })
            .map(|index| index.event_count_estimate)
            .sum::<usize>();
        let event_index_count = data
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .filter(|index| {
                include_global
                    || owner_hash
                        .as_deref()
                        .is_some_and(|hash| hash == index.owner_user_id_hash)
            })
            .count();
        let company_nodes = if include_global {
            data.company_context
                .iter()
                .filter(|node| tenant_matches(&node.tenant_id) && node.status == "active")
                .count()
        } else {
            0
        };
        let private_nodes = if include_global {
            data.personal_context
                .values()
                .flatten()
                .filter(|node| tenant_matches(&node.tenant_id) && node.status == "active")
                .count()
        } else {
            personal_context_index_uid
                .as_deref()
                .and_then(|uid| data.personal_context.get(uid))
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter(|node| tenant_matches(&node.tenant_id) && node.status == "active")
                        .count()
                })
                .unwrap_or(0)
        };
        let snapshot_ids = data
            .snapshots
            .values()
            .filter(|snapshot| snapshot.tenant_id == tenant_id)
            .filter(|snapshot| owner_matches(&snapshot.owner_user_id))
            .map(|snapshot| snapshot.id.clone())
            .collect::<HashSet<_>>();
        let snapshot_count = snapshot_ids.len();
        let row_count = data
            .snapshots
            .values()
            .filter(|snapshot| snapshot_ids.contains(&snapshot.id))
            .map(|snapshot| snapshot.row_count)
            .sum::<usize>();
        let summary_count = data
            .structured_summaries
            .values()
            .filter(|summary| {
                summary.get("tenant_id").and_then(Value::as_str) == Some(tenant_id)
                    && summary
                        .get("owner_user_id")
                        .and_then(Value::as_str)
                        .is_some_and(owner_matches)
            })
            .count();
        let structured_state_count = data
            .state_items
            .values()
            .filter(|item| {
                item.tenant_id == tenant_id
                    && item.state_type == "structured_summary"
                    && owner_matches(&item.owner_user_id)
            })
            .count();
        let trace_count = data
            .traces
            .values()
            .filter(|trace| trace.tenant_id == tenant_id)
            .filter(|trace| {
                include_global
                    || trace
                        .owner_user_id
                        .as_deref()
                        .is_some_and(|owner| owner_user_id == Some(owner))
            })
            .count();
        let link_count = data
            .links
            .values()
            .filter(|link| link.tenant_id == tenant_id)
            .filter(|link| {
                if include_global {
                    true
                } else {
                    link.owner_user_id
                        .as_deref()
                        .is_some_and(|owner| owner_user_id == Some(owner))
                }
            })
            .count();
        let dataset_count = if include_global {
            data.datasets
                .values()
                .filter(|dataset| dataset.tenant_id == tenant_id)
                .count()
        } else {
            0
        };
        let owner_option_matches = |owner: Option<&str>| {
            include_global || owner_user_id.is_some_and(|target| owner == Some(target))
        };
        let ingest_tasks = data
            .ingest_tasks
            .values()
            .filter(|task| task.tenant_id == tenant_id)
            .filter(|task| owner_option_matches(task.owner_user_id.as_deref()))
            .collect::<Vec<_>>();
        let parse_artifact_count = data
            .parse_artifacts
            .values()
            .filter(|artifact| artifact.tenant_id == tenant_id)
            .filter(|artifact| owner_option_matches(artifact.owner_user_id.as_deref()))
            .count();
        let parsed_block_count = data
            .parsed_blocks
            .iter()
            .filter(|(key, _)| {
                key.tenant_id == tenant_id && owner_option_matches(key.owner_user_id.as_deref())
            })
            .map(|(_, blocks)| blocks.len())
            .sum::<usize>();
        let sessions = data
            .sessions
            .values()
            .filter(|session| session.tenant_id == tenant_id)
            .filter(|session| owner_matches(&session.owner_user_id))
            .collect::<Vec<_>>();
        let message_count = sessions
            .iter()
            .map(|session| session.messages.len())
            .sum::<usize>();

        Ok(json!({
            "generated_at": now(),
            "scope": {
                "tenant_id": tenant_id,
                "owner_user_id": owner_user_id,
                "global": include_global
            },
            "providers": {
                "nowledge_api": {
                    "store_backend": self.backend_name(),
                    "run_scope": if include_global { "global" } else { "owner" }
                },
                "history_events": {
                    "event_count": event_count,
                    "user_event_index_count": event_index_count
                },
                "contextfs": {
                    "company_context_node_count": company_nodes,
                    "private_context_node_count": private_nodes,
                    "context_node_count": company_nodes + private_nodes
                },
                "rag": {
                    "trace_count": trace_count
                },
                "link_graph": {
                    "link_count": link_count
                },
                "ingest": {
                    "task_count": ingest_tasks.len(),
                    "queued": ingest_tasks.iter().filter(|task| task.state == "queued").count(),
                    "parsing": ingest_tasks.iter().filter(|task| task.state == "parsing").count(),
                    "parsed": ingest_tasks.iter().filter(|task| task.state == "parsed").count(),
                    "fragmenting": ingest_tasks.iter().filter(|task| task.state == "fragmenting").count(),
                    "indexing": ingest_tasks.iter().filter(|task| task.state == "indexing").count(),
                    "completed": ingest_tasks.iter().filter(|task| task.state == "completed").count(),
                    "failed": ingest_tasks.iter().filter(|task| task.state == "failed").count(),
                    "parse_artifact_count": parse_artifact_count,
                    "parsed_block_count": parsed_block_count
                },
                "structured_data": {
                    "dataset_count": dataset_count,
                    "snapshot_count": snapshot_count,
                    "row_count": row_count,
                    "summary_count": summary_count,
                    "structured_state_item_count": structured_state_count
                },
                "sessions": {
                    "session_count": sessions.len(),
                    "message_count": message_count
                }
            }
        }))
    }

    pub async fn debug_meili_search_async(
        &self,
        tenant_id: &str,
        index_uid: &str,
        query: &str,
    ) -> Result<Value, ApiError> {
        if let Some(raw) = self
            .repository
            .debug_search(tenant_id, index_uid, query)
            .await?
        {
            return Ok(raw);
        }
        self.debug_meili_search(tenant_id, index_uid, query)
    }

    pub async fn get_event_async(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        event_id: &str,
    ) -> Result<HistoryEvent, ApiError> {
        if let Ok(event) = self.get_event(tenant_id, owner_user_id, event_id) {
            return Ok(event);
        }
        let routing = self
            .resolver
            .resolve(tenant_id, owner_user_id, false, true)?;
        if let Some(event) = self.repository.get_event(&routing, event_id).await? {
            return Ok(event);
        }
        Err(ApiError::not_found("history event not found"))
    }

    pub async fn get_snapshot_async(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<StructuredSnapshot, ApiError> {
        if let Ok(snapshot) = self.get_snapshot(tenant_id, snapshot_id) {
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.repository.get_snapshot(tenant_id, snapshot_id).await? {
            let mut data = self.write()?;
            if let Some(current) = data.snapshots.get(snapshot_id) {
                return Ok(current.clone());
            }
            data.snapshots.insert(snapshot.id.clone(), snapshot.clone());
            return Ok(snapshot);
        }
        Err(ApiError::not_found("snapshot not found"))
    }

    pub async fn snapshot_owner_async(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<String, ApiError> {
        Ok(self
            .get_snapshot_async(tenant_id, snapshot_id)
            .await?
            .owner_user_id)
    }

    pub async fn list_rows_async(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<Value, ApiError> {
        self.ensure_snapshot_rows_loaded(tenant_id, snapshot_id)
            .await?;
        let rows = self
            .read()?
            .rows_by_snapshot
            .get(snapshot_id)
            .cloned()
            .unwrap_or_default();
        Ok(json!({ "snapshot_id": snapshot_id, "rows": rows }))
    }

    async fn ensure_snapshot_rows_loaded(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<StructuredSnapshot, ApiError> {
        let snapshot = self.get_snapshot_async(tenant_id, snapshot_id).await?;
        let already_loaded = self.read()?.rows_by_snapshot.contains_key(snapshot_id);
        if already_loaded {
            self.metrics.record_cache_access("structured_rows", "hit");
            return Ok(snapshot);
        }
        self.metrics.record_cache_access("structured_rows", "miss");
        let rows = match self.repository.list_rows(tenant_id, snapshot_id).await {
            Ok(rows) => rows.unwrap_or_default(),
            Err(error) => {
                self.metrics
                    .record_read_through("structured_rows", "failure", 1);
                return Err(error);
            }
        };
        if rows.is_empty() {
            self.metrics
                .record_read_through("structured_rows", "not_found", 1);
        } else {
            self.metrics
                .record_read_through("structured_rows", "loaded", rows.len());
        }
        let mut data = self.write()?;
        if !data.rows_by_snapshot.contains_key(snapshot_id) {
            for row_id in rows
                .iter()
                .filter_map(|row| row.get("id").and_then(Value::as_str))
            {
                data.row_idempotency
                    .insert((snapshot_id.to_string(), row_id.to_string()));
            }
            data.rows_by_snapshot.insert(snapshot_id.to_string(), rows);
        }
        Ok(snapshot)
    }

    pub async fn get_trace_async(
        &self,
        tenant_id: &str,
        trace_id: &str,
    ) -> Result<TraceRecord, ApiError> {
        if let Ok(trace) = self.get_trace(tenant_id, trace_id) {
            self.metrics.record_cache_access("trace", "hit");
            return Ok(trace);
        }
        self.metrics.record_cache_access("trace", "miss");
        let trace = match self.repository.get_trace(tenant_id, trace_id).await {
            Ok(trace) => trace,
            Err(error) => {
                self.metrics.record_read_through("trace", "failure", 1);
                return Err(error);
            }
        };
        if let Some(trace) = trace {
            self.metrics.record_read_through("trace", "loaded", 1);
            let mut data = self.write()?;
            if let Some(current) = data.traces.get(trace_id) {
                return Ok(current.clone());
            }
            data.traces.insert(trace.id.clone(), trace.clone());
            return Ok(trace);
        }
        self.metrics.record_read_through("trace", "not_found", 1);
        Err(ApiError::not_found("trace not found"))
    }

    pub async fn trace_owner_id_async(
        &self,
        tenant_id: &str,
        trace_id: &str,
    ) -> Result<Option<String>, ApiError> {
        Ok(self
            .get_trace_async(tenant_id, trace_id)
            .await?
            .owner_user_id)
    }

    async fn ensure_personal_context_loaded(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<(), ApiError> {
        let index_scopes = if let Some(owner_user_id) = owner_user_id {
            let owner_hash = self.resolver.user_hash(owner_user_id);
            if self
                .read()?
                .user_indexes
                .contains_key(&(tenant_id.to_string(), owner_hash))
            {
                vec![(
                    self.resolver
                        .resolve(tenant_id, owner_user_id, false, true)?
                        .personal_context_index_uid,
                    self.resolver.user_hash(owner_user_id),
                    Some(owner_user_id.to_string()),
                )]
            } else {
                Vec::new()
            }
        } else if include_all_private {
            self.read()?
                .user_indexes
                .values()
                .filter(|index| index.tenant_id == tenant_id)
                .map(|index| {
                    (
                        index.personal_context_index_uid.clone(),
                        index.owner_user_id_hash.clone(),
                        None,
                    )
                })
                .collect()
        } else {
            Vec::new()
        };

        for (index_uid, owner_hash, expected_owner) in index_scopes {
            if self.read()?.personal_context_loaded.contains(&index_uid) {
                self.metrics.record_cache_access("personal_context", "hit");
                continue;
            }
            self.metrics.record_cache_access("personal_context", "miss");
            let nodes = match self
                .repository
                .list_personal_context_nodes(tenant_id, &index_uid)
                .await
            {
                Ok(nodes) => nodes.unwrap_or_default(),
                Err(error) => {
                    self.metrics
                        .record_read_through("personal_context", "failure", 1);
                    return Err(error);
                }
            };
            let loaded_count = nodes.len();
            let nodes = nodes
                .into_iter()
                .filter(|node| {
                    node.tenant_id == tenant_id
                        && node.index_uid == index_uid
                        && node.index_kind == "personal"
                        && node.privacy == "private"
                        && node.owner_user_id.as_deref().is_some_and(|owner| {
                            self.resolver.user_hash(owner) == owner_hash
                                && expected_owner
                                    .as_deref()
                                    .is_none_or(|expected| owner == expected)
                        })
                })
                .collect::<Vec<_>>();
            let quarantined = loaded_count.saturating_sub(nodes.len());
            if loaded_count == 0 {
                self.metrics
                    .record_read_through("personal_context", "not_found", 1);
            } else {
                self.metrics.record_read_through(
                    "personal_context",
                    "loaded",
                    loaded_count.saturating_sub(quarantined),
                );
            }
            if quarantined > 0 {
                tracing::warn!(
                    index_uid,
                    quarantined,
                    "ignored personal-context rows outside the registry owner scope"
                );
            }
            let mut data = self.write()?;
            upsert_context_nodes(
                data.personal_context.entry(index_uid.clone()).or_default(),
                nodes,
            );
            data.personal_context_loaded.insert(index_uid);
            self.metrics
                .record_cache_access("personal_context", "stored");
        }
        Ok(())
    }

    async fn ensure_state_document_aggregate_loaded(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        state_type: &str,
        fact_key: &str,
    ) -> Result<(), ApiError> {
        self.ensure_personal_context_loaded(tenant_id, Some(owner_user_id), false)
            .await?;

        let source_id = state_document_source_id(owner_user_id, state_type, fact_key);
        let source_document_uris = {
            let data = self.read()?;
            let routing = self
                .resolver
                .resolve(tenant_id, owner_user_id, false, true)?;
            data.personal_context
                .get(&routing.personal_context_index_uid)
                .into_iter()
                .flatten()
                .filter(|node| {
                    node.tenant_id == tenant_id
                        && node.owner_user_id.as_deref() == Some(owner_user_id)
                        && node.source_id.as_deref() == Some(source_id.as_str())
                })
                .filter_map(|node| node.source_document_uri.clone())
                .collect::<HashSet<_>>()
        };

        for uri in source_document_uris {
            let key = SourceDocumentKey::new(tenant_id, Some(owner_user_id), &uri);
            if self.read()?.source_documents.contains_key(&key) {
                continue;
            }
            let Some(document) = self
                .repository
                .read_source_document(tenant_id, Some(owner_user_id), &uri)
                .await?
            else {
                continue;
            };
            if document.tenant_id != tenant_id
                || document.owner_user_id.as_deref() != Some(owner_user_id)
                || document.source_id != source_id
            {
                return Err(ApiError::Internal(
                    "repository state source document does not match its requested aggregate"
                        .to_string(),
                ));
            }
            self.cache_source_document(document)?;
        }
        Ok(())
    }

    async fn ensure_company_source_documents_loaded(
        &self,
        tenant_id: &str,
        source_id: &str,
    ) -> Result<(), ApiError> {
        let documents = self
            .repository
            .list_company_source_documents(tenant_id, source_id)
            .await?
            .unwrap_or_default();
        for document in documents {
            if document.tenant_id != tenant_id
                || document.owner_user_id.is_some()
                || document.source_id != source_id
            {
                return Err(ApiError::Internal(
                    "repository company source document does not match its requested aggregate"
                        .to_string(),
                ));
            }
            self.cache_source_document(document)?;
        }
        Ok(())
    }

    fn mutation_request_fingerprint<T: Serialize>(
        &self,
        tenant_id: &str,
        operation_kind: &str,
        request: &T,
    ) -> Result<String, ApiError> {
        let canonical =
            serde_json::to_string(&(tenant_id, operation_kind, request)).map_err(|error| {
                ApiError::Internal(format!("failed to fingerprint mutation request: {error}"))
            })?;
        Ok(hmac_hex(
            &self.redaction_config.index_hash_secret,
            "mutation-request-v1",
            &canonical,
            32,
        ))
    }

    fn ensure_state_aggregate_identity_available(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        state_type: &str,
        fact_key: &str,
    ) -> Result<(), ApiError> {
        let requested_state_type = sanitize_slug(state_type);
        let requested_fact_key = sanitize_slug(fact_key);
        let data = self.read()?;
        for item in data
            .state_items
            .values()
            .filter(|item| item.tenant_id == tenant_id && item.owner_user_id == owner_user_id)
        {
            if item.natural_key == fact_key && item.state_type != state_type {
                return Err(ApiError::conflict(
                    "state_type is immutable for an existing state fact",
                ));
            }
            if sanitize_slug(&item.state_type) == requested_state_type
                && sanitize_slug(&item.natural_key) == requested_fact_key
                && (item.state_type != state_type || item.natural_key != fact_key)
            {
                return Err(ApiError::conflict(
                    "state fact identity collides with an existing canonical state path",
                ));
            }
        }
        Ok(())
    }

    async fn ensure_state_operation_generation_ready(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        state_type: &str,
        fact_key: &str,
        current_operation_id: Option<&str>,
    ) -> Result<(), ApiError> {
        let state_type_slug = sanitize_slug(state_type);
        let fact_key_slug = sanitize_slug(fact_key);
        let (current_version, mut prior_operations) = {
            let data = self.read()?;
            let current_version = data
                .state_items
                .values()
                .filter(|item| {
                    item.tenant_id == tenant_id
                        && item.owner_user_id == owner_user_id
                        && sanitize_slug(&item.state_type) == state_type_slug
                        && sanitize_slug(&item.natural_key) == fact_key_slug
                })
                .map(|item| item.current_version)
                .max();
            let prior_operations = data
                .operations
                .values()
                .filter(|operation| operation.tenant_id == tenant_id)
                .filter(|operation| Some(operation.id.as_str()) != current_operation_id)
                .filter(|operation| {
                    matches!(
                        &operation.plan.primary.resource,
                        OperationResource::StateItem { item }
                            if item.owner_user_id == owner_user_id
                                && (item.natural_key == fact_key
                                    || (sanitize_slug(&item.state_type) == state_type_slug
                                        && sanitize_slug(&item.natural_key) == fact_key_slug))
                    )
                })
                .filter(|operation| {
                    operation.status != OperationStatus::Completed
                        || operation.indexing_state != OperationIndexingState::Completed
                })
                .cloned()
                .collect::<Vec<_>>();
            (current_version, prior_operations)
        };
        prior_operations.sort_by(|left, right| {
            left.created_at
                .cmp(&right.created_at)
                .then_with(|| left.id.cmp(&right.id))
        });

        for operation in prior_operations {
            let operation_item = match &operation.plan.primary.resource {
                OperationResource::StateItem { item } => item,
                _ => unreachable!("state generation candidates have a state-item primary"),
            };
            if operation_item.natural_key == fact_key && operation_item.state_type != state_type {
                return Err(ApiError::conflict(
                    "state_type is immutable for an existing state fact",
                ));
            }
            if sanitize_slug(&operation_item.state_type) == state_type_slug
                && sanitize_slug(&operation_item.natural_key) == fact_key_slug
                && (operation_item.state_type != state_type
                    || operation_item.natural_key != fact_key)
            {
                return Err(ApiError::conflict(
                    "state fact identity collides with an existing canonical state path",
                ));
            }
            let operation_version = operation_item.current_version;
            if current_version.is_some_and(|version| version > operation_version) {
                return Err(ApiError::conflict(
                    "a previous update for this state fact must be reconciled before a newer update",
                ));
            }
            if operation.status == OperationStatus::Completed
                && operation.indexing_state == OperationIndexingState::Pending
            {
                let reconciled = self.reconcile_operation_record(operation).await;
                if reconciled.as_ref().is_ok_and(|record| {
                    record.status == OperationStatus::Completed
                        && record.indexing_state == OperationIndexingState::Completed
                }) {
                    continue;
                }
            }
            return Err(ApiError::conflict(
                "a previous update for this state fact must be reconciled before a newer update",
            ));
        }
        Ok(())
    }

    pub async fn fs_ls_async(
        &self,
        tenant_id: &str,
        uri: Option<&str>,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<Value, ApiError> {
        self.ensure_personal_context_loaded(tenant_id, owner_user_id, include_all_private)
            .await?;
        self.fs_ls(tenant_id, uri, owner_user_id, include_all_private)
    }

    pub async fn fs_tree_async(
        &self,
        tenant_id: &str,
        uri: Option<&str>,
        depth: Option<usize>,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<Value, ApiError> {
        self.ensure_personal_context_loaded(tenant_id, owner_user_id, include_all_private)
            .await?;
        self.fs_tree(tenant_id, uri, depth, owner_user_id, include_all_private)
    }

    pub async fn fs_read_async(
        &self,
        tenant_id: &str,
        uri: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextNode, ApiError> {
        self.ensure_personal_context_loaded(tenant_id, owner_user_id, include_all_private)
            .await?;
        match self.fs_read(tenant_id, uri, owner_user_id, include_all_private) {
            Ok(node) => {
                self.metrics.record_cache_access("context_node", "hit");
                return Ok(self.sanitize_context_node_for_egress(node));
            }
            Err(ApiError::NotFound(_)) => {
                self.metrics.record_cache_access("context_node", "miss");
            }
            Err(error) => return Err(error),
        }
        let repository_node = match self
            .repository
            .read_context_node(tenant_id, owner_user_id, uri, None, &self.resolver)
            .await
        {
            Ok(node) => node,
            Err(error) => {
                self.metrics
                    .record_read_through("context_node", "failure", 1);
                return Err(error);
            }
        };
        if let Some(node) = repository_node {
            self.metrics
                .record_read_through("context_node", "loaded", 1);
            self.validate_repository_context_node(&node, tenant_id, owner_user_id)?;
            let node = self.cache_context_node(node)?;
            return Ok(self.sanitize_context_node_for_egress(node));
        }
        let source_document = if owner_user_id.is_none() && include_all_private {
            let documents = self
                .repository
                .list_source_documents_by_uri(tenant_id, uri)
                .await?
                .unwrap_or_default();
            select_admin_source_document(documents)?
        } else {
            self.repository
                .read_source_document(tenant_id, owner_user_id, uri)
                .await?
        };
        if let Some(source_document) = source_document {
            self.metrics
                .record_read_through("source_document", "loaded", 1);
            let source_document = self.cache_source_document(source_document)?;
            return Ok(self
                .sanitize_context_node_for_egress(source_document_context_node(source_document)));
        }
        self.metrics
            .record_read_through("context_node", "not_found", 1);
        self.metrics
            .record_read_through("source_document", "not_found", 1);
        Err(ApiError::not_found("context uri not found"))
    }

    pub async fn fs_layer_async(
        &self,
        tenant_id: &str,
        uri: &str,
        layer: u8,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextNode, ApiError> {
        self.ensure_personal_context_loaded(tenant_id, owner_user_id, include_all_private)
            .await?;
        match self.fs_layer(tenant_id, uri, layer, owner_user_id, include_all_private) {
            Ok(node) => {
                self.metrics.record_cache_access("context_node", "hit");
                return Ok(self.sanitize_context_node_for_egress(node));
            }
            Err(ApiError::NotFound(_)) => {
                self.metrics.record_cache_access("context_node", "miss");
            }
            Err(error) => return Err(error),
        }
        if let Some(node) = self
            .repository
            .read_context_node(tenant_id, owner_user_id, uri, Some(layer), &self.resolver)
            .await?
        {
            self.metrics
                .record_read_through("context_node", "loaded", 1);
            self.validate_repository_context_node(&node, tenant_id, owner_user_id)?;
            let node = self.cache_context_node(node)?;
            return Ok(self.sanitize_context_node_for_egress(node));
        }
        self.metrics
            .record_read_through("context_node", "not_found", 1);
        Err(ApiError::not_found("context layer not found"))
    }

    pub async fn traceback_async(
        &self,
        tenant_id: &str,
        req: ContextTracebackRequest,
        include_all_private: bool,
    ) -> Result<ContextTracebackResponse, ApiError> {
        let uri = req
            .uri
            .as_deref()
            .ok_or_else(|| ApiError::bad_request("uri is required"))?;
        let owner_user_id = req.owner_user_id.as_deref();
        let fragment = self
            .fs_read_async(tenant_id, uri, owner_user_id, include_all_private)
            .await?;
        let source_owner_user_id = fragment.owner_user_id.clone();
        let source_document_uri = fragment.source_document_uri.clone().or_else(|| {
            self.read().ok().and_then(|data| {
                data.links
                    .values()
                    .find(|link| {
                        link.tenant_id == tenant_id
                            && link.status == "active"
                            && link.relation == "part_of"
                            && link.source_uri == fragment.uri
                    })
                    .map(|link| link.target_uri.clone())
            })
        });
        if let Some(source_document_uri) = source_document_uri {
            let source_document_key = SourceDocumentKey::new(
                tenant_id,
                source_owner_user_id.as_deref(),
                &source_document_uri,
            );
            let cached = self
                .read()?
                .source_documents
                .get(&source_document_key)
                .is_some_and(|document| document.status == "active");
            if !cached {
                if let Some(document) = self
                    .repository
                    .read_source_document(
                        tenant_id,
                        source_owner_user_id.as_deref(),
                        &source_document_uri,
                    )
                    .await?
                {
                    self.cache_source_document(document)?;
                }
            }
        }
        self.traceback(tenant_id, req, include_all_private)
    }

    pub async fn reveal_context_async(
        &self,
        tenant_id: &str,
        req: ContextRevealRequest,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextRevealResponse, ApiError> {
        let layer = req.next_layer.unwrap_or(1);
        let uri = if let Some(uri) = req.uri {
            uri
        } else if let Some(trace_id) = req.trace_id {
            self.get_trace_async(tenant_id, &trace_id)
                .await?
                .context_uris
                .into_iter()
                .next()
                .ok_or_else(|| ApiError::not_found("trace has no context to reveal"))?
        } else {
            return Err(ApiError::bad_request("uri or trace_id is required"));
        };
        let node = self
            .fs_layer_async(tenant_id, &uri, layer, owner_user_id, include_all_private)
            .await?;
        Ok(ContextRevealResponse {
            uri: node.uri,
            layer: node.layer,
            content: node.body,
            source_ref: SourceRef {
                kind: node.index_kind,
                id: node.source_id.unwrap_or_default(),
                uri: Some(uri),
                meta: None,
            },
        })
    }

    fn cache_context_node(&self, node: ContextNode) -> Result<ContextNode, ApiError> {
        self.metrics.record_cache_access("context_node", "stored");
        let mut data = self.write()?;
        let nodes = if node.index_kind == "company" || node.index_uid == "rag_company_context" {
            &mut data.company_context
        } else {
            data.personal_context
                .entry(node.index_uid.clone())
                .or_default()
        };
        if let Some(existing) = nodes.iter_mut().find(|existing| {
            existing.tenant_id == node.tenant_id
                && existing.uri == node.uri
                && existing.layer == node.layer
        }) {
            if existing.updated_at <= node.updated_at {
                *existing = node;
            }
            Ok(existing.clone())
        } else {
            nodes.push(node.clone());
            Ok(node)
        }
    }

    fn validate_repository_context_node(
        &self,
        node: &ContextNode,
        tenant_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<(), ApiError> {
        let company_scope = node.tenant_id == tenant_id
            && node.owner_user_id.is_none()
            && node.privacy == "company"
            && node.index_kind == "company"
            && node.index_uid == "rag_company_context"
            && node.status == "active";
        let personal_scope = if let Some(owner_user_id) = owner_user_id {
            let routing = self
                .resolver
                .resolve(tenant_id, owner_user_id, false, true)?;
            node.tenant_id == tenant_id
                && node.owner_user_id.as_deref() == Some(owner_user_id)
                && node.privacy == "private"
                && node.index_kind == "personal"
                && node.index_uid == routing.personal_context_index_uid
                && node.status == "active"
        } else {
            false
        };

        if company_scope || personal_scope {
            return Ok(());
        }

        tracing::warn!(
            tenant_id,
            uri = %node.uri,
            index_uid = %node.index_uid,
            "repository context row failed scope validation"
        );
        Err(ApiError::Internal(
            "repository context row does not match its requested scope".to_string(),
        ))
    }

    fn cache_source_document(&self, document: SourceDocument) -> Result<SourceDocument, ApiError> {
        self.metrics
            .record_cache_access("source_document", "stored");
        let mut data = self.write()?;
        let key = SourceDocumentKey::from_document(&document);
        if let Some(existing) = data.source_documents.get(&key) {
            if existing.updated_at > document.updated_at {
                return Ok(existing.clone());
            }
        }
        data.source_documents.insert(key, document.clone());
        Ok(document)
    }

    fn sanitize_context_node_for_egress(&self, mut node: ContextNode) -> ContextNode {
        let secrets = self.redaction_config.configured_secret_values();
        node.title = mask_secret_egress_projection_preserving_chars(&node.title, &secrets);
        node.body = if matches!(node.node_kind.as_str(), "fragment" | "abstract") {
            mask_secret_fragment_projection_preserving_chars(&node.body, &secrets)
        } else {
            mask_secret_egress_projection_preserving_chars(&node.body, &secrets)
        };
        node.section_path = node
            .section_path
            .into_iter()
            .map(|part| mask_secret_egress_projection_preserving_chars(&part, &secrets))
            .collect();
        node
    }

    fn answer_from_context(&self, outcome: ContextSearchOutcome) -> RagAnswerResponse {
        let citations: Vec<_> = outcome
            .response
            .hits
            .iter()
            .take(5)
            .map(citation_from_hit)
            .collect();
        let answer = if citations.is_empty() {
            "I do not have enough indexed context to answer that yet.".to_string()
        } else {
            format!(
                "Based on staged ContextFS retrieval, the strongest matching context is: {}",
                citations
                    .iter()
                    .map(|c| c.quote.as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        };

        RagAnswerResponse {
            answer_id: new_id("answer"),
            trace_id: outcome.response.trace_id,
            answer,
            citations,
            usage: json!({
                "provider": "none",
                "backend": self.backend_name(),
                "stages": ["fragments"]
            }),
        }
    }

    fn evaluate_case_result(
        &self,
        tenant_id: &str,
        run_id: &str,
        case: &RagEvalCase,
        outcome: &ContextSearchOutcome,
        answer: RagAnswerResponse,
        latency_ms: u64,
    ) -> Result<RagEvalCaseResult, ApiError> {
        let retrieved_uris = outcome
            .response
            .hits
            .iter()
            .take(5)
            .map(|hit| hit.uri.clone())
            .collect::<Vec<_>>();
        let source_doc_leaks = outcome
            .nodes
            .iter()
            .filter(|node| {
                node.node_kind != "fragment"
                    || node.retrieval_role != "fragment"
                    || node.source_document_uri.as_deref() == Some(node.uri.as_str())
            })
            .count();
        let acl_violations = outcome
            .nodes
            .iter()
            .filter(|node| {
                node.owner_user_id
                    .as_deref()
                    .is_some_and(|owner| case.owner_user_id.as_deref() != Some(owner))
            })
            .count();
        let stale_fragments = outcome
            .nodes
            .iter()
            .filter(|node| !retrieval_candidate(node))
            .count();

        let mut citation_source_document_uris = Vec::new();
        let mut traceback_failures = 0usize;
        for hit in &outcome.response.hits {
            match self.traceback(
                tenant_id,
                ContextTracebackRequest {
                    uri: Some(hit.uri.clone()),
                    owner_user_id: case.owner_user_id.clone(),
                },
                false,
            ) {
                Ok(traceback) => citation_source_document_uris.push(traceback.source_document_uri),
                Err(_) => traceback_failures += 1,
            }
        }
        let mut source_document_uris = citation_source_document_uris.clone();
        source_document_uris.sort();
        source_document_uris.dedup();

        let expected_context_matches = case
            .expected_context_uris
            .iter()
            .filter(|uri| retrieved_uris.contains(uri))
            .count();
        let expected_source_matches = case
            .expected_source_document_uris
            .iter()
            .filter(|uri| source_document_uris.contains(uri))
            .count();
        let expected_total =
            case.expected_context_uris.len() + case.expected_source_document_uris.len();
        let retrieval_recall_at_5 = if expected_total == 0 {
            1.0
        } else {
            (expected_context_matches + expected_source_matches) as f64 / expected_total as f64
        };
        let citation_precision = if answer.citations.is_empty() {
            if case.expected_source_document_uris.is_empty() {
                1.0
            } else {
                0.0
            }
        } else if case.expected_source_document_uris.is_empty() {
            if source_doc_leaks == 0 && traceback_failures == 0 {
                1.0
            } else {
                0.0
            }
        } else {
            let correct = citation_source_document_uris
                .iter()
                .filter(|uri| case.expected_source_document_uris.contains(uri))
                .count();
            correct as f64 / answer.citations.len().max(1) as f64
        };
        let traceback_success_rate = if outcome.response.hits.is_empty() {
            1.0
        } else {
            (outcome
                .response
                .hits
                .len()
                .saturating_sub(traceback_failures)) as f64
                / outcome.response.hits.len() as f64
        };

        let mut failures = Vec::new();
        if retrieval_recall_at_5 < 1.0 {
            failures.push("retrieval_recall".to_string());
        }
        if citation_precision < 1.0 {
            failures.push("citation_precision".to_string());
        }
        if traceback_failures > 0 {
            failures.push("traceback_missing".to_string());
        }
        if source_doc_leaks > 0 {
            failures.push("source_doc_leak".to_string());
        }
        if acl_violations > 0 {
            failures.push("acl_violation".to_string());
        }
        if stale_fragments > 0 {
            failures.push("stale_fragment".to_string());
        }
        for expected in &case.expected_answer_contains {
            if !answer.answer.contains(expected) {
                failures.push("answer_expectation".to_string());
                break;
            }
        }
        failures.sort();
        failures.dedup();

        let guard_failures = failures
            .iter()
            .filter_map(|failure| guard_name_for_failure(failure).map(ToString::to_string))
            .collect::<Vec<_>>();
        let answer_text = answer.answer;
        let citations = answer.citations;
        let tokens_per_answer = answer_text.split_whitespace().count() as f64;
        Ok(RagEvalCaseResult {
            id: new_id("evalresult"),
            tenant_id: tenant_id.to_string(),
            run_id: run_id.to_string(),
            case_id: case.id.clone(),
            owner_user_id: case.owner_user_id.clone(),
            status: if failures.is_empty() {
                "passed".to_string()
            } else {
                "failed".to_string()
            },
            question: case.question.clone(),
            trace_id: answer.trace_id.clone(),
            answer: answer_text,
            citations,
            retrieved_uris,
            source_document_uris,
            failures: failures.clone(),
            guard_failures,
            metrics: json!({
                "retrieval_recall_at_5": retrieval_recall_at_5,
                "citation_precision": citation_precision,
                "traceback_success_rate": traceback_success_rate,
                "source_doc_leak_rate": if source_doc_leaks > 0 { 1.0 } else { 0.0 },
                "acl_violation_rate": if acl_violations > 0 { 1.0 } else { 0.0 },
                "stale_fragment_rate": if stale_fragments > 0 { 1.0 } else { 0.0 },
                "tokens_per_answer": tokens_per_answer
            }),
            latency_ms,
            created_at: now(),
        })
    }

    fn regression_guard_results(
        &self,
        tenant_id: &str,
        results: &[RagEvalCaseResult],
        llm_health_false_ready: bool,
    ) -> Result<Vec<RegressionGuardResult>, ApiError> {
        let has_failure = |name: &str| {
            results
                .iter()
                .any(|result| result.guard_failures.iter().any(|failure| failure == name))
        };
        let data = self.read()?;
        let (part_of_ok, part_of_evidence) = part_of_links_guard_locked(&data, tenant_id);
        let (superseded_ok, superseded_evidence) =
            superseded_fragments_guard_locked(&data, tenant_id);
        let (state_history_ok, state_history_evidence) =
            state_history_guard_locked(&data, tenant_id);
        Ok(vec![
            RegressionGuardResult {
                name: "source_doc_not_default_retrieved".to_string(),
                passed: !has_failure("source_doc_not_default_retrieved"),
                evidence: json!({ "failing_cases": guard_case_ids(results, "source_doc_not_default_retrieved") }),
            },
            RegressionGuardResult {
                name: "fragment_traceback_required".to_string(),
                passed: !has_failure("fragment_traceback_required"),
                evidence: json!({ "failing_cases": guard_case_ids(results, "fragment_traceback_required") }),
            },
            RegressionGuardResult {
                name: "owner_acl_never_leaks".to_string(),
                passed: !has_failure("owner_acl_never_leaks"),
                evidence: json!({ "failing_cases": guard_case_ids(results, "owner_acl_never_leaks") }),
            },
            RegressionGuardResult {
                name: "superseded_fragments_not_active".to_string(),
                passed: !has_failure("superseded_fragments_not_active") && superseded_ok,
                evidence: superseded_evidence,
            },
            RegressionGuardResult {
                name: "part_of_links_superseded_on_revision_update".to_string(),
                passed: part_of_ok,
                evidence: part_of_evidence,
            },
            RegressionGuardResult {
                name: "llm_health_controls_ready".to_string(),
                passed: !llm_health_false_ready,
                evidence: json!({ "llm_health_false_ready": llm_health_false_ready }),
            },
            RegressionGuardResult {
                name: "state_change_writes_history_event".to_string(),
                passed: state_history_ok,
                evidence: state_history_evidence.clone(),
            },
            RegressionGuardResult {
                name: "current_state_has_history_evidence".to_string(),
                passed: state_history_ok,
                evidence: state_history_evidence,
            },
        ])
    }

    fn write_eval_reports_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        run: &mut RagEvalRun,
        overview: &mut RagEvalOverview,
        results: &[RagEvalCaseResult],
    ) {
        for result in results {
            let uri = format!(
                "ctx://harness/eval/{}/cases/{}/report",
                sanitize_slug(&run.id),
                sanitize_slug(&result.case_id)
            );
            let content = case_result_markdown(result);
            let checksum = hmac_hex(
                tenant_id.as_bytes(),
                "eval-case-report",
                &format!("{}:{content}", result.id),
                32,
            );
            let now = now();
            let document = SourceDocument {
                id: source_document_id(
                    tenant_id,
                    result.owner_user_id.as_deref(),
                    &format!("eval-case:{}", result.id),
                    &run.id,
                ),
                tenant_id: tenant_id.to_string(),
                owner_user_id: result.owner_user_id.clone(),
                source_kind: "eval_case_report".to_string(),
                source_id: format!("eval-case:{}", result.id),
                revision_id: run.id.clone(),
                uri: uri.clone(),
                title: format!("Eval case {} report", result.case_id),
                content,
                checksum,
                status: "active".to_string(),
                retrieval_enabled: false,
                created_at: now,
                updated_at: now,
            };
            data.source_documents
                .insert(SourceDocumentKey::from_document(&document), document);
            run.report_source_document_uris.push(uri.clone());
            overview.case_report_uris.push(uri);
        }

        let overview_uri = format!("ctx://harness/eval/{}/overview", sanitize_slug(&run.id));
        let checksum = hmac_hex(
            tenant_id.as_bytes(),
            "eval-overview-report",
            &format!("{}:{}", run.id, overview.overview_markdown),
            32,
        );
        let ingest = self.write_source_document_fragments_locked(
            data,
            tenant_id,
            None,
            "eval_overview_report",
            &format!("eval-overview:{}", run.id),
            &run.id,
            &overview_uri,
            &format!("Eval overview {}", run.id),
            &overview.overview_markdown,
            &checksum,
            "company",
            "rag_company_context",
            None,
            &[],
            &[],
        );
        overview.overview_source_document_uri = Some(ingest.source_document_uri.clone());
        run.overview_source_document_uri = Some(ingest.source_document_uri);
    }

    fn transition_ingest_task(
        &self,
        task_id: &str,
        state: &str,
        error: Option<String>,
    ) -> Result<IngestTask, ApiError> {
        let mut data = self.write()?;
        let task = data
            .ingest_tasks
            .get_mut(task_id)
            .ok_or_else(|| ApiError::not_found("ingest task not found"))?;
        apply_ingest_task_transition(task, state, error.as_deref());
        Ok(task.clone())
    }

    fn fail_nonterminal_ingest_task(
        &self,
        task_id: &str,
        error: &'static str,
    ) -> Result<Option<IngestTask>, ApiError> {
        let mut data = self.write()?;
        let task = data
            .ingest_tasks
            .get_mut(task_id)
            .ok_or_else(|| ApiError::not_found("ingest task not found"))?;
        if !is_nonterminal_ingest_state(&task.state) {
            return Ok(None);
        }
        task.state = "failed".to_string();
        task.error = Some(error.to_string());
        task.updated_at = now();
        task.completed_at = Some(task.updated_at);
        Ok(Some(task.clone()))
    }

    async fn fail_nonterminal_ingest_task_async(
        &self,
        task_id: &str,
        error: &'static str,
    ) -> Result<bool, ApiError> {
        let current = self.ingest_task_for_run(task_id)?;
        let owner = current.owner_user_id.clone();
        let tenant_id = current.tenant_id.clone();
        let (task, _) = self
            .execute_staged_mutation(
                &tenant_id,
                "ingest_task.fail",
                owner.as_deref(),
                None,
                MutationPrimary::IngestTask,
                |staged| staged.fail_nonterminal_ingest_task(task_id, error),
            )
            .await?;
        Ok(task.is_some())
    }

    pub async fn mark_ingest_task_interrupted_async(
        &self,
        task_id: &str,
    ) -> Result<bool, ApiError> {
        self.fail_nonterminal_ingest_task_async(task_id, INGEST_ERROR_INTERRUPTED)
            .await
    }

    pub async fn mark_ingest_task_failed_async(&self, task_id: &str) -> Result<bool, ApiError> {
        self.fail_nonterminal_ingest_task_async(task_id, INGEST_ERROR_FAILED)
            .await
    }

    pub async fn interrupt_nonterminal_ingest_tasks_async(
        &self,
        tenant_id: &str,
    ) -> Result<usize, ApiError> {
        let (tasks, _) = self
            .execute_staged_mutation(
                tenant_id,
                "ingest_tasks.interrupt",
                None,
                None,
                MutationPrimary::IngestTask,
                |staged| staged.interrupt_nonterminal_ingest_tasks_local(tenant_id),
            )
            .await?;
        Ok(tasks.len())
    }

    pub(crate) fn interrupt_nonterminal_ingest_tasks_local(
        &self,
        tenant_id: &str,
    ) -> Result<Vec<IngestTask>, ApiError> {
        let mut data = self.write()?;
        let mut interrupted = Vec::new();
        for task in data.ingest_tasks.values_mut() {
            if task.tenant_id == tenant_id && is_nonterminal_ingest_state(&task.state) {
                apply_ingest_task_transition(task, "failed", Some(INGEST_ERROR_INTERRUPTED));
                interrupted.push(task.clone());
            }
        }
        Ok(interrupted)
    }

    async fn transition_ingest_task_async(
        &self,
        task_id: &str,
        state: &str,
        error: Option<String>,
    ) -> Result<IngestTask, ApiError> {
        let current = self.ingest_task_for_run(task_id)?;
        let tenant_id = current.tenant_id.clone();
        let owner = current.owner_user_id.clone();
        let (task, _) = self
            .execute_staged_mutation(
                &tenant_id,
                "ingest_task.transition",
                owner.as_deref(),
                None,
                MutationPrimary::IngestTask,
                |staged| staged.transition_ingest_task(task_id, state, error),
            )
            .await?;
        Ok(task)
    }

    fn ingest_task_for_run(&self, task_id: &str) -> Result<IngestTask, ApiError> {
        let data = self.read()?;
        data.ingest_tasks
            .get(task_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("ingest task not found"))
    }

    fn session_owner(&self, session_id: &str) -> Result<Option<String>, ApiError> {
        let data = self.read()?;
        Ok(data
            .sessions
            .get(session_id)
            .map(|session| session.owner_user_id.clone()))
    }

    fn insert_trace(&self, trace: TraceRecord) -> Result<(), ApiError> {
        let mut data = self.write()?;
        data.traces.insert(trace.id.clone(), trace);
        Ok(())
    }

    fn owner_from_path_or_body(
        &self,
        path_owner_user_id: Option<&str>,
        body_owner_user_id: Option<&str>,
    ) -> Result<String, ApiError> {
        match (path_owner_user_id, body_owner_user_id) {
            (Some(path), Some(body)) if path != body => Err(ApiError::bad_request(
                "owner_user_id in path and body must match",
            )),
            (Some(path), _) => Ok(path.to_string()),
            (_, Some(body)) => Ok(body.to_string()),
            _ => Err(ApiError::bad_request("owner_user_id is required")),
        }
    }

    fn validate_append_event_request(
        &self,
        path_owner_user_id: Option<&str>,
        req: &AppendHistoryEventRequest,
    ) -> Result<String, ApiError> {
        let owner =
            self.owner_from_path_or_body(path_owner_user_id, req.owner_user_id.as_deref())?;
        if req.event_index_hint.is_some() {
            return Err(ApiError::bad_request(
                "event_index_hint is not accepted; event index routing is server-side",
            ));
        }
        for (value, field) in [
            (req.event_type.as_deref(), "event_type"),
            (req.entity_type.as_deref(), "entity_type"),
            (req.entity_id.as_deref(), "entity_id"),
            (req.source_kind.as_deref(), "source_kind"),
        ] {
            if value.is_none_or(|value| value.trim().is_empty()) {
                return Err(ApiError::bad_request(format!("{field} is required")));
            }
        }
        if req.occurred_at.is_none() {
            return Err(ApiError::bad_request("occurred_at is required"));
        }
        if req.observed_at.is_none() {
            return Err(ApiError::bad_request("observed_at is required"));
        }
        if req.source_ref.is_none() {
            return Err(ApiError::bad_request("source_ref is required"));
        }
        Ok(owner)
    }

    fn ensure_user_index_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        owner_user_id: &str,
        schema_version: u32,
    ) -> Result<(UserEventIndex, EventIndexRouting), ApiError> {
        let key = (
            tenant_id.to_string(),
            self.resolver.user_hash(owner_user_id),
        );
        let existed = data.user_indexes.contains_key(&key);
        let routing = self
            .resolver
            .resolve(tenant_id, owner_user_id, !existed, existed)?;

        if !existed {
            let tenant_hash = self.resolver.tenant_hash(tenant_id);
            let index = UserEventIndex {
                id: user_event_index_id(&tenant_hash, &routing.owner_user_id_hash),
                tenant_id: tenant_id.to_string(),
                tenant_hash,
                owner_user_id_hash: routing.owner_user_id_hash.clone(),
                event_index_uid: routing.event_index_uid.clone(),
                personal_context_index_uid: routing.personal_context_index_uid.clone(),
                schema_version,
                settings_hash: EVENT_SETTINGS_HASH.to_string(),
                status: "active".to_string(),
                created_at: now(),
                last_event_at: None,
                event_count_estimate: 0,
            };
            data.user_indexes.insert(key.clone(), index);
            data.events_by_index
                .entry(routing.event_index_uid.clone())
                .or_default();
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default();
        } else if let Some(index) = data.user_indexes.get_mut(&key) {
            index.schema_version = schema_version;
            index.settings_hash = EVENT_SETTINGS_HASH.to_string();
            index.status = "active".to_string();
        }

        let index = data
            .user_indexes
            .get(&key)
            .cloned()
            .expect("user index exists");
        Ok((index, routing))
    }

    fn insert_event_locked(
        &self,
        data: &mut StoreData,
        routing: &EventIndexRouting,
        event: HistoryEvent,
        idempotency_key_hash: Option<String>,
    ) {
        if let Some(hash) = idempotency_key_hash {
            data.event_idempotency
                .insert((routing.event_index_uid.clone(), hash), event.id.clone());
        }
        data.event_by_id.insert(event.id.clone(), event.clone());
        data.events_by_index
            .entry(routing.event_index_uid.clone())
            .or_default()
            .push(event.clone());
        if let Some(index) = data
            .user_indexes
            .values_mut()
            .find(|index| index.event_index_uid == routing.event_index_uid)
        {
            index.last_event_at = Some(event.observed_at);
            index.event_count_estimate += 1;
        }
    }

    fn write_event_context_locked(
        &self,
        data: &mut StoreData,
        routing: &EventIndexRouting,
        event: &HistoryEvent,
    ) {
        let base = format!(
            "ctx://user/history/{}/{}",
            sanitize_slug(&event.event_type),
            sanitize_slug(&event.id)
        );
        let title = format!("{} {}", event.event_type, event.entity_id);
        let secrets = self.redaction_config.configured_secret_values();
        let abstract_body = truncate_chars(
            &mask_secret_fragment_projection_preserving_chars(&event.text, &secrets),
            500,
        );
        let overview_body = json!({
            "event_type": event.event_type,
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
            "occurred_at": event.occurred_at,
            "text": event.text,
            "payload": event.payload
        })
        .to_string();
        let nodes = vec![
            self.context_node(
                &format!("{base}/.abstract"),
                &title,
                0,
                &abstract_body,
                "personal",
                &routing.personal_context_index_uid,
                &event.tenant_id,
                Some(event.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/.overview"),
                &title,
                1,
                &overview_body,
                "personal",
                &routing.personal_context_index_uid,
                &event.tenant_id,
                Some(event.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/detail"),
                &title,
                2,
                &event.text,
                "personal",
                &routing.personal_context_index_uid,
                &event.tenant_id,
                Some(event.owner_user_id.clone()),
                None,
                None,
            ),
        ];
        upsert_context_nodes(
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default(),
            nodes,
        );
    }

    fn write_state_context_locked(
        &self,
        data: &mut StoreData,
        routing: &EventIndexRouting,
        item: &StateItem,
    ) {
        let base = item.context_uri.clone();
        let secrets = self.redaction_config.configured_secret_values();
        let safe_title = mask_secret_fragment_projection_preserving_chars(&item.title, &secrets);
        let safe_statement =
            mask_secret_fragment_projection_preserving_chars(&item.statement, &secrets);
        let body = format!("{safe_title}: {safe_statement}");
        let abstract_body = truncate_chars(
            &mask_secret_fragment_projection_preserving_chars(&body, &secrets),
            500,
        );
        let nodes = vec![
            self.context_node(
                &format!("{base}/.abstract"),
                &item.title,
                0,
                &abstract_body,
                "personal",
                &routing.personal_context_index_uid,
                &item.tenant_id,
                Some(item.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/.overview"),
                &item.title,
                1,
                &json!({ "state": item }).to_string(),
                "personal",
                &routing.personal_context_index_uid,
                &item.tenant_id,
                Some(item.owner_user_id.clone()),
                None,
                None,
            ),
        ];
        upsert_context_nodes(
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default(),
            nodes,
        );
    }

    fn write_insight_context_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        routing: &EventIndexRouting,
        insight: &InsightRecord,
        evidence_text: Option<String>,
    ) {
        let base = insight.context_uri.clone();
        let secrets = self.redaction_config.configured_secret_values();
        let abstract_body = truncate_chars(
            &mask_secret_fragment_projection_preserving_chars(&insight.statement, &secrets),
            500,
        );
        let nodes = vec![
            self.context_node(
                &format!("{base}/.abstract"),
                &insight.title,
                0,
                &abstract_body,
                "personal",
                &routing.personal_context_index_uid,
                tenant_id,
                Some(insight.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/.overview"),
                &insight.title,
                1,
                &json!({ "insight": insight, "evidence": evidence_text }).to_string(),
                "personal",
                &routing.personal_context_index_uid,
                tenant_id,
                Some(insight.owner_user_id.clone()),
                None,
                None,
            ),
        ];
        upsert_context_nodes(
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default(),
            nodes,
        );
    }

    fn write_company_revision_context_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        revision: &SourceRevision,
    ) -> DocumentIngestResult {
        let source_document_uri =
            company_revision_source_document_uri(&revision.source_id, &revision.id);
        self.write_source_document_fragments_locked(
            data,
            tenant_id,
            None,
            "company_doc",
            &revision.source_id,
            &revision.id,
            &source_document_uri,
            &revision.title,
            &revision.content,
            &revision.checksum,
            "company",
            "rag_company_context",
            None,
            &[],
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_state_document_context_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        routing: &EventIndexRouting,
        owner_user_id: &str,
        state_type: &str,
        fact_key: &str,
        version: u32,
        title: &str,
        document: &StateDocumentPayload,
        policy: Option<&FragmentPolicy>,
    ) -> Result<DocumentIngestResult, ApiError> {
        let content = require_string(document.content.clone(), "document.content")?;
        let source_id = state_document_source_id(owner_user_id, state_type, fact_key);
        let revision_id = format!("v{version}");
        let checksum = hmac_hex(
            tenant_id.as_bytes(),
            "state-document",
            &format!("{source_id}:{revision_id}:{content}"),
            32,
        );
        let source_document_uri = format!(
            "ctx://user/state/{}/{}/source/{}",
            sanitize_slug(state_type),
            sanitize_slug(fact_key),
            sanitize_slug(&revision_id)
        );
        Ok(self.write_source_document_fragments_locked(
            data,
            tenant_id,
            Some(owner_user_id.to_string()),
            "state_doc",
            &source_id,
            &revision_id,
            &source_document_uri,
            title,
            &content,
            &checksum,
            "personal",
            &routing.personal_context_index_uid,
            policy,
            &[],
            &[],
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn write_source_document_fragments_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        owner_user_id: Option<String>,
        source_kind: &str,
        source_id: &str,
        revision_id: &str,
        source_document_uri: &str,
        title: &str,
        content: &str,
        checksum: &str,
        index_kind: &str,
        index_uid: &str,
        policy: Option<&FragmentPolicy>,
        blocks: &[ParsedBlock],
        artifact_refs: &[ParseArtifactRef],
    ) -> DocumentIngestResult {
        self.supersede_source_artifacts_locked(
            data,
            tenant_id,
            owner_user_id.as_deref(),
            source_id,
        );

        let now = now();
        let source_document_id =
            source_document_id(tenant_id, owner_user_id.as_deref(), source_id, revision_id);
        let source_document_key =
            SourceDocumentKey::new(tenant_id, owner_user_id.as_deref(), source_document_uri);
        let created_at = data
            .source_documents
            .get(&source_document_key)
            .map(|document| document.created_at)
            .unwrap_or(now);
        let source_document = SourceDocument {
            id: source_document_id,
            tenant_id: tenant_id.to_string(),
            owner_user_id: owner_user_id.clone(),
            source_kind: source_kind.to_string(),
            source_id: source_id.to_string(),
            revision_id: revision_id.to_string(),
            uri: source_document_uri.to_string(),
            title: title.to_string(),
            content: content.to_string(),
            checksum: checksum.to_string(),
            status: "active".to_string(),
            retrieval_enabled: false,
            created_at,
            updated_at: now,
        };
        data.source_documents
            .insert(source_document_key.clone(), source_document);

        if !blocks.is_empty() {
            data.parsed_blocks
                .insert(source_document_key, blocks.to_vec());
        }

        let redaction_secrets = self.redaction_config.configured_secret_values();
        let retrieval_title =
            mask_secret_egress_projection_preserving_chars(title, &redaction_secrets);
        let retrieval_content =
            mask_secret_fragment_projection_preserving_chars(content, &redaction_secrets);
        let retrieval_blocks = blocks
            .iter()
            .cloned()
            .map(|block| mask_parsed_block_for_retrieval(block, &redaction_secrets))
            .collect::<Vec<_>>();
        let fragmenter = BlockAwareFragmenter::from_policy(policy);
        let fragments = fragmenter.fragment(&retrieval_content, &retrieval_blocks);
        let fragment_uris = fragments
            .iter()
            .map(|fragment| {
                format!(
                    "{source_document_uri}/fragments/{:04}",
                    fragment.fragment_index + 1
                )
            })
            .collect::<Vec<_>>();
        let nodes = fragments
            .iter()
            .zip(fragment_uris.iter())
            .map(|(fragment, uri)| {
                self.fragment_context_node(
                    uri,
                    &retrieval_title,
                    index_kind,
                    index_uid,
                    tenant_id,
                    owner_user_id.clone(),
                    source_id,
                    revision_id,
                    source_document_uri,
                    fragment,
                    artifact_refs,
                )
            })
            .collect::<Vec<_>>();

        // Pre-embed the saved document and its fragments so the first
        // search after a save does not pay the embedding cost. Collected
        // before the nodes move into the context store; applied after the
        // write completes (lock order stays data -> vector).
        let mut warm_entries = Vec::with_capacity(nodes.len() + 1);
        warm_entries.push((
            vector_doc_key(index_uid, source_document_uri),
            format!("{retrieval_title} {retrieval_content}"),
        ));
        warm_entries.extend(
            nodes
                .iter()
                .map(|node| (vector_match_key(node), node_match_text(node))),
        );

        if index_kind == "company" {
            upsert_context_nodes(&mut data.company_context, nodes);
        } else {
            upsert_context_nodes(
                data.personal_context
                    .entry(index_uid.to_string())
                    .or_default(),
                nodes,
            );
        }

        for (fragment_uri, fragment) in fragment_uris.iter().zip(fragments.iter()) {
            let id = part_of_link_id(
                tenant_id,
                owner_user_id.as_deref(),
                fragment_uri,
                source_document_uri,
            );
            data.links.insert(
                id.clone(),
                KnowledgeLink {
                    id,
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: owner_user_id.clone(),
                    source_uri: fragment_uri.clone(),
                    target_uri: source_document_uri.to_string(),
                    source_title: Some(format!(
                        "{} fragment {}",
                        retrieval_title,
                        fragment.fragment_index + 1
                    )),
                    target_title: Some(retrieval_title.clone()),
                    relation: "part_of".to_string(),
                    rationale: Some("fragment generated from source document".to_string()),
                    evidence_text: None,
                    confidence: 1.0,
                    created_by: "system_fragmenter".to_string(),
                    status: "active".to_string(),
                    tags: vec![source_kind.to_string()],
                    created_at: now,
                    updated_at: now,
                },
            );
        }

        self.vector_warm(warm_entries);

        DocumentIngestResult {
            source_id: source_id.to_string(),
            source_document_uri: source_document_uri.to_string(),
            fragment_uris,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn context_node(
        &self,
        uri: &str,
        title: &str,
        layer: u8,
        body: &str,
        index_kind: &str,
        index_uid: &str,
        tenant_id: &str,
        owner_user_id: Option<String>,
        source_id: Option<String>,
        revision_id: Option<String>,
    ) -> ContextNode {
        ContextNode {
            uri: uri.to_string(),
            title: title.to_string(),
            layer,
            body: body.to_string(),
            tenant_id: tenant_id.to_string(),
            owner_user_id,
            index_uid: index_uid.to_string(),
            index_kind: index_kind.to_string(),
            ancestor_uris: ancestor_uris(uri),
            node_kind: node_kind_for_layer(layer).to_string(),
            retrieval_role: retrieval_role_for_layer(layer).to_string(),
            retrieval_enabled: layer == 2,
            parent_uri: None,
            source_document_uri: None,
            fragment_index: None,
            char_start: None,
            char_end: None,
            token_estimate: None,
            checksum: None,
            source_id,
            revision_id,
            block_type: None,
            page_idx: None,
            bbox: None,
            section_path: Vec::new(),
            heading_level: None,
            asset_refs: Vec::new(),
            artifact_refs: Vec::new(),
            status: "active".to_string(),
            privacy: if index_kind == "company" {
                "company".to_string()
            } else {
                "private".to_string()
            },
            updated_at: now(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fragment_context_node(
        &self,
        uri: &str,
        title: &str,
        index_kind: &str,
        index_uid: &str,
        tenant_id: &str,
        owner_user_id: Option<String>,
        source_id: &str,
        revision_id: &str,
        source_document_uri: &str,
        fragment: &FragmentChunk,
        artifact_refs: &[ParseArtifactRef],
    ) -> ContextNode {
        let mut node = self.context_node(
            uri,
            &format!("{} fragment {}", title, fragment.fragment_index + 1),
            2,
            &fragment.content,
            index_kind,
            index_uid,
            tenant_id,
            owner_user_id,
            Some(source_id.to_string()),
            Some(revision_id.to_string()),
        );
        node.node_kind = "fragment".to_string();
        node.retrieval_role = "fragment".to_string();
        node.retrieval_enabled = true;
        node.parent_uri = Some(source_document_uri.to_string());
        node.source_document_uri = Some(source_document_uri.to_string());
        node.fragment_index = Some(fragment.fragment_index);
        node.char_start = fragment.char_start;
        node.char_end = fragment.char_end;
        node.token_estimate = Some(fragment.token_estimate);
        node.checksum = Some(fragment.checksum.clone());
        node.block_type = fragment.block_type.clone();
        node.page_idx = fragment.page_idx;
        node.bbox = fragment.bbox.clone();
        node.section_path = fragment.section_path.clone();
        node.heading_level = fragment.heading_level;
        node.asset_refs = fragment.asset_refs.clone();
        node.artifact_refs = artifact_refs.to_vec();
        node
    }

    fn resolve_state_key(
        &self,
        tenant_id: &str,
        fact_key: &str,
        owner_user_id: Option<&str>,
    ) -> Result<(String, String, String), ApiError> {
        let data = self.read()?;
        if let Some(owner) = owner_user_id {
            return Ok((
                tenant_id.to_string(),
                owner.to_string(),
                fact_key.to_string(),
            ));
        }
        let matches: Vec<_> = data
            .state_items
            .keys()
            .filter(|(tenant, _, key)| tenant == tenant_id && key == fact_key)
            .cloned()
            .collect();
        match matches.len() {
            0 => Err(ApiError::not_found("state item not found")),
            1 => Ok(matches[0].clone()),
            _ => Err(ApiError::bad_request(
                "owner_user_id is required because fact_key is ambiguous",
            )),
        }
    }

    fn context_scope_locked(
        &self,
        data: &StoreData,
        tenant_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<Vec<ContextNode>, ApiError> {
        let mut nodes: Vec<_> = data
            .company_context
            .iter()
            .filter(|node| {
                node.tenant_id == tenant_id
                    && node.owner_user_id.is_none()
                    && node.privacy == "company"
                    && node.index_kind == "company"
                    && node.index_uid == "rag_company_context"
            })
            .cloned()
            .collect();
        if let Some(owner) = owner_user_id {
            let routing = self.resolver.resolve(tenant_id, owner, false, true)?;
            nodes.extend(
                data.personal_context
                    .get(&routing.personal_context_index_uid)
                    .into_iter()
                    .flatten()
                    .filter(|node| {
                        node.tenant_id == tenant_id
                            && node.owner_user_id.as_deref() == Some(owner)
                            && node.privacy == "private"
                            && node.index_kind == "personal"
                            && node.index_uid == routing.personal_context_index_uid
                    })
                    .cloned(),
            );
        }
        Ok(nodes)
    }

    fn context_scope_for_acl_locked(
        &self,
        data: &StoreData,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<Vec<ContextNode>, ApiError> {
        if include_all_private && owner_user_id.is_none() {
            let mut nodes = self.context_scope_locked(data, tenant_id, None)?;
            for index in data
                .user_indexes
                .values()
                .filter(|index| index.tenant_id == tenant_id)
            {
                nodes.extend(
                    data.personal_context
                        .get(&index.personal_context_index_uid)
                        .into_iter()
                        .flatten()
                        .filter(|node| {
                            node.tenant_id == tenant_id
                                && node.privacy == "private"
                                && node.index_kind == "personal"
                                && node.index_uid == index.personal_context_index_uid
                                && node.owner_user_id.as_deref().is_some_and(|owner| {
                                    self.resolver.user_hash(owner) == index.owner_user_id_hash
                                })
                        })
                        .cloned(),
                );
            }
            return Ok(nodes);
        }
        self.context_scope_locked(data, tenant_id, owner_user_id)
    }

    fn context_node_for_acl_locked(
        &self,
        data: &StoreData,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
        predicate: impl Fn(&ContextNode) -> bool,
    ) -> Result<Option<ContextNode>, ApiError> {
        let nodes = self
            .context_scope_for_acl_locked(data, tenant_id, owner_user_id, include_all_private)?
            .into_iter()
            .filter(predicate)
            .collect::<Vec<_>>();

        if let Some(owner) = owner_user_id {
            return Ok(nodes
                .iter()
                .find(|node| node.owner_user_id.as_deref() == Some(owner))
                .cloned()
                .or_else(|| {
                    nodes
                        .iter()
                        .find(|node| node.owner_user_id.is_none())
                        .cloned()
                }));
        }
        if !include_all_private {
            return Ok(nodes.into_iter().find(|node| node.owner_user_id.is_none()));
        }
        if let Some(company) = nodes
            .iter()
            .find(|node| node.owner_user_id.is_none())
            .cloned()
        {
            return Ok(Some(company));
        }
        let private_nodes = nodes
            .into_iter()
            .filter(|node| node.owner_user_id.is_some())
            .collect::<Vec<_>>();
        match private_nodes.len() {
            0 => Ok(None),
            1 => Ok(private_nodes.into_iter().next()),
            _ => Err(ApiError::bad_request(
                "owner_user_id is required because the context uri is ambiguous",
            )),
        }
    }

    fn source_document_for_acl_locked(
        &self,
        data: &StoreData,
        tenant_id: &str,
        uri: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<Option<SourceDocument>, ApiError> {
        let document = if owner_user_id.is_some() {
            owner_user_id
                .and_then(|owner| {
                    data.source_documents
                        .get(&SourceDocumentKey::new(tenant_id, Some(owner), uri))
                        .filter(|document| document.status == "active")
                })
                .or_else(|| {
                    data.source_documents
                        .get(&SourceDocumentKey::new(tenant_id, None, uri))
                        .filter(|document| document.status == "active")
                })
        } else if include_all_private {
            let private_count = data
                .source_documents
                .iter()
                .filter(|(key, document)| {
                    key.tenant_id == tenant_id
                        && key.uri == uri
                        && key.owner_user_id.is_some()
                        && document.status == "active"
                })
                .count();
            let has_active_company = data
                .source_documents
                .get(&SourceDocumentKey::new(tenant_id, None, uri))
                .is_some_and(|document| document.status == "active");
            if !has_active_company && private_count > 1 {
                return Err(ApiError::bad_request(
                    "owner_user_id is required because the source-document uri is ambiguous",
                ));
            }
            source_document_for_admin_without_owner_locked(data, tenant_id, uri)
        } else {
            data.source_documents
                .get(&SourceDocumentKey::new(tenant_id, None, uri))
                .filter(|document| document.status == "active")
        };
        Ok(document.cloned())
    }

    fn supersede_source_artifacts_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        source_id: &str,
    ) {
        let superseded_source_document_uris = data
            .source_documents
            .values()
            .filter(|document| {
                document.tenant_id == tenant_id
                    && document.owner_user_id.as_deref() == owner_user_id
                    && document.source_id == source_id
                    && document.status == "active"
            })
            .map(|document| document.uri.clone())
            .collect::<HashSet<_>>();

        for document in data.source_documents.values_mut() {
            if document.tenant_id == tenant_id
                && document.owner_user_id.as_deref() == owner_user_id
                && document.source_id == source_id
                && document.status == "active"
            {
                document.status = "superseded".to_string();
                document.updated_at = now();
            }
        }

        for node in &mut data.company_context {
            if node.tenant_id == tenant_id
                && node.owner_user_id.as_deref() == owner_user_id
                && node.source_id.as_deref() == Some(source_id)
                && node.status == "active"
            {
                node.status = "superseded".to_string();
                node.retrieval_enabled = false;
                node.updated_at = now();
            }
        }
        for nodes in data.personal_context.values_mut() {
            for node in nodes {
                if node.tenant_id == tenant_id
                    && node.owner_user_id.as_deref() == owner_user_id
                    && node.source_id.as_deref() == Some(source_id)
                    && node.status == "active"
                {
                    node.status = "superseded".to_string();
                    node.retrieval_enabled = false;
                    node.updated_at = now();
                }
            }
        }

        for link in data.links.values_mut() {
            if link.tenant_id == tenant_id
                && link.owner_user_id.as_deref() == owner_user_id
                && link.relation == "part_of"
                && link.status == "active"
                && superseded_source_document_uris.contains(&link.target_uri)
            {
                link.status = "superseded".to_string();
                link.updated_at = now();
            }
        }
    }

    fn read(&self) -> Result<std::sync::RwLockReadGuard<'_, StoreData>, ApiError> {
        self.inner
            .read()
            .map_err(|_| ApiError::Internal("store read lock poisoned".to_string()))
    }

    fn write(&self) -> Result<std::sync::RwLockWriteGuard<'_, StoreData>, ApiError> {
        self.inner
            .write()
            .map_err(|_| ApiError::Internal("store write lock poisoned".to_string()))
    }
}

fn source_document_for_owner_locked<'a>(
    data: &'a StoreData,
    tenant_id: &str,
    owner_user_id: Option<&str>,
    uri: &str,
) -> Option<&'a SourceDocument> {
    owner_user_id
        .and_then(|owner| {
            data.source_documents
                .get(&SourceDocumentKey::new(tenant_id, Some(owner), uri))
        })
        .or_else(|| {
            data.source_documents
                .get(&SourceDocumentKey::new(tenant_id, None, uri))
        })
}

fn select_admin_source_document(
    documents: Vec<SourceDocument>,
) -> Result<Option<SourceDocument>, ApiError> {
    if let Some(company_document) = documents
        .iter()
        .find(|document| document.owner_user_id.is_none() && document.status == "active")
        .cloned()
    {
        return Ok(Some(company_document));
    }
    let private_documents = documents
        .into_iter()
        .filter(|document| document.owner_user_id.is_some() && document.status == "active")
        .collect::<Vec<_>>();
    match private_documents.len() {
        0 => Ok(None),
        1 => Ok(private_documents.into_iter().next()),
        _ => Err(ApiError::bad_request(
            "owner_user_id is required because the source-document uri is ambiguous",
        )),
    }
}

fn source_document_for_admin_without_owner_locked<'a>(
    data: &'a StoreData,
    tenant_id: &str,
    uri: &str,
) -> Option<&'a SourceDocument> {
    if let Some(company_document) = data
        .source_documents
        .get(&SourceDocumentKey::new(tenant_id, None, uri))
        .filter(|document| document.status == "active")
    {
        return Some(company_document);
    }

    let mut private_documents = data.source_documents.iter().filter_map(|(key, document)| {
        (key.tenant_id == tenant_id
            && key.uri == uri
            && key.owner_user_id.is_some()
            && document.status == "active")
            .then_some(document)
    });
    let document = private_documents.next()?;
    private_documents.next().is_none().then_some(document)
}

fn default_harness_components() -> Vec<(&'static str, &'static str, &'static str, &'static str)> {
    vec![
        (
            "retrieval.context_search",
            "Context Search",
            "retrieval",
            "Ranks active fragments for default RAG context retrieval.",
        ),
        (
            "retrieval.traceback",
            "Traceback",
            "retrieval",
            "Maps fragments back to source documents and parse artifacts.",
        ),
        (
            "ingestion.fragmenter",
            "Fragmenter",
            "ingestion",
            "Turns parsed documents into retrievable fragment nodes.",
        ),
        (
            "ingestion.parser_adapter",
            "Parser Adapter",
            "ingestion",
            "Normalizes parser output into blocks and artifacts.",
        ),
        (
            "llm.rag_answer_prompt",
            "RAG Answer Prompt",
            "llm",
            "Builds grounded answer prompts over retrieved citations.",
        ),
        (
            "llm.analysis_prompt",
            "Analysis Prompt",
            "llm",
            "Builds grounded analysis prompts for insight generation.",
        ),
        (
            "memory.insight_policy",
            "Insight Policy",
            "memory",
            "Controls insight extraction and update decisions.",
        ),
        (
            "memory.state_materialization_policy",
            "State Materialization Policy",
            "memory",
            "Controls current-state writes and history evidence.",
        ),
        (
            "safety.owner_acl",
            "Owner ACL",
            "safety",
            "Prevents cross-owner private context leakage.",
        ),
        (
            "safety.source_doc_retrieval_guard",
            "Source Document Retrieval Guard",
            "safety",
            "Keeps source documents out of default context retrieval.",
        ),
        (
            "health.llm_probe",
            "LLM Probe",
            "health",
            "Controls LLM health and readiness evidence.",
        ),
    ]
}

fn bootstrap_harness_revision_id(component_id: &str) -> String {
    format!("hrev_bootstrap_{}", sanitize_slug(component_id))
}

fn previous_revision_id(
    revisions: &[HarnessComponentRevision],
    current_revision_id: Option<&str>,
) -> Option<String> {
    revisions
        .iter()
        .filter(|revision| Some(revision.id.as_str()) != current_revision_id)
        .filter(|revision| revision.status != "rolled_back")
        .max_by_key(|revision| revision.iteration)
        .map(|revision| revision.id.clone())
}

fn latest_eval_run_for_change(data: &StoreData, change_id: &str) -> Option<RagEvalRun> {
    data.eval_runs
        .values()
        .filter(|run| run.change_id.as_deref() == Some(change_id))
        .cloned()
        .max_by_key(|run| run.created_at)
}

fn eval_results_by_case(data: &StoreData, run: &RagEvalRun) -> HashMap<String, RagEvalCaseResult> {
    run.result_ids
        .iter()
        .filter_map(|result_id| data.eval_case_results.get(result_id))
        .map(|result| (result.case_id.clone(), result.clone()))
        .collect()
}

fn predicted_fix_confirmations(predicted_fixes: &[String], delta: &EvalDeltaReport) -> Vec<String> {
    if predicted_fixes.is_empty() {
        return delta.fixed_cases.clone();
    }
    predicted_fixes
        .iter()
        .filter(|fix| {
            delta.fixed_cases.iter().any(|case_id| case_id == *fix)
                || delta.fixed_cases.iter().any(|case_id| {
                    delta
                        .risk_matrix
                        .iter()
                        .any(|risk| risk.case_id == *case_id && result_matches_label(risk, fix))
                })
        })
        .cloned()
        .collect()
}

fn result_matches_label(result: &RiskCaseResult, label: &str) -> bool {
    result.case_id == label
        || result
            .baseline_failures
            .iter()
            .chain(result.candidate_failures.iter())
            .any(|failure| failure == label)
}

fn risk_matrix_for_change(
    change: &HarnessChangeManifest,
    baseline_results: &HashMap<String, RagEvalCaseResult>,
    candidate_results: &HashMap<String, RagEvalCaseResult>,
    regressed_cases: &[String],
) -> Vec<RiskCaseResult> {
    let mut risk_case_ids = change
        .risk_cases
        .iter()
        .flat_map(|risk| {
            let mut ids = Vec::new();
            if baseline_results.contains_key(risk) || candidate_results.contains_key(risk) {
                ids.push(risk.clone());
            }
            let matching_cases = candidate_results
                .iter()
                .filter(|(case_id, result)| {
                    !ids.contains(case_id)
                        && (result.failures.iter().any(|failure| failure == risk)
                            || result.guard_failures.iter().any(|failure| failure == risk))
                })
                .map(|(case_id, _)| case_id.clone())
                .collect::<Vec<_>>();
            ids.extend(matching_cases);
            ids
        })
        .collect::<Vec<_>>();
    risk_case_ids.extend(regressed_cases.iter().cloned());
    risk_case_ids.sort();
    risk_case_ids.dedup();

    risk_case_ids
        .into_iter()
        .map(|case_id| {
            let baseline = baseline_results.get(&case_id);
            let candidate = candidate_results.get(&case_id);
            let baseline_status = baseline
                .map(|result| result.status.clone())
                .unwrap_or_else(|| "missing".to_string());
            let candidate_status = candidate
                .map(|result| result.status.clone())
                .unwrap_or_else(|| "missing".to_string());
            RiskCaseResult {
                case_id,
                regressed: baseline_status == "passed" && candidate_status == "failed",
                baseline_status,
                candidate_status,
                baseline_failures: baseline
                    .map(|result| result.failures.clone())
                    .unwrap_or_default(),
                candidate_failures: candidate
                    .map(|result| result.failures.clone())
                    .unwrap_or_default(),
            }
        })
        .collect()
}

fn metric_deltas(baseline: &RagEvalMetrics, candidate: &RagEvalMetrics) -> Value {
    json!({
        "pass_rate": candidate.pass_rate - baseline.pass_rate,
        "retrieval_recall_at_5": candidate.retrieval_recall_at_5 - baseline.retrieval_recall_at_5,
        "citation_precision": candidate.citation_precision - baseline.citation_precision,
        "traceback_success_rate": candidate.traceback_success_rate - baseline.traceback_success_rate,
        "source_doc_leak_rate": candidate.source_doc_leak_rate - baseline.source_doc_leak_rate,
        "acl_violation_rate": candidate.acl_violation_rate - baseline.acl_violation_rate,
        "stale_fragment_rate": candidate.stale_fragment_rate - baseline.stale_fragment_rate,
        "state_history_consistency_rate": candidate.state_history_consistency_rate - baseline.state_history_consistency_rate,
        "llm_health_false_ready_rate": candidate.llm_health_false_ready_rate - baseline.llm_health_false_ready_rate,
        "tokens_per_answer": candidate.tokens_per_answer - baseline.tokens_per_answer,
        "latency_p95": candidate.latency_p95 - baseline.latency_p95
    })
}

fn metrics_to_value(metrics: &RagEvalMetrics) -> Value {
    json!({
        "pass_rate": metrics.pass_rate,
        "retrieval_recall_at_5": metrics.retrieval_recall_at_5,
        "citation_precision": metrics.citation_precision,
        "traceback_success_rate": metrics.traceback_success_rate,
        "source_doc_leak_rate": metrics.source_doc_leak_rate,
        "acl_violation_rate": metrics.acl_violation_rate,
        "stale_fragment_rate": metrics.stale_fragment_rate,
        "state_history_consistency_rate": metrics.state_history_consistency_rate,
        "llm_health_false_ready_rate": metrics.llm_health_false_ready_rate,
        "tokens_per_answer": metrics.tokens_per_answer,
        "latency_p95": metrics.latency_p95
    })
}

fn verdict_evidence_text(run: Option<&RagEvalRun>, overview: Option<&RagEvalOverview>) -> String {
    let mut parts = Vec::new();
    if let Some(run) = run {
        parts.push(run.status.clone());
        for guard in &run.guard_results {
            if !guard.passed {
                parts.push(guard.name.clone());
            }
        }
    }
    if let Some(overview) = overview {
        for cluster in &overview.failure_patterns {
            parts.push(cluster.pattern.clone());
            parts.extend(cluster.root_cause_notes.clone());
        }
    }
    parts.join("\n").to_lowercase()
}

fn contains_folded(haystack: &str, needle: &str) -> bool {
    !needle.trim().is_empty() && haystack.to_lowercase().contains(&needle.to_lowercase())
}

fn aggregate_eval_metrics(results: &[RagEvalCaseResult]) -> RagEvalMetrics {
    if results.is_empty() {
        return RagEvalMetrics::default();
    }
    let total = results.len() as f64;
    RagEvalMetrics {
        pass_rate: results
            .iter()
            .filter(|result| result.status == "passed")
            .count() as f64
            / total,
        retrieval_recall_at_5: average_result_metric(results, "retrieval_recall_at_5"),
        citation_precision: average_result_metric(results, "citation_precision"),
        traceback_success_rate: average_result_metric(results, "traceback_success_rate"),
        source_doc_leak_rate: average_result_metric(results, "source_doc_leak_rate"),
        acl_violation_rate: average_result_metric(results, "acl_violation_rate"),
        stale_fragment_rate: average_result_metric(results, "stale_fragment_rate"),
        state_history_consistency_rate: 1.0,
        llm_health_false_ready_rate: 0.0,
        tokens_per_answer: average_result_metric(results, "tokens_per_answer"),
        latency_p95: latency_p95(results) as f64,
    }
}

fn average_result_metric(results: &[RagEvalCaseResult], key: &str) -> f64 {
    results
        .iter()
        .map(|result| {
            result
                .metrics
                .get(key)
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        })
        .sum::<f64>()
        / results.len().max(1) as f64
}

fn latency_p95(results: &[RagEvalCaseResult]) -> u64 {
    if results.is_empty() {
        return 0;
    }
    let mut latencies = results
        .iter()
        .map(|result| result.latency_ms)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let index = ((latencies.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
    latencies[index.min(latencies.len() - 1)]
}

fn build_eval_overview(run: &RagEvalRun, results: &[RagEvalCaseResult]) -> RagEvalOverview {
    let failure_patterns = failure_pattern_clusters(results);
    let suggested_target_component = failure_patterns
        .first()
        .map(|cluster| cluster.suggested_target_component.clone())
        .unwrap_or_else(|| "retrieval.context_search".to_string());
    let root_cause_notes = failure_patterns
        .iter()
        .flat_map(|cluster| cluster.root_cause_notes.clone())
        .collect::<Vec<_>>();
    let mut markdown = String::new();
    markdown.push_str(&format!("# RAG Eval Overview {}\n\n", run.id));
    markdown.push_str(&format!("status: {}\n\n", run.status));
    markdown.push_str("## Metrics\n");
    for (name, value) in [
        ("pass_rate", run.metrics.pass_rate),
        ("retrieval_recall_at_5", run.metrics.retrieval_recall_at_5),
        ("citation_precision", run.metrics.citation_precision),
        ("traceback_success_rate", run.metrics.traceback_success_rate),
        ("source_doc_leak_rate", run.metrics.source_doc_leak_rate),
        ("acl_violation_rate", run.metrics.acl_violation_rate),
        ("stale_fragment_rate", run.metrics.stale_fragment_rate),
        (
            "state_history_consistency_rate",
            run.metrics.state_history_consistency_rate,
        ),
        (
            "llm_health_false_ready_rate",
            run.metrics.llm_health_false_ready_rate,
        ),
        ("tokens_per_answer", run.metrics.tokens_per_answer),
        ("latency_p95", run.metrics.latency_p95),
    ] {
        markdown.push_str(&format!("- {name}: {value:.3}\n"));
    }
    markdown.push_str("\n## Failure Patterns\n");
    if failure_patterns.is_empty() {
        markdown.push_str("- none\n");
    } else {
        for cluster in &failure_patterns {
            markdown.push_str(&format!(
                "- {}: {} case(s), target {}\n",
                cluster.pattern, cluster.count, cluster.suggested_target_component
            ));
        }
    }
    markdown.push_str(&format!(
        "\n## Suggested Target Component\n{}\n",
        suggested_target_component
    ));
    RagEvalOverview {
        tenant_id: run.tenant_id.clone(),
        run_id: run.id.clone(),
        status: run.status.clone(),
        metrics: run.metrics.clone(),
        failure_patterns,
        suggested_target_component,
        root_cause_notes,
        overview_markdown: markdown,
        case_report_uris: Vec::new(),
        overview_source_document_uri: None,
        generated_at: now(),
    }
}

fn failure_pattern_clusters(results: &[RagEvalCaseResult]) -> Vec<FailurePatternCluster> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for result in results {
        for failure in &result.failures {
            grouped
                .entry(failure.clone())
                .or_default()
                .push(result.case_id.clone());
        }
    }
    let mut clusters = grouped
        .into_iter()
        .map(|(pattern, case_ids)| FailurePatternCluster {
            suggested_target_component: suggested_component_for_failure(&pattern).to_string(),
            root_cause_notes: vec![root_cause_note_for_failure(&pattern).to_string()],
            count: case_ids.len(),
            case_ids,
            pattern,
        })
        .collect::<Vec<_>>();
    clusters.sort_by_key(|cluster| Reverse(cluster.count));
    clusters
}

fn suggested_component_for_failure(failure: &str) -> &'static str {
    match failure {
        "traceback_missing" => "retrieval.traceback",
        "source_doc_leak" => "safety.source_doc_retrieval_guard",
        "acl_violation" => "safety.owner_acl",
        "stale_fragment" => "ingestion.fragmenter",
        "answer_expectation" => "llm.rag_answer_prompt",
        "citation_precision" => "retrieval.traceback",
        _ => "retrieval.context_search",
    }
}

fn root_cause_note_for_failure(failure: &str) -> &'static str {
    match failure {
        "traceback_missing" => "A retrieved fragment did not resolve to source-document evidence.",
        "source_doc_leak" => "Default retrieval included a non-fragment or source-document node.",
        "acl_violation" => "A private node crossed the requested owner boundary.",
        "stale_fragment" => {
            "A retrieved fragment was inactive, superseded, or not retrieval-enabled."
        }
        "answer_expectation" => "The grounded answer did not contain expected answer evidence.",
        "citation_precision" => "Retrieved citations did not align with expected source documents.",
        _ => "Expected evidence was not present in the top retrieved fragments.",
    }
}

fn guard_name_for_failure(failure: &str) -> Option<&'static str> {
    match failure {
        "source_doc_leak" => Some("source_doc_not_default_retrieved"),
        "traceback_missing" => Some("fragment_traceback_required"),
        "acl_violation" => Some("owner_acl_never_leaks"),
        "stale_fragment" => Some("superseded_fragments_not_active"),
        _ => None,
    }
}

fn guard_case_ids(results: &[RagEvalCaseResult], guard_name: &str) -> Vec<String> {
    results
        .iter()
        .filter(|result| {
            result
                .guard_failures
                .iter()
                .any(|failure| failure == guard_name)
        })
        .map(|result| result.case_id.clone())
        .collect()
}

fn part_of_links_guard_locked(data: &StoreData, tenant_id: &str) -> (bool, Value) {
    let nodes = all_context_nodes_for_guard(data);
    let mut missing_links = Vec::new();
    let mut stale_links = Vec::new();
    for node in nodes
        .iter()
        .filter(|node| node.tenant_id == tenant_id && node.node_kind == "fragment")
        .filter(|node| node.source_document_uri.is_some())
        .filter(|node| node.status == "active")
    {
        let has_active_link = data.links.values().any(|link| {
            link.tenant_id == tenant_id
                && link.status == "active"
                && link.relation == "part_of"
                && link.source_uri == node.uri
        });
        if !has_active_link {
            missing_links.push(node.uri.clone());
        }
    }
    for link in data.links.values().filter(|link| {
        link.tenant_id == tenant_id && link.status == "active" && link.relation == "part_of"
    }) {
        if source_document_for_owner_locked(
            data,
            tenant_id,
            link.owner_user_id.as_deref(),
            &link.target_uri,
        )
        .is_some_and(|document| document.status != "active")
        {
            stale_links.push(link.id.clone());
        }
    }
    (
        missing_links.is_empty() && stale_links.is_empty(),
        json!({ "missing_links": missing_links, "stale_links": stale_links }),
    )
}

fn superseded_fragments_guard_locked(data: &StoreData, tenant_id: &str) -> (bool, Value) {
    let nodes = all_context_nodes_for_guard(data);
    let mut unsafe_fragments = Vec::new();
    for node in nodes
        .iter()
        .filter(|node| node.tenant_id == tenant_id && node.node_kind == "fragment")
        .filter(|node| node.source_document_uri.is_some())
    {
        let source_superseded = node
            .source_document_uri
            .as_ref()
            .and_then(|uri| {
                source_document_for_owner_locked(
                    data,
                    tenant_id,
                    node.owner_user_id.as_deref(),
                    uri,
                )
            })
            .is_some_and(|document| document.status != "active");
        if (node.status == "superseded" && node.retrieval_enabled)
            || (node.status == "active" && source_superseded)
        {
            unsafe_fragments.push(node.uri.clone());
        }
    }
    (
        unsafe_fragments.is_empty(),
        json!({ "unsafe_fragments": unsafe_fragments }),
    )
}

fn state_history_guard_locked(data: &StoreData, tenant_id: &str) -> (bool, Value) {
    let mut missing_state_items = Vec::new();
    for item in data
        .state_items
        .values()
        .filter(|item| item.tenant_id == tenant_id && item.status == "active")
    {
        let has_history = data.event_by_id.values().any(|event| {
            event.tenant_id == tenant_id
                && event.owner_user_id == item.owner_user_id
                && event.entity_type == "state_item"
                && event.entity_id == item.id
                && matches!(event.event_type.as_str(), "state.changed" | "state.patched")
        });
        if !has_history {
            missing_state_items.push(item.id.clone());
        }
    }
    (
        missing_state_items.is_empty(),
        json!({ "missing_state_items": missing_state_items }),
    )
}

fn all_context_nodes_for_guard(data: &StoreData) -> Vec<&ContextNode> {
    let mut nodes = data.company_context.iter().collect::<Vec<_>>();
    for personal in data.personal_context.values() {
        nodes.extend(personal.iter());
    }
    nodes
}

fn case_result_markdown(result: &RagEvalCaseResult) -> String {
    format!(
        "# Eval Case {}\n\nstatus: {}\n\ntrace_id: {}\n\n## Retrieved URIs\n{}\n\n## Source Documents\n{}\n\n## Failures\n{}\n",
        result.case_id,
        result.status,
        result.trace_id,
        markdown_list(&result.retrieved_uris),
        markdown_list(&result.source_document_uris),
        markdown_list(&result.failures),
    )
}

fn markdown_list(values: &[String]) -> String {
    if values.is_empty() {
        "- none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn copy_idempotency_entries<K>(
    current: &mut HashMap<K, String>,
    staged: &HashMap<K, String>,
    accepted_ids: &HashSet<String>,
) where
    K: Clone + Eq + std::hash::Hash,
{
    for (key, id) in staged {
        if accepted_ids.contains(id) {
            current.insert(key.clone(), id.clone());
        }
    }
}

fn upsert_source_document_cache(data: &mut StoreData, document: SourceDocument) {
    let key = SourceDocumentKey::from_document(&document);
    if data
        .source_documents
        .get(&key)
        .is_none_or(|existing| existing.updated_at <= document.updated_at)
    {
        data.source_documents.insert(key, document);
    }
}

fn upsert_ingest_task_cache(data: &mut StoreData, task: IngestTask) {
    if data
        .ingest_tasks
        .get(&task.task_id)
        .is_none_or(|existing| existing.updated_at <= task.updated_at)
    {
        data.ingest_tasks.insert(task.task_id.clone(), task);
    }
}

fn upsert_context_nodes(target: &mut Vec<ContextNode>, nodes: Vec<ContextNode>) {
    for node in nodes {
        if let Some(existing) = target.iter_mut().find(|existing| {
            existing.tenant_id == node.tenant_id
                && existing.uri == node.uri
                && existing.layer == node.layer
        }) {
            if existing.updated_at <= node.updated_at {
                *existing = node;
            }
        } else {
            target.push(node);
        }
    }
}

/// Scoped key for vector-match entries. `index_uid` is the resolver-derived
/// per-user (or company) index UID, so keys cannot collide across owners —
/// the same isolation primitive the rest of the store relies on.
fn vector_match_key(node: &ContextNode) -> String {
    format!("{}|{}", node.index_uid, node.uri)
}

fn node_match_text(node: &ContextNode) -> String {
    format!("{} {}", node.title, node.body)
}

/// Scoped key for a document-level vector entry, derived from the same
/// index UID as the fragments that reference the document.
fn vector_doc_key(index_uid: &str, source_document_uri: &str) -> String {
    format!("doc|{index_uid}|{source_document_uri}")
}

/// Collect distinct `(scoped key, title + content)` candidates for the
/// source documents referenced by `nodes`. Scope safety: `nodes` are
/// already isolation-filtered and every fragment references its own
/// document, so the candidate set cannot leave the caller's visibility.
fn doc_candidates_locked(data: &StoreData, nodes: &[ContextNode]) -> Vec<(String, String)> {
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for node in nodes {
        let Some(uri) = node.source_document_uri.as_deref() else {
            continue;
        };
        let key = vector_doc_key(&node.index_uid, uri);
        if !seen.insert(key.clone()) {
            continue;
        }
        let Some(document) = source_document_for_owner_locked(
            data,
            &node.tenant_id,
            node.owner_user_id.as_deref(),
            uri,
        ) else {
            continue;
        };
        if document.status != "active" {
            continue;
        }
        candidates.push((key, format!("{} {}", document.title, document.content)));
    }
    candidates
}

/// Blended relevance for a node: lexical substring score plus
/// fragment-level vector score, boosted by document-level vector evidence
/// from the node's source document.
///
/// Document evidence only ever boosts a fragment that already matched on
/// its own (lexically or by fragment vector) — it never admits one. Source
/// document bodies are excluded from default retrieval by contract, so a
/// query matching only the raw document text must not surface its
/// fragments; the regression suite pins this.
fn hybrid_node_score(
    node: &ContextNode,
    query: &str,
    vector_scores: &VectorScoreMap,
    doc_scores: &VectorScoreMap,
) -> Option<f32> {
    let text = text_score(&node_match_text(node), query);
    let fragment = vector_scores.combined_score(&vector_match_key(node), text)?;
    let document = node
        .source_document_uri
        .as_deref()
        .and_then(|uri| doc_scores.evidence(&vector_doc_key(&node.index_uid, uri)))
        .unwrap_or(0.0);
    Some(fragment + document)
}

fn score_breakdown_value(
    node: &ContextNode,
    query: &str,
    vector_scores: &VectorScoreMap,
    doc_scores: &VectorScoreMap,
    combined: f32,
) -> Value {
    let mut breakdown = serde_json::Map::new();
    breakdown.insert(
        "lexical".to_string(),
        json!(text_score(&node_match_text(node), query)),
    );
    if let Some(vector) = vector_scores.vector_score(&vector_match_key(node)) {
        breakdown.insert("vector".to_string(), json!(vector));
    }
    if let Some(document) = node
        .source_document_uri
        .as_deref()
        .and_then(|uri| doc_scores.vector_score(&vector_doc_key(&node.index_uid, uri)))
    {
        breakdown.insert("document_vector".to_string(), json!(document));
    }
    breakdown.insert("combined".to_string(), json!(combined));
    Value::Object(breakdown)
}

fn rank_nodes(
    nodes: impl Iterator<Item = ContextNode>,
    query: &str,
    limit: usize,
    vector_scores: &VectorScoreMap,
    doc_scores: &VectorScoreMap,
) -> Vec<(ContextNode, f32)> {
    let mut scored: Vec<_> = nodes
        .filter_map(|node| {
            hybrid_node_score(&node, query, vector_scores, doc_scores)
                .filter(|score| *score > 0.0)
                .map(|score| (node, score))
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);
    scored
}

fn retrieval_candidate(node: &ContextNode) -> bool {
    node.status == "active" && node.retrieval_enabled && node.retrieval_role == "fragment"
}

fn context_hit_from_node(
    node: &ContextNode,
    score: f32,
    redaction_secrets: &[String],
) -> ContextHit {
    let safe_body = mask_secret_fragment_projection_preserving_chars(&node.body, redaction_secrets);
    ContextHit {
        uri: node.uri.clone(),
        title: node.title.clone(),
        layer: node.layer,
        score,
        node_kind: Some(node.node_kind.clone()),
        retrieval_role: Some(node.retrieval_role.clone()),
        source_id: node.source_id.clone(),
        revision_id: node.revision_id.clone(),
        source_document_uri: node.source_document_uri.clone(),
        source_title: None,
        source_relation: None,
        fragment_index: node.fragment_index,
        char_start: node.char_start,
        char_end: node.char_end,
        block_type: node.block_type.clone(),
        page_idx: node.page_idx,
        bbox: node.bbox.clone(),
        section_path: node.section_path.clone(),
        heading_level: node.heading_level,
        asset_refs: node.asset_refs.clone(),
        artifact_refs: node.artifact_refs.clone(),
        checksum: node.checksum.clone(),
        source_summary: None,
        neighbor_fragments: Vec::new(),
        related_links: Vec::new(),
        score_breakdown: None,
        snippet: truncate_chars(&safe_body, 240),
    }
}

fn citation_from_hit(hit: &ContextHit) -> Citation {
    Citation {
        uri: hit.uri.clone(),
        node_kind: hit.node_kind.clone(),
        retrieval_role: hit.retrieval_role.clone(),
        source_id: hit.source_id.clone(),
        revision_id: hit.revision_id.clone(),
        source_document_uri: hit.source_document_uri.clone(),
        source_title: hit.source_title.clone(),
        block_type: hit.block_type.clone(),
        page_idx: hit.page_idx,
        bbox: hit.bbox.clone(),
        section_path: hit.section_path.clone(),
        heading_level: hit.heading_level,
        asset_refs: hit.asset_refs.clone(),
        artifact_refs: hit.artifact_refs.clone(),
        fragment_index: hit.fragment_index,
        char_start: hit.char_start,
        char_end: hit.char_end,
        checksum: hit.checksum.clone(),
        title: hit.title.clone(),
        quote: hit.snippet.clone(),
        score: hit.score,
    }
}

fn source_document_context_node(document: SourceDocument) -> ContextNode {
    ContextNode {
        uri: document.uri.clone(),
        title: document.title.clone(),
        layer: 2,
        body: document.content.clone(),
        tenant_id: document.tenant_id.clone(),
        owner_user_id: document.owner_user_id.clone(),
        index_uid: "rag_source_documents".to_string(),
        index_kind: if document.owner_user_id.is_some() {
            "personal".to_string()
        } else {
            "company".to_string()
        },
        ancestor_uris: ancestor_uris(&document.uri),
        node_kind: "source_doc".to_string(),
        retrieval_role: "none".to_string(),
        retrieval_enabled: false,
        parent_uri: None,
        source_document_uri: Some(document.uri),
        fragment_index: None,
        char_start: None,
        char_end: None,
        token_estimate: Some(document.content.chars().count().div_ceil(4).max(1)),
        checksum: Some(document.checksum),
        source_id: Some(document.source_id),
        revision_id: Some(document.revision_id),
        block_type: None,
        page_idx: None,
        bbox: None,
        section_path: Vec::new(),
        heading_level: None,
        asset_refs: Vec::new(),
        artifact_refs: Vec::new(),
        status: document.status,
        privacy: if document.owner_user_id.is_some() {
            "private".to_string()
        } else {
            "company".to_string()
        },
        updated_at: document.updated_at,
    }
}

fn parsed_block_text(block: &ParsedBlock) -> Option<String> {
    block
        .text
        .clone()
        .or_else(|| block.html.clone())
        .or_else(|| block.latex.clone())
        .or_else(|| block.caption.clone())
        .filter(|value| !value.trim().is_empty())
}

#[allow(clippy::too_many_arguments)]
fn build_parse_artifacts(
    tenant_id: &str,
    owner_user_id: Option<String>,
    source_document_uri: &str,
    source_id: &str,
    revision_id: &str,
    parsed: &ParserOutput,
    original_content: &str,
) -> Result<Vec<ParseArtifact>, ApiError> {
    let mut artifacts = Vec::new();
    if !original_content.is_empty() {
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "original",
            format!("{source_document_uri}/artifacts/original"),
            original_content.as_bytes(),
        ));
    }
    if let Some(markdown) = parsed
        .markdown
        .as_deref()
        .filter(|markdown| !markdown.trim().is_empty())
    {
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "markdown",
            format!("{source_document_uri}/artifacts/markdown"),
            markdown.as_bytes(),
        ));
    }
    for (kind, value) in [
        ("content_list", parsed.content_list.as_ref()),
        ("content_list_v2", parsed.content_list_v2.as_ref()),
        ("middle_json", parsed.middle_json.as_ref()),
        ("model_json", parsed.model_json.as_ref()),
    ] {
        if let Some(value) = value {
            let bytes = serde_json::to_vec(value)
                .map_err(|err| ApiError::Internal(format!("failed to encode {kind}: {err}")))?;
            artifacts.push(parse_artifact_from_bytes(
                tenant_id,
                owner_user_id.clone(),
                source_document_uri,
                source_id,
                revision_id,
                parsed,
                kind,
                format!("{source_document_uri}/artifacts/{kind}"),
                &bytes,
            ));
        }
    }

    for (index, image) in parsed.images.iter().enumerate() {
        let uri = image_artifact_uri(source_document_uri, image, index as u32);
        let bytes = serde_json::to_vec(image)
            .map_err(|err| ApiError::Internal(format!("failed to encode image artifact: {err}")))?;
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "image",
            uri,
            &bytes,
        ));
    }

    for (index, image_ref) in parsed
        .blocks
        .iter()
        .filter_map(|block| block.image_ref.as_deref())
        .enumerate()
    {
        if artifacts.iter().any(|artifact| artifact.uri == image_ref) {
            continue;
        }
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "image",
            image_ref.to_string(),
            image_ref.as_bytes(),
        ));
        if index > 10_000 {
            break;
        }
    }

    Ok(artifacts)
}

#[allow(clippy::too_many_arguments)]
fn parse_artifact_from_bytes(
    tenant_id: &str,
    owner_user_id: Option<String>,
    source_document_uri: &str,
    source_id: &str,
    revision_id: &str,
    parsed: &ParserOutput,
    kind: &str,
    uri: String,
    bytes: &[u8],
) -> ParseArtifact {
    ParseArtifact {
        id: parse_artifact_id(&uri),
        tenant_id: tenant_id.to_string(),
        owner_user_id,
        source_document_uri: source_document_uri.to_string(),
        source_id: source_id.to_string(),
        revision_id: revision_id.to_string(),
        parser_provider: parsed.provider.clone(),
        parser_backend: parsed.backend.clone(),
        parser_version: parsed.parser_version.clone(),
        artifact_kind: kind.to_string(),
        uri,
        checksum: sha256_hex(bytes),
        byte_size: bytes.len(),
        created_at: now(),
    }
}

fn image_artifact_uri(source_document_uri: &str, image: &Value, index: u32) -> String {
    image
        .as_str()
        .map(ToString::to_string)
        .or_else(|| {
            image
                .get("uri")
                .or_else(|| image.get("path"))
                .or_else(|| image.get("image_path"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| format!("{source_document_uri}/artifacts/images/{index:04}"))
}

fn parse_artifact_id(uri: &str) -> String {
    format!(
        "artifact_{}",
        sha256_hex(uri.as_bytes())
            .chars()
            .take(24)
            .collect::<String>()
    )
}

fn sanitize_ingest_error(state: &str, error: Option<&str>) -> Option<&'static str> {
    if state != "failed" {
        return None;
    }
    match error {
        Some(INGEST_ERROR_PARSER_FAILED) => Some(INGEST_ERROR_PARSER_FAILED),
        Some(INGEST_ERROR_INDEXING_FAILED) => Some(INGEST_ERROR_INDEXING_FAILED),
        Some(INGEST_ERROR_INTERRUPTED) => Some(INGEST_ERROR_INTERRUPTED),
        Some(_) | None => Some(INGEST_ERROR_FAILED),
    }
}

fn apply_ingest_task_transition(task: &mut IngestTask, state: &str, error: Option<&str>) {
    task.state = state.to_string();
    task.error = sanitize_ingest_error(state, error).map(ToString::to_string);
    task.updated_at = now();
    if matches!(state, "completed" | "failed") {
        task.completed_at = Some(task.updated_at);
    } else {
        task.completed_at = None;
    }
}

fn is_nonterminal_ingest_state(state: &str) -> bool {
    matches!(
        state,
        "queued" | "parsing" | "parsed" | "fragmenting" | "indexing"
    )
}

fn sanitize_ingest_task(mut task: IngestTask) -> IngestTask {
    task.error = sanitize_ingest_error(&task.state, task.error.as_deref()).map(ToString::to_string);
    task
}

fn mask_parsed_block_for_retrieval(
    mut block: ParsedBlock,
    redaction_secrets: &[String],
) -> ParsedBlock {
    block.text = block
        .text
        .map(|value| mask_secret_fragment_projection_preserving_chars(&value, redaction_secrets));
    block.html = block
        .html
        .map(|value| mask_secret_fragment_projection_preserving_chars(&value, redaction_secrets));
    block.latex = block
        .latex
        .map(|value| mask_secret_fragment_projection_preserving_chars(&value, redaction_secrets));
    block.image_ref = block
        .image_ref
        .map(|value| mask_secret_fragment_projection_preserving_chars(&value, redaction_secrets));
    block.caption = block
        .caption
        .map(|value| mask_secret_fragment_projection_preserving_chars(&value, redaction_secrets));
    block.footnote = block
        .footnote
        .map(|value| mask_secret_fragment_projection_preserving_chars(&value, redaction_secrets));
    block.section_path = block
        .section_path
        .into_iter()
        .map(|value| mask_secret_egress_projection_preserving_chars(&value, redaction_secrets))
        .collect();
    block
}

fn ingest_task_visible(
    task: &IngestTask,
    owner_user_id: Option<&str>,
    include_all_private: bool,
) -> bool {
    include_all_private
        || task.owner_user_id.is_none()
        || task.owner_user_id.as_deref() == owner_user_id
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn node_kind_for_layer(layer: u8) -> &'static str {
    match layer {
        0 => "abstract",
        1 => "overview",
        _ => "fragment",
    }
}

fn retrieval_role_for_layer(layer: u8) -> &'static str {
    match layer {
        2 => "fragment",
        1 => "overview",
        _ => "none",
    }
}

fn stage_value(stage: &str, hits: &[(ContextNode, f32)], owner_user_id: Option<&str>) -> Value {
    json!({
        "stage": stage,
        "owner_scoped": owner_user_id.is_some(),
        "hits": hits.iter().map(|(node, score)| json!({
            "uri": node.uri,
            "layer": node.layer,
            "score": score,
            "index_alias": node.index_kind,
        })).collect::<Vec<_>>()
    })
}

fn strip_layer_suffix(uri: &str) -> String {
    uri.strip_suffix("/.abstract")
        .or_else(|| uri.strip_suffix("/.overview"))
        .or_else(|| uri.strip_suffix("/detail"))
        .or_else(|| uri.strip_suffix("/chunks/0001"))
        .unwrap_or(uri)
        .to_string()
}

fn canonical_link_uri(uri: &str) -> String {
    strip_layer_suffix(uri.trim())
}

fn validate_analysis_materialization_request(
    req: &AnalysisMaterializationRequest,
) -> Result<(), ApiError> {
    fn required_text(value: &str, field: &str, max_bytes: usize) -> Result<(), ApiError> {
        if value.trim().is_empty() {
            return Err(ApiError::bad_request(format!("{field} is required")));
        }
        bounded_text(value, field, max_bytes)
    }

    fn bounded_text(value: &str, field: &str, max_bytes: usize) -> Result<(), ApiError> {
        if value.len() > max_bytes {
            return Err(ApiError::bad_request(format!(
                "{field} exceeds the analysis materialization byte limit"
            )));
        }
        if value.chars().any(char::is_control) {
            return Err(ApiError::bad_request(format!(
                "{field} contains control characters"
            )));
        }
        Ok(())
    }

    fn score(value: f32, field: &str) -> Result<(), ApiError> {
        if !value.is_finite() || !(0.0..=1.0).contains(&value) {
            return Err(ApiError::bad_request(format!(
                "{field} must be a finite number between 0 and 1"
            )));
        }
        Ok(())
    }

    fn canonical_uri(value: &str, field: &str) -> Result<String, ApiError> {
        crate::analysis::canonicalize_analysis_uri(value)
            .ok_or_else(|| ApiError::bad_request(format!("{field} must be a valid ctx:// URI")))
    }

    for (index, candidate) in req.links.iter().enumerate() {
        let source_uri =
            canonical_uri(&candidate.source_uri, &format!("links[{index}].source_uri"))?;
        let target_uri =
            canonical_uri(&candidate.target_uri, &format!("links[{index}].target_uri"))?;
        if source_uri == target_uri {
            return Err(ApiError::bad_request(
                "source_uri and target_uri must refer to different context nodes",
            ));
        }
        let relation = candidate.relation.trim().to_ascii_lowercase();
        if !crate::analysis::ALLOWED_ANALYSIS_RELATIONS.contains(&relation.as_str()) {
            return Err(ApiError::bad_request(format!(
                "links[{index}].relation is not allowed for analysis materialization"
            )));
        }
        if let Some(title) = candidate.source_title.as_deref() {
            bounded_text(
                title,
                &format!("links[{index}].source_title"),
                crate::analysis::MAX_TITLE_BYTES,
            )?;
        }
        if let Some(title) = candidate.target_title.as_deref() {
            bounded_text(
                title,
                &format!("links[{index}].target_title"),
                crate::analysis::MAX_TITLE_BYTES,
            )?;
        }
        if let Some(rationale) = candidate.rationale.as_deref() {
            bounded_text(
                rationale,
                &format!("links[{index}].rationale"),
                crate::analysis::MAX_RATIONALE_BYTES,
            )?;
        }
        score(candidate.confidence, &format!("links[{index}].confidence"))?;
        if candidate.tags.len() > crate::analysis::MAX_TAGS_PER_CANDIDATE {
            return Err(ApiError::bad_request(format!(
                "links[{index}].tags exceeds the analysis materialization limit"
            )));
        }
        let mut tags = HashSet::with_capacity(candidate.tags.len());
        for (tag_index, tag) in candidate.tags.iter().enumerate() {
            required_text(
                tag,
                &format!("links[{index}].tags[{tag_index}]"),
                crate::analysis::MAX_TAG_BYTES,
            )?;
            if !tags.insert(tag.trim()) {
                return Err(ApiError::bad_request(format!(
                    "links[{index}].tags contains a duplicate"
                )));
            }
        }
    }

    for (index, candidate) in req.insights.iter().enumerate() {
        required_text(
            &candidate.insight_type,
            &format!("insights[{index}].insight_type"),
            crate::analysis::MAX_INSIGHT_TYPE_BYTES,
        )?;
        required_text(
            &candidate.title,
            &format!("insights[{index}].title"),
            crate::analysis::MAX_TITLE_BYTES,
        )?;
        required_text(
            &candidate.statement,
            &format!("insights[{index}].statement"),
            crate::analysis::MAX_STATEMENT_BYTES,
        )?;
        score(
            candidate.confidence,
            &format!("insights[{index}].confidence"),
        )?;
        score(candidate.salience, &format!("insights[{index}].salience"))?;
        if candidate.source_uris.is_empty() {
            return Err(ApiError::bad_request(format!(
                "insights[{index}].source_uris is required"
            )));
        }
        if candidate.source_uris.len() > crate::analysis::MAX_SOURCE_URIS_PER_INSIGHT {
            return Err(ApiError::bad_request(format!(
                "insights[{index}].source_uris exceeds the analysis materialization limit"
            )));
        }
        let mut source_uris = HashSet::with_capacity(candidate.source_uris.len());
        for (source_index, source_uri) in candidate.source_uris.iter().enumerate() {
            let source_uri = canonical_uri(
                source_uri,
                &format!("insights[{index}].source_uris[{source_index}]"),
            )?;
            if !source_uris.insert(source_uri) {
                return Err(ApiError::bad_request(format!(
                    "insights[{index}].source_uris contains a duplicate"
                )));
            }
        }
    }
    Ok(())
}

fn normalize_relation(relation: &str) -> String {
    let relation = relation.trim();
    if relation.is_empty() {
        "related".to_string()
    } else {
        sanitize_slug(relation)
    }
}

fn link_natural_key(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    source_uri: &str,
    target_uri: &str,
    relation: &str,
) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        tenant_id,
        owner_user_id.unwrap_or(""),
        source_uri,
        target_uri,
        relation
    )
}

fn link_search_text(link: &KnowledgeLink) -> String {
    format!(
        "{} {} {} {} {} {} {}",
        link.source_uri,
        link.target_uri,
        link.source_title.as_deref().unwrap_or_default(),
        link.target_title.as_deref().unwrap_or_default(),
        link.relation,
        link.rationale.as_deref().unwrap_or_default(),
        link.tags.join(" ")
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContextReturnProfile {
    Compact,
    Standard,
    Full,
}

impl ContextReturnProfile {
    fn from_request(value: &str) -> Result<Self, ApiError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "standard" => Ok(Self::Standard),
            "compact" => Ok(Self::Compact),
            "full" => Ok(Self::Full),
            other => Err(ApiError::bad_request(format!(
                "unsupported return_profile: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, Default)]
struct ContextIncludeSet {
    traceback: bool,
    links: bool,
    neighbor_fragments: bool,
    source_summary: bool,
    artifact_refs: bool,
    score_breakdown: bool,
}

impl ContextIncludeSet {
    fn from_request(values: &[String]) -> Result<Self, ApiError> {
        let mut include = Self::default();
        for value in values {
            match value.trim().to_ascii_lowercase().as_str() {
                "" => {}
                "traceback" => include.traceback = true,
                "links" => include.links = true,
                "neighbor_fragments" => include.neighbor_fragments = true,
                "source_summary" => include.source_summary = true,
                "artifact_refs" => include.artifact_refs = true,
                "score_breakdown" => include.score_breakdown = true,
                "raw_stage_debug" => {}
                other => {
                    return Err(ApiError::bad_request(format!(
                        "unsupported include value: {other}"
                    )));
                }
            }
        }
        Ok(include)
    }
}

fn resolve_context_owner(
    owner_user_id: Option<String>,
    filters: &Value,
) -> Result<Option<String>, ApiError> {
    let filter_owner = owner_from_filters(filters).map(ToString::to_string);
    match (owner_user_id, filter_owner) {
        (Some(owner), Some(filter_owner)) if owner != filter_owner => Err(ApiError::bad_request(
            "owner_user_id and filters.owner_user_id must match",
        )),
        (Some(owner), _) => Ok(Some(owner)),
        (None, owner) => Ok(owner),
    }
}

fn parse_context_filters(filters: &Value) -> Result<ContextStructuredFilters, ApiError> {
    if filters.is_null() {
        return Ok(ContextStructuredFilters::default());
    }
    let object = filters
        .as_object()
        .ok_or_else(|| ApiError::bad_request("filters must be an object"))?;
    Ok(ContextStructuredFilters {
        source_id: optional_filter_string(object, "source_id")?,
        revision_id: optional_filter_string(object, "revision_id")?,
        source_document_uri: optional_filter_string(object, "source_document_uri")?,
        block_type: optional_filter_string(object, "block_type")?,
        page_idx: optional_filter_u32(object, "page_idx")?,
        page_idx_gte: optional_filter_u32(object, "page_idx_gte")?,
        page_idx_lte: optional_filter_u32(object, "page_idx_lte")?,
        section_path_contains: optional_filter_string(object, "section_path_contains")?,
        artifact_kind: optional_filter_string(object, "artifact_kind")?,
    })
}

fn optional_filter_string(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<String>, ApiError> {
    match object.get(key) {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(Value::String(_)) | None | Some(Value::Null) => Ok(None),
        Some(_) => Err(ApiError::bad_request(format!("{key} must be a string"))),
    }
}

fn optional_filter_u32(
    object: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<Option<u32>, ApiError> {
    match object.get(key) {
        Some(Value::Number(value)) => value
            .as_u64()
            .and_then(|value| u32::try_from(value).ok())
            .map(Some)
            .ok_or_else(|| ApiError::bad_request(format!("{key} must be a non-negative integer"))),
        Some(Value::String(value)) if !value.trim().is_empty() => value
            .parse::<u32>()
            .map(Some)
            .map_err(|_| ApiError::bad_request(format!("{key} must be a non-negative integer"))),
        Some(Value::String(_)) | None | Some(Value::Null) => Ok(None),
        Some(_) => Err(ApiError::bad_request(format!(
            "{key} must be a non-negative integer"
        ))),
    }
}

fn enrich_context_hits_locked(
    data: &StoreData,
    tenant_id: &str,
    owner_user_id: Option<&str>,
    nodes: &[ContextNode],
    hits: &mut [ContextHit],
    include: &ContextIncludeSet,
    profile: ContextReturnProfile,
) {
    let wants_source_metadata = profile != ContextReturnProfile::Compact || include.traceback;
    let wants_source_summary =
        include.source_summary || matches!(profile, ContextReturnProfile::Full);
    let wants_links = include.links || matches!(profile, ContextReturnProfile::Full);
    let wants_neighbors = include.neighbor_fragments;

    for hit in hits {
        if wants_source_metadata {
            if let Some(document) =
                source_document_for_hit_locked(data, tenant_id, owner_user_id, hit)
            {
                hit.source_document_uri = Some(document.uri.clone());
                hit.source_id = Some(document.source_id.clone());
                hit.revision_id = Some(document.revision_id.clone());
                hit.source_title = Some(document.title.clone());
                if include.traceback || matches!(profile, ContextReturnProfile::Full) {
                    hit.source_relation = Some("part_of".to_string());
                }
                if wants_source_summary {
                    hit.source_summary = Some(ContextSourceSummary {
                        source_document_uri: document.uri.clone(),
                        source_id: document.source_id.clone(),
                        revision_id: document.revision_id.clone(),
                        source_title: document.title.clone(),
                    });
                }
            }
        }

        if wants_links {
            hit.related_links = related_links_for_hit_locked(
                data,
                tenant_id,
                owner_user_id,
                &hit.uri,
                include.links,
            );
        }
        if wants_neighbors {
            hit.neighbor_fragments =
                neighbor_fragments_for_hit_locked(data, tenant_id, owner_user_id, nodes, hit);
        }
        if include.score_breakdown && hit.score_breakdown.is_none() {
            hit.score_breakdown = Some(json!({ "lexical": hit.score }));
        }
    }
}

fn source_document_for_hit_locked(
    data: &StoreData,
    tenant_id: &str,
    owner_user_id: Option<&str>,
    hit: &ContextHit,
) -> Option<SourceDocument> {
    let source_document_uri = hit
        .source_document_uri
        .as_deref()
        .map(ToString::to_string)
        .or_else(|| {
            data.links
                .values()
                .find(|link| {
                    link.tenant_id == tenant_id
                        && link.status == "active"
                        && link.relation == "part_of"
                        && link.source_uri == hit.uri
                        && link_visible_to_owner(link, owner_user_id)
                })
                .map(|link| link.target_uri.clone())
        })?;
    source_document_for_owner_locked(data, tenant_id, owner_user_id, &source_document_uri)
        .filter(|document| document.status == "active")
        .cloned()
}

fn related_links_for_hit_locked(
    data: &StoreData,
    tenant_id: &str,
    owner_user_id: Option<&str>,
    uri: &str,
    include_non_part_of: bool,
) -> Vec<ContextRelatedLink> {
    let mut part_of = data
        .links
        .values()
        .filter(|link| {
            link.tenant_id == tenant_id
                && link.status == "active"
                && link.relation == "part_of"
                && link.source_uri == uri
                && link_visible_to_owner(link, owner_user_id)
        })
        .map(context_related_link)
        .collect::<Vec<_>>();
    part_of.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| b.updated_at.cmp(&a.updated_at))
    });

    if include_non_part_of {
        let mut other_links = data
            .links
            .values()
            .filter(|link| {
                link.tenant_id == tenant_id
                    && link.status == "active"
                    && link.relation != "part_of"
                    && (link.source_uri == uri || link.target_uri == uri)
                    && link_visible_to_owner(link, owner_user_id)
            })
            .map(context_related_link)
            .collect::<Vec<_>>();
        other_links.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.updated_at.cmp(&a.updated_at))
        });
        part_of.extend(other_links.into_iter().take(5));
    }

    part_of
}

fn context_related_link(link: &KnowledgeLink) -> ContextRelatedLink {
    ContextRelatedLink {
        id: link.id.clone(),
        source_uri: link.source_uri.clone(),
        target_uri: link.target_uri.clone(),
        relation: link.relation.clone(),
        source_title: link.source_title.clone(),
        target_title: link.target_title.clone(),
        confidence: link.confidence,
        updated_at: link.updated_at,
    }
}

fn neighbor_fragments_for_hit_locked(
    data: &StoreData,
    tenant_id: &str,
    owner_user_id: Option<&str>,
    scoped_nodes: &[ContextNode],
    hit: &ContextHit,
) -> Vec<ContextNeighborFragment> {
    let Some(source_document_uri) = hit.source_document_uri.as_deref() else {
        return Vec::new();
    };
    let Some(fragment_index) = hit.fragment_index else {
        return Vec::new();
    };
    let mut candidates = scoped_nodes
        .iter()
        .cloned()
        .chain(data.company_context.iter().cloned())
        .chain(data.personal_context.values().flatten().cloned())
        .filter(|node| node.uri != hit.uri)
        .filter(|node| node.tenant_id == tenant_id)
        .filter(retrieval_candidate)
        .filter(|node| node.source_document_uri.as_deref() == Some(source_document_uri))
        .filter(|node| {
            owner_can_see_optional(node.owner_user_id.as_deref(), owner_user_id)
                && node
                    .fragment_index
                    .is_some_and(|idx| idx.abs_diff(fragment_index) <= 1)
        })
        .map(|node| ContextNeighborFragment {
            uri: node.uri,
            title: node.title,
            fragment_index: node.fragment_index,
            page_idx: node.page_idx,
            block_type: node.block_type,
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|fragment| fragment.fragment_index.unwrap_or(u32::MAX));
    candidates.dedup_by(|a, b| a.uri == b.uri);
    candidates
}

fn link_visible_to_owner(link: &KnowledgeLink, owner_user_id: Option<&str>) -> bool {
    owner_can_see_optional(link.owner_user_id.as_deref(), owner_user_id)
}

fn owner_can_see_optional(resource_owner: Option<&str>, owner_user_id: Option<&str>) -> bool {
    if let Some(owner) = owner_user_id {
        resource_owner.is_none() || resource_owner == Some(owner)
    } else {
        resource_owner.is_none()
    }
}

fn shape_context_hits(
    hits: Vec<ContextHit>,
    profile: ContextReturnProfile,
    include: &ContextIncludeSet,
) -> Vec<ContextHit> {
    if profile != ContextReturnProfile::Compact {
        return hits;
    }
    hits.into_iter()
        .map(|mut hit| {
            hit.node_kind = None;
            hit.retrieval_role = None;
            if !(include.traceback || include.source_summary) {
                hit.source_id = None;
                hit.revision_id = None;
                hit.source_document_uri = None;
                hit.source_title = None;
                hit.source_relation = None;
            }
            hit.fragment_index = None;
            hit.char_start = None;
            hit.char_end = None;
            hit.block_type = None;
            hit.page_idx = None;
            hit.bbox = None;
            hit.section_path.clear();
            hit.heading_level = None;
            hit.asset_refs.clear();
            if !include.artifact_refs {
                hit.artifact_refs.clear();
            }
            hit.checksum = None;
            if !include.source_summary {
                hit.source_summary = None;
            }
            if !include.neighbor_fragments {
                hit.neighbor_fragments.clear();
            }
            if !include.links {
                hit.related_links.clear();
            }
            if !include.score_breakdown {
                hit.score_breakdown = None;
            }
            hit
        })
        .collect()
}

fn context_source_groups(
    profile: ContextReturnProfile,
    hits: &[ContextHit],
) -> Vec<ContextSourceGroup> {
    if matches!(profile, ContextReturnProfile::Compact) {
        return Vec::new();
    }

    #[derive(Default)]
    struct Accumulator {
        group: Option<ContextSourceGroup>,
        page_min: Option<u32>,
        page_max: Option<u32>,
    }

    let mut order = Vec::new();
    let mut groups: HashMap<String, Accumulator> = HashMap::new();
    for hit in hits {
        let key = hit
            .source_document_uri
            .clone()
            .unwrap_or_else(|| hit.uri.clone());
        if !groups.contains_key(&key) {
            order.push(key.clone());
        }
        let accumulator = groups.entry(key.clone()).or_default();
        if accumulator.group.is_none() {
            accumulator.group = Some(ContextSourceGroup {
                source_document_uri: key,
                source_id: hit.source_id.clone().unwrap_or_default(),
                revision_id: hit.revision_id.clone().unwrap_or_default(),
                source_title: hit
                    .source_title
                    .clone()
                    .unwrap_or_else(|| hit.title.clone()),
                top_score: hit.score,
                hit_count: 0,
                page_range: None,
                block_types: Vec::new(),
                top_hit_uri: hit.uri.clone(),
            });
        }
        let group = accumulator.group.as_mut().expect("group initialized");
        group.top_score = group.top_score.max(hit.score);
        group.hit_count += 1;
        if let Some(page_idx) = hit.page_idx {
            accumulator.page_min = Some(
                accumulator
                    .page_min
                    .map_or(page_idx, |min| min.min(page_idx)),
            );
            accumulator.page_max = Some(
                accumulator
                    .page_max
                    .map_or(page_idx, |max| max.max(page_idx)),
            );
        }
        if let Some(block_type) = hit.block_type.as_deref() {
            if !group.block_types.iter().any(|value| value == block_type) {
                group.block_types.push(block_type.to_string());
            }
        }
    }

    order
        .into_iter()
        .filter_map(|key| {
            let mut accumulator = groups.remove(&key)?;
            let mut group = accumulator.group.take()?;
            group.page_range = accumulator
                .page_min
                .zip(accumulator.page_max)
                .map(|(start, end)| ContextPageRange { start, end });
            Some(group)
        })
        .collect()
}

fn sanitize_context_stages(stages: Vec<Value>, debug: bool, is_admin: bool) -> Vec<Value> {
    stages
        .into_iter()
        .map(|stage| sanitize_context_stage(stage, debug, is_admin))
        .collect()
}

fn sanitize_context_stage(stage: Value, debug: bool, is_admin: bool) -> Value {
    let Value::Object(mut object) = stage else {
        return stage;
    };
    let raw_stage = Value::Object(object.clone());
    if !debug {
        object.remove("index_uid");
        object.remove("filter");
        object.remove("raw_stage_debug");
        return Value::Object(object);
    }

    if is_admin {
        object.insert("raw_stage_debug".to_string(), raw_stage);
        return Value::Object(object);
    }

    if let Some(index_uid) = object.get_mut("index_uid") {
        if index_uid
            .as_str()
            .is_some_and(|value| value != "rag_company_context")
        {
            *index_uid = json!("personal_context_redacted");
        }
    }
    if object.get("filter").is_some() {
        object.insert("filter".to_string(), json!("redacted"));
    }
    object.remove("raw_stage_debug");
    Value::Object(object)
}

fn owner_from_filters(filters: &Value) -> Option<&str> {
    filters
        .get("owner_user_id")
        .and_then(Value::as_str)
        .or_else(|| filters.get("owner").and_then(Value::as_str))
}

fn token_similarity(a: &str, b: &str) -> f32 {
    let left: HashSet<_> = a
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(ToString::to_string)
        .collect();
    let right: HashSet<_> = b
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(ToString::to_string)
        .collect();
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let intersection = left.intersection(&right).count() as f32;
    let union = left.union(&right).count() as f32;
    intersection / union
}

fn user_event_index_id(tenant_hash: &str, owner_user_id_hash: &str) -> String {
    format!("uei__t_{tenant_hash}__u_{owner_user_id_hash}")
}

fn company_revision_source_document_uri(source_id: &str, revision_id: &str) -> String {
    format!(
        "ctx://company/docs/{}/source/{}",
        sanitize_slug(source_id),
        sanitize_slug(revision_id)
    )
}

fn state_document_source_id(owner_user_id: &str, state_type: &str, fact_key: &str) -> String {
    format!(
        "state:{}:{}:{}",
        sanitize_slug(owner_user_id),
        sanitize_slug(state_type),
        sanitize_slug(fact_key)
    )
}

fn source_document_id(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    source_id: &str,
    revision_id: &str,
) -> String {
    format!(
        "srcdoc_{}",
        hmac_hex(
            b"nowledge-source-document",
            "source_document",
            &format!(
                "{}:{}:{}:{}",
                tenant_id,
                owner_user_id.unwrap_or(""),
                source_id,
                revision_id
            ),
            24,
        )
    )
}

fn part_of_link_id(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    source_uri: &str,
    target_uri: &str,
) -> String {
    format!(
        "link_{}",
        hmac_hex(
            b"nowledge-part-of-link",
            "part_of",
            &link_natural_key(tenant_id, owner_user_id, source_uri, target_uri, "part_of"),
            24,
        )
    )
}

fn deterministic_stats(rows: &[Value], prior_rows_by_period: &[(String, Vec<Value>)]) -> Value {
    let mut numeric: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for (key, value) in obj {
                if let Some(number) = value.as_f64() {
                    numeric.entry(key.clone()).or_default().push(number);
                }
            }
        }
    }
    let prior_stats = prior_rows_by_period
        .iter()
        .map(|(period_key, rows)| (period_key.clone(), numeric_means(rows)))
        .collect::<Vec<_>>();
    let metrics = numeric
        .into_iter()
        .map(|(key, values)| {
            let count = values.len();
            let sum: f64 = values.iter().sum();
            let mean = if count == 0 { 0.0 } else { sum / count as f64 };
            let min = values.iter().copied().fold(f64::INFINITY, f64::min);
            let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            let previous_mean = prior_stats
                .first()
                .and_then(|(_, means)| means.get(&key))
                .copied();
            let recent_values = prior_stats
                .iter()
                .filter_map(|(_, means)| means.get(&key).copied())
                .collect::<Vec<_>>();
            let recent_4_mean = mean_of(&recent_values);
            let delta_vs_previous = previous_mean.map(|previous| mean - previous);
            let delta_vs_recent_4 = recent_4_mean.map(|recent| mean - recent);
            let trend_direction = trend_direction(delta_vs_recent_4.or(delta_vs_previous));
            let anomaly = recent_4_mean
                .map(|recent| {
                    let baseline = recent.abs().max(1.0);
                    ((mean - recent).abs() / baseline) >= 0.35
                })
                .unwrap_or(false);
            json!({
                "metric": key,
                "count": count,
                "mean": mean,
                "min": min,
                "max": max,
                "slope": simple_slope(&values),
                "previous_mean": previous_mean,
                "delta_vs_previous": delta_vs_previous,
                "recent_4_mean": recent_4_mean,
                "delta_vs_recent_4": delta_vs_recent_4,
                "trend_direction": trend_direction,
                "anomaly": anomaly
            })
        })
        .collect::<Vec<_>>();
    json!({
        "row_count": rows.len(),
        "prior_period_count": prior_rows_by_period.len(),
        "prior_periods": prior_rows_by_period
            .iter()
            .map(|(period_key, rows)| json!({
                "period_key": period_key,
                "row_count": rows.len()
            }))
            .collect::<Vec<_>>(),
        "metrics": metrics
    })
}

fn numeric_means(rows: &[Value]) -> HashMap<String, f64> {
    let mut numeric: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for (key, value) in obj {
                if let Some(number) = value.as_f64() {
                    numeric.entry(key.clone()).or_default().push(number);
                }
            }
        }
    }
    numeric
        .into_iter()
        .filter_map(|(key, values)| mean_of(&values).map(|mean| (key, mean)))
        .collect()
}

fn mean_of(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn trend_direction(delta: Option<f64>) -> &'static str {
    match delta {
        Some(delta) if delta > 0.05 => "up",
        Some(delta) if delta < -0.05 => "down",
        Some(_) => "flat",
        None => "unknown",
    }
}

fn simple_slope(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    (values[values.len() - 1] - values[0]) / (values.len() - 1) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[tokio::test]
    async fn memory_audit_persistence_updates_one_record_in_place() {
        let config = Config::test();
        let store = Store::new(&config);
        let occurred_at = now();
        let mut record = AuditRecord {
            id: new_id("audit"),
            tenant_id: config.tenant_id.clone(),
            request_id: uuid::Uuid::now_v7().to_string(),
            principal_scope: AuditPrincipalScope::Admin,
            principal_owner_user_id_hash: None,
            resource_id_hash: format!("hmac:{}", "a".repeat(32)),
            action: AuditAction::CompanyDocDelete,
            reason_code: AuditReasonCode::AdminDelete,
            reason_fingerprint: format!("hmac:{}", "b".repeat(32)),
            outcome: AuditOutcome::Attempted,
            error_kind: None,
            operation_id: None,
            occurred_at,
            updated_at: occurred_at,
        };

        store.persist_audit_record(&record).await.unwrap();
        record.outcome = AuditOutcome::Success;
        record.updated_at = now();
        store.persist_audit_record(&record).await.unwrap();

        let data = store.read().unwrap();
        assert_eq!(data.audit_records.len(), 1);
        assert_eq!(
            data.audit_records[&record.id].outcome,
            AuditOutcome::Success
        );
    }

    #[tokio::test]
    async fn memory_audit_cache_evicts_the_oldest_record_at_its_fixed_ceiling() {
        let config = Config::test();
        let store = Store::new(&config);
        let started = now() - chrono::Duration::days(1);
        let mut oldest_id = None;
        {
            let mut data = store.write().unwrap();
            for offset in 0..MAX_IN_MEMORY_AUDIT_RECORDS {
                let occurred_at = started + chrono::Duration::milliseconds(offset as i64);
                let id = new_id("audit");
                if offset == 0 {
                    oldest_id = Some(id.clone());
                }
                data.audit_records.insert(
                    id.clone(),
                    AuditRecord {
                        id,
                        tenant_id: config.tenant_id.clone(),
                        request_id: uuid::Uuid::now_v7().to_string(),
                        principal_scope: AuditPrincipalScope::Admin,
                        principal_owner_user_id_hash: None,
                        resource_id_hash: format!("hmac:{}", "a".repeat(32)),
                        action: AuditAction::CompanyDocDelete,
                        reason_code: AuditReasonCode::AdminDelete,
                        reason_fingerprint: format!("hmac:{}", "b".repeat(32)),
                        outcome: AuditOutcome::Attempted,
                        error_kind: None,
                        operation_id: None,
                        occurred_at,
                        updated_at: occurred_at,
                    },
                );
            }
        }

        let occurred_at = now();
        let newest = AuditRecord {
            id: new_id("audit"),
            tenant_id: config.tenant_id.clone(),
            request_id: uuid::Uuid::now_v7().to_string(),
            principal_scope: AuditPrincipalScope::Admin,
            principal_owner_user_id_hash: None,
            resource_id_hash: format!("hmac:{}", "c".repeat(32)),
            action: AuditAction::CompanyDocDelete,
            reason_code: AuditReasonCode::AdminDelete,
            reason_fingerprint: format!("hmac:{}", "d".repeat(32)),
            outcome: AuditOutcome::Attempted,
            error_kind: None,
            operation_id: None,
            occurred_at,
            updated_at: occurred_at,
        };
        store.persist_audit_record(&newest).await.unwrap();

        let data = store.read().unwrap();
        assert_eq!(data.audit_records.len(), MAX_IN_MEMORY_AUDIT_RECORDS);
        assert!(!data
            .audit_records
            .contains_key(oldest_id.as_deref().expect("oldest audit id")));
        assert!(data.audit_records.contains_key(&newest.id));
    }

    #[test]
    fn direct_insight_patch_cannot_cross_tenants() {
        let config = Config::test();
        let store = Store::new(&config);
        let created = store
            .upsert_insight(
                "tenant-a",
                InsightUpsertRequest {
                    owner_user_id: Some("owner-a".to_string()),
                    insight_type: Some("isolation".to_string()),
                    title: Some("Tenant-bound insight".to_string()),
                    statement: Some("original statement".to_string()),
                    ..InsightUpsertRequest::default()
                },
            )
            .unwrap();

        assert!(matches!(
            store.patch_insight(
                "tenant-b",
                &created.insight.id,
                InsightPatchRequest {
                    statement: Some("foreign mutation".to_string()),
                    ..InsightPatchRequest::default()
                },
            ),
            Err(ApiError::NotFound(message)) if message == "insight not found"
        ));

        let stored = store
            .search_insights(InsightSearchRequest {
                owner_user_id: Some("owner-a".to_string()),
                ..InsightSearchRequest::default()
            })
            .unwrap();
        assert_eq!(stored.hits.len(), 1);
        assert_eq!(stored.hits[0].statement, "original statement");
    }

    #[test]
    fn local_event_idempotency_rejects_a_different_request_without_a_journal_record() {
        let config = Config::test();
        let store = Store::new(&config);
        let request = AppendHistoryEventRequest {
            event_type: Some("status.changed".to_string()),
            entity_type: Some("task".to_string()),
            entity_id: Some("task-1".to_string()),
            owner_user_id: Some("owner-a".to_string()),
            occurred_at: Some(now()),
            observed_at: Some(now()),
            source_kind: Some("test".to_string()),
            source_ref: Some(SourceRef {
                kind: "test".to_string(),
                id: "source-1".to_string(),
                uri: None,
                meta: None,
            }),
            text: Some("original".to_string()),
            payload: json!({"state": "open"}),
            tags: vec!["status".to_string()],
            privacy: "private".to_string(),
            promote_policy: "none".to_string(),
            idempotency_key: Some("stable-key".to_string()),
            event_index_hint: None,
        };

        let first = store
            .append_event("tenant-a", Some("owner-a"), request.clone())
            .unwrap();
        assert!(!first.duplicate);

        let replay = store
            .append_event("tenant-a", Some("owner-a"), request.clone())
            .unwrap();
        assert!(replay.duplicate);
        assert_eq!(replay.event.id, first.event.id);

        let mut changed = request;
        changed.text = Some("different".to_string());
        assert!(matches!(
            store.append_event("tenant-a", Some("owner-a"), changed),
            Err(ApiError::Conflict(message))
                if message == "idempotency key was already used for a different request"
        ));
    }

    #[test]
    fn mutation_plan_without_request_context_records_system_actor() {
        let config = Config::test();
        let store = Store::new(&config);
        let plan = store
            .mutation_plan(MutationPlanInput {
                tenant_id: "tenant-a",
                operation_kind: "structured_summary.upsert",
                owner_user_id: Some("target-owner"),
                idempotency_key: Some("stable-key"),
                primary_kind: MutationPrimary::StructuredSummary,
                resources: vec![OperationResource::StructuredSummary {
                    summary: json!({"id": "summary-1"}),
                }],
                response_snapshot: Value::Null,
                request_fingerprint: None,
            })
            .unwrap();

        assert_eq!(plan.actor.scope, OperationActorScope::System);
        assert!(plan.actor.owner_user_id_hash.is_none());
        assert!(plan.actor.roles.is_empty());
        assert!(plan.actor.request_id.is_none());
    }

    #[tokio::test]
    async fn analysis_materialization_is_one_replayable_confirmed_operation() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let request = AnalysisMaterializationRequest {
            links: vec![AnalysisLinkMaterialization {
                source_uri: "ctx://user/source-a".to_string(),
                target_uri: "ctx://user/source-b".to_string(),
                source_title: Some("Source A".to_string()),
                target_title: Some("Source B".to_string()),
                relation: "supports".to_string(),
                rationale: Some("Grounded association".to_string()),
                confidence: 0.8,
                tags: vec!["grounded".to_string()],
            }],
            insights: vec![
                AnalysisInsightMaterialization {
                    insight_type: "analysis".to_string(),
                    title: "First insight".to_string(),
                    statement: "First grounded statement".to_string(),
                    confidence: 0.9,
                    salience: 0.7,
                    source_uris: vec!["ctx://user/source-a".to_string()],
                },
                AnalysisInsightMaterialization {
                    insight_type: "analysis".to_string(),
                    title: "Second insight".to_string(),
                    statement: "Second grounded statement".to_string(),
                    confidence: 0.75,
                    salience: 0.6,
                    source_uris: vec!["ctx://user/source-b".to_string()],
                },
            ],
        };

        let first = store
            .materialize_analysis_async(&tenant_id, "owner-a", request.clone())
            .await
            .unwrap();
        let replay = store
            .materialize_analysis_async(&tenant_id, "owner-a", request)
            .await
            .unwrap();

        assert_eq!(first.created_links.len(), 1);
        assert_eq!(first.insights.len(), 2);
        assert_eq!(replay.created_links[0].id, first.created_links[0].id);
        assert_eq!(replay.insights[0].id, first.insights[0].id);
        assert_eq!(replay.insights[1].id, first.insights[1].id);
        let persistence = first.persistence.as_ref().unwrap();
        assert_eq!(persistence.status, OperationStatus::Completed);
        assert_eq!(
            persistence.indexing_state,
            OperationIndexingState::Completed
        );
        assert_eq!(
            replay.persistence.as_ref().map(|value| &value.operation_id),
            Some(&persistence.operation_id)
        );
        assert!(first
            .created_links
            .iter()
            .all(|link| link.tenant_id == tenant_id
                && link.owner_user_id.as_deref() == Some("owner-a")));
        assert!(first.insights.iter().all(|insight| {
            insight.tenant_id == tenant_id && insight.owner_user_id == "owner-a"
        }));

        let data = store.read().unwrap();
        assert_eq!(data.operations.len(), 1);
        let operation = data.operations.values().next().unwrap();
        assert_eq!(operation.operation_kind, "analysis.materialize");
        assert_eq!(operation.status, OperationStatus::Completed);
        assert_eq!(operation.indexing_state, OperationIndexingState::Completed);
        assert_eq!(
            operation.plan.redacted_metadata["wait_for_index"],
            json!(true)
        );
        assert_eq!(
            operation.plan.redacted_metadata["target_owner_user_id_hash"],
            json!(store.resolver.user_hash("owner-a"))
        );
        assert!(matches!(
            operation.plan.primary.resource,
            OperationResource::Insight { .. }
        ));
        assert!(operation
            .plan
            .side_effects
            .iter()
            .any(|step| matches!(step.resource, OperationResource::Insight { .. })));
        assert!(operation
            .plan
            .side_effects
            .iter()
            .any(|step| matches!(step.resource, OperationResource::Links { .. })));
        assert!(operation
            .plan
            .side_effects
            .iter()
            .any(|step| matches!(step.resource, OperationResource::HistoryEvents { .. })));
        assert!(operation
            .plan
            .side_effects
            .iter()
            .any(|step| matches!(step.resource, OperationResource::ContextNodes { .. })));
    }

    #[tokio::test]
    async fn invalid_analysis_batch_is_discarded_before_journal_or_publication() {
        let config = Config::test();
        let store = Store::new(&config);
        let error = store
            .materialize_analysis_async(
                &config.tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: vec![AnalysisLinkMaterialization {
                        source_uri: "ctx://user/source-a".to_string(),
                        target_uri: "ctx://user/source-b".to_string(),
                        source_title: None,
                        target_title: None,
                        relation: "related".to_string(),
                        rationale: None,
                        confidence: 0.8,
                        tags: Vec::new(),
                    }],
                    insights: vec![AnalysisInsightMaterialization {
                        insight_type: "analysis".to_string(),
                        title: "  ".to_string(),
                        statement: "This candidate must fail".to_string(),
                        confidence: 0.8,
                        salience: 0.5,
                        source_uris: vec!["ctx://user/source-a".to_string()],
                    }],
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "insights[0].title is required");
        let data = store.read().unwrap();
        assert!(data.operations.is_empty());
        assert!(data.links.is_empty());
        assert!(data.insights.is_empty());
        assert!(data.event_by_id.is_empty());
        assert!(data.user_indexes.is_empty());
        assert!(data.personal_context.is_empty());
    }

    #[tokio::test]
    async fn direct_analysis_materialization_enforces_the_validated_candidate_contract() {
        let config = Config::test();
        let store = Store::new(&config);
        let invalid_relation = store
            .materialize_analysis_async(
                &config.tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: vec![AnalysisLinkMaterialization {
                        source_uri: "ctx://user/source-a".to_string(),
                        target_uri: "ctx://user/source-b".to_string(),
                        source_title: None,
                        target_title: None,
                        relation: "part_of".to_string(),
                        rationale: None,
                        confidence: 0.8,
                        tags: Vec::new(),
                    }],
                    insights: Vec::new(),
                },
            )
            .await
            .unwrap_err();
        assert!(invalid_relation
            .to_string()
            .contains("relation is not allowed"));

        let invalid_text = store
            .materialize_analysis_async(
                &config.tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: Vec::new(),
                    insights: vec![AnalysisInsightMaterialization {
                        insight_type: "analysis".to_string(),
                        title: "Unsafe insight".to_string(),
                        statement: "unsafe\nstatement".to_string(),
                        confidence: f32::NAN,
                        salience: 0.5,
                        source_uris: vec!["ctx://user/source-a".to_string()],
                    }],
                },
            )
            .await
            .unwrap_err();
        assert!(invalid_text
            .to_string()
            .contains("statement contains control characters"));

        let data = store.read().unwrap();
        assert!(data.operations.is_empty());
        assert!(data.links.is_empty());
        assert!(data.insights.is_empty());
    }

    #[tokio::test]
    async fn analysis_materialization_rejects_colliding_context_identities() {
        let config = Config::test();
        let store = Store::new(&config);
        let error = store
            .materialize_analysis_async(
                &config.tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: Vec::new(),
                    insights: vec![
                        AnalysisInsightMaterialization {
                            insight_type: "Analysis".to_string(),
                            title: "Same title".to_string(),
                            statement: "First statement".to_string(),
                            confidence: 0.8,
                            salience: 0.5,
                            source_uris: vec!["ctx://user/source-a".to_string()],
                        },
                        AnalysisInsightMaterialization {
                            insight_type: "analysis".to_string(),
                            title: "same-title".to_string(),
                            statement: "Second statement".to_string(),
                            confidence: 0.7,
                            salience: 0.4,
                            source_uris: vec!["ctx://user/source-b".to_string()],
                        },
                    ],
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "analysis materialization contains a duplicate insight context identity"
        );
        let data = store.read().unwrap();
        assert!(data.operations.is_empty());
        assert!(data.insights.is_empty());
        assert!(data.personal_context.is_empty());
    }

    #[tokio::test]
    async fn analysis_materialization_rejects_lossy_uri_collision_after_identity_reset() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let original = store
            .upsert_insight(
                &tenant_id,
                InsightUpsertRequest {
                    owner_user_id: Some("owner-a".to_string()),
                    insight_type: Some("analysis".to_string()),
                    title: Some("Tax/Audit".to_string()),
                    statement: Some("Manual insight must remain unchanged".to_string()),
                    confidence: 0.4,
                    salience: 0.3,
                    privacy: "private".to_string(),
                    merge_policy: "merge".to_string(),
                    idempotency_key: Some("manual-tax-audit".to_string()),
                    ..InsightUpsertRequest::default()
                },
            )
            .unwrap()
            .insight;
        store.write().unwrap().insight_idempotency.clear();

        let error = store
            .materialize_analysis_async(
                &tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: Vec::new(),
                    insights: vec![AnalysisInsightMaterialization {
                        insight_type: "analysis".to_string(),
                        title: "Tax Audit".to_string(),
                        statement: "Provider replacement must be rejected".to_string(),
                        confidence: 0.9,
                        salience: 0.8,
                        source_uris: vec!["ctx://user/source-a".to_string()],
                    }],
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "analysis insight context identity collides with an existing insight"
        );
        let data = store.read().unwrap();
        let persisted = data.insights.get(&original.id).unwrap();
        assert_eq!(persisted.title, "Tax/Audit");
        assert_eq!(persisted.statement, "Manual insight must remain unchanged");
        assert_eq!(data.insights.len(), 1);
        assert!(data.operations.is_empty());
    }

    #[tokio::test]
    async fn analysis_materialization_rejects_duplicate_normalized_link_identities() {
        let config = Config::test();
        let store = Store::new(&config);
        let error = store
            .materialize_analysis_async(
                &config.tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: vec![
                        AnalysisLinkMaterialization {
                            source_uri: "ctx://user/source-a/.overview".to_string(),
                            target_uri: "ctx://user/source-b/detail".to_string(),
                            source_title: None,
                            target_title: None,
                            relation: "supports".to_string(),
                            rationale: None,
                            confidence: 0.8,
                            tags: Vec::new(),
                        },
                        AnalysisLinkMaterialization {
                            source_uri: "ctx://user/source-a".to_string(),
                            target_uri: "ctx://user/source-b".to_string(),
                            source_title: None,
                            target_title: None,
                            relation: " supports ".to_string(),
                            rationale: None,
                            confidence: 0.7,
                            tags: Vec::new(),
                        },
                    ],
                    insights: Vec::new(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "analysis materialization contains a duplicate link natural identity"
        );
        let data = store.read().unwrap();
        assert!(data.links.is_empty());
        assert!(data.operations.is_empty());
    }

    #[tokio::test]
    async fn analysis_materialization_rejects_ambiguous_persisted_link_identity() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let original = store
            .upsert_link(
                &tenant_id,
                LinkUpsertRequest {
                    owner_user_id: Some("owner-a".to_string()),
                    source_uri: Some("ctx://user/source-a".to_string()),
                    target_uri: Some("ctx://user/source-b".to_string()),
                    relation: "supports".to_string(),
                    rationale: Some("First persisted link".to_string()),
                    created_by: "manual".to_string(),
                    ..LinkUpsertRequest::default()
                },
            )
            .unwrap()
            .link;
        let mut duplicate = original.clone();
        duplicate.id = "hydrated-duplicate-link".to_string();
        store
            .write()
            .unwrap()
            .links
            .insert(duplicate.id.clone(), duplicate);

        let error = store
            .materialize_analysis_async(
                &tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: vec![AnalysisLinkMaterialization {
                        source_uri: original.source_uri.clone(),
                        target_uri: original.target_uri.clone(),
                        source_title: None,
                        target_title: None,
                        relation: original.relation.clone(),
                        rationale: Some("Provider must not pick a winner".to_string()),
                        confidence: 0.9,
                        tags: Vec::new(),
                    }],
                    insights: Vec::new(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "analysis link natural identity is ambiguous"
        );
        let data = store.read().unwrap();
        assert_eq!(data.links.len(), 2);
        assert!(data.operations.is_empty());
    }

    #[tokio::test]
    async fn analysis_materialization_reuses_manual_records_without_overwriting_them() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let original_link = store
            .upsert_link(
                &tenant_id,
                LinkUpsertRequest {
                    owner_user_id: Some("owner-a".to_string()),
                    source_uri: Some("ctx://user/source-a".to_string()),
                    target_uri: Some("ctx://user/source-b".to_string()),
                    source_title: Some("Manual source".to_string()),
                    target_title: Some("Manual target".to_string()),
                    relation: "supports".to_string(),
                    rationale: Some("Manual rationale must remain unchanged".to_string()),
                    confidence: 0.4,
                    created_by: "manual".to_string(),
                    tags: vec!["manual".to_string()],
                    idempotency_key: Some("manual-link".to_string()),
                    ..LinkUpsertRequest::default()
                },
            )
            .unwrap()
            .link;
        let original_insight = store
            .upsert_insight(
                &tenant_id,
                InsightUpsertRequest {
                    owner_user_id: Some("owner-a".to_string()),
                    insight_type: Some("analysis".to_string()),
                    title: Some("Manual stable insight".to_string()),
                    statement: Some("Manual statement must remain unchanged".to_string()),
                    confidence: 0.45,
                    salience: 0.35,
                    privacy: "private".to_string(),
                    merge_policy: "merge".to_string(),
                    idempotency_key: Some("manual-insight".to_string()),
                    ..InsightUpsertRequest::default()
                },
            )
            .unwrap()
            .insight;
        {
            let mut data = store.write().unwrap();
            data.link_idempotency.clear();
            data.insight_idempotency.clear();
        }
        let event_count_before = store.read().unwrap().event_by_id.len();

        let materialized = store
            .materialize_analysis_async(
                &tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: vec![AnalysisLinkMaterialization {
                        source_uri: original_link.source_uri.clone(),
                        target_uri: original_link.target_uri.clone(),
                        source_title: Some("Provider source".to_string()),
                        target_title: Some("Provider target".to_string()),
                        relation: original_link.relation.clone(),
                        rationale: Some("Provider replacement must be ignored".to_string()),
                        confidence: 0.99,
                        tags: vec!["provider".to_string()],
                    }],
                    insights: vec![AnalysisInsightMaterialization {
                        insight_type: original_insight.insight_type.clone(),
                        title: original_insight.title.clone(),
                        statement: "Provider replacement must be ignored".to_string(),
                        confidence: 0.99,
                        salience: 0.99,
                        source_uris: vec![original_link.source_uri.clone()],
                    }],
                },
            )
            .await
            .unwrap();

        assert_eq!(materialized.created_links[0].id, original_link.id);
        assert_eq!(materialized.insights[0].id, original_insight.id);
        assert!(materialized.persistence.is_none());
        let data = store.read().unwrap();
        assert_eq!(
            serde_json::to_value(data.links.get(&original_link.id).unwrap()).unwrap(),
            serde_json::to_value(&original_link).unwrap()
        );
        assert_eq!(
            serde_json::to_value(data.insights.get(&original_insight.id).unwrap()).unwrap(),
            serde_json::to_value(&original_insight).unwrap()
        );
        assert_eq!(data.event_by_id.len(), event_count_before);
        assert!(data.operations.is_empty());
    }

    #[tokio::test]
    async fn analysis_materialization_rejects_inactive_manual_link_reuse() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let original = store
            .upsert_link(
                &tenant_id,
                LinkUpsertRequest {
                    owner_user_id: Some("owner-a".to_string()),
                    source_uri: Some("ctx://user/source-a".to_string()),
                    target_uri: Some("ctx://user/source-b".to_string()),
                    relation: "supports".to_string(),
                    rationale: Some("Inactive manual link".to_string()),
                    created_by: "manual".to_string(),
                    ..LinkUpsertRequest::default()
                },
            )
            .unwrap()
            .link;
        store
            .write()
            .unwrap()
            .links
            .get_mut(&original.id)
            .unwrap()
            .status = "inactive".to_string();
        let persisted_before = store
            .read()
            .unwrap()
            .links
            .get(&original.id)
            .cloned()
            .unwrap();
        let event_count_before = store.read().unwrap().event_by_id.len();

        let error = store
            .materialize_analysis_async(
                &tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: vec![AnalysisLinkMaterialization {
                        source_uri: original.source_uri,
                        target_uri: original.target_uri,
                        source_title: Some("Provider source".to_string()),
                        target_title: Some("Provider target".to_string()),
                        relation: original.relation,
                        rationale: Some("Provider must not reactivate the link".to_string()),
                        confidence: 0.99,
                        tags: vec!["provider".to_string()],
                    }],
                    insights: Vec::new(),
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "analysis link natural identity is not active"
        );
        let data = store.read().unwrap();
        assert_eq!(
            serde_json::to_value(data.links.get(&persisted_before.id).unwrap()).unwrap(),
            serde_json::to_value(&persisted_before).unwrap()
        );
        assert_eq!(data.event_by_id.len(), event_count_before);
        assert!(data.operations.is_empty());
    }

    #[tokio::test]
    async fn analysis_materialization_rejects_inactive_manual_insight_reuse() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let original = store
            .upsert_insight(
                &tenant_id,
                InsightUpsertRequest {
                    owner_user_id: Some("owner-a".to_string()),
                    insight_type: Some("analysis".to_string()),
                    title: Some("Inactive manual insight".to_string()),
                    statement: Some("Manual statement".to_string()),
                    privacy: "private".to_string(),
                    merge_policy: "merge".to_string(),
                    ..InsightUpsertRequest::default()
                },
            )
            .unwrap()
            .insight;
        store
            .write()
            .unwrap()
            .insights
            .get_mut(&original.id)
            .unwrap()
            .status = "inactive".to_string();
        let persisted_before = store
            .read()
            .unwrap()
            .insights
            .get(&original.id)
            .cloned()
            .unwrap();
        let event_count_before = store.read().unwrap().event_by_id.len();

        let error = store
            .materialize_analysis_async(
                &tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: Vec::new(),
                    insights: vec![AnalysisInsightMaterialization {
                        insight_type: original.insight_type,
                        title: original.title,
                        statement: "Provider must not reactivate the insight".to_string(),
                        confidence: 0.99,
                        salience: 0.99,
                        source_uris: vec!["ctx://user/source-a".to_string()],
                    }],
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error.to_string(),
            "analysis insight context identity is not active"
        );
        let data = store.read().unwrap();
        assert_eq!(
            serde_json::to_value(data.insights.get(&persisted_before.id).unwrap()).unwrap(),
            serde_json::to_value(&persisted_before).unwrap()
        );
        assert_eq!(data.event_by_id.len(), event_count_before);
        assert!(data.operations.is_empty());
    }

    #[tokio::test]
    async fn hydrated_analysis_overlap_reuses_existing_insight_identity() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let first_candidate = AnalysisInsightMaterialization {
            insight_type: "analysis".to_string(),
            title: "Stable insight".to_string(),
            statement: "Stable grounded statement".to_string(),
            confidence: 0.9,
            salience: 0.7,
            source_uris: vec!["ctx://user/source-a".to_string()],
        };
        let first = store
            .materialize_analysis_async(
                &tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: Vec::new(),
                    insights: vec![
                        first_candidate.clone(),
                        AnalysisInsightMaterialization {
                            insight_type: "analysis".to_string(),
                            title: "Old companion".to_string(),
                            statement: "Old companion statement".to_string(),
                            confidence: 0.7,
                            salience: 0.4,
                            source_uris: vec!["ctx://user/source-b".to_string()],
                        },
                    ],
                },
            )
            .await
            .unwrap();
        let stable_id = first.insights[0].id.clone();
        let stable_created_at = first.insights[0].created_at;

        // Hydration intentionally does not restore ephemeral idempotency maps.
        store.write().unwrap().insight_idempotency.clear();

        let overlap = store
            .materialize_analysis_async(
                &tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: Vec::new(),
                    insights: vec![
                        first_candidate,
                        AnalysisInsightMaterialization {
                            insight_type: "analysis".to_string(),
                            title: "New companion".to_string(),
                            statement: "New companion statement".to_string(),
                            confidence: 0.8,
                            salience: 0.6,
                            source_uris: vec!["ctx://user/source-c".to_string()],
                        },
                    ],
                },
            )
            .await
            .unwrap();

        assert_eq!(overlap.insights[0].id, stable_id);
        assert_eq!(overlap.insights[0].created_at, stable_created_at);
        let data = store.read().unwrap();
        assert_eq!(data.insights.len(), 3);
        assert_eq!(data.operations.len(), 2);
        assert_eq!(
            data.insights
                .values()
                .filter(|insight| insight.context_uri.ends_with("/stable-insight"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn analysis_operation_replay_rejects_mixed_target_owners() {
        let config = Config::test();
        let store = Store::new(&config);
        store
            .materialize_analysis_async(
                &config.tenant_id,
                "owner-a",
                AnalysisMaterializationRequest {
                    links: Vec::new(),
                    insights: vec![AnalysisInsightMaterialization {
                        insight_type: "analysis".to_string(),
                        title: "Owner-bound insight".to_string(),
                        statement: "Grounded statement".to_string(),
                        confidence: 0.8,
                        salience: 0.5,
                        source_uris: vec!["ctx://user/source-a".to_string()],
                    }],
                },
            )
            .await
            .unwrap();
        let mut operation = store
            .read()
            .unwrap()
            .operations
            .values()
            .next()
            .unwrap()
            .clone();
        let insight = std::iter::once(&mut operation.plan.primary)
            .chain(operation.plan.side_effects.iter_mut())
            .find_map(|step| match &mut step.resource {
                OperationResource::Insight { insight } => Some(insight),
                _ => None,
            })
            .unwrap();
        insight.owner_user_id = "owner-b".to_string();

        let error = store.validate_operation_routing(&operation).unwrap_err();

        assert!(error
            .to_string()
            .contains("analysis materialization mixes target owners"));
    }

    #[test]
    fn analysis_candidate_idempotency_material_is_bounded_hmac_only() {
        let config = Config::test();
        let store = Store::new(&config);
        let candidate = AnalysisInsightMaterialization {
            insight_type: "analysis".to_string(),
            title: "raw-query-marker".to_string(),
            statement: "Grounded statement".to_string(),
            confidence: 0.8,
            salience: 0.5,
            source_uris: vec!["ctx://user/source-a".to_string()],
        };
        let key = store
            .mutation_request_fingerprint(
                &config.tenant_id,
                "analysis.materialize.insight",
                &("owner-a", candidate),
            )
            .unwrap();

        assert_eq!(key.len(), 32);
        assert!(key.chars().all(|ch| ch.is_ascii_hexdigit()));
        assert!(!key.contains("raw-query-marker"));
    }

    #[test]
    fn accepted_resource_projection_preserves_live_state_and_hides_other_staged_resources() {
        let created_at = now();
        let concurrent_session = SessionRecord {
            id: "session-concurrent".to_string(),
            tenant_id: "tenant-a".to_string(),
            owner_user_id: "owner-a".to_string(),
            title: "concurrent read-through".to_string(),
            status: "active".to_string(),
            messages: Vec::new(),
            created_at,
        };
        let unaccepted_session = SessionRecord {
            id: "session-unaccepted".to_string(),
            title: "unaccepted side effect".to_string(),
            ..concurrent_session.clone()
        };
        let trace = TraceRecord {
            id: "trace-accepted".to_string(),
            tenant_id: "tenant-a".to_string(),
            owner_user_id: Some("owner-a".to_string()),
            query: "accepted primary".to_string(),
            mode: "hybrid".to_string(),
            stages: Vec::new(),
            context_uris: Vec::new(),
            created_at,
        };

        let mut current = StoreData::default();
        current
            .sessions
            .insert(concurrent_session.id.clone(), concurrent_session.clone());
        let mut staged = StoreData::default();
        staged
            .sessions
            .insert(unaccepted_session.id.clone(), unaccepted_session.clone());
        staged.traces.insert(trace.id.clone(), trace.clone());

        Store::publish_resource_cache(
            &mut current,
            &staged,
            "tenant-a",
            &OperationResource::Trace {
                trace: trace.clone(),
            },
        );

        assert_eq!(
            current
                .sessions
                .get(&concurrent_session.id)
                .map(|session| session.title.as_str()),
            Some("concurrent read-through")
        );
        assert!(!current.sessions.contains_key(&unaccepted_session.id));
        assert_eq!(
            current
                .traces
                .get(&trace.id)
                .map(|trace| trace.query.as_str()),
            Some("accepted primary")
        );
    }

    #[test]
    fn context_cache_merge_never_replaces_a_newer_local_node() {
        let config = Config::test();
        let store = Store::new(&config);
        let mut newer = store.context_node(
            "ctx://company/fragments/stable",
            "newer",
            2,
            "newer body",
            "company",
            "rag_company_context",
            "tenant-a",
            None,
            None,
            None,
        );
        newer.updated_at = now();
        let mut stale = newer.clone();
        stale.title = "stale".to_string();
        stale.body = "stale body".to_string();
        stale.updated_at -= chrono::Duration::seconds(1);

        let mut nodes = vec![newer];
        upsert_context_nodes(&mut nodes, vec![stale]);

        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes[0].title, "newer");
        assert_eq!(nodes[0].body, "newer body");
    }

    #[test]
    fn repository_context_validation_rejects_cross_scope_rows() {
        let config = Config::test();
        let store = Store::new(&config);
        let routing = store
            .resolver
            .resolve("tenant-a", "owner-a", false, true)
            .unwrap();
        let personal = store.context_node(
            "ctx://private/note",
            "note",
            2,
            "body",
            "personal",
            &routing.personal_context_index_uid,
            "tenant-a",
            Some("owner-a".to_string()),
            None,
            None,
        );
        store
            .validate_repository_context_node(&personal, "tenant-a", Some("owner-a"))
            .expect("an exact personal row should pass");

        let mut wrong_owner = personal.clone();
        wrong_owner.owner_user_id = Some("owner-b".to_string());
        assert!(store
            .validate_repository_context_node(&wrong_owner, "tenant-a", Some("owner-a"))
            .is_err());

        let company = store.context_node(
            "ctx://company/note",
            "note",
            2,
            "body",
            "company",
            "rag_company_context",
            "tenant-a",
            None,
            None,
            None,
        );
        store
            .validate_repository_context_node(&company, "tenant-a", None)
            .expect("an exact company row should pass");

        let mut forged_company = company;
        forged_company.owner_user_id = Some("owner-a".to_string());
        assert!(store
            .validate_repository_context_node(&forged_company, "tenant-a", None)
            .is_err());

        let mut repository_search_rows = vec![personal, forged_company];
        repository_search_rows.retain(|node| {
            store
                .validate_repository_context_node(node, "tenant-a", Some("owner-a"))
                .is_ok()
        });
        assert_eq!(repository_search_rows.len(), 1);
        assert_eq!(
            repository_search_rows[0].owner_user_id.as_deref(),
            Some("owner-a")
        );
    }

    fn ingest_task_fixture(
        task_id: &str,
        tenant_id: &str,
        state: &str,
        age_seconds: i64,
    ) -> IngestTask {
        let stamp = chrono::Utc::now() - chrono::Duration::seconds(age_seconds);
        IngestTask {
            task_id: task_id.to_string(),
            tenant_id: tenant_id.to_string(),
            owner_user_id: None,
            source_id: format!("src-{task_id}"),
            revision_id: format!("rev-{task_id}"),
            source_document_uri: None,
            parser_provider: "builtin".to_string(),
            parser_backend: "text".to_string(),
            state: state.to_string(),
            error: None,
            created_at: stamp,
            updated_at: stamp,
            completed_at: matches!(state, "completed" | "failed").then_some(stamp),
            status_url: None,
            result_url: None,
            queued_ahead: None,
        }
    }

    fn source_document_fixture(
        tenant_id: &str,
        owner_user_id: Option<&str>,
        uri: &str,
        marker: &str,
    ) -> SourceDocument {
        let stamp = now();
        SourceDocument {
            id: format!("source-document-{marker}"),
            tenant_id: tenant_id.to_string(),
            owner_user_id: owner_user_id.map(ToString::to_string),
            source_kind: "test".to_string(),
            source_id: format!("source-{marker}"),
            revision_id: "v1".to_string(),
            uri: uri.to_string(),
            title: format!("{marker} title"),
            content: format!("{marker} content"),
            checksum: format!("{marker}-checksum"),
            status: "active".to_string(),
            retrieval_enabled: false,
            created_at: stamp,
            updated_at: stamp,
        }
    }

    #[test]
    fn same_uri_source_documents_remain_owner_scoped_in_cache() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let uri = "ctx://user/shared/source/v1";

        store
            .cache_source_document(source_document_fixture(
                tenant_id,
                Some("owner-a"),
                uri,
                "owner-a",
            ))
            .unwrap();
        store
            .cache_source_document(source_document_fixture(
                tenant_id,
                Some("owner-b"),
                uri,
                "owner-b",
            ))
            .unwrap();

        let owner_a = store
            .fs_read(tenant_id, uri, Some("owner-a"), false)
            .unwrap();
        let owner_b = store
            .fs_read(tenant_id, uri, Some("owner-b"), false)
            .unwrap();
        assert_eq!(owner_a.body, "owner-a content");
        assert_eq!(owner_b.body, "owner-b content");
        assert!(matches!(
            store.fs_read(tenant_id, uri, None, true),
            Err(ApiError::BadRequest(_))
        ));

        store
            .cache_source_document(source_document_fixture(tenant_id, None, uri, "company"))
            .unwrap();
        let company = store.fs_read(tenant_id, uri, None, true).unwrap();
        let owner_without_private = store
            .fs_read(tenant_id, uri, Some("owner-c"), false)
            .unwrap();
        assert_eq!(company.body, "company content");
        assert_eq!(owner_without_private.body, "company content");
        assert_eq!(store.read().unwrap().source_documents.len(), 3);
    }

    #[test]
    fn context_reads_are_owner_exact_and_admin_rejects_private_uri_ambiguity() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let uri = "ctx://user/shared/fragments/0001";
        let owner_a = store
            .ensure_user_index(tenant_id, "owner-a", EnsureUserEventIndexRequest::default())
            .unwrap()
            .routing;
        let owner_b = store
            .ensure_user_index(tenant_id, "owner-b", EnsureUserEventIndexRequest::default())
            .unwrap()
            .routing;

        let node_a = store.context_node(
            uri,
            "Owner A",
            2,
            "owner-a-body",
            "personal",
            &owner_a.personal_context_index_uid,
            tenant_id,
            Some("owner-a".to_string()),
            None,
            None,
        );
        let node_b = store.context_node(
            uri,
            "Owner B",
            2,
            "owner-b-body",
            "personal",
            &owner_b.personal_context_index_uid,
            tenant_id,
            Some("owner-b".to_string()),
            None,
            None,
        );
        let corrupt_owner_b_in_a = store.context_node(
            "ctx://user/corrupt/fragments/0001",
            "Misrouted owner B",
            2,
            "must-not-leak",
            "personal",
            &owner_a.personal_context_index_uid,
            tenant_id,
            Some("owner-b".to_string()),
            None,
            None,
        );
        {
            let mut data = store.write().unwrap();
            data.personal_context
                .entry(owner_a.personal_context_index_uid.clone())
                .or_default()
                .extend([node_a, corrupt_owner_b_in_a]);
            data.personal_context
                .entry(owner_b.personal_context_index_uid.clone())
                .or_default()
                .push(node_b);
        }

        assert_eq!(
            store
                .fs_read(tenant_id, uri, Some("owner-a"), false)
                .unwrap()
                .body,
            "owner-a-body"
        );
        assert_eq!(
            store
                .fs_read(tenant_id, uri, Some("owner-b"), false)
                .unwrap()
                .body,
            "owner-b-body"
        );
        assert!(matches!(
            store.fs_read(tenant_id, uri, None, true),
            Err(ApiError::BadRequest(_))
        ));
        assert!(matches!(
            store.fs_layer(tenant_id, uri, 2, None, true),
            Err(ApiError::BadRequest(_))
        ));
        assert!(matches!(
            store.traceback(
                tenant_id,
                ContextTracebackRequest {
                    uri: Some(uri.to_string()),
                    owner_user_id: None,
                },
                true,
            ),
            Err(ApiError::BadRequest(_))
        ));
        assert!(matches!(
            store.fs_read(
                tenant_id,
                "ctx://user/corrupt/fragments/0001",
                Some("owner-a"),
                false,
            ),
            Err(ApiError::NotFound(_))
        ));
    }

    #[test]
    fn document_vector_candidates_resolve_source_by_node_scope() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let uri = "ctx://user/shared/source/v1";
        store
            .cache_source_document(source_document_fixture(
                tenant_id,
                Some("owner-a"),
                uri,
                "owner-a-vector-marker",
            ))
            .unwrap();
        store
            .cache_source_document(source_document_fixture(
                tenant_id,
                Some("owner-b"),
                uri,
                "owner-b-vector-marker",
            ))
            .unwrap();

        let mut owner_a_node = store.context_node(
            "ctx://user/shared/source/v1/fragments/0000",
            "Owner A fragment",
            2,
            "owner-a fragment",
            "personal",
            "owner-a-context-index",
            tenant_id,
            Some("owner-a".to_string()),
            Some("source-owner-a".to_string()),
            Some("v1".to_string()),
        );
        owner_a_node.source_document_uri = Some(uri.to_string());

        let data = store.read().unwrap();
        let candidates = doc_candidates_locked(&data, &[owner_a_node]);
        assert_eq!(candidates.len(), 1);
        assert!(candidates[0].1.contains("owner-a-vector-marker"));
        assert!(!candidates[0].1.contains("owner-b-vector-marker"));
    }

    #[test]
    fn parsed_block_usage_is_scoped_by_tenant_owner_and_uri() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let uri = "ctx://user/shared/source/v1";
        {
            let mut data = store.write().unwrap();
            data.parsed_blocks.insert(
                SourceDocumentKey::new(tenant_id, Some("owner-a"), uri),
                vec![
                    ParsedBlock {
                        block_id: "owner-a-1".to_string(),
                        ..ParsedBlock::default()
                    },
                    ParsedBlock {
                        block_id: "owner-a-2".to_string(),
                        ..ParsedBlock::default()
                    },
                ],
            );
            data.parsed_blocks.insert(
                SourceDocumentKey::new(tenant_id, Some("owner-b"), uri),
                vec![ParsedBlock {
                    block_id: "owner-b-1".to_string(),
                    ..ParsedBlock::default()
                }],
            );
            data.parsed_blocks.insert(
                SourceDocumentKey::new("other-tenant", Some("owner-a"), uri),
                vec![ParsedBlock {
                    block_id: "other-tenant-1".to_string(),
                    ..ParsedBlock::default()
                }],
            );
        }

        let owner_a = store
            .usage_snapshot(tenant_id, Some("owner-a"), false)
            .unwrap();
        let owner_b = store
            .usage_snapshot(tenant_id, Some("owner-b"), false)
            .unwrap();
        let tenant = store.usage_snapshot(tenant_id, None, true).unwrap();
        assert_eq!(owner_a["providers"]["ingest"]["parsed_block_count"], 2);
        assert_eq!(owner_b["providers"]["ingest"]["parsed_block_count"], 1);
        assert_eq!(tenant["providers"]["ingest"]["parsed_block_count"], 3);
    }

    #[test]
    fn default_tenant_rows_are_never_visible_to_another_tenant() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = "tenant-b";
        let source_document_uri = "ctx://default/source-document";
        let mut default_neighbor = store.context_node(
            "ctx://default/fragments/0001",
            "Default neighbor",
            2,
            "default-only",
            "company",
            "rag_company_context",
            "default",
            None,
            Some("source-default".to_string()),
            Some("revision-default".to_string()),
        );
        default_neighbor.source_document_uri = Some(source_document_uri.to_string());
        default_neighbor.fragment_index = Some(1);
        let mut tenant_hit = store.context_node(
            "ctx://tenant-b/fragments/0001",
            "Tenant hit",
            2,
            "tenant-b-hit",
            "company",
            "rag_company_context",
            tenant_id,
            None,
            Some("source-b".to_string()),
            Some("revision-b".to_string()),
        );
        tenant_hit.source_document_uri = Some(source_document_uri.to_string());
        tenant_hit.fragment_index = Some(1);
        let mut tenant_neighbor = store.context_node(
            "ctx://tenant-b/fragments/0002",
            "Tenant neighbor",
            2,
            "tenant-b-neighbor",
            "company",
            "rag_company_context",
            tenant_id,
            None,
            Some("source-b".to_string()),
            Some("revision-b".to_string()),
        );
        tenant_neighbor.source_document_uri = Some(source_document_uri.to_string());
        tenant_neighbor.fragment_index = Some(2);

        let stamp = now();
        {
            let mut data = store.write().unwrap();
            data.company_context.extend([
                default_neighbor.clone(),
                tenant_hit.clone(),
                tenant_neighbor.clone(),
            ]);
            data.source_documents.insert(
                SourceDocumentKey::new("default", None, source_document_uri),
                SourceDocument {
                    id: "default-source-document".to_string(),
                    tenant_id: "default".to_string(),
                    owner_user_id: None,
                    source_kind: "test".to_string(),
                    source_id: "source-default".to_string(),
                    revision_id: "revision-default".to_string(),
                    uri: source_document_uri.to_string(),
                    title: "Default source".to_string(),
                    content: "default-only".to_string(),
                    checksum: "default-checksum".to_string(),
                    status: "active".to_string(),
                    retrieval_enabled: false,
                    created_at: stamp,
                    updated_at: stamp,
                },
            );
            for scope in ["default", tenant_id] {
                let dataset_key = format!("dataset-{scope}");
                data.datasets.insert(
                    dataset_key.clone(),
                    DatasetRecord {
                        id: format!("dataset-id-{scope}"),
                        tenant_id: scope.to_string(),
                        dataset_key,
                        title: format!("Dataset {scope}"),
                        schema_version: 1,
                        status: "active".to_string(),
                        columns: Vec::new(),
                    },
                );
                let snapshot_id = format!("snapshot-{scope}");
                data.snapshots.insert(
                    snapshot_id.clone(),
                    StructuredSnapshot {
                        id: snapshot_id.clone(),
                        tenant_id: scope.to_string(),
                        dataset_key: format!("dataset-{scope}"),
                        owner_user_id: "owner-a".to_string(),
                        period_key: "period-a".to_string(),
                        period_start: stamp,
                        period_end: stamp,
                        row_count: 1,
                        status: "open".to_string(),
                    },
                );
                data.rows_by_snapshot
                    .insert(snapshot_id, vec![json!({"value": scope})]);
                data.structured_summaries.insert(
                    format!("summary-{scope}"),
                    json!({
                        "id": format!("summary-{scope}"),
                        "tenant_id": scope,
                        "owner_user_id": "owner-a"
                    }),
                );
                data.sessions.insert(
                    format!("session-{scope}"),
                    SessionRecord {
                        id: format!("session-{scope}"),
                        tenant_id: scope.to_string(),
                        owner_user_id: "owner-a".to_string(),
                        title: format!("Session {scope}"),
                        status: "active".to_string(),
                        messages: vec![json!({"scope": scope})],
                        created_at: stamp,
                    },
                );
            }
        }

        let usage = store.usage_snapshot(tenant_id, None, true).unwrap();
        assert_eq!(
            usage["providers"]["contextfs"]["company_context_node_count"],
            2
        );
        assert_eq!(usage["providers"]["structured_data"]["dataset_count"], 1);
        assert_eq!(usage["providers"]["structured_data"]["snapshot_count"], 1);
        assert_eq!(usage["providers"]["structured_data"]["row_count"], 1);
        assert_eq!(usage["providers"]["structured_data"]["summary_count"], 1);
        assert_eq!(usage["providers"]["sessions"]["session_count"], 1);
        assert_eq!(usage["providers"]["sessions"]["message_count"], 1);

        let data = store.read().unwrap();
        let scoped = store
            .context_scope_for_acl_locked(&data, tenant_id, None, true)
            .unwrap();
        assert_eq!(scoped.len(), 2);
        assert!(scoped.iter().all(|node| node.tenant_id == tenant_id));
        assert!(store
            .source_document_for_acl_locked(&data, tenant_id, source_document_uri, None, true)
            .unwrap()
            .is_none());

        let hit = context_hit_from_node(&tenant_hit, 1.0, &[]);
        assert!(source_document_for_hit_locked(&data, tenant_id, None, &hit).is_none());
        let neighbors = neighbor_fragments_for_hit_locked(
            &data,
            tenant_id,
            None,
            std::slice::from_ref(&tenant_hit),
            &hit,
        );
        assert_eq!(neighbors.len(), 1);
        assert_eq!(neighbors[0].uri, tenant_neighbor.uri);
    }

    #[tokio::test]
    async fn blank_harness_change_id_is_rejected_without_mutation() {
        let config = Config::test();
        let store = Store::new(&config);

        let error = store
            .create_harness_change_async(
                &config.tenant_id,
                CreateHarnessChangeManifestRequest {
                    id: Some(" \t".to_string()),
                    change_type: Some("new".to_string()),
                    component_id: Some("retrieval.context_search".to_string()),
                    failure_pattern: Some("failure".to_string()),
                    root_cause: Some("cause".to_string()),
                    targeted_fix: Some("fix".to_string()),
                    why_this_component: Some("reason".to_string()),
                    ..CreateHarnessChangeManifestRequest::default()
                },
            )
            .await
            .unwrap_err();

        assert_eq!(error.to_string(), "id is required");
        assert!(store.read().unwrap().harness_changes.is_empty());
    }

    #[test]
    fn blank_eval_case_id_is_rejected_without_mutation() {
        let config = Config::test();
        let store = Store::new(&config);

        let error = store
            .create_eval_case(
                &config.tenant_id,
                CreateRagEvalCaseRequest {
                    id: Some("\n".to_string()),
                    question: Some("What changed?".to_string()),
                    ..CreateRagEvalCaseRequest::default()
                },
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "id is required");
        assert!(store.read().unwrap().eval_cases.is_empty());
    }

    #[test]
    fn blank_company_source_id_is_rejected_without_mutation() {
        let config = Config::test();
        let store = Store::new(&config);

        let error = store
            .create_revision(&config.tenant_id, "   ", CreateRevisionRequest::default())
            .unwrap_err();

        assert_eq!(error.to_string(), "source_id is required");
        let data = store.read().unwrap();
        assert!(data.sources.is_empty());
        assert!(data.source_revisions.is_empty());
        assert!(data.event_by_id.is_empty());
    }

    #[test]
    fn blank_structured_row_id_rejects_batch_without_partial_mutation() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let snapshot_id = "snapshot-blank-row-id";
        let stamp = now();
        store.write().unwrap().snapshots.insert(
            snapshot_id.to_string(),
            StructuredSnapshot {
                id: snapshot_id.to_string(),
                tenant_id: tenant_id.clone(),
                dataset_key: "dataset-a".to_string(),
                owner_user_id: "owner-a".to_string(),
                period_key: "period-a".to_string(),
                period_start: stamp,
                period_end: stamp,
                row_count: 0,
                status: "open".to_string(),
            },
        );

        let error = store
            .bulk_rows(
                &tenant_id,
                snapshot_id,
                BulkStructuredRowsRequest {
                    rows: vec![json!({ "id": "row-valid" }), json!({ "id": " \t" })],
                    ..BulkStructuredRowsRequest::default()
                },
            )
            .unwrap_err();

        assert_eq!(error.to_string(), "id is required");
        let data = store.read().unwrap();
        assert!(!data.rows_by_snapshot.contains_key(snapshot_id));
        assert!(data.row_idempotency.is_empty());
        assert_eq!(data.snapshots[snapshot_id].row_count, 0);
        assert!(data.event_by_id.is_empty());
    }

    #[test]
    fn structured_rows_overwrite_scope_fields_and_allow_ids_per_snapshot() {
        let config = Config::test();
        let tenant_id = config.tenant_id.clone();
        let store = Store::new(&config);
        let stamp = now();
        {
            let mut data = store.write().unwrap();
            for snapshot_id in ["snapshot-a", "snapshot-b"] {
                data.snapshots.insert(
                    snapshot_id.to_string(),
                    StructuredSnapshot {
                        id: snapshot_id.to_string(),
                        tenant_id: tenant_id.clone(),
                        dataset_key: "dataset-a".to_string(),
                        owner_user_id: "owner-a".to_string(),
                        period_key: snapshot_id.to_string(),
                        period_start: stamp,
                        period_end: stamp,
                        row_count: 0,
                        status: "open".to_string(),
                    },
                );
            }
        }

        for snapshot_id in ["snapshot-a", "snapshot-b"] {
            let response = store
                .bulk_rows(
                    &tenant_id,
                    snapshot_id,
                    BulkStructuredRowsRequest {
                        rows: vec![json!({
                            "id": "row-1",
                            "snapshot_id": "attacker-snapshot",
                            "tenant_id": "attacker-tenant",
                            "owner_user_id": "attacker-owner",
                            "value": snapshot_id,
                        })],
                        ..BulkStructuredRowsRequest::default()
                    },
                )
                .unwrap();
            assert_eq!(response.inserted, 1);
        }

        let data = store.read().unwrap();
        for snapshot_id in ["snapshot-a", "snapshot-b"] {
            let row = &data.rows_by_snapshot[snapshot_id][0];
            assert_eq!(row["id"], "row-1");
            assert_eq!(row["snapshot_id"], snapshot_id);
            assert_eq!(row["tenant_id"], tenant_id);
            assert_eq!(row["owner_user_id"], "owner-a");
        }
    }

    #[test]
    fn ingest_polling_sanitizes_legacy_persisted_failure_causes() {
        let config = Config::test();
        let store = Store::new(&config);
        let task_id = "task-legacy-private-error";
        let private_cause =
            "request failed for http://127.0.0.1/private-runtime-auth-marker/file_parse";
        let mut task = ingest_task_fixture(task_id, &config.tenant_id, "failed", 10);
        task.error = Some(private_cause.to_string());
        store
            .write()
            .unwrap()
            .ingest_tasks
            .insert(task_id.to_string(), task);

        let visible = store.get_ingest_task(task_id, None, true).unwrap();
        assert_eq!(visible.error.as_deref(), Some(INGEST_ERROR_FAILED));

        let error = store
            .get_ingest_task_result(task_id, None, true)
            .unwrap_err();
        assert_eq!(error.to_string(), "ingest task failed");
        assert!(!error.to_string().contains(private_cause));
    }

    #[test]
    fn context_search_rereads_rotated_codex_secrets_before_snippet_truncation() {
        const OLD_TOKEN: &str = "codex-old-rotation-token-private-value";
        const NEW_TOKEN: &str = "zxqv-rotated-codex-token-private-value";
        const ANCHOR: &str = "rotated-secret-boundary-anchor";
        let auth_path = std::env::temp_dir().join(format!(
            "nowledge-store-redaction-{}.json",
            uuid::Uuid::now_v7()
        ));
        std::fs::write(&auth_path, json!({ "access_token": OLD_TOKEN }).to_string()).unwrap();

        let mut config = Config::test();
        config.codex_auth_path = Some(auth_path.to_string_lossy().to_string());
        let store = Store::new(&config);
        std::fs::write(&auth_path, json!({ "access_token": NEW_TOKEN }).to_string()).unwrap();
        let _ = config.refresh_configured_secret_values();

        let prefix = format!("{ANCHOR} ");
        let body = format!("{}{}{NEW_TOKEN}", prefix, "x".repeat(229 - prefix.len()));
        let mut node = store.context_node(
            "ctx://test/company/rotated-secret/fragments/0001",
            "Rotated secret boundary",
            2,
            &body,
            "company",
            "rag_company_context",
            &config.tenant_id,
            None,
            Some("rotated-secret-source".to_string()),
            Some("v1".to_string()),
        );
        node.node_kind = "fragment".to_string();
        node.retrieval_role = "fragment".to_string();
        node.retrieval_enabled = true;
        store.write().unwrap().company_context.push(node);

        let outcome = store
            .search_context(
                &config.tenant_id,
                ContextSearchRequest {
                    query: Some(ANCHOR.to_string()),
                    limit: 1,
                    ..ContextSearchRequest::default()
                },
                false,
            )
            .unwrap();
        let snippet = &outcome.response.hits[0].snippet;
        // Retrieval snippets preserve character offsets, so masking uses
        // fixed-width `*` characters instead of the JSON egress marker.
        assert!(snippet.contains('*'), "{snippet}");
        assert!(!snippet.contains("zxqv-"), "{snippet}");
        assert!(!snippet.contains(NEW_TOKEN), "{snippet}");

        let _ = std::fs::remove_file(auth_path);
    }

    #[test]
    fn raw_source_egress_preserves_short_words_while_fragments_break_reconstruction() {
        let mut config = Config::test();
        config.admin_token = Some("owner-u1-token".to_string());
        let store = Store::new(&config);
        let mut source = store.context_node(
            "ctx://test/source",
            "owner",
            2,
            "owner guidance",
            "company",
            "rag_source_documents",
            &config.tenant_id,
            None,
            Some("source".to_string()),
            Some("v1".to_string()),
        );
        source.node_kind = "source_doc".to_string();
        source.retrieval_role = "none".to_string();
        let safe_source = store.sanitize_context_node_for_egress(source);
        assert_eq!(safe_source.title, "owner");
        assert_eq!(safe_source.body, "owner guidance");

        let mut fragment = store.context_node(
            "ctx://test/source/fragments/0001",
            "owner",
            2,
            "owner",
            "company",
            "rag_company_context",
            &config.tenant_id,
            None,
            Some("source".to_string()),
            Some("v1".to_string()),
        );
        fragment.node_kind = "fragment".to_string();
        fragment.retrieval_role = "fragment".to_string();
        let safe_fragment = store.sanitize_context_node_for_egress(fragment);
        assert_eq!(safe_fragment.title, "owner");
        assert_eq!(safe_fragment.body, "*****");
    }

    #[test]
    fn parsed_block_retrieval_masking_prevents_cross_fragment_secret_splitting() {
        const SECRET: &str = "zxqv-mineru-secret-token-private-value";
        let mut config = Config::test();
        config.admin_token = Some(SECRET.to_string());
        let secrets = config.configured_secret_values();
        let prefix = "parsed-block-boundary ";
        let body = format!("{}{}{SECRET}", prefix, "x".repeat(229 - prefix.len()));
        let block = ParsedBlock {
            block_id: "boundary-block".to_string(),
            block_type: "paragraph".to_string(),
            text: Some(body.clone()),
            ..ParsedBlock::default()
        };
        let masked = mask_parsed_block_for_retrieval(block, &secrets);
        assert_eq!(
            masked.text.as_deref().unwrap().chars().count(),
            body.chars().count()
        );

        let fragments = BlockAwareFragmenter::from_policy(Some(&FragmentPolicy {
            chunk_size_chars: Some(240),
            overlap_chars: Some(0),
            min_chunk_chars: Some(240),
        }))
        .fragment("ignored when parsed blocks exist", &[masked]);
        assert_eq!(fragments.len(), 2);
        let joined = fragments
            .iter()
            .map(|fragment| fragment.content.as_str())
            .collect::<String>();
        assert!(joined.contains('*'));
        assert!(!joined.contains("zxqv-"));
        assert!(!joined.contains(SECRET));
    }

    #[test]
    fn parsed_block_retrieval_masks_secret_projections_across_block_fields() {
        const SECRET: &str = "zxqv-parsed-block-field-secret-private-value";
        let left = &SECRET[..14];
        let middle = &SECRET[14..30];
        let right = &SECRET[30..];
        let mut config = Config::test();
        config.admin_token = Some(SECRET.to_string());
        let secrets = config.configured_secret_values();
        let masked = mask_parsed_block_for_retrieval(
            ParsedBlock {
                block_id: "multi-field-secret".to_string(),
                block_type: "image".to_string(),
                text: Some(left.to_string()),
                image_ref: Some(middle.to_string()),
                caption: Some(right.to_string()),
                ..ParsedBlock::default()
            },
            &secrets,
        );

        for field in [
            masked.text.as_deref().unwrap(),
            masked.image_ref.as_deref().unwrap(),
            masked.caption.as_deref().unwrap(),
        ] {
            assert!(field.chars().all(|ch| ch == '*'), "{field}");
        }
        let fragments = BlockAwareFragmenter::from_policy(None)
            .fragment("ignored when parsed blocks exist", &[masked]);
        let prompt_projection = fragments
            .iter()
            .map(|fragment| fragment.content.as_str())
            .collect::<String>();
        assert!(!prompt_projection.contains(left));
        assert!(!prompt_projection.contains(middle));
        assert!(!prompt_projection.contains(right));
        assert!(!prompt_projection.contains(SECRET));
    }

    #[test]
    fn query_time_masking_hides_a_configured_secret_split_between_parsed_blocks() {
        const OLD_SECRET: &str = "codex-old-parsed-block-token-private-value";
        const SECRET: &str = "zxqv-split-between-parsed-blocks-private-value";
        let split = 18;
        let auth_path = std::env::temp_dir().join(format!(
            "nowledge-store-parsed-block-redaction-{}.json",
            uuid::Uuid::now_v7()
        ));
        std::fs::write(
            &auth_path,
            json!({ "access_token": OLD_SECRET }).to_string(),
        )
        .unwrap();
        let mut config = Config::test();
        config.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
        let store = Store::new(&config);
        let ingress_secrets = config.configured_secret_values();
        let blocks = vec![
            ParsedBlock {
                block_id: "left-block".to_string(),
                block_type: "paragraph".to_string(),
                text: Some(format!("parsed-block-anchor {}", &SECRET[..split])),
                reading_order: 1,
                ..ParsedBlock::default()
            },
            ParsedBlock {
                block_id: "right-block".to_string(),
                block_type: "paragraph".to_string(),
                text: Some(format!("{} trailing text", &SECRET[split..])),
                reading_order: 2,
                ..ParsedBlock::default()
            },
        ];
        let masked_blocks = blocks
            .into_iter()
            .map(|block| mask_parsed_block_for_retrieval(block, &ingress_secrets))
            .collect::<Vec<_>>();
        let fragments = BlockAwareFragmenter::from_policy(None)
            .fragment("ignored when parsed blocks exist", &masked_blocks);
        assert!(fragments
            .iter()
            .any(|fragment| fragment.content.contains("zxqv-")));

        std::fs::write(&auth_path, json!({ "access_token": SECRET }).to_string()).unwrap();
        let query_secrets = config.refresh_configured_secret_values();

        assert_eq!(fragments.len(), 2);
        let snippets = fragments
            .iter()
            .enumerate()
            .map(|(index, fragment)| {
                let mut node = store.context_node(
                    &format!("ctx://test/source/fragments/{index:04}"),
                    "Parsed block split",
                    2,
                    &fragment.content,
                    "company",
                    "rag_company_context",
                    &config.tenant_id,
                    None,
                    Some("source".to_string()),
                    Some("v1".to_string()),
                );
                node.node_kind = "fragment".to_string();
                node.retrieval_role = "fragment".to_string();
                context_hit_from_node(&node, 1.0, &query_secrets).snippet
            })
            .collect::<Vec<_>>();

        assert!(snippets.iter().all(|snippet| snippet.contains('*')));
        assert!(!snippets[0].contains(&SECRET[..split]));
        assert!(!snippets[1].contains(&SECRET[split..]));
        assert!(!snippets.concat().contains(SECRET));

        let _ = std::fs::remove_file(auth_path);
    }

    #[test]
    fn query_time_masking_breaks_three_piece_rotated_secret_reconstruction() {
        const OLD_SECRET: &str = "codex-old-three-piece-token";
        const SECRET: &str = "abcdefghij";
        let auth_path = std::env::temp_dir().join(format!(
            "nowledge-store-three-piece-redaction-{}.json",
            uuid::Uuid::now_v7()
        ));
        std::fs::write(
            &auth_path,
            json!({ "access_token": OLD_SECRET }).to_string(),
        )
        .unwrap();
        let mut config = Config::test();
        config.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
        let store = Store::new(&config);

        std::fs::write(&auth_path, json!({ "access_token": SECRET }).to_string()).unwrap();
        let query_secrets = config.refresh_configured_secret_values();
        let snippets = ["Xabc", "defg", "hij"]
            .iter()
            .enumerate()
            .map(|(index, body)| {
                let mut node = store.context_node(
                    &format!("ctx://test/source/fragments/{index:04}"),
                    "Three piece split",
                    2,
                    body,
                    "company",
                    "rag_company_context",
                    &config.tenant_id,
                    None,
                    Some("source".to_string()),
                    Some("v1".to_string()),
                );
                node.node_kind = "fragment".to_string();
                node.retrieval_role = "fragment".to_string();
                context_hit_from_node(&node, 1.0, &query_secrets).snippet
            })
            .collect::<Vec<_>>();

        assert_eq!(snippets[0], "Xabc");
        assert_eq!(snippets[1], "****");
        assert_eq!(snippets[2], "hij");
        assert!(!snippets.concat().contains(SECRET));

        let _ = std::fs::remove_file(auth_path);
    }

    #[tokio::test]
    async fn ingest_cleanup_prunes_only_expired_terminal_tasks() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();

        let expired_completed = ingest_task_fixture("task-old-done", tenant_id, "completed", 7_200);
        let expired_failed = ingest_task_fixture("task-old-failed", tenant_id, "failed", 7_200);
        let expired_but_running =
            ingest_task_fixture("task-old-running", tenant_id, "parsing", 7_200);
        let fresh_completed = ingest_task_fixture("task-new-done", tenant_id, "completed", 10);

        {
            let mut data = store.write().unwrap();
            for task in [
                &expired_completed,
                &expired_failed,
                &expired_but_running,
                &fresh_completed,
            ] {
                data.ingest_tasks
                    .insert(task.task_id.clone(), (*task).clone());
            }
            data.ingest_results.insert(
                expired_completed.task_id.clone(),
                IngestTaskResult {
                    task: expired_completed.clone(),
                    source_document_uri: "ctx://test/source-doc/task-old-done".to_string(),
                    source_id: expired_completed.source_id.clone(),
                    revision_id: expired_completed.revision_id.clone(),
                    parse_artifacts: Vec::new(),
                    parsed_blocks: Vec::new(),
                    fragment_uris: Vec::new(),
                    context_uris: Vec::new(),
                },
            );
            data.parsed_blocks.insert(
                SourceDocumentKey::new(tenant_id, None, "ctx://test/source-doc/task-old-done"),
                vec![ParsedBlock {
                    block_id: "expired-block".to_string(),
                    ..ParsedBlock::default()
                }],
            );
        }

        let mut pruned = store
            .cleanup_ingest_tasks_async(tenant_id, 3_600)
            .await
            .unwrap();
        pruned.sort();
        assert_eq!(
            pruned,
            vec!["task-old-done".to_string(), "task-old-failed".to_string()]
        );

        let data = store.read().unwrap();
        assert!(!data.ingest_tasks.contains_key("task-old-done"));
        assert!(!data.ingest_tasks.contains_key("task-old-failed"));
        assert!(!data.ingest_results.contains_key("task-old-done"));
        assert!(!data.parsed_blocks.contains_key(&SourceDocumentKey::new(
            tenant_id,
            None,
            "ctx://test/source-doc/task-old-done",
        )));
        assert!(data.ingest_tasks.contains_key("task-old-running"));
        assert!(data.ingest_tasks.contains_key("task-new-done"));
    }

    #[tokio::test]
    async fn source_doc_leak_guard_fails_when_source_doc_is_retrieved() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let uri = "ctx://test/source/leaky";
        let mut node = store.context_node(
            uri,
            "Leaky source doc",
            2,
            "source-doc-leak-keyword",
            "company",
            "rag_company_context",
            tenant_id,
            None,
            Some("leaky-source".to_string()),
            Some("v1".to_string()),
        );
        node.node_kind = "source_doc".to_string();
        node.retrieval_role = "fragment".to_string();
        node.retrieval_enabled = true;
        node.source_document_uri = Some(uri.to_string());
        {
            let mut data = store.write().unwrap();
            data.company_context.push(node);
            data.source_documents.insert(
                SourceDocumentKey::new(tenant_id, None, uri),
                SourceDocument {
                    id: "source-doc-leak-fixture".to_string(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: None,
                    source_kind: "test".to_string(),
                    source_id: "leaky-source".to_string(),
                    revision_id: "v1".to_string(),
                    uri: uri.to_string(),
                    title: "Leaky source doc".to_string(),
                    content: "source-doc-leak-keyword".to_string(),
                    checksum: "checksum".to_string(),
                    status: "active".to_string(),
                    retrieval_enabled: false,
                    created_at: now(),
                    updated_at: now(),
                },
            );
        }
        let case = store
            .create_eval_case(
                tenant_id,
                CreateRagEvalCaseRequest {
                    question: Some("source-doc-leak-keyword".to_string()),
                    ..CreateRagEvalCaseRequest::default()
                },
            )
            .unwrap();
        let run = store
            .create_eval_run_async(
                tenant_id,
                CreateRagEvalRunRequest {
                    case_ids: vec![case.id],
                    ..CreateRagEvalRunRequest::default()
                },
                false,
            )
            .await
            .unwrap();
        assert_eq!(run.status, "failed");
        assert!(run
            .guard_results
            .iter()
            .any(|guard| { guard.name == "source_doc_not_default_retrieved" && !guard.passed }));
    }

    #[tokio::test]
    async fn misrouted_personal_row_is_filtered_before_owner_retrieval() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let routing = store
            .ensure_user_index(tenant_id, "u1", EnsureUserEventIndexRequest::default())
            .unwrap()
            .routing;
        let mut node = store.context_node(
            "ctx://test/private/cross-owner/fragments/0001",
            "Cross owner fragment",
            2,
            "cross-owner-leak-keyword",
            "personal",
            &routing.personal_context_index_uid,
            tenant_id,
            Some("u2".to_string()),
            Some("cross-owner-source".to_string()),
            Some("v1".to_string()),
        );
        node.node_kind = "fragment".to_string();
        node.retrieval_role = "fragment".to_string();
        node.retrieval_enabled = true;
        node.source_document_uri = Some("ctx://test/private/cross-owner/source".to_string());
        {
            let mut data = store.write().unwrap();
            data.personal_context
                .entry(routing.personal_context_index_uid)
                .or_default()
                .push(node);
        }
        let case = store
            .create_eval_case(
                tenant_id,
                CreateRagEvalCaseRequest {
                    owner_user_id: Some("u1".to_string()),
                    question: Some("cross-owner-leak-keyword".to_string()),
                    ..CreateRagEvalCaseRequest::default()
                },
            )
            .unwrap();
        let run = store
            .create_eval_run_async(
                tenant_id,
                CreateRagEvalRunRequest {
                    case_ids: vec![case.id],
                    ..CreateRagEvalRunRequest::default()
                },
                false,
            )
            .await
            .unwrap();
        assert_eq!(run.status, "failed");
        assert!(run
            .guard_results
            .iter()
            .any(|guard| guard.name == "owner_acl_never_leaks" && guard.passed));
    }

    #[tokio::test]
    async fn eval_delta_reports_fixed_and_regressed_risk_cases_for_verdict() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let created_at = now();
        {
            let mut data = store.write().unwrap();
            data.harness_changes.insert(
                "delta-change".to_string(),
                HarnessChangeManifest {
                    id: "delta-change".to_string(),
                    tenant_id: tenant_id.to_string(),
                    iteration: 1,
                    change_type: "improvement".to_string(),
                    component_id: "retrieval.context_search".to_string(),
                    files: vec!["src/store.rs".to_string()],
                    failure_pattern: "retrieval_recall".to_string(),
                    root_cause: "test".to_string(),
                    targeted_fix: "test".to_string(),
                    predicted_fixes: vec!["case-fixed".to_string()],
                    risk_cases: vec!["case-risk".to_string()],
                    expected_metric_deltas: json!({ "pass_rate": 0.5 }),
                    baseline_eval_run_id: Some("baseline-run".to_string()),
                    candidate_eval_run_id: Some("candidate-run".to_string()),
                    why_this_component: "test".to_string(),
                    created_by: "test".to_string(),
                    created_at,
                    status: "proposed".to_string(),
                },
            );
            data.eval_runs.insert(
                "baseline-run".to_string(),
                RagEvalRun {
                    id: "baseline-run".to_string(),
                    tenant_id: tenant_id.to_string(),
                    change_id: Some("delta-change".to_string()),
                    case_ids: vec![
                        "case-fixed".to_string(),
                        "case-risk".to_string(),
                        "case-still-failed".to_string(),
                        "case-still-passed".to_string(),
                    ],
                    result_ids: vec![
                        "baseline-fixed".to_string(),
                        "baseline-risk".to_string(),
                        "baseline-still-failed".to_string(),
                        "baseline-still-passed".to_string(),
                    ],
                    trace_ids: Vec::new(),
                    status: "failed".to_string(),
                    metrics: RagEvalMetrics {
                        pass_rate: 0.5,
                        retrieval_recall_at_5: 0.5,
                        ..RagEvalMetrics::default()
                    },
                    guard_results: Vec::new(),
                    overview_source_document_uri: None,
                    report_source_document_uris: Vec::new(),
                    created_by: "test".to_string(),
                    created_at,
                    completed_at: Some(created_at),
                },
            );
            data.eval_runs.insert(
                "candidate-run".to_string(),
                RagEvalRun {
                    id: "candidate-run".to_string(),
                    tenant_id: tenant_id.to_string(),
                    change_id: Some("delta-change".to_string()),
                    case_ids: vec![
                        "case-fixed".to_string(),
                        "case-risk".to_string(),
                        "case-still-failed".to_string(),
                        "case-still-passed".to_string(),
                    ],
                    result_ids: vec![
                        "candidate-fixed".to_string(),
                        "candidate-risk".to_string(),
                        "candidate-still-failed".to_string(),
                        "candidate-still-passed".to_string(),
                    ],
                    trace_ids: Vec::new(),
                    status: "failed".to_string(),
                    metrics: RagEvalMetrics {
                        pass_rate: 0.5,
                        retrieval_recall_at_5: 1.0,
                        ..RagEvalMetrics::default()
                    },
                    guard_results: Vec::new(),
                    overview_source_document_uri: None,
                    report_source_document_uris: Vec::new(),
                    created_by: "test".to_string(),
                    created_at,
                    completed_at: Some(created_at),
                },
            );
            for (id, run_id, case_id, status, failures) in [
                (
                    "baseline-fixed",
                    "baseline-run",
                    "case-fixed",
                    "failed",
                    vec!["retrieval_recall"],
                ),
                (
                    "candidate-fixed",
                    "candidate-run",
                    "case-fixed",
                    "passed",
                    vec![],
                ),
                (
                    "baseline-risk",
                    "baseline-run",
                    "case-risk",
                    "passed",
                    vec![],
                ),
                (
                    "candidate-risk",
                    "candidate-run",
                    "case-risk",
                    "failed",
                    vec!["citation_precision"],
                ),
                (
                    "baseline-still-failed",
                    "baseline-run",
                    "case-still-failed",
                    "failed",
                    vec!["retrieval_recall"],
                ),
                (
                    "candidate-still-failed",
                    "candidate-run",
                    "case-still-failed",
                    "failed",
                    vec!["retrieval_recall"],
                ),
                (
                    "baseline-still-passed",
                    "baseline-run",
                    "case-still-passed",
                    "passed",
                    vec![],
                ),
                (
                    "candidate-still-passed",
                    "candidate-run",
                    "case-still-passed",
                    "passed",
                    vec![],
                ),
            ] {
                data.eval_case_results.insert(
                    id.to_string(),
                    RagEvalCaseResult {
                        id: id.to_string(),
                        tenant_id: config.tenant_id.clone(),
                        run_id: run_id.to_string(),
                        case_id: case_id.to_string(),
                        owner_user_id: None,
                        status: status.to_string(),
                        question: case_id.to_string(),
                        trace_id: "trace".to_string(),
                        answer: String::new(),
                        citations: Vec::new(),
                        retrieved_uris: Vec::new(),
                        source_document_uris: Vec::new(),
                        failures: failures.into_iter().map(ToString::to_string).collect(),
                        guard_failures: Vec::new(),
                        metrics: json!({}),
                        latency_ms: 0,
                        created_at,
                    },
                );
            }
        }

        let delta = store
            .compare_harness_change("delta-change", None, None)
            .unwrap();
        assert_eq!(delta.fixed_cases, vec!["case-fixed"]);
        assert_eq!(delta.regressed_cases, vec!["case-risk"]);
        assert_eq!(delta.unchanged_failed_cases, vec!["case-still-failed"]);
        assert_eq!(delta.unchanged_passed_cases, vec!["case-still-passed"]);
        assert_eq!(delta.metric_deltas["retrieval_recall_at_5"], 0.5);
        assert!(delta
            .risk_matrix
            .iter()
            .any(|risk| risk.case_id == "case-risk" && risk.regressed));

        let verdict = store
            .create_harness_verdict_async(
                tenant_id,
                "delta-change",
                CreateHarnessChangeVerdictRequest {
                    eval_run_id: Some("candidate-run".to_string()),
                    ..CreateHarnessChangeVerdictRequest::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(verdict.verdict, "rollback");
        assert_eq!(verdict.predicted_fixes_confirmed, vec!["case-fixed"]);
        assert_eq!(verdict.risk_cases_regressed, vec!["case-risk"]);
        assert!(verdict.evidence.get("delta").is_some());
    }
}
