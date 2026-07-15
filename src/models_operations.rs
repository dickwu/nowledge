use super::*;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityClass {
    DurableCanonical,
    DerivedDurable,
    Ephemeral,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HydrationStrategy {
    Startup,
    ReadThrough,
    LazySnapshot,
    Regenerate,
    Ephemeral,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HydrationStatus {
    Pending,
    Complete,
    Incomplete,
    NotRequired,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HydrationDomainReport {
    pub durability: DurabilityClass,
    pub strategy: HydrationStrategy,
    pub mandatory: bool,
    pub status: String,
    pub expected: usize,
    pub loaded: usize,
    pub skipped: usize,
    pub quarantined: usize,
    pub recovered: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HydrationReport {
    pub tenant_id: String,
    pub backend: String,
    pub status: HydrationStatus,
    pub ready: bool,
    pub started_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    pub domains: BTreeMap<String, HydrationDomainReport>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationActorScope {
    Owner,
    TenantService,
    Admin,
    System,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationActor {
    pub scope: OperationActorScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id_hash: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub roles: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStepRole {
    Primary,
    SideEffect,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStepStatus {
    /// The resource has not been sent to the repository.
    Pending,
    /// The repository durably accepted the resource and returned task UIDs;
    /// read-your-writes may publish the cache, but indexing is not confirmed.
    Submitted,
    /// The write was synchronous or every returned task UID was confirmed.
    Completed,
    /// The current apply or task-confirmation attempt failed and is retryable.
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationStatus {
    /// The primary resource has not been accepted.
    Pending,
    /// The primary resource is durable; one or more side effects are pending.
    PrimaryCommitted,
    /// At least one side effect is durable while another remains pending.
    EffectsSubmitted,
    /// The primary is durable, but at least one side effect failed.
    PartiallyFailed,
    /// Every planned write is durably accepted. Indexing may still be pending.
    Completed,
    /// The primary resource failed.
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OperationIndexingState {
    /// At least one accepted asynchronous task has not been confirmed.
    Pending,
    /// Every planned write is confirmed searchable, or completed synchronously.
    Completed,
    /// A write or task-confirmation attempt failed.
    Failed,
}

/// One ordered Meilisearch deletion in a company-source removal operation.
///
/// Each target is persisted as its own operation step so an accepted task UID
/// is checkpointed before the next index is mutated. `Links` is optional and
/// carries the exact logical IDs selected from the staged cache snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum CompanySourceDeleteTarget {
    Fragments,
    Revisions,
    Source,
    SourceDocuments,
    ParseArtifacts,
    IngestTasks,
    IngestResults,
    Links {
        link_ids: Vec<String>,
        #[serde(default)]
        related_uris: Vec<String>,
    },
}

/// A complete, replayable description of one persistence mutation. The
/// operation plan is immutable after it is journaled; only OperationProgress
/// changes as individual steps are submitted, confirmed, or retried.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "payload", rename_all = "snake_case")]
pub enum OperationResource {
    EnsureUserEventIndex {
        index: UserEventIndex,
    },
    HistoryEvents {
        events: Vec<HistoryEvent>,
    },
    ContextNodes {
        index_uid: String,
        nodes: Vec<ContextNode>,
    },
    StateItem {
        item: StateItem,
    },
    Insight {
        insight: InsightRecord,
    },
    CompanySource {
        source: CompanySource,
    },
    SourceRevision {
        revision: SourceRevision,
    },
    DeleteCompanySourceIndex {
        source_id: String,
        target: CompanySourceDeleteTarget,
    },
    SourceDocuments {
        documents: Vec<SourceDocument>,
    },
    ParseArtifacts {
        artifacts: Vec<ParseArtifact>,
    },
    StructuredSnapshot {
        snapshot: StructuredSnapshot,
    },
    Dataset {
        dataset: DatasetRecord,
    },
    StructuredRows {
        rows: Vec<Value>,
    },
    StructuredSummary {
        summary: Value,
    },
    Session {
        session: SessionRecord,
    },
    Trace {
        trace: TraceRecord,
    },
    Links {
        links: Vec<KnowledgeLink>,
    },
    HarnessComponents {
        components: Vec<HarnessComponent>,
        revisions: Vec<HarnessComponentRevision>,
    },
    HarnessChanges {
        changes: Vec<HarnessChangeManifest>,
    },
    HarnessVerdicts {
        verdicts: Vec<HarnessChangeVerdict>,
    },
    IngestTask {
        task: IngestTask,
    },
    IngestTasks {
        tasks: Vec<IngestTask>,
    },
    DeleteIngestTasks {
        task_ids: Vec<String>,
    },
    IngestResult {
        result: IngestTaskResult,
    },
    EvalCase {
        case: RagEvalCase,
    },
    EvalRun {
        run: RagEvalRun,
    },
    EvalCaseResults {
        results: Vec<RagEvalCaseResult>,
    },
    EvalOverview {
        overview: RagEvalOverview,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStep {
    pub id: String,
    pub role: OperationStepRole,
    pub resource: OperationResource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationPlan {
    pub schema_version: u32,
    pub id: String,
    pub tenant_id: String,
    pub operation_kind: String,
    pub actor: OperationActor,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key_hash: Option<String>,
    pub primary: OperationStep,
    #[serde(default)]
    pub side_effects: Vec<OperationStep>,
    #[serde(default)]
    pub redacted_metadata: Value,
    /// Original application response retained when the operation has an
    /// idempotency key, enabling replay after completion or reconciliation.
    #[serde(default)]
    pub response_snapshot: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStepProgress {
    pub step_id: String,
    pub status: OperationStepStatus,
    pub attempts: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_uids: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_fingerprint: Option<String>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationProgress {
    pub attempt_count: u32,
    pub steps: BTreeMap<String, OperationStepProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationRecord {
    pub id: String,
    pub tenant_id: String,
    pub operation_kind: String,
    pub actor_scope: OperationActorScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key_hash: Option<String>,
    pub plan: OperationPlan,
    pub status: OperationStatus,
    pub indexing_state: OperationIndexingState,
    pub progress: OperationProgress,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistenceMetadata {
    pub operation_id: String,
    pub status: OperationStatus,
    pub indexing_state: OperationIndexingState,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_task_uids: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_uids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RepositoryWriteReceipt {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub task_uids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationSummary {
    pub id: String,
    pub tenant_id: String,
    pub operation_kind: String,
    pub actor_scope: OperationActorScope,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key_hash: Option<String>,
    pub status: OperationStatus,
    pub indexing_state: OperationIndexingState,
    pub attempt_count: u32,
    pub pending_steps: usize,
    pub failed_steps: usize,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_fingerprint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationListItem {
    #[serde(flatten)]
    pub summary: OperationSummary,
    /// Replay payloads are omitted unless an authorized caller explicitly
    /// requests them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<OperationPlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct OperationListRequest {
    #[serde(default)]
    pub statuses: Vec<OperationStatus>,
    #[serde(default)]
    pub operation_kinds: Vec<String>,
    #[serde(default = "default_operation_list_limit")]
    pub limit: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub include_plan: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationListResponse {
    pub operations: Vec<OperationListItem>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReconcileOperationsRequest {
    #[serde(default)]
    pub operation_ids: Vec<String>,
    #[serde(default)]
    pub statuses: Vec<OperationStatus>,
    #[serde(default = "default_operation_reconcile_limit")]
    pub limit: usize,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationReconcileError {
    pub operation_id: String,
    pub category: String,
    pub fingerprint: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileOperationsResponse {
    pub checked: usize,
    pub reconciled: usize,
    pub completed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub errors: Vec<OperationReconcileError>,
    /// Summary-only by design: reconcile responses never echo replay
    /// payloads.
    pub operations: Vec<OperationSummary>,
}
