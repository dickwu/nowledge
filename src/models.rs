use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SourceRef {
    pub kind: String,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventIndexRouting {
    pub tenant_id: String,
    pub owner_user_id_hash: String,
    pub event_index_uid: String,
    pub personal_context_index_uid: String,
    pub strategy: String,
    pub schema_version: u32,
    pub settings_hash: String,
    pub created: bool,
    pub reused: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEventIndex {
    pub id: String,
    pub tenant_id: String,
    pub tenant_hash: String,
    pub owner_user_id_hash: String,
    pub event_index_uid: String,
    pub personal_context_index_uid: String,
    pub schema_version: u32,
    pub settings_hash: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_event_at: Option<DateTime<Utc>>,
    pub event_count_estimate: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct EnsureUserEventIndexRequest {
    #[serde(default)]
    pub force_reapply_settings: bool,
    #[serde(default = "default_true")]
    pub create_personal_context_index: bool,
    #[serde(default)]
    pub schema_version: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEventIndexResponse {
    pub index: UserEventIndex,
    pub routing: EventIndexRouting,
    pub meili_task_uids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListUserEventIndexesResponse {
    pub indexes: Vec<UserEventIndex>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ReconcileUserEventIndexesRequest {
    #[serde(default)]
    pub owner_user_ids: Vec<String>,
    #[serde(default)]
    pub dry_run: bool,
    #[serde(default)]
    pub reapply_settings: bool,
    #[serde(default = "default_true")]
    pub create_missing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReconcileUserEventIndexesResponse {
    pub checked: usize,
    pub created: usize,
    pub updated_settings: usize,
    pub errors: Vec<String>,
    pub indexes: Vec<UserEventIndex>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AppendHistoryEventRequest {
    pub event_type: Option<String>,
    pub entity_type: Option<String>,
    pub entity_id: Option<String>,
    pub owner_user_id: Option<String>,
    pub occurred_at: Option<DateTime<Utc>>,
    pub observed_at: Option<DateTime<Utc>>,
    pub source_kind: Option<String>,
    pub source_ref: Option<SourceRef>,
    pub text: Option<String>,
    #[serde(default)]
    pub payload: Value,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default = "default_privacy")]
    pub privacy: String,
    #[serde(default = "default_promote_policy")]
    pub promote_policy: String,
    pub idempotency_key: Option<String>,
    #[serde(default)]
    pub event_index_hint: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BulkHistoryEventsRequest {
    #[serde(default)]
    pub events: Vec<AppendHistoryEventRequest>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEvent {
    pub id: String,
    pub event_type: String,
    pub entity_type: String,
    pub entity_id: String,
    pub occurred_at: DateTime<Utc>,
    pub observed_at: DateTime<Utc>,
    pub source_kind: String,
    pub source_ref: SourceRef,
    pub text: String,
    #[serde(default)]
    pub payload: Value,
    pub tags: Vec<String>,
    pub privacy: String,
    pub tenant_id: String,
    pub owner_user_id: String,
    pub owner_user_id_hash: String,
    pub event_index_uid: String,
    pub event_index_schema_version: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idempotency_key_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryEventResponse {
    pub event: HistoryEvent,
    pub duplicate: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub materialization_job_id: Option<String>,
    pub routing: EventIndexRouting,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meili_task_uid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkHistoryEventsResponse {
    pub inserted: usize,
    pub duplicates: usize,
    pub event_ids: Vec<String>,
    pub materialization_job_ids: Vec<String>,
    pub routing: EventIndexRouting,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meili_task_uid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HistorySearchRequest {
    pub query: Option<String>,
    #[serde(default)]
    pub event_types: Vec<String>,
    pub entity_type: Option<String>,
    pub entity_id: Option<String>,
    pub owner_user_id: Option<String>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistorySearchResponse {
    pub hits: Vec<HistoryEvent>,
    pub routing: EventIndexRouting,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TimelineQueryRequest {
    pub owner_user_id: Option<String>,
    #[serde(default)]
    pub entity_refs: Vec<Value>,
    pub from: Option<DateTime<Utc>>,
    pub to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub include_state_changes: bool,
    #[serde(default)]
    pub include_doc_revisions: bool,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineResponse {
    pub events: Vec<HistoryEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateItem {
    pub id: String,
    pub tenant_id: String,
    pub owner_user_id: String,
    pub state_type: String,
    pub natural_key: String,
    pub title: String,
    pub statement: String,
    #[serde(default)]
    pub value: Value,
    pub status: String,
    pub confidence: f32,
    pub salience: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_from: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<DateTime<Utc>>,
    pub source_refs: Vec<SourceRef>,
    pub context_uri: String,
    pub current_version: u32,
    pub supersedes: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct UpsertStateFactRequest {
    pub owner_user_id: Option<String>,
    pub state_type: Option<String>,
    pub title: Option<String>,
    pub statement: Option<String>,
    #[serde(default)]
    pub value: Value,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    #[serde(default = "default_salience")]
    pub salience: f32,
    pub valid_from: Option<DateTime<Utc>>,
    pub valid_to: Option<DateTime<Utc>>,
    #[serde(default)]
    pub source_refs: Vec<SourceRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub document: Option<StateDocumentPayload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_policy: Option<FragmentPolicy>,
    #[serde(default = "default_merge_policy")]
    pub merge_policy: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateDocumentPayload {
    pub content: Option<String>,
    pub content_type: Option<String>,
    pub source_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_policy: Option<FragmentPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FragmentPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_size_chars: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overlap_chars: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_chunk_chars: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PatchStateFactRequest {
    pub owner_user_id: Option<String>,
    pub statement: Option<String>,
    #[serde(default)]
    pub value: Option<Value>,
    pub confidence: Option<f32>,
    pub salience: Option<f32>,
    pub status: Option<String>,
    pub valid_to: Option<DateTime<Utc>>,
    pub patch_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateItemResponse {
    pub item: StateItem,
    pub history_event_id: String,
    pub context_uri: String,
    pub decision: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateSearchRequest {
    pub query: Option<String>,
    #[serde(default)]
    pub state_types: Vec<String>,
    pub owner_user_id: Option<String>,
    #[serde(default = "default_active")]
    pub status: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSearchResponse {
    pub hits: Vec<StateItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetColumn {
    pub name: String,
    pub kind: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trend_direction: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DatasetSchemaUpsertRequest {
    pub title: Option<String>,
    pub description: Option<String>,
    pub granularity: Option<String>,
    pub subject_type: Option<String>,
    #[serde(default)]
    pub columns: Vec<DatasetColumn>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRecord {
    pub id: String,
    pub dataset_key: String,
    pub title: String,
    pub schema_version: u32,
    pub status: String,
    pub columns: Vec<DatasetColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSchemaResponse {
    pub dataset: DatasetRecord,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ApplySnapshotRequest {
    pub snapshot_id: Option<String>,
    pub analysis_window: Option<String>,
    #[serde(default = "default_llm_none")]
    pub llm_mode: String,
    #[serde(default = "default_true")]
    pub materialize_context: bool,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplySnapshotResponse {
    pub snapshot_id: String,
    pub summary_ids: Vec<String>,
    pub state_item_ids: Vec<String>,
    pub insight_candidate_ids: Vec<String>,
    pub context_uris: Vec<String>,
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CurrentStructuredStateResponse {
    pub items: Vec<StateItem>,
    pub summaries: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InsightUpsertRequest {
    pub owner_user_id: Option<String>,
    pub insight_type: Option<String>,
    pub title: Option<String>,
    pub statement: Option<String>,
    pub evidence_text: Option<String>,
    #[serde(default)]
    pub source_refs: Vec<SourceRef>,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    #[serde(default = "default_salience")]
    pub salience: f32,
    #[serde(default = "default_privacy")]
    pub privacy: String,
    #[serde(default = "default_merge_policy")]
    pub merge_policy: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightRecord {
    pub id: String,
    pub insight_type: String,
    pub title: String,
    pub statement: String,
    pub status: String,
    pub confidence: f32,
    pub salience: f32,
    pub context_uri: String,
    pub source_refs: Vec<SourceRef>,
    pub owner_user_id: String,
    pub privacy: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightResponse {
    pub insight: InsightRecord,
    pub history_event_id: String,
    pub context_uri: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InsightPatchRequest {
    pub statement: Option<String>,
    pub status: Option<String>,
    pub confidence: Option<f32>,
    pub salience: Option<f32>,
    pub privacy: Option<String>,
    pub valid_to: Option<DateTime<Utc>>,
    pub patch_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InsightSearchRequest {
    pub query: Option<String>,
    pub owner_user_id: Option<String>,
    #[serde(default)]
    pub insight_types: Vec<String>,
    #[serde(default = "default_active")]
    pub status: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightSearchResponse {
    pub hits: Vec<InsightRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinkUpsertRequest {
    pub owner_user_id: Option<String>,
    pub source_uri: Option<String>,
    pub target_uri: Option<String>,
    pub source_title: Option<String>,
    pub target_title: Option<String>,
    #[serde(default = "default_related")]
    pub relation: String,
    pub rationale: Option<String>,
    pub evidence_text: Option<String>,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    #[serde(default = "default_api")]
    pub created_by: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnowledgeLink {
    pub id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub source_uri: String,
    pub target_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_title: Option<String>,
    pub relation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_text: Option<String>,
    pub confidence: f32,
    pub created_by: String,
    pub status: String,
    #[serde(default)]
    pub tags: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkResponse {
    pub link: KnowledgeLink,
    pub decision: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_event_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinkSearchRequest {
    pub owner_user_id: Option<String>,
    pub query: Option<String>,
    pub uri: Option<String>,
    #[serde(default = "default_both")]
    pub direction: String,
    #[serde(default)]
    pub relations: Vec<String>,
    #[serde(default = "default_active")]
    pub status: String,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LinkSearchResponse {
    pub links: Vec<KnowledgeLink>,
    pub outbound: Vec<KnowledgeLink>,
    pub backlinks: Vec<KnowledgeLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalysisInsightRequest {
    pub owner_user_id: Option<String>,
    pub history_event_id: Option<String>,
    pub query: Option<String>,
    #[serde(default)]
    pub seed_uris: Vec<String>,
    #[serde(default = "default_context_limit")]
    pub context_limit: usize,
    #[serde(default = "default_limit")]
    pub link_limit: usize,
    #[serde(default = "default_true")]
    pub create_links: bool,
    #[serde(default = "default_true")]
    pub upsert_insights: bool,
    #[serde(default)]
    pub debug: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LinkCandidate {
    pub source_uri: String,
    pub target_uri: String,
    #[serde(default = "default_related")]
    pub relation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct InsightCandidate {
    #[serde(default = "default_analysis")]
    pub insight_type: String,
    pub title: String,
    pub statement: String,
    #[serde(default = "default_confidence")]
    pub confidence: f32,
    #[serde(default = "default_salience")]
    pub salience: f32,
    #[serde(default)]
    pub source_uris: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnalysisInsightResponse {
    pub analysis_id: String,
    pub query: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_index_uid: Option<String>,
    pub context_hits: Vec<ContextHit>,
    pub existing_links: Vec<KnowledgeLink>,
    pub link_candidates: Vec<LinkCandidate>,
    pub insight_candidates: Vec<InsightCandidate>,
    pub created_links: Vec<KnowledgeLink>,
    pub insights: Vec<InsightRecord>,
    #[serde(default)]
    pub usage: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CompanyDocPreflightRequest {
    pub title: Option<String>,
    pub source_uri: Option<String>,
    pub content_type: Option<String>,
    pub text_preview: Option<String>,
    pub checksum: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub scope: Option<String>,
    #[serde(default = "default_similarity_threshold")]
    pub similarity_threshold: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompanyDocPreflightResponse {
    pub decision_id: String,
    pub recommended_action: String,
    pub confidence: f32,
    pub matched_sources: Vec<Value>,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateRevisionRequest {
    pub preflight_decision_id: Option<String>,
    pub title: Option<String>,
    pub source_uri: Option<String>,
    pub content: Option<String>,
    pub checksum: Option<String>,
    pub change_note: Option<String>,
    #[serde(default = "default_true")]
    pub ingest: bool,
    #[serde(default)]
    pub force_create: bool,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRevisionResponse {
    pub source_id: String,
    pub revision_id: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingest_job_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ActivateRevisionRequest {
    pub reason: Option<String>,
    #[serde(default = "default_true")]
    pub deactivate_previous: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivateRevisionResponse {
    pub source_id: String,
    pub active_revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_revision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub history_event_id: Option<String>,
    pub source_document_uri: String,
    pub fragment_uris: Vec<String>,
    pub context_uris: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateStructuredSnapshotRequest {
    pub dataset_key: Option<String>,
    pub owner_user_id: Option<String>,
    pub period_key: Option<String>,
    pub period_start: Option<DateTime<Utc>>,
    pub period_end: Option<DateTime<Utc>>,
    pub granularity: Option<String>,
    pub checksum: Option<String>,
    pub source_ref: Option<SourceRef>,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredSnapshot {
    pub id: String,
    pub dataset_key: String,
    pub owner_user_id: String,
    pub period_key: String,
    pub period_start: DateTime<Utc>,
    pub period_end: DateTime<Utc>,
    pub row_count: usize,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuredSnapshotResponse {
    pub snapshot: StructuredSnapshot,
    pub history_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BulkStructuredRowsRequest {
    #[serde(default)]
    pub rows: Vec<Value>,
    #[serde(default = "default_insert")]
    pub mode: String,
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkStructuredRowsResponse {
    pub snapshot_id: String,
    pub inserted: usize,
    pub duplicates: usize,
    pub invalid: usize,
    pub row_ids: Vec<String>,
    pub history_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContextSearchRequest {
    pub query: Option<String>,
    #[serde(default = "default_auto")]
    pub mode: String,
    pub target_uri: Option<String>,
    #[serde(default)]
    pub filters: Value,
    #[serde(default)]
    pub owner_user_id: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub debug: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextHit {
    pub uri: String,
    pub title: String,
    pub layer: u8,
    pub score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_kind: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retrieval_role: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_document_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_start: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_end: Option<usize>,
    pub snippet: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContextTracebackRequest {
    pub uri: Option<String>,
    #[serde(default)]
    pub owner_user_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextTracebackResponse {
    pub fragment_uri: String,
    pub fragment_title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate: Option<usize>,
    pub source_document_uri: String,
    pub source_id: String,
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_start: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_end: Option<usize>,
    pub source_title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSearchResponse {
    pub trace_id: String,
    pub hits: Vec<ContextHit>,
    pub stages: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContextRevealRequest {
    pub uri: Option<String>,
    pub trace_id: Option<String>,
    pub next_layer: Option<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextRevealResponse {
    pub uri: String,
    pub layer: u8,
    pub content: String,
    pub source_ref: SourceRef,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RagAnswerRequest {
    pub question: Option<String>,
    #[serde(default = "default_auto")]
    pub mode: String,
    pub session_id: Option<String>,
    #[serde(default)]
    pub owner_user_id: Option<String>,
    #[serde(default)]
    pub debug: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Citation {
    pub uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    pub title: String,
    pub quote: String,
    pub score: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagAnswerResponse {
    pub answer_id: String,
    pub trace_id: String,
    pub answer: String,
    pub citations: Vec<Citation>,
    #[serde(default)]
    pub usage: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmStatusResponse {
    pub provider: String,
    pub model: String,
    pub auth_source: String,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ImportCodexAuthRequest {
    pub codex_auth_path: Option<String>,
    #[serde(default)]
    pub store_imported_token: bool,
    #[serde(default)]
    pub test_after_import: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportCodexAuthResponse {
    pub status: String,
    pub auth_source: String,
    pub test_ok: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LlmTestRequest {
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmTestResponse {
    pub ok: bool,
    pub model: String,
    pub latency_ms: u64,
    pub sample: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionCreateRequest {
    pub owner_user_id: Option<String>,
    pub title: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionResponse {
    pub session_id: String,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionMessageRequest {
    pub role: Option<String>,
    pub content: Option<String>,
    #[serde(default)]
    pub write_history_event: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SessionCommitRequest {
    #[serde(default = "default_true")]
    pub extract_insights: bool,
    #[serde(default = "default_true")]
    pub archive_context: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionCommitResponse {
    pub session_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archive_uri: Option<String>,
    pub history_event_ids: Vec<String>,
    pub insight_candidate_ids: Vec<String>,
    pub memory_diff_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextNode {
    pub uri: String,
    pub title: String,
    pub layer: u8,
    pub body: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub index_uid: String,
    pub index_kind: String,
    pub ancestor_uris: Vec<String>,
    #[serde(default = "default_node_kind")]
    pub node_kind: String,
    #[serde(default = "default_retrieval_role")]
    pub retrieval_role: String,
    #[serde(default = "default_true")]
    pub retrieval_enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_document_uri: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_start: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_end: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token_estimate: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revision_id: Option<String>,
    #[serde(default = "default_active")]
    pub status: String,
    #[serde(default = "default_privacy")]
    pub privacy: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceDocument {
    pub id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub source_kind: String,
    pub source_id: String,
    pub revision_id: String,
    pub uri: String,
    pub title: String,
    pub content: String,
    pub checksum: String,
    #[serde(default = "default_active")]
    pub status: String,
    #[serde(default)]
    pub retrieval_enabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceRecord {
    pub id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub query: String,
    pub mode: String,
    pub stages: Vec<Value>,
    pub context_uris: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompanySource {
    pub id: String,
    pub title: String,
    pub canonical_key: String,
    pub source_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_revision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRevision {
    pub id: String,
    pub source_id: String,
    pub title: String,
    pub source_uri: String,
    pub checksum: String,
    pub content: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionRecord {
    pub id: String,
    pub owner_user_id: String,
    pub title: String,
    pub status: String,
    pub messages: Vec<Value>,
    pub created_at: DateTime<Utc>,
}

pub fn default_limit() -> usize {
    10
}

pub fn default_true() -> bool {
    true
}

pub fn default_node_kind() -> String {
    "abstract".to_string()
}

pub fn default_retrieval_role() -> String {
    "overview".to_string()
}

pub fn default_privacy() -> String {
    "private".to_string()
}

pub fn default_promote_policy() -> String {
    "none".to_string()
}

pub fn default_confidence() -> f32 {
    0.7
}

pub fn default_salience() -> f32 {
    0.5
}

pub fn default_merge_policy() -> String {
    "merge".to_string()
}

pub fn default_active() -> String {
    "active".to_string()
}

pub fn default_similarity_threshold() -> f32 {
    0.82
}

pub fn default_llm_none() -> String {
    "none".to_string()
}

pub fn default_insert() -> String {
    "insert".to_string()
}

pub fn default_auto() -> String {
    "auto".to_string()
}

pub fn default_related() -> String {
    "related".to_string()
}

pub fn default_api() -> String {
    "api".to_string()
}

pub fn default_both() -> String {
    "both".to_string()
}

pub fn default_context_limit() -> usize {
    8
}

pub fn default_analysis() -> String {
    "analysis".to_string()
}
