use super::*;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnsureUserEventIndexRequest {
    #[serde(default)]
    pub force_reapply_settings: bool,
    #[serde(default = "default_true")]
    pub create_personal_context_index: bool,
    #[serde(default)]
    pub schema_version: Option<u32>,
}

impl Default for EnsureUserEventIndexRequest {
    fn default() -> Self {
        Self {
            force_reapply_settings: false,
            create_personal_context_index: true,
            schema_version: None,
        }
    }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceMetadata>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceMetadata>,
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
