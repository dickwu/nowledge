use super::*;

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
    #[serde(default)]
    pub tenant_id: String,
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
    #[serde(default)]
    pub tenant_id: String,
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
