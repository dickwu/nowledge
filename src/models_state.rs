use super::*;

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
