use super::*;

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompanySource {
    pub id: String,
    #[serde(default)]
    pub tenant_id: String,
    pub title: String,
    pub canonical_key: String,
    pub source_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active_revision_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct DeleteSourceReport {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragments_task: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub revisions_task: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_task: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auxiliary_tasks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRevision {
    pub id: String,
    #[serde(default)]
    pub tenant_id: String,
    pub source_id: String,
    pub title: String,
    pub source_uri: String,
    pub checksum: String,
    pub content: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}
