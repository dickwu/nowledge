use super::*;

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
pub struct ParseArtifact {
    pub id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub source_document_uri: String,
    pub source_id: String,
    pub revision_id: String,
    pub parser_provider: String,
    pub parser_backend: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parser_version: Option<String>,
    pub artifact_kind: String,
    pub uri: String,
    pub checksum: String,
    pub byte_size: usize,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ParsedBlock {
    pub block_id: String,
    #[serde(rename = "type")]
    pub block_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_idx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub html: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latex: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caption: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub footnote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text_level: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub section_path: Vec<String>,
    #[serde(default)]
    pub reading_order: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct IngestTaskRequest {
    #[serde(default)]
    pub owner_user_id: Option<String>,
    #[serde(default)]
    pub source_id: Option<String>,
    #[serde(default)]
    pub revision_id: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub source_uri: Option<String>,
    #[serde(default)]
    pub source_document_uri: Option<String>,
    #[serde(default)]
    pub content: Option<String>,
    #[serde(default, skip_serializing, skip_deserializing)]
    pub bytes: Option<Vec<u8>>,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub checksum: Option<String>,
    #[serde(default)]
    pub parser_provider: Option<String>,
    #[serde(default)]
    pub parser_backend: Option<String>,
    #[serde(default)]
    pub content_list: Option<Value>,
    #[serde(default)]
    pub content_list_v2: Option<Value>,
    #[serde(default)]
    pub middle_json: Option<Value>,
    #[serde(default)]
    pub model_json: Option<Value>,
    #[serde(default)]
    pub fragment_policy: Option<FragmentPolicy>,
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IngestTask {
    pub task_id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub source_id: String,
    pub revision_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_document_uri: Option<String>,
    pub parser_provider: String,
    pub parser_backend: String,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queued_ahead: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestTaskResult {
    pub task: IngestTask,
    pub source_document_uri: String,
    pub source_id: String,
    pub revision_id: String,
    pub parse_artifacts: Vec<ParseArtifact>,
    pub parsed_blocks: Vec<ParsedBlock>,
    pub fragment_uris: Vec<String>,
    pub context_uris: Vec<String>,
}
