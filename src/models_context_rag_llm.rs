use super::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContextSearchRequest {
    pub query: Option<String>,
    #[serde(default = "default_auto")]
    pub mode: String,
    pub target_uri: Option<String>,
    #[serde(default)]
    pub filters: Value,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default = "default_return_profile")]
    pub return_profile: String,
    #[serde(default)]
    pub owner_user_id: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub debug: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ContextStructuredFilters {
    pub source_id: Option<String>,
    pub revision_id: Option<String>,
    pub source_document_uri: Option<String>,
    pub block_type: Option<String>,
    pub page_idx: Option<u32>,
    pub page_idx_gte: Option<u32>,
    pub page_idx_lte: Option<u32>,
    pub section_path_contains: Option<String>,
    pub artifact_kind: Option<String>,
}

impl ContextStructuredFilters {
    pub fn requires_post_filter(&self) -> bool {
        self.section_path_contains.is_some() || self.artifact_kind.is_some()
    }

    pub fn matches_node(&self, node: &ContextNode) -> bool {
        self.source_id
            .as_deref()
            .is_none_or(|source_id| node.source_id.as_deref() == Some(source_id))
            && self
                .revision_id
                .as_deref()
                .is_none_or(|revision_id| node.revision_id.as_deref() == Some(revision_id))
            && self
                .source_document_uri
                .as_deref()
                .is_none_or(|uri| node.source_document_uri.as_deref() == Some(uri))
            && self
                .block_type
                .as_deref()
                .is_none_or(|block_type| node.block_type.as_deref() == Some(block_type))
            && self
                .page_idx
                .is_none_or(|page_idx| node.page_idx == Some(page_idx))
            && self
                .page_idx_gte
                .is_none_or(|min| node.page_idx.is_some_and(|page_idx| page_idx >= min))
            && self
                .page_idx_lte
                .is_none_or(|max| node.page_idx.is_some_and(|page_idx| page_idx <= max))
            && self.section_path_contains.as_deref().is_none_or(|needle| {
                let needle = needle.to_ascii_lowercase();
                node.section_path
                    .iter()
                    .any(|part| part.to_ascii_lowercase().contains(&needle))
            })
            && self.artifact_kind.as_deref().is_none_or(|artifact_kind| {
                node.artifact_refs
                    .iter()
                    .any(|artifact| artifact.artifact_kind == artifact_kind)
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextRelatedLink {
    pub id: String,
    pub source_uri: String,
    pub target_uri: String,
    pub relation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_title: Option<String>,
    pub confidence: f32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSourceSummary {
    pub source_document_uri: String,
    pub source_id: String,
    pub revision_id: String,
    pub source_title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextNeighborFragment {
    pub uri: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_idx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_type: Option<String>,
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
    pub source_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_relation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_start: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_end: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_idx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub section_path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heading_level: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asset_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<ParseArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_summary: Option<ContextSourceSummary>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub neighbor_fragments: Vec<ContextNeighborFragment>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub related_links: Vec<ContextRelatedLink>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub score_breakdown: Option<Value>,
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
    pub page_idx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_type: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub section_path: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asset_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<ParseArtifactRef>,
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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub groups: Vec<ContextSourceGroup>,
    pub stages: Vec<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextPageRange {
    pub start: u32,
    pub end: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContextSourceGroup {
    pub source_document_uri: String,
    pub source_id: String,
    pub revision_id: String,
    pub source_title: String,
    pub top_score: f32,
    pub hit_count: usize,
    pub page_range: Option<ContextPageRange>,
    pub block_types: Vec<String>,
    pub top_hit_uri: String,
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
    pub source_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_idx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub section_path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heading_level: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asset_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<ParseArtifactRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fragment_index: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_start: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub char_end: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum: Option<String>,
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
pub struct LlmTestRequest {
    pub prompt: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmTestResponse {
    pub ok: bool,
    pub model: String,
    pub latency_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<crate::llm::LlmTokenUsage>,
    pub sample: String,
}

/// Ask the LLM to summarize document content into a short title.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LlmTitleRequest {
    pub content: String,
    /// Soft cap on the returned title; clamped to [20, 200], default 80.
    #[serde(default)]
    pub max_chars: Option<usize>,
    /// Optional language hint (e.g. "English", "Simplified Chinese"). When
    /// None the model is asked to match the input language.
    #[serde(default)]
    pub language: Option<String>,
    /// Optional user-supplied draft / keywords the model should incorporate.
    #[serde(default)]
    pub hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmTitleResponse {
    pub title: String,
    pub model: String,
    pub latency_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<crate::llm::LlmTokenUsage>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_type: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_idx: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bbox: Option<Value>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub section_path: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heading_level: Option<u8>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub asset_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifact_refs: Vec<ParseArtifactRef>,
    #[serde(default = "default_active")]
    pub status: String,
    #[serde(default = "default_privacy")]
    pub privacy: String,
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
