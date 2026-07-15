use super::*;

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
    #[serde(default)]
    pub tenant_id: String,
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

/// Maximum validated analysis candidates admitted into one durable
/// materialization operation. These are defense-in-depth limits at the store
/// boundary; provider output is expected to be checked before this type is
/// constructed.
pub const MAX_ANALYSIS_MATERIALIZATION_LINKS: usize = 32;
pub const MAX_ANALYSIS_MATERIALIZATION_INSIGHTS: usize = 16;

/// One server-authorized link proposal ready for durable materialization.
///
/// Tenant, owner, creator, privacy, and idempotency are intentionally absent:
/// the server supplies those trust-boundary fields when the batch is staged.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnalysisLinkMaterialization {
    pub source_uri: String,
    pub target_uri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_title: Option<String>,
    pub relation: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rationale: Option<String>,
    pub confidence: f32,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// One server-authorized insight proposal ready for durable materialization.
/// Source URIs are converted to typed context references by the store.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnalysisInsightMaterialization {
    pub insight_type: String,
    pub title: String,
    pub statement: String,
    pub confidence: f32,
    pub salience: f32,
    #[serde(default)]
    pub source_uris: Vec<String>,
}

/// Already-validated analysis output admitted as one immutable mutation plan.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct AnalysisMaterializationRequest {
    #[serde(default)]
    pub links: Vec<AnalysisLinkMaterialization>,
    #[serde(default)]
    pub insights: Vec<AnalysisInsightMaterialization>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnalysisMaterializationResponse {
    pub created_links: Vec<KnowledgeLink>,
    pub insights: Vec<InsightRecord>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceMetadata>,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub persistence: Option<PersistenceMetadata>,
    #[serde(default)]
    pub usage: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}
