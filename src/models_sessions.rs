use super::*;

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
pub struct SessionRecord {
    pub id: String,
    #[serde(default)]
    pub tenant_id: String,
    pub owner_user_id: String,
    pub title: String,
    pub status: String,
    pub messages: Vec<Value>,
    pub created_at: DateTime<Utc>,
}
