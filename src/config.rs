#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub tenant_id: String,
    pub bearer_token: Option<String>,
    pub admin_token: Option<String>,
    pub index_hash_secret: Vec<u8>,
    pub meili_url: Option<String>,
    pub meili_api_key: Option<String>,
    pub llm_provider: String,
    pub llm_model: Option<String>,
    pub allow_codex_auth_import: bool,
}

impl Config {
    pub fn from_env() -> Self {
        let index_hash_secret = std::env::var("RAG_INDEX_HASH_SECRET")
            .unwrap_or_else(|_| "dev-only-secret-change-me".to_string())
            .into_bytes();

        Self {
            host: std::env::var("RAG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("RAG_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(14242),
            tenant_id: std::env::var("RAG_TENANT_ID").unwrap_or_else(|_| "default".to_string()),
            bearer_token: std::env::var("RAG_BEARER_TOKEN").ok(),
            admin_token: std::env::var("RAG_ADMIN_TOKEN").ok(),
            index_hash_secret,
            meili_url: std::env::var("RAG_MEILI_URL").ok(),
            meili_api_key: std::env::var("RAG_MEILI_API_KEY").ok(),
            llm_provider: std::env::var("RAG_LLM_PROVIDER").unwrap_or_else(|_| "none".to_string()),
            llm_model: std::env::var("RAG_LLM_MODEL").ok(),
            allow_codex_auth_import: std::env::var("RAG_ALLOW_CODEX_AUTH_IMPORT")
                .map(|v| matches!(v.as_str(), "1" | "true" | "TRUE" | "yes"))
                .unwrap_or(false),
        }
    }

    pub fn test() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 0,
            tenant_id: "test-tenant".to_string(),
            bearer_token: None,
            admin_token: None,
            index_hash_secret: b"test-secret".to_vec(),
            meili_url: None,
            meili_api_key: None,
            llm_provider: "none".to_string(),
            llm_model: Some("none".to_string()),
            allow_codex_auth_import: false,
        }
    }
}
