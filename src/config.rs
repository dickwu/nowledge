#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub run_mode: String,
    pub tenant_id: String,
    pub bearer_token: Option<String>,
    pub admin_token: Option<String>,
    pub auth_users: Vec<AuthUserConfig>,
    pub allow_unsafe_unauthenticated: bool,
    pub index_hash_secret: Vec<u8>,
    pub store_backend: String,
    pub meili_url: Option<String>,
    pub meili_api_key: Option<String>,
    pub meili_wait_for_tasks: bool,
    pub llm_provider: String,
    pub llm_model: Option<String>,
    pub openai_api_key: Option<String>,
    pub codex_auth_path: Option<String>,
    pub allow_codex_auth_import: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthUserConfig {
    pub token: String,
    pub owner_user_id: Option<String>,
    pub roles: Vec<String>,
}

impl Config {
    pub fn from_env() -> Self {
        let index_hash_secret = std::env::var("RAG_INDEX_HASH_SECRET")
            .unwrap_or_else(|_| "dev-only-secret-change-me".to_string())
            .into_bytes();
        let run_mode = std::env::var("RAG_RUN_MODE").unwrap_or_else(|_| "development".to_string());

        Self {
            host: std::env::var("RAG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("RAG_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(14242),
            run_mode: run_mode.clone(),
            tenant_id: std::env::var("RAG_TENANT_ID").unwrap_or_else(|_| "default".to_string()),
            bearer_token: std::env::var("RAG_BEARER_TOKEN").ok(),
            admin_token: std::env::var("RAG_ADMIN_TOKEN").ok(),
            auth_users: std::env::var("RAG_AUTH_USERS")
                .map(|value| parse_auth_users(&value))
                .unwrap_or_default(),
            allow_unsafe_unauthenticated: std::env::var("RAG_ALLOW_UNSAFE_UNAUTHENTICATED")
                .map(|v| truthy(&v))
                .unwrap_or_else(|_| run_mode != "production"),
            index_hash_secret,
            store_backend: std::env::var("RAG_STORE_BACKEND")
                .unwrap_or_else(|_| "memory".to_string()),
            meili_url: std::env::var("RAG_MEILI_URL").ok(),
            meili_api_key: std::env::var("RAG_MEILI_API_KEY").ok(),
            meili_wait_for_tasks: std::env::var("RAG_MEILI_WAIT_FOR_TASKS")
                .map(|v| truthy(&v))
                .unwrap_or(false),
            llm_provider: std::env::var("RAG_LLM_PROVIDER").unwrap_or_else(|_| "none".to_string()),
            llm_model: std::env::var("RAG_LLM_MODEL").ok(),
            openai_api_key: std::env::var("RAG_OPENAI_API_KEY")
                .or_else(|_| std::env::var("OPENAI_API_KEY"))
                .ok(),
            codex_auth_path: std::env::var("RAG_CODEX_AUTH_PATH")
                .or_else(|_| std::env::var("CODEX_AUTH_PATH"))
                .ok(),
            allow_codex_auth_import: std::env::var("RAG_ALLOW_CODEX_AUTH_IMPORT")
                .map(|v| truthy(&v))
                .unwrap_or(false),
        }
    }

    pub fn validate_startup(&self) -> anyhow::Result<()> {
        if self.store_backend == "meili" && self.meili_url.is_none() {
            anyhow::bail!("RAG_STORE_BACKEND=meili requires RAG_MEILI_URL");
        }

        if self.run_mode == "production"
            && !self.has_any_auth()
            && !self.allow_unsafe_unauthenticated
        {
            anyhow::bail!(
                "production mode requires RAG_BEARER_TOKEN, RAG_ADMIN_TOKEN, RAG_AUTH_USERS, or explicit RAG_ALLOW_UNSAFE_UNAUTHENTICATED=true"
            );
        }

        Ok(())
    }

    pub fn has_any_auth(&self) -> bool {
        self.bearer_token.is_some() || self.admin_token.is_some() || !self.auth_users.is_empty()
    }

    pub fn test() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 0,
            run_mode: "test".to_string(),
            tenant_id: "test-tenant".to_string(),
            bearer_token: None,
            admin_token: None,
            auth_users: Vec::new(),
            allow_unsafe_unauthenticated: true,
            index_hash_secret: b"test-secret".to_vec(),
            store_backend: "memory".to_string(),
            meili_url: None,
            meili_api_key: None,
            meili_wait_for_tasks: true,
            llm_provider: "none".to_string(),
            llm_model: Some("none".to_string()),
            openai_api_key: None,
            codex_auth_path: None,
            allow_codex_auth_import: false,
        }
    }
}

fn truthy(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
}

fn parse_auth_users(value: &str) -> Vec<AuthUserConfig> {
    value
        .split(',')
        .filter_map(|entry| {
            let mut parts = entry.splitn(3, ':');
            let owner = parts.next()?.trim();
            let token = parts.next()?.trim();
            if owner.is_empty() || token.is_empty() {
                return None;
            }
            let roles = parts
                .next()
                .unwrap_or("user")
                .split('|')
                .map(str::trim)
                .filter(|role| !role.is_empty())
                .map(ToString::to_string)
                .collect();
            Some(AuthUserConfig {
                token: token.to_string(),
                owner_user_id: if owner == "*" {
                    None
                } else {
                    Some(owner.to_string())
                },
                roles,
            })
        })
        .collect()
}
