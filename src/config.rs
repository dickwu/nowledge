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
    pub parser_provider: String,
    pub mineru_api_url: String,
    pub mineru_backend: String,
    pub mineru_return_md: bool,
    pub mineru_return_content_list: bool,
    pub mineru_return_middle_json: bool,
    pub mineru_return_images: bool,
    pub ingest_max_concurrent_tasks: usize,
    pub ingest_task_retention_seconds: u64,
    pub ingest_cleanup_interval_seconds: u64,
    pub ingest_worker_enabled: bool,
    pub llm_provider: String,
    pub llm_model: Option<String>,
    pub analysis_llm_provider: String,
    pub analysis_llm_model: Option<String>,
    pub openai_api_key: Option<String>,
    pub codex_auth_path: Option<String>,
    pub codex_base_url: String,
    pub health_llm_enabled: bool,
    pub health_llm_probe_interval_seconds: u64,
    pub health_llm_probe_ttl_seconds: u64,
    pub health_llm_max_stale_seconds: u64,
    pub health_llm_timeout_ms: u64,
    pub health_require_llm: bool,
    pub health_llm_failure_threshold: u32,
    pub health_llm_rate_limit_unhealthy: bool,
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
            parser_provider: std::env::var("RAG_PARSER_PROVIDER")
                .unwrap_or_else(|_| "builtin".to_string()),
            mineru_api_url: std::env::var("RAG_MINERU_API_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8000".to_string()),
            mineru_backend: std::env::var("RAG_MINERU_BACKEND")
                .unwrap_or_else(|_| "hybrid-auto-engine".to_string()),
            mineru_return_md: std::env::var("RAG_MINERU_RETURN_MD")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            mineru_return_content_list: std::env::var("RAG_MINERU_RETURN_CONTENT_LIST")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            mineru_return_middle_json: std::env::var("RAG_MINERU_RETURN_MIDDLE_JSON")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            mineru_return_images: std::env::var("RAG_MINERU_RETURN_IMAGES")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            ingest_max_concurrent_tasks: std::env::var("RAG_INGEST_MAX_CONCURRENT_TASKS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2),
            ingest_task_retention_seconds: std::env::var("RAG_INGEST_TASK_RETENTION_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(86_400),
            ingest_cleanup_interval_seconds: std::env::var("RAG_INGEST_CLEANUP_INTERVAL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300),
            ingest_worker_enabled: std::env::var("RAG_INGEST_WORKER_ENABLED")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            llm_provider: std::env::var("RAG_LLM_PROVIDER").unwrap_or_else(|_| "none".to_string()),
            llm_model: std::env::var("RAG_LLM_MODEL").ok(),
            analysis_llm_provider: std::env::var("RAG_ANALYSIS_LLM_PROVIDER").unwrap_or_else(
                |_| std::env::var("RAG_LLM_PROVIDER").unwrap_or_else(|_| "none".to_string()),
            ),
            analysis_llm_model: std::env::var("RAG_ANALYSIS_LLM_MODEL")
                .ok()
                .or_else(|| std::env::var("RAG_LLM_MODEL").ok()),
            openai_api_key: std::env::var("RAG_OPENAI_API_KEY")
                .or_else(|_| std::env::var("OPENAI_API_KEY"))
                .ok(),
            codex_auth_path: std::env::var("RAG_CODEX_AUTH_PATH")
                .or_else(|_| std::env::var("CODEX_AUTH_PATH"))
                .ok(),
            codex_base_url: std::env::var("RAG_CODEX_BASE_URL")
                .or_else(|_| std::env::var("OPENVIKING_CODEX_BASE_URL"))
                .unwrap_or_else(|_| "https://chatgpt.com/backend-api/codex".to_string()),
            health_llm_enabled: std::env::var("RAG_HEALTH_LLM_ENABLED")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            health_llm_probe_interval_seconds: std::env::var(
                "RAG_HEALTH_LLM_PROBE_INTERVAL_SECONDS",
            )
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(30),
            health_llm_probe_ttl_seconds: std::env::var("RAG_HEALTH_LLM_PROBE_TTL_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(60),
            health_llm_max_stale_seconds: std::env::var("RAG_HEALTH_LLM_MAX_STALE_SECONDS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(120),
            health_llm_timeout_ms: std::env::var("RAG_HEALTH_LLM_TIMEOUT_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
            health_require_llm: std::env::var("RAG_HEALTH_REQUIRE_LLM")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            health_llm_failure_threshold: std::env::var("RAG_HEALTH_LLM_FAILURE_THRESHOLD")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(3),
            health_llm_rate_limit_unhealthy: std::env::var("RAG_HEALTH_LLM_RATE_LIMIT_UNHEALTHY")
                .map(|v| truthy(&v))
                .unwrap_or(false),
        }
    }

    pub fn analysis_llm_config(&self) -> Self {
        let mut config = self.clone();
        config.llm_provider = self.analysis_llm_provider.clone();
        config.llm_model = self.analysis_llm_model.clone();
        config
    }

    pub fn validate_startup(&self) -> anyhow::Result<()> {
        if self.store_backend == "meili" && self.meili_url.is_none() {
            anyhow::bail!("RAG_STORE_BACKEND=meili requires RAG_MEILI_URL");
        }
        if !matches!(self.parser_provider.as_str(), "builtin" | "mineru") {
            anyhow::bail!("RAG_PARSER_PROVIDER must be builtin or mineru");
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
            parser_provider: "builtin".to_string(),
            mineru_api_url: "http://127.0.0.1:8000".to_string(),
            mineru_backend: "hybrid-auto-engine".to_string(),
            mineru_return_md: true,
            mineru_return_content_list: true,
            mineru_return_middle_json: true,
            mineru_return_images: true,
            ingest_max_concurrent_tasks: 2,
            ingest_task_retention_seconds: 86_400,
            ingest_cleanup_interval_seconds: 300,
            ingest_worker_enabled: true,
            llm_provider: "none".to_string(),
            llm_model: Some("none".to_string()),
            analysis_llm_provider: "none".to_string(),
            analysis_llm_model: Some("none".to_string()),
            openai_api_key: None,
            codex_auth_path: None,
            codex_base_url: "https://chatgpt.com/backend-api/codex".to_string(),
            health_llm_enabled: true,
            health_llm_probe_interval_seconds: 30,
            health_llm_probe_ttl_seconds: 60,
            health_llm_max_stale_seconds: 120,
            health_llm_timeout_ms: 10_000,
            health_require_llm: true,
            health_llm_failure_threshold: 3,
            health_llm_rate_limit_unhealthy: false,
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
