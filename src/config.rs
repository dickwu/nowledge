use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

const DEVELOPMENT_INDEX_HASH_SECRET: &str = "dev-only-secret-change-me";
const DOCUMENTED_LEGACY_INDEX_HASH_SECRET: &str = "change-me";
const REJECTED_INDEX_HASH_SECRET_PLACEHOLDER: &str = "replace-with-at-least-32-random-bytes";
const MIN_AUTH_CREDENTIAL_CHARS: usize = 8;
const MIN_REDACTION_SECRET_CHARS: usize = 4;
const CODEX_SECRET_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const MIN_PRODUCTION_INDEX_HASH_SECRET_BYTES: usize = 32;
const MIN_PRODUCTION_INDEX_HASH_SECRET_DISTINCT_BYTES: usize = 12;
const DEFAULT_MAX_JSON_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_MAX_UPLOAD_BYTES: usize = 50 * 1024 * 1024;
const DEFAULT_MAX_MULTIPART_FIELDS: usize = 32;
const MULTIPART_FRAMING_ALLOWANCE_PER_FIELD_BYTES: usize = 16 * 1024;
const DEFAULT_MAX_BULK_EVENTS: usize = 500;
const DEFAULT_MAX_BULK_ROWS: usize = 5_000;
const DEFAULT_MAX_SEARCH_LIMIT: usize = 100;
pub(crate) const DEFAULT_MEILI_SCAN_PAGE_SIZE: usize = 500;
pub(crate) const DEFAULT_MEILI_SCAN_MAX_DOCUMENTS: usize = 100_000;
const MAX_MEILI_SCAN_PAGE_SIZE: usize = 1_000;
const DEFAULT_MAX_TAGS_PER_ITEM: usize = 64;
const DEFAULT_MAX_TAG_BYTES: usize = 128;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const DEFAULT_SYNC_INGEST_TIMEOUT_MS: u64 = 120_000;
const DEFAULT_MAX_IN_FLIGHT_REQUESTS: usize = 256;
const DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE: u64 = 600;
const DEFAULT_INGEST_MAX_CONCURRENT_TASKS: usize = 2;
const DEFAULT_INGEST_QUEUE_MULTIPLIER: usize = 8;
const DEFAULT_SHUTDOWN_TIMEOUT_MS: u64 = 30_000;
const MAX_BOUNDARY_DEADLINE_MS: u64 = 7 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteConsistency {
    Eventual,
    ReadYourWrites,
    WaitForIndex,
}

impl WriteConsistency {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Eventual => "eventual",
            Self::ReadYourWrites => "read_your_writes",
            Self::WaitForIndex => "wait_for_index",
        }
    }
}

impl std::str::FromStr for WriteConsistency {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "eventual" => Ok(Self::Eventual),
            "read_your_writes" => Ok(Self::ReadYourWrites),
            "wait_for_index" => Ok(Self::WaitForIndex),
            _ => Err("RAG_WRITE_CONSISTENCY must be eventual, read_your_writes, or wait_for_index"),
        }
    }
}

#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub run_mode: String,
    pub tenant_id: String,
    pub bearer_token: Option<String>,
    pub bearer_token_scope: Option<BearerTokenScope>,
    pub bearer_token_owner_user_id: Option<String>,
    pub allow_legacy_tenant_service_bearer: bool,
    pub allow_legacy_shared_writer: bool,
    pub allow_legacy_weak_index_hash_secret: bool,
    pub admin_token: Option<String>,
    pub auth_users: Vec<AuthUserConfig>,
    pub allow_unsafe_unauthenticated: bool,
    pub index_hash_secret: Vec<u8>,
    pub store_backend: String,
    pub meili_url: Option<String>,
    pub meili_api_key: Option<String>,
    pub write_consistency: WriteConsistency,
    /// Compatibility projection for repository code that has not yet moved to
    /// the typed write-consistency policy.
    pub meili_wait_for_tasks: bool,
    pub meili_scan_page_size: usize,
    pub meili_scan_max_documents: usize,
    pub parser_provider: String,
    pub mineru_api_url: String,
    pub mineru_backend: String,
    pub mineru_return_md: bool,
    pub mineru_return_content_list: bool,
    pub mineru_return_middle_json: bool,
    pub mineru_return_images: bool,
    pub max_json_bytes: usize,
    pub max_upload_bytes: usize,
    pub max_multipart_fields: usize,
    pub max_bulk_events: usize,
    pub max_bulk_rows: usize,
    pub max_search_limit: usize,
    pub max_tags_per_item: usize,
    pub max_tag_bytes: usize,
    pub upload_allowed_mime_types: Vec<String>,
    pub request_timeout_ms: u64,
    pub sync_ingest_timeout_ms: u64,
    pub max_in_flight_requests: usize,
    pub rate_limit_requests_per_minute: u64,
    pub ingest_queue_capacity: usize,
    pub shutdown_timeout_ms: u64,
    pub cors_allowed_origins: Vec<String>,
    pub allow_wildcard_cors: bool,
    pub ingest_max_concurrent_tasks: usize,
    pub ingest_task_retention_seconds: u64,
    pub ingest_cleanup_interval_seconds: u64,
    pub ingest_worker_enabled: bool,
    pub llm_provider: String,
    pub llm_model: Option<String>,
    pub llm_reasoning_effort: Option<String>,
    pub analysis_llm_provider: String,
    pub analysis_llm_model: Option<String>,
    pub analysis_llm_reasoning_effort: Option<String>,
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
    pub vector_match_enabled: bool,
    pub vector_match_weight: f32,
    pub vector_match_doc_weight: f32,
    pub vector_match_min_score: f32,
    auth_config_error: Option<String>,
    boundary_config_error: Option<String>,
    previous_redaction_secrets: Vec<String>,
    codex_secret_inventory: Arc<Mutex<CodexSecretInventory>>,
    codex_secret_refresh: Arc<Mutex<CodexSecretRefreshState>>,
    codex_secret_refresh_task_started: Arc<AtomicBool>,
}

#[derive(Default)]
struct CodexSecretInventory {
    history: Vec<String>,
    active_credentials: Option<crate::llm::CodexAuthCredentials>,
}

#[derive(Default)]
struct CodexSecretRefreshState {
    last_refresh: Option<Instant>,
    in_progress: bool,
}

pub(crate) struct ProviderSecuritySnapshot {
    pub credentials: Option<crate::llm::CodexAuthCredentials>,
    pub secrets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthUserConfig {
    pub token: String,
    pub scope: AuthUserScope,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthUserScope {
    Owner { owner_user_id: String },
    TenantService,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BearerTokenScope {
    Owner,
    TenantService,
}

impl Config {
    pub fn from_env() -> Self {
        let index_hash_secret = std::env::var("RAG_INDEX_HASH_SECRET")
            .unwrap_or_else(|_| DEVELOPMENT_INDEX_HASH_SECRET.to_string())
            .into_bytes();
        let run_mode = std::env::var("RAG_RUN_MODE").unwrap_or_else(|_| "development".to_string());
        let mut auth_errors = Vec::new();
        let auth_users = match std::env::var("RAG_AUTH_USERS") {
            Ok(value) => match parse_auth_users(&value) {
                Ok(users) => users,
                Err(error) => {
                    auth_errors.push(error.to_string());
                    Vec::new()
                }
            },
            Err(_) => Vec::new(),
        };
        let bearer_token_scope = match std::env::var("RAG_BEARER_TOKEN_SCOPE") {
            Ok(value) => match parse_bearer_token_scope(&value) {
                Ok(scope) => Some(scope),
                Err(error) => {
                    auth_errors.push(error.to_string());
                    None
                }
            },
            Err(_) => None,
        };
        let mut boundary_errors = Vec::new();
        let ingest_max_concurrent_tasks = parse_env_number(
            "RAG_INGEST_MAX_CONCURRENT_TASKS",
            DEFAULT_INGEST_MAX_CONCURRENT_TASKS,
            &mut boundary_errors,
        );
        let default_ingest_queue_capacity = ingest_max_concurrent_tasks
            .checked_mul(DEFAULT_INGEST_QUEUE_MULTIPLIER)
            .unwrap_or_else(|| {
                boundary_errors.push(
                    "RAG_INGEST_MAX_CONCURRENT_TASKS is too large to derive RAG_INGEST_QUEUE_CAPACITY"
                        .to_string(),
                );
                DEFAULT_INGEST_MAX_CONCURRENT_TASKS * DEFAULT_INGEST_QUEUE_MULTIPLIER
            });
        let max_json_bytes = parse_env_number(
            "RAG_MAX_JSON_BYTES",
            DEFAULT_MAX_JSON_BYTES,
            &mut boundary_errors,
        );
        let max_upload_bytes = parse_env_number(
            "RAG_MAX_UPLOAD_BYTES",
            DEFAULT_MAX_UPLOAD_BYTES,
            &mut boundary_errors,
        );
        let cors_allowed_origins = std::env::var("RAG_CORS_ALLOWED_ORIGINS")
            .map(|value| parse_cors_allowed_origins(&value))
            .unwrap_or_else(|_| default_cors_allowed_origins(&run_mode));
        let upload_allowed_mime_types = std::env::var("RAG_UPLOAD_ALLOWED_MIME_TYPES")
            .map(|value| parse_csv_values(&value))
            .unwrap_or_else(|_| default_upload_allowed_mime_types());
        let configured_write_consistency = std::env::var("RAG_WRITE_CONSISTENCY").ok();
        let legacy_wait_for_tasks = std::env::var("RAG_MEILI_WAIT_FOR_TASKS").ok();
        let write_consistency = resolve_write_consistency(
            &run_mode,
            configured_write_consistency.as_deref(),
            legacy_wait_for_tasks.as_deref(),
        )
        .unwrap_or_else(|error| {
            boundary_errors.push(error);
            default_write_consistency(&run_mode)
        });
        if configured_write_consistency.is_none() && legacy_wait_for_tasks.is_some() {
            tracing::warn!(
                removal_date = "2026-10-01",
                removal_version = "v0.13.0",
                "RAG_MEILI_WAIT_FOR_TASKS is deprecated; set RAG_WRITE_CONSISTENCY instead"
            );
        }
        let meili_wait_for_tasks = write_consistency == WriteConsistency::WaitForIndex;

        let config = Self {
            host: std::env::var("RAG_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            port: std::env::var("RAG_PORT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(14242),
            run_mode: run_mode.clone(),
            tenant_id: std::env::var("RAG_TENANT_ID").unwrap_or_else(|_| "default".to_string()),
            bearer_token: std::env::var("RAG_BEARER_TOKEN").ok(),
            bearer_token_scope,
            bearer_token_owner_user_id: std::env::var("RAG_BEARER_TOKEN_OWNER_USER_ID").ok(),
            allow_legacy_tenant_service_bearer: std::env::var(
                "RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER",
            )
            .map(|v| truthy(&v))
            .unwrap_or(false),
            allow_legacy_shared_writer: std::env::var("RAG_ALLOW_LEGACY_SHARED_WRITER")
                .map(|v| truthy(&v))
                .unwrap_or(false),
            allow_legacy_weak_index_hash_secret: std::env::var(
                "RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET",
            )
            .map(|v| truthy(&v))
            .unwrap_or(false),
            admin_token: std::env::var("RAG_ADMIN_TOKEN").ok(),
            auth_users,
            allow_unsafe_unauthenticated: std::env::var("RAG_ALLOW_UNSAFE_UNAUTHENTICATED")
                .map(|v| truthy(&v))
                .unwrap_or_else(|_| default_allow_unsafe_unauthenticated(&run_mode)),
            index_hash_secret,
            store_backend: std::env::var("RAG_STORE_BACKEND")
                .unwrap_or_else(|_| "memory".to_string()),
            meili_url: std::env::var("RAG_MEILI_URL").ok(),
            meili_api_key: std::env::var("RAG_MEILI_API_KEY").ok(),
            write_consistency,
            meili_wait_for_tasks,
            meili_scan_page_size: parse_env_number(
                "RAG_MEILI_SCAN_PAGE_SIZE",
                DEFAULT_MEILI_SCAN_PAGE_SIZE,
                &mut boundary_errors,
            ),
            meili_scan_max_documents: parse_env_number(
                "RAG_MEILI_SCAN_MAX_DOCUMENTS",
                DEFAULT_MEILI_SCAN_MAX_DOCUMENTS,
                &mut boundary_errors,
            ),
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
            max_json_bytes,
            max_upload_bytes,
            max_multipart_fields: parse_env_number(
                "RAG_MAX_MULTIPART_FIELDS",
                DEFAULT_MAX_MULTIPART_FIELDS,
                &mut boundary_errors,
            ),
            max_bulk_events: parse_env_number(
                "RAG_MAX_BULK_EVENTS",
                DEFAULT_MAX_BULK_EVENTS,
                &mut boundary_errors,
            ),
            max_bulk_rows: parse_env_number(
                "RAG_MAX_BULK_ROWS",
                DEFAULT_MAX_BULK_ROWS,
                &mut boundary_errors,
            ),
            max_search_limit: parse_env_number(
                "RAG_MAX_SEARCH_LIMIT",
                DEFAULT_MAX_SEARCH_LIMIT,
                &mut boundary_errors,
            ),
            max_tags_per_item: parse_env_number(
                "RAG_MAX_TAGS_PER_ITEM",
                DEFAULT_MAX_TAGS_PER_ITEM,
                &mut boundary_errors,
            ),
            max_tag_bytes: parse_env_number(
                "RAG_MAX_TAG_BYTES",
                DEFAULT_MAX_TAG_BYTES,
                &mut boundary_errors,
            ),
            upload_allowed_mime_types,
            request_timeout_ms: parse_env_number(
                "RAG_REQUEST_TIMEOUT_MS",
                DEFAULT_REQUEST_TIMEOUT_MS,
                &mut boundary_errors,
            ),
            sync_ingest_timeout_ms: parse_env_number(
                "RAG_SYNC_INGEST_TIMEOUT_MS",
                DEFAULT_SYNC_INGEST_TIMEOUT_MS,
                &mut boundary_errors,
            ),
            max_in_flight_requests: parse_env_number(
                "RAG_MAX_IN_FLIGHT_REQUESTS",
                DEFAULT_MAX_IN_FLIGHT_REQUESTS,
                &mut boundary_errors,
            ),
            rate_limit_requests_per_minute: parse_env_number(
                "RAG_RATE_LIMIT_REQUESTS_PER_MINUTE",
                DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE,
                &mut boundary_errors,
            ),
            ingest_queue_capacity: parse_env_number(
                "RAG_INGEST_QUEUE_CAPACITY",
                default_ingest_queue_capacity,
                &mut boundary_errors,
            ),
            shutdown_timeout_ms: parse_env_number(
                "RAG_SHUTDOWN_TIMEOUT_MS",
                DEFAULT_SHUTDOWN_TIMEOUT_MS,
                &mut boundary_errors,
            ),
            cors_allowed_origins,
            allow_wildcard_cors: std::env::var("RAG_ALLOW_WILDCARD_CORS")
                .map(|v| truthy(&v))
                .unwrap_or(false),
            ingest_max_concurrent_tasks,
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
            llm_reasoning_effort: std::env::var("RAG_LLM_REASONING_EFFORT").ok(),
            analysis_llm_provider: std::env::var("RAG_ANALYSIS_LLM_PROVIDER").unwrap_or_else(
                |_| std::env::var("RAG_LLM_PROVIDER").unwrap_or_else(|_| "none".to_string()),
            ),
            analysis_llm_model: std::env::var("RAG_ANALYSIS_LLM_MODEL")
                .ok()
                .or_else(|| std::env::var("RAG_LLM_MODEL").ok()),
            analysis_llm_reasoning_effort: std::env::var("RAG_ANALYSIS_LLM_REASONING_EFFORT")
                .ok()
                .or_else(|| std::env::var("RAG_LLM_REASONING_EFFORT").ok()),
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
            vector_match_enabled: std::env::var("RAG_VECTOR_MATCH_ENABLED")
                .map(|v| truthy(&v))
                .unwrap_or(true),
            vector_match_weight: std::env::var("RAG_VECTOR_MATCH_WEIGHT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(4.0),
            vector_match_doc_weight: std::env::var("RAG_VECTOR_MATCH_DOC_WEIGHT")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(2.0),
            vector_match_min_score: std::env::var("RAG_VECTOR_MATCH_MIN_SCORE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(0.25),
            auth_config_error: (!auth_errors.is_empty()).then(|| auth_errors.join("; ")),
            boundary_config_error: (!boundary_errors.is_empty())
                .then(|| boundary_errors.join("; ")),
            previous_redaction_secrets: std::env::var("RAG_REDACTION_PREVIOUS_SECRETS")
                .map(|value| parse_previous_redaction_secrets(&value))
                .unwrap_or_default(),
            codex_secret_inventory: Arc::new(Mutex::new(CodexSecretInventory::default())),
            codex_secret_refresh: Arc::new(Mutex::new(CodexSecretRefreshState::default())),
            codex_secret_refresh_task_started: Arc::new(AtomicBool::new(false)),
        };
        config.observe_codex_auth_token(true);
        config
    }

    pub fn analysis_llm_config(&self) -> Self {
        let mut config = self.clone();
        config.llm_provider = self.analysis_llm_provider.clone();
        config.llm_model = self.analysis_llm_model.clone();
        config.llm_reasoning_effort = self.analysis_llm_reasoning_effort.clone();
        config
    }

    pub fn configured_secret_values(&self) -> Vec<String> {
        self.cached_configured_secret_values()
    }

    pub fn cached_configured_secret_values(&self) -> Vec<String> {
        let history = self.codex_secret_history();
        self.configured_secret_values_with_history(&history)
    }

    fn configured_secret_values_with_history(&self, history: &[String]) -> Vec<String> {
        let mut secrets = Vec::new();
        if let Some(token) = &self.bearer_token {
            secrets.push(token.clone());
        }
        if let Some(token) = &self.admin_token {
            secrets.push(token.clone());
        }
        secrets.extend(self.auth_users.iter().map(|user| user.token.clone()));
        if let Some(key) = &self.meili_api_key {
            secrets.push(key.clone());
        }
        if let Some(key) = &self.openai_api_key {
            secrets.push(key.clone());
        }
        if let Ok(secret) = std::str::from_utf8(&self.index_hash_secret) {
            secrets.push(secret.to_string());
        }
        secrets.extend(self.previous_redaction_secrets.iter().cloned());
        secrets.extend(history.iter().cloned());
        secrets
    }

    /// Force a blocking dynamic credential refresh during startup, tests, or
    /// an operator-managed rotation hook. Request paths consume only the shared
    /// cached snapshot; the application refresh task performs ordinary reads
    /// once per second on Tokio's blocking pool.
    pub fn refresh_configured_secret_values(&self) -> Vec<String> {
        self.observe_codex_auth_token(true);
        self.cached_configured_secret_values()
    }

    pub(crate) fn codex_auth_credentials(&self) -> Option<crate::llm::CodexAuthCredentials> {
        self.codex_secret_inventory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .active_credentials
            .clone()
    }

    pub(crate) fn provider_security_snapshot(&self) -> ProviderSecuritySnapshot {
        let inventory = self
            .codex_secret_inventory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ProviderSecuritySnapshot {
            credentials: inventory.active_credentials.clone(),
            secrets: self.configured_secret_values_with_history(&inventory.history),
        }
    }

    pub(crate) fn start_codex_secret_refresh_task(self: &Arc<Self>) {
        if self.codex_auth_path.is_none()
            || self
                .codex_secret_refresh_task_started
                .swap(true, Ordering::AcqRel)
        {
            return;
        }

        let config = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(CODEX_SECRET_REFRESH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Config::from_env and Store::new capture the startup credential.
            // Delay the first background filesystem read by one full interval.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(config) = config.upgrade() else {
                    break;
                };
                let refresh_config = config.clone();
                if tokio::task::spawn_blocking(move || {
                    refresh_config.observe_codex_auth_token(false);
                })
                .await
                .is_err()
                {
                    config
                        .codex_secret_refresh_task_started
                        .store(false, Ordering::Release);
                    break;
                }
            }
        });
    }

    fn observe_codex_auth_token(&self, force: bool) {
        let Some(path) = self.codex_auth_path.as_deref() else {
            return;
        };
        let now = Instant::now();
        {
            let mut refresh = self
                .codex_secret_refresh
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if refresh.in_progress
                || (!force
                    && refresh.last_refresh.is_some_and(|last| {
                        now.duration_since(last) < CODEX_SECRET_REFRESH_INTERVAL
                    }))
            {
                return;
            }
            refresh.in_progress = true;
        }

        if let Some(credentials) = crate::llm::read_codex_auth_credentials(path).filter(|value| {
            !value.token.trim().is_empty()
                && value.token == value.token.trim()
                && value.token.chars().count() >= MIN_REDACTION_SECRET_CHARS
        }) {
            self.publish_codex_credentials(credentials);
        }
        let mut refresh = self
            .codex_secret_refresh
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        refresh.last_refresh = Some(now);
        refresh.in_progress = false;
    }

    fn codex_secret_history(&self) -> Vec<String> {
        self.codex_secret_inventory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .history
            .clone()
    }

    fn publish_codex_credentials(&self, credentials: crate::llm::CodexAuthCredentials) {
        let mut inventory = self
            .codex_secret_inventory
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !inventory
            .history
            .iter()
            .any(|known| known == &credentials.token)
        {
            inventory.history.push(credentials.token.clone());
        }
        inventory.active_credentials = Some(credentials);
    }

    pub fn validate_startup(&self) -> anyhow::Result<()> {
        if !matches!(
            self.run_mode.as_str(),
            "development" | "test" | "production"
        ) {
            anyhow::bail!("RAG_RUN_MODE must be development, test, or production");
        }
        if self.run_mode == "production" && self.write_consistency == WriteConsistency::Eventual {
            anyhow::bail!(
                "RAG_WRITE_CONSISTENCY=eventual is not allowed in production; use read_your_writes or wait_for_index"
            );
        }
        if self.tenant_id.trim().is_empty() {
            anyhow::bail!("RAG_TENANT_ID must be non-empty");
        }
        if self.store_backend == "meili" && self.meili_url.is_none() {
            anyhow::bail!("RAG_STORE_BACKEND=meili requires RAG_MEILI_URL");
        }
        if !matches!(self.parser_provider.as_str(), "builtin" | "mineru") {
            anyhow::bail!("RAG_PARSER_PROVIDER must be builtin or mineru");
        }

        self.validate_http_boundaries()?;
        self.validate_index_hash_secret()?;

        self.validate_auth_configuration()?;
        self.validate_redaction_secret_sources()?;

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

    pub fn max_multipart_body_bytes(&self) -> Option<usize> {
        let framing_allowance = self
            .max_multipart_fields
            .checked_mul(MULTIPART_FRAMING_ALLOWANCE_PER_FIELD_BYTES)?;
        self.max_upload_bytes
            .checked_add(self.max_json_bytes)?
            .checked_add(framing_allowance)
    }

    fn validate_http_boundaries(&self) -> anyhow::Result<()> {
        if let Some(error) = &self.boundary_config_error {
            anyhow::bail!("invalid boundary configuration: {error}");
        }

        for (name, value) in [
            ("RAG_MAX_JSON_BYTES", self.max_json_bytes as u128),
            ("RAG_MAX_UPLOAD_BYTES", self.max_upload_bytes as u128),
            (
                "RAG_MAX_MULTIPART_FIELDS",
                self.max_multipart_fields as u128,
            ),
            ("RAG_MAX_BULK_EVENTS", self.max_bulk_events as u128),
            ("RAG_MAX_BULK_ROWS", self.max_bulk_rows as u128),
            ("RAG_MAX_SEARCH_LIMIT", self.max_search_limit as u128),
            (
                "RAG_MEILI_SCAN_PAGE_SIZE",
                self.meili_scan_page_size as u128,
            ),
            (
                "RAG_MEILI_SCAN_MAX_DOCUMENTS",
                self.meili_scan_max_documents as u128,
            ),
            ("RAG_MAX_TAGS_PER_ITEM", self.max_tags_per_item as u128),
            ("RAG_MAX_TAG_BYTES", self.max_tag_bytes as u128),
            ("RAG_REQUEST_TIMEOUT_MS", self.request_timeout_ms as u128),
            (
                "RAG_SYNC_INGEST_TIMEOUT_MS",
                self.sync_ingest_timeout_ms as u128,
            ),
            (
                "RAG_MAX_IN_FLIGHT_REQUESTS",
                self.max_in_flight_requests as u128,
            ),
            (
                "RAG_RATE_LIMIT_REQUESTS_PER_MINUTE",
                self.rate_limit_requests_per_minute as u128,
            ),
            (
                "RAG_INGEST_MAX_CONCURRENT_TASKS",
                self.ingest_max_concurrent_tasks as u128,
            ),
            (
                "RAG_INGEST_QUEUE_CAPACITY",
                self.ingest_queue_capacity as u128,
            ),
            ("RAG_SHUTDOWN_TIMEOUT_MS", self.shutdown_timeout_ms as u128),
        ] {
            if value == 0 {
                anyhow::bail!("{name} must be greater than zero");
            }
        }

        let now = Instant::now();
        for (name, value) in [
            ("RAG_REQUEST_TIMEOUT_MS", self.request_timeout_ms),
            ("RAG_SYNC_INGEST_TIMEOUT_MS", self.sync_ingest_timeout_ms),
            ("RAG_SHUTDOWN_TIMEOUT_MS", self.shutdown_timeout_ms),
        ] {
            if value > MAX_BOUNDARY_DEADLINE_MS {
                anyhow::bail!("{name} must not exceed {MAX_BOUNDARY_DEADLINE_MS} milliseconds");
            }
            if now.checked_add(Duration::from_millis(value)).is_none() {
                anyhow::bail!("{name} is too large for platform deadline arithmetic");
            }
        }
        if self.sync_ingest_timeout_ms < self.request_timeout_ms {
            anyhow::bail!(
                "RAG_SYNC_INGEST_TIMEOUT_MS must be greater than or equal to RAG_REQUEST_TIMEOUT_MS"
            );
        }
        if self.meili_scan_page_size > MAX_MEILI_SCAN_PAGE_SIZE {
            anyhow::bail!("RAG_MEILI_SCAN_PAGE_SIZE must not exceed {MAX_MEILI_SCAN_PAGE_SIZE}");
        }
        if self.meili_scan_page_size > self.meili_scan_max_documents {
            anyhow::bail!("RAG_MEILI_SCAN_PAGE_SIZE must not exceed RAG_MEILI_SCAN_MAX_DOCUMENTS");
        }
        if self.max_multipart_body_bytes().is_none() {
            anyhow::bail!(
                "multipart upload, metadata, and framing limits exceed the platform size limit"
            );
        }
        for (name, value) in [
            ("RAG_MAX_IN_FLIGHT_REQUESTS", self.max_in_flight_requests),
            ("RAG_INGEST_QUEUE_CAPACITY", self.ingest_queue_capacity),
            (
                "RAG_INGEST_MAX_CONCURRENT_TASKS",
                self.ingest_max_concurrent_tasks,
            ),
        ] {
            if value > tokio::sync::Semaphore::MAX_PERMITS {
                anyhow::bail!(
                    "{name} must not exceed {}",
                    tokio::sync::Semaphore::MAX_PERMITS
                );
            }
        }

        validate_upload_allowed_mime_types(&self.upload_allowed_mime_types)?;

        validate_cors_allowed_origins(
            &self.run_mode,
            &self.cors_allowed_origins,
            self.allow_wildcard_cors,
        )?;
        Ok(())
    }

    fn validate_redaction_secret_sources(&self) -> anyhow::Result<()> {
        validate_redaction_secret("RAG_MEILI_API_KEY", self.meili_api_key.as_deref())?;
        validate_redaction_secret("RAG_OPENAI_API_KEY", self.openai_api_key.as_deref())?;
        if let Ok(secret) = std::str::from_utf8(&self.index_hash_secret) {
            validate_redaction_secret("RAG_INDEX_HASH_SECRET", Some(secret))?;
        }
        if let Some(path) = self.codex_auth_path.as_deref() {
            let token = crate::llm::read_codex_auth_token(path);
            validate_redaction_secret("Codex auth token", token.as_deref())?;
        }
        for secret in &self.previous_redaction_secrets {
            validate_redaction_secret("RAG_REDACTION_PREVIOUS_SECRETS", Some(secret))?;
        }
        Ok(())
    }

    fn validate_index_hash_secret(&self) -> anyhow::Result<()> {
        if self.run_mode != "production" {
            if self.allow_legacy_weak_index_hash_secret {
                anyhow::bail!(
                    "RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET is only valid in production"
                );
            }
            return Ok(());
        }

        if self.index_hash_secret == REJECTED_INDEX_HASH_SECRET_PLACEHOLDER.as_bytes() {
            anyhow::bail!(
                "RAG_INDEX_HASH_SECRET must be randomly generated; the documented placeholder is not a secret"
            );
        }

        let distinct_bytes = self
            .index_hash_secret
            .iter()
            .copied()
            .collect::<HashSet<_>>()
            .len();
        let is_strong = self.index_hash_secret.len() >= MIN_PRODUCTION_INDEX_HASH_SECRET_BYTES
            && distinct_bytes >= MIN_PRODUCTION_INDEX_HASH_SECRET_DISTINCT_BYTES;
        if !is_strong && self.allow_legacy_weak_index_hash_secret {
            tracing::warn!(
                removal_date = "2026-10-01",
                removal_version = "v0.13.0",
                migration = "index_hash_secret_v1",
                "legacy weak index-HMAC secret compatibility is enabled; do not rotate the key until per-user indexes are migrated or reindexed"
            );
            return Ok(());
        }
        if self.index_hash_secret == DEVELOPMENT_INDEX_HASH_SECRET.as_bytes()
            || self.index_hash_secret == DOCUMENTED_LEGACY_INDEX_HASH_SECRET.as_bytes()
        {
            anyhow::bail!(
                "production mode requires a strong RAG_INDEX_HASH_SECRET; an existing weak-key deployment must temporarily set RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET=true until its per-user indexes are migrated"
            );
        }
        if self.index_hash_secret.len() < MIN_PRODUCTION_INDEX_HASH_SECRET_BYTES {
            anyhow::bail!(
                "RAG_INDEX_HASH_SECRET must be at least {MIN_PRODUCTION_INDEX_HASH_SECRET_BYTES} bytes in production"
            );
        }
        if distinct_bytes < MIN_PRODUCTION_INDEX_HASH_SECRET_DISTINCT_BYTES {
            anyhow::bail!(
                "RAG_INDEX_HASH_SECRET must contain at least {MIN_PRODUCTION_INDEX_HASH_SECRET_DISTINCT_BYTES} distinct bytes in production"
            );
        }
        if self.allow_legacy_weak_index_hash_secret {
            anyhow::bail!(
                "RAG_ALLOW_LEGACY_WEAK_INDEX_HASH_SECRET is only valid with an existing weak index-HMAC secret"
            );
        }

        Ok(())
    }

    fn validate_auth_configuration(&self) -> anyhow::Result<()> {
        if let Some(error) = &self.auth_config_error {
            anyhow::bail!("invalid authentication configuration: {error}");
        }

        let mut credentials = HashSet::new();
        for (index, user) in self.auth_users.iter().enumerate() {
            validate_secret("RAG_AUTH_USERS", Some(&user.token))?;
            if !credentials.insert(user.token.as_str()) {
                anyhow::bail!(
                    "authentication credentials must be unique (duplicate RAG_AUTH_USERS entry {})",
                    index + 1
                );
            }
            validate_roles(&user.roles, index + 1)?;
            match &user.scope {
                AuthUserScope::Owner { owner_user_id } if owner_user_id.trim().is_empty() => {
                    anyhow::bail!(
                        "RAG_AUTH_USERS entry {} has an empty owner_user_id",
                        index + 1
                    );
                }
                AuthUserScope::Owner { .. } | AuthUserScope::TenantService
                    if user.roles.iter().any(|role| role == "admin") =>
                {
                    anyhow::bail!(
                        "RAG_AUTH_USERS entry {} assigns the admin role to a non-admin scope",
                        index + 1
                    );
                }
                _ => {}
            }
        }

        validate_secret("RAG_ADMIN_TOKEN", self.admin_token.as_deref())?;
        if let Some(token) = self.admin_token.as_deref() {
            if !credentials.insert(token) {
                anyhow::bail!(
                    "authentication credentials must be unique (RAG_ADMIN_TOKEN collides with another credential)"
                );
            }
        }

        validate_secret("RAG_BEARER_TOKEN", self.bearer_token.as_deref())?;
        match self.bearer_token.as_deref() {
            Some(token) => {
                if !credentials.insert(token) {
                    anyhow::bail!(
                        "authentication credentials must be unique (RAG_BEARER_TOKEN collides with another credential)"
                    );
                }
                match self.bearer_token_scope {
                    Some(BearerTokenScope::Owner) => {
                        let owner = self
                            .bearer_token_owner_user_id
                            .as_deref()
                            .filter(|owner| !owner.trim().is_empty())
                            .ok_or_else(|| {
                                anyhow::anyhow!(
                                    "RAG_BEARER_TOKEN_SCOPE=owner requires RAG_BEARER_TOKEN_OWNER_USER_ID"
                                )
                            })?;
                        if owner != owner.trim() {
                            anyhow::bail!(
                                "RAG_BEARER_TOKEN_OWNER_USER_ID must not have surrounding whitespace"
                            );
                        }
                        if self.allow_legacy_tenant_service_bearer {
                            anyhow::bail!(
                                "RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER cannot be combined with an explicit bearer scope"
                            );
                        }
                    }
                    Some(BearerTokenScope::TenantService) => {
                        if self.bearer_token_owner_user_id.is_some() {
                            anyhow::bail!(
                                "RAG_BEARER_TOKEN_OWNER_USER_ID is only valid with RAG_BEARER_TOKEN_SCOPE=owner"
                            );
                        }
                        if self.allow_legacy_tenant_service_bearer {
                            anyhow::bail!(
                                "RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER cannot be combined with an explicit bearer scope"
                            );
                        }
                    }
                    None if self.allow_legacy_tenant_service_bearer => {
                        if self.bearer_token_owner_user_id.is_some() {
                            anyhow::bail!(
                                "RAG_BEARER_TOKEN_OWNER_USER_ID requires RAG_BEARER_TOKEN_SCOPE=owner"
                            );
                        }
                        tracing::warn!(
                            removal_date = "2026-10-01",
                            removal_version = "v0.13.0",
                            "legacy tenant-service bearer compatibility is enabled; set RAG_BEARER_TOKEN_SCOPE=tenant_service"
                        );
                    }
                    None => {
                        anyhow::bail!(
                            "RAG_BEARER_TOKEN requires RAG_BEARER_TOKEN_SCOPE=owner|tenant_service; temporary tenant-wide compatibility requires RAG_ALLOW_LEGACY_TENANT_SERVICE_BEARER=true"
                        );
                    }
                }
            }
            None => {
                if self.bearer_token_scope.is_some()
                    || self.bearer_token_owner_user_id.is_some()
                    || self.allow_legacy_tenant_service_bearer
                {
                    anyhow::bail!(
                        "bearer scope, owner, and compatibility settings require RAG_BEARER_TOKEN"
                    );
                }
            }
        }

        let has_ordinary_credential_without_writer = self.bearer_token.is_some()
            || self.auth_users.iter().any(|user| {
                !matches!(&user.scope, AuthUserScope::Admin)
                    && !user.roles.iter().any(|role| role == "company_writer")
            });
        if self.allow_legacy_shared_writer {
            tracing::warn!(
                removal_date = "2026-10-01",
                removal_version = "v0.13.0",
                "legacy shared-writer compatibility is enabled; assign company_writer roles before the compatibility window closes"
            );
        } else if self.run_mode == "production" && has_ordinary_credential_without_writer {
            tracing::warn!(
                removal_date = "2026-10-01",
                removal_version = "v0.13.0",
                "ordinary credentials no longer authorize shared company or dataset mutations; assign company_writer or temporarily set RAG_ALLOW_LEGACY_SHARED_WRITER=true"
            );
        }

        Ok(())
    }

    pub fn has_any_auth(&self) -> bool {
        self.auth_config_error.is_some()
            || self.bearer_token.is_some()
            || self.admin_token.is_some()
            || !self.auth_users.is_empty()
    }

    pub fn test() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 0,
            run_mode: "test".to_string(),
            tenant_id: "test-tenant".to_string(),
            bearer_token: None,
            bearer_token_scope: None,
            bearer_token_owner_user_id: None,
            allow_legacy_tenant_service_bearer: false,
            allow_legacy_shared_writer: false,
            allow_legacy_weak_index_hash_secret: false,
            admin_token: None,
            auth_users: Vec::new(),
            allow_unsafe_unauthenticated: true,
            index_hash_secret: b"test-secret".to_vec(),
            store_backend: "memory".to_string(),
            meili_url: None,
            meili_api_key: None,
            write_consistency: WriteConsistency::WaitForIndex,
            meili_wait_for_tasks: true,
            meili_scan_page_size: DEFAULT_MEILI_SCAN_PAGE_SIZE,
            meili_scan_max_documents: DEFAULT_MEILI_SCAN_MAX_DOCUMENTS,
            parser_provider: "builtin".to_string(),
            mineru_api_url: "http://127.0.0.1:8000".to_string(),
            mineru_backend: "hybrid-auto-engine".to_string(),
            mineru_return_md: true,
            mineru_return_content_list: true,
            mineru_return_middle_json: true,
            mineru_return_images: true,
            max_json_bytes: DEFAULT_MAX_JSON_BYTES,
            max_upload_bytes: DEFAULT_MAX_UPLOAD_BYTES,
            max_multipart_fields: DEFAULT_MAX_MULTIPART_FIELDS,
            max_bulk_events: DEFAULT_MAX_BULK_EVENTS,
            max_bulk_rows: DEFAULT_MAX_BULK_ROWS,
            max_search_limit: DEFAULT_MAX_SEARCH_LIMIT,
            max_tags_per_item: DEFAULT_MAX_TAGS_PER_ITEM,
            max_tag_bytes: DEFAULT_MAX_TAG_BYTES,
            upload_allowed_mime_types: default_upload_allowed_mime_types(),
            request_timeout_ms: DEFAULT_REQUEST_TIMEOUT_MS,
            sync_ingest_timeout_ms: DEFAULT_SYNC_INGEST_TIMEOUT_MS,
            max_in_flight_requests: DEFAULT_MAX_IN_FLIGHT_REQUESTS,
            rate_limit_requests_per_minute: DEFAULT_RATE_LIMIT_REQUESTS_PER_MINUTE,
            ingest_queue_capacity: DEFAULT_INGEST_MAX_CONCURRENT_TASKS
                * DEFAULT_INGEST_QUEUE_MULTIPLIER,
            shutdown_timeout_ms: DEFAULT_SHUTDOWN_TIMEOUT_MS,
            // Test fixtures do not need browser access. Keeping this empty also
            // lets tests promote the fixture to production without inheriting
            // a development-only wildcard origin.
            cors_allowed_origins: Vec::new(),
            allow_wildcard_cors: false,
            ingest_max_concurrent_tasks: DEFAULT_INGEST_MAX_CONCURRENT_TASKS,
            ingest_task_retention_seconds: 86_400,
            ingest_cleanup_interval_seconds: 300,
            ingest_worker_enabled: true,
            llm_provider: "none".to_string(),
            llm_model: Some("none".to_string()),
            llm_reasoning_effort: None,
            analysis_llm_provider: "none".to_string(),
            analysis_llm_model: Some("none".to_string()),
            analysis_llm_reasoning_effort: None,
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
            vector_match_enabled: true,
            vector_match_weight: 4.0,
            vector_match_doc_weight: 2.0,
            vector_match_min_score: 0.25,
            auth_config_error: None,
            boundary_config_error: None,
            previous_redaction_secrets: Vec::new(),
            codex_secret_inventory: Arc::new(Mutex::new(CodexSecretInventory::default())),
            codex_secret_refresh: Arc::new(Mutex::new(CodexSecretRefreshState::default())),
            codex_secret_refresh_task_started: Arc::new(AtomicBool::new(false)),
        }
    }
}

fn validate_upload_allowed_mime_types(mime_types: &[String]) -> anyhow::Result<()> {
    if mime_types.is_empty() {
        anyhow::bail!("RAG_UPLOAD_ALLOWED_MIME_TYPES must contain at least one MIME type");
    }
    let mut unique = HashSet::new();
    for mime_type in mime_types {
        if mime_type.is_empty()
            || mime_type != mime_type.trim()
            || mime_type != &mime_type.to_ascii_lowercase()
            || mime_type.contains('*')
        {
            anyhow::bail!(
                "RAG_UPLOAD_ALLOWED_MIME_TYPES entries must be exact lowercase MIME types"
            );
        }
        reqwest::multipart::Part::bytes(Vec::new())
            .mime_str(mime_type)
            .map_err(|_| {
                anyhow::anyhow!("RAG_UPLOAD_ALLOWED_MIME_TYPES contains an invalid MIME type")
            })?;
        if !unique.insert(mime_type.as_str()) {
            anyhow::bail!("RAG_UPLOAD_ALLOWED_MIME_TYPES must not contain duplicate MIME types");
        }
    }
    Ok(())
}

fn parse_env_number<T>(name: &str, default: T, errors: &mut Vec<String>) -> T
where
    T: Copy + std::str::FromStr,
{
    match std::env::var(name) {
        Ok(value) => parse_number_value(name, &value, default, errors),
        Err(std::env::VarError::NotPresent) => default,
        Err(std::env::VarError::NotUnicode(_)) => {
            errors.push(format!("{name} must contain valid Unicode digits"));
            default
        }
    }
}

fn parse_number_value<T>(name: &str, value: &str, default: T, errors: &mut Vec<String>) -> T
where
    T: Copy + std::str::FromStr,
{
    match value.parse() {
        Ok(value) => value,
        Err(_) => {
            errors.push(format!("{name} must be a valid non-negative integer"));
            default
        }
    }
}

fn parse_cors_allowed_origins(value: &str) -> Vec<String> {
    if value.is_empty() {
        Vec::new()
    } else {
        value
            .split(',')
            .map(|origin| origin.trim().to_string())
            .collect()
    }
}

fn parse_csv_values(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|item| item.trim().to_ascii_lowercase())
        .filter(|item| !item.is_empty())
        .collect()
}

fn default_upload_allowed_mime_types() -> Vec<String> {
    [
        "text/plain",
        "text/markdown",
        "application/octet-stream",
        "application/pdf",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "image/png",
        "image/jpeg",
        "image/webp",
        "image/gif",
        "image/tiff",
    ]
    .into_iter()
    .map(ToString::to_string)
    .collect()
}

fn default_cors_allowed_origins(run_mode: &str) -> Vec<String> {
    if matches!(run_mode, "development" | "test") {
        vec!["*".to_string()]
    } else {
        Vec::new()
    }
}

pub(crate) fn validate_cors_allowed_origins(
    run_mode: &str,
    origins: &[String],
    allow_wildcard_cors: bool,
) -> anyhow::Result<()> {
    if origins.iter().any(|origin| origin == "*") {
        if origins.len() != 1 {
            anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS wildcard must be the sole origin");
        }
        if run_mode == "production" && !allow_wildcard_cors {
            anyhow::bail!("production wildcard CORS requires RAG_ALLOW_WILDCARD_CORS=true");
        }
        return Ok(());
    }

    let mut unique_origins = HashSet::new();
    for origin in origins {
        validate_cors_origin(origin)?;
        if !unique_origins.insert(origin.as_str()) {
            anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS must not contain duplicate origins");
        }
    }
    Ok(())
}

fn validate_cors_origin(origin: &str) -> anyhow::Result<()> {
    if origin.is_empty() || origin != origin.trim() {
        anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS contains an empty or padded origin");
    }

    let Some((scheme, authority)) = origin.split_once("://") else {
        anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS entries must be exact http/https origins");
    };
    if !matches!(scheme, "http" | "https")
        || authority.is_empty()
        || authority.contains(['/', '?', '#', '@'])
    {
        anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS entries must be exact http/https origins");
    }

    let uri = origin
        .parse::<axum::http::Uri>()
        .map_err(|_| anyhow::anyhow!("RAG_CORS_ALLOWED_ORIGINS contains an invalid origin"))?;
    if uri.scheme_str() != Some(scheme) {
        anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS entries must be exact http/https origins");
    }
    let Some(authority) = uri.authority() else {
        anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS entries must be exact http/https origins");
    };
    let authority_text = authority.as_str();
    let has_port_separator = if authority_text.starts_with('[') {
        authority_text
            .find(']')
            .is_some_and(|index| authority_text[index + 1..].starts_with(':'))
    } else {
        authority_text.contains(':')
    };
    if authority.host().is_empty() || (has_port_separator && authority.port().is_none()) {
        anyhow::bail!("RAG_CORS_ALLOWED_ORIGINS contains an invalid origin");
    }
    axum::http::HeaderValue::from_str(origin)
        .map_err(|_| anyhow::anyhow!("RAG_CORS_ALLOWED_ORIGINS contains an invalid origin"))?;
    Ok(())
}

fn default_allow_unsafe_unauthenticated(run_mode: &str) -> bool {
    matches!(run_mode, "development" | "test")
}

fn default_write_consistency(run_mode: &str) -> WriteConsistency {
    if run_mode == "test" {
        WriteConsistency::WaitForIndex
    } else {
        WriteConsistency::ReadYourWrites
    }
}

fn resolve_write_consistency(
    run_mode: &str,
    configured: Option<&str>,
    legacy_wait_for_tasks: Option<&str>,
) -> Result<WriteConsistency, String> {
    if configured.is_some() && legacy_wait_for_tasks.is_some() {
        return Err(
            "RAG_WRITE_CONSISTENCY cannot be combined with RAG_MEILI_WAIT_FOR_TASKS".to_string(),
        );
    }
    if let Some(configured) = configured {
        return configured.parse().map_err(str::to_string);
    }
    if let Some(legacy_wait_for_tasks) = legacy_wait_for_tasks {
        return Ok(if truthy(legacy_wait_for_tasks) {
            WriteConsistency::WaitForIndex
        } else {
            WriteConsistency::ReadYourWrites
        });
    }
    Ok(default_write_consistency(run_mode))
}

fn truthy(value: &str) -> bool {
    matches!(value, "1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
}

fn validate_secret(name: &str, value: Option<&str>) -> anyhow::Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        anyhow::bail!("{name} must not be empty when set");
    }
    if value != value.trim() {
        anyhow::bail!("{name} must not have surrounding whitespace");
    }
    if value.chars().count() < MIN_AUTH_CREDENTIAL_CHARS {
        anyhow::bail!("{name} must be at least {MIN_AUTH_CREDENTIAL_CHARS} characters when set");
    }
    Ok(())
}

fn validate_redaction_secret(name: &str, value: Option<&str>) -> anyhow::Result<()> {
    let Some(value) = value else {
        return Ok(());
    };
    if value.trim().is_empty() {
        anyhow::bail!("{name} must not contain empty values");
    }
    if value != value.trim() {
        anyhow::bail!("{name} values must not have surrounding whitespace");
    }
    if value.chars().count() < MIN_REDACTION_SECRET_CHARS {
        anyhow::bail!("{name} must be at least {MIN_REDACTION_SECRET_CHARS} characters when set");
    }
    Ok(())
}

fn parse_previous_redaction_secrets(value: &str) -> Vec<String> {
    value.split(',').map(ToString::to_string).collect()
}

fn validate_roles(roles: &[String], entry_number: usize) -> anyhow::Result<()> {
    if roles.is_empty() {
        anyhow::bail!(
            "RAG_AUTH_USERS entry {entry_number} must include at least one non-empty role"
        );
    }
    let mut seen = HashSet::new();
    for role in roles {
        if role.trim().is_empty()
            || role != role.trim()
            || !role.chars().all(|character| {
                character.is_ascii_alphanumeric() || matches!(character, '_' | '-')
            })
        {
            anyhow::bail!("RAG_AUTH_USERS entry {entry_number} contains an invalid role");
        }
        if !seen.insert(role.as_str()) {
            anyhow::bail!("RAG_AUTH_USERS entry {entry_number} contains a duplicate role");
        }
    }
    Ok(())
}

fn parse_bearer_token_scope(value: &str) -> anyhow::Result<BearerTokenScope> {
    match value {
        "owner" => Ok(BearerTokenScope::Owner),
        "tenant_service" => Ok(BearerTokenScope::TenantService),
        _ => anyhow::bail!("RAG_BEARER_TOKEN_SCOPE must be owner or tenant_service"),
    }
}

fn parse_auth_users(value: &str) -> anyhow::Result<Vec<AuthUserConfig>> {
    if value.trim().is_empty() {
        anyhow::bail!("RAG_AUTH_USERS must not be empty when set");
    }

    let mut users = Vec::new();
    let mut credentials = HashSet::new();
    for (index, entry) in value.split(',').enumerate() {
        let entry_number = index + 1;
        let parts = entry.split(':').collect::<Vec<_>>();
        if !(2..=3).contains(&parts.len()) {
            anyhow::bail!("RAG_AUTH_USERS entry {entry_number} must use owner:token[:role|role]");
        }
        let owner = parts[0];
        let token = parts[1];
        if owner.trim().is_empty() || token.trim().is_empty() {
            anyhow::bail!(
                "RAG_AUTH_USERS entry {entry_number} must include a non-empty owner and token"
            );
        }
        if owner != owner.trim() || token != token.trim() {
            anyhow::bail!(
                "RAG_AUTH_USERS entry {entry_number} must not contain surrounding owner or token whitespace"
            );
        }
        if !credentials.insert(token) {
            anyhow::bail!("RAG_AUTH_USERS contains a duplicate token");
        }

        let roles = parts
            .get(2)
            .copied()
            .unwrap_or("user")
            .split('|')
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        validate_roles(&roles, entry_number)?;

        let has_admin_role = roles.iter().any(|role| role == "admin");
        let scope = match (owner, has_admin_role) {
            ("*", true) => AuthUserScope::Admin,
            ("*", false) => AuthUserScope::TenantService,
            (_, true) => {
                tracing::warn!(
                    entry_number,
                    removal_date = "2026-10-01",
                    removal_version = "v0.13.0",
                    "legacy named-owner admin credential retains Admin scope; migrate it to *:token:admin or RAG_ADMIN_TOKEN"
                );
                AuthUserScope::Admin
            }
            (owner_user_id, false) => AuthUserScope::Owner {
                owner_user_id: owner_user_id.to_string(),
            },
        };
        users.push(AuthUserConfig {
            token: token.to_string(),
            scope,
            roles,
        });
    }
    Ok(users)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_consistency_parser_accepts_only_canonical_values() {
        assert_eq!("eventual".parse(), Ok(WriteConsistency::Eventual));
        assert_eq!(
            "read_your_writes".parse(),
            Ok(WriteConsistency::ReadYourWrites)
        );
        assert_eq!("wait_for_index".parse(), Ok(WriteConsistency::WaitForIndex));

        for invalid in [
            "",
            "Eventual",
            "read-your-writes",
            " wait_for_index",
            "wait_for_index ",
            "unknown",
        ] {
            assert!(
                invalid.parse::<WriteConsistency>().is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    #[test]
    fn write_consistency_defaults_are_mode_specific() {
        assert_eq!(
            default_write_consistency("test"),
            WriteConsistency::WaitForIndex
        );
        assert_eq!(
            default_write_consistency("development"),
            WriteConsistency::ReadYourWrites
        );
        assert_eq!(
            default_write_consistency("production"),
            WriteConsistency::ReadYourWrites
        );

        let config = Config::test();
        assert_eq!(config.write_consistency, WriteConsistency::WaitForIndex);
        assert!(config.meili_wait_for_tasks);
    }

    #[test]
    fn write_consistency_legacy_setting_maps_to_typed_policy() {
        assert_eq!(
            resolve_write_consistency("development", None, Some("true")),
            Ok(WriteConsistency::WaitForIndex)
        );
        assert_eq!(
            resolve_write_consistency("development", None, Some("false")),
            Ok(WriteConsistency::ReadYourWrites)
        );
    }

    #[test]
    fn write_consistency_rejects_new_and_legacy_settings_together() {
        let error =
            resolve_write_consistency("development", Some("read_your_writes"), Some("false"))
                .unwrap_err();

        assert!(error.contains("RAG_WRITE_CONSISTENCY"));
        assert!(error.contains("RAG_MEILI_WAIT_FOR_TASKS"));
    }

    #[test]
    fn production_rejects_eventual_write_consistency() {
        let mut config = Config::test();
        config.run_mode = "production".to_string();
        config.write_consistency = WriteConsistency::Eventual;
        config.meili_wait_for_tasks = false;

        let error = config.validate_startup().unwrap_err().to_string();

        assert!(error.contains("RAG_WRITE_CONSISTENCY=eventual"));
    }

    #[test]
    fn analysis_llm_config_uses_analysis_reasoning_effort() {
        let mut config = Config::test();
        config.llm_reasoning_effort = Some("high".to_string());
        config.analysis_llm_reasoning_effort = Some("xhigh".to_string());

        let analysis_config = config.analysis_llm_config();

        assert_eq!(
            analysis_config.llm_reasoning_effort.as_deref(),
            Some("xhigh")
        );
    }

    #[test]
    fn auth_users_parse_owner_service_and_legacy_admin_scopes_explicitly() {
        let users = parse_auth_users(
            "u1:owner-token:user|company_writer,*:service-token:user,*:admin-token:admin,legacy-owner:legacy-admin:admin",
        )
        .unwrap();

        assert_eq!(
            users[0].scope,
            AuthUserScope::Owner {
                owner_user_id: "u1".to_string()
            }
        );
        assert_eq!(users[1].scope, AuthUserScope::TenantService);
        assert_eq!(users[2].scope, AuthUserScope::Admin);
        assert_eq!(users[3].scope, AuthUserScope::Admin);
    }

    #[test]
    fn auth_users_reject_malformed_empty_and_duplicate_entries() {
        for value in [
            "",
            "u1",
            ":token:user",
            "u1::user",
            "u1:token:",
            " u1:token:user",
            "u1: token:user",
            "u1:token: user",
            "u1:token:user ",
            "u1:token:user,",
            "u1:token:user,u2:token:user",
        ] {
            assert!(parse_auth_users(value).is_err(), "accepted {value:?}");
        }
    }

    #[test]
    fn bearer_scope_requires_an_explicit_valid_binding() {
        let mut config = Config::test();
        config.bearer_token = Some("legacy-token".to_string());
        assert!(config.validate_startup().is_err());

        config.bearer_token_scope = Some(BearerTokenScope::Owner);
        assert!(config.validate_startup().is_err());

        config.bearer_token_owner_user_id = Some("u1".to_string());
        assert!(config.validate_startup().is_ok());

        config.bearer_token_scope = Some(BearerTokenScope::TenantService);
        assert!(config.validate_startup().is_err());
        config.bearer_token_owner_user_id = None;
        assert!(config.validate_startup().is_ok());
    }

    #[test]
    fn temporary_legacy_tenant_service_compatibility_is_explicit() {
        let mut config = Config::test();
        config.bearer_token = Some("legacy-token".to_string());
        config.allow_legacy_tenant_service_bearer = true;

        assert!(config.validate_startup().is_ok());
    }

    #[test]
    fn authentication_credentials_must_be_nonempty_and_unique_across_sources() {
        let mut config = Config::test();
        config.auth_users = vec![AuthUserConfig {
            token: "same-token".to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            roles: vec!["user".to_string()],
        }];
        config.admin_token = Some("same-token".to_string());
        assert!(config.validate_startup().is_err());

        config.admin_token = Some("   ".to_string());
        assert!(config.validate_startup().is_err());
    }

    #[test]
    fn authentication_credentials_shorter_than_eight_characters_are_rejected_without_echoing_them()
    {
        let short_token = "tiny123";

        let mut admin_config = Config::test();
        admin_config.admin_token = Some(short_token.to_string());
        let admin_error = admin_config.validate_startup().unwrap_err().to_string();
        assert!(admin_error.contains("at least 8 characters"));
        assert!(!admin_error.contains(short_token));

        let mut bearer_config = Config::test();
        bearer_config.bearer_token = Some(short_token.to_string());
        bearer_config.bearer_token_scope = Some(BearerTokenScope::TenantService);
        let bearer_error = bearer_config.validate_startup().unwrap_err().to_string();
        assert!(bearer_error.contains("at least 8 characters"));
        assert!(!bearer_error.contains(short_token));

        let mut user_config = Config::test();
        user_config.auth_users = vec![AuthUserConfig {
            token: short_token.to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            roles: vec!["user".to_string()],
        }];
        let user_error = user_config.validate_startup().unwrap_err().to_string();
        assert!(user_error.contains("at least 8 characters"));
        assert!(!user_error.contains(short_token));

        let mut boundary_config = Config::test();
        boundary_config.admin_token = Some("12345678".to_string());
        assert!(boundary_config.validate_startup().is_ok());

        let mut multibyte_config = Config::test();
        multibyte_config.admin_token = Some("秘密秘".to_string());
        assert!(multibyte_config.validate_startup().is_err());
    }

    #[test]
    fn codex_secret_inventory_retains_rotated_tokens_across_clones_and_read_failures() {
        let auth_path = std::env::temp_dir().join(format!(
            "nowledge-config-codex-auth-{}.json",
            uuid::Uuid::now_v7()
        ));
        let old_token = "codex-old-token-for-redaction";
        let new_token = "codex-new-token-for-redaction";
        std::fs::write(
            &auth_path,
            serde_json::json!({ "access_token": old_token }).to_string(),
        )
        .unwrap();

        let mut config = Config::test();
        config.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
        let clone = config.clone();

        let initial = config.refresh_configured_secret_values();
        assert!(initial.iter().any(|secret| secret == old_token));
        assert!(!initial.iter().any(|secret| secret == new_token));

        std::fs::write(
            &auth_path,
            serde_json::json!({ "access_token": new_token }).to_string(),
        )
        .unwrap();
        let rotated = clone.refresh_configured_secret_values();
        assert!(rotated.iter().any(|secret| secret == old_token));
        assert!(rotated.iter().any(|secret| secret == new_token));

        std::fs::write(&auth_path, "{invalid-json").unwrap();
        let retained = config.refresh_configured_secret_values();
        let _ = std::fs::remove_file(&auth_path);
        assert!(retained.iter().any(|secret| secret == old_token));
        assert!(retained.iter().any(|secret| secret == new_token));
    }

    #[test]
    fn explicit_previous_secrets_bridge_codex_rotation_across_process_restarts() {
        let auth_path = std::env::temp_dir().join(format!(
            "nowledge-restarted-codex-auth-{}.json",
            uuid::Uuid::now_v7()
        ));
        let old_token = "codex-old-token-before-restart";
        let new_token = "codex-new-token-after-restart";
        std::fs::write(
            &auth_path,
            serde_json::json!({ "access_token": new_token }).to_string(),
        )
        .unwrap();

        let mut restarted = Config::test();
        restarted.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
        restarted.previous_redaction_secrets = vec![old_token.to_string()];
        assert!(restarted.validate_startup().is_ok());
        let inventory = restarted.refresh_configured_secret_values();
        let _ = std::fs::remove_file(auth_path);

        assert!(inventory.iter().any(|secret| secret == old_token));
        assert!(inventory.iter().any(|secret| secret == new_token));
    }

    #[test]
    fn immediate_rotation_keeps_client_and_redaction_on_one_cached_snapshot() {
        let auth_path = std::env::temp_dir().join(format!(
            "nowledge-cached-codex-auth-{}.json",
            uuid::Uuid::now_v7()
        ));
        let old_token = "codex-old-cached-token-private";
        let new_token = "codex-new-unobserved-token-private";
        std::fs::write(
            &auth_path,
            serde_json::json!({ "access_token": old_token }).to_string(),
        )
        .unwrap();

        let mut config = Config::test();
        config.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
        config.refresh_configured_secret_values();
        std::fs::write(
            &auth_path,
            serde_json::json!({ "access_token": new_token }).to_string(),
        )
        .unwrap();

        // Neither consumer rereads the file on the request path. Until the
        // background refresh publishes the next snapshot, both consistently
        // use the previously observed credential.
        let snapshot = config.provider_security_snapshot();
        let client_token = snapshot.credentials.unwrap().token;
        let inventory = snapshot.secrets;
        assert_eq!(client_token, old_token);
        assert!(inventory.iter().any(|secret| secret == &client_token));
        assert!(!inventory.iter().any(|secret| secret == new_token));

        let refreshed = config.refresh_configured_secret_values();
        let _ = std::fs::remove_file(auth_path);
        assert_eq!(config.codex_auth_credentials().unwrap().token, new_token);
        assert!(refreshed.iter().any(|secret| secret == old_token));
        assert!(refreshed.iter().any(|secret| secret == new_token));
    }

    #[test]
    fn provider_snapshot_is_atomic_during_concurrent_credential_publication() {
        use std::sync::Barrier;

        let config = Arc::new(Config::test());
        config.publish_codex_credentials(crate::llm::CodexAuthCredentials {
            token: "codex-snapshot-token-a".to_string(),
            account_id: None,
            token_kind: crate::llm::CodexAuthTokenKind::Other,
        });
        let barrier = Arc::new(Barrier::new(2));
        let writer_config = config.clone();
        let writer_barrier = barrier.clone();
        let writer = std::thread::spawn(move || {
            writer_barrier.wait();
            for index in 0..10_000 {
                let suffix = if index % 2 == 0 { 'a' } else { 'b' };
                writer_config.publish_codex_credentials(crate::llm::CodexAuthCredentials {
                    token: format!("codex-snapshot-token-{suffix}"),
                    account_id: None,
                    token_kind: crate::llm::CodexAuthTokenKind::Other,
                });
            }
        });

        barrier.wait();
        for _ in 0..10_000 {
            let snapshot = config.provider_security_snapshot();
            let active = snapshot.credentials.unwrap().token;
            assert!(snapshot.secrets.iter().any(|secret| secret == &active));
        }
        writer.join().unwrap();
    }

    #[test]
    fn every_configured_redaction_secret_has_a_non_amplifying_minimum_length() {
        let configurations: [fn(&mut Config); 3] = [
            |config: &mut Config| config.meili_api_key = Some("abc".to_string()),
            |config: &mut Config| config.openai_api_key = Some("abc".to_string()),
            |config: &mut Config| config.index_hash_secret = b"abc".to_vec(),
        ];
        for configure in configurations {
            let mut config = Config::test();
            configure(&mut config);
            let error = config.validate_startup().unwrap_err().to_string();
            assert!(error.contains("at least 4 characters"), "{error}");
            assert!(!error.contains("abc"), "{error}");
        }

        let auth_path = std::env::temp_dir().join(format!(
            "nowledge-short-codex-auth-{}.json",
            uuid::Uuid::now_v7()
        ));
        std::fs::write(
            &auth_path,
            serde_json::json!({ "access_token": "abc" }).to_string(),
        )
        .unwrap();
        let mut config = Config::test();
        config.codex_auth_path = Some(auth_path.to_string_lossy().into_owned());
        let error = config.validate_startup().unwrap_err().to_string();
        let _ = std::fs::remove_file(auth_path);
        assert!(error.contains("at least 4 characters"), "{error}");
        assert!(!error.contains("abc"), "{error}");

        let mut previous = Config::test();
        previous.previous_redaction_secrets = parse_previous_redaction_secrets("valid-secret,");
        assert!(previous.validate_startup().is_err());
        previous.previous_redaction_secrets =
            parse_previous_redaction_secrets("valid-secret, spaced-secret ");
        assert!(previous.validate_startup().is_err());
    }

    #[test]
    fn malformed_environment_auth_is_treated_as_configured_until_rejected() {
        let mut config = Config::test();
        config.auth_config_error = Some("malformed auth fixture".to_string());

        assert!(config.has_any_auth());
        assert!(config.validate_startup().is_err());
    }

    #[test]
    fn bearer_scope_parser_rejects_unknown_values() {
        assert_eq!(
            parse_bearer_token_scope("owner").unwrap(),
            BearerTokenScope::Owner
        );
        assert_eq!(
            parse_bearer_token_scope("tenant_service").unwrap(),
            BearerTokenScope::TenantService
        );
        assert!(parse_bearer_token_scope("user").is_err());
        assert!(parse_bearer_token_scope(" owner ").is_err());
    }

    #[test]
    fn production_rejects_the_public_development_index_hash_secret() {
        let mut config = Config::test();
        config.run_mode = "production".to_string();
        config.index_hash_secret = DEVELOPMENT_INDEX_HASH_SECRET.as_bytes().to_vec();

        let error = config.validate_startup().unwrap_err().to_string();

        assert!(error.contains("strong RAG_INDEX_HASH_SECRET"));
        assert!(!error.contains(DEVELOPMENT_INDEX_HASH_SECRET));
    }

    #[test]
    fn legacy_weak_index_hash_secrets_require_an_explicit_bounded_compatibility_flag() {
        let mut config = Config::test();
        config.run_mode = "production".to_string();
        config.allow_unsafe_unauthenticated = false;
        config.admin_token = Some("admin-test-token".to_string());
        config.allow_legacy_weak_index_hash_secret = true;

        for legacy_secret in [
            DEVELOPMENT_INDEX_HASH_SECRET,
            DOCUMENTED_LEGACY_INDEX_HASH_SECRET,
            "an-existing-custom-weak-key",
        ] {
            config.index_hash_secret = legacy_secret.as_bytes().to_vec();
            assert!(config.validate_startup().is_ok(), "{legacy_secret}");
        }

        config.index_hash_secret = b"7Qv!n2$La9@Xm4#Rp8%Wd3&Ks6*Hy1+Tz5".to_vec();
        let error = config.validate_startup().unwrap_err().to_string();
        assert!(error.contains("only valid with an existing weak"));
    }

    #[test]
    fn production_rejects_the_documented_placeholder_even_with_legacy_compatibility() {
        let mut config = Config::test();
        config.run_mode = "production".to_string();
        config.index_hash_secret = REJECTED_INDEX_HASH_SECRET_PLACEHOLDER.as_bytes().to_vec();
        config.allow_legacy_weak_index_hash_secret = true;

        let error = config.validate_startup().unwrap_err().to_string();

        assert!(error.contains("documented placeholder is not a secret"));
        assert!(!error.contains(REJECTED_INDEX_HASH_SECRET_PLACEHOLDER));
    }

    #[test]
    fn production_rejects_short_or_insufficiently_varied_index_hash_secrets() {
        let mut config = Config::test();
        config.run_mode = "production".to_string();

        let short_secret = "short-but-varied-secret";
        config.index_hash_secret = short_secret.as_bytes().to_vec();
        let short_error = config.validate_startup().unwrap_err().to_string();
        assert!(short_error.contains("at least 32 bytes"));
        assert!(!short_error.contains(short_secret));

        let repetitive_secret = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabbbbbbbb";
        config.index_hash_secret = repetitive_secret.as_bytes().to_vec();
        let repetitive_error = config.validate_startup().unwrap_err().to_string();
        assert!(repetitive_error.contains("at least 12 distinct bytes"));
        assert!(!repetitive_error.contains(repetitive_secret));
    }

    #[test]
    fn strong_index_hash_secret_is_required_only_in_production() {
        let mut config = Config::test();
        config.index_hash_secret = DEVELOPMENT_INDEX_HASH_SECRET.as_bytes().to_vec();
        assert!(config.validate_startup().is_ok());

        config.run_mode = "production".to_string();
        config.index_hash_secret = b"7Qv!n2$La9@Xm4#Rp8%Wd3&Ks6*Hy1+Tz5".to_vec();
        assert!(config.validate_startup().is_ok());
    }

    #[test]
    fn unknown_run_modes_are_rejected_and_never_default_to_unsafe_auth() {
        assert!(!default_allow_unsafe_unauthenticated("prod"));

        let mut config = Config::test();
        config.run_mode = "prod".to_string();
        let error = config.validate_startup().unwrap_err().to_string();
        assert!(error.contains("RAG_RUN_MODE must be development, test, or production"));
    }

    #[test]
    fn startup_rejects_blank_tenant_id() {
        for tenant_id in ["", "   ", "\t\n"] {
            let mut config = Config::test();
            config.tenant_id = tenant_id.to_string();

            let error = config.validate_startup().unwrap_err().to_string();

            assert!(error.contains("RAG_TENANT_ID must be non-empty"));
        }
    }

    #[test]
    fn boundary_defaults_are_positive_and_internally_consistent() {
        let config = Config::test();

        assert_eq!(config.max_json_bytes, 2 * 1024 * 1024);
        assert_eq!(config.max_upload_bytes, 50 * 1024 * 1024);
        assert_eq!(config.ingest_queue_capacity, 16);
        assert_eq!(config.meili_scan_page_size, 500);
        assert_eq!(config.meili_scan_max_documents, 100_000);
        assert_eq!(
            config.max_multipart_body_bytes(),
            Some(52 * 1024 * 1024 + 32 * 16 * 1024)
        );
        assert!(config.validate_http_boundaries().is_ok());
    }

    #[test]
    fn boundary_validation_rejects_zero_ordering_and_size_overflow() {
        let mut zero = Config::test();
        zero.max_bulk_events = 0;
        assert!(zero
            .validate_http_boundaries()
            .unwrap_err()
            .to_string()
            .contains("RAG_MAX_BULK_EVENTS"));

        let mut timeout = Config::test();
        timeout.sync_ingest_timeout_ms = timeout.request_timeout_ms - 1;
        assert!(timeout
            .validate_http_boundaries()
            .unwrap_err()
            .to_string()
            .contains("RAG_SYNC_INGEST_TIMEOUT_MS"));

        let mut overflow = Config::test();
        overflow.max_upload_bytes = usize::MAX;
        assert!(overflow.validate_http_boundaries().is_err());
        assert_eq!(overflow.max_multipart_body_bytes(), None);
    }

    #[test]
    fn meili_scan_limits_reject_zero_oversized_and_inverted_values() {
        let mut zero_page = Config::test();
        zero_page.meili_scan_page_size = 0;
        assert!(zero_page
            .validate_http_boundaries()
            .unwrap_err()
            .to_string()
            .contains("RAG_MEILI_SCAN_PAGE_SIZE"));

        let mut oversized_page = Config::test();
        oversized_page.meili_scan_page_size = MAX_MEILI_SCAN_PAGE_SIZE + 1;
        assert!(oversized_page
            .validate_http_boundaries()
            .unwrap_err()
            .to_string()
            .contains("must not exceed 1000"));

        let mut inverted = Config::test();
        inverted.meili_scan_page_size = 10;
        inverted.meili_scan_max_documents = 9;
        assert!(inverted
            .validate_http_boundaries()
            .unwrap_err()
            .to_string()
            .contains("RAG_MEILI_SCAN_MAX_DOCUMENTS"));
    }

    #[test]
    fn boundary_validation_rejects_capacity_values_above_tokio_limits() {
        let above_max = tokio::sync::Semaphore::MAX_PERMITS
            .checked_add(1)
            .expect("Tokio semaphore limit must leave room for an invalid test value");

        for (name, configure) in [
            (
                "RAG_MAX_IN_FLIGHT_REQUESTS",
                (|config: &mut Config, value| config.max_in_flight_requests = value)
                    as fn(&mut Config, usize),
            ),
            (
                "RAG_INGEST_QUEUE_CAPACITY",
                (|config: &mut Config, value| config.ingest_queue_capacity = value)
                    as fn(&mut Config, usize),
            ),
            (
                "RAG_INGEST_MAX_CONCURRENT_TASKS",
                (|config: &mut Config, value| config.ingest_max_concurrent_tasks = value)
                    as fn(&mut Config, usize),
            ),
        ] {
            let mut config = Config::test();
            configure(&mut config, above_max);
            let error = config.validate_http_boundaries().unwrap_err().to_string();
            assert!(error.contains(name), "unexpected error: {error}");
        }
    }

    #[test]
    fn boundary_validation_rejects_unrepresentable_deadlines() {
        for (name, configure) in [
            (
                "RAG_REQUEST_TIMEOUT_MS",
                (|config: &mut Config| config.request_timeout_ms = u64::MAX) as fn(&mut Config),
            ),
            (
                "RAG_SYNC_INGEST_TIMEOUT_MS",
                (|config: &mut Config| config.sync_ingest_timeout_ms = u64::MAX) as fn(&mut Config),
            ),
            (
                "RAG_SHUTDOWN_TIMEOUT_MS",
                (|config: &mut Config| config.shutdown_timeout_ms = u64::MAX) as fn(&mut Config),
            ),
        ] {
            let mut config = Config::test();
            configure(&mut config);
            let error = config.validate_http_boundaries().unwrap_err().to_string();
            assert!(error.contains(name), "unexpected error: {error}");
        }
    }

    #[test]
    fn upload_mime_policy_is_exact_normalized_and_nonempty() {
        let config = Config::test();
        assert!(config
            .upload_allowed_mime_types
            .iter()
            .any(|value| value == "application/octet-stream"));
        assert!(config.validate_http_boundaries().is_ok());

        for invalid in [
            Vec::<String>::new(),
            vec!["text/*".to_string()],
            vec!["Text/Plain".to_string()],
            vec!["not-a-mime".to_string()],
            vec!["text/plain".to_string(), "text/plain".to_string()],
        ] {
            let mut config = Config::test();
            config.upload_allowed_mime_types = invalid;
            assert!(config.validate_http_boundaries().is_err());
        }

        assert_eq!(
            parse_csv_values(" Text/Plain, APPLICATION/PDF "),
            ["text/plain", "application/pdf"]
        );
    }

    #[test]
    fn malformed_boundary_numbers_are_retained_as_startup_errors() {
        let mut errors = Vec::new();
        let parsed = parse_number_value("RAG_MAX_JSON_BYTES", "not-a-number", 42usize, &mut errors);

        assert_eq!(parsed, 42);
        assert_eq!(errors.len(), 1);
        assert!(errors[0].contains("RAG_MAX_JSON_BYTES"));
        assert!(!errors[0].contains("not-a-number"));

        let mut config = Config::test();
        config.boundary_config_error = Some(errors.join("; "));
        assert!(config.validate_http_boundaries().is_err());
    }

    #[test]
    fn cors_origins_are_exact_unique_and_wildcard_is_bounded() {
        assert!(validate_cors_allowed_origins(
            "production",
            &[
                "https://app.example.com".to_string(),
                "http://localhost:3000".to_string(),
            ],
            false,
        )
        .is_ok());
        for invalid in [
            "ftp://app.example.com",
            "https://app.example.com/",
            "https://app.example.com/path",
            "https://user@app.example.com",
            "http://:80",
            "http://app.example.com:99999",
            "",
        ] {
            assert!(
                validate_cors_allowed_origins("production", &[invalid.to_string()], false,)
                    .is_err(),
                "accepted {invalid:?}"
            );
        }

        assert!(validate_cors_allowed_origins(
            "development",
            &["*".to_string(), "https://app.example.com".to_string()],
            true,
        )
        .is_err());
        assert!(validate_cors_allowed_origins("production", &["*".to_string()], false,).is_err());
        assert!(validate_cors_allowed_origins("production", &["*".to_string()], true,).is_ok());
        assert!(validate_cors_allowed_origins(
            "production",
            &[
                "https://app.example.com".to_string(),
                "https://app.example.com".to_string(),
            ],
            false,
        )
        .is_err());
    }

    #[test]
    fn cors_defaults_are_open_only_in_development_and_test() {
        assert_eq!(default_cors_allowed_origins("development"), vec!["*"]);
        assert_eq!(default_cors_allowed_origins("test"), vec!["*"]);
        assert!(default_cors_allowed_origins("production").is_empty());
        assert!(default_cors_allowed_origins("unknown").is_empty());
        assert!(parse_cors_allowed_origins("").is_empty());
        assert_eq!(
            parse_cors_allowed_origins("https://a.example, https://b.example"),
            vec!["https://a.example", "https://b.example"]
        );
    }
}
