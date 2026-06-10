use std::{
    path::Path,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::{
    header::{HeaderMap, ACCEPT},
    StatusCode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::time::timeout;

use crate::{config::Config, error::ApiError};

#[derive(Debug, Clone)]
pub struct LlmRequest {
    pub prompt: String,
}

#[derive(Debug, Clone)]
pub struct LlmTextResponse {
    pub text: String,
    pub latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct LlmRuntimeStatus {
    pub provider: String,
    pub model: String,
    pub auth_source: String,
    pub healthy: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexAuthTokenKind {
    OpenAiApiKey,
    CodexOauth,
    Other,
}

#[derive(Clone)]
pub struct CodexAuthCredentials {
    pub token: String,
    pub account_id: Option<String>,
    pub token_kind: CodexAuthTokenKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RateLimitSnapshot {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_requests: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remaining_tokens: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_requests: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reset_tokens: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LlmHealthProbeResult {
    pub provider: String,
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    pub status: String,
    pub can_call: bool,
    pub auth_valid: bool,
    pub quota_state: String,
    pub rate_limit_state: String,
    pub checked_at: DateTime<Utc>,
    pub latency_ms: u64,
    pub stale: bool,
    pub age_seconds: u64,
    pub consecutive_failures: u32,
    pub rate_limits: RateLimitSnapshot,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone)]
struct CachedLlmProbe {
    result: LlmHealthProbeResult,
    checked_instant: Instant,
    consecutive_failures: u32,
}

#[derive(Debug, Clone, Default)]
pub struct LlmHealthProbe {
    cache: Arc<RwLock<Option<CachedLlmProbe>>>,
}

#[async_trait]
pub trait LlmClient: Send + Sync {
    async fn status(&self) -> LlmRuntimeStatus;
    async fn complete_text(&self, request: LlmRequest) -> Result<LlmTextResponse, ApiError>;
}

#[derive(Debug, Clone)]
pub struct NoneLlmClient {
    model: String,
}

#[async_trait]
impl LlmClient for NoneLlmClient {
    async fn status(&self) -> LlmRuntimeStatus {
        LlmRuntimeStatus {
            provider: "none".to_string(),
            model: self.model.clone(),
            auth_source: "none".to_string(),
            healthy: true,
        }
    }

    async fn complete_text(&self, request: LlmRequest) -> Result<LlmTextResponse, ApiError> {
        let started = Instant::now();
        Ok(LlmTextResponse {
            text: format!(
                "provider=none echo: {}",
                request.prompt.chars().take(80).collect::<String>()
            ),
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MockLlmClient {
    model: String,
}

#[async_trait]
impl LlmClient for MockLlmClient {
    async fn status(&self) -> LlmRuntimeStatus {
        LlmRuntimeStatus {
            provider: "mock".to_string(),
            model: self.model.clone(),
            auth_source: "mock".to_string(),
            healthy: true,
        }
    }

    async fn complete_text(&self, request: LlmRequest) -> Result<LlmTextResponse, ApiError> {
        let started = Instant::now();
        Ok(LlmTextResponse {
            text: format!(
                "mock summary: {}",
                request.prompt.chars().take(160).collect::<String>()
            ),
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

#[derive(Debug, Clone)]
pub struct OpenAiResponsesClient {
    provider: String,
    model: String,
    reasoning_effort: Option<String>,
    auth_source: String,
    api_key: Option<String>,
    client: reqwest::Client,
}

#[derive(Clone)]
pub struct CodexResponsesClient {
    model: String,
    reasoning_effort: Option<String>,
    auth_source: String,
    credentials: Option<CodexAuthCredentials>,
    base_url: String,
    client: reqwest::Client,
}

#[async_trait]
impl LlmClient for OpenAiResponsesClient {
    async fn status(&self) -> LlmRuntimeStatus {
        LlmRuntimeStatus {
            provider: self.provider.clone(),
            model: self.model.clone(),
            auth_source: self.auth_source.clone(),
            healthy: self.api_key.is_some(),
        }
    }

    async fn complete_text(&self, request: LlmRequest) -> Result<LlmTextResponse, ApiError> {
        let api_key = self
            .api_key
            .as_deref()
            .ok_or_else(|| ApiError::Unauthorized("LLM API key is not configured".to_string()))?;
        let started = Instant::now();
        let body = complete_openai_responses(
            &self.client,
            &self.model,
            self.reasoning_effort.as_deref(),
            api_key,
            &request.prompt,
        )
        .await?;
        Ok(LlmTextResponse {
            text: extract_response_text(&body)
                .unwrap_or_else(|| "LLM response did not contain output text".to_string()),
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

#[async_trait]
impl LlmClient for CodexResponsesClient {
    async fn status(&self) -> LlmRuntimeStatus {
        LlmRuntimeStatus {
            provider: "codex_auth".to_string(),
            model: self.model.clone(),
            auth_source: self.auth_source.clone(),
            healthy: self.credentials.is_some(),
        }
    }

    async fn complete_text(&self, request: LlmRequest) -> Result<LlmTextResponse, ApiError> {
        let credentials = self.credentials.as_ref().ok_or_else(|| {
            ApiError::Unauthorized("Codex auth token is not configured".to_string())
        })?;

        if credentials.token_kind == CodexAuthTokenKind::OpenAiApiKey {
            let started = Instant::now();
            let body = complete_openai_responses(
                &self.client,
                &self.model,
                self.reasoning_effort.as_deref(),
                &credentials.token,
                &request.prompt,
            )
            .await?;
            return Ok(LlmTextResponse {
                text: extract_response_text(&body)
                    .unwrap_or_else(|| "LLM response did not contain output text".to_string()),
                latency_ms: started.elapsed().as_millis() as u64,
            });
        }

        let started = Instant::now();
        let endpoint = codex_responses_endpoint(&self.base_url);
        let mut builder = self
            .client
            .post(endpoint)
            .bearer_auth(&credentials.token)
            .header(ACCEPT, "text/event-stream")
            .json(&codex_responses_payload(
                &self.model,
                &request.prompt,
                self.reasoning_effort.as_deref(),
            ));
        if let Some(account_id) = credentials.account_id.as_deref() {
            builder = builder.header("ChatGPT-Account-Id", account_id);
        }

        let response = builder
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;
        if !status.is_success() {
            let message = extract_error_message(&body).unwrap_or_else(|| status.to_string());
            return Err(ApiError::Upstream(format!(
                "Codex Responses API request failed: {status}: {message}"
            )));
        }

        Ok(LlmTextResponse {
            text: extract_codex_sse_text(&body)
                .unwrap_or_else(|| "LLM response did not contain output text".to_string()),
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
}

async fn complete_openai_responses(
    client: &reqwest::Client,
    model: &str,
    reasoning_effort: Option<&str>,
    api_key: &str,
    prompt: &str,
) -> Result<Value, ApiError> {
    let mut payload = json!({
        "model": model,
        "input": prompt,
        "store": false
    });
    set_reasoning_effort(&mut payload, reasoning_effort);

    let response = client
        .post("https://api.openai.com/v1/responses")
        .bearer_auth(api_key)
        .json(&payload)
        .send()
        .await
        .map_err(|e| ApiError::Upstream(e.to_string()))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        let message = extract_error_message(&body).unwrap_or_else(|| status.to_string());
        return Err(ApiError::Upstream(format!(
            "OpenAI Responses API request failed: {status}: {message}"
        )));
    }

    response
        .json::<Value>()
        .await
        .map_err(|e| ApiError::Upstream(e.to_string()))
}

fn codex_responses_payload(model: &str, prompt: &str, reasoning_effort: Option<&str>) -> Value {
    let mut payload = json!({
        "model": model,
        "instructions": "Answer the user request directly. When context is supplied, stay grounded in that context.",
        "input": [{
            "role": "user",
            "content": [{
                "type": "input_text",
                "text": prompt
            }]
        }],
        "store": false,
        "stream": true
    });
    set_reasoning_effort(&mut payload, reasoning_effort);
    payload
}

fn set_reasoning_effort(payload: &mut Value, reasoning_effort: Option<&str>) {
    let Some(reasoning_effort) = reasoning_effort
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return;
    };
    payload["reasoning"] = json!({ "effort": reasoning_effort });
}

fn codex_responses_endpoint(base_url: &str) -> String {
    format!("{}/responses", base_url.trim_end_matches('/'))
}

pub fn llm_client_from_config(config: &Config) -> Box<dyn LlmClient> {
    let model = config
        .llm_model
        .clone()
        .unwrap_or_else(|| "gpt-5.4-mini".to_string());
    match config.llm_provider.as_str() {
        "mock" => Box::new(MockLlmClient { model }),
        "openai_api_key" => Box::new(OpenAiResponsesClient {
            provider: "openai_api_key".to_string(),
            model,
            reasoning_effort: config.llm_reasoning_effort.clone(),
            auth_source: "RAG_OPENAI_API_KEY".to_string(),
            api_key: config.openai_api_key.clone(),
            client: reqwest::Client::new(),
        }),
        "codex_auth" => Box::new(CodexResponsesClient {
            model,
            reasoning_effort: config.llm_reasoning_effort.clone(),
            auth_source: config
                .codex_auth_path
                .clone()
                .unwrap_or_else(|| "explicit_path_missing".to_string()),
            credentials: config
                .codex_auth_path
                .as_deref()
                .and_then(read_codex_auth_credentials),
            base_url: config.codex_base_url.clone(),
            client: reqwest::Client::new(),
        }),
        _ => Box::new(NoneLlmClient {
            model: config
                .llm_model
                .clone()
                .unwrap_or_else(|| "none".to_string()),
        }),
    }
}

impl LlmHealthProbe {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn check(&self, config: &Config) -> LlmHealthProbeResult {
        if !config.health_llm_enabled {
            return with_reasoning_effort(disabled_probe(config), config);
        }

        if let Ok(cache) = self.cache.read() {
            if let Some(cached) = cache.as_ref() {
                if cached.checked_instant.elapsed()
                    < Duration::from_secs(config.health_llm_probe_interval_seconds)
                {
                    return self.cached_with_age(cached, config);
                }
            }
        }

        let previous_failures = self
            .cache
            .read()
            .ok()
            .and_then(|cache| cache.as_ref().map(|cached| cached.consecutive_failures))
            .unwrap_or(0);
        let mut result = with_reasoning_effort(probe_now(config).await, config);
        let consecutive_failures = if is_threshold_failure(&result) {
            previous_failures.saturating_add(1)
        } else {
            0
        };
        if is_threshold_failure(&result)
            && consecutive_failures >= config.health_llm_failure_threshold.max(1)
        {
            result.status = "unhealthy".to_string();
            result.can_call = false;
        }
        result.consecutive_failures = consecutive_failures;

        let cached = CachedLlmProbe {
            result: result.clone(),
            checked_instant: Instant::now(),
            consecutive_failures,
        };
        if let Ok(mut cache) = self.cache.write() {
            *cache = Some(cached);
        }
        result
    }

    pub fn cached(&self, config: &Config) -> Option<LlmHealthProbeResult> {
        self.cache.read().ok().and_then(|cache| {
            cache
                .as_ref()
                .map(|cached| self.cached_with_age(cached, config))
        })
    }

    fn cached_with_age(&self, cached: &CachedLlmProbe, config: &Config) -> LlmHealthProbeResult {
        let mut result = cached.result.clone();
        result.reasoning_effort = config.llm_reasoning_effort.clone();
        let age = cached.checked_instant.elapsed();
        result.age_seconds = age.as_secs();
        result.stale = age > Duration::from_secs(config.health_llm_probe_ttl_seconds);
        result.consecutive_failures = cached.consecutive_failures;
        if age > Duration::from_secs(config.health_llm_max_stale_seconds) {
            result.status = "unhealthy".to_string();
            result.can_call = false;
            result.error_kind = Some("stale_probe".to_string());
            result.message = Some("LLM health probe cache exceeded max stale age".to_string());
        }
        result
    }
}

fn with_reasoning_effort(
    mut result: LlmHealthProbeResult,
    config: &Config,
) -> LlmHealthProbeResult {
    result.reasoning_effort = config.llm_reasoning_effort.clone();
    result
}

async fn probe_now(config: &Config) -> LlmHealthProbeResult {
    let provider = config.llm_provider.clone();
    let model = config
        .llm_model
        .clone()
        .unwrap_or_else(|| "gpt-5.4-mini".to_string());
    match provider.as_str() {
        "none" => {
            if config.health_require_llm {
                unhealthy_probe(
                    provider,
                    config
                        .llm_model
                        .clone()
                        .unwrap_or_else(|| "none".to_string()),
                    "provider_disabled",
                    "LLM provider is none but RAG_HEALTH_REQUIRE_LLM=true",
                )
            } else {
                ok_probe(provider, "none".to_string(), RateLimitSnapshot::default())
            }
        }
        "mock" => ok_probe(
            provider,
            model,
            RateLimitSnapshot {
                remaining_requests: Some("1000".to_string()),
                remaining_tokens: Some("100000".to_string()),
                reset_requests: None,
                reset_tokens: None,
            },
        ),
        "mock_auth_failure" => auth_failure_probe(provider, model, "mock auth failure"),
        "mock_quota_exhausted" => quota_exhausted_probe(provider, model, "mock quota exhausted"),
        "mock_rate_limited" => rate_limited_probe(
            provider,
            model,
            config.health_llm_rate_limit_unhealthy,
            RateLimitSnapshot {
                remaining_requests: Some("0".to_string()),
                remaining_tokens: Some("0".to_string()),
                reset_requests: Some("1s".to_string()),
                reset_tokens: Some("1s".to_string()),
            },
            "mock short rate limit",
        ),
        "mock_server_error" => degraded_probe(provider, model, "server_error", "mock server error"),
        "mock_timeout" => degraded_probe(provider, model, "timeout", "mock timeout"),
        "openai_api_key" => {
            let Some(api_key) = config.openai_api_key.as_deref() else {
                return auth_failure_probe(provider, model, "LLM API key is not configured");
            };
            probe_openai_responses(config, provider, model, api_key).await
        }
        "codex_auth" => {
            let Some(path) = config.codex_auth_path.as_deref() else {
                return auth_failure_probe(provider, model, "Codex auth path is not configured");
            };
            let Some(credentials) = read_codex_auth_credentials(path) else {
                return auth_failure_probe(provider, model, "Codex auth token could not be read");
            };
            if credentials.token_kind == CodexAuthTokenKind::OpenAiApiKey {
                probe_openai_responses(config, provider, model, &credentials.token).await
            } else {
                probe_codex_responses(config, provider, model, &credentials).await
            }
        }
        _ => unhealthy_probe(
            provider,
            model,
            "unsupported_provider",
            "unsupported LLM provider",
        ),
    }
}

async fn probe_openai_responses(
    config: &Config,
    provider: String,
    model: String,
    api_key: &str,
) -> LlmHealthProbeResult {
    let started = Instant::now();
    let client = reqwest::Client::new();
    let mut payload = json!({
        "model": model,
        "input": "health check",
        "store": false,
        "max_output_tokens": 8
    });
    set_reasoning_effort(&mut payload, config.llm_reasoning_effort.as_deref());
    let request = client
        .post("https://api.openai.com/v1/responses")
        .bearer_auth(api_key)
        .json(&payload);
    let sent = timeout(
        Duration::from_millis(config.health_llm_timeout_ms),
        request.send(),
    )
    .await;

    let response = match sent {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            return degraded_probe(
                provider,
                model,
                "server_error",
                &format!("LLM probe request failed: {err}"),
            )
        }
        Err(_) => return degraded_probe(provider, model, "timeout", "LLM probe timed out"),
    };
    let latency_ms = started.elapsed().as_millis() as u64;
    let status = response.status();
    let headers = response.headers().clone();
    let rate_limits = rate_limits_from_headers(&headers);
    let body = response.text().await.unwrap_or_default();

    if status.is_success() {
        return ok_probe_with_latency(provider, model, rate_limits, latency_ms);
    }
    classify_http_probe(
        config,
        provider,
        model,
        status,
        rate_limits,
        body,
        latency_ms,
    )
}

async fn probe_codex_responses(
    config: &Config,
    provider: String,
    model: String,
    credentials: &CodexAuthCredentials,
) -> LlmHealthProbeResult {
    let started = Instant::now();
    let client = reqwest::Client::new();
    let mut request = client
        .post(codex_responses_endpoint(&config.codex_base_url))
        .bearer_auth(&credentials.token)
        .header(ACCEPT, "text/event-stream")
        .json(&codex_responses_payload(
            &model,
            "Reply with exactly: ok",
            config.llm_reasoning_effort.as_deref(),
        ));
    if let Some(account_id) = credentials.account_id.as_deref() {
        request = request.header("ChatGPT-Account-Id", account_id);
    }

    let sent = timeout(
        Duration::from_millis(config.health_llm_timeout_ms),
        request.send(),
    )
    .await;

    let response = match sent {
        Ok(Ok(response)) => response,
        Ok(Err(err)) => {
            return degraded_probe(
                provider,
                model,
                "server_error",
                &format!("LLM probe request failed: {err}"),
            )
        }
        Err(_) => return degraded_probe(provider, model, "timeout", "LLM probe timed out"),
    };
    let latency_ms = started.elapsed().as_millis() as u64;
    let status = response.status();
    let headers = response.headers().clone();
    let rate_limits = rate_limits_from_headers(&headers);
    let body = response.text().await.unwrap_or_default();

    if status.is_success() {
        if extract_codex_sse_text(&body).is_some() {
            return ok_probe_with_latency(provider, model, rate_limits, latency_ms);
        }
        return probe_result(ProbeResultInput {
            provider,
            model,
            status: "unhealthy",
            can_call: false,
            auth_valid: true,
            quota_state: "unknown",
            rate_limit_state: "unknown",
            error_kind: Some("request_failed"),
            message: Some("Codex Responses API returned no output text".to_string()),
            rate_limits,
            latency_ms,
        });
    }

    classify_http_probe(
        config,
        provider,
        model,
        status,
        rate_limits,
        body,
        latency_ms,
    )
}

fn classify_http_probe(
    config: &Config,
    provider: String,
    model: String,
    status: StatusCode,
    rate_limits: RateLimitSnapshot,
    body: String,
    latency_ms: u64,
) -> LlmHealthProbeResult {
    let body_lower = body.to_ascii_lowercase();
    let message = extract_error_message(&body).unwrap_or_else(|| status.to_string());
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        return probe_result(ProbeResultInput {
            provider,
            model,
            status: "unhealthy",
            can_call: false,
            auth_valid: false,
            quota_state: "unknown",
            rate_limit_state: "unknown",
            error_kind: Some("auth_failed"),
            message: Some(message),
            rate_limits,
            latency_ms,
        });
    }
    if status == StatusCode::TOO_MANY_REQUESTS {
        if body_lower.contains("insufficient_quota")
            || body_lower.contains("quota")
            || body_lower.contains("monthly")
            || body_lower.contains("spend limit")
        {
            return probe_result(ProbeResultInput {
                provider,
                model,
                status: "unhealthy",
                can_call: false,
                auth_valid: true,
                quota_state: "exhausted",
                rate_limit_state: "limited",
                error_kind: Some("quota_exhausted"),
                message: Some(message),
                rate_limits,
                latency_ms,
            });
        }
        return rate_limited_probe_with_latency(
            provider,
            model,
            config.health_llm_rate_limit_unhealthy,
            rate_limits,
            &message,
            latency_ms,
        );
    }
    if status.is_server_error() {
        return probe_result(ProbeResultInput {
            provider,
            model,
            status: "degraded",
            can_call: false,
            auth_valid: true,
            quota_state: "unknown",
            rate_limit_state: "unknown",
            error_kind: Some("server_error"),
            message: Some(message),
            rate_limits,
            latency_ms,
        });
    }
    probe_result(ProbeResultInput {
        provider,
        model,
        status: "unhealthy",
        can_call: false,
        auth_valid: true,
        quota_state: "unknown",
        rate_limit_state: "unknown",
        error_kind: Some("request_failed"),
        message: Some(message),
        rate_limits,
        latency_ms,
    })
}

fn ok_probe(
    provider: String,
    model: String,
    rate_limits: RateLimitSnapshot,
) -> LlmHealthProbeResult {
    ok_probe_with_latency(provider, model, rate_limits, 0)
}

fn ok_probe_with_latency(
    provider: String,
    model: String,
    rate_limits: RateLimitSnapshot,
    latency_ms: u64,
) -> LlmHealthProbeResult {
    probe_result(ProbeResultInput {
        provider,
        model,
        status: "ok",
        can_call: true,
        auth_valid: true,
        quota_state: "available",
        rate_limit_state: "ok",
        error_kind: None,
        message: None,
        rate_limits,
        latency_ms,
    })
}

fn disabled_probe(config: &Config) -> LlmHealthProbeResult {
    probe_result(ProbeResultInput {
        provider: config.llm_provider.clone(),
        model: config
            .llm_model
            .clone()
            .unwrap_or_else(|| "none".to_string()),
        status: "disabled",
        can_call: !config.health_require_llm,
        auth_valid: !config.health_require_llm,
        quota_state: "unknown",
        rate_limit_state: "unknown",
        error_kind: None,
        message: Some("LLM health probing is disabled".to_string()),
        rate_limits: RateLimitSnapshot::default(),
        latency_ms: 0,
    })
}

fn unhealthy_probe(
    provider: String,
    model: String,
    error_kind: &str,
    message: &str,
) -> LlmHealthProbeResult {
    probe_result(ProbeResultInput {
        provider,
        model,
        status: "unhealthy",
        can_call: false,
        auth_valid: false,
        quota_state: "unknown",
        rate_limit_state: "unknown",
        error_kind: Some(error_kind),
        message: Some(message.to_string()),
        rate_limits: RateLimitSnapshot::default(),
        latency_ms: 0,
    })
}

fn auth_failure_probe(provider: String, model: String, message: &str) -> LlmHealthProbeResult {
    probe_result(ProbeResultInput {
        provider,
        model,
        status: "unhealthy",
        can_call: false,
        auth_valid: false,
        quota_state: "unknown",
        rate_limit_state: "unknown",
        error_kind: Some("auth_failed"),
        message: Some(message.to_string()),
        rate_limits: RateLimitSnapshot::default(),
        latency_ms: 0,
    })
}

fn quota_exhausted_probe(provider: String, model: String, message: &str) -> LlmHealthProbeResult {
    probe_result(ProbeResultInput {
        provider,
        model,
        status: "unhealthy",
        can_call: false,
        auth_valid: true,
        quota_state: "exhausted",
        rate_limit_state: "limited",
        error_kind: Some("quota_exhausted"),
        message: Some(message.to_string()),
        rate_limits: RateLimitSnapshot::default(),
        latency_ms: 0,
    })
}

fn rate_limited_probe(
    provider: String,
    model: String,
    unhealthy: bool,
    rate_limits: RateLimitSnapshot,
    message: &str,
) -> LlmHealthProbeResult {
    rate_limited_probe_with_latency(provider, model, unhealthy, rate_limits, message, 0)
}

fn rate_limited_probe_with_latency(
    provider: String,
    model: String,
    unhealthy: bool,
    rate_limits: RateLimitSnapshot,
    message: &str,
    latency_ms: u64,
) -> LlmHealthProbeResult {
    probe_result(ProbeResultInput {
        provider,
        model,
        status: if unhealthy { "unhealthy" } else { "degraded" },
        can_call: false,
        auth_valid: true,
        quota_state: "available",
        rate_limit_state: "limited",
        error_kind: Some("rate_limited"),
        message: Some(message.to_string()),
        rate_limits,
        latency_ms,
    })
}

fn degraded_probe(
    provider: String,
    model: String,
    error_kind: &str,
    message: &str,
) -> LlmHealthProbeResult {
    probe_result(ProbeResultInput {
        provider,
        model,
        status: "degraded",
        can_call: false,
        auth_valid: true,
        quota_state: "unknown",
        rate_limit_state: "unknown",
        error_kind: Some(error_kind),
        message: Some(message.to_string()),
        rate_limits: RateLimitSnapshot::default(),
        latency_ms: 0,
    })
}

struct ProbeResultInput<'a> {
    provider: String,
    model: String,
    status: &'a str,
    can_call: bool,
    auth_valid: bool,
    quota_state: &'a str,
    rate_limit_state: &'a str,
    error_kind: Option<&'a str>,
    message: Option<String>,
    rate_limits: RateLimitSnapshot,
    latency_ms: u64,
}

fn probe_result(input: ProbeResultInput<'_>) -> LlmHealthProbeResult {
    LlmHealthProbeResult {
        provider: input.provider,
        model: input.model,
        reasoning_effort: None,
        status: input.status.to_string(),
        can_call: input.can_call,
        auth_valid: input.auth_valid,
        quota_state: input.quota_state.to_string(),
        rate_limit_state: input.rate_limit_state.to_string(),
        checked_at: Utc::now(),
        latency_ms: input.latency_ms,
        stale: false,
        age_seconds: 0,
        consecutive_failures: 0,
        rate_limits: input.rate_limits,
        error_kind: input.error_kind.map(ToString::to_string),
        message: input.message,
    }
}

fn is_threshold_failure(result: &LlmHealthProbeResult) -> bool {
    matches!(
        result.error_kind.as_deref(),
        Some("server_error" | "timeout")
    )
}

fn rate_limits_from_headers(headers: &HeaderMap) -> RateLimitSnapshot {
    RateLimitSnapshot {
        remaining_requests: header_value(headers, "x-ratelimit-remaining-requests"),
        remaining_tokens: header_value(headers, "x-ratelimit-remaining-tokens"),
        reset_requests: header_value(headers, "x-ratelimit-reset-requests"),
        reset_tokens: header_value(headers, "x-ratelimit-reset-tokens"),
    }
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn extract_error_message(body: &str) -> Option<String> {
    if let Ok(value) = serde_json::from_str::<Value>(body) {
        if let Some(error) = value.get("error") {
            if let Some(message) = error
                .get("message")
                .and_then(Value::as_str)
                .or_else(|| error.get("code").and_then(Value::as_str))
                .or_else(|| error.get("type").and_then(Value::as_str))
            {
                return Some(message.to_string());
            }
        }
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.chars().take(240).collect())
    }
}

pub fn read_codex_auth_token(path: &str) -> Option<String> {
    read_codex_auth_credentials(path).map(|credentials| credentials.token)
}

pub fn read_codex_auth_credentials(path: &str) -> Option<CodexAuthCredentials> {
    let path = Path::new(path);
    let content = std::fs::read_to_string(path).ok()?;
    let json = serde_json::from_str::<Value>(&content).ok()?;
    let account_id = json
        .get("tokens")
        .and_then(|tokens| tokens.get("account_id"))
        .and_then(Value::as_str)
        .or_else(|| json.get("account_id").and_then(Value::as_str))
        .map(ToString::to_string);

    let top_level_keys = [
        "api_key",
        "openai_api_key",
        "OPENAI_API_KEY",
        "access_token",
        "token",
    ];
    if let Some(credentials) = top_level_keys.iter().find_map(|key| {
        json.get(*key)
            .and_then(Value::as_str)
            .map(|token| credentials_from_token(key, token, account_id.clone()))
    }) {
        return Some(credentials);
    }

    json.get("tokens").and_then(|tokens| {
        top_level_keys.iter().find_map(|key| {
            tokens
                .get(*key)
                .and_then(Value::as_str)
                .map(|token| credentials_from_token(key, token, account_id.clone()))
        })
    })
}

fn credentials_from_token(
    key: &str,
    token: &str,
    account_id: Option<String>,
) -> CodexAuthCredentials {
    CodexAuthCredentials {
        token: token.to_string(),
        account_id,
        token_kind: codex_token_kind(key, token),
    }
}

fn codex_token_kind(key: &str, token: &str) -> CodexAuthTokenKind {
    let normalized_key = key.to_ascii_lowercase();
    if normalized_key.contains("api_key") || token.starts_with("sk-") {
        CodexAuthTokenKind::OpenAiApiKey
    } else if normalized_key == "access_token" || looks_like_jwt(token) {
        CodexAuthTokenKind::CodexOauth
    } else {
        CodexAuthTokenKind::Other
    }
}

fn looks_like_jwt(token: &str) -> bool {
    token.split('.').count() == 3
}

fn extract_response_text(value: &Value) -> Option<String> {
    let output = value.get("output")?.as_array()?;
    let mut text = String::new();
    for item in output {
        let Some(content) = item.get("content").and_then(Value::as_array) else {
            continue;
        };
        for part in content {
            if part.get("type").and_then(Value::as_str) == Some("output_text") {
                if let Some(part_text) = part.get("text").and_then(Value::as_str) {
                    text.push_str(part_text);
                }
            }
        }
    }
    if text.is_empty() {
        None
    } else {
        Some(text)
    }
}

fn extract_codex_sse_text(body: &str) -> Option<String> {
    let mut deltas = String::new();
    let mut done_text: Option<String> = None;
    let mut completed_text: Option<String> = None;

    for line in body.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        match value.get("type").and_then(Value::as_str) {
            Some("response.output_text.delta") => {
                if let Some(delta) = value.get("delta").and_then(Value::as_str) {
                    deltas.push_str(delta);
                }
            }
            Some("response.output_text.done") => {
                if let Some(text) = value.get("text").and_then(Value::as_str) {
                    done_text = Some(text.to_string());
                }
            }
            Some("response.completed") => {
                completed_text = value.get("response").and_then(extract_response_text);
            }
            _ => {}
        }
    }

    if let Some(text) = done_text.filter(|text| !text.is_empty()) {
        Some(text)
    } else if !deltas.is_empty() {
        Some(deltas)
    } else {
        completed_text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codex_sse_text_prefers_done_text() {
        let body = format!(
            "event: response.output_text.delta\ndata: {}\n\nevent: response.output_text.done\ndata: {}\n\n",
            json!({
                "type": "response.output_text.delta",
                "delta": "partial"
            }),
            json!({
                "type": "response.output_text.done",
                "text": "final"
            })
        );

        assert_eq!(extract_codex_sse_text(&body).as_deref(), Some("final"));
    }

    #[test]
    fn codex_auth_credentials_include_account_id_and_oauth_kind() {
        let path = std::env::temp_dir().join(format!(
            "nowledge-codex-credentials-{}.json",
            uuid::Uuid::now_v7()
        ));
        std::fs::write(
            &path,
            json!({
                "tokens": {
                    "access_token": "header.payload.signature",
                    "account_id": "acct-test"
                }
            })
            .to_string(),
        )
        .unwrap();

        let credentials = read_codex_auth_credentials(&path.to_string_lossy()).unwrap();
        let _ = std::fs::remove_file(&path);
        assert_eq!(credentials.token, "header.payload.signature");
        assert_eq!(credentials.account_id.as_deref(), Some("acct-test"));
        assert_eq!(credentials.token_kind, CodexAuthTokenKind::CodexOauth);
    }

    #[test]
    fn codex_payload_includes_reasoning_effort() {
        let payload = codex_responses_payload("gpt-5.5", "hello", Some("xhigh"));

        assert_eq!(
            payload
                .get("reasoning")
                .and_then(|reasoning| reasoning.get("effort"))
                .and_then(Value::as_str),
            Some("xhigh")
        );
    }

    #[test]
    fn codex_payload_omits_empty_reasoning_effort() {
        let payload = codex_responses_payload("gpt-5.5", "hello", Some(" "));

        assert!(payload.get("reasoning").is_none());
    }
}
