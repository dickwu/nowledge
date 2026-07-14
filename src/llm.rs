use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, OnceLock, RwLock},
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
    /// Real token counts reported by the upstream provider, when available.
    pub usage: Option<LlmTokenUsage>,
}

/// Token counts as reported by the provider (OpenAI/Codex Responses API).
/// Serialized flat into API `usage` blocks; absent fields are omitted so
/// downstream consumers can distinguish "reported" from "unknown".
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
pub struct LlmTokenUsage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_output_tokens: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total_tokens: Option<u64>,
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
    /// When this snapshot was observed on a live upstream response.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub captured_at: Option<DateTime<Utc>>,
    /// ChatGPT/Codex subscription plan (`x-codex-plan-type`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_type: Option<String>,
    /// Which limit bucket is currently governing (`x-codex-active-limit`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_limit: Option<String>,
    /// Codex short-window budget (5h rolling window).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<RateLimitWindow>,
    /// Codex long-window budget (weekly rolling window).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary: Option<RateLimitWindow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credits: Option<CodexCredits>,
    /// Model-scoped limit buckets (`x-codex-{bucket}-primary-...` families).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub additional_limits: Vec<NamedRateLimit>,
}

impl RateLimitSnapshot {
    pub fn has_data(&self) -> bool {
        self.remaining_requests.is_some()
            || self.remaining_tokens.is_some()
            || self.reset_requests.is_some()
            || self.reset_tokens.is_some()
            || self.plan_type.is_some()
            || self.active_limit.is_some()
            || self.primary.is_some()
            || self.secondary.is_some()
            || self.credits.is_some()
            || !self.additional_limits.is_empty()
    }
}

/// One rolling rate-limit window as reported by the Codex backend.
/// `remaining_percent` is derived (`100 - used_percent`, clamped) so status
/// consumers can render "left available usage" without recomputing.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RateLimitWindow {
    pub used_percent: f64,
    pub remaining_percent: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_minutes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resets_in_seconds: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CodexCredits {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_credits: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unlimited: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub balance: Option<String>,
}

/// A named model-scoped limit bucket, e.g. the `bengalfox` family carrying
/// `x-codex-bengalfox-limit-name: GPT-5.3-Codex-Spark`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NamedRateLimit {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary: Option<RateLimitWindow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary: Option<RateLimitWindow>,
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
            usage: None,
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
        let text = format!(
            "mock summary: {}",
            request.prompt.chars().take(160).collect::<String>()
        );
        // Deterministic synthetic counts so downstream usage plumbing is
        // testable without a live provider.
        let input_tokens = (request.prompt.chars().count() as u64 / 4).max(1);
        let output_tokens = (text.chars().count() as u64 / 4).max(1);
        Ok(LlmTextResponse {
            text,
            latency_ms: started.elapsed().as_millis() as u64,
            usage: Some(LlmTokenUsage {
                input_tokens: Some(input_tokens),
                cached_input_tokens: Some(0),
                output_tokens: Some(output_tokens),
                reasoning_output_tokens: Some(0),
                total_tokens: Some(input_tokens + output_tokens),
            }),
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
            &self.provider,
        )
        .await?;
        Ok(LlmTextResponse {
            text: extract_response_text(&body)
                .unwrap_or_else(|| "LLM response did not contain output text".to_string()),
            latency_ms: started.elapsed().as_millis() as u64,
            usage: token_usage_from_value(body.get("usage")),
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
                "codex_auth",
            )
            .await?;
            return Ok(LlmTextResponse {
                text: extract_response_text(&body)
                    .unwrap_or_else(|| "LLM response did not contain output text".to_string()),
                latency_ms: started.elapsed().as_millis() as u64,
                usage: token_usage_from_value(body.get("usage")),
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
        record_rate_limit_snapshot("codex_auth", &rate_limits_from_headers(response.headers()));
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
            usage: extract_codex_sse_usage(&body),
        })
    }
}

async fn complete_openai_responses(
    client: &reqwest::Client,
    model: &str,
    reasoning_effort: Option<&str>,
    api_key: &str,
    prompt: &str,
    provider_label: &str,
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
    record_rate_limit_snapshot(
        provider_label,
        &rate_limits_from_headers(response.headers()),
    );
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
    llm_client_from_config_with_credentials(config, config.codex_auth_credentials())
}

pub(crate) fn llm_client_from_config_with_credentials(
    config: &Config,
    codex_credentials: Option<CodexAuthCredentials>,
) -> Box<dyn LlmClient> {
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
            credentials: codex_credentials,
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
                ..RateLimitSnapshot::default()
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
                ..RateLimitSnapshot::default()
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
            if config.codex_auth_path.is_none() {
                return auth_failure_probe(provider, model, "Codex auth path is not configured");
            }
            let Some(credentials) = config.codex_auth_credentials() else {
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
    record_rate_limit_snapshot(&provider, &rate_limits);
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
    record_rate_limit_snapshot(&provider, &rate_limits);
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
    // A successful call can still be close to the budget ceiling; surface
    // that as a soft state so dashboards can warn before hard 429s begin.
    let rate_limit_state = codex_rate_limit_state(&rate_limits).unwrap_or("ok");
    probe_result(ProbeResultInput {
        provider,
        model,
        status: "ok",
        can_call: true,
        auth_valid: true,
        quota_state: "available",
        rate_limit_state,
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

/// Last live rate-limit snapshot per provider, updated on every upstream
/// response (health probes and real completions alike). LLM clients are
/// constructed per call, so this is the one shared place status surfaces can
/// read the freshest "left available usage" from without issuing a new call.
static LATEST_RATE_LIMITS: OnceLock<RwLock<HashMap<String, RateLimitSnapshot>>> = OnceLock::new();

fn latest_rate_limits_store() -> &'static RwLock<HashMap<String, RateLimitSnapshot>> {
    LATEST_RATE_LIMITS.get_or_init(|| RwLock::new(HashMap::new()))
}

pub fn record_rate_limit_snapshot(provider: &str, snapshot: &RateLimitSnapshot) {
    if !snapshot.has_data() {
        return;
    }
    let stamped = RateLimitSnapshot {
        captured_at: Some(Utc::now()),
        ..snapshot.clone()
    };
    if let Ok(mut store) = latest_rate_limits_store().write() {
        store.insert(provider.to_string(), stamped);
    }
}

pub fn latest_rate_limit_snapshot(provider: &str) -> Option<RateLimitSnapshot> {
    latest_rate_limits_store()
        .read()
        .ok()
        .and_then(|store| store.get(provider).cloned())
}

/// Rate limits to render on status surfaces: the freshest live snapshot for
/// the probe's provider, falling back to whatever the probe itself captured
/// (mock providers synthesize snapshots without touching the live store).
pub fn effective_rate_limits(probe: &LlmHealthProbeResult) -> RateLimitSnapshot {
    latest_rate_limit_snapshot(&probe.provider).unwrap_or_else(|| probe.rate_limits.clone())
}

fn rate_limits_from_headers(headers: &HeaderMap) -> RateLimitSnapshot {
    RateLimitSnapshot {
        remaining_requests: header_value(headers, "x-ratelimit-remaining-requests"),
        remaining_tokens: header_value(headers, "x-ratelimit-remaining-tokens"),
        reset_requests: header_value(headers, "x-ratelimit-reset-requests"),
        reset_tokens: header_value(headers, "x-ratelimit-reset-tokens"),
        captured_at: None,
        plan_type: header_value(headers, "x-codex-plan-type"),
        active_limit: header_value(headers, "x-codex-active-limit"),
        primary: codex_window_from_headers(headers, "x-codex-primary"),
        secondary: codex_window_from_headers(headers, "x-codex-secondary"),
        credits: codex_credits_from_headers(headers),
        additional_limits: codex_named_limits_from_headers(headers),
    }
}

fn codex_window_from_headers(headers: &HeaderMap, prefix: &str) -> Option<RateLimitWindow> {
    let used_percent = header_f64(headers, &format!("{prefix}-used-percent"))?;
    Some(RateLimitWindow {
        used_percent,
        remaining_percent: (100.0 - used_percent).clamp(0.0, 100.0),
        window_minutes: header_u64(headers, &format!("{prefix}-window-minutes")),
        resets_in_seconds: header_u64(headers, &format!("{prefix}-reset-after-seconds")),
        resets_at: header_u64(headers, &format!("{prefix}-reset-at"))
            .and_then(|ts| DateTime::<Utc>::from_timestamp(ts as i64, 0)),
    })
}

fn codex_credits_from_headers(headers: &HeaderMap) -> Option<CodexCredits> {
    let has_credits = header_bool(headers, "x-codex-credits-has-credits");
    let unlimited = header_bool(headers, "x-codex-credits-unlimited");
    let balance =
        header_value(headers, "x-codex-credits-balance").filter(|value| !value.trim().is_empty());
    if has_credits.is_none() && unlimited.is_none() && balance.is_none() {
        return None;
    }
    Some(CodexCredits {
        has_credits,
        unlimited,
        balance,
    })
}

fn codex_named_limits_from_headers(headers: &HeaderMap) -> Vec<NamedRateLimit> {
    let mut names: Vec<String> = headers
        .keys()
        .filter_map(|name| {
            let rest = name.as_str().strip_prefix("x-codex-")?;
            let bucket = rest
                .strip_suffix("-primary-used-percent")
                .or_else(|| rest.strip_suffix("-secondary-used-percent"))?;
            if bucket.is_empty() {
                None
            } else {
                Some(bucket.to_string())
            }
        })
        .collect();
    names.sort();
    names.dedup();
    names
        .into_iter()
        .map(|name| NamedRateLimit {
            limit_name: header_value(headers, &format!("x-codex-{name}-limit-name")),
            primary: codex_window_from_headers(headers, &format!("x-codex-{name}-primary")),
            secondary: codex_window_from_headers(headers, &format!("x-codex-{name}-secondary")),
            name,
        })
        .collect()
}

/// Worst-case state across the plain Codex windows; `None` means the snapshot
/// carries no window data (OpenAI-key style or mock providers).
fn codex_rate_limit_state(snapshot: &RateLimitSnapshot) -> Option<&'static str> {
    let mut worst = None;
    for window in [snapshot.primary.as_ref(), snapshot.secondary.as_ref()]
        .into_iter()
        .flatten()
    {
        if window.used_percent >= 100.0 {
            return Some("limited");
        }
        if window.used_percent >= 90.0 {
            worst = Some("near_limit");
        }
    }
    worst
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn header_f64(headers: &HeaderMap, name: &str) -> Option<f64> {
    header_value(headers, name)?
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    header_value(headers, name)?.trim().parse::<u64>().ok()
}

fn header_bool(headers: &HeaderMap, name: &str) -> Option<bool> {
    match header_value(headers, name)?
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

/// Extract token counts from a Responses API `usage` object
/// (`input_tokens` / `output_tokens` / `total_tokens` plus cached and
/// reasoning detail counters).
pub fn token_usage_from_value(usage: Option<&Value>) -> Option<LlmTokenUsage> {
    let usage = usage?;
    let input_tokens = usage.get("input_tokens").and_then(Value::as_u64);
    let output_tokens = usage.get("output_tokens").and_then(Value::as_u64);
    let total_tokens = usage.get("total_tokens").and_then(Value::as_u64);
    if input_tokens.is_none() && output_tokens.is_none() && total_tokens.is_none() {
        return None;
    }
    Some(LlmTokenUsage {
        input_tokens,
        cached_input_tokens: usage
            .get("input_tokens_details")
            .and_then(|details| details.get("cached_tokens"))
            .and_then(Value::as_u64),
        output_tokens,
        reasoning_output_tokens: usage
            .get("output_tokens_details")
            .and_then(|details| details.get("reasoning_tokens"))
            .and_then(Value::as_u64),
        total_tokens: total_tokens.or(match (input_tokens, output_tokens) {
            (Some(input), Some(output)) => Some(input + output),
            _ => None,
        }),
    })
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

/// Pull real token counts out of the terminal `response.completed` SSE event.
fn extract_codex_sse_usage(body: &str) -> Option<LlmTokenUsage> {
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
        if value.get("type").and_then(Value::as_str) == Some("response.completed") {
            return token_usage_from_value(
                value
                    .get("response")
                    .and_then(|response| response.get("usage")),
            );
        }
    }
    None
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

    fn codex_headers() -> HeaderMap {
        use reqwest::header::HeaderValue;
        let mut headers = HeaderMap::new();
        for (name, value) in [
            ("x-codex-primary-used-percent", "0"),
            ("x-codex-secondary-used-percent", "55"),
            ("x-codex-primary-window-minutes", "300"),
            ("x-codex-secondary-window-minutes", "10080"),
            ("x-codex-primary-reset-after-seconds", "18000"),
            ("x-codex-secondary-reset-after-seconds", "18058"),
            ("x-codex-primary-reset-at", "1783414681"),
            ("x-codex-secondary-reset-at", "1783414739"),
            ("x-codex-plan-type", "prolite"),
            ("x-codex-active-limit", "premium"),
            ("x-codex-credits-has-credits", "False"),
            ("x-codex-credits-unlimited", "False"),
            ("x-codex-credits-balance", ""),
            ("x-codex-bengalfox-primary-used-percent", "4"),
            ("x-codex-bengalfox-secondary-used-percent", "49"),
            ("x-codex-bengalfox-limit-name", "GPT-5.3-Codex-Spark"),
        ] {
            headers.insert(name, HeaderValue::from_static(value));
        }
        headers
    }

    #[test]
    fn codex_rate_limit_headers_parse_into_windows() {
        let snapshot = rate_limits_from_headers(&codex_headers());

        let primary = snapshot.primary.as_ref().expect("primary window");
        assert_eq!(primary.used_percent, 0.0);
        assert_eq!(primary.remaining_percent, 100.0);
        assert_eq!(primary.window_minutes, Some(300));
        assert_eq!(primary.resets_in_seconds, Some(18000));
        assert!(primary.resets_at.is_some());

        let secondary = snapshot.secondary.as_ref().expect("secondary window");
        assert_eq!(secondary.used_percent, 55.0);
        assert_eq!(secondary.remaining_percent, 45.0);
        assert_eq!(secondary.window_minutes, Some(10080));

        assert_eq!(snapshot.plan_type.as_deref(), Some("prolite"));
        assert_eq!(snapshot.active_limit.as_deref(), Some("premium"));
        let credits = snapshot.credits.as_ref().expect("credits");
        assert_eq!(credits.has_credits, Some(false));
        assert_eq!(credits.unlimited, Some(false));
        assert_eq!(credits.balance, None);

        assert_eq!(snapshot.additional_limits.len(), 1);
        let bucket = &snapshot.additional_limits[0];
        assert_eq!(bucket.name, "bengalfox");
        assert_eq!(bucket.limit_name.as_deref(), Some("GPT-5.3-Codex-Spark"));
        assert_eq!(
            bucket
                .primary
                .as_ref()
                .map(|window| window.remaining_percent),
            Some(96.0)
        );
        assert!(snapshot.has_data());
    }

    #[test]
    fn rate_limit_state_reflects_window_pressure() {
        let calm = rate_limits_from_headers(&codex_headers());
        assert_eq!(codex_rate_limit_state(&calm), None);

        let near = RateLimitSnapshot {
            secondary: Some(RateLimitWindow {
                used_percent: 92.0,
                remaining_percent: 8.0,
                ..RateLimitWindow::default()
            }),
            ..RateLimitSnapshot::default()
        };
        assert_eq!(codex_rate_limit_state(&near), Some("near_limit"));

        let exhausted = RateLimitSnapshot {
            primary: Some(RateLimitWindow {
                used_percent: 100.0,
                remaining_percent: 0.0,
                ..RateLimitWindow::default()
            }),
            ..RateLimitSnapshot::default()
        };
        assert_eq!(codex_rate_limit_state(&exhausted), Some("limited"));

        let probe = ok_probe_with_latency("codex_auth".to_string(), "gpt-5.5".to_string(), near, 5);
        assert_eq!(probe.rate_limit_state, "near_limit");
        assert!(probe.can_call);
    }

    #[test]
    fn codex_sse_usage_comes_from_response_completed() {
        let body = format!(
            "data: {}\n\ndata: {}\n\n",
            json!({
                "type": "response.output_text.done",
                "text": "ok"
            }),
            json!({
                "type": "response.completed",
                "response": {
                    "usage": {
                        "input_tokens": 21,
                        "input_tokens_details": {"cached_tokens": 3},
                        "output_tokens": 5,
                        "output_tokens_details": {"reasoning_tokens": 2},
                        "total_tokens": 26
                    }
                }
            })
        );

        let usage = extract_codex_sse_usage(&body).expect("usage");
        assert_eq!(usage.input_tokens, Some(21));
        assert_eq!(usage.cached_input_tokens, Some(3));
        assert_eq!(usage.output_tokens, Some(5));
        assert_eq!(usage.reasoning_output_tokens, Some(2));
        assert_eq!(usage.total_tokens, Some(26));
    }

    #[test]
    fn token_usage_total_falls_back_to_sum() {
        let usage = token_usage_from_value(Some(&json!({
            "input_tokens": 10,
            "output_tokens": 6
        })))
        .expect("usage");
        assert_eq!(usage.total_tokens, Some(16));

        assert!(token_usage_from_value(Some(&json!({}))).is_none());
        assert!(token_usage_from_value(None).is_none());
    }

    #[test]
    fn latest_snapshot_store_records_only_real_data() {
        let provider = "test-provider-latest-snapshot";
        record_rate_limit_snapshot(provider, &RateLimitSnapshot::default());
        assert!(latest_rate_limit_snapshot(provider).is_none());

        record_rate_limit_snapshot(provider, &rate_limits_from_headers(&codex_headers()));
        let stored = latest_rate_limit_snapshot(provider).expect("stored snapshot");
        assert!(stored.captured_at.is_some());
        assert_eq!(
            stored
                .secondary
                .as_ref()
                .map(|window| window.remaining_percent),
            Some(45.0)
        );
    }
}
