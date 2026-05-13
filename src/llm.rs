use std::{path::Path, time::Instant};

use async_trait::async_trait;
use serde_json::{json, Value};

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
    auth_source: String,
    api_key: Option<String>,
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
        let response = self
            .client
            .post("https://api.openai.com/v1/responses")
            .bearer_auth(api_key)
            .json(&json!({
                "model": self.model,
                "input": request.prompt,
                "store": false
            }))
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;

        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "OpenAI Responses API request failed: {}",
                response.status()
            )));
        }

        let body = response
            .json::<Value>()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;
        Ok(LlmTextResponse {
            text: extract_response_text(&body)
                .unwrap_or_else(|| "LLM response did not contain output text".to_string()),
            latency_ms: started.elapsed().as_millis() as u64,
        })
    }
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
            auth_source: "RAG_OPENAI_API_KEY".to_string(),
            api_key: config.openai_api_key.clone(),
            client: reqwest::Client::new(),
        }),
        "codex_auth" => Box::new(OpenAiResponsesClient {
            provider: "codex_auth".to_string(),
            model,
            auth_source: config
                .codex_auth_path
                .clone()
                .unwrap_or_else(|| "explicit_path_missing".to_string()),
            api_key: config
                .codex_auth_path
                .as_deref()
                .and_then(read_codex_auth_token),
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

fn read_codex_auth_token(path: &str) -> Option<String> {
    let path = Path::new(path);
    let content = std::fs::read_to_string(path).ok()?;
    let json = serde_json::from_str::<Value>(&content).ok()?;
    ["api_key", "openai_api_key", "access_token", "token"]
        .iter()
        .find_map(|key| json.get(*key).and_then(Value::as_str))
        .map(ToString::to_string)
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
