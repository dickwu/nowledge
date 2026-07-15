use std::collections::VecDeque;

use serde_json::{json, Value};

use crate::{
    app::AppState,
    error::ApiError,
    llm::{LlmProfile, LlmStreamEvent, LlmTextStream, LlmTokenUsage},
    llm_service::{known_secrets_for_state, merge_token_usage},
    models::RagAnswerRequest,
    rag_service::build_rag_llm_request,
    util::{redact_secrets, StreamingTextRedactor},
};

pub(crate) struct RagStreamService;

impl RagStreamService {
    pub(crate) async fn open(
        state: &AppState,
        req: RagAnswerRequest,
        is_admin: bool,
        provider_budget_key: &str,
        request_id: &str,
    ) -> Result<RagStreamSession, ApiError> {
        let answer = state
            .store
            .answer_rag_async(state.tenant_id(), req.clone(), is_admin)
            .await?;
        let status = state.llm_providers.status(LlmProfile::Primary).await;
        let grounded = !answer.citations.is_empty();
        let backend = state.store.backend_name().to_string();

        let provider_stream = if state.config.llm_provider == "none" {
            None
        } else {
            let security = state.config.provider_security_snapshot();
            let request = build_rag_llm_request(
                &req.question.unwrap_or_default(),
                &answer.citations,
                &security.secrets,
                state.config.llm_max_output_tokens,
            );
            Some(
                state
                    .llm_providers
                    .stream_text(LlmProfile::Primary, provider_budget_key, request)
                    .await?,
            )
        };

        let known_secrets = known_secrets_for_state(state);
        let provider = provider_stream
            .as_ref()
            .map(|stream| stream.provider.clone())
            .unwrap_or(status.provider);
        let model = provider_stream
            .as_ref()
            .map(|stream| stream.model.clone())
            .unwrap_or(status.model);

        let mut pending = VecDeque::new();
        pending.push_back(RagStreamEvent::new(
            "meta",
            redact_secrets(
                &json!({
                    "answer_id": answer.answer_id,
                    "trace_id": answer.trace_id,
                    "provider": provider,
                    "model": model,
                    "backend": backend,
                    "grounded": grounded
                }),
                &known_secrets,
            ),
        ));
        for citation in &answer.citations {
            let citation = serde_json::to_value(citation)
                .map_err(|error| ApiError::Internal(error.to_string()))?;
            pending.push_back(RagStreamEvent::new(
                "citation",
                redact_secrets(&citation, &known_secrets),
            ));
        }

        let mut session = RagStreamSession {
            pending,
            provider_stream,
            route_redactor: StreamingTextRedactor::new(&known_secrets),
            known_secrets,
            request_id: request_id.to_string(),
            answer_id: answer.answer_id,
            trace_id: answer.trace_id,
            provider,
            model,
            backend,
            grounded,
            completed: false,
        };

        if session.provider_stream.is_none() {
            let delta = session.route_redactor.push(&answer.answer);
            if !delta.is_empty() {
                session
                    .pending
                    .push_back(RagStreamEvent::new("delta", json!({ "text": delta })));
            }
            let tail =
                std::mem::replace(&mut session.route_redactor, StreamingTextRedactor::new(&[]))
                    .finish();
            if !tail.is_empty() {
                session
                    .pending
                    .push_back(RagStreamEvent::new("delta", json!({ "text": tail })));
            }
            let usage = stream_usage(
                answer.usage,
                &session.provider,
                &session.model,
                &session.backend,
                session.grounded,
                None,
                None,
            );
            session.pending.push_back(RagStreamEvent::new(
                "usage",
                redact_secrets(&usage, &session.known_secrets),
            ));
            session.pending.push_back(RagStreamEvent::new(
                "done",
                json!({
                    "answer_id": session.answer_id,
                    "trace_id": session.trace_id
                }),
            ));
            session.completed = true;
        }

        Ok(session)
    }
}

pub(crate) struct RagStreamEvent {
    name: &'static str,
    data: Value,
}

impl RagStreamEvent {
    fn new(name: &'static str, data: Value) -> Self {
        Self { name, data }
    }

    pub(crate) fn into_parts(self) -> (&'static str, Value) {
        (self.name, self.data)
    }
}

pub(crate) struct RagStreamSession {
    pending: VecDeque<RagStreamEvent>,
    provider_stream: Option<LlmTextStream>,
    route_redactor: StreamingTextRedactor,
    known_secrets: Vec<String>,
    request_id: String,
    answer_id: String,
    trace_id: String,
    provider: String,
    model: String,
    backend: String,
    grounded: bool,
    completed: bool,
}

impl RagStreamSession {
    pub(crate) async fn next_event(&mut self) -> Option<RagStreamEvent> {
        loop {
            if let Some(event) = self.pending.pop_front() {
                return Some(event);
            }
            if self.completed {
                return None;
            }

            let next = match self.provider_stream.as_mut() {
                Some(provider_stream) => provider_stream.next_event().await,
                None => return None,
            };
            match next {
                Ok(Some(LlmStreamEvent::Delta(delta))) => {
                    let delta = self.route_redactor.push(&delta);
                    if !delta.is_empty() {
                        return Some(RagStreamEvent::new("delta", json!({ "text": delta })));
                    }
                }
                Ok(Some(LlmStreamEvent::Completed { latency_ms, usage })) => {
                    let redactor = std::mem::replace(
                        &mut self.route_redactor,
                        StreamingTextRedactor::new(&[]),
                    );
                    let tail = redactor.finish();
                    if !tail.is_empty() {
                        self.pending
                            .push_back(RagStreamEvent::new("delta", json!({ "text": tail })));
                    }
                    let usage = stream_usage(
                        json!({}),
                        &self.provider,
                        &self.model,
                        &self.backend,
                        self.grounded,
                        Some(latency_ms),
                        usage.as_ref(),
                    );
                    self.pending.push_back(RagStreamEvent::new(
                        "usage",
                        redact_secrets(&usage, &self.known_secrets),
                    ));
                    self.pending.push_back(RagStreamEvent::new(
                        "done",
                        json!({
                            "answer_id": self.answer_id,
                            "trace_id": self.trace_id
                        }),
                    ));
                    self.provider_stream.take();
                    self.completed = true;
                }
                Ok(None) => {
                    self.queue_error(ApiError::Upstream(
                        "LLM stream ended without a completion event".to_string(),
                    ));
                }
                Err(error) => self.queue_error(error),
            }
        }
    }

    fn queue_error(&mut self, error: ApiError) {
        let redactor = std::mem::replace(&mut self.route_redactor, StreamingTextRedactor::new(&[]));
        redactor.abort();
        self.provider_stream.take();
        let envelope = serde_json::to_value(error.public_body(Some(&self.request_id)))
            .unwrap_or_else(|_| {
                json!({
                    "error": {
                        "code": "internal_error",
                        "message": "internal server error",
                        "details": { "status": 500, "request_id": self.request_id }
                    }
                })
            });
        self.pending.push_back(RagStreamEvent::new(
            "error",
            redact_secrets(&envelope, &self.known_secrets),
        ));
        self.completed = true;
    }
}

fn stream_usage(
    mut usage: Value,
    provider: &str,
    model: &str,
    backend: &str,
    grounded: bool,
    latency_ms: Option<u64>,
    tokens: Option<&LlmTokenUsage>,
) -> Value {
    if !usage.is_object() {
        usage = json!({});
    }
    usage["provider"] = json!(provider);
    usage["model"] = json!(model);
    usage["backend"] = json!(backend);
    usage["grounded"] = json!(grounded);
    if let Some(latency_ms) = latency_ms {
        usage["latency_ms"] = json!(latency_ms);
    }
    if let Some(tokens) = tokens {
        merge_token_usage(&mut usage, tokens);
    }
    usage
}
