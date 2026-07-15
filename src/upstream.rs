use std::{
    fmt,
    future::Future,
    str::FromStr,
    time::{Duration, SystemTime},
};

use chrono::{DateTime, Datelike, NaiveDateTime, Utc};
use reqwest::{
    header::{HeaderMap, HeaderValue, RETRY_AFTER},
    redirect::Policy as RedirectPolicy,
    Client, RequestBuilder, Response, StatusCode,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::time::{self, Instant};

pub const MAX_UPSTREAM_RETRIES: u8 = 2;
pub const X_CLIENT_REQUEST_ID: &str = "x-client-request-id";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProxyMode {
    #[default]
    System,
    Direct,
}

impl FromStr for ProxyMode {
    type Err = ProxyModeParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "system" => Ok(Self::System),
            "direct" => Ok(Self::Direct),
            _ => Err(ProxyModeParseError),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProxyModeParseError;

impl fmt::Display for ProxyModeParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("proxy mode must be `system` or `direct`")
    }
}

impl std::error::Error for ProxyModeParseError {}

#[derive(Debug, Clone)]
pub struct ClientPolicy {
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub read_timeout: Duration,
    pub proxy_mode: ProxyMode,
}

impl Default for ClientPolicy {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(60),
            read_timeout: Duration::from_secs(30),
            proxy_mode: ProxyMode::System,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientPolicyViolation {
    ZeroConnectTimeout,
    ZeroRequestTimeout,
    ZeroReadTimeout,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientBuildError {
    InvalidPolicy(ClientPolicyViolation),
    BuildFailed,
}

impl fmt::Display for ClientBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPolicy(violation) => {
                write!(formatter, "invalid upstream client policy: {violation:?}")
            }
            Self::BuildFailed => formatter.write_str("failed to build upstream HTTP client"),
        }
    }
}

impl std::error::Error for ClientBuildError {}

impl ClientPolicy {
    fn validate(&self) -> Result<(), ClientBuildError> {
        if self.connect_timeout.is_zero() {
            return Err(ClientBuildError::InvalidPolicy(
                ClientPolicyViolation::ZeroConnectTimeout,
            ));
        }
        if self.request_timeout.is_zero() {
            return Err(ClientBuildError::InvalidPolicy(
                ClientPolicyViolation::ZeroRequestTimeout,
            ));
        }
        if self.read_timeout.is_zero() {
            return Err(ClientBuildError::InvalidPolicy(
                ClientPolicyViolation::ZeroReadTimeout,
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamOperation {
    LlmCompletion,
    LlmHealth,
    ParserUpload,
    ParserHealth,
}

impl UpstreamOperation {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LlmCompletion => "llm_completion",
            Self::LlmHealth => "llm_health",
            Self::ParserUpload => "parser_upload",
            Self::ParserHealth => "parser_health",
        }
    }
}

#[derive(Debug, Clone)]
pub struct OperationPolicy {
    pub deadline: Duration,
    pub max_response_bytes: usize,
    pub max_retries: u8,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl OperationPolicy {
    pub fn without_retries(deadline: Duration, max_response_bytes: usize) -> Self {
        Self {
            deadline,
            max_response_bytes,
            max_retries: 0,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    fn validate(&self) -> Result<(), PolicyViolation> {
        if self.deadline.is_zero() {
            return Err(PolicyViolation::ZeroDeadline);
        }
        if self.max_response_bytes == 0 {
            return Err(PolicyViolation::ZeroResponseLimit);
        }
        if self.max_retries > MAX_UPSTREAM_RETRIES {
            return Err(PolicyViolation::TooManyRetries);
        }
        if self.initial_backoff > self.max_backoff {
            return Err(PolicyViolation::InvalidBackoffRange);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyViolation {
    ZeroDeadline,
    ZeroResponseLimit,
    TooManyRetries,
    InvalidBackoffRange,
    DeadlineOverflow,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestFactoryError {
    Io,
    InvalidInput,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportFailureKind {
    Connection,
    Timeout,
    RequestBody,
    Decode,
    Request,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HttpFailureKind {
    Authentication,
    RateLimited,
    Quota,
    RequestRejected,
    RequestTimeout,
    Server,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseDisposition {
    Success,
    Retryable(HttpFailureKind),
    Terminal(HttpFailureKind),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamFailureCategory {
    InvalidPolicy,
    InvalidRequestId,
    Deadline,
    RequestBuild,
    Connection,
    Timeout,
    RequestBody,
    Decode,
    Request,
    ResponseTooLarge,
    Authentication,
    RateLimited,
    Quota,
    RequestRejected,
    Server,
    Other,
}

impl UpstreamFailureCategory {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidPolicy => "invalid_policy",
            Self::InvalidRequestId => "invalid_request_id",
            Self::Deadline => "deadline",
            Self::RequestBuild => "request_build",
            Self::Connection => "connection",
            Self::Timeout => "timeout",
            Self::RequestBody => "request_body",
            Self::Decode => "decode",
            Self::Request => "request",
            Self::ResponseTooLarge => "response_too_large",
            Self::Authentication => "authentication",
            Self::RateLimited => "rate_limited",
            Self::Quota => "quota",
            Self::RequestRejected => "request_rejected",
            Self::Server => "server",
            Self::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpstreamDiagnostic {
    pub operation: UpstreamOperation,
    pub category: UpstreamFailureCategory,
    pub status: Option<u16>,
    pub attempts: u8,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpstreamError {
    InvalidPolicy {
        operation: UpstreamOperation,
        violation: PolicyViolation,
    },
    InvalidRequestId {
        operation: UpstreamOperation,
    },
    DeadlineExceeded {
        operation: UpstreamOperation,
        attempts: u8,
    },
    RequestBuild {
        operation: UpstreamOperation,
        kind: RequestFactoryError,
        attempt: u8,
    },
    Transport {
        operation: UpstreamOperation,
        kind: TransportFailureKind,
        attempts: u8,
    },
    ResponseTooLarge {
        operation: UpstreamOperation,
        attempts: u8,
    },
    HttpStatus {
        operation: UpstreamOperation,
        status: u16,
        kind: HttpFailureKind,
        attempts: u8,
    },
}

impl UpstreamError {
    pub fn diagnostic(self) -> UpstreamDiagnostic {
        match self {
            Self::InvalidPolicy { operation, .. } => UpstreamDiagnostic {
                operation,
                category: UpstreamFailureCategory::InvalidPolicy,
                status: None,
                attempts: 0,
            },
            Self::InvalidRequestId { operation } => UpstreamDiagnostic {
                operation,
                category: UpstreamFailureCategory::InvalidRequestId,
                status: None,
                attempts: 0,
            },
            Self::DeadlineExceeded {
                operation,
                attempts,
            } => UpstreamDiagnostic {
                operation,
                category: UpstreamFailureCategory::Deadline,
                status: None,
                attempts,
            },
            Self::RequestBuild {
                operation, attempt, ..
            } => UpstreamDiagnostic {
                operation,
                category: UpstreamFailureCategory::RequestBuild,
                status: None,
                attempts: attempt,
            },
            Self::Transport {
                operation,
                kind,
                attempts,
            } => UpstreamDiagnostic {
                operation,
                category: transport_category(kind),
                status: None,
                attempts,
            },
            Self::ResponseTooLarge {
                operation,
                attempts,
            } => UpstreamDiagnostic {
                operation,
                category: UpstreamFailureCategory::ResponseTooLarge,
                status: None,
                attempts,
            },
            Self::HttpStatus {
                operation,
                status,
                kind,
                attempts,
            } => UpstreamDiagnostic {
                operation,
                category: http_category(kind),
                status: Some(status),
                attempts,
            },
        }
    }
}

impl fmt::Display for UpstreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let diagnostic = self.diagnostic();
        write!(
            formatter,
            "{} upstream request failed: category={} attempts={}",
            diagnostic.operation.as_str(),
            diagnostic.category.as_str(),
            diagnostic.attempts
        )?;
        if let Some(status) = diagnostic.status {
            write!(formatter, " status={status}")?;
        }
        Ok(())
    }
}

impl std::error::Error for UpstreamError {}

fn transport_category(kind: TransportFailureKind) -> UpstreamFailureCategory {
    match kind {
        TransportFailureKind::Connection => UpstreamFailureCategory::Connection,
        TransportFailureKind::Timeout => UpstreamFailureCategory::Timeout,
        TransportFailureKind::RequestBody => UpstreamFailureCategory::RequestBody,
        TransportFailureKind::Decode => UpstreamFailureCategory::Decode,
        TransportFailureKind::Request => UpstreamFailureCategory::Request,
        TransportFailureKind::Other => UpstreamFailureCategory::Other,
    }
}

fn http_category(kind: HttpFailureKind) -> UpstreamFailureCategory {
    match kind {
        HttpFailureKind::Authentication => UpstreamFailureCategory::Authentication,
        HttpFailureKind::RateLimited => UpstreamFailureCategory::RateLimited,
        HttpFailureKind::Quota => UpstreamFailureCategory::Quota,
        HttpFailureKind::RequestRejected => UpstreamFailureCategory::RequestRejected,
        HttpFailureKind::RequestTimeout => UpstreamFailureCategory::Timeout,
        HttpFailureKind::Server => UpstreamFailureCategory::Server,
        HttpFailureKind::Other => UpstreamFailureCategory::Other,
    }
}

#[derive(Clone)]
pub struct UpstreamHttpClient {
    client: Client,
}

impl fmt::Debug for UpstreamHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("UpstreamHttpClient")
            .finish_non_exhaustive()
    }
}

impl UpstreamHttpClient {
    pub fn build(policy: &ClientPolicy) -> Result<Self, ClientBuildError> {
        policy.validate()?;
        let builder = Client::builder()
            .connect_timeout(policy.connect_timeout)
            .timeout(policy.request_timeout)
            .read_timeout(policy.read_timeout)
            .redirect(RedirectPolicy::none())
            .retry(reqwest::retry::never())
            .user_agent(format!(
                "nowledge/{} ({})",
                env!("CARGO_PKG_VERSION"),
                env!("NOWLEDGE_GIT_REV")
            ));
        let builder = match policy.proxy_mode {
            ProxyMode::System => builder,
            ProxyMode::Direct => builder.no_proxy(),
        };
        let client = builder.build().map_err(|_| ClientBuildError::BuildFailed)?;
        Ok(Self { client })
    }

    /// A cheap clone of the pooled client for async request factories.
    pub fn client(&self) -> Client {
        self.client.clone()
    }

    /// Execute a request under one overall deadline.
    ///
    /// The factory is invoked again for every attempt. This is required for
    /// non-cloneable streaming bodies such as multipart file uploads.
    pub async fn execute<F, Fut>(
        &self,
        operation: UpstreamOperation,
        policy: &OperationPolicy,
        request_id: &str,
        mut make_request: F,
    ) -> Result<BoundedResponse, UpstreamError>
    where
        F: FnMut(u8) -> Fut,
        Fut: Future<Output = Result<RequestBuilder, RequestFactoryError>>,
    {
        policy
            .validate()
            .map_err(|violation| UpstreamError::InvalidPolicy {
                operation,
                violation,
            })?;
        let request_id = HeaderValue::from_str(request_id)
            .map_err(|_| UpstreamError::InvalidRequestId { operation })?;
        let deadline =
            OperationDeadline::from_now(policy.deadline).ok_or(UpstreamError::InvalidPolicy {
                operation,
                violation: PolicyViolation::DeadlineOverflow,
            })?;

        for attempt in 1..=policy.max_retries.saturating_add(1) {
            let request = deadline
                .run(make_request(attempt))
                .await
                .map_err(|_| UpstreamError::DeadlineExceeded {
                    operation,
                    attempts: attempt.saturating_sub(1),
                })?
                .map_err(|kind| UpstreamError::RequestBuild {
                    operation,
                    kind,
                    attempt,
                })?;
            let remaining = deadline
                .remaining()
                .ok_or(UpstreamError::DeadlineExceeded {
                    operation,
                    attempts: attempt.saturating_sub(1),
                })?;
            let request = request
                .header(X_CLIENT_REQUEST_ID, request_id.clone())
                .timeout(remaining);
            let response = match deadline.run(request.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    let kind = classify_transport_error(&error);
                    if is_retryable_pre_response_transport(kind) && attempt <= policy.max_retries {
                        wait_for_retry(&deadline, policy, attempt, None, request_id.as_bytes())
                            .await
                            .map_err(|_| UpstreamError::DeadlineExceeded {
                                operation,
                                attempts: attempt,
                            })?;
                        continue;
                    }
                    return Err(UpstreamError::Transport {
                        operation,
                        kind,
                        attempts: attempt,
                    });
                }
                Err(_) => {
                    return Err(UpstreamError::DeadlineExceeded {
                        operation,
                        attempts: attempt,
                    });
                }
            };

            let status = response.status();
            let headers = response.headers().clone();
            let retry_after = retry_after_from_headers(&headers, SystemTime::now());
            let body =
                match read_bounded_response(response, policy.max_response_bytes, deadline).await {
                    Ok(body) => body,
                    Err(BoundedReadError::DeadlineExceeded) => {
                        return Err(UpstreamError::DeadlineExceeded {
                            operation,
                            attempts: attempt,
                        });
                    }
                    Err(BoundedReadError::TooLarge) => {
                        return Err(UpstreamError::ResponseTooLarge {
                            operation,
                            attempts: attempt,
                        });
                    }
                    Err(BoundedReadError::Transport(kind)) => {
                        return Err(UpstreamError::Transport {
                            operation,
                            kind,
                            attempts: attempt,
                        });
                    }
                };

            match classify_response(status, &body) {
                ResponseDisposition::Success => {
                    return Ok(BoundedResponse {
                        status,
                        headers,
                        body,
                        attempts: attempt,
                    });
                }
                ResponseDisposition::Retryable(_) if attempt <= policy.max_retries => {
                    wait_for_retry(
                        &deadline,
                        policy,
                        attempt,
                        retry_after,
                        request_id.as_bytes(),
                    )
                    .await
                    .map_err(|_| UpstreamError::DeadlineExceeded {
                        operation,
                        attempts: attempt,
                    })?;
                }
                ResponseDisposition::Retryable(kind) | ResponseDisposition::Terminal(kind) => {
                    return Err(UpstreamError::HttpStatus {
                        operation,
                        status: status.as_u16(),
                        kind,
                        attempts: attempt,
                    });
                }
            }
        }

        Err(UpstreamError::DeadlineExceeded {
            operation,
            attempts: policy.max_retries.saturating_add(1),
        })
    }

    /// Start a response whose successful body will be consumed incrementally.
    ///
    /// Request construction, response headers, retry backoff, and every body
    /// chunk share one overall deadline. Retryable status responses are read
    /// with the configured bound before classification. Once a successful
    /// response has been accepted, its body is never retried: chunk failures,
    /// deadline exhaustion, and cumulative size-limit violations are terminal.
    /// Dropping the returned response drops the owned upstream response and
    /// cancels any unread body.
    pub async fn execute_stream<F, Fut>(
        &self,
        operation: UpstreamOperation,
        policy: &OperationPolicy,
        request_id: &str,
        mut make_request: F,
    ) -> Result<StreamingResponse, UpstreamError>
    where
        F: FnMut(u8) -> Fut,
        Fut: Future<Output = Result<RequestBuilder, RequestFactoryError>>,
    {
        policy
            .validate()
            .map_err(|violation| UpstreamError::InvalidPolicy {
                operation,
                violation,
            })?;
        let request_id = HeaderValue::from_str(request_id)
            .map_err(|_| UpstreamError::InvalidRequestId { operation })?;
        let deadline =
            OperationDeadline::from_now(policy.deadline).ok_or(UpstreamError::InvalidPolicy {
                operation,
                violation: PolicyViolation::DeadlineOverflow,
            })?;

        for attempt in 1..=policy.max_retries.saturating_add(1) {
            let request = deadline
                .run(make_request(attempt))
                .await
                .map_err(|_| UpstreamError::DeadlineExceeded {
                    operation,
                    attempts: attempt.saturating_sub(1),
                })?
                .map_err(|kind| UpstreamError::RequestBuild {
                    operation,
                    kind,
                    attempt,
                })?;
            let remaining = deadline
                .remaining()
                .ok_or(UpstreamError::DeadlineExceeded {
                    operation,
                    attempts: attempt.saturating_sub(1),
                })?;
            let request = request
                .header(X_CLIENT_REQUEST_ID, request_id.clone())
                .timeout(remaining);
            let response = match deadline.run(request.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(error)) => {
                    let kind = classify_transport_error(&error);
                    if is_retryable_pre_response_transport(kind) && attempt <= policy.max_retries {
                        wait_for_retry(&deadline, policy, attempt, None, request_id.as_bytes())
                            .await
                            .map_err(|_| UpstreamError::DeadlineExceeded {
                                operation,
                                attempts: attempt,
                            })?;
                        continue;
                    }
                    return Err(UpstreamError::Transport {
                        operation,
                        kind,
                        attempts: attempt,
                    });
                }
                Err(_) => {
                    return Err(UpstreamError::DeadlineExceeded {
                        operation,
                        attempts: attempt,
                    });
                }
            };

            let status = response.status();
            if status.is_success() {
                return StreamingResponse::new(
                    response,
                    operation,
                    deadline,
                    policy.max_response_bytes,
                    attempt,
                );
            }

            let headers = response.headers().clone();
            let retry_after = retry_after_from_headers(&headers, SystemTime::now());
            let body =
                match read_bounded_response(response, policy.max_response_bytes, deadline).await {
                    Ok(body) => body,
                    Err(BoundedReadError::DeadlineExceeded) => {
                        return Err(UpstreamError::DeadlineExceeded {
                            operation,
                            attempts: attempt,
                        });
                    }
                    Err(BoundedReadError::TooLarge) => {
                        return Err(UpstreamError::ResponseTooLarge {
                            operation,
                            attempts: attempt,
                        });
                    }
                    Err(BoundedReadError::Transport(kind)) => {
                        return Err(UpstreamError::Transport {
                            operation,
                            kind,
                            attempts: attempt,
                        });
                    }
                };

            match classify_response(status, &body) {
                ResponseDisposition::Success => unreachable!("success handled before body read"),
                ResponseDisposition::Retryable(_) if attempt <= policy.max_retries => {
                    wait_for_retry(
                        &deadline,
                        policy,
                        attempt,
                        retry_after,
                        request_id.as_bytes(),
                    )
                    .await
                    .map_err(|_| UpstreamError::DeadlineExceeded {
                        operation,
                        attempts: attempt,
                    })?;
                }
                ResponseDisposition::Retryable(kind) | ResponseDisposition::Terminal(kind) => {
                    return Err(UpstreamError::HttpStatus {
                        operation,
                        status: status.as_u16(),
                        kind,
                        attempts: attempt,
                    });
                }
            }
        }

        Err(UpstreamError::DeadlineExceeded {
            operation,
            attempts: policy.max_retries.saturating_add(1),
        })
    }
}

fn is_retryable_pre_response_transport(kind: TransportFailureKind) -> bool {
    // Reqwest's generic timeout classification spans connect, request-body,
    // response-header, and response-body phases. Only `is_connect()` proves
    // that the request was not accepted by the upstream, so every other
    // transport failure is terminal to avoid duplicating POST side effects.
    kind == TransportFailureKind::Connection
}

pub struct BoundedResponse {
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
    attempts: u8,
}

impl fmt::Debug for BoundedResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BoundedResponse")
            .field("status", &self.status)
            .field("body_len", &self.body.len())
            .field("attempts", &self.attempts)
            .finish()
    }
}

impl BoundedResponse {
    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn into_body(self) -> Vec<u8> {
        self.body
    }

    pub fn attempts(&self) -> u8 {
        self.attempts
    }
}

pub struct StreamingResponse {
    status: StatusCode,
    headers: HeaderMap,
    response: Option<Response>,
    operation: UpstreamOperation,
    deadline: OperationDeadline,
    max_response_bytes: usize,
    bytes_read: usize,
    attempts: u8,
}

impl fmt::Debug for StreamingResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StreamingResponse")
            .field("status", &self.status)
            .field("bytes_read", &self.bytes_read)
            .field("attempts", &self.attempts)
            .field("finished", &self.response.is_none())
            .finish()
    }
}

impl StreamingResponse {
    fn new(
        response: Response,
        operation: UpstreamOperation,
        deadline: OperationDeadline,
        max_response_bytes: usize,
        attempts: u8,
    ) -> Result<Self, UpstreamError> {
        let max_response_bytes_u64 = u64::try_from(max_response_bytes).unwrap_or(u64::MAX);
        if response
            .content_length()
            .is_some_and(|length| length > max_response_bytes_u64)
        {
            return Err(UpstreamError::ResponseTooLarge {
                operation,
                attempts,
            });
        }

        Ok(Self {
            status: response.status(),
            headers: response.headers().clone(),
            response: Some(response),
            operation,
            deadline,
            max_response_bytes,
            bytes_read: 0,
            attempts,
        })
    }

    pub fn status(&self) -> StatusCode {
        self.status
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }

    pub fn attempts(&self) -> u8 {
        self.attempts
    }

    pub fn bytes_read(&self) -> usize {
        self.bytes_read
    }

    /// Read the next upstream body chunk under the original operation deadline.
    ///
    /// The returned `Vec` preserves the chunk boundary produced by reqwest.
    /// Once this method returns an error, the owned response is dropped and
    /// subsequent calls return end-of-stream.
    pub async fn next_chunk(&mut self) -> Result<Option<Vec<u8>>, UpstreamError> {
        let Some(response) = self.response.as_mut() else {
            return Ok(None);
        };
        let chunk = self.deadline.run(response.chunk()).await;
        match chunk {
            Ok(Ok(Some(chunk))) => {
                let next_len = match self.bytes_read.checked_add(chunk.len()) {
                    Some(next_len) if next_len <= self.max_response_bytes => next_len,
                    _ => {
                        self.response.take();
                        return Err(UpstreamError::ResponseTooLarge {
                            operation: self.operation,
                            attempts: self.attempts,
                        });
                    }
                };
                self.bytes_read = next_len;
                Ok(Some(chunk.to_vec()))
            }
            Ok(Ok(None)) => {
                self.response.take();
                Ok(None)
            }
            Ok(Err(error)) => {
                let kind = classify_transport_error(&error);
                self.response.take();
                Err(UpstreamError::Transport {
                    operation: self.operation,
                    kind,
                    attempts: self.attempts,
                })
            }
            Err(_) => {
                self.response.take();
                Err(UpstreamError::DeadlineExceeded {
                    operation: self.operation,
                    attempts: self.attempts,
                })
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundedReadError {
    DeadlineExceeded,
    TooLarge,
    Transport(TransportFailureKind),
}

#[derive(Debug, Clone, Copy)]
pub struct OperationDeadline {
    ends_at: Instant,
}

impl OperationDeadline {
    pub fn from_now(duration: Duration) -> Option<Self> {
        Instant::now()
            .checked_add(duration)
            .map(|ends_at| Self { ends_at })
    }

    pub fn remaining(self) -> Option<Duration> {
        self.ends_at.checked_duration_since(Instant::now())
    }

    async fn run<F: Future>(self, future: F) -> Result<F::Output, time::error::Elapsed> {
        time::timeout_at(self.ends_at, future).await
    }
}

pub async fn read_bounded_response(
    mut response: Response,
    max_bytes: usize,
    deadline: OperationDeadline,
) -> Result<Vec<u8>, BoundedReadError> {
    let max_bytes_u64 = u64::try_from(max_bytes).unwrap_or(u64::MAX);
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes_u64)
    {
        return Err(BoundedReadError::TooLarge);
    }

    let mut body = Vec::with_capacity(
        response
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
            .unwrap_or(0)
            .min(max_bytes),
    );
    loop {
        let chunk = deadline
            .run(response.chunk())
            .await
            .map_err(|_| BoundedReadError::DeadlineExceeded)?
            .map_err(|error| BoundedReadError::Transport(classify_transport_error(&error)))?;
        let Some(chunk) = chunk else {
            return Ok(body);
        };
        let next_len = body
            .len()
            .checked_add(chunk.len())
            .ok_or(BoundedReadError::TooLarge)?;
        if next_len > max_bytes {
            return Err(BoundedReadError::TooLarge);
        }
        body.extend_from_slice(&chunk);
    }
}

pub fn classify_transport_error(error: &reqwest::Error) -> TransportFailureKind {
    if error.is_connect() {
        TransportFailureKind::Connection
    } else if error.is_timeout() {
        TransportFailureKind::Timeout
    } else if error.is_body() {
        TransportFailureKind::RequestBody
    } else if error.is_decode() {
        TransportFailureKind::Decode
    } else if error.is_request() {
        TransportFailureKind::Request
    } else {
        TransportFailureKind::Other
    }
}

pub fn classify_response(status: StatusCode, body: &[u8]) -> ResponseDisposition {
    if status.is_success() {
        return ResponseDisposition::Success;
    }
    if structured_quota_exhaustion(body) {
        return ResponseDisposition::Terminal(HttpFailureKind::Quota);
    }
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            ResponseDisposition::Terminal(HttpFailureKind::Authentication)
        }
        StatusCode::PAYMENT_REQUIRED => ResponseDisposition::Terminal(HttpFailureKind::Quota),
        StatusCode::REQUEST_TIMEOUT => {
            ResponseDisposition::Retryable(HttpFailureKind::RequestTimeout)
        }
        StatusCode::TOO_MANY_REQUESTS => {
            ResponseDisposition::Retryable(HttpFailureKind::RateLimited)
        }
        StatusCode::INTERNAL_SERVER_ERROR
        | StatusCode::BAD_GATEWAY
        | StatusCode::SERVICE_UNAVAILABLE
        | StatusCode::GATEWAY_TIMEOUT => ResponseDisposition::Retryable(HttpFailureKind::Server),
        status if status.is_client_error() => {
            ResponseDisposition::Terminal(HttpFailureKind::RequestRejected)
        }
        _ => ResponseDisposition::Terminal(HttpFailureKind::Other),
    }
}

fn structured_quota_exhaustion(body: &[u8]) -> bool {
    let Ok(value) = serde_json::from_slice::<Value>(body) else {
        return false;
    };
    let exhausted = [
        value.pointer("/error/code"),
        value.pointer("/error/type"),
        value.get("code"),
        value.get("type"),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_str)
    .any(is_quota_marker);
    exhausted
}

fn is_quota_marker(marker: &str) -> bool {
    [
        "insufficient_quota",
        "quota_exceeded",
        "resource_exhausted",
        "billing_hard_limit_reached",
        "billing_not_active",
        "credit_balance_too_low",
        "credits_exhausted",
        "usage_limit_reached",
    ]
    .iter()
    .any(|known| marker.eq_ignore_ascii_case(known))
}

pub fn retry_after_from_headers(headers: &HeaderMap, now: SystemTime) -> Option<Duration> {
    headers
        .get_all(RETRY_AFTER)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find_map(|value| parse_retry_after(value, now))
}

pub fn parse_retry_after(value: &str, now: SystemTime) -> Option<Duration> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    if value.bytes().all(|byte| byte.is_ascii_digit()) {
        return value.parse::<u64>().ok().map(Duration::from_secs);
    }
    let retry_at = parse_http_date(value)?;
    let now: DateTime<Utc> = now.into();
    let delay = retry_at.signed_duration_since(now);
    if delay <= chrono::Duration::zero() {
        Some(Duration::ZERO)
    } else {
        delay.to_std().ok()
    }
}

fn parse_http_date(value: &str) -> Option<DateTime<Utc>> {
    let imf_fixdate = NaiveDateTime::parse_from_str(value, "%a, %d %b %Y %H:%M:%S GMT")
        .ok()
        .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc));
    if imf_fixdate.is_some() {
        return imf_fixdate;
    }

    let rfc850 = NaiveDateTime::parse_from_str(value, "%A, %d-%b-%y %H:%M:%S GMT")
        .ok()
        .and_then(|mut value| {
            let current_year = Utc::now().year();
            if value.year() > current_year.saturating_add(50) {
                value = value.with_year(value.year().saturating_sub(100))?;
            }
            Some(DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
        });
    if rfc850.is_some() {
        return rfc850;
    }

    NaiveDateTime::parse_from_str(value, "%a %b %e %H:%M:%S %Y")
        .ok()
        .map(|value| DateTime::<Utc>::from_naive_utc_and_offset(value, Utc))
}

pub fn retry_delay(
    initial_backoff: Duration,
    max_backoff: Duration,
    retry_number: u8,
    retry_after: Option<Duration>,
    jitter_seed: &[u8],
) -> Duration {
    let exponent = u32::from(retry_number.saturating_sub(1)).min(31);
    let multiplier = 1_u32.checked_shl(exponent).unwrap_or(u32::MAX);
    let exponential = initial_backoff
        .checked_mul(multiplier)
        .unwrap_or(Duration::MAX)
        .min(max_backoff);
    let jitter_ceiling = exponential / 4;
    let jitter = deterministic_jitter(jitter_seed, retry_number, jitter_ceiling);
    let backoff = exponential.saturating_add(jitter).min(max_backoff);
    backoff.max(retry_after.unwrap_or(Duration::ZERO))
}

fn deterministic_jitter(seed: &[u8], retry_number: u8, ceiling: Duration) -> Duration {
    let ceiling_nanos = ceiling.as_nanos();
    if ceiling_nanos == 0 {
        return Duration::ZERO;
    }
    let mut hasher = Sha256::new();
    hasher.update(seed);
    hasher.update([retry_number]);
    let digest = hasher.finalize();
    let mut sample = [0_u8; 8];
    sample.copy_from_slice(&digest[..8]);
    let sample = u64::from_be_bytes(sample);
    let modulus = ceiling_nanos.saturating_add(1).min(u128::from(u64::MAX));
    Duration::from_nanos(sample % u64::try_from(modulus).unwrap_or(u64::MAX))
}

async fn wait_for_retry(
    deadline: &OperationDeadline,
    policy: &OperationPolicy,
    retry_number: u8,
    retry_after: Option<Duration>,
    jitter_seed: &[u8],
) -> Result<(), ()> {
    let delay = retry_delay(
        policy.initial_backoff,
        policy.max_backoff,
        retry_number,
        retry_after,
        jitter_seed,
    );
    let remaining = deadline.remaining().ok_or(())?;
    if delay >= remaining {
        return Err(());
    }
    deadline.run(time::sleep(delay)).await.map_err(|_| ())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{mpsc, oneshot, Mutex},
    };

    use super::*;

    #[test]
    fn parses_retry_after_delta_and_all_http_date_forms() {
        let now: SystemTime = DateTime::parse_from_rfc3339("2015-10-21T07:27:00Z")
            .unwrap()
            .with_timezone(&Utc)
            .into();

        assert_eq!(parse_retry_after("15", now), Some(Duration::from_secs(15)));
        assert_eq!(
            parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT", now),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            parse_retry_after("Wednesday, 21-Oct-15 07:28:00 GMT", now),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            parse_retry_after("Wed Oct 21 07:28:00 2015", now),
            Some(Duration::from_secs(60))
        );
        assert_eq!(parse_retry_after("-1", now), None);
    }

    #[test]
    fn classification_is_conservative_and_uses_structured_quota_codes_only() {
        assert_eq!(
            classify_response(StatusCode::INTERNAL_SERVER_ERROR, b"{}"),
            ResponseDisposition::Retryable(HttpFailureKind::Server)
        );
        assert_eq!(
            classify_response(StatusCode::UNAUTHORIZED, b"{}"),
            ResponseDisposition::Terminal(HttpFailureKind::Authentication)
        );
        assert_eq!(
            classify_response(
                StatusCode::TOO_MANY_REQUESTS,
                br#"{"error":{"code":"insufficient_quota"}}"#,
            ),
            ResponseDisposition::Terminal(HttpFailureKind::Quota)
        );
        assert_eq!(
            classify_response(
                StatusCode::TOO_MANY_REQUESTS,
                br#"{"error":{"message":"insufficient_quota"}}"#,
            ),
            ResponseDisposition::Retryable(HttpFailureKind::RateLimited)
        );
        assert_eq!(
            classify_response(StatusCode::CONFLICT, b"{}"),
            ResponseDisposition::Terminal(HttpFailureKind::RequestRejected)
        );
    }

    #[test]
    fn retry_delay_is_deterministic_capped_and_honors_retry_after() {
        let first = retry_delay(
            Duration::from_millis(100),
            Duration::from_secs(1),
            2,
            None,
            b"request-id",
        );
        let second = retry_delay(
            Duration::from_millis(100),
            Duration::from_secs(1),
            2,
            None,
            b"request-id",
        );
        assert_eq!(first, second);
        assert!((Duration::from_millis(200)..=Duration::from_millis(250)).contains(&first));
        assert_eq!(
            retry_delay(
                Duration::from_millis(100),
                Duration::from_secs(1),
                2,
                Some(Duration::from_millis(750)),
                b"request-id",
            ),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn safe_diagnostic_contains_no_provider_material() {
        let error = UpstreamError::HttpStatus {
            operation: UpstreamOperation::LlmCompletion,
            status: 429,
            kind: HttpFailureKind::Quota,
            attempts: 1,
        };
        let rendered = error.to_string();
        assert_eq!(
            rendered,
            "llm_completion upstream request failed: category=quota attempts=1 status=429"
        );
        assert!(!rendered.contains("http"));
        assert!(!rendered.contains("secret"));
    }

    #[tokio::test]
    async fn retries_retryable_status_with_one_stable_request_id() {
        let responses = vec![
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
        ];
        let (url, requests) = spawn_server(responses).await;
        let client = test_client();
        let request_client = client.client();
        let request_url = url.clone();
        let response = client
            .execute(
                UpstreamOperation::LlmCompletion,
                &OperationPolicy {
                    deadline: Duration::from_secs(2),
                    max_response_bytes: 1024,
                    max_retries: 1,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "stable-request-id",
                move |_| {
                    let request = request_client.get(request_url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.attempts(), 2);
        let requests = requests.lock().await;
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert!(request
                .to_ascii_lowercase()
                .contains("x-client-request-id: stable-request-id"));
        }
    }

    #[tokio::test]
    async fn retries_non_quota_rate_limit_but_not_authentication_or_quota() {
        let rate_limit_responses = vec![
            "HTTP/1.1 429 Too Many Requests\r\nRetry-After: 0\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            "HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{\"ok\":true}",
        ];
        let (rate_limit_url, rate_limit_requests) = spawn_server(rate_limit_responses).await;
        let client = test_client();
        let request_client = client.client();
        let response = client
            .execute(
                UpstreamOperation::LlmCompletion,
                &retrying_test_policy(),
                "rate-limit-request-id",
                move |_| {
                    let request = request_client.get(rate_limit_url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap();
        assert_eq!(response.attempts(), 2);
        assert_eq!(rate_limit_requests.lock().await.len(), 2);

        for (response, expected_kind) in [
            (
                "HTTP/1.1 401 Unauthorized\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
                HttpFailureKind::Authentication,
            ),
            (
                "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 39\r\nConnection: close\r\n\r\n{\"error\":{\"code\":\"insufficient_quota\"}}",
                HttpFailureKind::Quota,
            ),
        ] {
            let (url, requests) = spawn_server(vec![response]).await;
            let request_client = client.client();
            let error = client
                .execute(
                    UpstreamOperation::LlmCompletion,
                    &retrying_test_policy(),
                    "terminal-request-id",
                    move |_| {
                        let request = request_client.get(url.clone());
                        async move { Ok(request) }
                    },
                )
                .await
                .unwrap_err();
            assert_eq!(
                error,
                UpstreamError::HttpStatus {
                    operation: UpstreamOperation::LlmCompletion,
                    status: if expected_kind == HttpFailureKind::Authentication {
                        401
                    } else {
                        429
                    },
                    kind: expected_kind,
                    attempts: 1,
                }
            );
            assert_eq!(requests.lock().await.len(), 1);
        }
    }

    #[tokio::test]
    async fn overall_deadline_stops_a_stalled_response_without_retrying() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, mut request_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0_u8; 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                if count == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_tx.send(request).await.unwrap();
            std::future::pending::<()>().await;
        });

        let client = test_client();
        let request_client = client.client();
        let url = format!("http://{address}");
        let error = client
            .execute(
                UpstreamOperation::ParserHealth,
                &OperationPolicy {
                    deadline: Duration::from_millis(75),
                    max_response_bytes: 1024,
                    max_retries: 2,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "deadline-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            UpstreamError::DeadlineExceeded {
                operation: UpstreamOperation::ParserHealth,
                attempts: 1,
            } | UpstreamError::Transport {
                operation: UpstreamOperation::ParserHealth,
                kind: TransportFailureKind::Timeout,
                attempts: 1,
            }
        ));
        let request = request_rx.recv().await.unwrap();
        assert!(String::from_utf8_lossy(&request)
            .to_ascii_lowercase()
            .contains("x-client-request-id: deadline-request-id"));
        assert!(request_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn does_not_retry_a_timeout_after_a_post_body_is_sent() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, mut request_rx) = mpsc::channel(2);
        let expected_body = b"provider-side-effect";
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            while !request
                .windows(expected_body.len())
                .any(|window| window == expected_body)
            {
                let mut buffer = [0_u8; 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                if count == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..count]);
            }
            request_tx.send(request).await.unwrap();
            std::future::pending::<()>().await;
        });

        let client = UpstreamHttpClient::build(&ClientPolicy {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_millis(40),
            proxy_mode: ProxyMode::Direct,
        })
        .unwrap();
        let request_client = client.client();
        let url = format!("http://{address}");
        let error = client
            .execute(
                UpstreamOperation::LlmCompletion,
                &OperationPolicy {
                    deadline: Duration::from_secs(1),
                    max_response_bytes: 1024,
                    max_retries: 1,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "post-timeout-request-id",
                move |_| {
                    let request = request_client
                        .post(url.clone())
                        .body(expected_body.as_slice());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            UpstreamError::Transport {
                operation: UpstreamOperation::LlmCompletion,
                kind: TransportFailureKind::Timeout,
                attempts: 1,
            }
        );
        let request = request_rx.recv().await.unwrap();
        assert!(request
            .windows(expected_body.len())
            .any(|window| window == expected_body));
        assert!(request_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn does_not_retry_a_body_read_timeout_after_response_headers() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, mut request_rx) = mpsc::channel(2);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0_u8; 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                if count == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            request_tx.send(request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            time::sleep(Duration::from_millis(120)).await;
            let _ = stream.shutdown().await;
        });

        let client = UpstreamHttpClient::build(&ClientPolicy {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            read_timeout: Duration::from_millis(40),
            proxy_mode: ProxyMode::Direct,
        })
        .unwrap();
        let request_client = client.client();
        let url = format!("http://{address}");
        let error = client
            .execute(
                UpstreamOperation::ParserUpload,
                &OperationPolicy {
                    deadline: Duration::from_secs(1),
                    max_response_bytes: 1024,
                    max_retries: 1,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "read-timeout-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            UpstreamError::Transport {
                operation: UpstreamOperation::ParserUpload,
                kind: TransportFailureKind::Timeout,
                attempts: 1,
            }
        );
        let request = request_rx.recv().await.unwrap();
        assert!(String::from_utf8_lossy(&request)
            .to_ascii_lowercase()
            .contains("x-client-request-id: read-timeout-request-id"));
        assert!(request_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn retries_connection_refusal_only_up_to_the_configured_limit() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let client = test_client();
        let request_client = client.client();
        let error = client
            .execute(
                UpstreamOperation::LlmCompletion,
                &retrying_test_policy(),
                "connection-refusal-request-id",
                move |_| {
                    let request = request_client.get(format!("http://{address}"));
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            UpstreamError::Transport {
                operation: UpstreamOperation::LlmCompletion,
                kind: TransportFailureKind::Connection,
                attempts: 3,
            }
        );
    }

    #[tokio::test]
    async fn cancelling_the_caller_closes_an_in_flight_upstream_request() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted_tx, mut accepted_rx) = mpsc::channel(1);
        let (closed_tx, mut closed_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            loop {
                let mut buffer = [0_u8; 1024];
                let count = stream.read(&mut buffer).await.unwrap();
                if count == 0 {
                    return;
                }
                request.extend_from_slice(&buffer[..count]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            accepted_tx.send(()).await.unwrap();
            let mut byte = [0_u8; 1];
            if stream.read(&mut byte).await.unwrap() == 0 {
                closed_tx.send(()).await.unwrap();
            }
        });

        let client = test_client();
        let request_client = client.client();
        let url = format!("http://{address}");
        let request = tokio::spawn(async move {
            client
                .execute(
                    UpstreamOperation::ParserUpload,
                    &OperationPolicy::without_retries(Duration::from_secs(5), 1024),
                    "cancelled-request-id",
                    move |_| {
                        let request = request_client.get(url.clone());
                        async move { Ok(request) }
                    },
                )
                .await
        });

        accepted_rx.recv().await.unwrap();
        request.abort();
        assert!(request.await.unwrap_err().is_cancelled());
        time::timeout(Duration::from_secs(1), closed_rx.recv())
            .await
            .expect("cancelled upstream connection did not close")
            .expect("upstream connection closed without a cancellation signal");
    }

    #[tokio::test]
    async fn rejects_cumulative_chunked_body_over_limit() {
        let responses = vec![
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nabcd\r\n4\r\nefgh\r\n0\r\n\r\n",
        ];
        let (url, _) = spawn_server(responses).await;
        let client = test_client();
        let request_client = client.client();
        let error = client
            .execute(
                UpstreamOperation::ParserUpload,
                &OperationPolicy::without_retries(Duration::from_secs(2), 6),
                "stable-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap_err();

        assert_eq!(
            error,
            UpstreamError::ResponseTooLarge {
                operation: UpstreamOperation::ParserUpload,
                attempts: 1,
            }
        );
    }

    #[tokio::test]
    async fn stream_retries_retryable_status_before_accepting_success() {
        let responses = vec![
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n",
        ];
        let (url, requests) = spawn_server(responses).await;
        let client = test_client();
        let request_client = client.client();
        let mut response = client
            .execute_stream(
                UpstreamOperation::LlmCompletion,
                &OperationPolicy {
                    deadline: Duration::from_secs(2),
                    max_response_bytes: 1024,
                    max_retries: 1,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "stream-retry-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.attempts(), 2);
        let mut body = Vec::new();
        while let Some(chunk) = response.next_chunk().await.unwrap() {
            body.extend_from_slice(&chunk);
        }
        assert_eq!(body, b"hello world");
        assert_eq!(response.bytes_read(), body.len());
        let requests = requests.lock().await;
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert!(request
                .to_ascii_lowercase()
                .contains("x-client-request-id: stream-retry-request-id"));
        }
    }

    #[tokio::test]
    async fn stream_does_not_retry_a_body_failure_after_success_headers() {
        let responses =
            vec!["HTTP/1.1 200 OK\r\nContent-Length: 11\r\nConnection: close\r\n\r\npartial"];
        let (url, requests) = spawn_server(responses).await;
        let client = test_client();
        let request_client = client.client();
        let mut response = client
            .execute_stream(
                UpstreamOperation::LlmCompletion,
                &retrying_test_policy(),
                "stream-body-failure-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap();

        let error = loop {
            match response.next_chunk().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("truncated successful response ended without an error"),
                Err(error) => break error,
            }
        };
        assert!(matches!(
            error,
            UpstreamError::Transport {
                operation: UpstreamOperation::LlmCompletion,
                attempts: 1,
                ..
            }
        ));
        assert!(response.next_chunk().await.unwrap().is_none());
        assert_eq!(requests.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn stream_enforces_the_cumulative_body_limit_without_retrying() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, mut request_rx) = mpsc::channel(1);
        let (continue_tx, continue_rx) = oneshot::channel();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request_headers(&mut stream).await;
            request_tx.send(request).await.unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nabcd\r\n",
                )
                .await
                .unwrap();
            stream.flush().await.unwrap();
            continue_rx.await.unwrap();
            stream.write_all(b"4\r\nefgh\r\n0\r\n\r\n").await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let client = test_client();
        let request_client = client.client();
        let url = format!("http://{address}");
        let mut response = client
            .execute_stream(
                UpstreamOperation::LlmCompletion,
                &OperationPolicy {
                    deadline: Duration::from_secs(2),
                    max_response_bytes: 6,
                    max_retries: 2,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "stream-limit-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap();

        assert_eq!(response.next_chunk().await.unwrap(), Some(b"abcd".to_vec()));
        assert_eq!(response.bytes_read(), 4);
        continue_tx.send(()).unwrap();
        assert_eq!(
            response.next_chunk().await.unwrap_err(),
            UpstreamError::ResponseTooLarge {
                operation: UpstreamOperation::LlmCompletion,
                attempts: 1,
            }
        );
        assert_eq!(response.bytes_read(), 4);
        assert!(response.next_chunk().await.unwrap().is_none());
        assert!(String::from_utf8_lossy(&request_rx.recv().await.unwrap())
            .to_ascii_lowercase()
            .contains("x-client-request-id: stream-limit-request-id"));
        assert!(request_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn stream_body_uses_the_original_deadline_and_is_not_retried() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, mut request_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let request = read_request_headers(&mut stream).await;
            request_tx.send(request).await.unwrap();
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
            stream.flush().await.unwrap();
            std::future::pending::<()>().await;
        });

        let client = test_client();
        let request_client = client.client();
        let url = format!("http://{address}");
        let mut response = client
            .execute_stream(
                UpstreamOperation::LlmCompletion,
                &OperationPolicy {
                    deadline: Duration::from_millis(75),
                    max_response_bytes: 1024,
                    max_retries: 2,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "stream-deadline-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap();

        assert!(matches!(
            response.next_chunk().await.unwrap_err(),
            UpstreamError::DeadlineExceeded {
                operation: UpstreamOperation::LlmCompletion,
                attempts: 1,
            } | UpstreamError::Transport {
                operation: UpstreamOperation::LlmCompletion,
                kind: TransportFailureKind::Timeout,
                attempts: 1,
            }
        ));
        assert!(String::from_utf8_lossy(&request_rx.recv().await.unwrap())
            .to_ascii_lowercase()
            .contains("x-client-request-id: stream-deadline-request-id"));
        assert!(request_rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn dropping_a_streaming_response_cancels_its_unread_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (closed_tx, mut closed_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            read_request_headers(&mut stream).await;
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nopen\r\n",
                )
                .await
                .unwrap();
            stream.flush().await.unwrap();
            let mut byte = [0_u8; 1];
            if stream.read(&mut byte).await.unwrap() == 0 {
                closed_tx.send(()).await.unwrap();
            }
        });

        let client = test_client();
        let request_client = client.client();
        let url = format!("http://{address}");
        let mut response = client
            .execute_stream(
                UpstreamOperation::LlmCompletion,
                &OperationPolicy::without_retries(Duration::from_secs(5), 1024),
                "stream-drop-request-id",
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap();
        assert_eq!(response.next_chunk().await.unwrap(), Some(b"open".to_vec()));

        drop(response);
        time::timeout(Duration::from_secs(1), closed_rx.recv())
            .await
            .expect("dropping the streaming response did not close the upstream connection")
            .expect("upstream connection closed without a drop signal");
    }

    #[tokio::test]
    async fn stream_reads_non_success_bodies_with_a_bound_for_classification() {
        let quota =
            "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 39\r\nConnection: close\r\n\r\n{\"error\":{\"code\":\"insufficient_quota\"}}";
        let (quota_url, quota_requests) = spawn_server(vec![quota]).await;
        let client = test_client();
        let request_client = client.client();
        let error = client
            .execute_stream(
                UpstreamOperation::LlmCompletion,
                &retrying_test_policy(),
                "stream-quota-request-id",
                move |_| {
                    let request = request_client.get(quota_url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            UpstreamError::HttpStatus {
                operation: UpstreamOperation::LlmCompletion,
                status: 429,
                kind: HttpFailureKind::Quota,
                attempts: 1,
            }
        );
        assert_eq!(quota_requests.lock().await.len(), 1);

        let oversized = "HTTP/1.1 503 Service Unavailable\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n4\r\nabcd\r\n4\r\nefgh\r\n0\r\n\r\n";
        let (oversized_url, oversized_requests) = spawn_server(vec![oversized]).await;
        let request_client = client.client();
        let error = client
            .execute_stream(
                UpstreamOperation::LlmCompletion,
                &OperationPolicy {
                    deadline: Duration::from_secs(2),
                    max_response_bytes: 6,
                    max_retries: 2,
                    initial_backoff: Duration::ZERO,
                    max_backoff: Duration::ZERO,
                },
                "stream-error-limit-request-id",
                move |_| {
                    let request = request_client.get(oversized_url.clone());
                    async move { Ok(request) }
                },
            )
            .await
            .unwrap_err();
        assert_eq!(
            error,
            UpstreamError::ResponseTooLarge {
                operation: UpstreamOperation::LlmCompletion,
                attempts: 1,
            }
        );
        assert_eq!(oversized_requests.lock().await.len(), 1);
    }

    fn test_client() -> UpstreamHttpClient {
        UpstreamHttpClient::build(&ClientPolicy {
            connect_timeout: Duration::from_secs(1),
            request_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(1),
            proxy_mode: ProxyMode::Direct,
        })
        .unwrap()
    }

    fn retrying_test_policy() -> OperationPolicy {
        OperationPolicy {
            deadline: Duration::from_secs(2),
            max_response_bytes: 1024,
            max_retries: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
        }
    }

    async fn spawn_server(responses: Vec<&'static str>) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let (ready_tx, mut ready_rx) = mpsc::channel(1);
        tokio::spawn(async move {
            ready_tx.send(()).await.unwrap();
            for response in responses {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0_u8; 1024];
                    let count = stream.read(&mut buffer).await.unwrap();
                    if count == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..count]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                captured
                    .lock()
                    .await
                    .push(String::from_utf8_lossy(&request).into_owned());
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            }
        });
        ready_rx.recv().await.unwrap();
        (format!("http://{address}"), requests)
    }

    async fn read_request_headers(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        loop {
            let mut buffer = [0_u8; 1024];
            let count = stream.read(&mut buffer).await.unwrap();
            if count == 0 {
                return request;
            }
            request.extend_from_slice(&buffer[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                return request;
            }
        }
    }
}
