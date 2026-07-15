use std::{
    borrow::Cow,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use reqwest::multipart::{Form, Part};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::{
    config::Config,
    error::ApiError,
    models::*,
    upstream::{
        ClientPolicy, OperationPolicy, ProxyMode, RequestFactoryError, UpstreamHttpClient,
        UpstreamOperation,
    },
    util::new_id,
};

const MAX_PARSED_BLOCKS: usize = 10_000;
const MAX_PARSED_IMAGES: usize = 10_000;
const MAX_BLOCK_FIELD_BYTES: usize = 1024 * 1024;
const MAX_BLOCK_ID_BYTES: usize = 256;
const MAX_BLOCK_TYPE_BYTES: usize = 64;
const MAX_SECTION_DEPTH: usize = 64;
const MAX_SECTION_BYTES: usize = 16 * 1024;
const MAX_REFERENCE_BYTES: usize = 16 * 1024;
const MAX_BBOX_BYTES: usize = 64 * 1024;
const MAX_PARSER_LABEL_BYTES: usize = 128;
const MAX_FILE_NAME_BYTES: usize = 512;
const PARSER_HEALTH_TIMEOUT: Duration = Duration::from_secs(2);
const PARSER_HEALTH_MAX_RESPONSE_BYTES: usize = 64 * 1024;

#[derive(Debug)]
pub struct ParserInput {
    pub content: Option<String>,
    pub bytes: Option<Vec<u8>>,
    pub staged_upload: Option<StagedUpload>,
    pub content_type: Option<String>,
    pub file_name: Option<String>,
    pub content_list: Option<Value>,
    pub content_list_v2: Option<Value>,
    pub middle_json: Option<Value>,
    pub model_json: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct StagedUpload {
    inner: Arc<StagedUploadInner>,
    pub byte_len: u64,
    pub sha256: String,
}

#[derive(Debug)]
struct StagedUploadInner {
    path: PathBuf,
}

impl StagedUpload {
    pub(crate) fn new(path: PathBuf, byte_len: u64, sha256: String) -> Self {
        Self {
            inner: Arc::new(StagedUploadInner { path }),
            byte_len,
            sha256,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        &self.inner.path
    }

    pub(crate) async fn read_utf8(&self) -> Result<Option<String>, ApiError> {
        match tokio::fs::read_to_string(self.path()).await {
            Ok(value) => Ok(Some(value)),
            Err(err) if err.kind() == ErrorKind::InvalidData => Ok(None),
            Err(err) => Err(ApiError::Internal(format!(
                "failed to read staged upload: {err}"
            ))),
        }
    }
}

impl Drop for StagedUploadInner {
    fn drop(&mut self) {
        if let Err(err) = std::fs::remove_file(&self.path) {
            if err.kind() != ErrorKind::NotFound {
                tracing::warn!(
                    error_kind = "staged_upload_cleanup",
                    "failed to remove staged upload"
                );
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct ParserOutput {
    pub provider: String,
    pub backend: String,
    pub parser_version: Option<String>,
    pub markdown: Option<String>,
    pub content_list: Option<Value>,
    pub content_list_v2: Option<Value>,
    pub middle_json: Option<Value>,
    pub model_json: Option<Value>,
    pub images: Vec<Value>,
    pub blocks: Vec<ParsedBlock>,
}

#[async_trait]
pub trait DocumentParser: Send + Sync {
    async fn parse(&self, input: ParserInput) -> Result<ParserOutput, ApiError>;
}

/// Cloneable parser factory backed by one pooled HTTP client.
///
/// Per-request parser provider/backend overrides create only a lightweight
/// adapter. All MinerU adapters share the same connection pool and upstream
/// policy boundary captured at application startup.
#[derive(Clone)]
pub struct ParserRegistry {
    builtin: Arc<BuiltinTextParser>,
    http: Option<UpstreamHttpClient>,
}

impl ParserRegistry {
    pub fn new(config: &Config) -> Self {
        let proxy_mode = config.provider_proxy_mode.parse::<ProxyMode>().ok();
        let http = proxy_mode.and_then(|proxy_mode| {
            UpstreamHttpClient::build(&ClientPolicy {
                connect_timeout: Duration::from_millis(config.provider_connect_timeout_ms),
                request_timeout: Duration::from_millis(config.parser_timeout_ms),
                read_timeout: Duration::from_millis(config.parser_timeout_ms),
                proxy_mode,
            })
            .ok()
        });
        Self {
            builtin: Arc::new(BuiltinTextParser {
                max_metadata_bytes: config.parser_max_response_bytes,
            }),
            http,
        }
    }

    pub fn parser_for_config(&self, config: &Config) -> Result<Arc<dyn DocumentParser>, ApiError> {
        validate_parser_config(config)?;
        match config.parser_provider.as_str() {
            "builtin" => Ok(self.builtin.clone()),
            "mineru" => {
                let http = self.http.clone().ok_or_else(|| {
                    ApiError::Upstream("parser HTTP client initialization failed".to_string())
                })?;
                Ok(Arc::new(MineruParserClient::new(config, http)))
            }
            _ => Err(ApiError::bad_request(
                "parser_provider must be builtin or mineru",
            )),
        }
    }

    pub async fn health_status(&self, config: &Config) -> Value {
        if config.parser_provider != "mineru" {
            return builtin_health_status();
        }

        let started = Instant::now();
        let Some(http) = self.http.clone() else {
            return unhealthy_health_status(config, started, None, "client_initialization");
        };
        let url = format!("{}/health", config.mineru_api_url.trim_end_matches('/'));
        let request_client = http.client();
        let response = http
            .execute(
                UpstreamOperation::ParserHealth,
                &OperationPolicy::without_retries(
                    PARSER_HEALTH_TIMEOUT.min(Duration::from_millis(config.parser_timeout_ms)),
                    PARSER_HEALTH_MAX_RESPONSE_BYTES.min(config.parser_max_response_bytes),
                ),
                &new_id("parser_health"),
                move |_| {
                    let request = request_client.get(url.clone());
                    async move { Ok(request) }
                },
            )
            .await;

        match response {
            Ok(response) => match serde_json::from_slice::<Value>(response.body()) {
                Ok(status) => match mineru_health_signal(&status) {
                    Ok(MineruHealthSignal::Healthy) => json!({
                        "provider": "mineru",
                        "backend": &config.mineru_backend,
                        "healthy": true,
                        "configured": true,
                        "status": "ok",
                        "latency_ms": started.elapsed().as_millis() as u64,
                        "mineru": status
                    }),
                    Ok(MineruHealthSignal::Unhealthy) => {
                        unhealthy_health_status(config, started, None, "reported_unhealthy")
                    }
                    Err(()) => {
                        unhealthy_health_status(config, started, None, "invalid_health_contract")
                    }
                },
                Err(_) => unhealthy_health_status(config, started, None, "invalid_json"),
            },
            Err(error) => {
                let diagnostic = error.diagnostic();
                unhealthy_health_status(
                    config,
                    started,
                    diagnostic.status,
                    diagnostic.category.as_str(),
                )
            }
        }
    }
}

pub fn validate_parser_config(config: &Config) -> Result<(), ApiError> {
    if !matches!(config.parser_provider.as_str(), "builtin" | "mineru") {
        return Err(ApiError::bad_request(
            "parser_provider must be builtin or mineru",
        ));
    }
    if config.parser_provider == "mineru"
        && (config.mineru_backend.is_empty()
            || config.mineru_backend.len() > MAX_PARSER_LABEL_BYTES
            || !config
                .mineru_backend
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    {
        return Err(ApiError::bad_request("parser_backend is invalid"));
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct BuiltinTextParser {
    max_metadata_bytes: usize,
}

#[async_trait]
impl DocumentParser for BuiltinTextParser {
    async fn parse(&self, input: ParserInput) -> Result<ParserOutput, ApiError> {
        let markdown = match (input.content, input.bytes, input.staged_upload) {
            (Some(content), _, _) => Some(content),
            (None, Some(bytes), _) => Some(String::from_utf8(bytes).map_err(|_| {
                ApiError::bad_request("builtin parser only supports UTF-8 text uploads")
            })?),
            (None, None, Some(upload)) => Some(upload.read_utf8().await?.ok_or_else(|| {
                ApiError::bad_request("builtin parser only supports UTF-8 text uploads")
            })?),
            (None, None, None) => None,
        };
        let retained_base = preflight_parser_output_base(
            "builtin",
            "text",
            Some("builtin-text"),
            markdown.as_deref(),
            input.content_list.as_ref(),
            input.content_list_v2.as_ref(),
            input.middle_json.as_ref(),
            input.model_json.as_ref(),
            &[],
            self.max_metadata_bytes,
        )?;
        let blocks = parse_supplied_blocks(
            input.content_list.as_ref(),
            input.content_list_v2.as_ref(),
            self.max_metadata_bytes,
            retained_base,
        )?;
        let output = ParserOutput {
            provider: "builtin".to_string(),
            backend: "text".to_string(),
            parser_version: Some("builtin-text".to_string()),
            markdown,
            content_list: input.content_list,
            content_list_v2: input.content_list_v2,
            middle_json: input.middle_json,
            model_json: input.model_json,
            images: Vec::new(),
            blocks,
        };
        validate_parser_output(&output, self.max_metadata_bytes)?;
        Ok(output)
    }
}

#[derive(Debug, Clone)]
pub struct MineruParserClient {
    api_url: String,
    backend: String,
    return_md: bool,
    return_content_list: bool,
    return_middle_json: bool,
    return_images: bool,
    max_input_bytes: usize,
    client: UpstreamHttpClient,
    policy: OperationPolicy,
}

impl MineruParserClient {
    fn new(config: &Config, client: UpstreamHttpClient) -> Self {
        Self {
            api_url: config.mineru_api_url.clone(),
            backend: config.mineru_backend.clone(),
            return_md: config.mineru_return_md,
            return_content_list: config.mineru_return_content_list,
            return_middle_json: config.mineru_return_middle_json,
            return_images: config.mineru_return_images,
            max_input_bytes: config.max_upload_bytes,
            client,
            policy: OperationPolicy {
                deadline: Duration::from_millis(config.parser_timeout_ms),
                max_response_bytes: config.parser_max_response_bytes,
                max_retries: u8::try_from(config.provider_max_retries)
                    .unwrap_or(crate::upstream::MAX_UPSTREAM_RETRIES),
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
            },
        }
    }
}

#[derive(Clone)]
enum ReplayableParserBody {
    Staged(StagedUpload),
    Bytes(Arc<[u8]>),
}

impl ReplayableParserBody {
    async fn into_part(self, file_name: String) -> Result<Part, RequestFactoryError> {
        match self {
            Self::Staged(upload) => {
                let file = tokio::fs::File::open(upload.path())
                    .await
                    .map_err(|_| RequestFactoryError::Io)?;
                Ok(Part::stream_with_length(file, upload.byte_len).file_name(file_name))
            }
            Self::Bytes(bytes) => Ok(Part::bytes(bytes.to_vec()).file_name(file_name)),
        }
    }
}

#[async_trait]
impl DocumentParser for MineruParserClient {
    async fn parse(&self, input: ParserInput) -> Result<ParserOutput, ApiError> {
        if input.content_list.is_some() || input.content_list_v2.is_some() {
            let retained_base = preflight_parser_output_base(
                "mineru",
                &self.backend,
                Some("mineru-supplied"),
                input.content.as_deref(),
                input.content_list.as_ref(),
                input.content_list_v2.as_ref(),
                input.middle_json.as_ref(),
                input.model_json.as_ref(),
                &[],
                self.policy.max_response_bytes,
            )?;
            let blocks = parse_supplied_blocks(
                input.content_list.as_ref(),
                input.content_list_v2.as_ref(),
                self.policy.max_response_bytes,
                retained_base,
            )?;
            let output = ParserOutput {
                provider: "mineru".to_string(),
                backend: self.backend.clone(),
                parser_version: Some("mineru-supplied".to_string()),
                markdown: input.content,
                content_list: input.content_list,
                content_list_v2: input.content_list_v2,
                middle_json: input.middle_json,
                model_json: input.model_json,
                images: Vec::new(),
                blocks,
            };
            validate_parser_output(&output, self.policy.max_response_bytes)?;
            return Ok(output);
        }

        let file_name = input
            .file_name
            .unwrap_or_else(|| "document.txt".to_string());
        if file_name.is_empty()
            || file_name.len() > MAX_FILE_NAME_BYTES
            || file_name.chars().any(char::is_control)
        {
            return Err(ApiError::bad_request("file_name is invalid"));
        }
        if let Some(content_type) = input.content_type.as_deref() {
            Part::bytes(Vec::new())
                .mime_str(content_type)
                .map_err(|_| ApiError::bad_request("content_type is invalid"))?;
        }
        let body = if let Some(upload) = input.staged_upload {
            if upload.byte_len > u64::try_from(self.max_input_bytes).unwrap_or(u64::MAX) {
                return Err(ApiError::payload_too_large());
            }
            ReplayableParserBody::Staged(upload)
        } else {
            let bytes = input
                .bytes
                .or_else(|| input.content.map(String::into_bytes))
                .ok_or_else(|| {
                    ApiError::bad_request("content or uploaded file bytes are required")
                })?;
            if bytes.len() > self.max_input_bytes {
                return Err(ApiError::payload_too_large());
            }
            ReplayableParserBody::Bytes(Arc::from(bytes))
        };
        let url = format!("{}/file_parse", self.api_url.trim_end_matches('/'));
        let client = self.client.client();
        let backend = self.backend.clone();
        let content_type = input.content_type;
        let return_md = self.return_md;
        let return_content_list = self.return_content_list;
        let return_middle_json = self.return_middle_json;
        let return_images = self.return_images;
        let response = self
            .client
            .execute(
                UpstreamOperation::ParserUpload,
                &self.policy,
                &new_id("parser"),
                move |_| {
                    let body = body.clone();
                    let file_name = file_name.clone();
                    let content_type = content_type.clone();
                    let backend = backend.clone();
                    let client = client.clone();
                    let url = url.clone();
                    async move {
                        let mut part = body.into_part(file_name).await?;
                        if let Some(content_type) = content_type.as_deref() {
                            part = part
                                .mime_str(content_type)
                                .map_err(|_| RequestFactoryError::InvalidInput)?;
                        }
                        let form = Form::new()
                            .part("files", part)
                            .text("backend", backend)
                            .text("return_md", return_md.to_string())
                            .text("return_content_list", return_content_list.to_string())
                            .text("return_middle_json", return_middle_json.to_string())
                            .text("return_images", return_images.to_string());
                        Ok(client.post(url).multipart(form))
                    }
                },
            )
            .await
            .map_err(parser_upstream_error)?;

        let value = serde_json::from_slice::<Value>(response.body())
            .map_err(|_| ApiError::Upstream("parser response was not valid JSON".to_string()))?;
        let payload = mineru_payload(&value);
        let markdown = first_stringish_field(payload, &["markdown", "md", "content", "text"]);
        let content_list = first_value_field_ref(payload, &["content_list"]);
        let content_list_v2 = first_value_field_ref(payload, &["content_list_v2"]);
        let middle_json =
            first_value_field_ref(payload, &["middle_json", "middle", "middle_json_data"]);
        let model_json = first_value_field_ref(payload, &["model_json", "model"]);
        let images = first_value_field_ref(payload, &["images"])
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let parser_version = first_stringish_field(payload, &["parser_version", "version"])
            .unwrap_or(Cow::Borrowed("mineru-api"));
        let retained_base = preflight_parser_output_base(
            "mineru",
            &self.backend,
            Some(parser_version.as_ref()),
            markdown.as_deref(),
            content_list,
            content_list_v2,
            middle_json,
            model_json,
            images,
            self.policy.max_response_bytes,
        )?;
        let blocks = parse_supplied_blocks(
            content_list,
            content_list_v2,
            self.policy.max_response_bytes,
            retained_base,
        )?;

        let output = ParserOutput {
            provider: "mineru".to_string(),
            backend: self.backend.clone(),
            parser_version: Some(parser_version.into_owned()),
            markdown: markdown.map(Cow::into_owned),
            content_list: content_list.cloned(),
            content_list_v2: content_list_v2.cloned(),
            middle_json: middle_json.cloned(),
            model_json: model_json.cloned(),
            images: images.to_vec(),
            blocks,
        };
        validate_parser_output(&output, self.policy.max_response_bytes)?;
        Ok(output)
    }
}

fn builtin_health_status() -> Value {
    json!({
        "provider": "builtin",
        "backend": "text",
        "healthy": true,
        "configured": true,
        "status": "ok",
        "latency_ms": 0
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MineruHealthSignal {
    Healthy,
    Unhealthy,
}

/// Interpret the smallest health contract already used by this service.
///
/// MinerU must return a JSON object containing `healthy: bool` and/or a
/// `status` string. The accepted status vocabulary mirrors our dependency
/// health surfaces and MinerU's published endpoint: `ok` or `healthy` are
/// positive, while `unhealthy` and `degraded` are negative. If both fields are
/// present they must agree; missing, mistyped, unknown, or conflicting fields
/// fail closed.
fn mineru_health_signal(value: &Value) -> Result<MineruHealthSignal, ()> {
    let object = value.as_object().ok_or(())?;
    let healthy = match object.get("healthy") {
        Some(Value::Bool(true)) => Some(MineruHealthSignal::Healthy),
        Some(Value::Bool(false)) => Some(MineruHealthSignal::Unhealthy),
        Some(_) => return Err(()),
        None => None,
    };
    let status = match object.get("status") {
        Some(Value::String(status)) if matches!(status.as_str(), "ok" | "healthy") => {
            Some(MineruHealthSignal::Healthy)
        }
        Some(Value::String(status)) if matches!(status.as_str(), "unhealthy" | "degraded") => {
            Some(MineruHealthSignal::Unhealthy)
        }
        Some(_) => return Err(()),
        None => None,
    };

    match (healthy, status) {
        (Some(left), Some(right)) if left == right => Ok(left),
        (Some(signal), None) | (None, Some(signal)) => Ok(signal),
        _ => Err(()),
    }
}

fn unhealthy_health_status(
    config: &Config,
    started: Instant,
    http_status: Option<u16>,
    category: &str,
) -> Value {
    let mut status = json!({
        "provider": "mineru",
        "backend": &config.mineru_backend,
        "healthy": false,
        "configured": true,
        "status": "unhealthy",
        "error_category": category,
        "latency_ms": started.elapsed().as_millis() as u64
    });
    if let (Some(object), Some(http_status)) = (status.as_object_mut(), http_status) {
        object.insert("http_status".to_string(), json!(http_status));
    }
    status
}

fn parser_upstream_error(error: crate::upstream::UpstreamError) -> ApiError {
    ApiError::Upstream(error.to_string())
}

/// Defense-in-depth validation before parser output can be persisted.
///
/// The HTTP body limit bounds wire bytes. These checks additionally bound
/// post-parse amplification (for example, cloned block fields and nested image
/// metadata) and reject malformed provider labels without echoing any content.
pub fn validate_parser_output(
    output: &ParserOutput,
    max_metadata_bytes: usize,
) -> Result<(), ApiError> {
    if max_metadata_bytes == 0
        || !is_safe_label(&output.provider)
        || !is_safe_label(&output.backend)
        || output
            .parser_version
            .as_deref()
            .is_some_and(|value| !is_safe_label(value))
    {
        return Err(unsafe_parser_output());
    }
    if output.blocks.len() > MAX_PARSED_BLOCKS || output.images.len() > MAX_PARSED_IMAGES {
        return Err(unsafe_parser_output());
    }

    let mut retained_bytes = 0usize;
    for value in [
        Some(output.provider.as_str()),
        Some(output.backend.as_str()),
        output.parser_version.as_deref(),
        output.markdown.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        add_retained_bytes(&mut retained_bytes, value.len(), max_metadata_bytes)?;
    }

    for value in [
        output.content_list.as_ref(),
        output.content_list_v2.as_ref(),
        output.middle_json.as_ref(),
        output.model_json.as_ref(),
    ]
    .into_iter()
    .flatten()
    {
        let len = encoded_json_len(value)?;
        add_retained_bytes(&mut retained_bytes, len, max_metadata_bytes)?;
    }

    for image in &output.images {
        let len = encoded_json_len(image)?;
        let reference = image.as_str().or_else(|| {
            image
                .get("uri")
                .or_else(|| image.get("path"))
                .or_else(|| image.get("image_path"))
                .and_then(Value::as_str)
        });
        if len > MAX_BLOCK_FIELD_BYTES
            || reference.is_some_and(|value| value.len() > MAX_REFERENCE_BYTES)
        {
            return Err(unsafe_parser_output());
        }
        add_retained_bytes(&mut retained_bytes, len, max_metadata_bytes)?;
    }

    for block in &output.blocks {
        let bbox_len = block
            .bbox
            .as_ref()
            .map(encoded_json_len)
            .transpose()?
            .unwrap_or(0);
        if block.block_id.is_empty()
            || block.block_id.len() > MAX_BLOCK_ID_BYTES
            || block.block_id.chars().any(char::is_control)
            || block.block_type.is_empty()
            || block.block_type.len() > MAX_BLOCK_TYPE_BYTES
            || block.block_type.chars().any(char::is_control)
            || block.section_path.len() > MAX_SECTION_DEPTH
            || block
                .image_ref
                .as_deref()
                .is_some_and(|value| value.len() > MAX_REFERENCE_BYTES)
            || bbox_len > MAX_BBOX_BYTES
        {
            return Err(unsafe_parser_output());
        }
        add_retained_bytes(
            &mut retained_bytes,
            block.block_id.len(),
            max_metadata_bytes,
        )?;
        add_retained_bytes(
            &mut retained_bytes,
            block.block_type.len(),
            max_metadata_bytes,
        )?;
        add_retained_bytes(&mut retained_bytes, bbox_len, max_metadata_bytes)?;
        for field in [
            block.text.as_deref(),
            block.html.as_deref(),
            block.latex.as_deref(),
            block.image_ref.as_deref(),
            block.caption.as_deref(),
            block.footnote.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if field.len() > MAX_BLOCK_FIELD_BYTES {
                return Err(unsafe_parser_output());
            }
            add_retained_bytes(&mut retained_bytes, field.len(), max_metadata_bytes)?;
        }
        for section in &block.section_path {
            if section.len() > MAX_SECTION_BYTES {
                return Err(unsafe_parser_output());
            }
            add_retained_bytes(&mut retained_bytes, section.len(), max_metadata_bytes)?;
        }
    }
    Ok(())
}

fn add_retained_bytes(
    total: &mut usize,
    amount: usize,
    max_metadata_bytes: usize,
) -> Result<(), ApiError> {
    *total = total.checked_add(amount).ok_or_else(unsafe_parser_output)?;
    if *total > max_metadata_bytes {
        return Err(unsafe_parser_output());
    }
    Ok(())
}

fn is_safe_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_PARSER_LABEL_BYTES
        && !value.chars().any(char::is_control)
}

fn encoded_json_len(value: &Value) -> Result<usize, ApiError> {
    encoded_json_len_bounded(value, usize::MAX)
}

fn encoded_json_len_bounded(value: &Value, max_bytes: usize) -> Result<usize, ApiError> {
    let mut writer = BoundedCountingWriter {
        written: 0,
        max_bytes,
    };
    serde_json::to_writer(&mut writer, value).map_err(|_| unsafe_parser_output())?;
    Ok(writer.written)
}

struct BoundedCountingWriter {
    written: usize,
    max_bytes: usize,
}

impl Write for BoundedCountingWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let next = self
            .written
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("JSON size overflow"))?;
        if next > self.max_bytes {
            return Err(std::io::Error::other(
                "JSON exceeds parser materialization limit",
            ));
        }
        self.written = next;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct ParserMaterializationBudget {
    retained_bytes: usize,
    max_metadata_bytes: usize,
}

impl ParserMaterializationBudget {
    fn with_retained(max_metadata_bytes: usize, retained_bytes: usize) -> Result<Self, ApiError> {
        if max_metadata_bytes == 0 || retained_bytes > max_metadata_bytes {
            return Err(unsafe_parser_output());
        }
        Ok(Self {
            retained_bytes,
            max_metadata_bytes,
        })
    }

    fn reserve(&mut self, amount: usize) -> Result<(), ApiError> {
        add_retained_bytes(&mut self.retained_bytes, amount, self.max_metadata_bytes)
    }

    fn remaining(&self) -> usize {
        self.max_metadata_bytes - self.retained_bytes
    }
}

#[allow(clippy::too_many_arguments)]
fn preflight_parser_output_base(
    provider: &str,
    backend: &str,
    parser_version: Option<&str>,
    markdown: Option<&str>,
    content_list: Option<&Value>,
    content_list_v2: Option<&Value>,
    middle_json: Option<&Value>,
    model_json: Option<&Value>,
    images: &[Value],
    max_metadata_bytes: usize,
) -> Result<usize, ApiError> {
    if !is_safe_label(provider)
        || !is_safe_label(backend)
        || parser_version.is_some_and(|value| !is_safe_label(value))
        || images.len() > MAX_PARSED_IMAGES
    {
        return Err(unsafe_parser_output());
    }

    let mut budget = ParserMaterializationBudget::with_retained(max_metadata_bytes, 0)?;
    for value in [Some(provider), Some(backend), parser_version, markdown]
        .into_iter()
        .flatten()
    {
        budget.reserve(value.len())?;
    }
    for value in [content_list, content_list_v2, middle_json, model_json]
        .into_iter()
        .flatten()
    {
        let len = encoded_json_len_bounded(value, budget.remaining())?;
        budget.reserve(len)?;
    }
    for image in images {
        let len = encoded_json_len_bounded(image, MAX_BLOCK_FIELD_BYTES.min(budget.remaining()))?;
        let reference = image.as_str().or_else(|| {
            image
                .get("uri")
                .or_else(|| image.get("path"))
                .or_else(|| image.get("image_path"))
                .and_then(Value::as_str)
        });
        if reference.is_some_and(|value| value.len() > MAX_REFERENCE_BYTES) {
            return Err(unsafe_parser_output());
        }
        budget.reserve(len)?;
    }
    Ok(budget.retained_bytes)
}

fn unsafe_parser_output() -> ApiError {
    ApiError::Upstream("parser output failed safety validation".to_string())
}

pub struct MineruContentListParser;

impl MineruContentListParser {
    pub fn parse(value: &Value, max_metadata_bytes: usize) -> Result<Vec<ParsedBlock>, ApiError> {
        let retained_raw = encoded_json_len_bounded(value, max_metadata_bytes)?;
        Self::parse_with_retained_base(value, max_metadata_bytes, retained_raw)
    }

    fn parse_with_retained_base(
        value: &Value,
        max_metadata_bytes: usize,
        retained_base: usize,
    ) -> Result<Vec<ParsedBlock>, ApiError> {
        let mut items = Vec::new();
        collect_block_items(value, &mut items, 0)?;

        let mut budget =
            ParserMaterializationBudget::with_retained(max_metadata_bytes, retained_base)?;
        let mut section_stack: Vec<String> = Vec::new();
        let mut blocks = Vec::with_capacity(items.len());
        for (idx, item) in items.iter().enumerate() {
            let raw_block_type = first_stringish_field(item, &["type", "content_type"]);
            if raw_block_type.as_deref().is_some_and(|value| {
                value.len() > MAX_BLOCK_TYPE_BYTES || value.chars().any(char::is_control)
            }) {
                return Err(unsafe_parser_output());
            }
            let block_type = normalize_block_type(raw_block_type.as_deref());
            budget.reserve(block_type.len())?;

            let text_limit = if block_type == "title" {
                MAX_SECTION_BYTES
            } else {
                MAX_BLOCK_FIELD_BYTES
            };
            let text = bounded_string_field(
                item,
                &["text", "content", "md", "markdown"],
                text_limit,
                false,
                &mut budget,
            )?;
            let html = bounded_string_field(
                item,
                &["html", "table_html", "table_body"],
                MAX_BLOCK_FIELD_BYTES,
                false,
                &mut budget,
            )?;
            let latex = bounded_string_field(
                item,
                &["latex", "latex_text", "formula", "equation"],
                MAX_BLOCK_FIELD_BYTES,
                false,
                &mut budget,
            )?;
            let image_ref = bounded_string_field(
                item,
                &["image_ref", "image_path", "img_path", "image", "src"],
                MAX_REFERENCE_BYTES,
                false,
                &mut budget,
            )?;
            let caption = bounded_textish_field(
                item,
                &["caption", "image_caption", "table_caption"],
                MAX_BLOCK_FIELD_BYTES,
                &mut budget,
            )?;
            let footnote = bounded_textish_field(
                item,
                &["footnote", "image_footnote", "table_footnote"],
                MAX_BLOCK_FIELD_BYTES,
                &mut budget,
            )?;
            let text_level = first_u8_field(item, &["text_level", "level", "heading_level"])
                .or_else(|| (block_type == "title").then_some(1));
            let section_path = if let Some(section_path) =
                bounded_section_path_from_item(item, &mut budget)?
            {
                section_path
            } else {
                if block_type == "title" {
                    if let Some(title) = text.as_ref().filter(|value| !value.trim().is_empty()) {
                        let level = text_level.unwrap_or(1).max(1) as usize;
                        if level > MAX_SECTION_DEPTH {
                            return Err(unsafe_parser_output());
                        }
                        section_stack.truncate(level.saturating_sub(1));
                        budget.reserve(title.len())?;
                        section_stack.push(title.clone());
                    }
                }
                clone_section_stack(&section_stack, &mut budget)?
            };
            let reading_order =
                first_u32_field(item, &["reading_order", "order", "index"]).unwrap_or(idx as u32);
            let page_idx = first_u32_field(item, &["page_idx", "page_index", "page"]);
            let bbox = bounded_json_field(
                item,
                &["bbox", "box", "bounding_box"],
                MAX_BBOX_BYTES,
                &mut budget,
            )?;
            let block_id = if let Some(block_id) = bounded_string_field(
                item,
                &["block_id", "id"],
                MAX_BLOCK_ID_BYTES,
                true,
                &mut budget,
            )? {
                block_id
            } else {
                let block_id = stable_block_id(
                    idx as u32,
                    &block_type,
                    page_idx,
                    text.as_deref()
                        .or(html.as_deref())
                        .or(latex.as_deref())
                        .or(caption.as_deref())
                        .unwrap_or_default(),
                );
                budget.reserve(block_id.len())?;
                block_id
            };

            blocks.push(ParsedBlock {
                block_id,
                block_type,
                page_idx,
                bbox,
                text,
                html,
                latex,
                image_ref,
                caption,
                footnote,
                text_level,
                section_path,
                reading_order,
            });
        }
        Ok(blocks)
    }
}

fn parse_supplied_blocks(
    content_list: Option<&Value>,
    content_list_v2: Option<&Value>,
    max_metadata_bytes: usize,
    retained_base: usize,
) -> Result<Vec<ParsedBlock>, ApiError> {
    if let Some(value) = content_list_v2 {
        let blocks = MineruContentListParser::parse_with_retained_base(
            value,
            max_metadata_bytes,
            retained_base,
        )?;
        if !blocks.is_empty() {
            return Ok(blocks);
        }
    }
    match content_list {
        Some(value) => MineruContentListParser::parse_with_retained_base(
            value,
            max_metadata_bytes,
            retained_base,
        ),
        None => Ok(Vec::new()),
    }
}

fn collect_block_items<'a>(
    value: &'a Value,
    out: &mut Vec<&'a Value>,
    depth: usize,
) -> Result<(), ApiError> {
    if depth > MAX_SECTION_DEPTH {
        return Err(unsafe_parser_output());
    }
    match value {
        Value::Array(items) => {
            if items.len() > MAX_PARSED_BLOCKS.saturating_sub(out.len()) {
                return Err(unsafe_parser_output());
            }
            out.extend(items.iter());
        }
        Value::Object(map) => {
            for key in [
                "content_list_v2",
                "content_list",
                "blocks",
                "items",
                "data",
                "result",
            ] {
                if let Some(nested) = map.get(key) {
                    return collect_block_items(nested, out, depth + 1);
                }
            }
            if out.len() == MAX_PARSED_BLOCKS {
                return Err(unsafe_parser_output());
            }
            out.push(value);
        }
        _ => {}
    }
    Ok(())
}

fn mineru_payload(value: &Value) -> &Value {
    if let Some(data) = value.get("data") {
        if let Some(results) = data.get("results").and_then(Value::as_array) {
            return results.first().unwrap_or(data);
        }
        return data;
    }
    if let Some(result) = value.get("result") {
        return result;
    }
    if let Some(results) = value.get("results").and_then(Value::as_array) {
        return results.first().unwrap_or(value);
    }
    value
}

fn first_value_field_ref<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a Value> {
    keys.iter().find_map(|key| value.get(*key))
}

fn first_stringish_field<'a>(value: &'a Value, keys: &[&str]) -> Option<Cow<'a, str>> {
    keys.iter().find_map(|key| stringish_ref(value.get(*key)?))
}

fn bounded_string_field(
    value: &Value,
    keys: &[&str],
    max_bytes: usize,
    reject_control: bool,
    budget: &mut ParserMaterializationBudget,
) -> Result<Option<String>, ApiError> {
    let Some(text) = first_stringish_field(value, keys) else {
        return Ok(None);
    };
    if text.len() > max_bytes || (reject_control && text.chars().any(char::is_control)) {
        return Err(unsafe_parser_output());
    }
    budget.reserve(text.len())?;
    Ok(Some(text.into_owned()))
}

fn bounded_json_field(
    value: &Value,
    keys: &[&str],
    max_bytes: usize,
    budget: &mut ParserMaterializationBudget,
) -> Result<Option<Value>, ApiError> {
    let Some(value) = first_value_field_ref(value, keys) else {
        return Ok(None);
    };
    let len = encoded_json_len_bounded(value, max_bytes.min(budget.remaining()))?;
    budget.reserve(len)?;
    Ok(Some(value.clone()))
}

fn first_u32_field(value: &Value, keys: &[&str]) -> Option<u32> {
    keys.iter().find_map(|key| u32ish(value.get(*key)?))
}

fn first_u8_field(value: &Value, keys: &[&str]) -> Option<u8> {
    keys.iter()
        .find_map(|key| u32ish(value.get(*key)?).and_then(|value| u8::try_from(value).ok()))
}

fn stringish_ref(value: &Value) -> Option<Cow<'_, str>> {
    match value {
        Value::String(text) if !text.trim().is_empty() => Some(Cow::Borrowed(text)),
        Value::Number(number) => Some(Cow::Owned(number.to_string())),
        _ => None,
    }
}

#[derive(Debug, Default)]
struct TextishMeasure {
    bytes: usize,
    parts: usize,
}

fn measure_textish(
    value: &Value,
    depth: usize,
    measure: &mut TextishMeasure,
) -> Result<(), ApiError> {
    if depth > MAX_SECTION_DEPTH {
        return Err(unsafe_parser_output());
    }
    match value {
        Value::String(text) if !text.trim().is_empty() => add_textish_part(measure, text.len())?,
        Value::Array(items) => {
            for item in items {
                measure_textish(item, depth + 1, measure)?;
            }
        }
        Value::Object(_) => {
            if let Some(text) = first_stringish_field(value, &["text", "content", "caption"]) {
                add_textish_part(measure, text.len())?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn add_textish_part(measure: &mut TextishMeasure, bytes: usize) -> Result<(), ApiError> {
    let separator = usize::from(measure.parts > 0);
    measure.bytes = measure
        .bytes
        .checked_add(separator)
        .and_then(|total| total.checked_add(bytes))
        .ok_or_else(unsafe_parser_output)?;
    measure.parts = measure
        .parts
        .checked_add(1)
        .ok_or_else(unsafe_parser_output)?;
    Ok(())
}

fn append_textish(value: &Value, output: &mut String, has_part: &mut bool) {
    match value {
        Value::String(text) if !text.trim().is_empty() => {
            append_textish_part(output, has_part, text);
        }
        Value::Array(items) => {
            for item in items {
                append_textish(item, output, has_part);
            }
        }
        Value::Object(_) => {
            if let Some(text) = first_stringish_field(value, &["text", "content", "caption"]) {
                append_textish_part(output, has_part, text.as_ref());
            }
        }
        _ => {}
    }
}

fn append_textish_part(output: &mut String, has_part: &mut bool, text: &str) {
    if *has_part {
        output.push('\n');
    }
    output.push_str(text);
    *has_part = true;
}

fn bounded_textish_field(
    value: &Value,
    keys: &[&str],
    max_bytes: usize,
    budget: &mut ParserMaterializationBudget,
) -> Result<Option<String>, ApiError> {
    for key in keys {
        let Some(candidate) = value.get(*key) else {
            continue;
        };
        let mut measure = TextishMeasure::default();
        measure_textish(candidate, 0, &mut measure)?;
        if measure.parts == 0 {
            continue;
        }
        if measure.bytes > max_bytes {
            return Err(unsafe_parser_output());
        }
        budget.reserve(measure.bytes)?;
        let mut output = String::with_capacity(measure.bytes);
        let mut has_part = false;
        append_textish(candidate, &mut output, &mut has_part);
        debug_assert_eq!(output.len(), measure.bytes);
        return Ok(Some(output));
    }
    Ok(None)
}

fn u32ish(value: &Value) -> Option<u32> {
    match value {
        Value::Number(number) => number.as_u64().and_then(|value| u32::try_from(value).ok()),
        Value::String(text) => text.parse::<u32>().ok(),
        _ => None,
    }
}

fn bounded_section_path_from_item(
    value: &Value,
    budget: &mut ParserMaterializationBudget,
) -> Result<Option<Vec<String>>, ApiError> {
    let Some(path) = value.get("section_path") else {
        return Ok(None);
    };
    match path {
        Value::Array(items) => {
            if items.len() > MAX_SECTION_DEPTH {
                return Err(unsafe_parser_output());
            }
            let sections = items.iter().filter_map(stringish_ref).collect::<Vec<_>>();
            if sections.is_empty() {
                return Ok(None);
            }
            let mut total = 0usize;
            for section in &sections {
                if section.len() > MAX_SECTION_BYTES {
                    return Err(unsafe_parser_output());
                }
                total = total
                    .checked_add(section.len())
                    .ok_or_else(unsafe_parser_output)?;
            }
            budget.reserve(total)?;
            Ok(Some(sections.into_iter().map(Cow::into_owned).collect()))
        }
        Value::String(text) if !text.trim().is_empty() => {
            if text.len() > MAX_SECTION_BYTES {
                return Err(unsafe_parser_output());
            }
            budget.reserve(text.len())?;
            Ok(Some(vec![text.clone()]))
        }
        _ => Ok(None),
    }
}

fn clone_section_stack(
    section_stack: &[String],
    budget: &mut ParserMaterializationBudget,
) -> Result<Vec<String>, ApiError> {
    if section_stack.len() > MAX_SECTION_DEPTH {
        return Err(unsafe_parser_output());
    }
    let total = section_stack.iter().try_fold(0usize, |total, section| {
        if section.len() > MAX_SECTION_BYTES {
            return Err(unsafe_parser_output());
        }
        total
            .checked_add(section.len())
            .ok_or_else(unsafe_parser_output)
    })?;
    budget.reserve(total)?;
    Ok(section_stack.to_vec())
}

fn normalize_block_type(value: Option<&str>) -> String {
    match value.unwrap_or("paragraph").to_ascii_lowercase().as_str() {
        "title" | "heading" => "title",
        "paragraph" | "para" | "text" | "plain_text" => "paragraph",
        "table" | "table_body" => "table",
        "equation" | "formula" | "interline_equation" | "inline_equation" => "equation",
        "image" | "figure" | "image_body" => "image",
        "chart" => "chart",
        "code" | "code_block" => "code",
        "list" | "list_item" => "list",
        "seal" => "seal",
        "header" => "header",
        "footer" => "footer",
        "page_number" | "page" => "page_number",
        "footnote" => "footnote",
        _ => "paragraph",
    }
    .to_string()
}

fn stable_block_id(index: u32, block_type: &str, page_idx: Option<u32>, body: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(index.to_le_bytes());
    hasher.update(block_type.as_bytes());
    hasher.update(page_idx.unwrap_or_default().to_le_bytes());
    hasher.update(body.as_bytes());
    format!(
        "block_{}",
        hex::encode(hasher.finalize())
            .chars()
            .take(24)
            .collect::<String>()
    )
}

#[cfg(test)]
mod tests {
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };

    use axum::{
        body::Bytes,
        extract::State,
        http::{HeaderMap, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, post},
        Json, Router,
    };

    use super::*;

    #[derive(Clone, Copy)]
    enum MockBehavior {
        FailOnce,
        Oversized,
    }

    #[derive(Clone)]
    struct MockState {
        behavior: MockBehavior,
        attempts: Arc<AtomicUsize>,
        bodies: Arc<Mutex<Vec<Vec<u8>>>>,
        request_ids: Arc<Mutex<Vec<String>>>,
    }

    struct MockServer {
        url: String,
        task: tokio::task::JoinHandle<()>,
        state: MockState,
    }

    struct HealthMockServer {
        url: String,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for MockServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    impl Drop for HealthMockServer {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    async fn mock_parse(
        State(state): State<MockState>,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        let attempt = state.attempts.fetch_add(1, Ordering::SeqCst);
        state.bodies.lock().unwrap().push(body.to_vec());
        state.request_ids.lock().unwrap().push(
            headers
                .get(crate::upstream::X_CLIENT_REQUEST_ID)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string(),
        );
        match state.behavior {
            MockBehavior::FailOnce if attempt == 0 => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": {"type": "server_error"}})),
            )
                .into_response(),
            MockBehavior::FailOnce => Json(json!({
                "data": {
                    "results": [{
                        "markdown": "parsed",
                        "content_list": [{"type": "text", "text": "parsed"}]
                    }]
                }
            }))
            .into_response(),
            MockBehavior::Oversized => (StatusCode::OK, "x".repeat(4096)).into_response(),
        }
    }

    async fn spawn_mock(behavior: MockBehavior) -> MockServer {
        let state = MockState {
            behavior,
            attempts: Arc::new(AtomicUsize::new(0)),
            bodies: Arc::new(Mutex::new(Vec::new())),
            request_ids: Arc::new(Mutex::new(Vec::new())),
        };
        let app = Router::new()
            .route("/file_parse", post(mock_parse))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        MockServer {
            url: format!("http://{address}"),
            task,
            state,
        }
    }

    async fn mock_health(State(body): State<Value>) -> Json<Value> {
        Json(body)
    }

    async fn spawn_health_mock(body: Value) -> HealthMockServer {
        let app = Router::new()
            .route("/health", get(mock_health))
            .with_state(body);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        HealthMockServer {
            url: format!("http://{address}"),
            task,
        }
    }

    fn mineru_config(server: &MockServer) -> Config {
        mineru_config_for_url(&server.url)
    }

    fn mineru_config_for_url(url: &str) -> Config {
        let mut config = Config::test();
        config.parser_provider = "mineru".to_string();
        config.mineru_api_url = url.to_string();
        config.provider_proxy_mode = "direct".to_string();
        config.provider_max_retries = 1;
        config.provider_connect_timeout_ms = 1_000;
        config.parser_timeout_ms = 5_000;
        config.parser_max_response_bytes = 16 * 1024;
        config
    }

    fn parser_input(staged_upload: StagedUpload) -> ParserInput {
        ParserInput {
            content: None,
            bytes: None,
            staged_upload: Some(staged_upload),
            content_type: Some("text/plain".to_string()),
            file_name: Some("document.txt".to_string()),
            content_list: None,
            content_list_v2: None,
            middle_json: None,
            model_json: None,
        }
    }

    #[tokio::test]
    async fn mineru_retry_reopens_and_replays_staged_multipart() {
        let server = spawn_mock(MockBehavior::FailOnce).await;
        let config = mineru_config(&server);
        let parser = ParserRegistry::new(&config)
            .parser_for_config(&config)
            .unwrap();
        let bytes = b"multipart-replay-evidence";
        let path = std::env::temp_dir().join(format!("{}.txt", new_id("parser_test")));
        tokio::fs::write(&path, bytes).await.unwrap();
        let upload = StagedUpload::new(path, bytes.len() as u64, "test-sha256".to_string());

        let output = parser.parse(parser_input(upload)).await.unwrap();

        assert_eq!(output.markdown.as_deref(), Some("parsed"));
        assert_eq!(server.state.attempts.load(Ordering::SeqCst), 2);
        let bodies = server.state.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 2);
        assert!(bodies
            .iter()
            .all(|body| body.windows(bytes.len()).any(|window| window == bytes)));
        let request_ids = server.state.request_ids.lock().unwrap();
        assert_eq!(request_ids.len(), 2);
        assert!(!request_ids[0].is_empty());
        assert_eq!(request_ids[0], request_ids[1]);
    }

    #[tokio::test]
    async fn mineru_rejects_oversized_response_without_body_preview() {
        let server = spawn_mock(MockBehavior::Oversized).await;
        let mut config = mineru_config(&server);
        config.provider_max_retries = 0;
        config.parser_max_response_bytes = 128;
        let parser = ParserRegistry::new(&config)
            .parser_for_config(&config)
            .unwrap();

        let error = parser
            .parse(ParserInput {
                content: Some("input".to_string()),
                bytes: None,
                staged_upload: None,
                content_type: Some("text/plain".to_string()),
                file_name: Some("document.txt".to_string()),
                content_list: None,
                content_list_v2: None,
                middle_json: None,
                model_json: None,
            })
            .await
            .unwrap_err();

        let ApiError::Upstream(cause) = error else {
            panic!("expected safe upstream error");
        };
        assert!(cause.contains("response_too_large"));
        assert!(!cause.contains(&"x".repeat(32)));
        assert_eq!(server.state.attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn mineru_health_accepts_official_healthy_status() {
        let server = spawn_health_mock(json!({
            "status": "healthy",
            "version": "2.1.0",
            "protocol_version": "1.0"
        }))
        .await;
        let config = mineru_config_for_url(&server.url);

        let status = ParserRegistry::new(&config).health_status(&config).await;

        assert_eq!(status["healthy"], true);
        assert_eq!(status["status"], "ok");
    }

    #[tokio::test]
    async fn mineru_health_accepts_explicit_healthy_bool() {
        let server = spawn_health_mock(json!({"healthy": true})).await;
        let config = mineru_config_for_url(&server.url);

        let status = ParserRegistry::new(&config).health_status(&config).await;

        assert_eq!(status["healthy"], true);
        assert_eq!(status["status"], "ok");
    }

    #[tokio::test]
    async fn mineru_health_rejects_200_reported_unhealthy() {
        let server = spawn_health_mock(json!({"healthy": false, "status": "degraded"})).await;
        let config = mineru_config_for_url(&server.url);

        let status = ParserRegistry::new(&config).health_status(&config).await;

        assert_eq!(status["healthy"], false);
        assert_eq!(status["status"], "unhealthy");
        assert_eq!(status["error_category"], "reported_unhealthy");
    }

    #[tokio::test]
    async fn mineru_health_rejects_200_without_health_signal() {
        let server = spawn_health_mock(json!({"version": "2.1.0"})).await;
        let config = mineru_config_for_url(&server.url);

        let status = ParserRegistry::new(&config).health_status(&config).await;

        assert_eq!(status["healthy"], false);
        assert_eq!(status["status"], "unhealthy");
        assert_eq!(status["error_category"], "invalid_health_contract");
    }

    #[tokio::test]
    async fn mineru_health_rejects_conflicting_signals() {
        let server = spawn_health_mock(json!({"healthy": true, "status": "unhealthy"})).await;
        let config = mineru_config_for_url(&server.url);

        let status = ParserRegistry::new(&config).health_status(&config).await;

        assert_eq!(status["healthy"], false);
        assert_eq!(status["error_category"], "invalid_health_contract");
    }

    #[test]
    fn parser_output_validation_rejects_post_parse_amplification() {
        let output = ParserOutput {
            provider: "mineru".to_string(),
            backend: "pipeline".to_string(),
            parser_version: Some("test".to_string()),
            markdown: Some("m".repeat(600)),
            content_list: None,
            content_list_v2: None,
            middle_json: None,
            model_json: None,
            images: Vec::new(),
            blocks: vec![ParsedBlock {
                block_id: "block-a".to_string(),
                block_type: "paragraph".to_string(),
                text: Some("a".repeat(600)),
                ..ParsedBlock::default()
            }],
        };

        assert!(validate_parser_output(&output, 1024).is_err());
    }

    #[test]
    fn content_list_rejects_oversized_field_before_materialization() {
        let value = json!([{
            "type": "paragraph",
            "text": "x".repeat(MAX_BLOCK_FIELD_BYTES + 1)
        }]);

        assert!(MineruContentListParser::parse(&value, 4 * MAX_BLOCK_FIELD_BYTES).is_err());
    }

    #[test]
    fn content_list_rejects_excessive_section_depth() {
        let value = json!([{
            "type": "paragraph",
            "text": "bounded",
            "section_path": vec!["section"; MAX_SECTION_DEPTH + 1]
        }]);

        assert!(MineruContentListParser::parse(&value, 1024 * 1024).is_err());
    }

    #[test]
    fn content_list_rejects_excessive_item_count_without_cloning() {
        let value = Value::Array(
            (0..=MAX_PARSED_BLOCKS)
                .map(|_| json!({"type": "paragraph"}))
                .collect(),
        );

        assert!(MineruContentListParser::parse(&value, 1024 * 1024).is_err());
    }

    #[test]
    fn content_list_rejects_aggregate_amplification_before_clone() {
        let text = "x".repeat(1024);
        let value = json!([{"type": "paragraph", "text": text}]);
        let retained_raw = encoded_json_len(&value).unwrap();

        assert!(MineruContentListParser::parse(&value, retained_raw + 1023).is_err());
    }
}
