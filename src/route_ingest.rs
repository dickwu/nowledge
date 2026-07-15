use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    extract::{
        multipart::{Field, MultipartError},
        Extension, Multipart, Path, Query, Request, State,
    },
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::io::AsyncWriteExt;

use crate::{
    app::AppState,
    auth::UserGuard,
    config::Config,
    error::ApiError,
    http_boundary::{self, RequestDeadline},
    ingest_service::{IngestService, IngestTerminalTransition},
    models::{FragmentPolicy, IngestTask, IngestTaskRequest, IngestTaskResult},
    parser::StagedUpload,
};

#[derive(Clone, Default)]
pub(crate) struct SyncIngestTracker {
    task_id: Arc<Mutex<Option<String>>>,
}

impl SyncIngestTracker {
    fn set_task_id(&self, task_id: &str) {
        *self
            .task_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(task_id.to_string());
    }

    fn task_id(&self) -> Option<String> {
        self.task_id
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }
}

#[derive(Clone)]
pub(crate) struct SyncIngestTimeoutState {
    timeout: Duration,
    service: IngestService,
}

impl SyncIngestTimeoutState {
    pub(crate) fn new(state: &AppState) -> Self {
        Self {
            timeout: Duration::from_millis(state.config.sync_ingest_timeout_ms),
            service: ingest_service(state),
        }
    }
}

pub(crate) async fn enforce_sync_ingest_timeout(
    State(state): State<SyncIngestTimeoutState>,
    mut request: Request,
    next: Next,
) -> Response {
    if !http_boundary::store_owns_timeout(request.uri().path()) {
        return next.run(request).await;
    }

    let deadline = tokio::time::Instant::now() + state.timeout;
    let tracker = SyncIngestTracker::default();
    request
        .extensions_mut()
        .insert(RequestDeadline::new(deadline));
    request.extensions_mut().insert(tracker.clone());

    match tokio::time::timeout_at(deadline, next.run(request)).await {
        Ok(response) => {
            if response.status().is_client_error() || response.status().is_server_error() {
                if let Some(task_id) = tracker.task_id() {
                    state.service.supervise_terminal_transition(
                        task_id,
                        IngestTerminalTransition::Failed,
                        "failed to finalize sync ingest task",
                    );
                }
            }
            response
        }
        Err(_) => {
            if let Some(task_id) = tracker.task_id() {
                if let Ok(result) = state.service.task_result(&task_id, None, true) {
                    return Json(result).into_response();
                }
                state.service.supervise_terminal_transition(
                    task_id,
                    IngestTerminalTransition::Interrupted,
                    "failed to interrupt timed-out ingest task",
                );
            }
            ApiError::timeout().into_response()
        }
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct OwnerQuery {
    owner_user_id: Option<String>,
}

pub(crate) async fn create_ingest_task(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(deadline): Extension<RequestDeadline>,
    Json(mut request): Json<IngestTaskRequest>,
) -> Result<Json<IngestTask>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_owner_for_write(&user, request.owner_user_id.as_deref())?;
    Ok(Json(
        ingest_service(&state)
            .submit(request, None, deadline)
            .await?,
    ))
}

pub(crate) async fn get_ingest_task(
    user: UserGuard,
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<IngestTask>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let include_all_private = user.principal.is_admin() && query.owner_user_id.is_none();
    Ok(Json(ingest_service(&state).task(
        &task_id,
        query.owner_user_id.as_deref(),
        include_all_private,
    )?))
}

pub(crate) async fn get_ingest_task_result(
    user: UserGuard,
    State(state): State<AppState>,
    Path(task_id): Path<String>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<IngestTaskResult>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let include_all_private = user.principal.is_admin() && query.owner_user_id.is_none();
    Ok(Json(ingest_service(&state).task_result(
        &task_id,
        query.owner_user_id.as_deref(),
        include_all_private,
    )?))
}

pub(crate) async fn ingest_file_sync(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(tracker): Extension<SyncIngestTracker>,
    Json(mut request): Json<IngestTaskRequest>,
) -> Result<Json<IngestTaskResult>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_owner_for_write(&user, request.owner_user_id.as_deref())?;
    Ok(Json(
        ingest_service(&state)
            .ingest_sync(request, None, |task_id| tracker.set_task_id(task_id))
            .await?,
    ))
}

pub(crate) async fn create_ingest_upload(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(deadline): Extension<RequestDeadline>,
    multipart: Multipart,
) -> Result<Json<IngestTask>, ApiError> {
    let service = ingest_service(&state);
    service.ensure_async_available()?;
    let mut prepared = ingest_request_from_multipart(multipart, &state.config).await?;
    user.apply_owner_default(&mut prepared.request.owner_user_id)?;
    require_owner_for_write(&user, prepared.request.owner_user_id.as_deref())?;
    Ok(Json(
        service
            .submit(prepared.request, prepared.staged_upload, deadline)
            .await?,
    ))
}

pub(crate) async fn ingest_upload_sync(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(tracker): Extension<SyncIngestTracker>,
    multipart: Multipart,
) -> Result<Json<IngestTaskResult>, ApiError> {
    let mut prepared = ingest_request_from_multipart(multipart, &state.config).await?;
    user.apply_owner_default(&mut prepared.request.owner_user_id)?;
    require_owner_for_write(&user, prepared.request.owner_user_id.as_deref())?;
    Ok(Json(
        ingest_service(&state)
            .ingest_sync(prepared.request, prepared.staged_upload, |task_id| {
                tracker.set_task_id(task_id)
            })
            .await?,
    ))
}

fn ingest_service(state: &AppState) -> IngestService {
    IngestService::new(
        state.config.clone(),
        state.store.clone(),
        state.ingest_manager.clone(),
        state.runtime.clone(),
    )
}

fn require_owner_for_write(user: &UserGuard, owner_user_id: Option<&str>) -> Result<(), ApiError> {
    if user.principal.is_admin() || owner_user_id.is_some() {
        Ok(())
    } else {
        Err(ApiError::forbidden(
            "owner_user_id is required for non-admin writes",
        ))
    }
}

struct PreparedIngestRequest {
    request: IngestTaskRequest,
    staged_upload: Option<StagedUpload>,
}

struct TemporaryUploadPath {
    path: Option<PathBuf>,
}

impl TemporaryUploadPath {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn path(&self) -> &std::path::Path {
        self.path
            .as_deref()
            .expect("temporary upload path must exist until staged")
    }

    fn into_staged(mut self, byte_len: u64, sha256: String) -> StagedUpload {
        let path = self
            .path
            .take()
            .expect("temporary upload path must exist until staged");
        StagedUpload::new(path, byte_len, sha256)
    }
}

impl Drop for TemporaryUploadPath {
    fn drop(&mut self) {
        let Some(path) = self.path.take() else {
            return;
        };
        if let Err(err) = std::fs::remove_file(path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    error_kind = "temporary_upload_cleanup",
                    "failed to remove incomplete staged upload"
                );
            }
        }
    }
}

async fn ingest_request_from_multipart(
    mut multipart: Multipart,
    config: &Config,
) -> Result<PreparedIngestRequest, ApiError> {
    let mut request = IngestTaskRequest::default();
    let mut staged_upload = None;
    let mut file_part_content_type = None;
    let mut field_count = 0_usize;
    let mut metadata_bytes = 0_usize;
    while let Some(field) = multipart.next_field().await.map_err(map_multipart_error)? {
        field_count = field_count.saturating_add(1);
        if field_count > config.max_multipart_fields {
            return Err(ApiError::payload_too_large());
        }
        let name = field.name().map(ToString::to_string).unwrap_or_default();
        if matches!(name.as_str(), "file" | "document" | "upload") {
            if staged_upload.is_some() {
                return Err(ApiError::validation(
                    "file",
                    "only one upload file is allowed",
                ));
            }
            if request.file_name.is_none() {
                request.file_name = field.file_name().map(sanitize_upload_filename);
            }
            let part_content_type = field
                .content_type()
                .ok_or_else(|| ApiError::validation("content_type", "is required for uploads"))
                .and_then(validate_multipart_content_type)?;
            validate_upload_content_type_policy(&part_content_type, config)?;
            if request
                .content_type
                .as_deref()
                .is_some_and(|declared| !declared.eq_ignore_ascii_case(&part_content_type))
            {
                return Err(ApiError::validation(
                    "content_type",
                    "metadata must match the upload part Content-Type",
                ));
            }
            request.content_type = Some(part_content_type.clone());
            file_part_content_type = Some(part_content_type);
            staged_upload = Some(stage_multipart_upload(field, config.max_upload_bytes).await?);
            continue;
        }

        let text =
            read_multipart_metadata_field(field, &name, &mut metadata_bytes, config.max_json_bytes)
                .await?;
        apply_ingest_multipart_field(&mut request, &name, text)?;
    }

    if staged_upload.is_some()
        && (request.content.is_some()
            || request.bytes.is_some()
            || request.content_list.is_some()
            || request.content_list_v2.is_some()
            || request.middle_json.is_some()
            || request.model_json.is_some())
    {
        return Err(ApiError::validation(
            "multipart",
            "file uploads cannot be combined with alternate content or parser output fields",
        ));
    }

    if let (Some(part_content_type), Some(effective_content_type)) = (
        file_part_content_type.as_deref(),
        request.content_type.as_deref(),
    ) {
        if !part_content_type.eq_ignore_ascii_case(effective_content_type) {
            return Err(ApiError::validation(
                "content_type",
                "metadata must match the upload part Content-Type",
            ));
        }
    }

    if let Some(content_type) = request.content_type.as_deref() {
        validate_upload_content_type_policy(content_type, config)?;
    }

    if let (Some(checksum), Some(upload)) = (request.checksum.as_deref(), staged_upload.as_ref()) {
        verify_upload_checksum(checksum, &upload.sha256)?;
    }

    Ok(PreparedIngestRequest {
        request,
        staged_upload,
    })
}

async fn stage_multipart_upload(
    mut field: Field<'_>,
    max_upload_bytes: usize,
) -> Result<StagedUpload, ApiError> {
    let path =
        std::env::temp_dir().join(format!("nowledge-upload-{}", uuid::Uuid::now_v7().simple()));
    let temporary_path = TemporaryUploadPath::new(path);
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    options.mode(0o600);
    let mut file = options
        .open(temporary_path.path())
        .await
        .map_err(|err| ApiError::Internal(format!("failed to create temporary upload: {err}")))?;
    let mut byte_len = 0_u64;
    let max_upload_bytes = u64::try_from(max_upload_bytes).unwrap_or(u64::MAX);
    let mut hasher = Sha256::new();

    while let Some(chunk) = field.chunk().await.map_err(map_multipart_error)? {
        let next_len = byte_len
            .checked_add(u64::try_from(chunk.len()).unwrap_or(u64::MAX))
            .ok_or_else(ApiError::payload_too_large)?;
        if next_len > max_upload_bytes {
            return Err(ApiError::payload_too_large());
        }
        file.write_all(&chunk).await.map_err(|err| {
            ApiError::Internal(format!("failed to write temporary upload: {err}"))
        })?;
        hasher.update(&chunk);
        byte_len = next_len;
    }
    file.flush()
        .await
        .map_err(|err| ApiError::Internal(format!("failed to flush temporary upload: {err}")))?;
    drop(file);

    if byte_len == 0 {
        return Err(ApiError::validation("file", "must not be empty"));
    }
    Ok(temporary_path.into_staged(byte_len, hex::encode(hasher.finalize())))
}

async fn read_multipart_metadata_field(
    mut field: Field<'_>,
    name: &str,
    metadata_bytes: &mut usize,
    max_json_bytes: usize,
) -> Result<String, ApiError> {
    let mut bytes = Vec::new();
    while let Some(chunk) = field.chunk().await.map_err(map_multipart_error)? {
        let next_total = metadata_bytes
            .checked_add(chunk.len())
            .ok_or_else(ApiError::payload_too_large)?;
        if next_total > max_json_bytes {
            return Err(ApiError::payload_too_large());
        }
        *metadata_bytes = next_total;
        bytes.extend_from_slice(&chunk);
    }
    String::from_utf8(bytes).map_err(|_| {
        ApiError::validation(
            if name.is_empty() { "multipart" } else { name },
            "must be valid UTF-8",
        )
    })
}

fn map_multipart_error(err: MultipartError) -> ApiError {
    if err.status() == StatusCode::PAYLOAD_TOO_LARGE {
        ApiError::payload_too_large()
    } else {
        ApiError::bad_request("invalid multipart body")
    }
}

fn sanitize_upload_filename(value: &str) -> String {
    let leaf = value.rsplit(['/', '\\']).next().unwrap_or_default();
    let mut sanitized = leaf
        .chars()
        .filter(|character| !character.is_control())
        .collect::<String>()
        .trim()
        .to_string();
    while sanitized.len() > 255 {
        sanitized.pop();
    }
    if sanitized.is_empty() || matches!(sanitized.as_str(), "." | "..") {
        "upload.bin".to_string()
    } else {
        sanitized
    }
}

fn verify_upload_checksum(expected: &str, actual: &str) -> Result<(), ApiError> {
    let expected = expected.trim();
    if expected.len() != 64 || !expected.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(ApiError::validation(
            "checksum",
            "must be exactly 64 hexadecimal SHA-256 characters",
        ));
    }
    if !expected.eq_ignore_ascii_case(actual) {
        return Err(ApiError::validation(
            "checksum",
            "does not match the uploaded file",
        ));
    }
    Ok(())
}

fn apply_ingest_multipart_field(
    request: &mut IngestTaskRequest,
    name: &str,
    value: String,
) -> Result<(), ApiError> {
    match name {
        "owner_user_id" => request.owner_user_id = non_empty(value),
        "source_id" => request.source_id = non_empty(value),
        "revision_id" => request.revision_id = non_empty(value),
        "title" => request.title = non_empty(value),
        "source_uri" => request.source_uri = non_empty(value),
        "source_document_uri" => request.source_document_uri = non_empty(value),
        "content" => request.content = Some(value),
        "content_type" => {
            request.content_type = non_empty(validate_multipart_content_type(&value)?)
        }
        "file_name" => request.file_name = non_empty(sanitize_upload_filename(&value)),
        "checksum" => request.checksum = non_empty(value),
        "parser_provider" => request.parser_provider = non_empty(value),
        "parser_backend" => request.parser_backend = non_empty(value),
        "content_list" => request.content_list = Some(parse_json_field(name, &value)?),
        "content_list_v2" => request.content_list_v2 = Some(parse_json_field(name, &value)?),
        "middle_json" => request.middle_json = Some(parse_json_field(name, &value)?),
        "model_json" => request.model_json = Some(parse_json_field(name, &value)?),
        "fragment_policy" => {
            request.fragment_policy = Some(parse_json_field::<FragmentPolicy>(name, &value)?)
        }
        "fragment_policy.chunk_size_chars" => {
            request
                .fragment_policy
                .get_or_insert_with(FragmentPolicy::default)
                .chunk_size_chars = Some(parse_usize_field(name, &value)?);
        }
        "fragment_policy.overlap_chars" => {
            request
                .fragment_policy
                .get_or_insert_with(FragmentPolicy::default)
                .overlap_chars = Some(parse_usize_field(name, &value)?);
        }
        "fragment_policy.min_chunk_chars" => {
            request
                .fragment_policy
                .get_or_insert_with(FragmentPolicy::default)
                .min_chunk_chars = Some(parse_usize_field(name, &value)?);
        }
        "idempotency_key" => request.idempotency_key = non_empty(value),
        _ => {}
    }
    Ok(())
}

fn non_empty(value: String) -> Option<String> {
    let value = value.trim().to_string();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

fn validate_multipart_content_type(value: &str) -> Result<String, ApiError> {
    reqwest::multipart::Part::bytes(Vec::new())
        .mime_str(value)
        .map_err(|_| ApiError::validation("content_type", "must be a valid MIME type"))?;
    Ok(value.trim().to_ascii_lowercase())
}

fn validate_upload_content_type_policy(value: &str, config: &Config) -> Result<(), ApiError> {
    if config
        .upload_allowed_mime_types
        .iter()
        .any(|allowed| allowed.eq_ignore_ascii_case(value))
    {
        Ok(())
    } else {
        Err(ApiError::validation(
            "content_type",
            "is not allowed by RAG_UPLOAD_ALLOWED_MIME_TYPES",
        ))
    }
}

fn parse_json_field<T: serde::de::DeserializeOwned>(
    name: &str,
    value: &str,
) -> Result<T, ApiError> {
    serde_json::from_str(value)
        .map_err(|err| ApiError::bad_request(format!("{name} must be valid JSON: {err}")))
}

fn parse_usize_field(name: &str, value: &str) -> Result<usize, ApiError> {
    value
        .parse::<usize>()
        .map_err(|err| ApiError::bad_request(format!("{name} must be a positive integer: {err}")))
}
