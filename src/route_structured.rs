use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    app::AppState,
    auth::{CompanyWriterGuard, UserGuard},
    error::ApiError,
    models::{
        ApplySnapshotRequest, ApplySnapshotResponse, BulkStructuredRowsRequest,
        BulkStructuredRowsResponse, CreateStructuredSnapshotRequest,
        CurrentStructuredStateResponse, DatasetSchemaResponse, DatasetSchemaUpsertRequest,
        StructuredSnapshot, StructuredSnapshotResponse,
    },
    request_validation::validate_max_items,
    store::{decode_rows_cursor, RowsPageRequest},
    structured_service::StructuredService,
};

#[derive(Debug, Deserialize)]
pub(crate) struct OwnerQuery {
    owner_user_id: Option<String>,
}

pub(crate) async fn upsert_dataset(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Path(dataset_key): Path<String>,
    Json(request): Json<DatasetSchemaUpsertRequest>,
) -> Result<Json<DatasetSchemaResponse>, ApiError> {
    Ok(Json(
        structured_service(&state)
            .upsert_dataset(&user.principal, &dataset_key, request)
            .await?,
    ))
}

pub(crate) async fn apply_snapshot(
    user: UserGuard,
    State(state): State<AppState>,
    Path(dataset_key): Path<String>,
    Json(request): Json<ApplySnapshotRequest>,
) -> Result<Json<ApplySnapshotResponse>, ApiError> {
    let service = structured_service(&state);
    // The owner check must not depend on a downstream "required" rule: reject a
    // missing snapshot_id here (same 400 contract the store uses) so the
    // authorization step can never be skipped.
    let snapshot_id = request
        .snapshot_id
        .as_deref()
        .ok_or_else(|| ApiError::bad_request("snapshot_id is required"))?;
    let owner = service.snapshot_owner(snapshot_id).await?;
    user.require_owner_access(&owner)?;
    Ok(Json(service.apply_snapshot(&dataset_key, request).await?))
}

pub(crate) async fn current_structured(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<CurrentStructuredStateResponse>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    Ok(Json(structured_service(&state).current_state(
        query.owner_user_id.as_deref(),
        user.principal.is_admin(),
    )?))
}

pub(crate) async fn create_snapshot(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<CreateStructuredSnapshotRequest>,
) -> Result<Json<StructuredSnapshotResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    Ok(Json(
        structured_service(&state).create_snapshot(request).await?,
    ))
}

pub(crate) async fn get_snapshot(
    user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
) -> Result<Json<StructuredSnapshot>, ApiError> {
    let service = structured_service(&state);
    let owner = service.snapshot_owner(&snapshot_id).await?;
    user.require_owner_access(&owner)?;
    Ok(Json(service.get_snapshot(&snapshot_id).await?))
}

pub(crate) async fn bulk_rows(
    user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
    Json(request): Json<BulkStructuredRowsRequest>,
) -> Result<Json<BulkStructuredRowsResponse>, ApiError> {
    let service = structured_service(&state);
    let owner = service.snapshot_owner(&snapshot_id).await?;
    user.require_owner_access(&owner)?;
    validate_max_items("rows", request.rows.len(), state.config.max_bulk_rows)?;
    Ok(Json(service.bulk_rows(&snapshot_id, request).await?))
}

#[derive(Debug, Deserialize)]
pub(crate) struct ListRowsQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

pub(crate) async fn list_rows(
    user: UserGuard,
    State(state): State<AppState>,
    Path(snapshot_id): Path<String>,
    Query(query): Query<ListRowsQuery>,
) -> Result<Json<Value>, ApiError> {
    let page = rows_page_request(query, state.config.max_bulk_rows)?;
    let service = structured_service(&state);
    let owner = service.snapshot_owner(&snapshot_id).await?;
    user.require_owner_access(&owner)?;
    Ok(Json(service.list_rows(&snapshot_id, page).await?))
}

/// Default page size when a caller supplies only `cursor`.
const DEFAULT_ROWS_PAGE_LIMIT: usize = 500;

fn rows_page_request(
    query: ListRowsQuery,
    max_page_limit: usize,
) -> Result<Option<RowsPageRequest>, ApiError> {
    if query.limit.is_none() && query.cursor.is_none() {
        // Backward-compatible unpaginated response.
        return Ok(None);
    }
    let limit = query
        .limit
        .unwrap_or_else(|| DEFAULT_ROWS_PAGE_LIMIT.min(max_page_limit));
    if limit == 0 {
        return Err(ApiError::validation("limit", "must be at least 1"));
    }
    if limit > max_page_limit {
        return Err(ApiError::validation(
            "limit",
            format!("must be at most {max_page_limit}"),
        ));
    }
    let after_row_id = query
        .cursor
        .as_deref()
        .map(decode_rows_cursor)
        .transpose()?;
    Ok(Some(RowsPageRequest {
        limit,
        after_row_id,
    }))
}

fn structured_service(state: &AppState) -> StructuredService {
    StructuredService::new(
        state.config.clone(),
        state.store.clone(),
        state.audit_recorder.clone(),
    )
}
