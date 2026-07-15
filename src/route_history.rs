use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;

use crate::{
    app::AppState,
    auth::{AdminGuard, UserGuard},
    error::ApiError,
    history_service::HistoryService,
    models::{
        AppendHistoryEventRequest, BulkHistoryEventsRequest, BulkHistoryEventsResponse,
        EnsureUserEventIndexRequest, HistoryEvent, HistoryEventResponse, HistorySearchRequest,
        HistorySearchResponse, ListUserEventIndexesResponse, OperationListRequest,
        OperationListResponse, ReconcileOperationsRequest, ReconcileOperationsResponse,
        ReconcileUserEventIndexesRequest, ReconcileUserEventIndexesResponse, TimelineQueryRequest,
        TimelineResponse, UserEventIndexResponse,
    },
    request_validation::{validate_history_bulk, validate_search_limit, validate_tags},
    util::require_string,
};

#[derive(Debug, Deserialize)]
pub(crate) struct OwnerQuery {
    owner_user_id: Option<String>,
}

pub(crate) async fn ensure_user_event_index(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(request): Json<EnsureUserEventIndexRequest>,
) -> Result<Json<UserEventIndexResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    Ok(Json(
        history_service(&state)
            .ensure_user_index(&owner_user_id, request)
            .await?,
    ))
}

pub(crate) async fn get_user_event_index(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
) -> Result<Json<UserEventIndexResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    Ok(Json(
        history_service(&state)
            .ensure_user_index(&owner_user_id, EnsureUserEventIndexRequest::default())
            .await?,
    ))
}

pub(crate) async fn list_user_event_indexes(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<ListUserEventIndexesResponse>, ApiError> {
    Ok(Json(history_service(&state).list_user_indexes()?))
}

pub(crate) async fn reconcile_user_event_indexes(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(request): Json<ReconcileUserEventIndexesRequest>,
) -> Result<Json<ReconcileUserEventIndexesResponse>, ApiError> {
    Ok(Json(
        history_service(&state)
            .reconcile_user_indexes(request)
            .await?,
    ))
}

pub(crate) async fn search_operations(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(request): Json<OperationListRequest>,
) -> Result<Json<OperationListResponse>, ApiError> {
    Ok(Json(
        history_service(&state).list_operations(request).await?,
    ))
}

pub(crate) async fn reconcile_operations(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(request): Json<ReconcileOperationsRequest>,
) -> Result<Json<ReconcileOperationsResponse>, ApiError> {
    Ok(Json(
        history_service(&state)
            .reconcile_operations(request)
            .await?,
    ))
}

pub(crate) async fn append_user_event(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(request): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_tags("tags", &request.tags, &state.config)?;
    Ok(Json(
        history_service(&state)
            .append_event(Some(&owner_user_id), request)
            .await?,
    ))
}

pub(crate) async fn append_user_events_bulk(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(request): Json<BulkHistoryEventsRequest>,
) -> Result<Json<BulkHistoryEventsResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_history_bulk(&request, &state.config)?;
    Ok(Json(
        history_service(&state)
            .append_bulk_events(Some(&owner_user_id), request)
            .await?,
    ))
}

pub(crate) async fn search_user_events(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(mut request): Json<HistorySearchRequest>,
) -> Result<Json<HistorySearchResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_search_limit("limit", request.limit, &state.config)?;
    request.owner_user_id = Some(owner_user_id.clone());
    Ok(Json(
        history_service(&state)
            .search_events(Some(&owner_user_id), request)
            .await?,
    ))
}

pub(crate) async fn get_user_event(
    user: UserGuard,
    State(state): State<AppState>,
    Path((owner_user_id, event_id)): Path<(String, String)>,
) -> Result<Json<HistoryEvent>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    Ok(Json(
        history_service(&state)
            .get_event(&owner_user_id, &event_id)
            .await?,
    ))
}

pub(crate) async fn user_timeline(
    user: UserGuard,
    State(state): State<AppState>,
    Path(owner_user_id): Path<String>,
    Json(request): Json<TimelineQueryRequest>,
) -> Result<Json<TimelineResponse>, ApiError> {
    user.require_owner_access(&owner_user_id)?;
    validate_search_limit("limit", request.limit, &state.config)?;
    Ok(Json(
        history_service(&state)
            .timeline(Some(&owner_user_id), request)
            .await?,
    ))
}

pub(crate) async fn append_event_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<AppendHistoryEventRequest>,
) -> Result<Json<HistoryEventResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_owner_for_write(&user, request.owner_user_id.as_deref())?;
    validate_tags("tags", &request.tags, &state.config)?;
    Ok(Json(
        history_service(&state).append_event(None, request).await?,
    ))
}

pub(crate) async fn append_events_bulk_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<BulkHistoryEventsRequest>,
) -> Result<Json<BulkHistoryEventsResponse>, ApiError> {
    if let Some(first) = request.events.first_mut() {
        user.apply_owner_default(&mut first.owner_user_id)?;
    }
    if let Some(owner) = request
        .events
        .first()
        .and_then(|event| event.owner_user_id.clone())
    {
        user.require_owner_access(&owner)?;
    }
    validate_history_bulk(&request, &state.config)?;
    Ok(Json(
        history_service(&state)
            .append_bulk_events(None, request)
            .await?,
    ))
}

pub(crate) async fn search_events_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<HistorySearchRequest>,
) -> Result<Json<HistorySearchResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    validate_search_limit("limit", request.limit, &state.config)?;
    Ok(Json(
        history_service(&state).search_events(None, request).await?,
    ))
}

pub(crate) async fn get_event_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Path(event_id): Path<String>,
    Query(query): Query<OwnerQuery>,
) -> Result<Json<HistoryEvent>, ApiError> {
    let owner = require_string(query.owner_user_id, "owner_user_id")?;
    user.require_owner_access(&owner)?;
    Ok(Json(
        history_service(&state).get_event(&owner, &event_id).await?,
    ))
}

pub(crate) async fn timeline_alias(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<TimelineQueryRequest>,
) -> Result<Json<TimelineResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    validate_search_limit("limit", request.limit, &state.config)?;
    Ok(Json(history_service(&state).timeline(None, request).await?))
}

fn history_service(state: &AppState) -> HistoryService {
    HistoryService::new(state.tenant_id().to_string(), state.store.clone())
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
