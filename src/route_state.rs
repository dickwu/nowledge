use axum::{
    extract::{Path, Query, State},
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    app::AppState,
    auth::UserGuard,
    error::ApiError,
    models::{
        InsightPatchRequest, InsightResponse, InsightSearchRequest, InsightSearchResponse,
        InsightUpsertRequest, LinkResponse, LinkSearchRequest, LinkSearchResponse,
        LinkUpsertRequest, PatchStateFactRequest, StateItemResponse, StateSearchRequest,
        StateSearchResponse, UpsertStateFactRequest,
    },
    request_validation::{validate_search_limit, validate_tags},
    state_service::StateService,
};

#[derive(Debug, Deserialize)]
pub(crate) struct OwnerQuery {
    owner_user_id: Option<String>,
}

pub(crate) async fn upsert_state_fact(
    user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Json(mut request): Json<UpsertStateFactRequest>,
) -> Result<Json<StateItemResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_owner_for_write(&user, request.owner_user_id.as_deref())?;
    Ok(Json(
        state_service(&state)
            .upsert_fact(&fact_key, request)
            .await?,
    ))
}

pub(crate) async fn patch_state_fact(
    user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Json(mut request): Json<PatchStateFactRequest>,
) -> Result<Json<StateItemResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_owner_for_write(&user, request.owner_user_id.as_deref())?;
    Ok(Json(
        state_service(&state).patch_fact(&fact_key, request).await?,
    ))
}

pub(crate) async fn get_state_fact(
    user: UserGuard,
    State(state): State<AppState>,
    Path(fact_key): Path<String>,
    Query(mut query): Query<OwnerQuery>,
) -> Result<Json<StateItemResponse>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    require_explicit_owner_for_unbound_private_read(&user, query.owner_user_id.as_deref())?;
    Ok(Json(
        state_service(&state).get_fact(&fact_key, query.owner_user_id.as_deref())?,
    ))
}

pub(crate) async fn search_state(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<StateSearchRequest>,
) -> Result<Json<StateSearchResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_explicit_owner_for_unbound_private_read(&user, request.owner_user_id.as_deref())?;
    validate_search_limit("limit", request.limit, &state.config)?;
    Ok(Json(state_service(&state).search_state(request)?))
}

pub(crate) async fn upsert_insight(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<InsightUpsertRequest>,
) -> Result<Json<InsightResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    Ok(Json(state_service(&state).upsert_insight(request).await?))
}

pub(crate) async fn patch_insight(
    user: UserGuard,
    State(state): State<AppState>,
    Path(insight_id): Path<String>,
    Json(request): Json<InsightPatchRequest>,
) -> Result<Json<InsightResponse>, ApiError> {
    let service = state_service(&state);
    let owner = service.insight_owner(&insight_id)?;
    user.require_owner_access(&owner)?;
    Ok(Json(service.patch_insight(&insight_id, request).await?))
}

pub(crate) async fn search_insights(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<InsightSearchRequest>,
) -> Result<Json<InsightSearchResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_explicit_owner_for_unbound_private_read(&user, request.owner_user_id.as_deref())?;
    validate_search_limit("limit", request.limit, &state.config)?;
    Ok(Json(state_service(&state).search_insights(request)?))
}

pub(crate) async fn upsert_link(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<LinkUpsertRequest>,
) -> Result<Json<LinkResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    require_owner_for_write(&user, request.owner_user_id.as_deref())?;
    validate_tags("tags", &request.tags, &state.config)?;
    Ok(Json(state_service(&state).upsert_link(request).await?))
}

pub(crate) async fn search_links(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<LinkSearchRequest>,
) -> Result<Json<LinkSearchResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    validate_search_limit("limit", request.limit, &state.config)?;
    Ok(Json(
        state_service(&state).search_links(request, user.principal.is_admin())?,
    ))
}

pub(crate) async fn insight_events(
    _user: UserGuard,
    Path(insight_id): Path<String>,
) -> Json<Value> {
    Json(json!({ "insight_id": insight_id, "events": [] }))
}

fn state_service(state: &AppState) -> StateService {
    StateService::new(state.tenant_id().to_string(), state.store.clone())
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

fn require_explicit_owner_for_unbound_private_read(
    user: &UserGuard,
    owner_user_id: Option<&str>,
) -> Result<(), ApiError> {
    let is_tenant_service = !user.principal.is_admin() && user.principal.owner_user_id().is_none();
    if is_tenant_service && owner_user_id.is_none() {
        Err(ApiError::forbidden(
            "owner_user_id is required for tenant-service private access",
        ))
    } else {
        Ok(())
    }
}
