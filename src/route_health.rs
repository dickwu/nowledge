use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    app::AppState,
    auth::{AdminGuard, UserGuard},
    error::ApiError,
    health_service::{DiagnosticsService, HealthPayload, HealthService},
    models::{LlmStatusResponse, LlmTestRequest},
};

#[derive(Debug, Deserialize)]
pub(crate) struct UsageQuery {
    owner_user_id: Option<String>,
}

pub(crate) async fn livez() -> Json<Value> {
    Json(HealthService::liveness())
}

pub(crate) async fn healthz(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> impl IntoResponse {
    health_response(HealthService::health(&state).await)
}

pub(crate) async fn readyz(State(state): State<AppState>) -> impl IntoResponse {
    health_response(HealthService::readiness(&state).await)
}

pub(crate) async fn usage(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<UsageQuery>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let include_global = user.principal.is_admin() && query.owner_user_id.is_none();
    if !include_global && query.owner_user_id.is_none() {
        return Err(ApiError::forbidden(
            "owner_user_id is required for non-admin usage",
        ));
    }
    Ok(Json(HealthService::usage(
        &state,
        query.owner_user_id.as_deref(),
        include_global,
        user.principal.is_admin(),
    )?))
}

pub(crate) async fn bootstrap(
    _admin: AdminGuard,
    Json(_req): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(HealthService::bootstrap()?))
}

pub(crate) async fn llm_status(
    _user: UserGuard,
    State(state): State<AppState>,
) -> Json<LlmStatusResponse> {
    Json(HealthService::llm_status(&state).await)
}

pub(crate) async fn llm_test(
    admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<LlmTestRequest>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        DiagnosticsService::llm_test(&state, &admin.principal, req).await?,
    ))
}

pub(crate) async fn get_trace(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(trace_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(DiagnosticsService::trace(&state, &trace_id).await?))
}

pub(crate) async fn debug_meili_search(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<Value>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(DiagnosticsService::meili_search(&state, req).await?))
}

fn health_response(payload: HealthPayload) -> impl IntoResponse {
    (
        if payload.ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(payload.body),
    )
}
