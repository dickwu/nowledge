use axum::{extract::Path, extract::State, Json};
use serde_json::Value;

use crate::{
    app::AppState,
    auth::AdminGuard,
    error::ApiError,
    harness_service::HarnessService,
    models::{
        CreateHarnessChangeManifestRequest, CreateHarnessChangeVerdictRequest,
        CreateHarnessComponentRevisionRequest, EvalDeltaReport, HarnessChangeManifest,
        HarnessChangeVerdict, HarnessComponent, HarnessComponentDetail, HarnessComponentRevision,
        HarnessRollbackResponse, RollbackHarnessComponentRequest,
    },
};

pub(crate) async fn list_harness_components(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<Vec<HarnessComponent>>, ApiError> {
    Ok(Json(harness_service(&state).list_components()?))
}

pub(crate) async fn get_harness_component(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(component_id): Path<String>,
) -> Result<Json<HarnessComponentDetail>, ApiError> {
    Ok(Json(
        harness_service(&state).component_detail(&component_id)?,
    ))
}

pub(crate) async fn create_harness_component_revision(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(component_id): Path<String>,
    Json(request): Json<CreateHarnessComponentRevisionRequest>,
) -> Result<Json<HarnessComponentRevision>, ApiError> {
    Ok(Json(
        harness_service(&state)
            .create_component_revision(&component_id, request)
            .await?,
    ))
}

pub(crate) async fn rollback_harness_component(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(component_id): Path<String>,
    Json(request): Json<RollbackHarnessComponentRequest>,
) -> Result<Json<HarnessRollbackResponse>, ApiError> {
    Ok(Json(
        harness_service(&state)
            .rollback_component(&component_id, request)
            .await?,
    ))
}

pub(crate) async fn create_harness_change(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(request): Json<CreateHarnessChangeManifestRequest>,
) -> Result<Json<HarnessChangeManifest>, ApiError> {
    Ok(Json(harness_service(&state).create_change(request).await?))
}

pub(crate) async fn list_harness_changes(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<Vec<HarnessChangeManifest>>, ApiError> {
    Ok(Json(harness_service(&state).list_changes()?))
}

pub(crate) async fn get_harness_change(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
) -> Result<Json<HarnessChangeManifest>, ApiError> {
    Ok(Json(harness_service(&state).change(&change_id)?))
}

pub(crate) async fn create_harness_verdict(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
    Json(request): Json<CreateHarnessChangeVerdictRequest>,
) -> Result<Json<HarnessChangeVerdict>, ApiError> {
    Ok(Json(
        harness_service(&state)
            .create_verdict(&change_id, request)
            .await?,
    ))
}

pub(crate) async fn compare_harness_change(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
    Json(request): Json<Value>,
) -> Result<Json<EvalDeltaReport>, ApiError> {
    let baseline = request
        .get("baseline_eval_run_id")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let candidate = request
        .get("candidate_eval_run_id")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    Ok(Json(
        harness_service(&state).compare_change(&change_id, baseline, candidate)?,
    ))
}

pub(crate) async fn get_harness_change_delta(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(change_id): Path<String>,
) -> Result<Json<EvalDeltaReport>, ApiError> {
    Ok(Json(
        harness_service(&state).compare_change(&change_id, None, None)?,
    ))
}

fn harness_service(state: &AppState) -> HarnessService {
    HarnessService::new(state.store.clone(), state.tenant_id().to_string())
}
