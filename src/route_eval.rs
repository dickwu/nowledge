use axum::{extract::Path, extract::State, Json};
use serde_json::Value;

use crate::{
    app::AppState,
    auth::AdminGuard,
    error::ApiError,
    eval_service::EvalService,
    models::{
        CreateRagEvalCaseRequest, CreateRagEvalRunRequest, RagEvalCase, RagEvalCaseResult,
        RagEvalOverview, RagEvalRun,
    },
    request_validation::validate_tags,
};

pub(crate) async fn create_eval_case(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(request): Json<CreateRagEvalCaseRequest>,
) -> Result<Json<RagEvalCase>, ApiError> {
    validate_tags("tags", &request.tags, &state.config)?;
    Ok(Json(EvalService::new(&state).create_case(request).await?))
}

pub(crate) async fn list_eval_cases(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Json<Vec<RagEvalCase>>, ApiError> {
    Ok(Json(EvalService::new(&state).list_cases()?))
}

pub(crate) async fn create_eval_run(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Json(request): Json<CreateRagEvalRunRequest>,
) -> Result<Json<RagEvalRun>, ApiError> {
    Ok(Json(EvalService::new(&state).create_run(request).await?))
}

pub(crate) async fn get_eval_run(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<RagEvalRun>, ApiError> {
    Ok(Json(EvalService::new(&state).run(&run_id)?))
}

pub(crate) async fn get_eval_run_report(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(EvalService::new(&state).run_report(&run_id)?))
}

pub(crate) async fn get_eval_overview(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<RagEvalOverview>, ApiError> {
    Ok(Json(EvalService::new(&state).overview(&run_id)?))
}

pub(crate) async fn get_eval_case_analysis(
    _admin: AdminGuard,
    State(state): State<AppState>,
    Path((run_id, case_id)): Path<(String, String)>,
) -> Result<Json<RagEvalCaseResult>, ApiError> {
    Ok(Json(
        EvalService::new(&state).case_analysis(&run_id, &case_id)?,
    ))
}
