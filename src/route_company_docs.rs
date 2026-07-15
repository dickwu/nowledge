use axum::{extract::Path, extract::State, Json};
use serde_json::Value;

use crate::{
    app::AppState,
    auth::{AdminGuard, CompanyWriterGuard, UserGuard},
    company_docs_service::CompanyDocsService,
    error::ApiError,
    models::{
        ActivateRevisionRequest, ActivateRevisionResponse, CompanyDocPreflightRequest,
        CompanyDocPreflightResponse, CreateRevisionRequest, CreateRevisionResponse,
    },
    request_validation::validate_tags,
};

pub(crate) async fn preflight_doc(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Json(request): Json<CompanyDocPreflightRequest>,
) -> Result<Json<CompanyDocPreflightResponse>, ApiError> {
    validate_tags("tags", &request.tags, &state.config)?;
    Ok(Json(
        company_docs_service(&state)
            .preflight(&user.principal, request)
            .await?,
    ))
}

pub(crate) async fn create_revision(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
    Json(request): Json<CreateRevisionRequest>,
) -> Result<Json<CreateRevisionResponse>, ApiError> {
    Ok(Json(
        company_docs_service(&state)
            .create_revision(&user.principal, &source_id, request)
            .await?,
    ))
}

pub(crate) async fn activate_revision(
    user: CompanyWriterGuard,
    State(state): State<AppState>,
    Path((source_id, revision_id)): Path<(String, String)>,
    Json(request): Json<ActivateRevisionRequest>,
) -> Result<Json<ActivateRevisionResponse>, ApiError> {
    Ok(Json(
        company_docs_service(&state)
            .activate_revision(&user.principal, &source_id, &revision_id, request)
            .await?,
    ))
}

pub(crate) async fn list_company_docs(
    _user: UserGuard,
    State(state): State<AppState>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(company_docs_service(&state).list()?))
}

pub(crate) async fn get_company_doc(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(company_docs_service(&state).get(&source_id)?))
}

pub(crate) async fn delete_company_doc(
    admin: AdminGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        company_docs_service(&state)
            .delete(&admin.principal, &source_id)
            .await?,
    ))
}

pub(crate) async fn list_revisions(
    _user: UserGuard,
    State(state): State<AppState>,
    Path(source_id): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        company_docs_service(&state).list_revisions(&source_id)?,
    ))
}

fn company_docs_service(state: &AppState) -> CompanyDocsService {
    CompanyDocsService::new(
        state.config.clone(),
        state.store.clone(),
        state.audit_recorder.clone(),
    )
}
