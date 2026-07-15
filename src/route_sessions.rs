use axum::{extract::Path, extract::State, Json};
use serde_json::Value;

use crate::{
    app::AppState,
    auth::UserGuard,
    error::ApiError,
    models::{
        SessionCommitRequest, SessionCommitResponse, SessionCreateRequest, SessionMessageRequest,
        SessionResponse,
    },
    session_service::SessionService,
};

pub(crate) async fn create_session(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<SessionCreateRequest>,
) -> Result<Json<SessionResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    Ok(Json(
        SessionService::new(state.store.clone(), state.tenant_id().to_string())
            .create(request)
            .await?,
    ))
}

pub(crate) async fn add_session_message(
    user: UserGuard,
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<SessionMessageRequest>,
) -> Result<Json<Value>, ApiError> {
    let service = session_service(&state);
    let owner = service.owner_id(&session_id)?;
    user.require_owner_access(&owner)?;
    Ok(Json(service.add_message(&session_id, request).await?))
}

pub(crate) async fn commit_session(
    user: UserGuard,
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(request): Json<SessionCommitRequest>,
) -> Result<Json<SessionCommitResponse>, ApiError> {
    let service = session_service(&state);
    let owner = service.owner_id(&session_id)?;
    user.require_owner_access(&owner)?;
    Ok(Json(service.commit(&session_id, request).await?))
}

fn session_service(state: &AppState) -> SessionService {
    SessionService::new(state.store.clone(), state.tenant_id().to_string())
}
