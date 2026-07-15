use axum::{extract::State, Json};
use serde_json::Value;

use crate::{
    app::AppState, auth::UserGuard, error::ApiError, llm_service::LlmService,
    models::LlmTitleRequest,
};

pub(crate) async fn llm_title(
    user: UserGuard,
    State(state): State<AppState>,
    Json(req): Json<LlmTitleRequest>,
) -> Result<Json<Value>, ApiError> {
    let budget_key = user
        .principal
        .provider_budget_key(&state.config.index_hash_secret);
    Ok(Json(LlmService::title(&state, req, &budget_key).await?))
}
