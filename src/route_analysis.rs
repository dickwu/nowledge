use axum::{extract::State, Json};
use serde_json::Value;

use crate::{
    analysis_service::AnalysisService, app::AppState, auth::UserGuard, error::ApiError,
    models::AnalysisInsightRequest, request_validation::validate_search_limit,
};

pub(crate) async fn analyze_insights(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<AnalysisInsightRequest>,
) -> Result<Json<Value>, ApiError> {
    if req.debug && !user.principal.is_admin() {
        return Err(ApiError::forbidden(
            "admin permission is required for analysis debug output",
        ));
    }
    user.apply_owner_default(&mut req.owner_user_id)?;
    require_owner_for_write(&user, req.owner_user_id.as_deref())?;
    validate_search_limit("context_limit", req.context_limit, &state.config)?;
    validate_search_limit("link_limit", req.link_limit, &state.config)?;
    if req.history_event_id.is_some() && req.owner_user_id.is_none() {
        return Err(ApiError::bad_request(
            "owner_user_id is required for history_event_id analysis",
        ));
    }
    let budget_key = user
        .principal
        .provider_budget_key(&state.config.index_hash_secret);
    Ok(Json(
        AnalysisService::analyze(&state, req, user.principal.is_admin(), &budget_key).await?,
    ))
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
