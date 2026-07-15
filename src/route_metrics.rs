use axum::{
    extract::State,
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};

use crate::{app::AppState, auth::AdminGuard, error::ApiError};

const OPENMETRICS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

pub(crate) async fn metrics(
    _admin: AdminGuard,
    State(state): State<AppState>,
) -> Result<Response, ApiError> {
    let snapshot = state
        .store
        .operational_metrics_snapshot(state.tenant_id())?;
    let body = state
        .metrics
        .render(state.ingest_manager.metrics(), &snapshot)?;
    Ok(([(CONTENT_TYPE, OPENMETRICS_CONTENT_TYPE)], body).into_response())
}
