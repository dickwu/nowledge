use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, request::Parts},
};

use crate::{error::ApiError, routes::AppState};

#[derive(Debug, Clone, Copy)]
pub struct UserGuard;

#[derive(Debug, Clone, Copy)]
pub struct AdminGuard;

impl FromRequestParts<AppState> for UserGuard {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        if let Some(expected) = &state.config.bearer_token {
            let actual = bearer(parts)?;
            if actual != expected {
                return Err(ApiError::Unauthorized("invalid bearer token".to_string()));
            }
        }
        Ok(Self)
    }
}

impl FromRequestParts<AppState> for AdminGuard {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let expected = state
            .config
            .admin_token
            .as_ref()
            .or(state.config.bearer_token.as_ref());

        if let Some(expected) = expected {
            let actual = bearer(parts)?;
            if actual != expected {
                return Err(ApiError::Forbidden("admin token required".to_string()));
            }
        }
        Ok(Self)
    }
}

fn bearer(parts: &Parts) -> Result<&str, ApiError> {
    let header = parts
        .headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::Unauthorized("missing Authorization bearer token".to_string()))?;

    header
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::Unauthorized("Authorization must be a Bearer token".to_string()))
}
