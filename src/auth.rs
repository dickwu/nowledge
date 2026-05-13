use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, request::Parts},
};

use crate::{error::ApiError, routes::AppState};

#[derive(Debug, Clone)]
pub struct Principal {
    pub tenant_id: String,
    pub owner_user_id: Option<String>,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UserGuard {
    pub principal: Principal,
}

#[derive(Debug, Clone)]
pub struct AdminGuard {
    pub principal: Principal,
}

impl Principal {
    pub fn is_admin(&self) -> bool {
        self.roles.iter().any(|role| role == "admin")
    }

    pub fn require_owner_access(&self, owner_user_id: &str) -> Result<(), ApiError> {
        if self.is_admin() {
            return Ok(());
        }
        if let Some(owner) = &self.owner_user_id {
            if owner == owner_user_id {
                return Ok(());
            }
            return Err(ApiError::Forbidden(
                "principal is not allowed to access this owner_user_id".to_string(),
            ));
        }
        Ok(())
    }

    pub fn apply_owner_default(&self, owner_user_id: &mut Option<String>) -> Result<(), ApiError> {
        match (owner_user_id.as_deref(), self.owner_user_id.as_deref()) {
            (Some(owner), _) => self.require_owner_access(owner),
            (None, Some(owner)) if !self.is_admin() => {
                *owner_user_id = Some(owner.to_string());
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

impl FromRequestParts<AppState> for UserGuard {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let principal = authenticate(parts, state, false)?;
        Ok(Self { principal })
    }
}

impl FromRequestParts<AppState> for AdminGuard {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let principal = authenticate(parts, state, true)?;
        if !principal.is_admin() {
            return Err(ApiError::Forbidden("admin token required".to_string()));
        }
        Ok(Self { principal })
    }
}

impl UserGuard {
    pub fn require_owner_access(&self, owner_user_id: &str) -> Result<(), ApiError> {
        self.principal.require_owner_access(owner_user_id)
    }

    pub fn apply_owner_default(&self, owner_user_id: &mut Option<String>) -> Result<(), ApiError> {
        self.principal.apply_owner_default(owner_user_id)
    }
}

fn authenticate(
    parts: &Parts,
    state: &AppState,
    admin_required: bool,
) -> Result<Principal, ApiError> {
    let config = &state.config;
    let unauthenticated = || Principal {
        tenant_id: config.tenant_id.clone(),
        owner_user_id: None,
        roles: vec![if admin_required || !config.has_any_auth() {
            "admin"
        } else {
            "user"
        }
        .to_string()],
    };

    if !config.has_any_auth() {
        if config.allow_unsafe_unauthenticated {
            return Ok(unauthenticated());
        }
        return Err(ApiError::Unauthorized(
            "authentication is required".to_string(),
        ));
    }

    let actual = bearer(parts)?;
    if let Some(user) = config.auth_users.iter().find(|user| user.token == actual) {
        return Ok(Principal {
            tenant_id: config.tenant_id.clone(),
            owner_user_id: user.owner_user_id.clone(),
            roles: user.roles.clone(),
        });
    }

    if config.admin_token.as_deref() == Some(actual) {
        return Ok(Principal {
            tenant_id: config.tenant_id.clone(),
            owner_user_id: None,
            roles: vec!["admin".to_string()],
        });
    }

    if !admin_required && config.bearer_token.as_deref() == Some(actual) {
        return Ok(Principal {
            tenant_id: config.tenant_id.clone(),
            owner_user_id: None,
            roles: vec!["user".to_string()],
        });
    }

    Err(ApiError::Unauthorized("invalid bearer token".to_string()))
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
