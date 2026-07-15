use axum::{
    extract::{FromRef, FromRequestParts},
    http::{header::AUTHORIZATION, request::Parts},
};
use hmac::{Hmac, Mac};
use sha2::Sha256;

use crate::{
    app::AuthState,
    audit_service::DenialPrincipalIdentity,
    config::{AuthUserConfig, AuthUserScope, BearerTokenScope},
    error::ApiError,
    request_context::{self, RequestPrincipal, RequestPrincipalScope},
    shared_audit::audit_shared_write_denial,
    util::hmac_hex,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrincipalScope {
    Owner { owner_user_id: String },
    TenantService,
    Admin,
}

#[derive(Debug, Clone)]
pub struct Principal {
    pub tenant_id: String,
    pub scope: PrincipalScope,
    pub roles: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct UserGuard {
    pub principal: Principal,
}

#[derive(Debug, Clone)]
pub struct CompanyWriterGuard {
    pub principal: Principal,
}

#[derive(Debug, Clone)]
pub struct AdminGuard {
    pub principal: Principal,
}

impl Principal {
    pub fn is_admin(&self) -> bool {
        matches!(self.scope, PrincipalScope::Admin)
    }

    pub fn owner_user_id(&self) -> Option<&str> {
        match &self.scope {
            PrincipalScope::Owner { owner_user_id } => Some(owner_user_id),
            PrincipalScope::TenantService | PrincipalScope::Admin => None,
        }
    }

    pub fn scope_label(&self) -> &'static str {
        match self.scope {
            PrincipalScope::Owner { .. } => "owner",
            PrincipalScope::TenantService => "tenant_service",
            PrincipalScope::Admin => "admin",
        }
    }

    pub fn has_role(&self, role: &str) -> bool {
        self.roles.iter().any(|candidate| candidate == role)
    }

    pub fn require_owner_read(&self, owner_user_id: &str) -> Result<(), ApiError> {
        match &self.scope {
            PrincipalScope::Owner {
                owner_user_id: principal_owner,
            } if principal_owner != owner_user_id => Err(ApiError::Forbidden(
                "principal is not allowed to access this owner_user_id".to_string(),
            )),
            PrincipalScope::Owner { .. }
            | PrincipalScope::TenantService
            | PrincipalScope::Admin => Ok(()),
        }
    }

    pub fn require_owner_write(&self, owner_user_id: &str) -> Result<(), ApiError> {
        self.require_owner_read(owner_user_id)
    }

    pub fn require_owner_access(&self, owner_user_id: &str) -> Result<(), ApiError> {
        self.require_owner_read(owner_user_id)
    }

    pub fn require_tenant_read(&self) -> Result<(), ApiError> {
        Ok(())
    }

    pub fn require_company_write(&self) -> Result<(), ApiError> {
        if self.is_admin() || self.has_role("company_writer") {
            Ok(())
        } else {
            Err(ApiError::Forbidden(
                "company_writer permission is required".to_string(),
            ))
        }
    }

    pub fn require_admin(&self) -> Result<(), ApiError> {
        if self.is_admin() {
            Ok(())
        } else {
            Err(ApiError::Forbidden("admin token required".to_string()))
        }
    }

    pub fn apply_owner_default(&self, owner_user_id: &mut Option<String>) -> Result<(), ApiError> {
        match (&self.scope, owner_user_id.as_deref()) {
            (PrincipalScope::Owner { .. }, Some(owner)) => self.require_owner_read(owner),
            (
                PrincipalScope::Owner {
                    owner_user_id: owner,
                },
                None,
            ) => {
                *owner_user_id = Some(owner.clone());
                Ok(())
            }
            (PrincipalScope::TenantService | PrincipalScope::Admin, _) => Ok(()),
        }
    }

    fn rate_limit_key(&self, index_hash_secret: &[u8]) -> String {
        let owner = self.owner_user_id().unwrap_or_default();
        let identity = format!("{}\0{}\0{owner}", self.tenant_id, self.scope_label());
        hmac_hex(index_hash_secret, "rate-limit-principal", &identity, 32)
    }

    pub(crate) fn denial_audit_identity(
        &self,
        index_hash_secret: &[u8],
    ) -> DenialPrincipalIdentity {
        // Denial admission intentionally shares the credential-independent
        // logical identity used by the HTTP limiter. Tokens, tenant IDs, and
        // owner IDs never become map keys in the audit recorder.
        DenialPrincipalIdentity::from_hmac_hex(self.rate_limit_key(index_hash_secret))
            .expect("the logical principal HMAC has the required bounded format")
    }

    /// Stable, non-secret identity for application-scoped upstream budgets.
    /// It intentionally reuses the authenticated principal scope but a
    /// distinct HMAC domain so provider accounting cannot reveal raw owner or
    /// tenant identifiers and cannot be confused with the HTTP limiter key.
    pub(crate) fn provider_budget_key(&self, index_hash_secret: &[u8]) -> String {
        let owner = self.owner_user_id().unwrap_or_default();
        let identity = format!("{}\0{}\0{owner}", self.tenant_id, self.scope_label());
        hmac_hex(
            index_hash_secret,
            "provider-budget-principal",
            &identity,
            32,
        )
    }
}

impl<S> FromRequestParts<S> for UserGuard
where
    S: Send + Sync,
    AuthState: FromRef<S>,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth_state = AuthState::from_ref(state);
        let principal = authenticate(parts, &auth_state)?;
        enforce_principal_rate_limit(&principal, &auth_state)?;
        record_request_principal(&principal, &auth_state);
        Ok(Self { principal })
    }
}

impl<S> FromRequestParts<S> for CompanyWriterGuard
where
    S: Send + Sync,
    AuthState: FromRef<S>,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth_state = AuthState::from_ref(state);
        let principal = match authenticate(parts, &auth_state) {
            Ok(principal) => principal,
            Err(error) => {
                audit_shared_write_denial(
                    None,
                    &auth_state,
                    &parts.method,
                    parts.uri.path(),
                    "authentication_failed",
                    &error,
                );
                return Err(error);
            }
        };
        enforce_principal_rate_limit(&principal, &auth_state)?;
        if let Err(error) = principal.require_company_write() {
            if !auth_state.config().allow_legacy_shared_writer {
                audit_shared_write_denial(
                    Some(&principal),
                    &auth_state,
                    &parts.method,
                    parts.uri.path(),
                    "company_writer_required",
                    &error,
                );
                return Err(error);
            }
        }
        record_request_principal(&principal, &auth_state);
        Ok(Self { principal })
    }
}

impl<S> FromRequestParts<S> for AdminGuard
where
    S: Send + Sync,
    AuthState: FromRef<S>,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let auth_state = AuthState::from_ref(state);
        let principal = match authenticate(parts, &auth_state) {
            Ok(principal) => principal,
            Err(error) => {
                audit_shared_write_denial(
                    None,
                    &auth_state,
                    &parts.method,
                    parts.uri.path(),
                    "authentication_failed",
                    &error,
                );
                return Err(error);
            }
        };
        enforce_principal_rate_limit(&principal, &auth_state)?;
        if let Err(error) = principal.require_admin() {
            audit_shared_write_denial(
                Some(&principal),
                &auth_state,
                &parts.method,
                parts.uri.path(),
                "admin_required",
                &error,
            );
            return Err(error);
        }
        record_request_principal(&principal, &auth_state);
        Ok(Self { principal })
    }
}

fn record_request_principal(principal: &Principal, state: &AuthState) {
    let config = state.config();
    let scope = match &principal.scope {
        PrincipalScope::Owner { owner_user_id } => RequestPrincipalScope::Owner {
            owner_user_id_hash: hmac_hex(&config.index_hash_secret, "user", owner_user_id, 16),
        },
        PrincipalScope::TenantService => RequestPrincipalScope::TenantService,
        PrincipalScope::Admin => RequestPrincipalScope::Admin,
    };
    request_context::set_current_principal(RequestPrincipal {
        scope,
        roles: principal.roles.clone(),
    });
}

fn enforce_principal_rate_limit(principal: &Principal, state: &AuthState) -> Result<(), ApiError> {
    let key = principal.rate_limit_key(&state.config().index_hash_secret);
    state.http_boundary().check_rate_limit(&key)
}

impl UserGuard {
    pub fn require_owner_read(&self, owner_user_id: &str) -> Result<(), ApiError> {
        self.principal.require_owner_read(owner_user_id)
    }

    pub fn require_owner_write(&self, owner_user_id: &str) -> Result<(), ApiError> {
        self.principal.require_owner_write(owner_user_id)
    }

    pub fn require_owner_access(&self, owner_user_id: &str) -> Result<(), ApiError> {
        self.principal.require_owner_access(owner_user_id)
    }

    pub fn apply_owner_default(&self, owner_user_id: &mut Option<String>) -> Result<(), ApiError> {
        self.principal.apply_owner_default(owner_user_id)
    }
}

fn authenticate(parts: &Parts, state: &AuthState) -> Result<Principal, ApiError> {
    let config = state.config();
    if !config.has_any_auth() {
        if config.allow_unsafe_unauthenticated {
            return Ok(Principal {
                tenant_id: config.tenant_id.clone(),
                scope: PrincipalScope::Admin,
                roles: vec!["admin".to_string()],
            });
        }
        return Err(ApiError::Unauthorized(
            "authentication is required".to_string(),
        ));
    }

    let actual = bearer(parts)?;
    if actual.is_empty() {
        return Err(ApiError::Unauthorized("invalid bearer token".to_string()));
    }

    let mut matched_user = None;
    let mut match_count = 0usize;
    for user in &config.auth_users {
        if token_matches(&user.token, actual) {
            matched_user = Some(user);
            match_count += 1;
        }
    }
    let admin_matches = config
        .admin_token
        .as_deref()
        .is_some_and(|expected| token_matches(expected, actual));
    match_count += usize::from(admin_matches);
    let bearer_matches = config
        .bearer_token
        .as_deref()
        .is_some_and(|expected| token_matches(expected, actual));
    match_count += usize::from(bearer_matches);

    if match_count != 1 {
        return Err(ApiError::Unauthorized("invalid bearer token".to_string()));
    }
    if let Some(user) = matched_user {
        return Ok(principal_from_auth_user(&config.tenant_id, user));
    }
    if admin_matches {
        return Ok(Principal {
            tenant_id: config.tenant_id.clone(),
            scope: PrincipalScope::Admin,
            roles: vec!["admin".to_string()],
        });
    }
    if bearer_matches {
        let scope = match (
            config.bearer_token_scope,
            config.bearer_token_owner_user_id.as_deref(),
            config.allow_legacy_tenant_service_bearer,
        ) {
            (Some(BearerTokenScope::Owner), Some(owner), false)
                if !owner.trim().is_empty() && owner == owner.trim() =>
            {
                Some(PrincipalScope::Owner {
                    owner_user_id: owner.to_string(),
                })
            }
            (Some(BearerTokenScope::TenantService), None, false) | (None, None, true) => {
                Some(PrincipalScope::TenantService)
            }
            _ => None,
        }
        .ok_or_else(|| ApiError::Unauthorized("invalid bearer token".to_string()))?;
        return Ok(Principal {
            tenant_id: config.tenant_id.clone(),
            scope,
            roles: vec!["user".to_string()],
        });
    }

    Err(ApiError::Unauthorized("invalid bearer token".to_string()))
}

fn principal_from_auth_user(tenant_id: &str, user: &AuthUserConfig) -> Principal {
    let scope = match &user.scope {
        AuthUserScope::Owner { owner_user_id } => PrincipalScope::Owner {
            owner_user_id: owner_user_id.clone(),
        },
        AuthUserScope::TenantService => PrincipalScope::TenantService,
        AuthUserScope::Admin => PrincipalScope::Admin,
    };
    Principal {
        tenant_id: tenant_id.to_string(),
        scope,
        roles: user.roles.clone(),
    }
}

fn bearer(parts: &Parts) -> Result<&str, ApiError> {
    let header = parts
        .headers
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or_else(|| ApiError::Unauthorized("missing Authorization bearer token".to_string()))?;

    header
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::Unauthorized("Authorization must be a Bearer token".to_string()))
}

fn token_matches(expected: &str, actual: &str) -> bool {
    type HmacSha256 = Hmac<Sha256>;
    const COMPARISON_KEY: &[u8] = b"nowledge-auth-token-comparison-v1";

    let mut expected_mac =
        HmacSha256::new_from_slice(COMPARISON_KEY).expect("comparison key is valid");
    expected_mac.update(expected.as_bytes());
    let expected_tag = expected_mac.finalize().into_bytes();

    let mut actual_mac =
        HmacSha256::new_from_slice(COMPARISON_KEY).expect("comparison key is valid");
    actual_mac.update(actual.as_bytes());
    actual_mac.verify_slice(&expected_tag).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn principal(scope: PrincipalScope, roles: &[&str]) -> Principal {
        Principal {
            tenant_id: "tenant".to_string(),
            scope,
            roles: roles.iter().map(|role| (*role).to_string()).collect(),
        }
    }

    #[test]
    fn owner_scope_defaults_and_rejects_cross_owner_access() {
        let principal = principal(
            PrincipalScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            &["user"],
        );
        let mut owner = None;
        principal.apply_owner_default(&mut owner).unwrap();
        assert_eq!(owner.as_deref(), Some("u1"));
        assert!(principal.require_owner_read("u1").is_ok());
        assert!(matches!(
            principal.require_owner_write("u2"),
            Err(ApiError::Forbidden(_))
        ));
    }

    #[test]
    fn service_admin_and_feature_roles_remain_orthogonal() {
        let service = principal(PrincipalScope::TenantService, &["admin"]);
        assert!(!service.is_admin());
        assert!(service.require_owner_read("u1").is_ok());
        assert!(matches!(
            service.require_admin(),
            Err(ApiError::Forbidden(_))
        ));

        let writer = principal(
            PrincipalScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            &["company_writer"],
        );
        assert!(writer.require_company_write().is_ok());
        assert!(writer.require_owner_read("u2").is_err());

        let admin = principal(PrincipalScope::Admin, &[]);
        assert!(admin.require_admin().is_ok());
        assert!(admin.require_company_write().is_ok());
    }

    #[test]
    fn token_comparison_handles_equal_near_and_different_length_values() {
        assert!(token_matches("correct-token", "correct-token"));
        assert!(!token_matches("correct-token", "correct-tokeo"));
        assert!(!token_matches("correct-token", "short"));
        assert!(!token_matches("correct-token", "correct-token-longer"));
        assert!(token_matches("", ""));
    }

    #[test]
    fn logical_rate_keys_ignore_credentials_and_roles_but_separate_scope() {
        let secret = b"rate-key-test-secret";
        let first = principal(
            PrincipalScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            &["user"],
        );
        let rotated = principal(
            PrincipalScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            &["user", "company_writer"],
        );
        let other = principal(
            PrincipalScope::Owner {
                owner_user_id: "u2".to_string(),
            },
            &["user"],
        );

        assert_eq!(first.rate_limit_key(secret), rotated.rate_limit_key(secret));
        assert_ne!(first.rate_limit_key(secret), other.rate_limit_key(secret));
        assert_ne!(
            first.rate_limit_key(secret),
            principal(PrincipalScope::TenantService, &["user"]).rate_limit_key(secret)
        );

        let denial_identity = first.denial_audit_identity(secret);
        assert_eq!(denial_identity.as_str(), first.rate_limit_key(secret));
        assert_eq!(denial_identity.as_str().len(), 32);
        assert!(denial_identity
            .as_str()
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit()));
        assert!(!denial_identity.as_str().contains("tenant"));
        assert!(!denial_identity.as_str().contains("u1"));
    }
}
