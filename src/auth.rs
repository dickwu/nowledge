use std::collections::HashMap;

use axum::{
    extract::{FromRef, FromRequestParts},
    http::{header::AUTHORIZATION, request::Parts},
};
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

use crate::{
    app::AuthState,
    audit_service::DenialPrincipalIdentity,
    config::{AuthUserConfig, AuthUserScope, BearerTokenScope, Config},
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

    /// Intentionally identical to `require_owner_read` today: data scope, not
    /// the read/write verb, decides private-data reach (no read-only owner
    /// scope exists). Call sites keep the verb-specific name so a future
    /// read-only scope can diverge here without touching every route.
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

    // Exactly one configured credential must match; duplicates stay ambiguous
    // and are rejected just like the previous linear scan did.
    let credential = match state.credentials().lookup(actual) {
        [credential] => *credential,
        _ => return Err(ApiError::Unauthorized("invalid bearer token".to_string())),
    };
    match credential {
        CredentialRef::AuthUser(index) => {
            let user = config
                .auth_users
                .get(index)
                .ok_or_else(|| ApiError::Unauthorized("invalid bearer token".to_string()))?;
            Ok(principal_from_auth_user(&config.tenant_id, user))
        }
        CredentialRef::Admin => Ok(Principal {
            tenant_id: config.tenant_id.clone(),
            scope: PrincipalScope::Admin,
            roles: vec!["admin".to_string()],
        }),
        CredentialRef::Bearer => bearer_token_principal(config),
    }
}

fn bearer_token_principal(config: &Config) -> Result<Principal, ApiError> {
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
    Ok(Principal {
        tenant_id: config.tenant_id.clone(),
        scope,
        roles: vec!["user".to_string()],
    })
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

    bearer_credentials(header)
        .ok_or_else(|| ApiError::Unauthorized("Authorization must be a Bearer token".to_string()))
}

/// Parse `Authorization: Bearer <token>`. The auth-scheme is
/// case-insensitive (RFC 7235 §2.1 / RFC 6750 §2.1) and is separated from the
/// credentials by one or more spaces.
fn bearer_credentials(header: &str) -> Option<&str> {
    let (scheme, credentials) = header.split_once(' ')?;
    scheme
        .eq_ignore_ascii_case("bearer")
        .then(|| credentials.trim_start_matches(' '))
}

type HmacSha256 = Hmac<Sha256>;
type CredentialTag = [u8; 32];

/// Which configured credential a token tag belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CredentialRef {
    AuthUser(usize),
    Admin,
    Bearer,
}

/// Startup-precomputed index of configured credentials.
///
/// Every configured token is reduced once to an HMAC-SHA256 tag under a
/// per-process key derived from fresh randomness (plus the index-hash
/// secret), and requests perform one HMAC plus one map lookup instead of
/// recomputing tags for every configured user.
///
/// Timing: the map's hashing and the final tag equality check are not
/// constant-time, but they only ever operate on keyed tags. A remote caller
/// cannot compute the tag of any candidate token without the per-process key,
/// so lookup timing is uncorrelated with how close a guess is to a real
/// credential and reveals nothing exploitable about stored tokens. The key is
/// never exposed and changes on every restart.
pub(crate) struct CredentialIndex {
    key: Vec<u8>,
    tags: HashMap<CredentialTag, Vec<CredentialRef>>,
}

impl CredentialIndex {
    pub(crate) fn from_config(config: &Config) -> Self {
        let mut seed = HmacSha256::new_from_slice(b"nowledge-auth-credential-index-v1")
            .expect("HMAC accepts any key length");
        // Each UUIDv7 carries 74 OS-random bits; four of them give the key
        // ample entropy without adding a dependency.
        for _ in 0..4 {
            seed.update(uuid::Uuid::now_v7().as_bytes());
        }
        seed.update(&config.index_hash_secret);
        let key = seed.finalize().into_bytes().to_vec();

        let mut index = Self {
            key,
            tags: HashMap::new(),
        };
        for (position, user) in config.auth_users.iter().enumerate() {
            index.insert(&user.token, CredentialRef::AuthUser(position));
        }
        if let Some(token) = config.admin_token.as_deref() {
            index.insert(token, CredentialRef::Admin);
        }
        if let Some(token) = config.bearer_token.as_deref() {
            index.insert(token, CredentialRef::Bearer);
        }
        index
    }

    fn tag(&self, token: &str) -> CredentialTag {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC accepts any key length");
        mac.update(token.as_bytes());
        mac.finalize().into_bytes().into()
    }

    fn insert(&mut self, token: &str, credential: CredentialRef) {
        let tag = self.tag(token);
        self.tags.entry(tag).or_default().push(credential);
    }

    /// All configured credentials whose token equals `token`. Callers must
    /// treat anything other than exactly one match as a failed login.
    pub(crate) fn lookup(&self, token: &str) -> &[CredentialRef] {
        self.tags
            .get(&self.tag(token))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}

impl std::fmt::Debug for CredentialIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CredentialIndex")
            .field(
                "credentials",
                &self.tags.values().map(Vec::len).sum::<usize>(),
            )
            .finish_non_exhaustive()
    }
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

    fn credential_config() -> Config {
        let mut config = Config::test();
        config.auth_users = vec![
            AuthUserConfig {
                token: "correct-token".to_string(),
                roles: vec!["user".to_string()],
                scope: AuthUserScope::Owner {
                    owner_user_id: "u1".to_string(),
                },
            },
            AuthUserConfig {
                token: "service-token".to_string(),
                roles: vec!["user".to_string()],
                scope: AuthUserScope::TenantService,
            },
        ];
        config.admin_token = Some("admin-token".to_string());
        config.bearer_token = None;
        config
    }

    #[test]
    fn credential_index_matches_exact_tokens_only() {
        let index = CredentialIndex::from_config(&credential_config());
        assert_eq!(index.lookup("correct-token"), &[CredentialRef::AuthUser(0)]);
        assert_eq!(index.lookup("service-token"), &[CredentialRef::AuthUser(1)]);
        assert_eq!(index.lookup("admin-token"), &[CredentialRef::Admin]);
        assert!(index.lookup("correct-tokeo").is_empty());
        assert!(index.lookup("short").is_empty());
        assert!(index.lookup("correct-token-longer").is_empty());
        assert!(index.lookup("Correct-Token").is_empty());
        assert!(index.lookup("").is_empty());
    }

    #[test]
    fn credential_index_keeps_duplicate_tokens_ambiguous() {
        let mut config = credential_config();
        config.bearer_token = Some("admin-token".to_string());
        let index = CredentialIndex::from_config(&config);
        assert_eq!(
            index.lookup("admin-token"),
            &[CredentialRef::Admin, CredentialRef::Bearer]
        );
    }

    #[test]
    fn credential_index_keys_are_per_process_secrets() {
        let config = credential_config();
        let first = CredentialIndex::from_config(&config);
        let second = CredentialIndex::from_config(&config);
        assert_ne!(first.key, second.key);
        assert_ne!(first.tag("correct-token"), second.tag("correct-token"));
        assert!(!format!("{first:?}").contains("correct-token"));
    }

    #[test]
    fn bearer_scheme_is_case_insensitive_and_requires_a_separator() {
        assert_eq!(bearer_credentials("Bearer abc"), Some("abc"));
        assert_eq!(bearer_credentials("bearer abc"), Some("abc"));
        assert_eq!(bearer_credentials("BEARER abc"), Some("abc"));
        assert_eq!(bearer_credentials("bEaReR   abc"), Some("abc"));
        assert_eq!(bearer_credentials("Bearer"), None);
        assert_eq!(bearer_credentials("Bearerabc"), None);
        assert_eq!(bearer_credentials("Basic abc"), None);
        assert_eq!(bearer_credentials("Bearer "), Some(""));
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
