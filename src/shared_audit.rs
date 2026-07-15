use axum::http::Method;

use crate::{
    app::{AppState, AuthState},
    auth::Principal,
    config::Config,
    error::ApiError,
    request_context,
    util::hmac_hex,
};

pub(crate) fn audit_shared_write<T>(
    result: Result<T, ApiError>,
    principal: &Principal,
    state: &AppState,
    action: &str,
    resource_id: &str,
    reason: &str,
) -> Result<T, ApiError> {
    match &result {
        Ok(_) => emit_shared_mutation_audit(
            Some(principal),
            &state.config,
            state.tenant_id(),
            action,
            resource_id,
            reason,
            "success",
            None,
        ),
        Err(error) => emit_shared_mutation_audit(
            Some(principal),
            &state.config,
            state.tenant_id(),
            action,
            resource_id,
            reason,
            "failure",
            Some(api_error_kind(error)),
        ),
    }
    result
}

pub(crate) fn audit_shared_write_denial(
    principal: Option<&Principal>,
    state: &AuthState,
    method: &Method,
    path: &str,
    reason: &str,
    error: &ApiError,
) {
    let Some((action, resource_id)) = shared_mutation_audit_target(method, path) else {
        return;
    };
    emit_shared_mutation_audit(
        principal,
        state.config(),
        state.tenant_id(),
        action,
        &resource_id,
        reason,
        "denied",
        Some(api_error_kind(error)),
    );
}

fn shared_mutation_audit_target(method: &Method, path: &str) -> Option<(&'static str, String)> {
    let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
    match (method, segments.as_slice()) {
        (&Method::POST, ["v1", "state", "company-docs", "preflight"]) => {
            Some(("company_doc.preflight", "company-doc:preflight".to_string()))
        }
        (&Method::POST, ["v1", "state", "company-docs", source_id, "revisions"]) => {
            Some(("company_doc.create_revision", (*source_id).to_string()))
        }
        (
            &Method::POST,
            ["v1", "state", "company-docs", source_id, "revisions", revision_id, "activate"],
        ) => Some((
            "company_doc.activate_revision",
            format!("{source_id}:{revision_id}"),
        )),
        (&Method::PUT, ["v1", "state", "structured", "datasets", dataset_key]) => {
            Some(("dataset.upsert_schema", (*dataset_key).to_string()))
        }
        (&Method::DELETE, ["v1", "state", "company-docs", source_id]) => {
            Some(("company_doc.delete", (*source_id).to_string()))
        }
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_shared_mutation_audit(
    principal: Option<&Principal>,
    config: &Config,
    tenant_id: &str,
    action: &str,
    resource_id: &str,
    reason: &str,
    outcome: &str,
    error_kind: Option<&str>,
) {
    let request_id = request_context::current_or_new_id();
    let resource_id = audit_identifier(config, "resource", resource_id);
    let tenant_id = audit_identifier(config, "tenant", tenant_id);
    let principal_scope = principal
        .map(Principal::scope_label)
        .unwrap_or("unauthenticated");
    let owner_user_id = principal
        .and_then(Principal::owner_user_id)
        .map(|owner| audit_identifier(config, "principal-owner", owner))
        .unwrap_or_else(|| "none".to_string());
    let (reason_code, reason_fingerprint) = audit_reason(config, reason);
    if outcome == "success" {
        tracing::info!(
            target: "nowledge::audit",
            %request_id,
            %tenant_id,
            principal_scope,
            principal_owner_user_id = %owner_user_id,
            action,
            %resource_id,
            reason = reason_code,
            %reason_fingerprint,
            outcome,
            "shared knowledge mutation"
        );
    } else {
        tracing::warn!(
            target: "nowledge::audit",
            %request_id,
            %tenant_id,
            principal_scope,
            principal_owner_user_id = %owner_user_id,
            action,
            %resource_id,
            reason = reason_code,
            %reason_fingerprint,
            outcome,
            error_kind = error_kind.unwrap_or("unknown"),
            "shared knowledge mutation"
        );
    }
}

fn audit_identifier(config: &Config, namespace: &str, value: &str) -> String {
    format!(
        "hmac:{}",
        hmac_hex(&config.index_hash_secret, namespace, value, 16)
    )
}

fn audit_reason(config: &Config, reason: &str) -> (&'static str, String) {
    let reason_code = match reason {
        "authentication_failed" => "authentication_failed",
        "company_writer_required" => "company_writer_required",
        "admin_required" => "admin_required",
        "preflight_requested" => "preflight_requested",
        "revision_create_requested" => "revision_create_requested",
        "activation_reason_unspecified" => "activation_reason_unspecified",
        "admin_delete" => "admin_delete",
        "schema_upsert" => "schema_upsert",
        _ => "caller_supplied",
    };
    let reason_fingerprint = format!(
        "hmac:{}",
        hmac_hex(&config.index_hash_secret, "audit-reason", reason, 16)
    );
    (reason_code, reason_fingerprint)
}

fn api_error_kind(error: &ApiError) -> &'static str {
    match error {
        ApiError::BadRequest(_) => "bad_request",
        ApiError::Validation { .. } => "validation_error",
        ApiError::Unauthorized(_) => "unauthorized",
        ApiError::Forbidden(_) => "forbidden",
        ApiError::NotFound(_) => "not_found",
        ApiError::Conflict(_) => "conflict",
        ApiError::PayloadTooLarge => "payload_too_large",
        ApiError::TooManyRequests(_) => "too_many_requests",
        ApiError::ServiceUnavailable(_) => "service_unavailable",
        ApiError::Timeout => "timeout",
        ApiError::Upstream(_) => "upstream_error",
        ApiError::Internal(_) => "internal_error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn audit_identifiers_and_caller_reasons_are_never_logged_raw() {
        let config = Config::test();
        let raw_identifier = "tenant/private-owner/source-id";
        let identifier = audit_identifier(&config, "resource", raw_identifier);
        assert!(identifier.starts_with("hmac:"));
        assert!(!identifier.contains(raw_identifier));

        let raw_reason = "activate because /private/auth.json contains a provider token";
        let (reason_code, reason_fingerprint) = audit_reason(&config, raw_reason);
        assert_eq!(reason_code, "caller_supplied");
        assert!(reason_fingerprint.starts_with("hmac:"));
        assert!(!reason_fingerprint.contains(raw_reason));

        let (system_code, _) = audit_reason(&config, "company_writer_required");
        assert_eq!(system_code, "company_writer_required");
    }
}
