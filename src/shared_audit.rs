use std::{fmt::Write, future::Future};

use axum::http::Method;

use crate::{
    app::AuthState,
    audit_service::AuditRecorder,
    auth::{Principal, PrincipalScope},
    error::ApiError,
    models::{AuditAction, AuditPrincipalScope},
};

#[derive(Debug, Clone)]
pub(crate) struct SharedMutationAuditTarget {
    action: AuditAction,
    resource_identity: String,
}

pub(crate) fn company_doc_preflight_target() -> SharedMutationAuditTarget {
    SharedMutationAuditTarget {
        action: AuditAction::CompanyDocPreflight,
        resource_identity: length_prefixed_identity(&["company-doc", "preflight"]),
    }
}

pub(crate) fn company_doc_create_revision_target(source_id: &str) -> SharedMutationAuditTarget {
    SharedMutationAuditTarget {
        action: AuditAction::CompanyDocCreateRevision,
        resource_identity: length_prefixed_identity(&[source_id]),
    }
}

pub(crate) fn company_doc_activate_revision_target(
    source_id: &str,
    revision_id: &str,
) -> SharedMutationAuditTarget {
    SharedMutationAuditTarget {
        action: AuditAction::CompanyDocActivateRevision,
        resource_identity: length_prefixed_identity(&[source_id, revision_id]),
    }
}

pub(crate) fn company_doc_delete_target(source_id: &str) -> SharedMutationAuditTarget {
    SharedMutationAuditTarget {
        action: AuditAction::CompanyDocDelete,
        resource_identity: length_prefixed_identity(&[source_id]),
    }
}

pub(crate) fn dataset_upsert_schema_target(dataset_key: &str) -> SharedMutationAuditTarget {
    SharedMutationAuditTarget {
        action: AuditAction::DatasetUpsertSchema,
        resource_identity: length_prefixed_identity(&[dataset_key]),
    }
}

pub(crate) async fn audit_shared_write<T, F, Fut>(
    recorder: &AuditRecorder,
    principal: &Principal,
    target: SharedMutationAuditTarget,
    reason: &str,
    mutation: F,
) -> Result<T, ApiError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, ApiError>>,
{
    recorder
        .record_mutation(
            principal_scope(Some(principal)),
            principal.owner_user_id(),
            target.action,
            &target.resource_identity,
            reason,
            None,
            mutation,
        )
        .await
}

pub(crate) async fn audit_shared_write_denial(
    principal: Option<&Principal>,
    state: &AuthState,
    method: &Method,
    path: &str,
    reason: &str,
    error: &ApiError,
) {
    let Some(target) = shared_mutation_audit_target(method, path) else {
        return;
    };
    state
        .audit_recorder()
        .record_denial(
            principal_scope(principal),
            principal.and_then(Principal::owner_user_id),
            target.action,
            &target.resource_identity,
            reason,
            error,
        )
        .await;
}

fn shared_mutation_audit_target(method: &Method, path: &str) -> Option<SharedMutationAuditTarget> {
    let segments = path.trim_matches('/').split('/').collect::<Vec<_>>();
    match (method, segments.as_slice()) {
        (&Method::POST, ["v1", "state", "company-docs", "preflight"]) => {
            Some(company_doc_preflight_target())
        }
        (&Method::POST, ["v1", "state", "company-docs", source_id, "revisions"]) => {
            Some(company_doc_create_revision_target(source_id))
        }
        (
            &Method::POST,
            ["v1", "state", "company-docs", source_id, "revisions", revision_id, "activate"],
        ) => Some(company_doc_activate_revision_target(source_id, revision_id)),
        (&Method::PUT, ["v1", "state", "structured", "datasets", dataset_key]) => {
            Some(dataset_upsert_schema_target(dataset_key))
        }
        (&Method::DELETE, ["v1", "state", "company-docs", source_id]) => {
            Some(company_doc_delete_target(source_id))
        }
        _ => None,
    }
}

fn principal_scope(principal: Option<&Principal>) -> AuditPrincipalScope {
    match principal.map(|principal| &principal.scope) {
        None => AuditPrincipalScope::Unauthenticated,
        Some(PrincipalScope::Owner { .. }) => AuditPrincipalScope::Owner,
        Some(PrincipalScope::TenantService) => AuditPrincipalScope::TenantService,
        Some(PrincipalScope::Admin) => AuditPrincipalScope::Admin,
    }
}

fn length_prefixed_identity(parts: &[&str]) -> String {
    let mut identity = String::new();
    for part in parts {
        let _ = write!(&mut identity, "{}:{part}", part.len());
    }
    identity
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn denial_mapping_is_exact_and_composite_ids_are_unambiguous() {
        let target = shared_mutation_audit_target(
            &Method::POST,
            "/v1/state/company-docs/source/revisions/revision/activate",
        )
        .unwrap();
        assert_eq!(target.action, AuditAction::CompanyDocActivateRevision);
        assert_eq!(target.resource_identity, "6:source8:revision");

        assert!(shared_mutation_audit_target(
            &Method::GET,
            "/v1/state/company-docs/source/revisions/revision/activate"
        )
        .is_none());
        assert_ne!(
            length_prefixed_identity(&["a:b", "c"]),
            length_prefixed_identity(&["a", "b:c"])
        );
    }
}
