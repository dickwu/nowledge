use std::sync::Arc;

use serde_json::Value;

use crate::{
    audit_service::{caller_supplied_audit_reason, AuditRecorder},
    auth::Principal,
    config::Config,
    error::ApiError,
    models::{
        ActivateRevisionRequest, ActivateRevisionResponse, CompanyDocPreflightRequest,
        CompanyDocPreflightResponse, CreateRevisionRequest, CreateRevisionResponse,
    },
    shared_audit::{
        audit_shared_write, company_doc_activate_revision_target,
        company_doc_create_revision_target, company_doc_delete_target,
        company_doc_preflight_target,
    },
    store::Store,
};

#[derive(Clone)]
pub(crate) struct CompanyDocsService {
    config: Arc<Config>,
    store: Store,
    audit_recorder: AuditRecorder,
}

impl CompanyDocsService {
    pub(crate) fn new(config: Arc<Config>, store: Store, audit_recorder: AuditRecorder) -> Self {
        Self {
            config,
            store,
            audit_recorder,
        }
    }

    pub(crate) async fn preflight(
        &self,
        principal: &Principal,
        request: CompanyDocPreflightRequest,
    ) -> Result<CompanyDocPreflightResponse, ApiError> {
        let store = self.store.clone();
        audit_shared_write(
            &self.audit_recorder,
            principal,
            company_doc_preflight_target(),
            "preflight_requested",
            || async move { store.preflight_company_doc(request) },
        )
        .await
    }

    pub(crate) async fn create_revision(
        &self,
        principal: &Principal,
        source_id: &str,
        request: CreateRevisionRequest,
    ) -> Result<CreateRevisionResponse, ApiError> {
        let store = self.store.clone();
        let tenant_id = self.config.tenant_id.clone();
        audit_shared_write(
            &self.audit_recorder,
            principal,
            company_doc_create_revision_target(source_id),
            "revision_create_requested",
            || async move {
                store
                    .create_revision_async(&tenant_id, source_id, request)
                    .await
            },
        )
        .await
    }

    pub(crate) async fn activate_revision(
        &self,
        principal: &Principal,
        source_id: &str,
        revision_id: &str,
        request: ActivateRevisionRequest,
    ) -> Result<ActivateRevisionResponse, ApiError> {
        let audit_reason = request
            .reason
            .as_deref()
            .map(caller_supplied_audit_reason)
            .unwrap_or_else(|| "activation_reason_unspecified".to_string());
        let store = self.store.clone();
        let tenant_id = self.config.tenant_id.clone();
        audit_shared_write(
            &self.audit_recorder,
            principal,
            company_doc_activate_revision_target(source_id, revision_id),
            &audit_reason,
            || async move {
                store
                    .activate_revision_async(&tenant_id, source_id, revision_id, request)
                    .await
            },
        )
        .await
    }

    pub(crate) fn list(&self) -> Result<Value, ApiError> {
        self.store.list_company_docs()
    }

    pub(crate) fn get(&self, source_id: &str) -> Result<Value, ApiError> {
        self.store.get_company_doc(source_id)
    }

    pub(crate) async fn delete(
        &self,
        principal: &Principal,
        source_id: &str,
    ) -> Result<Value, ApiError> {
        let store = self.store.clone();
        let tenant_id = self.config.tenant_id.clone();
        audit_shared_write(
            &self.audit_recorder,
            principal,
            company_doc_delete_target(source_id),
            "admin_delete",
            || async move { store.delete_company_doc(&tenant_id, source_id).await },
        )
        .await
    }

    pub(crate) fn list_revisions(&self, source_id: &str) -> Result<Value, ApiError> {
        self.store.list_revisions(source_id)
    }
}
