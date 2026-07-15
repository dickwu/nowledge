use std::sync::Arc;

use serde_json::Value;

use crate::{
    auth::Principal,
    config::Config,
    error::ApiError,
    models::{
        ActivateRevisionRequest, ActivateRevisionResponse, CompanyDocPreflightRequest,
        CompanyDocPreflightResponse, CreateRevisionRequest, CreateRevisionResponse,
    },
    shared_audit::audit_shared_write,
    store::Store,
};

#[derive(Clone)]
pub(crate) struct CompanyDocsService {
    config: Arc<Config>,
    store: Store,
}

impl CompanyDocsService {
    pub(crate) fn new(config: Arc<Config>, store: Store) -> Self {
        Self { config, store }
    }

    pub(crate) fn preflight(
        &self,
        principal: &Principal,
        request: CompanyDocPreflightRequest,
    ) -> Result<CompanyDocPreflightResponse, ApiError> {
        audit_shared_write(
            self.store.preflight_company_doc(request),
            principal,
            &self.config,
            &self.config.tenant_id,
            "company_doc.preflight",
            "company-doc:preflight",
            "preflight_requested",
        )
    }

    pub(crate) async fn create_revision(
        &self,
        principal: &Principal,
        source_id: &str,
        request: CreateRevisionRequest,
    ) -> Result<CreateRevisionResponse, ApiError> {
        let result = self
            .store
            .create_revision_async(&self.config.tenant_id, source_id, request)
            .await;
        audit_shared_write(
            result,
            principal,
            &self.config,
            &self.config.tenant_id,
            "company_doc.create_revision",
            source_id,
            "revision_create_requested",
        )
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
            .unwrap_or("activation_reason_unspecified")
            .to_string();
        let result = self
            .store
            .activate_revision_async(&self.config.tenant_id, source_id, revision_id, request)
            .await;
        audit_shared_write(
            result,
            principal,
            &self.config,
            &self.config.tenant_id,
            "company_doc.activate_revision",
            &format!("{source_id}:{revision_id}"),
            &audit_reason,
        )
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
        let result = self
            .store
            .delete_company_doc(&self.config.tenant_id, source_id)
            .await;
        audit_shared_write(
            result,
            principal,
            &self.config,
            &self.config.tenant_id,
            "company_doc.delete",
            source_id,
            "admin_delete",
        )
    }

    pub(crate) fn list_revisions(&self, source_id: &str) -> Result<Value, ApiError> {
        self.store.list_revisions(source_id)
    }
}
