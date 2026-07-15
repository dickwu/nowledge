use crate::{
    error::ApiError,
    models::{
        CreateHarnessChangeManifestRequest, CreateHarnessChangeVerdictRequest,
        CreateHarnessComponentRevisionRequest, EvalDeltaReport, HarnessChangeManifest,
        HarnessChangeVerdict, HarnessComponent, HarnessComponentDetail, HarnessComponentRevision,
        HarnessRollbackResponse, RollbackHarnessComponentRequest,
    },
    store::Store,
};

pub(crate) struct HarnessService {
    store: Store,
    tenant_id: String,
}

impl HarnessService {
    pub(crate) fn new(store: Store, tenant_id: String) -> Self {
        Self { store, tenant_id }
    }

    pub(crate) fn list_components(&self) -> Result<Vec<HarnessComponent>, ApiError> {
        self.store.list_harness_components()
    }

    pub(crate) fn component_detail(
        &self,
        component_id: &str,
    ) -> Result<HarnessComponentDetail, ApiError> {
        self.store.harness_component_detail(component_id)
    }

    pub(crate) async fn create_component_revision(
        &self,
        component_id: &str,
        request: CreateHarnessComponentRevisionRequest,
    ) -> Result<HarnessComponentRevision, ApiError> {
        self.store
            .create_harness_component_revision_async(&self.tenant_id, component_id, request)
            .await
    }

    pub(crate) async fn rollback_component(
        &self,
        component_id: &str,
        request: RollbackHarnessComponentRequest,
    ) -> Result<HarnessRollbackResponse, ApiError> {
        self.store
            .rollback_harness_component_async(&self.tenant_id, component_id, request)
            .await
    }

    pub(crate) async fn create_change(
        &self,
        request: CreateHarnessChangeManifestRequest,
    ) -> Result<HarnessChangeManifest, ApiError> {
        self.store
            .create_harness_change_async(&self.tenant_id, request)
            .await
    }

    pub(crate) fn list_changes(&self) -> Result<Vec<HarnessChangeManifest>, ApiError> {
        self.store.list_harness_changes()
    }

    pub(crate) fn change(&self, change_id: &str) -> Result<HarnessChangeManifest, ApiError> {
        self.store.harness_change(change_id)
    }

    pub(crate) async fn create_verdict(
        &self,
        change_id: &str,
        request: CreateHarnessChangeVerdictRequest,
    ) -> Result<HarnessChangeVerdict, ApiError> {
        self.store
            .create_harness_verdict_async(&self.tenant_id, change_id, request)
            .await
    }

    pub(crate) fn compare_change(
        &self,
        change_id: &str,
        baseline_eval_run_id: Option<String>,
        candidate_eval_run_id: Option<String>,
    ) -> Result<EvalDeltaReport, ApiError> {
        self.store
            .compare_harness_change(change_id, baseline_eval_run_id, candidate_eval_run_id)
    }
}
