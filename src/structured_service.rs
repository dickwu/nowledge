use std::sync::Arc;

use serde_json::Value;

use crate::{
    audit_service::AuditRecorder,
    auth::Principal,
    config::Config,
    error::ApiError,
    models::{
        ApplySnapshotRequest, ApplySnapshotResponse, BulkStructuredRowsRequest,
        BulkStructuredRowsResponse, CreateStructuredSnapshotRequest,
        CurrentStructuredStateResponse, DatasetSchemaResponse, DatasetSchemaUpsertRequest,
        StructuredSnapshot, StructuredSnapshotResponse,
    },
    shared_audit::{audit_shared_write, dataset_upsert_schema_target},
    store::Store,
};

#[derive(Clone)]
pub(crate) struct StructuredService {
    config: Arc<Config>,
    store: Store,
    audit_recorder: AuditRecorder,
}

impl StructuredService {
    pub(crate) fn new(config: Arc<Config>, store: Store, audit_recorder: AuditRecorder) -> Self {
        Self {
            config,
            store,
            audit_recorder,
        }
    }

    pub(crate) async fn upsert_dataset(
        &self,
        principal: &Principal,
        dataset_key: &str,
        request: DatasetSchemaUpsertRequest,
    ) -> Result<DatasetSchemaResponse, ApiError> {
        let store = self.store.clone();
        let tenant_id = self.config.tenant_id.clone();
        audit_shared_write(
            &self.audit_recorder,
            principal,
            dataset_upsert_schema_target(dataset_key),
            "schema_upsert",
            || async move {
                store
                    .upsert_dataset_async(&tenant_id, dataset_key, request)
                    .await
            },
        )
        .await
    }

    pub(crate) async fn snapshot_owner(&self, snapshot_id: &str) -> Result<String, ApiError> {
        self.store
            .snapshot_owner_async(&self.config.tenant_id, snapshot_id)
            .await
    }

    pub(crate) async fn apply_snapshot(
        &self,
        dataset_key: &str,
        request: ApplySnapshotRequest,
    ) -> Result<ApplySnapshotResponse, ApiError> {
        self.store
            .apply_snapshot_async(&self.config.tenant_id, dataset_key, request)
            .await
    }

    pub(crate) fn current_state(
        &self,
        owner_user_id: Option<&str>,
        is_admin: bool,
    ) -> Result<CurrentStructuredStateResponse, ApiError> {
        self.store
            .current_structured_state(&self.config.tenant_id, owner_user_id, is_admin)
    }

    pub(crate) async fn create_snapshot(
        &self,
        request: CreateStructuredSnapshotRequest,
    ) -> Result<StructuredSnapshotResponse, ApiError> {
        self.store
            .create_snapshot_async(&self.config.tenant_id, request)
            .await
    }

    pub(crate) async fn get_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<StructuredSnapshot, ApiError> {
        self.store
            .get_snapshot_async(&self.config.tenant_id, snapshot_id)
            .await
    }

    pub(crate) async fn bulk_rows(
        &self,
        snapshot_id: &str,
        request: BulkStructuredRowsRequest,
    ) -> Result<BulkStructuredRowsResponse, ApiError> {
        self.store
            .bulk_rows_async(&self.config.tenant_id, snapshot_id, request)
            .await
    }

    pub(crate) async fn list_rows(&self, snapshot_id: &str) -> Result<Value, ApiError> {
        self.store
            .list_rows_async(&self.config.tenant_id, snapshot_id)
            .await
    }
}
