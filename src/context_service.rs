use serde_json::Value;

use crate::{
    error::ApiError,
    models::{
        ContextNode, ContextRevealRequest, ContextRevealResponse, ContextSearchRequest,
        ContextSearchResponse, ContextTracebackRequest, ContextTracebackResponse,
    },
    store::Store,
};

#[derive(Clone)]
pub(crate) struct ContextService {
    tenant_id: String,
    store: Store,
}

impl ContextService {
    pub(crate) fn new(tenant_id: String, store: Store) -> Self {
        Self { tenant_id, store }
    }

    pub(crate) async fn list(
        &self,
        uri: Option<&str>,
        owner_user_id: Option<&str>,
        is_admin: bool,
    ) -> Result<Value, ApiError> {
        self.store
            .fs_ls_async(&self.tenant_id, uri, owner_user_id, is_admin)
            .await
    }

    pub(crate) async fn tree(
        &self,
        uri: Option<&str>,
        depth: Option<usize>,
        owner_user_id: Option<&str>,
        is_admin: bool,
    ) -> Result<Value, ApiError> {
        self.store
            .fs_tree_async(&self.tenant_id, uri, depth, owner_user_id, is_admin)
            .await
    }

    pub(crate) async fn read(
        &self,
        uri: &str,
        owner_user_id: Option<&str>,
        is_admin: bool,
    ) -> Result<ContextNode, ApiError> {
        self.store
            .fs_read_async(&self.tenant_id, uri, owner_user_id, is_admin)
            .await
    }

    pub(crate) async fn layer(
        &self,
        uri: &str,
        layer: u8,
        owner_user_id: Option<&str>,
        is_admin: bool,
    ) -> Result<ContextNode, ApiError> {
        self.store
            .fs_layer_async(&self.tenant_id, uri, layer, owner_user_id, is_admin)
            .await
    }

    pub(crate) async fn search(
        &self,
        request: ContextSearchRequest,
        is_admin: bool,
    ) -> Result<ContextSearchResponse, ApiError> {
        Ok(self
            .store
            .search_context_async(&self.tenant_id, request, is_admin)
            .await?
            .response)
    }

    pub(crate) async fn trace_owner(&self, trace_id: &str) -> Result<Option<String>, ApiError> {
        self.store
            .trace_owner_id_async(&self.tenant_id, trace_id)
            .await
    }

    pub(crate) async fn reveal(
        &self,
        request: ContextRevealRequest,
        owner_user_id: Option<&str>,
        is_admin: bool,
    ) -> Result<ContextRevealResponse, ApiError> {
        self.store
            .reveal_context_async(&self.tenant_id, request, owner_user_id, is_admin)
            .await
    }

    pub(crate) async fn traceback(
        &self,
        request: ContextTracebackRequest,
        is_admin: bool,
    ) -> Result<ContextTracebackResponse, ApiError> {
        self.store
            .traceback_async(&self.tenant_id, request, is_admin)
            .await
    }
}
