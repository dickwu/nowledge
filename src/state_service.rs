use crate::{
    error::ApiError,
    models::{
        InsightPatchRequest, InsightResponse, InsightSearchRequest, InsightSearchResponse,
        InsightUpsertRequest, LinkResponse, LinkSearchRequest, LinkSearchResponse,
        LinkUpsertRequest, PatchStateFactRequest, StateItemResponse, StateSearchRequest,
        StateSearchResponse, UpsertStateFactRequest,
    },
    store::Store,
};

#[derive(Clone)]
pub(crate) struct StateService {
    tenant_id: String,
    store: Store,
}

impl StateService {
    pub(crate) fn new(tenant_id: String, store: Store) -> Self {
        Self { tenant_id, store }
    }

    pub(crate) async fn upsert_fact(
        &self,
        fact_key: &str,
        request: UpsertStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        self.store
            .upsert_state_fact_async(&self.tenant_id, fact_key, request)
            .await
    }

    pub(crate) async fn patch_fact(
        &self,
        fact_key: &str,
        request: PatchStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        self.store
            .patch_state_fact_async(&self.tenant_id, fact_key, request)
            .await
    }

    pub(crate) fn get_fact(
        &self,
        fact_key: &str,
        owner_user_id: Option<&str>,
    ) -> Result<StateItemResponse, ApiError> {
        self.store
            .get_state_fact(&self.tenant_id, fact_key, owner_user_id)
    }

    pub(crate) fn search_state(
        &self,
        request: StateSearchRequest,
    ) -> Result<StateSearchResponse, ApiError> {
        self.store.search_state(&self.tenant_id, request)
    }

    pub(crate) async fn upsert_insight(
        &self,
        request: InsightUpsertRequest,
    ) -> Result<InsightResponse, ApiError> {
        self.store
            .upsert_insight_async(&self.tenant_id, request)
            .await
    }

    pub(crate) fn insight_owner(&self, insight_id: &str) -> Result<String, ApiError> {
        self.store.insight_owner(insight_id)
    }

    pub(crate) async fn patch_insight(
        &self,
        insight_id: &str,
        request: InsightPatchRequest,
    ) -> Result<InsightResponse, ApiError> {
        self.store
            .patch_insight_async(&self.tenant_id, insight_id, request)
            .await
    }

    pub(crate) fn search_insights(
        &self,
        request: InsightSearchRequest,
    ) -> Result<InsightSearchResponse, ApiError> {
        self.store.search_insights(request)
    }

    pub(crate) async fn upsert_link(
        &self,
        request: LinkUpsertRequest,
    ) -> Result<LinkResponse, ApiError> {
        self.store.upsert_link_async(&self.tenant_id, request).await
    }

    pub(crate) fn search_links(
        &self,
        request: LinkSearchRequest,
        is_admin: bool,
    ) -> Result<LinkSearchResponse, ApiError> {
        self.store.search_links(&self.tenant_id, request, is_admin)
    }
}
