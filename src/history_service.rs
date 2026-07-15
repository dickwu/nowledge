use crate::{
    error::ApiError,
    models::{
        AppendHistoryEventRequest, BulkHistoryEventsRequest, BulkHistoryEventsResponse,
        EnsureUserEventIndexRequest, HistoryEvent, HistoryEventResponse, HistorySearchRequest,
        HistorySearchResponse, ListUserEventIndexesResponse, OperationListRequest,
        OperationListResponse, ReconcileOperationsRequest, ReconcileOperationsResponse,
        ReconcileUserEventIndexesRequest, ReconcileUserEventIndexesResponse, TimelineQueryRequest,
        TimelineResponse, UserEventIndexResponse,
    },
    store::Store,
};

#[derive(Clone)]
pub(crate) struct HistoryService {
    tenant_id: String,
    store: Store,
}

impl HistoryService {
    pub(crate) fn new(tenant_id: String, store: Store) -> Self {
        Self { tenant_id, store }
    }

    pub(crate) async fn ensure_user_index(
        &self,
        owner_user_id: &str,
        request: EnsureUserEventIndexRequest,
    ) -> Result<UserEventIndexResponse, ApiError> {
        self.store
            .ensure_user_index_async(&self.tenant_id, owner_user_id, request)
            .await
    }

    pub(crate) fn list_user_indexes(&self) -> Result<ListUserEventIndexesResponse, ApiError> {
        self.store.list_user_indexes(&self.tenant_id)
    }

    pub(crate) async fn reconcile_user_indexes(
        &self,
        request: ReconcileUserEventIndexesRequest,
    ) -> Result<ReconcileUserEventIndexesResponse, ApiError> {
        self.store
            .reconcile_user_indexes_async(&self.tenant_id, request)
            .await
    }

    pub(crate) async fn list_operations(
        &self,
        request: OperationListRequest,
    ) -> Result<OperationListResponse, ApiError> {
        self.store.list_operations(&self.tenant_id, request).await
    }

    pub(crate) async fn reconcile_operations(
        &self,
        request: ReconcileOperationsRequest,
    ) -> Result<ReconcileOperationsResponse, ApiError> {
        self.store
            .reconcile_operations_async(&self.tenant_id, request)
            .await
    }

    pub(crate) async fn append_event(
        &self,
        owner_user_id: Option<&str>,
        request: AppendHistoryEventRequest,
    ) -> Result<HistoryEventResponse, ApiError> {
        self.store
            .append_event_async(&self.tenant_id, owner_user_id, request)
            .await
    }

    pub(crate) async fn append_bulk_events(
        &self,
        owner_user_id: Option<&str>,
        request: BulkHistoryEventsRequest,
    ) -> Result<BulkHistoryEventsResponse, ApiError> {
        self.store
            .append_bulk_events_async(&self.tenant_id, owner_user_id, request)
            .await
    }

    pub(crate) async fn search_events(
        &self,
        owner_user_id: Option<&str>,
        request: HistorySearchRequest,
    ) -> Result<HistorySearchResponse, ApiError> {
        self.store
            .search_events_async(&self.tenant_id, owner_user_id, request)
            .await
    }

    pub(crate) async fn get_event(
        &self,
        owner_user_id: &str,
        event_id: &str,
    ) -> Result<HistoryEvent, ApiError> {
        self.store
            .get_event_async(&self.tenant_id, owner_user_id, event_id)
            .await
    }

    pub(crate) async fn timeline(
        &self,
        owner_user_id: Option<&str>,
        request: TimelineQueryRequest,
    ) -> Result<TimelineResponse, ApiError> {
        self.store
            .timeline_async(&self.tenant_id, owner_user_id, request)
            .await
    }
}
