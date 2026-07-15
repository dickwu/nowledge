use serde_json::Value;

use crate::{
    error::ApiError,
    models::{
        SessionCommitRequest, SessionCommitResponse, SessionCreateRequest, SessionMessageRequest,
        SessionResponse,
    },
    store::Store,
};

pub(crate) struct SessionService {
    store: Store,
    tenant_id: String,
}

impl SessionService {
    pub(crate) fn new(store: Store, tenant_id: String) -> Self {
        Self { store, tenant_id }
    }

    pub(crate) fn owner_id(&self, session_id: &str) -> Result<String, ApiError> {
        self.store.session_owner_id(session_id)
    }

    pub(crate) async fn create(
        &self,
        request: SessionCreateRequest,
    ) -> Result<SessionResponse, ApiError> {
        self.store
            .create_session_async(&self.tenant_id, request)
            .await
    }

    pub(crate) async fn add_message(
        &self,
        session_id: &str,
        request: SessionMessageRequest,
    ) -> Result<Value, ApiError> {
        self.store
            .add_session_message_async(&self.tenant_id, session_id, request)
            .await
    }

    pub(crate) async fn commit(
        &self,
        session_id: &str,
        request: SessionCommitRequest,
    ) -> Result<SessionCommitResponse, ApiError> {
        self.store
            .commit_session_async(&self.tenant_id, session_id, request)
            .await
    }
}
