use std::sync::Arc;

use crate::{
    app::{log_ingest_failure, IngestTaskManager},
    config::Config,
    error::{safe_value_fingerprint, ApiError},
    http_boundary::RequestDeadline,
    models::{IngestTask, IngestTaskRequest, IngestTaskResult},
    parser::StagedUpload,
    runtime::RuntimeSupervisor,
    store::Store,
};

#[derive(Clone)]
pub(crate) struct IngestService {
    config: Arc<Config>,
    store: Store,
    ingest_manager: IngestTaskManager,
    runtime: RuntimeSupervisor,
}

#[derive(Clone, Copy)]
pub(crate) enum IngestTerminalTransition {
    Failed,
    Interrupted,
}

impl IngestService {
    pub(crate) fn new(
        config: Arc<Config>,
        store: Store,
        ingest_manager: IngestTaskManager,
        runtime: RuntimeSupervisor,
    ) -> Self {
        Self {
            config,
            store,
            ingest_manager,
            runtime,
        }
    }

    pub(crate) fn ensure_async_available(&self) -> Result<(), ApiError> {
        self.ingest_manager.ensure_available()
    }

    pub(crate) async fn submit(
        &self,
        request: IngestTaskRequest,
        staged_upload: Option<StagedUpload>,
        deadline: RequestDeadline,
    ) -> Result<IngestTask, ApiError> {
        self.ingest_manager
            .submit(
                self.store.clone(),
                self.config.tenant_id.clone(),
                request,
                staged_upload,
                (*self.config).clone(),
                deadline,
            )
            .await
    }

    pub(crate) fn task(
        &self,
        task_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<IngestTask, ApiError> {
        self.store
            .get_ingest_task(task_id, owner_user_id, include_all_private)
    }

    pub(crate) fn task_result(
        &self,
        task_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<IngestTaskResult, ApiError> {
        self.store
            .get_ingest_task_result(task_id, owner_user_id, include_all_private)
    }

    pub(crate) async fn ingest_sync<F>(
        &self,
        request: IngestTaskRequest,
        staged_upload: Option<StagedUpload>,
        on_task_created: F,
    ) -> Result<IngestTaskResult, ApiError>
    where
        F: FnOnce(&str),
    {
        let config = (*self.config).clone();
        self.store
            .ingest_file_sync_async(
                &self.config.tenant_id,
                request,
                staged_upload,
                &config,
                on_task_created,
            )
            .await
    }

    pub(crate) fn supervise_terminal_transition(
        &self,
        task_id: String,
        transition: IngestTerminalTransition,
        message: &'static str,
    ) {
        let store = self.store.clone();
        let rejected_task_id = task_id.clone();
        if !self.runtime.spawn(async move {
            let result = match transition {
                IngestTerminalTransition::Failed => {
                    store.mark_ingest_task_failed_async(&task_id).await
                }
                IngestTerminalTransition::Interrupted => {
                    store.mark_ingest_task_interrupted_async(&task_id).await
                }
            };
            if let Err(err) = result {
                log_ingest_failure(&task_id, &err, message);
            }
        }) {
            tracing::warn!(
                task_fingerprint = %safe_value_fingerprint("ingest_task_id", &rejected_task_id),
                "ingest runtime closed before terminal task persistence could be supervised"
            );
        }
    }
}
