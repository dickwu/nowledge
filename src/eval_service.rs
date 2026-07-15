use serde_json::Value;

use crate::{
    app::AppState,
    error::ApiError,
    health_service::llm_health_false_ready,
    models::{
        CreateRagEvalCaseRequest, CreateRagEvalRunRequest, RagEvalCase, RagEvalCaseResult,
        RagEvalOverview, RagEvalRun,
    },
};

pub(crate) struct EvalService {
    state: AppState,
}

impl EvalService {
    pub(crate) fn new(state: &AppState) -> Self {
        Self {
            state: state.clone(),
        }
    }

    pub(crate) async fn create_case(
        &self,
        request: CreateRagEvalCaseRequest,
    ) -> Result<RagEvalCase, ApiError> {
        self.state
            .store
            .create_eval_case_async(self.state.tenant_id(), request)
            .await
    }

    pub(crate) fn list_cases(&self) -> Result<Vec<RagEvalCase>, ApiError> {
        self.state.store.list_eval_cases()
    }

    pub(crate) async fn create_run(
        &self,
        request: CreateRagEvalRunRequest,
    ) -> Result<RagEvalRun, ApiError> {
        let llm_false_ready = llm_health_false_ready(&self.state).await;
        self.state
            .store
            .create_eval_run_async(self.state.tenant_id(), request, llm_false_ready)
            .await
    }

    pub(crate) fn run(&self, run_id: &str) -> Result<RagEvalRun, ApiError> {
        self.state.store.get_eval_run(run_id)
    }

    pub(crate) fn run_report(&self, run_id: &str) -> Result<Value, ApiError> {
        self.state.store.eval_run_report(run_id)
    }

    pub(crate) fn overview(&self, run_id: &str) -> Result<RagEvalOverview, ApiError> {
        self.state.store.eval_overview(run_id)
    }

    pub(crate) fn case_analysis(
        &self,
        run_id: &str,
        case_id: &str,
    ) -> Result<RagEvalCaseResult, ApiError> {
        self.state.store.eval_case_result(run_id, case_id)
    }
}
