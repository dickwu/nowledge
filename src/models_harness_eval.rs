use super::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessComponent {
    pub id: String,
    pub tenant_id: String,
    pub display_name: String,
    pub component_kind: String,
    pub description: String,
    pub status: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_revision_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessComponentRevision {
    pub id: String,
    pub tenant_id: String,
    pub component_id: String,
    pub iteration: u32,
    pub manifest_id: String,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub content: Value,
    pub status: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessChangeManifest {
    pub id: String,
    pub tenant_id: String,
    pub iteration: u32,
    #[serde(rename = "type")]
    pub change_type: String,
    pub component_id: String,
    #[serde(default)]
    pub files: Vec<String>,
    pub failure_pattern: String,
    pub root_cause: String,
    pub targeted_fix: String,
    #[serde(default)]
    pub predicted_fixes: Vec<String>,
    #[serde(default)]
    pub risk_cases: Vec<String>,
    #[serde(default)]
    pub expected_metric_deltas: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_eval_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_eval_run_id: Option<String>,
    pub why_this_component: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessChangeVerdict {
    pub id: String,
    pub tenant_id: String,
    pub change_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub eval_run_id: Option<String>,
    pub verdict: String,
    #[serde(default)]
    pub predicted_fixes_confirmed: Vec<String>,
    #[serde(default)]
    pub risk_cases_regressed: Vec<String>,
    #[serde(default)]
    pub observed_metric_deltas: Value,
    #[serde(default)]
    pub evidence: Value,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateHarnessComponentRevisionRequest {
    #[serde(default)]
    pub manifest_id: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub content: Value,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RollbackHarnessComponentRequest {
    #[serde(default)]
    pub target_revision_id: Option<String>,
    #[serde(default)]
    pub manifest_id: Option<String>,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessComponentDetail {
    pub component: HarnessComponent,
    pub revisions: Vec<HarnessComponentRevision>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarnessRollbackResponse {
    pub component: HarnessComponent,
    pub active_revision: HarnessComponentRevision,
    pub history_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateHarnessChangeManifestRequest {
    #[serde(default)]
    pub id: Option<String>,
    pub iteration: Option<u32>,
    #[serde(rename = "type", default)]
    pub change_type: Option<String>,
    #[serde(default)]
    pub component_id: Option<String>,
    #[serde(default)]
    pub files: Vec<String>,
    #[serde(default)]
    pub failure_pattern: Option<String>,
    #[serde(default)]
    pub root_cause: Option<String>,
    #[serde(default)]
    pub targeted_fix: Option<String>,
    #[serde(default)]
    pub predicted_fixes: Vec<String>,
    #[serde(default)]
    pub risk_cases: Vec<String>,
    #[serde(default)]
    pub expected_metric_deltas: Value,
    #[serde(default)]
    pub baseline_eval_run_id: Option<String>,
    #[serde(default)]
    pub candidate_eval_run_id: Option<String>,
    #[serde(default)]
    pub why_this_component: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateHarnessChangeVerdictRequest {
    #[serde(default)]
    pub eval_run_id: Option<String>,
    #[serde(default)]
    pub observed_metric_deltas: Value,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateRagEvalCaseRequest {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub owner_user_id: Option<String>,
    #[serde(default)]
    pub question: Option<String>,
    #[serde(default)]
    pub expected_context_uris: Vec<String>,
    #[serde(default)]
    pub expected_source_document_uris: Vec<String>,
    #[serde(default)]
    pub expected_answer_contains: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagEvalCase {
    pub id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub question: String,
    #[serde(default)]
    pub expected_context_uris: Vec<String>,
    #[serde(default)]
    pub expected_source_document_uris: Vec<String>,
    #[serde(default)]
    pub expected_answer_contains: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CreateRagEvalRunRequest {
    #[serde(default)]
    pub case_ids: Vec<String>,
    #[serde(default)]
    pub change_id: Option<String>,
    #[serde(default)]
    pub created_by: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RagEvalMetrics {
    pub pass_rate: f64,
    pub retrieval_recall_at_5: f64,
    pub citation_precision: f64,
    pub traceback_success_rate: f64,
    pub source_doc_leak_rate: f64,
    pub acl_violation_rate: f64,
    pub stale_fragment_rate: f64,
    pub state_history_consistency_rate: f64,
    pub llm_health_false_ready_rate: f64,
    pub tokens_per_answer: f64,
    pub latency_p95: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegressionGuardResult {
    pub name: String,
    pub passed: bool,
    #[serde(default)]
    pub evidence: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagEvalCaseResult {
    pub id: String,
    #[serde(default)]
    pub tenant_id: String,
    pub run_id: String,
    pub case_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner_user_id: Option<String>,
    pub status: String,
    pub question: String,
    pub trace_id: String,
    pub answer: String,
    #[serde(default)]
    pub citations: Vec<Citation>,
    #[serde(default)]
    pub retrieved_uris: Vec<String>,
    #[serde(default)]
    pub source_document_uris: Vec<String>,
    #[serde(default)]
    pub failures: Vec<String>,
    #[serde(default)]
    pub guard_failures: Vec<String>,
    #[serde(default)]
    pub metrics: Value,
    pub latency_ms: u64,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskCaseResult {
    pub case_id: String,
    pub baseline_status: String,
    pub candidate_status: String,
    pub regressed: bool,
    #[serde(default)]
    pub baseline_failures: Vec<String>,
    #[serde(default)]
    pub candidate_failures: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalDeltaReport {
    pub change_id: String,
    pub baseline_run_id: String,
    pub candidate_run_id: String,
    #[serde(default)]
    pub fixed_cases: Vec<String>,
    #[serde(default)]
    pub regressed_cases: Vec<String>,
    #[serde(default)]
    pub unchanged_failed_cases: Vec<String>,
    #[serde(default)]
    pub unchanged_passed_cases: Vec<String>,
    #[serde(default)]
    pub metric_deltas: Value,
    #[serde(default)]
    pub risk_matrix: Vec<RiskCaseResult>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailurePatternCluster {
    pub pattern: String,
    pub count: usize,
    #[serde(default)]
    pub case_ids: Vec<String>,
    pub suggested_target_component: String,
    #[serde(default)]
    pub root_cause_notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagEvalOverview {
    #[serde(default)]
    pub tenant_id: String,
    pub run_id: String,
    pub status: String,
    pub metrics: RagEvalMetrics,
    #[serde(default)]
    pub failure_patterns: Vec<FailurePatternCluster>,
    pub suggested_target_component: String,
    #[serde(default)]
    pub root_cause_notes: Vec<String>,
    pub overview_markdown: String,
    #[serde(default)]
    pub case_report_uris: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overview_source_document_uri: Option<String>,
    pub generated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RagEvalRun {
    pub id: String,
    pub tenant_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub change_id: Option<String>,
    #[serde(default)]
    pub case_ids: Vec<String>,
    #[serde(default)]
    pub result_ids: Vec<String>,
    #[serde(default)]
    pub trace_ids: Vec<String>,
    pub status: String,
    pub metrics: RagEvalMetrics,
    #[serde(default)]
    pub guard_results: Vec<RegressionGuardResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub overview_source_document_uri: Option<String>,
    #[serde(default)]
    pub report_source_document_uris: Vec<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<DateTime<Utc>>,
}
