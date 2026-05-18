use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Instant,
};

use serde_json::{json, Value};

use crate::{
    config::Config,
    error::ApiError,
    fragmenter::{BlockAwareFragmenter, FragmentChunk},
    models::*,
    parser::{parser_from_config, ParserInput, ParserOutput},
    repository::{repository_from_config, KnowledgeRepository},
    resolver::{EventIndexResolver, EVENT_INDEX_SCHEMA_VERSION, EVENT_SETTINGS_HASH},
    util::{
        ancestor_uris, hmac_hex, new_id, now, require_string, sanitize_slug, text_score,
        truncate_chars,
    },
};

#[derive(Clone)]
pub struct Store {
    inner: Arc<RwLock<StoreData>>,
    resolver: EventIndexResolver,
    repository: Arc<dyn KnowledgeRepository>,
}

#[derive(Default)]
struct StoreData {
    user_indexes: HashMap<(String, String), UserEventIndex>,
    events_by_index: HashMap<String, Vec<HistoryEvent>>,
    event_by_id: HashMap<String, HistoryEvent>,
    event_idempotency: HashMap<(String, String), String>,
    personal_context: HashMap<String, Vec<ContextNode>>,
    company_context: Vec<ContextNode>,
    state_items: HashMap<(String, String, String), StateItem>,
    insights: HashMap<String, InsightRecord>,
    insight_idempotency: HashMap<(String, String), String>,
    sources: HashMap<String, CompanySource>,
    source_revisions: HashMap<String, Vec<SourceRevision>>,
    source_documents: HashMap<String, SourceDocument>,
    parse_artifacts: HashMap<String, ParseArtifact>,
    parsed_blocks: HashMap<String, Vec<ParsedBlock>>,
    ingest_tasks: HashMap<String, IngestTask>,
    ingest_results: HashMap<String, IngestTaskResult>,
    preflight_decisions: HashMap<String, CompanyDocPreflightResponse>,
    datasets: HashMap<String, DatasetRecord>,
    snapshots: HashMap<String, StructuredSnapshot>,
    snapshot_idempotency: HashMap<String, String>,
    rows_by_snapshot: HashMap<String, Vec<Value>>,
    row_idempotency: HashSet<(String, String)>,
    structured_summaries: HashMap<String, Value>,
    sessions: HashMap<String, SessionRecord>,
    traces: HashMap<String, TraceRecord>,
    links: HashMap<String, KnowledgeLink>,
    link_idempotency: HashMap<(String, String), String>,
    harness_components: HashMap<String, HarnessComponent>,
    harness_revisions: HashMap<String, Vec<HarnessComponentRevision>>,
    harness_changes: HashMap<String, HarnessChangeManifest>,
    harness_verdicts: HashMap<String, HarnessChangeVerdict>,
    eval_cases: HashMap<String, RagEvalCase>,
    eval_runs: HashMap<String, RagEvalRun>,
    eval_case_results: HashMap<String, RagEvalCaseResult>,
    eval_overviews: HashMap<String, RagEvalOverview>,
}

impl StoreData {
    fn seed_harness_components(&mut self, tenant_id: &str) {
        let created_at = now();
        for (component_id, display_name, component_kind, description) in
            default_harness_components()
        {
            if self.harness_components.contains_key(component_id) {
                continue;
            }
            let revision_id = bootstrap_harness_revision_id(component_id);
            self.harness_components.insert(
                component_id.to_string(),
                HarnessComponent {
                    id: component_id.to_string(),
                    tenant_id: tenant_id.to_string(),
                    display_name: display_name.to_string(),
                    component_kind: component_kind.to_string(),
                    description: description.to_string(),
                    status: "active".to_string(),
                    current_revision_id: Some(revision_id.clone()),
                    created_at,
                    updated_at: created_at,
                },
            );
            self.harness_revisions.insert(
                component_id.to_string(),
                vec![HarnessComponentRevision {
                    id: revision_id,
                    tenant_id: tenant_id.to_string(),
                    component_id: component_id.to_string(),
                    iteration: 0,
                    manifest_id: "bootstrap".to_string(),
                    files: Vec::new(),
                    content: json!({
                        "source": "built_in_registry",
                        "invariants": [
                            "preserve public API behavior",
                            "preserve fragment-first retrieval",
                            "preserve source-document traceback"
                        ]
                    }),
                    status: "active".to_string(),
                    created_by: "system_bootstrap".to_string(),
                    created_at,
                }],
            );
        }
    }
}

#[derive(Debug, Clone)]
pub struct ContextSearchOutcome {
    pub response: ContextSearchResponse,
    pub trace: TraceRecord,
    pub nodes: Vec<ContextNode>,
}

#[derive(Debug, Clone)]
struct DocumentIngestResult {
    source_id: String,
    source_document_uri: String,
    fragment_uris: Vec<String>,
}

impl Store {
    pub fn new(config: &Config) -> Self {
        let mut data = StoreData::default();
        data.seed_harness_components(&config.tenant_id);
        Self {
            inner: Arc::new(RwLock::new(data)),
            resolver: EventIndexResolver::new(config.index_hash_secret.clone()),
            repository: repository_from_config(config),
        }
    }

    pub fn resolver(&self) -> &EventIndexResolver {
        &self.resolver
    }

    pub fn backend_name(&self) -> &'static str {
        self.repository.backend_name()
    }

    pub fn list_harness_components(&self) -> Result<Vec<HarnessComponent>, ApiError> {
        let data = self.read()?;
        let mut components = data
            .harness_components
            .values()
            .cloned()
            .collect::<Vec<_>>();
        components.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(components)
    }

    pub fn harness_component_detail(
        &self,
        component_id: &str,
    ) -> Result<HarnessComponentDetail, ApiError> {
        let data = self.read()?;
        let component = data
            .harness_components
            .get(component_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("harness component not found"))?;
        let mut revisions = data
            .harness_revisions
            .get(component_id)
            .cloned()
            .unwrap_or_default();
        revisions.sort_by_key(|revision| revision.iteration);
        Ok(HarnessComponentDetail {
            component,
            revisions,
        })
    }

    pub async fn create_harness_change_async(
        &self,
        tenant_id: &str,
        req: CreateHarnessChangeManifestRequest,
    ) -> Result<HarnessChangeManifest, ApiError> {
        let component_id = require_string(req.component_id, "component_id")?;
        let change_type = require_string(req.change_type, "type")?;
        if !matches!(change_type.as_str(), "new" | "improvement" | "rollback") {
            return Err(ApiError::bad_request(
                "type must be one of new, improvement, rollback",
            ));
        }
        let failure_pattern = require_string(req.failure_pattern, "failure_pattern")?;
        let root_cause = require_string(req.root_cause, "root_cause")?;
        let targeted_fix = require_string(req.targeted_fix, "targeted_fix")?;
        let why_this_component = require_string(req.why_this_component, "why_this_component")?;
        let created_by = req.created_by.unwrap_or_else(|| "admin".to_string());
        let change = HarnessChangeManifest {
            id: req.id.unwrap_or_else(|| new_id("hchange")),
            tenant_id: tenant_id.to_string(),
            iteration: req.iteration.unwrap_or(1),
            change_type,
            component_id,
            files: req.files,
            failure_pattern,
            root_cause,
            targeted_fix,
            predicted_fixes: req.predicted_fixes,
            risk_cases: req.risk_cases,
            expected_metric_deltas: req.expected_metric_deltas,
            why_this_component,
            created_by,
            created_at: now(),
            status: "proposed".to_string(),
        };
        {
            let mut data = self.write()?;
            if !data.harness_components.contains_key(&change.component_id) {
                return Err(ApiError::not_found("harness component not found"));
            }
            if data.harness_changes.contains_key(&change.id) {
                return Err(ApiError::conflict("harness change already exists"));
            }
            data.harness_changes
                .insert(change.id.clone(), change.clone());
        }
        let _ = self
            .repository
            .upsert_harness_changes(std::slice::from_ref(&change))
            .await?;
        Ok(change)
    }

    pub fn list_harness_changes(&self) -> Result<Vec<HarnessChangeManifest>, ApiError> {
        let data = self.read()?;
        let mut changes = data.harness_changes.values().cloned().collect::<Vec<_>>();
        changes.sort_by_key(|change| Reverse(change.created_at));
        Ok(changes)
    }

    pub fn harness_change(&self, change_id: &str) -> Result<HarnessChangeManifest, ApiError> {
        let data = self.read()?;
        data.harness_changes
            .get(change_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("harness change not found"))
    }

    pub async fn create_harness_component_revision_async(
        &self,
        tenant_id: &str,
        component_id: &str,
        req: CreateHarnessComponentRevisionRequest,
    ) -> Result<HarnessComponentRevision, ApiError> {
        let manifest_id = require_string(req.manifest_id, "manifest_id")?;
        let created_by = req.created_by.unwrap_or_else(|| "admin".to_string());
        let (component, revisions, change, revision) = {
            let mut data = self.write()?;
            if !data.harness_components.contains_key(component_id) {
                return Err(ApiError::not_found("harness component not found"));
            }
            let change = data
                .harness_changes
                .get(&manifest_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("harness change manifest not found"))?;
            if change.component_id != component_id {
                return Err(ApiError::bad_request(
                    "manifest component_id does not match revision component_id",
                ));
            }
            let revisions = data
                .harness_revisions
                .entry(component_id.to_string())
                .or_default();
            let iteration = revisions
                .iter()
                .map(|revision| revision.iteration)
                .max()
                .unwrap_or(0)
                + 1;
            for existing in revisions.iter_mut() {
                if existing.status == "active" {
                    existing.status = "superseded".to_string();
                }
            }
            let revision = HarnessComponentRevision {
                id: new_id("hrev"),
                tenant_id: tenant_id.to_string(),
                component_id: component_id.to_string(),
                iteration,
                manifest_id: manifest_id.clone(),
                files: if req.files.is_empty() {
                    change.files.clone()
                } else {
                    req.files
                },
                content: req.content,
                status: "active".to_string(),
                created_by,
                created_at: now(),
            };
            revisions.push(revision.clone());
            let revisions = revisions.clone();
            let component = data
                .harness_components
                .get_mut(component_id)
                .ok_or_else(|| ApiError::not_found("harness component not found"))?;
            component.current_revision_id = Some(revision.id.clone());
            component.status = "active".to_string();
            component.updated_at = now();
            let component = component.clone();
            let change = data
                .harness_changes
                .get_mut(&manifest_id)
                .ok_or_else(|| ApiError::not_found("harness change manifest not found"))?;
            change.status = "applied".to_string();
            let change = change.clone();
            (component, revisions, change, revision)
        };
        let _ = self
            .repository
            .upsert_harness_components(std::slice::from_ref(&component), &revisions)
            .await?;
        let _ = self
            .repository
            .upsert_harness_changes(std::slice::from_ref(&change))
            .await?;
        Ok(revision)
    }

    pub async fn rollback_harness_component_async(
        &self,
        tenant_id: &str,
        component_id: &str,
        req: RollbackHarnessComponentRequest,
    ) -> Result<HarnessRollbackResponse, ApiError> {
        let created_by = req.created_by.unwrap_or_else(|| "admin".to_string());
        let (component, revisions, manifest, active_revision) = {
            let mut data = self.write()?;
            let component = data
                .harness_components
                .get(component_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("harness component not found"))?;
            let current_revision_id = component.current_revision_id.clone();
            let revisions = data
                .harness_revisions
                .get_mut(component_id)
                .ok_or_else(|| ApiError::not_found("harness revisions not found"))?;
            let target_revision_id = req
                .target_revision_id
                .clone()
                .or_else(|| previous_revision_id(revisions, current_revision_id.as_deref()))
                .ok_or_else(|| ApiError::bad_request("target_revision_id is required"))?;
            if !revisions
                .iter()
                .any(|revision| revision.id == target_revision_id)
            {
                return Err(ApiError::not_found("target harness revision not found"));
            }
            for revision in revisions.iter_mut() {
                if revision.id == target_revision_id {
                    revision.status = "active".to_string();
                } else if Some(revision.id.as_str()) == current_revision_id.as_deref() {
                    revision.status = "rolled_back".to_string();
                } else if revision.status == "active" {
                    revision.status = "superseded".to_string();
                }
            }
            let active_revision = revisions
                .iter()
                .find(|revision| revision.id == target_revision_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("target harness revision not found"))?;
            let revisions = revisions.clone();
            let component = data
                .harness_components
                .get_mut(component_id)
                .ok_or_else(|| ApiError::not_found("harness component not found"))?;
            component.current_revision_id = Some(target_revision_id);
            component.status = "active".to_string();
            component.updated_at = now();
            let component = component.clone();
            let manifest = if let Some(manifest_id) = req.manifest_id.as_deref() {
                let manifest = data
                    .harness_changes
                    .get_mut(manifest_id)
                    .ok_or_else(|| ApiError::not_found("harness change manifest not found"))?;
                manifest.status = "rollback".to_string();
                Some(manifest.clone())
            } else {
                None
            };
            (component, revisions, manifest, active_revision)
        };

        let history = self.append_internal_event(
            tenant_id,
            "company",
            "harness.component.rollback",
            "harness_component",
            component_id,
            req.reason.unwrap_or_else(|| {
                format!("Harness component {component_id} rolled back by {created_by}")
            }),
            json!({
                "component_id": component_id,
                "active_revision_id": active_revision.id,
                "created_by": created_by,
                "scope": "harness_only"
            }),
        )?;
        let _ = self.persist_history_event_by_id(&history.event.id).await?;
        let _ = self
            .repository
            .upsert_harness_components(std::slice::from_ref(&component), &revisions)
            .await?;
        if let Some(manifest) = &manifest {
            let _ = self
                .repository
                .upsert_harness_changes(std::slice::from_ref(manifest))
                .await?;
        }
        Ok(HarnessRollbackResponse {
            component,
            active_revision,
            history_event_id: history.event.id,
        })
    }

    pub async fn create_harness_verdict_async(
        &self,
        tenant_id: &str,
        change_id: &str,
        req: CreateHarnessChangeVerdictRequest,
    ) -> Result<HarnessChangeVerdict, ApiError> {
        let created_by = req.created_by.unwrap_or_else(|| "admin".to_string());
        let (change, run, overview) = {
            let data = self.read()?;
            let change = data
                .harness_changes
                .get(change_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("harness change not found"))?;
            let run = req
                .eval_run_id
                .as_deref()
                .and_then(|run_id| data.eval_runs.get(run_id).cloned())
                .or_else(|| latest_eval_run_for_change(&data, change_id));
            let overview = run
                .as_ref()
                .and_then(|run| data.eval_overviews.get(&run.id).cloned());
            (change, run, overview)
        };
        let observed_metric_deltas = if req.observed_metric_deltas.is_null() {
            overview
                .as_ref()
                .map(|overview| metrics_to_value(&overview.metrics))
                .unwrap_or_else(|| json!({}))
        } else {
            req.observed_metric_deltas
        };
        let evidence_text = verdict_evidence_text(run.as_ref(), overview.as_ref());
        let risk_cases_regressed = change
            .risk_cases
            .iter()
            .filter(|risk| contains_folded(&evidence_text, risk))
            .cloned()
            .collect::<Vec<_>>();
        let predicted_fixes_confirmed = if run.as_ref().is_some_and(|run| run.status == "passed") {
            change.predicted_fixes.clone()
        } else {
            Vec::new()
        };
        let verdict = if !risk_cases_regressed.is_empty() {
            if predicted_fixes_confirmed.is_empty() {
                "rollback_and_pivot"
            } else {
                "rollback"
            }
        } else if run
            .as_ref()
            .is_some_and(|run| run.status == "passed" && run.metrics.pass_rate >= 1.0)
        {
            "keep"
        } else {
            "improve"
        }
        .to_string();
        let verdict_record = HarnessChangeVerdict {
            id: new_id("hverdict"),
            tenant_id: tenant_id.to_string(),
            change_id: change_id.to_string(),
            eval_run_id: run.as_ref().map(|run| run.id.clone()),
            verdict,
            predicted_fixes_confirmed,
            risk_cases_regressed,
            observed_metric_deltas,
            evidence: json!({
                "change_failure_pattern": change.failure_pattern,
                "eval_run_status": run.as_ref().map(|run| run.status.clone()),
                "overview": overview.as_ref().map(|overview| overview.overview_markdown.clone())
            }),
            created_by,
            created_at: now(),
        };
        {
            let mut data = self.write()?;
            if let Some(change) = data.harness_changes.get_mut(change_id) {
                change.status = verdict_record.verdict.clone();
            }
            data.harness_verdicts
                .insert(verdict_record.id.clone(), verdict_record.clone());
        }
        let _ = self
            .repository
            .upsert_harness_verdicts(std::slice::from_ref(&verdict_record))
            .await?;
        Ok(verdict_record)
    }

    pub fn create_eval_case(
        &self,
        tenant_id: &str,
        req: CreateRagEvalCaseRequest,
    ) -> Result<RagEvalCase, ApiError> {
        let question = require_string(req.question, "question")?;
        let case = RagEvalCase {
            id: req.id.unwrap_or_else(|| new_id("evalcase")),
            tenant_id: tenant_id.to_string(),
            owner_user_id: req.owner_user_id,
            question,
            expected_context_uris: req.expected_context_uris,
            expected_source_document_uris: req.expected_source_document_uris,
            expected_answer_contains: req.expected_answer_contains,
            tags: req.tags,
            metadata: req.metadata,
            created_at: now(),
        };
        let mut data = self.write()?;
        if data.eval_cases.contains_key(&case.id) {
            return Err(ApiError::conflict("eval case already exists"));
        }
        data.eval_cases.insert(case.id.clone(), case.clone());
        Ok(case)
    }

    pub fn list_eval_cases(&self) -> Result<Vec<RagEvalCase>, ApiError> {
        let data = self.read()?;
        let mut cases = data.eval_cases.values().cloned().collect::<Vec<_>>();
        cases.sort_by_key(|case| case.created_at);
        Ok(cases)
    }

    pub async fn create_eval_run_async(
        &self,
        tenant_id: &str,
        req: CreateRagEvalRunRequest,
        llm_health_false_ready: bool,
    ) -> Result<RagEvalRun, ApiError> {
        let cases = {
            let data = self.read()?;
            let mut cases = if req.case_ids.is_empty() {
                data.eval_cases.values().cloned().collect::<Vec<_>>()
            } else {
                req.case_ids
                    .iter()
                    .map(|case_id| {
                        data.eval_cases
                            .get(case_id)
                            .cloned()
                            .ok_or_else(|| ApiError::not_found("eval case not found"))
                    })
                    .collect::<Result<Vec<_>, _>>()?
            };
            cases.sort_by_key(|case| case.created_at);
            cases
        };
        if cases.is_empty() {
            return Err(ApiError::bad_request("at least one eval case is required"));
        }

        let run_id = new_id("evalrun");
        let mut results = Vec::new();
        let mut trace_ids = Vec::new();
        for case in &cases {
            let started = Instant::now();
            let outcome = self
                .search_context_async(
                    tenant_id,
                    ContextSearchRequest {
                        query: Some(case.question.clone()),
                        owner_user_id: case.owner_user_id.clone(),
                        limit: 5,
                        ..ContextSearchRequest::default()
                    },
                )
                .await?;
            let latency_ms = started.elapsed().as_millis() as u64;
            let answer = self.answer_from_context(outcome.clone());
            trace_ids.push(answer.trace_id.clone());
            results.push(
                self.evaluate_case_result(tenant_id, &run_id, case, &outcome, answer, latency_ms)?,
            );
        }

        let guard_results =
            self.regression_guard_results(tenant_id, &results, llm_health_false_ready)?;
        let mut metrics = aggregate_eval_metrics(&results);
        metrics.llm_health_false_ready_rate = if llm_health_false_ready { 1.0 } else { 0.0 };
        metrics.state_history_consistency_rate = if guard_results
            .iter()
            .filter(|guard| {
                guard.name == "state_change_writes_history_event"
                    || guard.name == "current_state_has_history_evidence"
            })
            .all(|guard| guard.passed)
        {
            1.0
        } else {
            0.0
        };
        let guard_failed = guard_results.iter().any(|guard| !guard.passed);
        let status = if guard_failed || results.iter().any(|result| result.status == "failed") {
            "failed"
        } else {
            "passed"
        }
        .to_string();
        let mut run = RagEvalRun {
            id: run_id.clone(),
            tenant_id: tenant_id.to_string(),
            change_id: req.change_id.clone(),
            case_ids: cases.iter().map(|case| case.id.clone()).collect(),
            result_ids: results.iter().map(|result| result.id.clone()).collect(),
            trace_ids,
            status: status.clone(),
            metrics: metrics.clone(),
            guard_results,
            overview_source_document_uri: None,
            report_source_document_uris: Vec::new(),
            created_by: req.created_by.unwrap_or_else(|| "admin".to_string()),
            created_at: now(),
            completed_at: Some(now()),
        };
        let mut overview = build_eval_overview(&run, &results);
        {
            let mut data = self.write()?;
            self.write_eval_reports_locked(&mut data, tenant_id, &mut run, &mut overview, &results);
            for result in &results {
                data.eval_case_results
                    .insert(result.id.clone(), result.clone());
            }
            data.eval_overviews.insert(run.id.clone(), overview);
            data.eval_runs.insert(run.id.clone(), run.clone());
        }
        let source_documents = self.source_documents_for_scope(tenant_id, None)?;
        let _ = self
            .repository
            .upsert_source_documents(&source_documents)
            .await?;
        let company_nodes = self.context_nodes_for_index("rag_company_context")?;
        let _ = self
            .repository
            .upsert_context_nodes("rag_company_context", &company_nodes)
            .await?;
        Ok(run)
    }

    pub fn get_eval_run(&self, run_id: &str) -> Result<RagEvalRun, ApiError> {
        let data = self.read()?;
        data.eval_runs
            .get(run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("eval run not found"))
    }

    pub fn eval_run_report(&self, run_id: &str) -> Result<Value, ApiError> {
        let data = self.read()?;
        let run = data
            .eval_runs
            .get(run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("eval run not found"))?;
        let overview = data
            .eval_overviews
            .get(run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("eval overview not found"))?;
        let results = run
            .result_ids
            .iter()
            .filter_map(|result_id| data.eval_case_results.get(result_id).cloned())
            .collect::<Vec<_>>();
        Ok(json!({
            "run": run,
            "overview": overview,
            "case_results": results
        }))
    }

    pub fn eval_overview(&self, run_id: &str) -> Result<RagEvalOverview, ApiError> {
        let data = self.read()?;
        data.eval_overviews
            .get(run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("eval overview not found"))
    }

    pub fn eval_case_result(
        &self,
        run_id: &str,
        case_id: &str,
    ) -> Result<RagEvalCaseResult, ApiError> {
        let data = self.read()?;
        data.eval_case_results
            .values()
            .find(|result| result.run_id == run_id && result.case_id == case_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("eval case result not found"))
    }

    pub fn usage_snapshot(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        include_global: bool,
    ) -> Result<Value, ApiError> {
        let owner_hash = owner_user_id.map(|owner| self.resolver.user_hash(owner));
        let personal_context_index_uid = owner_user_id
            .map(|owner| {
                self.resolver
                    .resolve(tenant_id, owner, false, true)
                    .map(|routing| routing.personal_context_index_uid)
            })
            .transpose()?;
        let data = self.read()?;
        let owner_matches =
            |owner: &str| include_global || owner_user_id.is_some_and(|target| target == owner);
        let tenant_matches = |value: &str| value == tenant_id || value == "default";

        let event_count = data
            .event_by_id
            .values()
            .filter(|event| event.tenant_id == tenant_id && owner_matches(&event.owner_user_id))
            .count();
        let event_index_count = data
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .filter(|index| {
                include_global
                    || owner_hash
                        .as_deref()
                        .is_some_and(|hash| hash == index.owner_user_id_hash)
            })
            .count();
        let company_nodes = data
            .company_context
            .iter()
            .filter(|node| tenant_matches(&node.tenant_id) && node.status == "active")
            .count();
        let private_nodes = if include_global {
            data.personal_context
                .values()
                .flatten()
                .filter(|node| tenant_matches(&node.tenant_id) && node.status == "active")
                .count()
        } else {
            personal_context_index_uid
                .as_deref()
                .and_then(|uid| data.personal_context.get(uid))
                .map(|nodes| {
                    nodes
                        .iter()
                        .filter(|node| tenant_matches(&node.tenant_id) && node.status == "active")
                        .count()
                })
                .unwrap_or(0)
        };
        let snapshot_ids = data
            .snapshots
            .values()
            .filter(|snapshot| owner_matches(&snapshot.owner_user_id))
            .map(|snapshot| snapshot.id.clone())
            .collect::<HashSet<_>>();
        let snapshot_count = snapshot_ids.len();
        let row_count = data
            .rows_by_snapshot
            .iter()
            .filter(|(snapshot_id, _)| include_global || snapshot_ids.contains(*snapshot_id))
            .map(|(_, rows)| rows.len())
            .sum::<usize>();
        let summary_count = data
            .structured_summaries
            .values()
            .filter(|summary| {
                summary
                    .get("owner_user_id")
                    .and_then(Value::as_str)
                    .is_some_and(owner_matches)
            })
            .count();
        let structured_state_count = data
            .state_items
            .values()
            .filter(|item| {
                item.tenant_id == tenant_id
                    && item.state_type == "structured_summary"
                    && owner_matches(&item.owner_user_id)
            })
            .count();
        let trace_count = data
            .traces
            .values()
            .filter(|trace| trace.tenant_id == tenant_id)
            .filter(|trace| {
                include_global
                    || trace
                        .owner_user_id
                        .as_deref()
                        .is_some_and(|owner| owner_user_id == Some(owner))
            })
            .count();
        let link_count = data
            .links
            .values()
            .filter(|link| link.tenant_id == tenant_id)
            .filter(|link| {
                if include_global {
                    true
                } else {
                    link.owner_user_id
                        .as_deref()
                        .is_none_or(|owner| owner_user_id == Some(owner))
                }
            })
            .count();
        let owner_option_matches = |owner: Option<&str>| {
            include_global || owner_user_id.is_some_and(|target| owner == Some(target))
        };
        let ingest_tasks = data
            .ingest_tasks
            .values()
            .filter(|task| task.tenant_id == tenant_id)
            .filter(|task| owner_option_matches(task.owner_user_id.as_deref()))
            .collect::<Vec<_>>();
        let parse_artifact_count = data
            .parse_artifacts
            .values()
            .filter(|artifact| artifact.tenant_id == tenant_id)
            .filter(|artifact| owner_option_matches(artifact.owner_user_id.as_deref()))
            .count();
        let parsed_block_count = data
            .parsed_blocks
            .iter()
            .filter(|(uri, _)| {
                data.source_documents.get(*uri).is_some_and(|document| {
                    document.tenant_id == tenant_id
                        && owner_option_matches(document.owner_user_id.as_deref())
                })
            })
            .map(|(_, blocks)| blocks.len())
            .sum::<usize>();
        let sessions = data
            .sessions
            .values()
            .filter(|session| owner_matches(&session.owner_user_id))
            .collect::<Vec<_>>();
        let message_count = sessions
            .iter()
            .map(|session| session.messages.len())
            .sum::<usize>();

        Ok(json!({
            "generated_at": now(),
            "scope": {
                "tenant_id": tenant_id,
                "owner_user_id": owner_user_id,
                "global": include_global
            },
            "providers": {
                "nowledge_api": {
                    "store_backend": self.backend_name(),
                    "run_scope": if include_global { "global" } else { "owner" }
                },
                "history_events": {
                    "event_count": event_count,
                    "user_event_index_count": event_index_count
                },
                "contextfs": {
                    "company_context_node_count": company_nodes,
                    "private_context_node_count": private_nodes,
                    "context_node_count": company_nodes + private_nodes
                },
                "rag": {
                    "trace_count": trace_count
                },
                "link_graph": {
                    "link_count": link_count
                },
                "ingest": {
                    "task_count": ingest_tasks.len(),
                    "queued": ingest_tasks.iter().filter(|task| task.state == "queued").count(),
                    "parsing": ingest_tasks.iter().filter(|task| task.state == "parsing").count(),
                    "parsed": ingest_tasks.iter().filter(|task| task.state == "parsed").count(),
                    "fragmenting": ingest_tasks.iter().filter(|task| task.state == "fragmenting").count(),
                    "indexing": ingest_tasks.iter().filter(|task| task.state == "indexing").count(),
                    "completed": ingest_tasks.iter().filter(|task| task.state == "completed").count(),
                    "failed": ingest_tasks.iter().filter(|task| task.state == "failed").count(),
                    "parse_artifact_count": parse_artifact_count,
                    "parsed_block_count": parsed_block_count
                },
                "structured_data": {
                    "dataset_count": data.datasets.len(),
                    "snapshot_count": snapshot_count,
                    "row_count": row_count,
                    "summary_count": summary_count,
                    "structured_state_item_count": structured_state_count
                },
                "sessions": {
                    "session_count": sessions.len(),
                    "message_count": message_count
                }
            }
        }))
    }

    pub async fn ensure_user_index_async(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        req: EnsureUserEventIndexRequest,
    ) -> Result<UserEventIndexResponse, ApiError> {
        let mut response = self.ensure_user_index(tenant_id, owner_user_id, req)?;
        let task_uids = self
            .repository
            .ensure_user_event_index(&response.index)
            .await?;
        response.meili_task_uids.extend(task_uids);
        Ok(response)
    }

    pub async fn append_event_async(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: AppendHistoryEventRequest,
    ) -> Result<HistoryEventResponse, ApiError> {
        let mut response = self.append_event(tenant_id, path_owner_user_id, req)?;
        if !response.duplicate {
            response.meili_task_uid = self.persist_event_to_repository(&response.event).await?;
        }
        Ok(response)
    }

    async fn persist_event_to_repository(
        &self,
        event: &HistoryEvent,
    ) -> Result<Option<String>, ApiError> {
        self.ensure_user_indexes_for_owner(&event.tenant_id, &event.owner_user_id)
            .await?;
        let task_uid = self.repository.append_event(event).await?;
        let routing = self
            .resolver
            .resolve(&event.tenant_id, &event.owner_user_id, false, true)?;
        let nodes = self.context_nodes_for_index(&routing.personal_context_index_uid)?;
        let _ = self
            .repository
            .upsert_context_nodes(&routing.personal_context_index_uid, &nodes)
            .await?;
        Ok(task_uid)
    }

    async fn ensure_user_indexes_for_owner(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
    ) -> Result<(), ApiError> {
        let index = self.get_user_index(tenant_id, owner_user_id)?;
        let _ = self
            .repository
            .ensure_user_event_index(&index.index)
            .await?;
        Ok(())
    }

    async fn persist_history_event_by_id(
        &self,
        event_id: &str,
    ) -> Result<Option<String>, ApiError> {
        let event = {
            let data = self.read()?;
            data.event_by_id
                .get(event_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("history event not found"))?
        };
        self.persist_event_to_repository(&event).await
    }

    pub async fn append_bulk_events_async(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: BulkHistoryEventsRequest,
    ) -> Result<BulkHistoryEventsResponse, ApiError> {
        if req.events.is_empty() {
            return Err(ApiError::bad_request("events must not be empty"));
        }

        let owner = self
            .owner_from_path_or_body(path_owner_user_id, req.events[0].owner_user_id.as_deref())?;
        let mut inserted = 0;
        let mut duplicates = 0;
        let mut event_ids = Vec::new();
        let mut routing = None;
        let mut last_task = None;

        for mut event in req.events {
            if event
                .owner_user_id
                .as_deref()
                .is_some_and(|body_owner| body_owner != owner)
            {
                return Err(ApiError::bad_request(
                    "all bulk events must match the path owner_user_id",
                ));
            }
            event.owner_user_id = Some(owner.clone());
            let response = self
                .append_event_async(tenant_id, Some(&owner), event)
                .await?;
            if response.duplicate {
                duplicates += 1;
            } else {
                inserted += 1;
            }
            event_ids.push(response.event.id);
            routing = Some(response.routing);
            last_task = response.meili_task_uid;
        }

        Ok(BulkHistoryEventsResponse {
            inserted,
            duplicates,
            event_ids,
            materialization_job_ids: Vec::new(),
            routing: routing.expect("bulk events are non-empty"),
            meili_task_uid: last_task,
        })
    }

    pub async fn search_events_async(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: HistorySearchRequest,
    ) -> Result<HistorySearchResponse, ApiError> {
        let owner_user_id =
            self.owner_from_path_or_body(path_owner_user_id, req.owner_user_id.as_deref())?;
        let routing = self
            .resolver
            .resolve(tenant_id, &owner_user_id, false, true)?;
        if let Some(hits) = self.repository.search_user_events(&routing, &req).await? {
            return Ok(HistorySearchResponse { hits, routing });
        }
        self.search_events(tenant_id, path_owner_user_id, req)
    }

    pub async fn upsert_state_fact_async(
        &self,
        tenant_id: &str,
        fact_key: &str,
        req: UpsertStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        let response = self.upsert_state_fact(tenant_id, fact_key, req)?;
        let _ = self.repository.upsert_state_item(&response.item).await?;
        let routing =
            self.resolver
                .resolve(tenant_id, &response.item.owner_user_id, false, true)?;
        let nodes = self.context_nodes_for_index(&routing.personal_context_index_uid)?;
        self.ensure_user_indexes_for_owner(tenant_id, &response.item.owner_user_id)
            .await?;
        let _ = self
            .repository
            .upsert_context_nodes(&routing.personal_context_index_uid, &nodes)
            .await?;
        let source_documents =
            self.source_documents_for_scope(tenant_id, Some(&response.item.owner_user_id))?;
        let _ = self
            .repository
            .upsert_source_documents(&source_documents)
            .await?;
        let links = self.links_for_tenant(tenant_id)?;
        let _ = self.repository.upsert_links(&links).await?;
        let _ = self
            .persist_history_event_by_id(&response.history_event_id)
            .await?;
        Ok(response)
    }

    pub async fn patch_state_fact_async(
        &self,
        tenant_id: &str,
        fact_key: &str,
        req: PatchStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        let response = self.patch_state_fact(tenant_id, fact_key, req)?;
        let _ = self.repository.upsert_state_item(&response.item).await?;
        let routing =
            self.resolver
                .resolve(tenant_id, &response.item.owner_user_id, false, true)?;
        let nodes = self.context_nodes_for_index(&routing.personal_context_index_uid)?;
        self.ensure_user_indexes_for_owner(tenant_id, &response.item.owner_user_id)
            .await?;
        let _ = self
            .repository
            .upsert_context_nodes(&routing.personal_context_index_uid, &nodes)
            .await?;
        let _ = self
            .persist_history_event_by_id(&response.history_event_id)
            .await?;
        Ok(response)
    }

    pub async fn upsert_insight_async(
        &self,
        tenant_id: &str,
        req: InsightUpsertRequest,
    ) -> Result<InsightResponse, ApiError> {
        let response = self.upsert_insight(tenant_id, req)?;
        let routing =
            self.resolver
                .resolve(tenant_id, &response.insight.owner_user_id, false, true)?;
        let nodes = self.context_nodes_for_index(&routing.personal_context_index_uid)?;
        self.ensure_user_indexes_for_owner(tenant_id, &response.insight.owner_user_id)
            .await?;
        let _ = self
            .repository
            .upsert_context_nodes(&routing.personal_context_index_uid, &nodes)
            .await?;
        let _ = self
            .persist_history_event_by_id(&response.history_event_id)
            .await?;
        Ok(response)
    }

    pub async fn patch_insight_async(
        &self,
        tenant_id: &str,
        insight_id: &str,
        req: InsightPatchRequest,
    ) -> Result<InsightResponse, ApiError> {
        let response = self.patch_insight(tenant_id, insight_id, req)?;
        let routing =
            self.resolver
                .resolve(tenant_id, &response.insight.owner_user_id, false, true)?;
        let nodes = self.context_nodes_for_index(&routing.personal_context_index_uid)?;
        self.ensure_user_indexes_for_owner(tenant_id, &response.insight.owner_user_id)
            .await?;
        let _ = self
            .repository
            .upsert_context_nodes(&routing.personal_context_index_uid, &nodes)
            .await?;
        let _ = self
            .persist_history_event_by_id(&response.history_event_id)
            .await?;
        Ok(response)
    }

    pub async fn upsert_link_async(
        &self,
        tenant_id: &str,
        req: LinkUpsertRequest,
    ) -> Result<LinkResponse, ApiError> {
        let response = self.upsert_link(tenant_id, req)?;
        let _ = self
            .repository
            .upsert_links(std::slice::from_ref(&response.link))
            .await?;
        if let Some(history_event_id) = &response.history_event_id {
            let _ = self.persist_history_event_by_id(history_event_id).await?;
        }
        Ok(response)
    }

    pub async fn create_revision_async(
        &self,
        tenant_id: &str,
        source_id: &str,
        req: CreateRevisionRequest,
    ) -> Result<CreateRevisionResponse, ApiError> {
        let response = self.create_revision(tenant_id, source_id, req)?;
        if let Some(source) = self.company_source(source_id)? {
            let _ = self.repository.upsert_company_source(&source).await?;
        }
        if let Some(revision) = self.source_revision(source_id, &response.revision_id)? {
            let _ = self.repository.upsert_source_revision(&revision).await?;
        }
        if let Some(history_event_id) = &response.history_event_id {
            let _ = self.persist_history_event_by_id(history_event_id).await?;
        }
        Ok(response)
    }

    pub async fn activate_revision_async(
        &self,
        tenant_id: &str,
        source_id: &str,
        revision_id: &str,
        req: ActivateRevisionRequest,
    ) -> Result<ActivateRevisionResponse, ApiError> {
        let response = self.activate_revision(tenant_id, source_id, revision_id, req)?;
        if let Some(source) = self.company_source(source_id)? {
            let _ = self.repository.upsert_company_source(&source).await?;
        }
        if let Some(revision) = self.source_revision(source_id, revision_id)? {
            let _ = self.repository.upsert_source_revision(&revision).await?;
        }
        let nodes = self.context_nodes_for_index("rag_company_context")?;
        let _ = self
            .repository
            .upsert_context_nodes("rag_company_context", &nodes)
            .await?;
        let source_documents = self.source_documents_for_scope(tenant_id, None)?;
        let _ = self
            .repository
            .upsert_source_documents(&source_documents)
            .await?;
        let links = self.links_for_tenant(tenant_id)?;
        let _ = self.repository.upsert_links(&links).await?;
        if let Some(history_event_id) = &response.history_event_id {
            let _ = self.persist_history_event_by_id(history_event_id).await?;
        }
        Ok(response)
    }

    pub async fn create_ingest_task_async(
        &self,
        tenant_id: &str,
        req: IngestTaskRequest,
        config: &Config,
    ) -> Result<IngestTask, ApiError> {
        self.ingest_file_sync_async(tenant_id, req, config)
            .await
            .map(|result| result.task)
    }

    pub async fn ingest_file_sync_async(
        &self,
        tenant_id: &str,
        req: IngestTaskRequest,
        config: &Config,
    ) -> Result<IngestTaskResult, ApiError> {
        let mut parser_config = config.clone();
        if let Some(provider) = req.parser_provider.as_deref() {
            parser_config.parser_provider = provider.to_string();
        }
        if let Some(backend) = req.parser_backend.as_deref() {
            parser_config.mineru_backend = backend.to_string();
        }
        if !matches!(parser_config.parser_provider.as_str(), "builtin" | "mineru") {
            return Err(ApiError::bad_request(
                "parser_provider must be builtin or mineru",
            ));
        }

        let content = req.content.clone().unwrap_or_default();
        if content.trim().is_empty() && req.content_list.is_none() && req.content_list_v2.is_none()
        {
            return Err(ApiError::bad_request(
                "content or MinerU content_list output is required",
            ));
        }

        let title = req
            .title
            .clone()
            .or_else(|| req.file_name.clone())
            .or_else(|| req.source_uri.clone())
            .unwrap_or_else(|| "Parsed document".to_string());
        let source_id = req.source_id.clone().unwrap_or_else(|| {
            format!(
                "ingest:{}",
                sanitize_slug(
                    req.source_uri
                        .as_deref()
                        .or(req.file_name.as_deref())
                        .unwrap_or(&title)
                )
            )
        });
        let revision_id = req.revision_id.clone().unwrap_or_else(|| new_id("rev"));
        let source_document_uri = req.source_document_uri.clone().unwrap_or_else(|| {
            let scope = if req.owner_user_id.is_some() {
                "user"
            } else {
                "company"
            };
            format!(
                "ctx://{scope}/ingest/{}/source/{}",
                sanitize_slug(&source_id),
                sanitize_slug(&revision_id)
            )
        });
        let parser_provider = parser_config.parser_provider.clone();
        let parser_backend = if parser_provider == "mineru" {
            parser_config.mineru_backend.clone()
        } else {
            "text".to_string()
        };
        let now = now();
        let task = IngestTask {
            task_id: new_id("ingest"),
            tenant_id: tenant_id.to_string(),
            owner_user_id: req.owner_user_id.clone(),
            source_id: source_id.clone(),
            revision_id: revision_id.clone(),
            source_document_uri: Some(source_document_uri.clone()),
            parser_provider: parser_provider.clone(),
            parser_backend: parser_backend.clone(),
            state: "queued".to_string(),
            error: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
        };
        {
            let mut data = self.write()?;
            data.ingest_tasks.insert(task.task_id.clone(), task.clone());
        }

        self.transition_ingest_task(&task.task_id, "parsing", None)?;
        let parser = parser_from_config(&parser_config);
        let parsed = match parser
            .parse(ParserInput {
                content: content.clone(),
                content_type: req.content_type.clone(),
                file_name: req.file_name.clone(),
                content_list: req.content_list.clone(),
                content_list_v2: req.content_list_v2.clone(),
                middle_json: req.middle_json.clone(),
                model_json: req.model_json.clone(),
            })
            .await
        {
            Ok(parsed) => parsed,
            Err(err) => {
                let _ = self.transition_ingest_task(&task.task_id, "failed", Some(err.to_string()));
                return Err(err);
            }
        };

        self.transition_ingest_task(&task.task_id, "parsed", None)?;
        let document_content = parsed_content(&content, &parsed);
        let checksum = req
            .checksum
            .clone()
            .unwrap_or_else(|| sha256_hex(document_content.as_bytes()));
        let artifacts = build_parse_artifacts(
            tenant_id,
            req.owner_user_id.clone(),
            &source_document_uri,
            &source_id,
            &revision_id,
            &parsed,
            &content,
        )?;
        let artifact_refs = artifacts
            .iter()
            .map(|artifact| ParseArtifactRef {
                id: artifact.id.clone(),
                artifact_kind: artifact.artifact_kind.clone(),
                uri: artifact.uri.clone(),
                checksum: Some(artifact.checksum.clone()),
            })
            .collect::<Vec<_>>();

        self.transition_ingest_task(&task.task_id, "fragmenting", None)?;
        let (index_kind, index_uid) = if let Some(owner) = req.owner_user_id.as_deref() {
            let routing = self.resolver.resolve(tenant_id, owner, false, true)?;
            ("personal".to_string(), routing.personal_context_index_uid)
        } else {
            ("company".to_string(), "rag_company_context".to_string())
        };
        let ingest = {
            let mut data = self.write()?;
            for artifact in artifacts.iter().cloned() {
                data.parse_artifacts.insert(artifact.uri.clone(), artifact);
            }
            self.write_source_document_fragments_locked(
                &mut data,
                tenant_id,
                req.owner_user_id.clone(),
                "parsed_doc",
                &source_id,
                &revision_id,
                &source_document_uri,
                &title,
                &document_content,
                &checksum,
                &index_kind,
                &index_uid,
                req.fragment_policy.as_ref(),
                &parsed.blocks,
                &artifact_refs,
            )
        };

        self.transition_ingest_task(&task.task_id, "indexing", None)?;
        if let Err(err) = self
            .persist_ingest_outputs(tenant_id, req.owner_user_id.as_deref())
            .await
        {
            let _ = self.transition_ingest_task(&task.task_id, "failed", Some(err.to_string()));
            return Err(err);
        }

        let task = self.transition_ingest_task(&task.task_id, "completed", None)?;
        let result = IngestTaskResult {
            task: task.clone(),
            source_document_uri: ingest.source_document_uri,
            source_id: ingest.source_id,
            revision_id,
            parse_artifacts: artifacts,
            parsed_blocks: parsed.blocks,
            context_uris: ingest.fragment_uris.clone(),
            fragment_uris: ingest.fragment_uris,
        };
        let mut data = self.write()?;
        data.ingest_results
            .insert(task.task_id.clone(), result.clone());
        Ok(result)
    }

    pub fn get_ingest_task(
        &self,
        task_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<IngestTask, ApiError> {
        let data = self.read()?;
        data.ingest_tasks
            .get(task_id)
            .filter(|task| ingest_task_visible(task, owner_user_id, include_all_private))
            .cloned()
            .ok_or_else(|| ApiError::not_found("ingest task not found"))
    }

    pub fn get_ingest_task_result(
        &self,
        task_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<IngestTaskResult, ApiError> {
        let data = self.read()?;
        data.ingest_results
            .get(task_id)
            .filter(|result| ingest_task_visible(&result.task, owner_user_id, include_all_private))
            .cloned()
            .ok_or_else(|| ApiError::not_found("ingest result not found"))
    }

    pub async fn create_snapshot_async(
        &self,
        tenant_id: &str,
        req: CreateStructuredSnapshotRequest,
    ) -> Result<StructuredSnapshotResponse, ApiError> {
        let response = self.create_snapshot(tenant_id, req)?;
        let _ = self
            .repository
            .upsert_structured_snapshot(&response.snapshot)
            .await?;
        let _ = self
            .persist_history_event_by_id(&response.history_event_id)
            .await?;
        Ok(response)
    }

    pub async fn bulk_rows_async(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
        req: BulkStructuredRowsRequest,
    ) -> Result<BulkStructuredRowsResponse, ApiError> {
        let response = self.bulk_rows(tenant_id, snapshot_id, req)?;
        let rows = self.snapshot_rows(snapshot_id)?;
        let _ = self.repository.upsert_structured_rows(&rows).await?;
        if let Some(snapshot) = self.snapshot(snapshot_id)? {
            let _ = self
                .repository
                .upsert_structured_snapshot(&snapshot)
                .await?;
        }
        let _ = self
            .persist_history_event_by_id(&response.history_event_id)
            .await?;
        Ok(response)
    }

    pub async fn apply_snapshot_async(
        &self,
        tenant_id: &str,
        dataset_key: &str,
        req: ApplySnapshotRequest,
    ) -> Result<ApplySnapshotResponse, ApiError> {
        let response = self.apply_snapshot(tenant_id, dataset_key, req)?;
        for summary in self.structured_summaries(&response.summary_ids)? {
            let _ = self.repository.upsert_structured_summary(&summary).await?;
        }
        let snapshot = self
            .snapshot(&response.snapshot_id)?
            .ok_or_else(|| ApiError::not_found("snapshot not found"))?;
        let routing = self
            .resolver
            .resolve(tenant_id, &snapshot.owner_user_id, false, true)?;
        let nodes = self.context_nodes_for_index(&routing.personal_context_index_uid)?;
        self.ensure_user_indexes_for_owner(tenant_id, &snapshot.owner_user_id)
            .await?;
        let _ = self
            .repository
            .upsert_context_nodes(&routing.personal_context_index_uid, &nodes)
            .await?;
        if let Some(event_id) = self.latest_event_id_for_entity(
            &snapshot.owner_user_id,
            "structured.snapshot.applied",
            "structured_snapshot",
            &response.snapshot_id,
        )? {
            let _ = self.persist_history_event_by_id(&event_id).await?;
        }
        Ok(response)
    }

    pub async fn search_context_async(
        &self,
        tenant_id: &str,
        req: ContextSearchRequest,
    ) -> Result<ContextSearchOutcome, ApiError> {
        let query = require_string(req.query.clone(), "query")?;
        let owner_user_id = req
            .owner_user_id
            .clone()
            .or_else(|| owner_from_filters(&req.filters).map(ToString::to_string));
        let limit = req.limit.max(1);
        if let Some(result) = self
            .repository
            .search_context(
                tenant_id,
                owner_user_id.as_deref(),
                &query,
                &req.mode,
                limit,
                &self.resolver,
            )
            .await?
        {
            let hits = result
                .nodes
                .iter()
                .map(|node| context_hit_from_node(node, &query))
                .collect::<Vec<_>>();
            let trace = TraceRecord {
                id: new_id("trace"),
                tenant_id: tenant_id.to_string(),
                owner_user_id,
                query,
                mode: req.mode,
                stages: result.stages.clone(),
                context_uris: hits.iter().map(|hit| hit.uri.clone()).collect(),
                created_at: now(),
            };
            let response = ContextSearchResponse {
                trace_id: trace.id.clone(),
                hits,
                stages: result.stages,
            };
            self.insert_trace(trace.clone())?;
            let _ = self.repository.upsert_trace(&trace).await?;
            return Ok(ContextSearchOutcome {
                response,
                trace,
                nodes: result.nodes,
            });
        }
        self.search_context(tenant_id, req)
    }

    pub async fn answer_rag_async(
        &self,
        tenant_id: &str,
        req: RagAnswerRequest,
    ) -> Result<RagAnswerResponse, ApiError> {
        let question = require_string(req.question.clone(), "question")?;
        let owner_user_id = req.owner_user_id.clone().or_else(|| {
            req.session_id
                .as_ref()
                .and_then(|session_id| self.session_owner(session_id).ok().flatten())
        });
        let outcome = self
            .search_context_async(
                tenant_id,
                ContextSearchRequest {
                    query: Some(question),
                    mode: req.mode,
                    owner_user_id,
                    debug: req.debug,
                    ..ContextSearchRequest::default()
                },
            )
            .await?;
        Ok(self.answer_from_context(outcome))
    }

    pub async fn commit_session_async(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionCommitRequest,
    ) -> Result<SessionCommitResponse, ApiError> {
        let response = self.commit_session(tenant_id, session_id, req)?;
        if let Some(uri) = &response.archive_uri {
            let owner = self.session_owner_id(session_id)?;
            let node = self.fs_read(tenant_id, uri, Some(&owner), false)?;
            let index_uid = node.index_uid.clone();
            self.ensure_user_indexes_for_owner(tenant_id, &owner)
                .await?;
            let _ = self
                .repository
                .upsert_context_nodes(&index_uid, &[node])
                .await?;
        }
        for event_id in &response.history_event_ids {
            let _ = self.persist_history_event_by_id(event_id).await?;
        }
        Ok(response)
    }

    pub async fn add_session_message_async(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionMessageRequest,
    ) -> Result<Value, ApiError> {
        let response = self.add_session_message(tenant_id, session_id, req)?;
        if let Some(event_id) = response
            .get("history_event_id")
            .and_then(Value::as_str)
            .filter(|event_id| !event_id.is_empty())
        {
            let _ = self.persist_history_event_by_id(event_id).await?;
        }
        Ok(response)
    }

    pub async fn debug_meili_search_async(
        &self,
        index_uid: &str,
        query: &str,
    ) -> Result<Value, ApiError> {
        if let Some(raw) = self.repository.debug_search(index_uid, query).await? {
            return Ok(raw);
        }
        self.debug_meili_search(index_uid, query)
    }

    pub async fn get_event_async(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        event_id: &str,
    ) -> Result<HistoryEvent, ApiError> {
        if let Ok(event) = self.get_event(tenant_id, owner_user_id, event_id) {
            return Ok(event);
        }
        let routing = self
            .resolver
            .resolve(tenant_id, owner_user_id, false, true)?;
        if let Some(event) = self.repository.get_event(&routing, event_id).await? {
            return Ok(event);
        }
        Err(ApiError::not_found("history event not found"))
    }

    pub async fn get_snapshot_async(
        &self,
        snapshot_id: &str,
    ) -> Result<StructuredSnapshot, ApiError> {
        if let Ok(snapshot) = self.get_snapshot(snapshot_id) {
            return Ok(snapshot);
        }
        if let Some(snapshot) = self.repository.get_snapshot(snapshot_id).await? {
            return Ok(snapshot);
        }
        Err(ApiError::not_found("snapshot not found"))
    }

    pub async fn snapshot_owner_async(&self, snapshot_id: &str) -> Result<String, ApiError> {
        if let Ok(owner) = self.snapshot_owner(snapshot_id) {
            return Ok(owner);
        }
        self.repository
            .get_snapshot(snapshot_id)
            .await?
            .map(|snapshot| snapshot.owner_user_id)
            .ok_or_else(|| ApiError::not_found("snapshot not found"))
    }

    pub async fn list_rows_async(&self, snapshot_id: &str) -> Result<Value, ApiError> {
        let memory_rows = {
            let data = self.read()?;
            data.rows_by_snapshot.get(snapshot_id).cloned()
        };
        if let Some(rows) = memory_rows {
            return Ok(json!({ "snapshot_id": snapshot_id, "rows": rows }));
        }
        if let Some(rows) = self.repository.list_rows(snapshot_id).await? {
            return Ok(json!({ "snapshot_id": snapshot_id, "rows": rows }));
        }
        Ok(json!({ "snapshot_id": snapshot_id, "rows": [] }))
    }

    pub async fn get_trace_async(&self, trace_id: &str) -> Result<TraceRecord, ApiError> {
        if let Ok(trace) = self.get_trace(trace_id) {
            return Ok(trace);
        }
        if let Some(trace) = self.repository.get_trace(trace_id).await? {
            return Ok(trace);
        }
        Err(ApiError::not_found("trace not found"))
    }

    pub async fn fs_read_async(
        &self,
        tenant_id: &str,
        uri: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextNode, ApiError> {
        if let Ok(node) = self.fs_read(tenant_id, uri, owner_user_id, include_all_private) {
            return Ok(node);
        }
        if !include_all_private {
            if let Some(node) = self
                .repository
                .read_context_node(tenant_id, owner_user_id, uri, None, &self.resolver)
                .await?
            {
                return Ok(node);
            }
            if let Some(source_document) = self
                .repository
                .read_source_document(tenant_id, owner_user_id, uri)
                .await?
            {
                return Ok(source_document_context_node(source_document));
            }
        }
        Err(ApiError::not_found("context uri not found"))
    }

    pub async fn fs_layer_async(
        &self,
        tenant_id: &str,
        uri: &str,
        layer: u8,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextNode, ApiError> {
        if let Ok(node) = self.fs_layer(tenant_id, uri, layer, owner_user_id, include_all_private) {
            return Ok(node);
        }
        if !include_all_private {
            if let Some(node) = self
                .repository
                .read_context_node(tenant_id, owner_user_id, uri, Some(layer), &self.resolver)
                .await?
            {
                return Ok(node);
            }
        }
        Err(ApiError::not_found("context layer not found"))
    }

    pub fn ensure_user_index(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        req: EnsureUserEventIndexRequest,
    ) -> Result<UserEventIndexResponse, ApiError> {
        let mut data = self.write()?;
        let (index, routing) = self.ensure_user_index_locked(
            &mut data,
            tenant_id,
            owner_user_id,
            req.schema_version.unwrap_or(EVENT_INDEX_SCHEMA_VERSION),
        )?;

        let _ = (
            req.force_reapply_settings,
            req.create_personal_context_index,
        );

        Ok(UserEventIndexResponse {
            index,
            routing,
            meili_task_uids: Vec::new(),
        })
    }

    pub fn get_user_index(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
    ) -> Result<UserEventIndexResponse, ApiError> {
        self.ensure_user_index(
            tenant_id,
            owner_user_id,
            EnsureUserEventIndexRequest::default(),
        )
    }

    pub fn list_user_indexes(&self) -> Result<ListUserEventIndexesResponse, ApiError> {
        let data = self.read()?;
        let mut indexes: Vec<_> = data.user_indexes.values().cloned().collect();
        indexes.sort_by_key(|index| index.created_at);
        Ok(ListUserEventIndexesResponse {
            indexes,
            next_cursor: None,
        })
    }

    pub fn reconcile_user_indexes(
        &self,
        tenant_id: &str,
        req: ReconcileUserEventIndexesRequest,
    ) -> Result<ReconcileUserEventIndexesResponse, ApiError> {
        let mut data = self.write()?;
        let mut created = 0;
        let mut updated_settings = 0;
        let mut indexes = Vec::new();
        let owners = if req.owner_user_ids.is_empty() {
            data.user_indexes
                .keys()
                .filter(|(tenant, _)| tenant == tenant_id)
                .map(|(_, owner)| owner.clone())
                .collect()
        } else {
            req.owner_user_ids.clone()
        };

        for owner in owners {
            if req.dry_run {
                let routing = self.resolver.resolve(tenant_id, &owner, false, true)?;
                let tenant_hash = self.resolver.tenant_hash(tenant_id);
                indexes.push(UserEventIndex {
                    id: user_event_index_id(&tenant_hash, &routing.owner_user_id_hash),
                    tenant_id: routing.tenant_id.clone(),
                    tenant_hash,
                    owner_user_id_hash: routing.owner_user_id_hash,
                    event_index_uid: routing.event_index_uid,
                    personal_context_index_uid: routing.personal_context_index_uid,
                    schema_version: routing.schema_version,
                    settings_hash: routing.settings_hash,
                    status: "dry_run".to_string(),
                    created_at: now(),
                    last_event_at: None,
                    event_count_estimate: 0,
                });
                continue;
            }

            let existed = data
                .user_indexes
                .contains_key(&(tenant_id.to_string(), owner.clone()));
            if req.create_missing || existed {
                let (index, _) = self.ensure_user_index_locked(
                    &mut data,
                    tenant_id,
                    &owner,
                    EVENT_INDEX_SCHEMA_VERSION,
                )?;
                if !existed {
                    created += 1;
                }
                if req.reapply_settings {
                    updated_settings += 1;
                }
                indexes.push(index);
            }
        }

        Ok(ReconcileUserEventIndexesResponse {
            checked: indexes.len(),
            created,
            updated_settings,
            errors: Vec::new(),
            indexes,
        })
    }

    pub fn append_event(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: AppendHistoryEventRequest,
    ) -> Result<HistoryEventResponse, ApiError> {
        let owner_user_id =
            self.owner_from_path_or_body(path_owner_user_id, req.owner_user_id.as_deref())?;
        if req.event_index_hint.is_some() {
            return Err(ApiError::bad_request(
                "event_index_hint is not accepted; event index routing is server-side",
            ));
        }

        let event_type = require_string(req.event_type, "event_type")?;
        let entity_type = require_string(req.entity_type, "entity_type")?;
        let entity_id = require_string(req.entity_id, "entity_id")?;
        let occurred_at = req
            .occurred_at
            .ok_or_else(|| ApiError::bad_request("occurred_at is required"))?;
        let observed_at = req
            .observed_at
            .ok_or_else(|| ApiError::bad_request("observed_at is required"))?;
        let source_kind = require_string(req.source_kind, "source_kind")?;
        let source_ref = req
            .source_ref
            .ok_or_else(|| ApiError::bad_request("source_ref is required"))?;

        let mut data = self.write()?;
        let (index, routing) = self.ensure_user_index_locked(
            &mut data,
            tenant_id,
            &owner_user_id,
            EVENT_INDEX_SCHEMA_VERSION,
        )?;

        let idempotency_key_hash = req
            .idempotency_key
            .as_deref()
            .map(|key| self.resolver.idempotency_hash(key));
        if let Some(hash) = &idempotency_key_hash {
            if let Some(existing_id) = data
                .event_idempotency
                .get(&(routing.event_index_uid.clone(), hash.clone()))
            {
                if let Some(event) = data.event_by_id.get(existing_id).cloned() {
                    return Ok(HistoryEventResponse {
                        event,
                        duplicate: true,
                        materialization_job_id: None,
                        routing,
                        meili_task_uid: None,
                    });
                }
            }
        }

        let event = HistoryEvent {
            id: new_id("evt"),
            event_type,
            entity_type,
            entity_id,
            occurred_at,
            observed_at,
            source_kind,
            source_ref,
            text: req.text.unwrap_or_default(),
            payload: req.payload,
            tags: req.tags,
            privacy: req.privacy,
            tenant_id: tenant_id.to_string(),
            owner_user_id: owner_user_id.clone(),
            owner_user_id_hash: routing.owner_user_id_hash.clone(),
            event_index_uid: routing.event_index_uid.clone(),
            event_index_schema_version: index.schema_version,
            idempotency_key_hash: idempotency_key_hash.clone(),
        };

        self.insert_event_locked(&mut data, &routing, event.clone(), idempotency_key_hash);
        self.write_event_context_locked(&mut data, &routing, &event);

        Ok(HistoryEventResponse {
            event,
            duplicate: false,
            materialization_job_id: Some(new_id("job")),
            routing,
            meili_task_uid: None,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn append_internal_event(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        event_type: &str,
        entity_type: &str,
        entity_id: &str,
        text: String,
        payload: Value,
    ) -> Result<HistoryEventResponse, ApiError> {
        self.append_event(
            tenant_id,
            Some(owner_user_id),
            AppendHistoryEventRequest {
                event_type: Some(event_type.to_string()),
                entity_type: Some(entity_type.to_string()),
                entity_id: Some(entity_id.to_string()),
                owner_user_id: Some(owner_user_id.to_string()),
                occurred_at: Some(now()),
                observed_at: Some(now()),
                source_kind: Some("state_api".to_string()),
                source_ref: Some(SourceRef {
                    kind: "api".to_string(),
                    id: entity_id.to_string(),
                    uri: None,
                    meta: None,
                }),
                text: Some(text),
                payload,
                tags: Vec::new(),
                privacy: "private".to_string(),
                promote_policy: "none".to_string(),
                idempotency_key: None,
                event_index_hint: None,
            },
        )
    }

    pub fn append_bulk_events(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: BulkHistoryEventsRequest,
    ) -> Result<BulkHistoryEventsResponse, ApiError> {
        if req.events.is_empty() {
            return Err(ApiError::bad_request("events must not be empty"));
        }

        let owner = self
            .owner_from_path_or_body(path_owner_user_id, req.events[0].owner_user_id.as_deref())?;
        let mut inserted = 0;
        let mut duplicates = 0;
        let mut event_ids = Vec::new();
        let mut routing = None;

        for mut event in req.events {
            if event
                .owner_user_id
                .as_deref()
                .is_some_and(|body_owner| body_owner != owner)
            {
                return Err(ApiError::bad_request(
                    "all bulk events must match the path owner_user_id",
                ));
            }
            event.owner_user_id = Some(owner.clone());
            let response = self.append_event(tenant_id, Some(&owner), event)?;
            if response.duplicate {
                duplicates += 1;
            } else {
                inserted += 1;
            }
            event_ids.push(response.event.id);
            routing = Some(response.routing);
        }

        Ok(BulkHistoryEventsResponse {
            inserted,
            duplicates,
            event_ids,
            materialization_job_ids: Vec::new(),
            routing: routing.expect("bulk events are non-empty"),
            meili_task_uid: None,
        })
    }

    pub fn search_events(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: HistorySearchRequest,
    ) -> Result<HistorySearchResponse, ApiError> {
        let owner_user_id =
            self.owner_from_path_or_body(path_owner_user_id, req.owner_user_id.as_deref())?;
        let routing = self
            .resolver
            .resolve(tenant_id, &owner_user_id, false, true)?;
        let data = self.read()?;
        let mut hits: Vec<_> = data
            .events_by_index
            .get(&routing.event_index_uid)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter(|event| {
                req.event_types.is_empty() || req.event_types.contains(&event.event_type)
            })
            .filter(|event| {
                req.entity_type
                    .as_ref()
                    .map(|v| &event.entity_type == v)
                    .unwrap_or(true)
            })
            .filter(|event| {
                req.entity_id
                    .as_ref()
                    .map(|v| &event.entity_id == v)
                    .unwrap_or(true)
            })
            .filter(|event| {
                req.from
                    .map(|from| event.occurred_at >= from)
                    .unwrap_or(true)
            })
            .filter(|event| req.to.map(|to| event.occurred_at <= to).unwrap_or(true))
            .filter(|event| {
                req.query
                    .as_deref()
                    .map(|q| text_score(&event.text, q) > 0.0)
                    .unwrap_or(true)
            })
            .collect();

        hits.sort_by_key(|event| Reverse(event.occurred_at));
        hits.truncate(req.limit.max(1));

        Ok(HistorySearchResponse { hits, routing })
    }

    pub fn get_event(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        event_id: &str,
    ) -> Result<HistoryEvent, ApiError> {
        let routing = self
            .resolver
            .resolve(tenant_id, owner_user_id, false, true)?;
        let data = self.read()?;
        data.events_by_index
            .get(&routing.event_index_uid)
            .and_then(|events| events.iter().find(|event| event.id == event_id))
            .cloned()
            .ok_or_else(|| ApiError::not_found("history event not found"))
    }

    fn latest_event_id_for_entity(
        &self,
        owner_user_id: &str,
        event_type: &str,
        entity_type: &str,
        entity_id: &str,
    ) -> Result<Option<String>, ApiError> {
        let data = self.read()?;
        Ok(data
            .event_by_id
            .values()
            .filter(|event| {
                event.owner_user_id == owner_user_id
                    && event.event_type == event_type
                    && event.entity_type == entity_type
                    && event.entity_id == entity_id
            })
            .max_by_key(|event| event.observed_at)
            .map(|event| event.id.clone()))
    }

    pub fn timeline(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: TimelineQueryRequest,
    ) -> Result<TimelineResponse, ApiError> {
        let owner_user_id =
            self.owner_from_path_or_body(path_owner_user_id, req.owner_user_id.as_deref())?;
        let search = HistorySearchRequest {
            owner_user_id: Some(owner_user_id),
            from: req.from,
            to: req.to,
            limit: req.limit,
            ..HistorySearchRequest::default()
        };
        let mut events = self.search_events(tenant_id, None, search)?.hits;
        events.sort_by_key(|event| event.occurred_at);
        Ok(TimelineResponse { events })
    }

    pub fn upsert_state_fact(
        &self,
        tenant_id: &str,
        fact_key: &str,
        req: UpsertStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        let owner_user_id = require_string(req.owner_user_id, "owner_user_id")?;
        let state_type = require_string(req.state_type, "state_type")?;
        let statement = require_string(req.statement, "statement")?;
        let value = req.value;
        let confidence = req.confidence;
        let salience = req.salience;
        let valid_from = req.valid_from;
        let valid_to = req.valid_to;
        let document = req.document.clone();
        let fragment_policy = req.fragment_policy.clone();
        let mut source_refs = req.source_refs.clone();
        let title = req
            .title
            .unwrap_or_else(|| fact_key.replace(['-', '_'], " "));
        let now = now();
        let context_uri = format!(
            "ctx://user/state/{}/{}",
            sanitize_slug(&state_type),
            sanitize_slug(fact_key)
        );
        let key = (
            tenant_id.to_string(),
            owner_user_id.clone(),
            fact_key.to_string(),
        );

        let (item, decision) = {
            let mut data = self.write()?;
            let existing = data.state_items.get(&key).cloned();
            let next_version = existing
                .as_ref()
                .map(|item| item.current_version + 1)
                .unwrap_or(1);
            let routing = self
                .resolver
                .resolve(tenant_id, &owner_user_id, false, true)?;
            if let Some(document) = document.as_ref() {
                let policy = document
                    .fragment_policy
                    .as_ref()
                    .or(fragment_policy.as_ref());
                let ingest = self.write_state_document_context_locked(
                    &mut data,
                    tenant_id,
                    &routing,
                    &owner_user_id,
                    &state_type,
                    fact_key,
                    next_version,
                    &title,
                    document,
                    policy,
                )?;
                source_refs.push(SourceRef {
                    kind: "source_document".to_string(),
                    id: ingest.source_id,
                    uri: Some(ingest.source_document_uri.clone()),
                    meta: Some(json!({
                        "source_document_uri": ingest.source_document_uri,
                        "fragment_uris": ingest.fragment_uris,
                        "content_type": document.content_type.clone(),
                        "source_uri": document.source_uri.clone()
                    })),
                });
            }
            let item = if let Some(mut item) = existing {
                item.title = title;
                item.statement = statement;
                item.value = value;
                item.confidence = confidence;
                item.salience = salience;
                item.valid_from = valid_from;
                item.valid_to = valid_to;
                item.source_refs = source_refs.clone();
                item.status = "active".to_string();
                item.current_version = next_version;
                item.updated_at = now;
                item
            } else {
                StateItem {
                    id: new_id("state"),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: owner_user_id.clone(),
                    state_type: state_type.clone(),
                    natural_key: fact_key.to_string(),
                    title,
                    statement,
                    value,
                    status: "active".to_string(),
                    confidence,
                    salience,
                    valid_from,
                    valid_to,
                    source_refs,
                    context_uri: context_uri.clone(),
                    current_version: next_version,
                    supersedes: Vec::new(),
                    created_at: now,
                    updated_at: now,
                }
            };
            let decision = if data.state_items.contains_key(&key) {
                "updated"
            } else {
                "created"
            }
            .to_string();
            data.state_items.insert(key, item.clone());
            self.write_state_context_locked(&mut data, &routing, &item);
            (item, decision)
        };

        let history = self.append_internal_event(
            tenant_id,
            &owner_user_id,
            "state.changed",
            "state_item",
            &item.id,
            format!("State fact {} was {}", fact_key, decision),
            json!({ "natural_key": fact_key, "state_type": state_type, "decision": decision }),
        )?;

        Ok(StateItemResponse {
            item,
            history_event_id: history.event.id,
            context_uri,
            decision,
        })
    }

    pub fn patch_state_fact(
        &self,
        tenant_id: &str,
        fact_key: &str,
        req: PatchStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        let key = self.resolve_state_key(tenant_id, fact_key, req.owner_user_id.as_deref())?;
        let (item, owner_user_id) = {
            let mut data = self.write()?;
            let item = data
                .state_items
                .get_mut(&key)
                .ok_or_else(|| ApiError::not_found("state item not found"))?;
            if let Some(statement) = req.statement {
                item.statement = statement;
            }
            if let Some(value) = req.value {
                item.value = value;
            }
            if let Some(confidence) = req.confidence {
                item.confidence = confidence;
            }
            if let Some(salience) = req.salience {
                item.salience = salience;
            }
            if let Some(status) = req.status {
                item.status = status;
            }
            if let Some(valid_to) = req.valid_to {
                item.valid_to = Some(valid_to);
            }
            item.current_version += 1;
            item.updated_at = now();
            let item = item.clone();
            let owner = item.owner_user_id.clone();
            let routing = self.resolver.resolve(tenant_id, &owner, false, true)?;
            self.write_state_context_locked(&mut data, &routing, &item);
            (item, owner)
        };

        let history = self.append_internal_event(
            tenant_id,
            &owner_user_id,
            "state.patched",
            "state_item",
            &item.id,
            req.patch_reason
                .unwrap_or_else(|| format!("State fact {fact_key} was patched")),
            json!({ "natural_key": fact_key }),
        )?;

        Ok(StateItemResponse {
            context_uri: item.context_uri.clone(),
            item,
            history_event_id: history.event.id,
            decision: "patched".to_string(),
        })
    }

    pub fn get_state_fact(
        &self,
        tenant_id: &str,
        fact_key: &str,
        owner_user_id: Option<&str>,
    ) -> Result<StateItemResponse, ApiError> {
        let key = self.resolve_state_key(tenant_id, fact_key, owner_user_id)?;
        let data = self.read()?;
        let item = data
            .state_items
            .get(&key)
            .cloned()
            .ok_or_else(|| ApiError::not_found("state item not found"))?;
        Ok(StateItemResponse {
            history_event_id: String::new(),
            context_uri: item.context_uri.clone(),
            item,
            decision: "read".to_string(),
        })
    }

    pub fn search_state(
        &self,
        tenant_id: &str,
        req: StateSearchRequest,
    ) -> Result<StateSearchResponse, ApiError> {
        let data = self.read()?;
        let mut hits: Vec<_> = data
            .state_items
            .values()
            .filter(|item| item.tenant_id == tenant_id)
            .filter(|item| {
                req.owner_user_id
                    .as_ref()
                    .map(|owner| &item.owner_user_id == owner)
                    .unwrap_or(true)
            })
            .filter(|item| req.status.is_empty() || item.status == req.status)
            .filter(|item| req.state_types.is_empty() || req.state_types.contains(&item.state_type))
            .filter(|item| {
                req.query
                    .as_deref()
                    .map(|q| text_score(&format!("{} {}", item.title, item.statement), q) > 0.0)
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        hits.sort_by_key(|item| Reverse(item.updated_at));
        hits.truncate(req.limit.max(1));
        Ok(StateSearchResponse { hits })
    }

    pub fn upsert_insight(
        &self,
        tenant_id: &str,
        req: InsightUpsertRequest,
    ) -> Result<InsightResponse, ApiError> {
        let owner_user_id = require_string(req.owner_user_id, "owner_user_id")?;
        let insight_type = require_string(req.insight_type, "insight_type")?;
        let title = require_string(req.title, "title")?;
        let statement = require_string(req.statement, "statement")?;
        let now = now();

        let mut data = self.write()?;
        let id = if let Some(key) = &req.idempotency_key {
            let hash = self.resolver.idempotency_hash(key);
            if let Some(id) = data
                .insight_idempotency
                .get(&(owner_user_id.clone(), hash.clone()))
                .cloned()
            {
                id
            } else {
                let id = new_id("insight");
                data.insight_idempotency
                    .insert((owner_user_id.clone(), hash), id.clone());
                id
            }
        } else {
            new_id("insight")
        };

        let context_uri = format!(
            "ctx://user/insights/{}/{}",
            sanitize_slug(&insight_type),
            sanitize_slug(&title)
        );
        let insight = InsightRecord {
            id: id.clone(),
            insight_type,
            title: title.clone(),
            statement: statement.clone(),
            status: "active".to_string(),
            confidence: req.confidence,
            salience: req.salience,
            context_uri: context_uri.clone(),
            source_refs: req.source_refs,
            owner_user_id: owner_user_id.clone(),
            privacy: req.privacy,
            created_at: now,
            updated_at: now,
        };
        data.insights.insert(id.clone(), insight.clone());
        let routing = self
            .resolver
            .resolve(tenant_id, &owner_user_id, false, true)?;
        self.write_insight_context_locked(
            &mut data,
            tenant_id,
            &routing,
            &insight,
            req.evidence_text.clone(),
        );
        drop(data);

        let history = self.append_internal_event(
            tenant_id,
            &owner_user_id,
            "insight.upserted",
            "insight",
            &id,
            req.evidence_text
                .unwrap_or_else(|| format!("Insight saved: {statement}")),
            json!({ "insight_id": id, "title": title }),
        )?;

        Ok(InsightResponse {
            insight,
            history_event_id: history.event.id,
            context_uri,
        })
    }

    pub fn patch_insight(
        &self,
        tenant_id: &str,
        insight_id: &str,
        req: InsightPatchRequest,
    ) -> Result<InsightResponse, ApiError> {
        let (insight, owner_user_id) = {
            let mut data = self.write()?;
            let insight = data
                .insights
                .get_mut(insight_id)
                .ok_or_else(|| ApiError::not_found("insight not found"))?;
            if let Some(statement) = req.statement {
                insight.statement = statement;
            }
            if let Some(status) = req.status {
                insight.status = status;
            }
            if let Some(confidence) = req.confidence {
                insight.confidence = confidence;
            }
            if let Some(salience) = req.salience {
                insight.salience = salience;
            }
            if let Some(privacy) = req.privacy {
                insight.privacy = privacy;
            }
            insight.updated_at = now();
            let insight = insight.clone();
            let owner = insight.owner_user_id.clone();
            let routing = self.resolver.resolve(tenant_id, &owner, false, true)?;
            self.write_insight_context_locked(&mut data, tenant_id, &routing, &insight, None);
            (insight, owner)
        };

        let history = self.append_internal_event(
            tenant_id,
            &owner_user_id,
            "insight.patched",
            "insight",
            insight_id,
            req.patch_reason
                .unwrap_or_else(|| format!("Insight {insight_id} was patched")),
            json!({ "insight_id": insight_id }),
        )?;

        Ok(InsightResponse {
            context_uri: insight.context_uri.clone(),
            insight,
            history_event_id: history.event.id,
        })
    }

    pub fn search_insights(
        &self,
        req: InsightSearchRequest,
    ) -> Result<InsightSearchResponse, ApiError> {
        let data = self.read()?;
        let mut hits: Vec<_> = data
            .insights
            .values()
            .filter(|insight| {
                req.owner_user_id
                    .as_ref()
                    .map(|owner| &insight.owner_user_id == owner)
                    .unwrap_or(true)
            })
            .filter(|insight| req.status.is_empty() || insight.status == req.status)
            .filter(|insight| {
                req.insight_types.is_empty() || req.insight_types.contains(&insight.insight_type)
            })
            .filter(|insight| {
                req.query
                    .as_deref()
                    .map(|q| {
                        text_score(&format!("{} {}", insight.title, insight.statement), q) > 0.0
                    })
                    .unwrap_or(true)
            })
            .cloned()
            .collect();
        hits.sort_by_key(|insight| Reverse(insight.updated_at));
        hits.truncate(req.limit.max(1));
        Ok(InsightSearchResponse { hits })
    }

    pub fn upsert_link(
        &self,
        tenant_id: &str,
        req: LinkUpsertRequest,
    ) -> Result<LinkResponse, ApiError> {
        let source_uri = canonical_link_uri(&require_string(req.source_uri, "source_uri")?);
        let target_uri = canonical_link_uri(&require_string(req.target_uri, "target_uri")?);
        if source_uri == target_uri {
            return Err(ApiError::bad_request(
                "source_uri and target_uri must refer to different context nodes",
            ));
        }
        let relation = normalize_relation(&req.relation);
        let now = now();
        let owner_scope = req.owner_user_id.clone().unwrap_or_default();
        let idempotency_hash = req
            .idempotency_key
            .as_deref()
            .map(|key| self.resolver.idempotency_hash(key));
        let natural_key = link_natural_key(
            tenant_id,
            req.owner_user_id.as_deref(),
            &source_uri,
            &target_uri,
            &relation,
        );

        let (link, decision) = {
            let mut data = self.write()?;
            if let Some(hash) = &idempotency_hash {
                if let Some(existing_id) = data
                    .link_idempotency
                    .get(&(owner_scope.clone(), hash.clone()))
                    .cloned()
                {
                    if let Some(link) = data.links.get(&existing_id).cloned() {
                        return Ok(LinkResponse {
                            link,
                            decision: "duplicate".to_string(),
                            history_event_id: None,
                        });
                    }
                }
            }

            let existing_id = data
                .links
                .values()
                .find(|link| {
                    link_natural_key(
                        &link.tenant_id,
                        link.owner_user_id.as_deref(),
                        &link.source_uri,
                        &link.target_uri,
                        &link.relation,
                    ) == natural_key
                })
                .map(|link| link.id.clone());

            let (id, created_at, decision) = if let Some(existing_id) = existing_id {
                let created_at = data
                    .links
                    .get(&existing_id)
                    .map(|link| link.created_at)
                    .unwrap_or(now);
                (existing_id, created_at, "updated".to_string())
            } else {
                (new_id("link"), now, "created".to_string())
            };

            let link = KnowledgeLink {
                id: id.clone(),
                tenant_id: tenant_id.to_string(),
                owner_user_id: req.owner_user_id.clone(),
                source_uri,
                target_uri,
                source_title: req.source_title,
                target_title: req.target_title,
                relation,
                rationale: req.rationale,
                evidence_text: req.evidence_text,
                confidence: req.confidence,
                created_by: req.created_by,
                status: "active".to_string(),
                tags: req.tags,
                created_at,
                updated_at: now,
            };
            if let Some(hash) = idempotency_hash {
                data.link_idempotency
                    .insert((owner_scope, hash), id.clone());
            }
            data.links.insert(id, link.clone());
            (link, decision)
        };

        let history_event_id = if let Some(owner_user_id) = link.owner_user_id.as_deref() {
            Some(
                self.append_internal_event(
                    tenant_id,
                    owner_user_id,
                    "link.upserted",
                    "knowledge_link",
                    &link.id,
                    format!(
                        "Link {} {} {}",
                        &link.source_uri, &link.relation, &link.target_uri
                    ),
                    json!({
                        "link_id": &link.id,
                        "source_uri": &link.source_uri,
                        "target_uri": &link.target_uri,
                        "relation": &link.relation,
                        "decision": &decision
                    }),
                )?
                .event
                .id,
            )
        } else {
            None
        };

        Ok(LinkResponse {
            link,
            decision,
            history_event_id,
        })
    }

    pub fn search_links(
        &self,
        tenant_id: &str,
        req: LinkSearchRequest,
        include_all_private: bool,
    ) -> Result<LinkSearchResponse, ApiError> {
        let target_uri = req.uri.as_deref().map(canonical_link_uri);
        let limit = req.limit.max(1);
        let data = self.read()?;
        let mut links = data
            .links
            .values()
            .filter(|link| link.tenant_id == tenant_id)
            .filter(|link| {
                if let Some(owner) = req.owner_user_id.as_deref() {
                    link.owner_user_id.as_deref().is_none()
                        || link.owner_user_id.as_deref() == Some(owner)
                } else {
                    include_all_private || link.owner_user_id.is_none()
                }
            })
            .filter(|link| req.status.is_empty() || link.status == req.status)
            .filter(|link| req.relations.is_empty() || req.relations.contains(&link.relation))
            .filter(|link| {
                target_uri
                    .as_ref()
                    .is_none_or(|uri| match req.direction.as_str() {
                        "outbound" => &link.source_uri == uri,
                        "backlinks" | "backlink" => &link.target_uri == uri,
                        _ => &link.source_uri == uri || &link.target_uri == uri,
                    })
            })
            .filter(|link| {
                req.query
                    .as_deref()
                    .map(|query| text_score(&link_search_text(link), query) > 0.0)
                    .unwrap_or(true)
            })
            .cloned()
            .collect::<Vec<_>>();
        links.sort_by_key(|link| Reverse(link.updated_at));
        links.truncate(limit);

        let (outbound, backlinks) = if let Some(uri) = target_uri {
            let mut outbound = links
                .iter()
                .filter(|link| link.source_uri == uri)
                .cloned()
                .collect::<Vec<_>>();
            let mut backlinks = links
                .iter()
                .filter(|link| link.target_uri == uri)
                .cloned()
                .collect::<Vec<_>>();
            outbound.sort_by_key(|link| Reverse(link.updated_at));
            backlinks.sort_by_key(|link| Reverse(link.updated_at));
            (outbound, backlinks)
        } else {
            (Vec::new(), Vec::new())
        };

        Ok(LinkSearchResponse {
            links,
            outbound,
            backlinks,
        })
    }

    pub fn preflight_company_doc(
        &self,
        req: CompanyDocPreflightRequest,
    ) -> Result<CompanyDocPreflightResponse, ApiError> {
        let title = req.title.unwrap_or_else(|| "Untitled".to_string());
        let source_uri = req.source_uri.unwrap_or_default();
        let preview = req.text_preview.unwrap_or_default();
        let canonical_key = sanitize_slug(&title);
        let data = self.read()?;
        let mut matched_sources = Vec::new();
        let mut best = 0.0f32;
        let mut reasons = Vec::new();

        for source in data.sources.values() {
            let mut confidence: f32 = 0.0;
            if source.source_uri == source_uri && !source_uri.is_empty() {
                confidence = 1.0;
                reasons.push("source_uri matched existing source".to_string());
            } else if source.canonical_key == canonical_key {
                confidence = confidence.max(0.9);
                reasons.push("canonical title matched existing source".to_string());
            } else {
                let score = token_similarity(&source.title, &title)
                    .max(token_similarity(&source.canonical_key, &canonical_key))
                    .max(token_similarity(&source.title, &preview));
                confidence = confidence.max(score);
            }

            if confidence >= req.similarity_threshold {
                matched_sources.push(json!({
                    "source_id": source.id,
                    "title": source.title,
                    "source_uri": source.source_uri,
                    "confidence": confidence
                }));
            }
            best = best.max(confidence);
        }
        drop(data);

        let recommended_action = if matched_sources.is_empty() {
            "create_source"
        } else {
            "update_revision"
        };
        if matched_sources.is_empty() {
            reasons.push("no similar active source crossed threshold".to_string());
        }

        let response = CompanyDocPreflightResponse {
            decision_id: new_id("preflight"),
            recommended_action: recommended_action.to_string(),
            confidence: if matched_sources.is_empty() {
                0.0
            } else {
                best
            },
            matched_sources,
            reasons,
        };

        let mut data = self.write()?;
        data.preflight_decisions
            .insert(response.decision_id.clone(), response.clone());
        Ok(response)
    }

    pub fn create_revision(
        &self,
        tenant_id: &str,
        source_id: &str,
        req: CreateRevisionRequest,
    ) -> Result<CreateRevisionResponse, ApiError> {
        if let Some(decision_id) = &req.preflight_decision_id {
            let data = self.read()?;
            if let Some(decision) = data.preflight_decisions.get(decision_id) {
                if decision.recommended_action == "update_revision" && req.force_create {
                    return Err(ApiError::conflict(
                        "preflight recommended update_revision; force_create is blocked by default",
                    ));
                }
            }
        }

        let title = req.title.unwrap_or_else(|| source_id.replace('-', " "));
        let source_uri = req.source_uri.unwrap_or_default();
        let content = req.content.unwrap_or_default();
        let checksum = req.checksum.unwrap_or_else(|| {
            hmac_hex(
                tenant_id.as_bytes(),
                "content",
                &format!("{source_id}:{content}"),
                24,
            )
        });
        let revision = SourceRevision {
            id: new_id("rev"),
            source_id: source_id.to_string(),
            title: title.clone(),
            source_uri: source_uri.clone(),
            checksum,
            content,
            status: "staged".to_string(),
            created_at: now(),
        };

        let mut data = self.write()?;
        data.sources
            .entry(source_id.to_string())
            .or_insert_with(|| CompanySource {
                id: source_id.to_string(),
                title: title.clone(),
                canonical_key: sanitize_slug(&title),
                source_uri: source_uri.clone(),
                active_revision_id: None,
            });
        data.source_revisions
            .entry(source_id.to_string())
            .or_default()
            .push(revision.clone());

        let revision_id = revision.id.clone();
        drop(data);

        let history = self.append_internal_event(
            tenant_id,
            "company",
            "company_doc.revision_created",
            "company_doc_revision",
            &revision_id,
            format!("Company document revision created for {source_id}"),
            json!({ "source_id": source_id, "revision_id": revision_id.clone() }),
        )?;

        Ok(CreateRevisionResponse {
            source_id: source_id.to_string(),
            revision_id,
            status: "staged".to_string(),
            history_event_id: Some(history.event.id),
            ingest_job_id: if req.ingest {
                Some(new_id("ingest"))
            } else {
                None
            },
        })
    }

    pub fn activate_revision(
        &self,
        tenant_id: &str,
        source_id: &str,
        revision_id: &str,
        _req: ActivateRevisionRequest,
    ) -> Result<ActivateRevisionResponse, ApiError> {
        let mut data = self.write()?;
        let revisions = data
            .source_revisions
            .get_mut(source_id)
            .ok_or_else(|| ApiError::not_found("source revisions not found"))?;
        let revision = revisions
            .iter_mut()
            .find(|revision| revision.id == revision_id)
            .ok_or_else(|| ApiError::not_found("revision not found"))?;
        revision.status = "active".to_string();
        let revision = revision.clone();

        let source = data
            .sources
            .get_mut(source_id)
            .ok_or_else(|| ApiError::not_found("source not found"))?;
        let previous_revision_id = source.active_revision_id.replace(revision_id.to_string());
        source.title = revision.title.clone();
        source.source_uri = revision.source_uri.clone();
        source.canonical_key = sanitize_slug(&revision.title);

        let ingest = self.write_company_revision_context_locked(&mut data, tenant_id, &revision);

        drop(data);

        let history = self.append_internal_event(
            tenant_id,
            "company",
            "company_doc.revision_activated",
            "company_doc_revision",
            revision_id,
            format!("Company document revision activated for {source_id}"),
            json!({ "source_id": source_id, "revision_id": revision_id }),
        )?;

        Ok(ActivateRevisionResponse {
            source_id: source_id.to_string(),
            active_revision_id: revision_id.to_string(),
            previous_revision_id,
            history_event_id: Some(history.event.id),
            source_document_uri: ingest.source_document_uri,
            fragment_uris: ingest.fragment_uris.clone(),
            context_uris: ingest.fragment_uris,
        })
    }

    pub fn list_revisions(&self, source_id: &str) -> Result<Value, ApiError> {
        let data = self.read()?;
        Ok(json!({
            "source_id": source_id,
            "revisions": data.source_revisions.get(source_id).cloned().unwrap_or_default()
        }))
    }

    pub fn upsert_dataset(
        &self,
        dataset_key: &str,
        req: DatasetSchemaUpsertRequest,
    ) -> Result<DatasetSchemaResponse, ApiError> {
        let mut data = self.write()?;
        let existing_version = data
            .datasets
            .get(dataset_key)
            .map(|dataset| dataset.schema_version)
            .unwrap_or(0);
        let dataset = DatasetRecord {
            id: format!("dataset_{}", sanitize_slug(dataset_key)),
            dataset_key: dataset_key.to_string(),
            title: req.title.unwrap_or_else(|| dataset_key.replace('-', " ")),
            schema_version: existing_version + 1,
            status: "active".to_string(),
            columns: req.columns,
        };
        data.datasets
            .insert(dataset_key.to_string(), dataset.clone());
        Ok(DatasetSchemaResponse {
            dataset,
            history_event_id: None,
        })
    }

    pub fn create_snapshot(
        &self,
        tenant_id: &str,
        req: CreateStructuredSnapshotRequest,
    ) -> Result<StructuredSnapshotResponse, ApiError> {
        let dataset_key = require_string(req.dataset_key, "dataset_key")?;
        let owner_user_id = require_string(req.owner_user_id, "owner_user_id")?;
        let period_key = require_string(req.period_key, "period_key")?;
        let period_start = req
            .period_start
            .ok_or_else(|| ApiError::bad_request("period_start is required"))?;
        let period_end = req
            .period_end
            .ok_or_else(|| ApiError::bad_request("period_end is required"))?;

        let id = if let Some(key) = req.idempotency_key {
            let hash = self.resolver.idempotency_hash(&key);
            let mut data = self.write()?;
            if let Some(id) = data.snapshot_idempotency.get(&hash).cloned() {
                id
            } else {
                let id = new_id("snapshot");
                data.snapshot_idempotency.insert(hash, id.clone());
                id
            }
        } else {
            new_id("snapshot")
        };

        let snapshot = StructuredSnapshot {
            id: id.clone(),
            dataset_key: dataset_key.clone(),
            owner_user_id: owner_user_id.clone(),
            period_key,
            period_start,
            period_end,
            row_count: 0,
            status: "open".to_string(),
        };

        let mut data = self.write()?;
        data.snapshots.insert(id.clone(), snapshot.clone());
        drop(data);

        let history = self.append_internal_event(
            tenant_id,
            &owner_user_id,
            "structured.snapshot.created",
            "structured_snapshot",
            &id,
            format!("Snapshot created for dataset {dataset_key}"),
            json!({ "dataset_key": dataset_key }),
        )?;

        Ok(StructuredSnapshotResponse {
            snapshot,
            history_event_id: history.event.id,
        })
    }

    pub fn get_snapshot(&self, snapshot_id: &str) -> Result<StructuredSnapshot, ApiError> {
        let data = self.read()?;
        data.snapshots
            .get(snapshot_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("snapshot not found"))
    }

    pub fn bulk_rows(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
        req: BulkStructuredRowsRequest,
    ) -> Result<BulkStructuredRowsResponse, ApiError> {
        let mut data = self.write()?;
        let owner_user_id = data
            .snapshots
            .get(snapshot_id)
            .map(|snapshot| snapshot.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("snapshot not found"))?;

        let mut inserted = 0;
        let mut duplicates = 0;
        let mut invalid = 0;
        let mut row_ids = Vec::new();
        let mut rows_to_add = Vec::new();
        for mut row in req.rows {
            if !row.is_object() {
                invalid += 1;
                continue;
            }
            let row_id = row
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    req.idempotency_key
                        .as_deref()
                        .map(|key| self.resolver.idempotency_hash(key))
                })
                .unwrap_or_else(|| new_id("row"));
            let key = (snapshot_id.to_string(), row_id.clone());
            if data.row_idempotency.contains(&key) {
                duplicates += 1;
            } else {
                if let Some(obj) = row.as_object_mut() {
                    obj.entry("id".to_string())
                        .or_insert_with(|| Value::String(row_id.clone()));
                    obj.entry("snapshot_id".to_string())
                        .or_insert_with(|| Value::String(snapshot_id.to_string()));
                    obj.entry("tenant_id".to_string())
                        .or_insert_with(|| Value::String(tenant_id.to_string()));
                    obj.entry("owner_user_id".to_string())
                        .or_insert_with(|| Value::String(owner_user_id.clone()));
                }
                data.row_idempotency.insert(key);
                rows_to_add.push(row);
                row_ids.push(row_id);
                inserted += 1;
            }
        }
        data.rows_by_snapshot
            .entry(snapshot_id.to_string())
            .or_default()
            .extend(rows_to_add);
        let row_count = data
            .rows_by_snapshot
            .get(snapshot_id)
            .map(Vec::len)
            .unwrap_or(0);
        if let Some(snapshot) = data.snapshots.get_mut(snapshot_id) {
            snapshot.row_count = row_count;
        }
        drop(data);

        let history = self.append_internal_event(
            tenant_id,
            &owner_user_id,
            "structured.rows.bulk_inserted",
            "structured_snapshot",
            snapshot_id,
            format!("Inserted {inserted} structured rows"),
            json!({ "inserted": inserted, "duplicates": duplicates, "invalid": invalid }),
        )?;

        Ok(BulkStructuredRowsResponse {
            snapshot_id: snapshot_id.to_string(),
            inserted,
            duplicates,
            invalid,
            row_ids,
            history_event_id: history.event.id,
        })
    }

    pub fn list_rows(&self, snapshot_id: &str) -> Result<Value, ApiError> {
        let data = self.read()?;
        Ok(json!({
            "snapshot_id": snapshot_id,
            "rows": data.rows_by_snapshot.get(snapshot_id).cloned().unwrap_or_default()
        }))
    }

    pub fn apply_snapshot(
        &self,
        tenant_id: &str,
        dataset_key: &str,
        req: ApplySnapshotRequest,
    ) -> Result<ApplySnapshotResponse, ApiError> {
        let snapshot_id = require_string(req.snapshot_id, "snapshot_id")?;
        let (snapshot, rows, prior_rows_by_period) = {
            let data = self.read()?;
            let snapshot = data
                .snapshots
                .get(&snapshot_id)
                .cloned()
                .ok_or_else(|| ApiError::not_found("snapshot not found"))?;
            let rows = data
                .rows_by_snapshot
                .get(&snapshot_id)
                .cloned()
                .unwrap_or_default();
            let mut prior_snapshots = data
                .snapshots
                .values()
                .filter(|candidate| {
                    candidate.id != snapshot.id
                        && candidate.dataset_key == snapshot.dataset_key
                        && candidate.owner_user_id == snapshot.owner_user_id
                        && candidate.period_start < snapshot.period_start
                })
                .cloned()
                .collect::<Vec<_>>();
            prior_snapshots.sort_by_key(|snapshot| Reverse(snapshot.period_start));
            let prior_rows_by_period = prior_snapshots
                .into_iter()
                .take(4)
                .map(|prior| {
                    (
                        prior.period_key,
                        data.rows_by_snapshot
                            .get(&prior.id)
                            .cloned()
                            .unwrap_or_default(),
                    )
                })
                .collect::<Vec<_>>();
            (snapshot, rows, prior_rows_by_period)
        };
        if snapshot.dataset_key != dataset_key {
            return Err(ApiError::bad_request(
                "snapshot dataset_key does not match path dataset_key",
            ));
        }

        let stats = deterministic_stats(&rows, &prior_rows_by_period);
        let summary_id = new_id("summary");
        let insight_candidate_id = (req.llm_mode != "none").then(|| new_id("candidate"));
        let context_uri = format!(
            "ctx://user/structured/{}/snapshots/{}/trend/.overview",
            sanitize_slug(dataset_key),
            sanitize_slug(&snapshot.period_key)
        );
        let llm_summary = (req.llm_mode != "none").then(|| {
            format!(
                "LLM trend summary for {dataset_key} {} over {} rows.",
                snapshot.period_key,
                rows.len()
            )
        });
        let summary = json!({
            "id": summary_id,
            "snapshot_id": snapshot_id,
            "dataset_key": dataset_key,
            "owner_user_id": snapshot.owner_user_id,
            "stats": stats,
            "analysis_window": req.analysis_window.unwrap_or_else(|| "last_4_periods".to_string()),
            "llm_mode": req.llm_mode,
            "llm_summary": llm_summary,
            "insight_candidate_ids": insight_candidate_id.iter().collect::<Vec<_>>(),
            "context_uri": context_uri
        });

        let mut data = self.write()?;
        data.structured_summaries
            .insert(summary["id"].as_str().unwrap().to_string(), summary.clone());
        let routing = self
            .resolver
            .resolve(tenant_id, &snapshot.owner_user_id, false, true)?;
        if req.materialize_context {
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default()
                .push(ContextNode {
                    uri: context_uri.clone(),
                    title: format!("{} trend summary", dataset_key),
                    layer: 1,
                    body: summary.to_string(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: Some(snapshot.owner_user_id.clone()),
                    index_uid: routing.personal_context_index_uid,
                    index_kind: "personal".to_string(),
                    ancestor_uris: ancestor_uris(&context_uri),
                    node_kind: "overview".to_string(),
                    retrieval_role: "overview".to_string(),
                    retrieval_enabled: false,
                    parent_uri: None,
                    source_document_uri: None,
                    fragment_index: None,
                    char_start: None,
                    char_end: None,
                    token_estimate: None,
                    checksum: None,
                    source_id: None,
                    revision_id: None,
                    block_type: None,
                    page_idx: None,
                    bbox: None,
                    section_path: Vec::new(),
                    heading_level: None,
                    asset_refs: Vec::new(),
                    artifact_refs: Vec::new(),
                    status: "active".to_string(),
                    privacy: "private".to_string(),
                    updated_at: now(),
                });
        }
        drop(data);

        let history = self.append_internal_event(
            tenant_id,
            &snapshot.owner_user_id,
            "structured.snapshot.applied",
            "structured_snapshot",
            &snapshot_id,
            format!("Structured snapshot {} applied", snapshot.period_key),
            json!({
                "dataset_key": dataset_key,
                "summary_id": summary["id"].as_str().unwrap(),
                "llm_mode": summary["llm_mode"]
            }),
        )?;
        let _history_event_id = history.event.id;

        Ok(ApplySnapshotResponse {
            snapshot_id,
            summary_ids: vec![summary["id"].as_str().unwrap().to_string()],
            state_item_ids: Vec::new(),
            insight_candidate_ids: insight_candidate_id.into_iter().collect(),
            context_uris: vec![context_uri],
            job_id: new_id("job"),
        })
    }

    pub fn current_structured_state(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<CurrentStructuredStateResponse, ApiError> {
        let data = self.read()?;
        let private_allowed = |owner: &str| include_all_private || owner_user_id == Some(owner);
        Ok(CurrentStructuredStateResponse {
            items: data
                .state_items
                .values()
                .filter(|item| {
                    item.tenant_id == tenant_id
                        && item.state_type == "structured_summary"
                        && private_allowed(&item.owner_user_id)
                })
                .cloned()
                .collect(),
            summaries: data
                .structured_summaries
                .values()
                .filter(|summary| {
                    summary
                        .get("owner_user_id")
                        .and_then(Value::as_str)
                        .is_some_and(private_allowed)
                })
                .cloned()
                .collect(),
        })
    }

    pub fn fs_ls(
        &self,
        tenant_id: &str,
        uri: Option<&str>,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<Value, ApiError> {
        let data = self.read()?;
        let prefix = uri.unwrap_or("ctx://");
        let nodes = self.context_scope_for_acl_locked(
            &data,
            tenant_id,
            owner_user_id,
            include_all_private,
        )?;
        let mut children: Vec<_> = nodes
            .into_iter()
            .filter(|node| node.status == "active")
            .filter(|node| node.uri.starts_with(prefix))
            .map(|node| {
                json!({
                    "uri": node.uri,
                    "title": node.title,
                    "layer": node.layer,
                    "index_kind": node.index_kind
                })
            })
            .collect();
        children.sort_by_key(|node| node["uri"].as_str().unwrap_or("").to_string());
        Ok(json!({ "uri": prefix, "children": children }))
    }

    pub fn fs_tree(
        &self,
        tenant_id: &str,
        uri: Option<&str>,
        depth: Option<usize>,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<Value, ApiError> {
        let mut tree = self.fs_ls(tenant_id, uri, owner_user_id, include_all_private)?;
        tree["depth"] = json!(depth.unwrap_or(2));
        Ok(tree)
    }

    pub fn fs_read(
        &self,
        tenant_id: &str,
        uri: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextNode, ApiError> {
        let data = self.read()?;
        if let Some(node) = self
            .context_scope_for_acl_locked(&data, tenant_id, owner_user_id, include_all_private)?
            .into_iter()
            .find(|node| node.uri == uri && node.status == "active")
        {
            return Ok(node);
        }
        self.source_document_for_acl_locked(
            &data,
            tenant_id,
            uri,
            owner_user_id,
            include_all_private,
        )
        .map(source_document_context_node)
        .ok_or_else(|| ApiError::not_found("context uri not found"))
    }

    pub fn fs_layer(
        &self,
        tenant_id: &str,
        uri: &str,
        layer: u8,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextNode, ApiError> {
        let target = strip_layer_suffix(uri);
        let data = self.read()?;
        self.context_scope_for_acl_locked(&data, tenant_id, owner_user_id, include_all_private)?
            .into_iter()
            .find(|node| {
                strip_layer_suffix(&node.uri) == target
                    && node.layer == layer
                    && node.status == "active"
            })
            .ok_or_else(|| ApiError::not_found("context layer not found"))
    }

    pub fn traceback(
        &self,
        tenant_id: &str,
        req: ContextTracebackRequest,
        include_all_private: bool,
    ) -> Result<ContextTracebackResponse, ApiError> {
        let uri = require_string(req.uri, "uri")?;
        let owner_user_id = req.owner_user_id.as_deref();
        let data = self.read()?;
        let fragment = self
            .context_scope_for_acl_locked(&data, tenant_id, owner_user_id, include_all_private)?
            .into_iter()
            .find(|node| node.uri == uri && node.status == "active")
            .ok_or_else(|| ApiError::not_found("fragment uri not found"))?;
        if fragment.node_kind != "fragment" {
            return Err(ApiError::bad_request(
                "traceback uri must identify a fragment",
            ));
        }

        let part_of = data
            .links
            .values()
            .find(|link| {
                link.tenant_id == tenant_id
                    && link.status == "active"
                    && link.relation == "part_of"
                    && link.source_uri == fragment.uri
            })
            .cloned();
        let source_document_uri = fragment
            .source_document_uri
            .clone()
            .or_else(|| part_of.map(|link| link.target_uri))
            .ok_or_else(|| ApiError::not_found("source document link not found"))?;
        let source_document = self
            .source_document_for_acl_locked(
                &data,
                tenant_id,
                &source_document_uri,
                owner_user_id,
                include_all_private,
            )
            .ok_or_else(|| ApiError::not_found("source document not found"))?;

        Ok(ContextTracebackResponse {
            fragment_uri: fragment.uri,
            fragment_title: fragment.title,
            fragment_index: fragment.fragment_index,
            checksum: fragment.checksum,
            token_estimate: fragment.token_estimate,
            source_document_uri: source_document.uri,
            source_id: source_document.source_id,
            revision_id: source_document.revision_id,
            page_idx: fragment.page_idx,
            bbox: fragment.bbox,
            block_type: fragment.block_type,
            section_path: fragment.section_path,
            asset_refs: fragment.asset_refs,
            artifact_refs: fragment.artifact_refs,
            char_start: fragment.char_start,
            char_end: fragment.char_end,
            source_title: source_document.title,
        })
    }

    pub fn search_context(
        &self,
        tenant_id: &str,
        req: ContextSearchRequest,
    ) -> Result<ContextSearchOutcome, ApiError> {
        let query = require_string(req.query, "query")?;
        let owner_user_id = req
            .owner_user_id
            .or_else(|| owner_from_filters(&req.filters).map(ToString::to_string));
        let limit = req.limit.max(1);
        let data = self.read()?;
        let nodes = self.context_scope_locked(&data, tenant_id, owner_user_id.as_deref())?;

        let fragments = rank_nodes(
            nodes
                .iter()
                .filter(|node| retrieval_candidate(node))
                .cloned(),
            &query,
            limit,
        );
        drop(data);

        let selected_nodes: Vec<_> = fragments
            .iter()
            .map(|(node, _)| node.clone())
            .take(limit)
            .collect();
        let hits: Vec<_> = selected_nodes
            .iter()
            .map(|node| context_hit_from_node(node, &query))
            .collect();
        let stages = vec![stage_value(
            "fragments",
            &fragments,
            owner_user_id.as_deref(),
        )];
        let trace = TraceRecord {
            id: new_id("trace"),
            tenant_id: tenant_id.to_string(),
            owner_user_id,
            query,
            mode: req.mode,
            stages: stages.clone(),
            context_uris: hits.iter().map(|hit| hit.uri.clone()).collect(),
            created_at: now(),
        };

        let response = ContextSearchResponse {
            trace_id: trace.id.clone(),
            hits,
            stages,
        };
        let mut data = self.write()?;
        data.traces.insert(trace.id.clone(), trace.clone());

        Ok(ContextSearchOutcome {
            response,
            trace,
            nodes: selected_nodes,
        })
    }

    pub fn reveal_context(
        &self,
        tenant_id: &str,
        req: ContextRevealRequest,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<ContextRevealResponse, ApiError> {
        let layer = req.next_layer.unwrap_or(1);
        let uri = if let Some(uri) = req.uri {
            uri
        } else if let Some(trace_id) = req.trace_id {
            let data = self.read()?;
            data.traces
                .get(&trace_id)
                .and_then(|trace| trace.context_uris.first().cloned())
                .ok_or_else(|| ApiError::not_found("trace has no context to reveal"))?
        } else {
            return Err(ApiError::bad_request("uri or trace_id is required"));
        };
        let node = self.fs_layer(tenant_id, &uri, layer, owner_user_id, include_all_private)?;
        Ok(ContextRevealResponse {
            uri: node.uri,
            layer: node.layer,
            content: node.body,
            source_ref: SourceRef {
                kind: node.index_kind,
                id: node.source_id.unwrap_or_default(),
                uri: Some(uri),
                meta: None,
            },
        })
    }

    pub fn answer_rag(
        &self,
        tenant_id: &str,
        req: RagAnswerRequest,
    ) -> Result<RagAnswerResponse, ApiError> {
        let question = require_string(req.question, "question")?;
        let owner_user_id = req.owner_user_id.or_else(|| {
            req.session_id.as_ref().and_then(|session_id| {
                self.read().ok().and_then(|data| {
                    data.sessions
                        .get(session_id)
                        .map(|s| s.owner_user_id.clone())
                })
            })
        });
        let outcome = self.search_context(
            tenant_id,
            ContextSearchRequest {
                query: Some(question.clone()),
                mode: req.mode,
                owner_user_id,
                debug: req.debug,
                ..ContextSearchRequest::default()
            },
        )?;
        Ok(self.answer_from_context(outcome))
    }

    pub fn create_session(&self, req: SessionCreateRequest) -> Result<SessionResponse, ApiError> {
        let owner_user_id = require_string(req.owner_user_id, "owner_user_id")?;
        let session = SessionRecord {
            id: new_id("session"),
            owner_user_id,
            title: req.title.unwrap_or_else(|| "Untitled session".to_string()),
            status: "active".to_string(),
            messages: Vec::new(),
            created_at: now(),
        };
        let mut data = self.write()?;
        data.sessions.insert(session.id.clone(), session.clone());
        Ok(SessionResponse {
            session_id: session.id,
            status: "active".to_string(),
        })
    }

    pub fn add_session_message(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionMessageRequest,
    ) -> Result<Value, ApiError> {
        let role = require_string(req.role, "role")?;
        let content = require_string(req.content, "content")?;
        let owner_user_id = {
            let mut data = self.write()?;
            let session = data
                .sessions
                .get_mut(session_id)
                .ok_or_else(|| ApiError::not_found("session not found"))?;
            session.messages.push(json!({
                "role": role,
                "content": content,
                "created_at": now()
            }));
            session.owner_user_id.clone()
        };

        let history_event_id = if req.write_history_event {
            Some(
                self.append_internal_event(
                    tenant_id,
                    &owner_user_id,
                    "session.message",
                    "session",
                    session_id,
                    content,
                    json!({ "role": role }),
                )?
                .event
                .id,
            )
        } else {
            None
        };

        Ok(json!({
            "session_id": session_id,
            "history_event_id": history_event_id
        }))
    }

    pub fn commit_session(
        &self,
        tenant_id: &str,
        session_id: &str,
        req: SessionCommitRequest,
    ) -> Result<SessionCommitResponse, ApiError> {
        let session = {
            let mut data = self.write()?;
            let session = data
                .sessions
                .get_mut(session_id)
                .ok_or_else(|| ApiError::not_found("session not found"))?;
            session.status = "archived".to_string();
            session.clone()
        };

        let archive_uri = if req.archive_context {
            let routing = self
                .resolver
                .resolve(tenant_id, &session.owner_user_id, false, true)?;
            let uri = format!(
                "ctx://session/{}/history/archive_0001",
                sanitize_slug(session_id)
            );
            let mut data = self.write()?;
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default()
                .push(ContextNode {
                    uri: uri.clone(),
                    title: session.title.clone(),
                    layer: 2,
                    body: serde_json::to_string(&session.messages).unwrap_or_default(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: Some(session.owner_user_id.clone()),
                    index_uid: routing.personal_context_index_uid,
                    index_kind: "personal".to_string(),
                    ancestor_uris: ancestor_uris(&uri),
                    node_kind: "source_doc".to_string(),
                    retrieval_role: "none".to_string(),
                    retrieval_enabled: false,
                    parent_uri: None,
                    source_document_uri: None,
                    fragment_index: None,
                    char_start: None,
                    char_end: None,
                    token_estimate: None,
                    checksum: None,
                    source_id: None,
                    revision_id: None,
                    block_type: None,
                    page_idx: None,
                    bbox: None,
                    section_path: Vec::new(),
                    heading_level: None,
                    asset_refs: Vec::new(),
                    artifact_refs: Vec::new(),
                    status: "active".to_string(),
                    privacy: "private".to_string(),
                    updated_at: now(),
                });
            Some(uri)
        } else {
            None
        };

        let history = self.append_internal_event(
            tenant_id,
            &session.owner_user_id,
            "session.committed",
            "session",
            session_id,
            format!("Session {} committed", session.title),
            json!({ "archive_uri": archive_uri }),
        )?;

        Ok(SessionCommitResponse {
            session_id: session_id.to_string(),
            archive_uri,
            history_event_ids: vec![history.event.id],
            insight_candidate_ids: if req.extract_insights {
                vec![new_id("candidate")]
            } else {
                Vec::new()
            },
            memory_diff_ids: Vec::new(),
        })
    }

    pub fn get_trace(&self, trace_id: &str) -> Result<TraceRecord, ApiError> {
        let data = self.read()?;
        data.traces
            .get(trace_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("trace not found"))
    }

    pub fn trace_owner_id(&self, trace_id: &str) -> Result<Option<String>, ApiError> {
        let data = self.read()?;
        data.traces
            .get(trace_id)
            .map(|trace| trace.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("trace not found"))
    }

    pub fn debug_meili_search(&self, index_uid: &str, query: &str) -> Result<Value, ApiError> {
        let data = self.read()?;
        let nodes = if index_uid == "rag_company_context" {
            data.company_context.clone()
        } else {
            data.personal_context
                .get(index_uid)
                .cloned()
                .unwrap_or_default()
        };
        let hits = rank_nodes(nodes.into_iter(), query, 20)
            .into_iter()
            .map(|(node, score)| {
                json!({
                    "uri": node.uri,
                    "title": node.title,
                    "layer": node.layer,
                    "score": score,
                    "index_uid": index_uid
                })
            })
            .collect::<Vec<_>>();
        Ok(json!({ "index_uid": index_uid, "hits": hits }))
    }

    pub fn insight_owner(&self, insight_id: &str) -> Result<String, ApiError> {
        let data = self.read()?;
        data.insights
            .get(insight_id)
            .map(|insight| insight.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("insight not found"))
    }

    pub fn snapshot_owner(&self, snapshot_id: &str) -> Result<String, ApiError> {
        let data = self.read()?;
        data.snapshots
            .get(snapshot_id)
            .map(|snapshot| snapshot.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("snapshot not found"))
    }

    pub fn session_owner_id(&self, session_id: &str) -> Result<String, ApiError> {
        self.session_owner(session_id)?
            .ok_or_else(|| ApiError::not_found("session not found"))
    }

    fn answer_from_context(&self, outcome: ContextSearchOutcome) -> RagAnswerResponse {
        let citations: Vec<_> = outcome
            .response
            .hits
            .iter()
            .take(5)
            .map(|hit| Citation {
                uri: hit.uri.clone(),
                source_id: hit.source_id.clone(),
                revision_id: hit.revision_id.clone(),
                title: hit.title.clone(),
                quote: hit.snippet.clone(),
                score: hit.score,
            })
            .collect();
        let answer = if citations.is_empty() {
            "I do not have enough indexed context to answer that yet.".to_string()
        } else {
            format!(
                "Based on staged ContextFS retrieval, the strongest matching context is: {}",
                citations
                    .iter()
                    .map(|c| c.quote.as_str())
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        };

        RagAnswerResponse {
            answer_id: new_id("answer"),
            trace_id: outcome.response.trace_id,
            answer,
            citations,
            usage: json!({
                "provider": "none",
                "backend": self.backend_name(),
                "stages": ["fragments"]
            }),
        }
    }

    fn evaluate_case_result(
        &self,
        tenant_id: &str,
        run_id: &str,
        case: &RagEvalCase,
        outcome: &ContextSearchOutcome,
        answer: RagAnswerResponse,
        latency_ms: u64,
    ) -> Result<RagEvalCaseResult, ApiError> {
        let retrieved_uris = outcome
            .response
            .hits
            .iter()
            .take(5)
            .map(|hit| hit.uri.clone())
            .collect::<Vec<_>>();
        let source_doc_leaks = outcome
            .nodes
            .iter()
            .filter(|node| {
                node.node_kind != "fragment"
                    || node.retrieval_role != "fragment"
                    || node.source_document_uri.as_deref() == Some(node.uri.as_str())
            })
            .count();
        let acl_violations = outcome
            .nodes
            .iter()
            .filter(|node| {
                node.owner_user_id
                    .as_deref()
                    .is_some_and(|owner| case.owner_user_id.as_deref() != Some(owner))
            })
            .count();
        let stale_fragments = outcome
            .nodes
            .iter()
            .filter(|node| !retrieval_candidate(node))
            .count();

        let mut citation_source_document_uris = Vec::new();
        let mut traceback_failures = 0usize;
        for hit in &outcome.response.hits {
            match self.traceback(
                tenant_id,
                ContextTracebackRequest {
                    uri: Some(hit.uri.clone()),
                    owner_user_id: case.owner_user_id.clone(),
                },
                false,
            ) {
                Ok(traceback) => citation_source_document_uris.push(traceback.source_document_uri),
                Err(_) => traceback_failures += 1,
            }
        }
        let mut source_document_uris = citation_source_document_uris.clone();
        source_document_uris.sort();
        source_document_uris.dedup();

        let expected_context_matches = case
            .expected_context_uris
            .iter()
            .filter(|uri| retrieved_uris.contains(uri))
            .count();
        let expected_source_matches = case
            .expected_source_document_uris
            .iter()
            .filter(|uri| source_document_uris.contains(uri))
            .count();
        let expected_total =
            case.expected_context_uris.len() + case.expected_source_document_uris.len();
        let retrieval_recall_at_5 = if expected_total == 0 {
            1.0
        } else {
            (expected_context_matches + expected_source_matches) as f64 / expected_total as f64
        };
        let citation_precision = if answer.citations.is_empty() {
            if case.expected_source_document_uris.is_empty() {
                1.0
            } else {
                0.0
            }
        } else if case.expected_source_document_uris.is_empty() {
            if source_doc_leaks == 0 && traceback_failures == 0 {
                1.0
            } else {
                0.0
            }
        } else {
            let correct = citation_source_document_uris
                .iter()
                .filter(|uri| case.expected_source_document_uris.contains(uri))
                .count();
            correct as f64 / answer.citations.len().max(1) as f64
        };
        let traceback_success_rate = if outcome.response.hits.is_empty() {
            1.0
        } else {
            (outcome
                .response
                .hits
                .len()
                .saturating_sub(traceback_failures)) as f64
                / outcome.response.hits.len() as f64
        };

        let mut failures = Vec::new();
        if retrieval_recall_at_5 < 1.0 {
            failures.push("retrieval_recall".to_string());
        }
        if citation_precision < 1.0 {
            failures.push("citation_precision".to_string());
        }
        if traceback_failures > 0 {
            failures.push("traceback_missing".to_string());
        }
        if source_doc_leaks > 0 {
            failures.push("source_doc_leak".to_string());
        }
        if acl_violations > 0 {
            failures.push("acl_violation".to_string());
        }
        if stale_fragments > 0 {
            failures.push("stale_fragment".to_string());
        }
        for expected in &case.expected_answer_contains {
            if !answer.answer.contains(expected) {
                failures.push("answer_expectation".to_string());
                break;
            }
        }
        failures.sort();
        failures.dedup();

        let guard_failures = failures
            .iter()
            .filter_map(|failure| guard_name_for_failure(failure).map(ToString::to_string))
            .collect::<Vec<_>>();
        let answer_text = answer.answer;
        let citations = answer.citations;
        let tokens_per_answer = answer_text.split_whitespace().count() as f64;
        Ok(RagEvalCaseResult {
            id: new_id("evalresult"),
            run_id: run_id.to_string(),
            case_id: case.id.clone(),
            owner_user_id: case.owner_user_id.clone(),
            status: if failures.is_empty() {
                "passed".to_string()
            } else {
                "failed".to_string()
            },
            question: case.question.clone(),
            trace_id: answer.trace_id.clone(),
            answer: answer_text,
            citations,
            retrieved_uris,
            source_document_uris,
            failures: failures.clone(),
            guard_failures,
            metrics: json!({
                "retrieval_recall_at_5": retrieval_recall_at_5,
                "citation_precision": citation_precision,
                "traceback_success_rate": traceback_success_rate,
                "source_doc_leak_rate": if source_doc_leaks > 0 { 1.0 } else { 0.0 },
                "acl_violation_rate": if acl_violations > 0 { 1.0 } else { 0.0 },
                "stale_fragment_rate": if stale_fragments > 0 { 1.0 } else { 0.0 },
                "tokens_per_answer": tokens_per_answer
            }),
            latency_ms,
            created_at: now(),
        })
    }

    fn regression_guard_results(
        &self,
        tenant_id: &str,
        results: &[RagEvalCaseResult],
        llm_health_false_ready: bool,
    ) -> Result<Vec<RegressionGuardResult>, ApiError> {
        let has_failure = |name: &str| {
            results
                .iter()
                .any(|result| result.guard_failures.iter().any(|failure| failure == name))
        };
        let data = self.read()?;
        let (part_of_ok, part_of_evidence) = part_of_links_guard_locked(&data, tenant_id);
        let (superseded_ok, superseded_evidence) =
            superseded_fragments_guard_locked(&data, tenant_id);
        let (state_history_ok, state_history_evidence) =
            state_history_guard_locked(&data, tenant_id);
        Ok(vec![
            RegressionGuardResult {
                name: "source_doc_not_default_retrieved".to_string(),
                passed: !has_failure("source_doc_not_default_retrieved"),
                evidence: json!({ "failing_cases": guard_case_ids(results, "source_doc_not_default_retrieved") }),
            },
            RegressionGuardResult {
                name: "fragment_traceback_required".to_string(),
                passed: !has_failure("fragment_traceback_required"),
                evidence: json!({ "failing_cases": guard_case_ids(results, "fragment_traceback_required") }),
            },
            RegressionGuardResult {
                name: "owner_acl_never_leaks".to_string(),
                passed: !has_failure("owner_acl_never_leaks"),
                evidence: json!({ "failing_cases": guard_case_ids(results, "owner_acl_never_leaks") }),
            },
            RegressionGuardResult {
                name: "superseded_fragments_not_active".to_string(),
                passed: !has_failure("superseded_fragments_not_active") && superseded_ok,
                evidence: superseded_evidence,
            },
            RegressionGuardResult {
                name: "part_of_links_superseded_on_revision_update".to_string(),
                passed: part_of_ok,
                evidence: part_of_evidence,
            },
            RegressionGuardResult {
                name: "llm_health_controls_ready".to_string(),
                passed: !llm_health_false_ready,
                evidence: json!({ "llm_health_false_ready": llm_health_false_ready }),
            },
            RegressionGuardResult {
                name: "state_change_writes_history_event".to_string(),
                passed: state_history_ok,
                evidence: state_history_evidence.clone(),
            },
            RegressionGuardResult {
                name: "current_state_has_history_evidence".to_string(),
                passed: state_history_ok,
                evidence: state_history_evidence,
            },
        ])
    }

    fn write_eval_reports_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        run: &mut RagEvalRun,
        overview: &mut RagEvalOverview,
        results: &[RagEvalCaseResult],
    ) {
        for result in results {
            let uri = format!(
                "ctx://harness/eval/{}/cases/{}/report",
                sanitize_slug(&run.id),
                sanitize_slug(&result.case_id)
            );
            let content = case_result_markdown(result);
            let checksum = hmac_hex(
                tenant_id.as_bytes(),
                "eval-case-report",
                &format!("{}:{content}", result.id),
                32,
            );
            let now = now();
            data.source_documents.insert(
                uri.clone(),
                SourceDocument {
                    id: source_document_id(
                        tenant_id,
                        result.owner_user_id.as_deref(),
                        &format!("eval-case:{}", result.id),
                        &run.id,
                    ),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: result.owner_user_id.clone(),
                    source_kind: "eval_case_report".to_string(),
                    source_id: format!("eval-case:{}", result.id),
                    revision_id: run.id.clone(),
                    uri: uri.clone(),
                    title: format!("Eval case {} report", result.case_id),
                    content,
                    checksum,
                    status: "active".to_string(),
                    retrieval_enabled: false,
                    created_at: now,
                    updated_at: now,
                },
            );
            run.report_source_document_uris.push(uri.clone());
            overview.case_report_uris.push(uri);
        }

        let overview_uri = format!("ctx://harness/eval/{}/overview", sanitize_slug(&run.id));
        let checksum = hmac_hex(
            tenant_id.as_bytes(),
            "eval-overview-report",
            &format!("{}:{}", run.id, overview.overview_markdown),
            32,
        );
        let ingest = self.write_source_document_fragments_locked(
            data,
            tenant_id,
            None,
            "eval_overview_report",
            &format!("eval-overview:{}", run.id),
            &run.id,
            &overview_uri,
            &format!("Eval overview {}", run.id),
            &overview.overview_markdown,
            &checksum,
            "company",
            "rag_company_context",
            None,
            &[],
            &[],
        );
        overview.overview_source_document_uri = Some(ingest.source_document_uri.clone());
        run.overview_source_document_uri = Some(ingest.source_document_uri);
    }

    fn context_nodes_for_index(&self, index_uid: &str) -> Result<Vec<ContextNode>, ApiError> {
        let data = self.read()?;
        if index_uid == "rag_company_context" {
            return Ok(data.company_context.clone());
        }
        Ok(data
            .personal_context
            .get(index_uid)
            .cloned()
            .unwrap_or_default())
    }

    async fn persist_ingest_outputs(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<(), ApiError> {
        let (index_uid, source_owner) = if let Some(owner) = owner_user_id {
            self.ensure_user_indexes_for_owner(tenant_id, owner).await?;
            let routing = self.resolver.resolve(tenant_id, owner, false, true)?;
            (routing.personal_context_index_uid, Some(owner))
        } else {
            ("rag_company_context".to_string(), None)
        };
        let nodes = self.context_nodes_for_index(&index_uid)?;
        let _ = self
            .repository
            .upsert_context_nodes(&index_uid, &nodes)
            .await?;
        let source_documents = self.source_documents_for_scope(tenant_id, source_owner)?;
        let _ = self
            .repository
            .upsert_source_documents(&source_documents)
            .await?;
        let artifacts = self.parse_artifacts_for_scope(tenant_id, source_owner)?;
        let _ = self.repository.upsert_parse_artifacts(&artifacts).await?;
        let links = self.links_for_tenant(tenant_id)?;
        let _ = self.repository.upsert_links(&links).await?;
        Ok(())
    }

    fn transition_ingest_task(
        &self,
        task_id: &str,
        state: &str,
        error: Option<String>,
    ) -> Result<IngestTask, ApiError> {
        let mut data = self.write()?;
        let task = data
            .ingest_tasks
            .get_mut(task_id)
            .ok_or_else(|| ApiError::not_found("ingest task not found"))?;
        task.state = state.to_string();
        task.error = error;
        task.updated_at = now();
        if matches!(state, "completed" | "failed") {
            task.completed_at = Some(task.updated_at);
        }
        Ok(task.clone())
    }

    fn company_source(&self, source_id: &str) -> Result<Option<CompanySource>, ApiError> {
        let data = self.read()?;
        Ok(data.sources.get(source_id).cloned())
    }

    fn source_revision(
        &self,
        source_id: &str,
        revision_id: &str,
    ) -> Result<Option<SourceRevision>, ApiError> {
        let data = self.read()?;
        Ok(data.source_revisions.get(source_id).and_then(|revisions| {
            revisions
                .iter()
                .find(|revision| revision.id == revision_id)
                .cloned()
        }))
    }

    fn source_documents_for_scope(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<Vec<SourceDocument>, ApiError> {
        let data = self.read()?;
        Ok(data
            .source_documents
            .values()
            .filter(|document| document.tenant_id == tenant_id)
            .filter(|document| document.owner_user_id.as_deref() == owner_user_id)
            .cloned()
            .collect())
    }

    fn parse_artifacts_for_scope(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<Vec<ParseArtifact>, ApiError> {
        let data = self.read()?;
        Ok(data
            .parse_artifacts
            .values()
            .filter(|artifact| artifact.tenant_id == tenant_id)
            .filter(|artifact| artifact.owner_user_id.as_deref() == owner_user_id)
            .cloned()
            .collect())
    }

    fn links_for_tenant(&self, tenant_id: &str) -> Result<Vec<KnowledgeLink>, ApiError> {
        let data = self.read()?;
        Ok(data
            .links
            .values()
            .filter(|link| link.tenant_id == tenant_id)
            .cloned()
            .collect())
    }

    fn snapshot(&self, snapshot_id: &str) -> Result<Option<StructuredSnapshot>, ApiError> {
        let data = self.read()?;
        Ok(data.snapshots.get(snapshot_id).cloned())
    }

    fn snapshot_rows(&self, snapshot_id: &str) -> Result<Vec<Value>, ApiError> {
        let data = self.read()?;
        Ok(data
            .rows_by_snapshot
            .get(snapshot_id)
            .cloned()
            .unwrap_or_default())
    }

    fn structured_summaries(&self, summary_ids: &[String]) -> Result<Vec<Value>, ApiError> {
        let data = self.read()?;
        Ok(summary_ids
            .iter()
            .filter_map(|id| data.structured_summaries.get(id).cloned())
            .collect())
    }

    fn session_owner(&self, session_id: &str) -> Result<Option<String>, ApiError> {
        let data = self.read()?;
        Ok(data
            .sessions
            .get(session_id)
            .map(|session| session.owner_user_id.clone()))
    }

    fn insert_trace(&self, trace: TraceRecord) -> Result<(), ApiError> {
        let mut data = self.write()?;
        data.traces.insert(trace.id.clone(), trace);
        Ok(())
    }

    fn owner_from_path_or_body(
        &self,
        path_owner_user_id: Option<&str>,
        body_owner_user_id: Option<&str>,
    ) -> Result<String, ApiError> {
        match (path_owner_user_id, body_owner_user_id) {
            (Some(path), Some(body)) if path != body => Err(ApiError::bad_request(
                "owner_user_id in path and body must match",
            )),
            (Some(path), _) => Ok(path.to_string()),
            (_, Some(body)) => Ok(body.to_string()),
            _ => Err(ApiError::bad_request("owner_user_id is required")),
        }
    }

    fn ensure_user_index_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        owner_user_id: &str,
        schema_version: u32,
    ) -> Result<(UserEventIndex, EventIndexRouting), ApiError> {
        let key = (tenant_id.to_string(), owner_user_id.to_string());
        let existed = data.user_indexes.contains_key(&key);
        let routing = self
            .resolver
            .resolve(tenant_id, owner_user_id, !existed, existed)?;

        if !existed {
            let tenant_hash = self.resolver.tenant_hash(tenant_id);
            let index = UserEventIndex {
                id: user_event_index_id(&tenant_hash, &routing.owner_user_id_hash),
                tenant_id: tenant_id.to_string(),
                tenant_hash,
                owner_user_id_hash: routing.owner_user_id_hash.clone(),
                event_index_uid: routing.event_index_uid.clone(),
                personal_context_index_uid: routing.personal_context_index_uid.clone(),
                schema_version,
                settings_hash: EVENT_SETTINGS_HASH.to_string(),
                status: "active".to_string(),
                created_at: now(),
                last_event_at: None,
                event_count_estimate: 0,
            };
            data.user_indexes.insert(key.clone(), index);
            data.events_by_index
                .entry(routing.event_index_uid.clone())
                .or_default();
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default();
        }

        let index = data
            .user_indexes
            .get(&key)
            .cloned()
            .expect("user index exists");
        Ok((index, routing))
    }

    fn insert_event_locked(
        &self,
        data: &mut StoreData,
        routing: &EventIndexRouting,
        event: HistoryEvent,
        idempotency_key_hash: Option<String>,
    ) {
        if let Some(hash) = idempotency_key_hash {
            data.event_idempotency
                .insert((routing.event_index_uid.clone(), hash), event.id.clone());
        }
        data.event_by_id.insert(event.id.clone(), event.clone());
        data.events_by_index
            .entry(routing.event_index_uid.clone())
            .or_default()
            .push(event.clone());
        if let Some(index) = data
            .user_indexes
            .values_mut()
            .find(|index| index.event_index_uid == routing.event_index_uid)
        {
            index.last_event_at = Some(event.observed_at);
            index.event_count_estimate += 1;
        }
    }

    fn write_event_context_locked(
        &self,
        data: &mut StoreData,
        routing: &EventIndexRouting,
        event: &HistoryEvent,
    ) {
        let base = format!(
            "ctx://user/history/{}/{}",
            sanitize_slug(&event.event_type),
            sanitize_slug(&event.id)
        );
        let title = format!("{} {}", event.event_type, event.entity_id);
        let abstract_body = truncate_chars(&event.text, 500);
        let overview_body = json!({
            "event_type": event.event_type,
            "entity_type": event.entity_type,
            "entity_id": event.entity_id,
            "occurred_at": event.occurred_at,
            "text": event.text,
            "payload": event.payload
        })
        .to_string();
        let nodes = vec![
            self.context_node(
                &format!("{base}/.abstract"),
                &title,
                0,
                &abstract_body,
                "personal",
                &routing.personal_context_index_uid,
                &event.tenant_id,
                Some(event.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/.overview"),
                &title,
                1,
                &overview_body,
                "personal",
                &routing.personal_context_index_uid,
                &event.tenant_id,
                Some(event.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/detail"),
                &title,
                2,
                &event.text,
                "personal",
                &routing.personal_context_index_uid,
                &event.tenant_id,
                Some(event.owner_user_id.clone()),
                None,
                None,
            ),
        ];
        upsert_context_nodes(
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default(),
            nodes,
        );
    }

    fn write_state_context_locked(
        &self,
        data: &mut StoreData,
        routing: &EventIndexRouting,
        item: &StateItem,
    ) {
        let base = item.context_uri.clone();
        let body = format!("{}: {}", item.title, item.statement);
        let nodes = vec![
            self.context_node(
                &format!("{base}/.abstract"),
                &item.title,
                0,
                &truncate_chars(&body, 500),
                "personal",
                &routing.personal_context_index_uid,
                &item.tenant_id,
                Some(item.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/.overview"),
                &item.title,
                1,
                &json!({ "state": item }).to_string(),
                "personal",
                &routing.personal_context_index_uid,
                &item.tenant_id,
                Some(item.owner_user_id.clone()),
                None,
                None,
            ),
        ];
        upsert_context_nodes(
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default(),
            nodes,
        );
    }

    fn write_insight_context_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        routing: &EventIndexRouting,
        insight: &InsightRecord,
        evidence_text: Option<String>,
    ) {
        let base = insight.context_uri.clone();
        let nodes = vec![
            self.context_node(
                &format!("{base}/.abstract"),
                &insight.title,
                0,
                &truncate_chars(&insight.statement, 500),
                "personal",
                &routing.personal_context_index_uid,
                tenant_id,
                Some(insight.owner_user_id.clone()),
                None,
                None,
            ),
            self.context_node(
                &format!("{base}/.overview"),
                &insight.title,
                1,
                &json!({ "insight": insight, "evidence": evidence_text }).to_string(),
                "personal",
                &routing.personal_context_index_uid,
                tenant_id,
                Some(insight.owner_user_id.clone()),
                None,
                None,
            ),
        ];
        upsert_context_nodes(
            data.personal_context
                .entry(routing.personal_context_index_uid.clone())
                .or_default(),
            nodes,
        );
    }

    fn write_company_revision_context_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        revision: &SourceRevision,
    ) -> DocumentIngestResult {
        let source_document_uri = format!(
            "ctx://company/docs/{}/source/{}",
            sanitize_slug(&revision.source_id),
            sanitize_slug(&revision.id)
        );
        self.write_source_document_fragments_locked(
            data,
            tenant_id,
            None,
            "company_doc",
            &revision.source_id,
            &revision.id,
            &source_document_uri,
            &revision.title,
            &revision.content,
            &revision.checksum,
            "company",
            "rag_company_context",
            None,
            &[],
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn write_state_document_context_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        routing: &EventIndexRouting,
        owner_user_id: &str,
        state_type: &str,
        fact_key: &str,
        version: u32,
        title: &str,
        document: &StateDocumentPayload,
        policy: Option<&FragmentPolicy>,
    ) -> Result<DocumentIngestResult, ApiError> {
        let content = require_string(document.content.clone(), "document.content")?;
        let source_id = format!(
            "state:{}:{}:{}",
            sanitize_slug(owner_user_id),
            sanitize_slug(state_type),
            sanitize_slug(fact_key)
        );
        let revision_id = format!("v{version}");
        let checksum = hmac_hex(
            tenant_id.as_bytes(),
            "state-document",
            &format!("{source_id}:{revision_id}:{content}"),
            32,
        );
        let source_document_uri = format!(
            "ctx://user/state/{}/{}/source/{}",
            sanitize_slug(state_type),
            sanitize_slug(fact_key),
            sanitize_slug(&revision_id)
        );
        Ok(self.write_source_document_fragments_locked(
            data,
            tenant_id,
            Some(owner_user_id.to_string()),
            "state_doc",
            &source_id,
            &revision_id,
            &source_document_uri,
            title,
            &content,
            &checksum,
            "personal",
            &routing.personal_context_index_uid,
            policy,
            &[],
            &[],
        ))
    }

    #[allow(clippy::too_many_arguments)]
    fn write_source_document_fragments_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        owner_user_id: Option<String>,
        source_kind: &str,
        source_id: &str,
        revision_id: &str,
        source_document_uri: &str,
        title: &str,
        content: &str,
        checksum: &str,
        index_kind: &str,
        index_uid: &str,
        policy: Option<&FragmentPolicy>,
        blocks: &[ParsedBlock],
        artifact_refs: &[ParseArtifactRef],
    ) -> DocumentIngestResult {
        self.supersede_source_artifacts_locked(
            data,
            tenant_id,
            owner_user_id.as_deref(),
            source_id,
        );

        let now = now();
        let source_document_id =
            source_document_id(tenant_id, owner_user_id.as_deref(), source_id, revision_id);
        let created_at = data
            .source_documents
            .get(source_document_uri)
            .map(|document| document.created_at)
            .unwrap_or(now);
        let source_document = SourceDocument {
            id: source_document_id,
            tenant_id: tenant_id.to_string(),
            owner_user_id: owner_user_id.clone(),
            source_kind: source_kind.to_string(),
            source_id: source_id.to_string(),
            revision_id: revision_id.to_string(),
            uri: source_document_uri.to_string(),
            title: title.to_string(),
            content: content.to_string(),
            checksum: checksum.to_string(),
            status: "active".to_string(),
            retrieval_enabled: false,
            created_at,
            updated_at: now,
        };
        data.source_documents
            .insert(source_document.uri.clone(), source_document);

        if !blocks.is_empty() {
            data.parsed_blocks
                .insert(source_document_uri.to_string(), blocks.to_vec());
        }

        let fragmenter = BlockAwareFragmenter::from_policy(policy);
        let fragments = fragmenter.fragment(content, blocks);
        let fragment_uris = fragments
            .iter()
            .map(|fragment| {
                format!(
                    "{source_document_uri}/fragments/{:04}",
                    fragment.fragment_index + 1
                )
            })
            .collect::<Vec<_>>();
        let nodes = fragments
            .iter()
            .zip(fragment_uris.iter())
            .map(|(fragment, uri)| {
                self.fragment_context_node(
                    uri,
                    title,
                    index_kind,
                    index_uid,
                    tenant_id,
                    owner_user_id.clone(),
                    source_id,
                    revision_id,
                    source_document_uri,
                    fragment,
                    artifact_refs,
                )
            })
            .collect::<Vec<_>>();

        if index_kind == "company" {
            upsert_context_nodes(&mut data.company_context, nodes);
        } else {
            upsert_context_nodes(
                data.personal_context
                    .entry(index_uid.to_string())
                    .or_default(),
                nodes,
            );
        }

        for (fragment_uri, fragment) in fragment_uris.iter().zip(fragments.iter()) {
            let id = part_of_link_id(
                tenant_id,
                owner_user_id.as_deref(),
                fragment_uri,
                source_document_uri,
            );
            data.links.insert(
                id.clone(),
                KnowledgeLink {
                    id,
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: owner_user_id.clone(),
                    source_uri: fragment_uri.clone(),
                    target_uri: source_document_uri.to_string(),
                    source_title: Some(format!(
                        "{} fragment {}",
                        title,
                        fragment.fragment_index + 1
                    )),
                    target_title: Some(title.to_string()),
                    relation: "part_of".to_string(),
                    rationale: Some("fragment generated from source document".to_string()),
                    evidence_text: None,
                    confidence: 1.0,
                    created_by: "system_fragmenter".to_string(),
                    status: "active".to_string(),
                    tags: vec![source_kind.to_string()],
                    created_at: now,
                    updated_at: now,
                },
            );
        }

        DocumentIngestResult {
            source_id: source_id.to_string(),
            source_document_uri: source_document_uri.to_string(),
            fragment_uris,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn context_node(
        &self,
        uri: &str,
        title: &str,
        layer: u8,
        body: &str,
        index_kind: &str,
        index_uid: &str,
        tenant_id: &str,
        owner_user_id: Option<String>,
        source_id: Option<String>,
        revision_id: Option<String>,
    ) -> ContextNode {
        ContextNode {
            uri: uri.to_string(),
            title: title.to_string(),
            layer,
            body: body.to_string(),
            tenant_id: tenant_id.to_string(),
            owner_user_id,
            index_uid: index_uid.to_string(),
            index_kind: index_kind.to_string(),
            ancestor_uris: ancestor_uris(uri),
            node_kind: node_kind_for_layer(layer).to_string(),
            retrieval_role: retrieval_role_for_layer(layer).to_string(),
            retrieval_enabled: layer == 2,
            parent_uri: None,
            source_document_uri: None,
            fragment_index: None,
            char_start: None,
            char_end: None,
            token_estimate: None,
            checksum: None,
            source_id,
            revision_id,
            block_type: None,
            page_idx: None,
            bbox: None,
            section_path: Vec::new(),
            heading_level: None,
            asset_refs: Vec::new(),
            artifact_refs: Vec::new(),
            status: "active".to_string(),
            privacy: if index_kind == "company" {
                "company".to_string()
            } else {
                "private".to_string()
            },
            updated_at: now(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn fragment_context_node(
        &self,
        uri: &str,
        title: &str,
        index_kind: &str,
        index_uid: &str,
        tenant_id: &str,
        owner_user_id: Option<String>,
        source_id: &str,
        revision_id: &str,
        source_document_uri: &str,
        fragment: &FragmentChunk,
        artifact_refs: &[ParseArtifactRef],
    ) -> ContextNode {
        let mut node = self.context_node(
            uri,
            &format!("{} fragment {}", title, fragment.fragment_index + 1),
            2,
            &fragment.content,
            index_kind,
            index_uid,
            tenant_id,
            owner_user_id,
            Some(source_id.to_string()),
            Some(revision_id.to_string()),
        );
        node.node_kind = "fragment".to_string();
        node.retrieval_role = "fragment".to_string();
        node.retrieval_enabled = true;
        node.parent_uri = Some(source_document_uri.to_string());
        node.source_document_uri = Some(source_document_uri.to_string());
        node.fragment_index = Some(fragment.fragment_index);
        node.char_start = fragment.char_start;
        node.char_end = fragment.char_end;
        node.token_estimate = Some(fragment.token_estimate);
        node.checksum = Some(fragment.checksum.clone());
        node.block_type = fragment.block_type.clone();
        node.page_idx = fragment.page_idx;
        node.bbox = fragment.bbox.clone();
        node.section_path = fragment.section_path.clone();
        node.heading_level = fragment.heading_level;
        node.asset_refs = fragment.asset_refs.clone();
        node.artifact_refs = artifact_refs.to_vec();
        node
    }

    fn resolve_state_key(
        &self,
        tenant_id: &str,
        fact_key: &str,
        owner_user_id: Option<&str>,
    ) -> Result<(String, String, String), ApiError> {
        let data = self.read()?;
        if let Some(owner) = owner_user_id {
            return Ok((
                tenant_id.to_string(),
                owner.to_string(),
                fact_key.to_string(),
            ));
        }
        let matches: Vec<_> = data
            .state_items
            .keys()
            .filter(|(tenant, _, key)| tenant == tenant_id && key == fact_key)
            .cloned()
            .collect();
        match matches.len() {
            0 => Err(ApiError::not_found("state item not found")),
            1 => Ok(matches[0].clone()),
            _ => Err(ApiError::bad_request(
                "owner_user_id is required because fact_key is ambiguous",
            )),
        }
    }

    fn context_scope_locked(
        &self,
        data: &StoreData,
        tenant_id: &str,
        owner_user_id: Option<&str>,
    ) -> Result<Vec<ContextNode>, ApiError> {
        let mut nodes: Vec<_> = data
            .company_context
            .iter()
            .filter(|node| node.tenant_id == tenant_id || node.tenant_id == "default")
            .cloned()
            .collect();
        if let Some(owner) = owner_user_id {
            let routing = self.resolver.resolve(tenant_id, owner, false, true)?;
            nodes.extend(
                data.personal_context
                    .get(&routing.personal_context_index_uid)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
        Ok(nodes)
    }

    fn context_scope_for_acl_locked(
        &self,
        data: &StoreData,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<Vec<ContextNode>, ApiError> {
        if include_all_private && owner_user_id.is_none() {
            return Ok(self
                .all_context_nodes_locked(data)
                .into_iter()
                .filter(|node| node.tenant_id == tenant_id || node.tenant_id == "default")
                .collect());
        }
        self.context_scope_locked(data, tenant_id, owner_user_id)
    }

    fn source_document_for_acl_locked(
        &self,
        data: &StoreData,
        tenant_id: &str,
        uri: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Option<SourceDocument> {
        data.source_documents
            .get(uri)
            .filter(|document| document.status == "active")
            .filter(|document| document.tenant_id == tenant_id || document.tenant_id == "default")
            .filter(|document| {
                if include_all_private {
                    true
                } else if let Some(owner) = owner_user_id {
                    document.owner_user_id.as_deref().is_none()
                        || document.owner_user_id.as_deref() == Some(owner)
                } else {
                    document.owner_user_id.is_none()
                }
            })
            .cloned()
    }

    fn supersede_source_artifacts_locked(
        &self,
        data: &mut StoreData,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        source_id: &str,
    ) {
        let superseded_source_document_uris = data
            .source_documents
            .values()
            .filter(|document| {
                document.tenant_id == tenant_id
                    && document.owner_user_id.as_deref() == owner_user_id
                    && document.source_id == source_id
                    && document.status == "active"
            })
            .map(|document| document.uri.clone())
            .collect::<HashSet<_>>();

        for document in data.source_documents.values_mut() {
            if document.tenant_id == tenant_id
                && document.owner_user_id.as_deref() == owner_user_id
                && document.source_id == source_id
                && document.status == "active"
            {
                document.status = "superseded".to_string();
                document.updated_at = now();
            }
        }

        for node in &mut data.company_context {
            if node.tenant_id == tenant_id
                && node.owner_user_id.as_deref() == owner_user_id
                && node.source_id.as_deref() == Some(source_id)
                && node.status == "active"
            {
                node.status = "superseded".to_string();
                node.retrieval_enabled = false;
                node.updated_at = now();
            }
        }
        for nodes in data.personal_context.values_mut() {
            for node in nodes {
                if node.tenant_id == tenant_id
                    && node.owner_user_id.as_deref() == owner_user_id
                    && node.source_id.as_deref() == Some(source_id)
                    && node.status == "active"
                {
                    node.status = "superseded".to_string();
                    node.retrieval_enabled = false;
                    node.updated_at = now();
                }
            }
        }

        for link in data.links.values_mut() {
            if link.tenant_id == tenant_id
                && link.owner_user_id.as_deref() == owner_user_id
                && link.relation == "part_of"
                && link.status == "active"
                && superseded_source_document_uris.contains(&link.target_uri)
            {
                link.status = "superseded".to_string();
                link.updated_at = now();
            }
        }
    }

    fn all_context_nodes_locked(&self, data: &StoreData) -> Vec<ContextNode> {
        let mut nodes = data.company_context.clone();
        for personal in data.personal_context.values() {
            nodes.extend(personal.clone());
        }
        nodes
    }

    fn read(&self) -> Result<std::sync::RwLockReadGuard<'_, StoreData>, ApiError> {
        self.inner
            .read()
            .map_err(|_| ApiError::Internal("store read lock poisoned".to_string()))
    }

    fn write(&self) -> Result<std::sync::RwLockWriteGuard<'_, StoreData>, ApiError> {
        self.inner
            .write()
            .map_err(|_| ApiError::Internal("store write lock poisoned".to_string()))
    }
}

fn default_harness_components() -> Vec<(&'static str, &'static str, &'static str, &'static str)> {
    vec![
        (
            "retrieval.context_search",
            "Context Search",
            "retrieval",
            "Ranks active fragments for default RAG context retrieval.",
        ),
        (
            "retrieval.traceback",
            "Traceback",
            "retrieval",
            "Maps fragments back to source documents and parse artifacts.",
        ),
        (
            "ingestion.fragmenter",
            "Fragmenter",
            "ingestion",
            "Turns parsed documents into retrievable fragment nodes.",
        ),
        (
            "ingestion.parser_adapter",
            "Parser Adapter",
            "ingestion",
            "Normalizes parser output into blocks and artifacts.",
        ),
        (
            "llm.rag_answer_prompt",
            "RAG Answer Prompt",
            "llm",
            "Builds grounded answer prompts over retrieved citations.",
        ),
        (
            "llm.analysis_prompt",
            "Analysis Prompt",
            "llm",
            "Builds grounded analysis prompts for insight generation.",
        ),
        (
            "memory.insight_policy",
            "Insight Policy",
            "memory",
            "Controls insight extraction and update decisions.",
        ),
        (
            "memory.state_materialization_policy",
            "State Materialization Policy",
            "memory",
            "Controls current-state writes and history evidence.",
        ),
        (
            "safety.owner_acl",
            "Owner ACL",
            "safety",
            "Prevents cross-owner private context leakage.",
        ),
        (
            "safety.source_doc_retrieval_guard",
            "Source Document Retrieval Guard",
            "safety",
            "Keeps source documents out of default context retrieval.",
        ),
        (
            "health.llm_probe",
            "LLM Probe",
            "health",
            "Controls LLM health and readiness evidence.",
        ),
    ]
}

fn bootstrap_harness_revision_id(component_id: &str) -> String {
    format!("hrev_bootstrap_{}", sanitize_slug(component_id))
}

fn previous_revision_id(
    revisions: &[HarnessComponentRevision],
    current_revision_id: Option<&str>,
) -> Option<String> {
    revisions
        .iter()
        .filter(|revision| Some(revision.id.as_str()) != current_revision_id)
        .filter(|revision| revision.status != "rolled_back")
        .max_by_key(|revision| revision.iteration)
        .map(|revision| revision.id.clone())
}

fn latest_eval_run_for_change(data: &StoreData, change_id: &str) -> Option<RagEvalRun> {
    data.eval_runs
        .values()
        .filter(|run| run.change_id.as_deref() == Some(change_id))
        .cloned()
        .max_by_key(|run| run.created_at)
}

fn metrics_to_value(metrics: &RagEvalMetrics) -> Value {
    json!({
        "pass_rate": metrics.pass_rate,
        "retrieval_recall_at_5": metrics.retrieval_recall_at_5,
        "citation_precision": metrics.citation_precision,
        "traceback_success_rate": metrics.traceback_success_rate,
        "source_doc_leak_rate": metrics.source_doc_leak_rate,
        "acl_violation_rate": metrics.acl_violation_rate,
        "stale_fragment_rate": metrics.stale_fragment_rate,
        "state_history_consistency_rate": metrics.state_history_consistency_rate,
        "llm_health_false_ready_rate": metrics.llm_health_false_ready_rate,
        "tokens_per_answer": metrics.tokens_per_answer,
        "latency_p95": metrics.latency_p95
    })
}

fn verdict_evidence_text(run: Option<&RagEvalRun>, overview: Option<&RagEvalOverview>) -> String {
    let mut parts = Vec::new();
    if let Some(run) = run {
        parts.push(run.status.clone());
        for guard in &run.guard_results {
            if !guard.passed {
                parts.push(guard.name.clone());
            }
        }
    }
    if let Some(overview) = overview {
        for cluster in &overview.failure_patterns {
            parts.push(cluster.pattern.clone());
            parts.extend(cluster.root_cause_notes.clone());
        }
    }
    parts.join("\n").to_lowercase()
}

fn contains_folded(haystack: &str, needle: &str) -> bool {
    !needle.trim().is_empty() && haystack.to_lowercase().contains(&needle.to_lowercase())
}

fn aggregate_eval_metrics(results: &[RagEvalCaseResult]) -> RagEvalMetrics {
    if results.is_empty() {
        return RagEvalMetrics::default();
    }
    let total = results.len() as f64;
    RagEvalMetrics {
        pass_rate: results
            .iter()
            .filter(|result| result.status == "passed")
            .count() as f64
            / total,
        retrieval_recall_at_5: average_result_metric(results, "retrieval_recall_at_5"),
        citation_precision: average_result_metric(results, "citation_precision"),
        traceback_success_rate: average_result_metric(results, "traceback_success_rate"),
        source_doc_leak_rate: average_result_metric(results, "source_doc_leak_rate"),
        acl_violation_rate: average_result_metric(results, "acl_violation_rate"),
        stale_fragment_rate: average_result_metric(results, "stale_fragment_rate"),
        state_history_consistency_rate: 1.0,
        llm_health_false_ready_rate: 0.0,
        tokens_per_answer: average_result_metric(results, "tokens_per_answer"),
        latency_p95: latency_p95(results) as f64,
    }
}

fn average_result_metric(results: &[RagEvalCaseResult], key: &str) -> f64 {
    results
        .iter()
        .map(|result| {
            result
                .metrics
                .get(key)
                .and_then(Value::as_f64)
                .unwrap_or(0.0)
        })
        .sum::<f64>()
        / results.len().max(1) as f64
}

fn latency_p95(results: &[RagEvalCaseResult]) -> u64 {
    if results.is_empty() {
        return 0;
    }
    let mut latencies = results
        .iter()
        .map(|result| result.latency_ms)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let index = ((latencies.len() as f64 * 0.95).ceil() as usize).saturating_sub(1);
    latencies[index.min(latencies.len() - 1)]
}

fn build_eval_overview(run: &RagEvalRun, results: &[RagEvalCaseResult]) -> RagEvalOverview {
    let failure_patterns = failure_pattern_clusters(results);
    let suggested_target_component = failure_patterns
        .first()
        .map(|cluster| cluster.suggested_target_component.clone())
        .unwrap_or_else(|| "retrieval.context_search".to_string());
    let root_cause_notes = failure_patterns
        .iter()
        .flat_map(|cluster| cluster.root_cause_notes.clone())
        .collect::<Vec<_>>();
    let mut markdown = String::new();
    markdown.push_str(&format!("# RAG Eval Overview {}\n\n", run.id));
    markdown.push_str(&format!("status: {}\n\n", run.status));
    markdown.push_str("## Metrics\n");
    for (name, value) in [
        ("pass_rate", run.metrics.pass_rate),
        ("retrieval_recall_at_5", run.metrics.retrieval_recall_at_5),
        ("citation_precision", run.metrics.citation_precision),
        ("traceback_success_rate", run.metrics.traceback_success_rate),
        ("source_doc_leak_rate", run.metrics.source_doc_leak_rate),
        ("acl_violation_rate", run.metrics.acl_violation_rate),
        ("stale_fragment_rate", run.metrics.stale_fragment_rate),
        (
            "state_history_consistency_rate",
            run.metrics.state_history_consistency_rate,
        ),
        (
            "llm_health_false_ready_rate",
            run.metrics.llm_health_false_ready_rate,
        ),
        ("tokens_per_answer", run.metrics.tokens_per_answer),
        ("latency_p95", run.metrics.latency_p95),
    ] {
        markdown.push_str(&format!("- {name}: {value:.3}\n"));
    }
    markdown.push_str("\n## Failure Patterns\n");
    if failure_patterns.is_empty() {
        markdown.push_str("- none\n");
    } else {
        for cluster in &failure_patterns {
            markdown.push_str(&format!(
                "- {}: {} case(s), target {}\n",
                cluster.pattern, cluster.count, cluster.suggested_target_component
            ));
        }
    }
    markdown.push_str(&format!(
        "\n## Suggested Target Component\n{}\n",
        suggested_target_component
    ));
    RagEvalOverview {
        run_id: run.id.clone(),
        status: run.status.clone(),
        metrics: run.metrics.clone(),
        failure_patterns,
        suggested_target_component,
        root_cause_notes,
        overview_markdown: markdown,
        case_report_uris: Vec::new(),
        overview_source_document_uri: None,
        generated_at: now(),
    }
}

fn failure_pattern_clusters(results: &[RagEvalCaseResult]) -> Vec<FailurePatternCluster> {
    let mut grouped: HashMap<String, Vec<String>> = HashMap::new();
    for result in results {
        for failure in &result.failures {
            grouped
                .entry(failure.clone())
                .or_default()
                .push(result.case_id.clone());
        }
    }
    let mut clusters = grouped
        .into_iter()
        .map(|(pattern, case_ids)| FailurePatternCluster {
            suggested_target_component: suggested_component_for_failure(&pattern).to_string(),
            root_cause_notes: vec![root_cause_note_for_failure(&pattern).to_string()],
            count: case_ids.len(),
            case_ids,
            pattern,
        })
        .collect::<Vec<_>>();
    clusters.sort_by_key(|cluster| Reverse(cluster.count));
    clusters
}

fn suggested_component_for_failure(failure: &str) -> &'static str {
    match failure {
        "traceback_missing" => "retrieval.traceback",
        "source_doc_leak" => "safety.source_doc_retrieval_guard",
        "acl_violation" => "safety.owner_acl",
        "stale_fragment" => "ingestion.fragmenter",
        "answer_expectation" => "llm.rag_answer_prompt",
        "citation_precision" => "retrieval.traceback",
        _ => "retrieval.context_search",
    }
}

fn root_cause_note_for_failure(failure: &str) -> &'static str {
    match failure {
        "traceback_missing" => "A retrieved fragment did not resolve to source-document evidence.",
        "source_doc_leak" => "Default retrieval included a non-fragment or source-document node.",
        "acl_violation" => "A private node crossed the requested owner boundary.",
        "stale_fragment" => {
            "A retrieved fragment was inactive, superseded, or not retrieval-enabled."
        }
        "answer_expectation" => "The grounded answer did not contain expected answer evidence.",
        "citation_precision" => "Retrieved citations did not align with expected source documents.",
        _ => "Expected evidence was not present in the top retrieved fragments.",
    }
}

fn guard_name_for_failure(failure: &str) -> Option<&'static str> {
    match failure {
        "source_doc_leak" => Some("source_doc_not_default_retrieved"),
        "traceback_missing" => Some("fragment_traceback_required"),
        "acl_violation" => Some("owner_acl_never_leaks"),
        "stale_fragment" => Some("superseded_fragments_not_active"),
        _ => None,
    }
}

fn guard_case_ids(results: &[RagEvalCaseResult], guard_name: &str) -> Vec<String> {
    results
        .iter()
        .filter(|result| {
            result
                .guard_failures
                .iter()
                .any(|failure| failure == guard_name)
        })
        .map(|result| result.case_id.clone())
        .collect()
}

fn part_of_links_guard_locked(data: &StoreData, tenant_id: &str) -> (bool, Value) {
    let nodes = all_context_nodes_for_guard(data);
    let mut missing_links = Vec::new();
    let mut stale_links = Vec::new();
    for node in nodes
        .iter()
        .filter(|node| node.tenant_id == tenant_id && node.node_kind == "fragment")
        .filter(|node| node.source_document_uri.is_some())
        .filter(|node| node.status == "active")
    {
        let has_active_link = data.links.values().any(|link| {
            link.tenant_id == tenant_id
                && link.status == "active"
                && link.relation == "part_of"
                && link.source_uri == node.uri
        });
        if !has_active_link {
            missing_links.push(node.uri.clone());
        }
    }
    for link in data.links.values().filter(|link| {
        link.tenant_id == tenant_id && link.status == "active" && link.relation == "part_of"
    }) {
        if data
            .source_documents
            .get(&link.target_uri)
            .is_some_and(|document| document.status != "active")
        {
            stale_links.push(link.id.clone());
        }
    }
    (
        missing_links.is_empty() && stale_links.is_empty(),
        json!({ "missing_links": missing_links, "stale_links": stale_links }),
    )
}

fn superseded_fragments_guard_locked(data: &StoreData, tenant_id: &str) -> (bool, Value) {
    let nodes = all_context_nodes_for_guard(data);
    let mut unsafe_fragments = Vec::new();
    for node in nodes
        .iter()
        .filter(|node| node.tenant_id == tenant_id && node.node_kind == "fragment")
        .filter(|node| node.source_document_uri.is_some())
    {
        let source_superseded = node
            .source_document_uri
            .as_ref()
            .and_then(|uri| data.source_documents.get(uri))
            .is_some_and(|document| document.status != "active");
        if (node.status == "superseded" && node.retrieval_enabled)
            || (node.status == "active" && source_superseded)
        {
            unsafe_fragments.push(node.uri.clone());
        }
    }
    (
        unsafe_fragments.is_empty(),
        json!({ "unsafe_fragments": unsafe_fragments }),
    )
}

fn state_history_guard_locked(data: &StoreData, tenant_id: &str) -> (bool, Value) {
    let mut missing_state_items = Vec::new();
    for item in data
        .state_items
        .values()
        .filter(|item| item.tenant_id == tenant_id && item.status == "active")
    {
        let has_history = data.event_by_id.values().any(|event| {
            event.tenant_id == tenant_id
                && event.owner_user_id == item.owner_user_id
                && event.entity_type == "state_item"
                && event.entity_id == item.id
                && matches!(event.event_type.as_str(), "state.changed" | "state.patched")
        });
        if !has_history {
            missing_state_items.push(item.id.clone());
        }
    }
    (
        missing_state_items.is_empty(),
        json!({ "missing_state_items": missing_state_items }),
    )
}

fn all_context_nodes_for_guard(data: &StoreData) -> Vec<&ContextNode> {
    let mut nodes = data.company_context.iter().collect::<Vec<_>>();
    for personal in data.personal_context.values() {
        nodes.extend(personal.iter());
    }
    nodes
}

fn case_result_markdown(result: &RagEvalCaseResult) -> String {
    format!(
        "# Eval Case {}\n\nstatus: {}\n\ntrace_id: {}\n\n## Retrieved URIs\n{}\n\n## Source Documents\n{}\n\n## Failures\n{}\n",
        result.case_id,
        result.status,
        result.trace_id,
        markdown_list(&result.retrieved_uris),
        markdown_list(&result.source_document_uris),
        markdown_list(&result.failures),
    )
}

fn markdown_list(values: &[String]) -> String {
    if values.is_empty() {
        "- none".to_string()
    } else {
        values
            .iter()
            .map(|value| format!("- {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn upsert_context_nodes(target: &mut Vec<ContextNode>, nodes: Vec<ContextNode>) {
    for node in nodes {
        if let Some(existing) = target.iter_mut().find(|existing| existing.uri == node.uri) {
            *existing = node;
        } else {
            target.push(node);
        }
    }
}

fn rank_nodes(
    nodes: impl Iterator<Item = ContextNode>,
    query: &str,
    limit: usize,
) -> Vec<(ContextNode, f32)> {
    let mut scored: Vec<_> = nodes
        .map(|node| {
            let score = text_score(&format!("{} {}", node.title, node.body), query);
            (node, score)
        })
        .filter(|(_, score)| *score > 0.0)
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
    scored.truncate(limit);
    scored
}

fn retrieval_candidate(node: &ContextNode) -> bool {
    node.status == "active" && node.retrieval_enabled && node.retrieval_role == "fragment"
}

fn context_hit_from_node(node: &ContextNode, query: &str) -> ContextHit {
    ContextHit {
        uri: node.uri.clone(),
        title: node.title.clone(),
        layer: node.layer,
        score: text_score(&format!("{} {}", node.title, node.body), query),
        node_kind: Some(node.node_kind.clone()),
        retrieval_role: Some(node.retrieval_role.clone()),
        source_id: node.source_id.clone(),
        revision_id: node.revision_id.clone(),
        source_document_uri: node.source_document_uri.clone(),
        fragment_index: node.fragment_index,
        char_start: node.char_start,
        char_end: node.char_end,
        block_type: node.block_type.clone(),
        page_idx: node.page_idx,
        bbox: node.bbox.clone(),
        section_path: node.section_path.clone(),
        heading_level: node.heading_level,
        asset_refs: node.asset_refs.clone(),
        artifact_refs: node.artifact_refs.clone(),
        checksum: node.checksum.clone(),
        snippet: truncate_chars(&node.body, 240),
    }
}

fn source_document_context_node(document: SourceDocument) -> ContextNode {
    ContextNode {
        uri: document.uri.clone(),
        title: document.title.clone(),
        layer: 2,
        body: document.content.clone(),
        tenant_id: document.tenant_id.clone(),
        owner_user_id: document.owner_user_id.clone(),
        index_uid: "rag_source_documents".to_string(),
        index_kind: if document.owner_user_id.is_some() {
            "personal".to_string()
        } else {
            "company".to_string()
        },
        ancestor_uris: ancestor_uris(&document.uri),
        node_kind: "source_doc".to_string(),
        retrieval_role: "none".to_string(),
        retrieval_enabled: false,
        parent_uri: None,
        source_document_uri: Some(document.uri),
        fragment_index: None,
        char_start: None,
        char_end: None,
        token_estimate: Some(document.content.chars().count().div_ceil(4).max(1)),
        checksum: Some(document.checksum),
        source_id: Some(document.source_id),
        revision_id: Some(document.revision_id),
        block_type: None,
        page_idx: None,
        bbox: None,
        section_path: Vec::new(),
        heading_level: None,
        asset_refs: Vec::new(),
        artifact_refs: Vec::new(),
        status: document.status,
        privacy: if document.owner_user_id.is_some() {
            "private".to_string()
        } else {
            "company".to_string()
        },
        updated_at: document.updated_at,
    }
}

fn parsed_content(original: &str, parsed: &ParserOutput) -> String {
    parsed
        .markdown
        .clone()
        .filter(|content| !content.trim().is_empty())
        .unwrap_or_else(|| {
            if original.trim().is_empty() {
                parsed
                    .blocks
                    .iter()
                    .filter_map(parsed_block_text)
                    .collect::<Vec<_>>()
                    .join("\n\n")
            } else {
                original.to_string()
            }
        })
}

fn parsed_block_text(block: &ParsedBlock) -> Option<String> {
    block
        .text
        .clone()
        .or_else(|| block.html.clone())
        .or_else(|| block.latex.clone())
        .or_else(|| block.caption.clone())
        .filter(|value| !value.trim().is_empty())
}

#[allow(clippy::too_many_arguments)]
fn build_parse_artifacts(
    tenant_id: &str,
    owner_user_id: Option<String>,
    source_document_uri: &str,
    source_id: &str,
    revision_id: &str,
    parsed: &ParserOutput,
    original_content: &str,
) -> Result<Vec<ParseArtifact>, ApiError> {
    let mut artifacts = Vec::new();
    if !original_content.is_empty() {
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "original",
            format!("{source_document_uri}/artifacts/original"),
            original_content.as_bytes(),
        ));
    }
    if let Some(markdown) = parsed
        .markdown
        .as_deref()
        .filter(|markdown| !markdown.trim().is_empty())
    {
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "markdown",
            format!("{source_document_uri}/artifacts/markdown"),
            markdown.as_bytes(),
        ));
    }
    for (kind, value) in [
        ("content_list", parsed.content_list.as_ref()),
        ("content_list_v2", parsed.content_list_v2.as_ref()),
        ("middle_json", parsed.middle_json.as_ref()),
        ("model_json", parsed.model_json.as_ref()),
    ] {
        if let Some(value) = value {
            let bytes = serde_json::to_vec(value)
                .map_err(|err| ApiError::Internal(format!("failed to encode {kind}: {err}")))?;
            artifacts.push(parse_artifact_from_bytes(
                tenant_id,
                owner_user_id.clone(),
                source_document_uri,
                source_id,
                revision_id,
                parsed,
                kind,
                format!("{source_document_uri}/artifacts/{kind}"),
                &bytes,
            ));
        }
    }

    for (index, image) in parsed.images.iter().enumerate() {
        let uri = image_artifact_uri(source_document_uri, image, index as u32);
        let bytes = serde_json::to_vec(image)
            .map_err(|err| ApiError::Internal(format!("failed to encode image artifact: {err}")))?;
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "image",
            uri,
            &bytes,
        ));
    }

    for (index, image_ref) in parsed
        .blocks
        .iter()
        .filter_map(|block| block.image_ref.as_deref())
        .enumerate()
    {
        if artifacts.iter().any(|artifact| artifact.uri == image_ref) {
            continue;
        }
        artifacts.push(parse_artifact_from_bytes(
            tenant_id,
            owner_user_id.clone(),
            source_document_uri,
            source_id,
            revision_id,
            parsed,
            "image",
            image_ref.to_string(),
            image_ref.as_bytes(),
        ));
        if index > 10_000 {
            break;
        }
    }

    Ok(artifacts)
}

#[allow(clippy::too_many_arguments)]
fn parse_artifact_from_bytes(
    tenant_id: &str,
    owner_user_id: Option<String>,
    source_document_uri: &str,
    source_id: &str,
    revision_id: &str,
    parsed: &ParserOutput,
    kind: &str,
    uri: String,
    bytes: &[u8],
) -> ParseArtifact {
    ParseArtifact {
        id: parse_artifact_id(&uri),
        tenant_id: tenant_id.to_string(),
        owner_user_id,
        source_document_uri: source_document_uri.to_string(),
        source_id: source_id.to_string(),
        revision_id: revision_id.to_string(),
        parser_provider: parsed.provider.clone(),
        parser_backend: parsed.backend.clone(),
        parser_version: parsed.parser_version.clone(),
        artifact_kind: kind.to_string(),
        uri,
        checksum: sha256_hex(bytes),
        byte_size: bytes.len(),
        created_at: now(),
    }
}

fn image_artifact_uri(source_document_uri: &str, image: &Value, index: u32) -> String {
    image
        .as_str()
        .map(ToString::to_string)
        .or_else(|| {
            image
                .get("uri")
                .or_else(|| image.get("path"))
                .or_else(|| image.get("image_path"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| format!("{source_document_uri}/artifacts/images/{index:04}"))
}

fn parse_artifact_id(uri: &str) -> String {
    format!(
        "artifact_{}",
        sha256_hex(uri.as_bytes())
            .chars()
            .take(24)
            .collect::<String>()
    )
}

fn ingest_task_visible(
    task: &IngestTask,
    owner_user_id: Option<&str>,
    include_all_private: bool,
) -> bool {
    include_all_private
        || task.owner_user_id.is_none()
        || task.owner_user_id.as_deref() == owner_user_id
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn node_kind_for_layer(layer: u8) -> &'static str {
    match layer {
        0 => "abstract",
        1 => "overview",
        _ => "fragment",
    }
}

fn retrieval_role_for_layer(layer: u8) -> &'static str {
    match layer {
        2 => "fragment",
        1 => "overview",
        _ => "none",
    }
}

fn stage_value(stage: &str, hits: &[(ContextNode, f32)], owner_user_id: Option<&str>) -> Value {
    json!({
        "stage": stage,
        "owner_scoped": owner_user_id.is_some(),
        "hits": hits.iter().map(|(node, score)| json!({
            "uri": node.uri,
            "layer": node.layer,
            "score": score,
            "index_alias": node.index_kind,
        })).collect::<Vec<_>>()
    })
}

fn strip_layer_suffix(uri: &str) -> String {
    uri.strip_suffix("/.abstract")
        .or_else(|| uri.strip_suffix("/.overview"))
        .or_else(|| uri.strip_suffix("/detail"))
        .or_else(|| uri.strip_suffix("/chunks/0001"))
        .unwrap_or(uri)
        .to_string()
}

fn canonical_link_uri(uri: &str) -> String {
    strip_layer_suffix(uri.trim())
}

fn normalize_relation(relation: &str) -> String {
    let relation = relation.trim();
    if relation.is_empty() {
        "related".to_string()
    } else {
        sanitize_slug(relation)
    }
}

fn link_natural_key(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    source_uri: &str,
    target_uri: &str,
    relation: &str,
) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
        tenant_id,
        owner_user_id.unwrap_or(""),
        source_uri,
        target_uri,
        relation
    )
}

fn link_search_text(link: &KnowledgeLink) -> String {
    format!(
        "{} {} {} {} {} {} {}",
        link.source_uri,
        link.target_uri,
        link.source_title.as_deref().unwrap_or_default(),
        link.target_title.as_deref().unwrap_or_default(),
        link.relation,
        link.rationale.as_deref().unwrap_or_default(),
        link.tags.join(" ")
    )
}

fn owner_from_filters(filters: &Value) -> Option<&str> {
    filters
        .get("owner_user_id")
        .and_then(Value::as_str)
        .or_else(|| filters.get("owner").and_then(Value::as_str))
}

fn token_similarity(a: &str, b: &str) -> f32 {
    let left: HashSet<_> = a
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(ToString::to_string)
        .collect();
    let right: HashSet<_> = b
        .to_lowercase()
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(ToString::to_string)
        .collect();
    if left.is_empty() || right.is_empty() {
        return 0.0;
    }
    let intersection = left.intersection(&right).count() as f32;
    let union = left.union(&right).count() as f32;
    intersection / union
}

fn user_event_index_id(tenant_hash: &str, owner_user_id_hash: &str) -> String {
    format!("uei__t_{tenant_hash}__u_{owner_user_id_hash}")
}

fn source_document_id(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    source_id: &str,
    revision_id: &str,
) -> String {
    format!(
        "srcdoc_{}",
        hmac_hex(
            b"nowledge-source-document",
            "source_document",
            &format!(
                "{}:{}:{}:{}",
                tenant_id,
                owner_user_id.unwrap_or(""),
                source_id,
                revision_id
            ),
            24,
        )
    )
}

fn part_of_link_id(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    source_uri: &str,
    target_uri: &str,
) -> String {
    format!(
        "link_{}",
        hmac_hex(
            b"nowledge-part-of-link",
            "part_of",
            &link_natural_key(tenant_id, owner_user_id, source_uri, target_uri, "part_of"),
            24,
        )
    )
}

fn deterministic_stats(rows: &[Value], prior_rows_by_period: &[(String, Vec<Value>)]) -> Value {
    let mut numeric: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for (key, value) in obj {
                if let Some(number) = value.as_f64() {
                    numeric.entry(key.clone()).or_default().push(number);
                }
            }
        }
    }
    let prior_stats = prior_rows_by_period
        .iter()
        .map(|(period_key, rows)| (period_key.clone(), numeric_means(rows)))
        .collect::<Vec<_>>();
    let metrics = numeric
        .into_iter()
        .map(|(key, values)| {
            let count = values.len();
            let sum: f64 = values.iter().sum();
            let mean = if count == 0 { 0.0 } else { sum / count as f64 };
            let min = values.iter().copied().fold(f64::INFINITY, f64::min);
            let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            let previous_mean = prior_stats
                .first()
                .and_then(|(_, means)| means.get(&key))
                .copied();
            let recent_values = prior_stats
                .iter()
                .filter_map(|(_, means)| means.get(&key).copied())
                .collect::<Vec<_>>();
            let recent_4_mean = mean_of(&recent_values);
            let delta_vs_previous = previous_mean.map(|previous| mean - previous);
            let delta_vs_recent_4 = recent_4_mean.map(|recent| mean - recent);
            let trend_direction = trend_direction(delta_vs_recent_4.or(delta_vs_previous));
            let anomaly = recent_4_mean
                .map(|recent| {
                    let baseline = recent.abs().max(1.0);
                    ((mean - recent).abs() / baseline) >= 0.35
                })
                .unwrap_or(false);
            json!({
                "metric": key,
                "count": count,
                "mean": mean,
                "min": min,
                "max": max,
                "slope": simple_slope(&values),
                "previous_mean": previous_mean,
                "delta_vs_previous": delta_vs_previous,
                "recent_4_mean": recent_4_mean,
                "delta_vs_recent_4": delta_vs_recent_4,
                "trend_direction": trend_direction,
                "anomaly": anomaly
            })
        })
        .collect::<Vec<_>>();
    json!({
        "row_count": rows.len(),
        "prior_period_count": prior_rows_by_period.len(),
        "prior_periods": prior_rows_by_period
            .iter()
            .map(|(period_key, rows)| json!({
                "period_key": period_key,
                "row_count": rows.len()
            }))
            .collect::<Vec<_>>(),
        "metrics": metrics
    })
}

fn numeric_means(rows: &[Value]) -> HashMap<String, f64> {
    let mut numeric: HashMap<String, Vec<f64>> = HashMap::new();
    for row in rows {
        if let Some(obj) = row.as_object() {
            for (key, value) in obj {
                if let Some(number) = value.as_f64() {
                    numeric.entry(key.clone()).or_default().push(number);
                }
            }
        }
    }
    numeric
        .into_iter()
        .filter_map(|(key, values)| mean_of(&values).map(|mean| (key, mean)))
        .collect()
}

fn mean_of(values: &[f64]) -> Option<f64> {
    if values.is_empty() {
        None
    } else {
        Some(values.iter().sum::<f64>() / values.len() as f64)
    }
}

fn trend_direction(delta: Option<f64>) -> &'static str {
    match delta {
        Some(delta) if delta > 0.05 => "up",
        Some(delta) if delta < -0.05 => "down",
        Some(_) => "flat",
        None => "unknown",
    }
}

fn simple_slope(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    (values[values.len() - 1] - values[0]) / (values.len() - 1) as f64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[tokio::test]
    async fn source_doc_leak_guard_fails_when_source_doc_is_retrieved() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let uri = "ctx://test/source/leaky";
        let mut node = store.context_node(
            uri,
            "Leaky source doc",
            2,
            "source-doc-leak-keyword",
            "company",
            "rag_company_context",
            tenant_id,
            None,
            Some("leaky-source".to_string()),
            Some("v1".to_string()),
        );
        node.node_kind = "source_doc".to_string();
        node.retrieval_role = "fragment".to_string();
        node.retrieval_enabled = true;
        node.source_document_uri = Some(uri.to_string());
        {
            let mut data = store.write().unwrap();
            data.company_context.push(node);
            data.source_documents.insert(
                uri.to_string(),
                SourceDocument {
                    id: "source-doc-leak-fixture".to_string(),
                    tenant_id: tenant_id.to_string(),
                    owner_user_id: None,
                    source_kind: "test".to_string(),
                    source_id: "leaky-source".to_string(),
                    revision_id: "v1".to_string(),
                    uri: uri.to_string(),
                    title: "Leaky source doc".to_string(),
                    content: "source-doc-leak-keyword".to_string(),
                    checksum: "checksum".to_string(),
                    status: "active".to_string(),
                    retrieval_enabled: false,
                    created_at: now(),
                    updated_at: now(),
                },
            );
        }
        let case = store
            .create_eval_case(
                tenant_id,
                CreateRagEvalCaseRequest {
                    question: Some("source-doc-leak-keyword".to_string()),
                    ..CreateRagEvalCaseRequest::default()
                },
            )
            .unwrap();
        let run = store
            .create_eval_run_async(
                tenant_id,
                CreateRagEvalRunRequest {
                    case_ids: vec![case.id],
                    ..CreateRagEvalRunRequest::default()
                },
                false,
            )
            .await
            .unwrap();
        assert_eq!(run.status, "failed");
        assert!(run
            .guard_results
            .iter()
            .any(|guard| { guard.name == "source_doc_not_default_retrieved" && !guard.passed }));
    }

    #[tokio::test]
    async fn owner_acl_guard_fails_on_cross_owner_retrieval_and_blocks_run() {
        let config = Config::test();
        let store = Store::new(&config);
        let tenant_id = config.tenant_id.as_str();
        let routing = store
            .ensure_user_index(tenant_id, "u1", EnsureUserEventIndexRequest::default())
            .unwrap()
            .routing;
        let mut node = store.context_node(
            "ctx://test/private/cross-owner/fragments/0001",
            "Cross owner fragment",
            2,
            "cross-owner-leak-keyword",
            "personal",
            &routing.personal_context_index_uid,
            tenant_id,
            Some("u2".to_string()),
            Some("cross-owner-source".to_string()),
            Some("v1".to_string()),
        );
        node.node_kind = "fragment".to_string();
        node.retrieval_role = "fragment".to_string();
        node.retrieval_enabled = true;
        node.source_document_uri = Some("ctx://test/private/cross-owner/source".to_string());
        {
            let mut data = store.write().unwrap();
            data.personal_context
                .entry(routing.personal_context_index_uid)
                .or_default()
                .push(node);
        }
        let case = store
            .create_eval_case(
                tenant_id,
                CreateRagEvalCaseRequest {
                    owner_user_id: Some("u1".to_string()),
                    question: Some("cross-owner-leak-keyword".to_string()),
                    ..CreateRagEvalCaseRequest::default()
                },
            )
            .unwrap();
        let run = store
            .create_eval_run_async(
                tenant_id,
                CreateRagEvalRunRequest {
                    case_ids: vec![case.id],
                    ..CreateRagEvalRunRequest::default()
                },
                false,
            )
            .await
            .unwrap();
        assert_eq!(run.status, "failed");
        assert!(run
            .guard_results
            .iter()
            .any(|guard| guard.name == "owner_acl_never_leaks" && !guard.passed));
    }
}
