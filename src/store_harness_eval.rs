use super::*;

impl Store {
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

    fn create_harness_change(
        &self,
        tenant_id: &str,
        req: CreateHarnessChangeManifestRequest,
    ) -> Result<HarnessChangeManifest, ApiError> {
        let id = match req.id {
            Some(id) => require_string(Some(id), "id")?,
            None => new_id("hchange"),
        };
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
            id,
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
            baseline_eval_run_id: req.baseline_eval_run_id,
            candidate_eval_run_id: req.candidate_eval_run_id,
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
        Ok(change)
    }

    pub async fn create_harness_change_async(
        &self,
        tenant_id: &str,
        req: CreateHarnessChangeManifestRequest,
    ) -> Result<HarnessChangeManifest, ApiError> {
        let (change, _) = self
            .execute_staged_mutation(
                tenant_id,
                "harness_change.create",
                None,
                None,
                MutationPrimary::HarnessChanges,
                |staged| staged.create_harness_change(tenant_id, req),
            )
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

    fn create_harness_component_revision(
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
        let _ = (component, revisions, change);
        Ok(revision)
    }

    pub async fn create_harness_component_revision_async(
        &self,
        tenant_id: &str,
        component_id: &str,
        req: CreateHarnessComponentRevisionRequest,
    ) -> Result<HarnessComponentRevision, ApiError> {
        let (revision, _) = self
            .execute_staged_mutation(
                tenant_id,
                "harness_component.revise",
                None,
                None,
                MutationPrimary::HarnessComponents,
                |staged| staged.create_harness_component_revision(tenant_id, component_id, req),
            )
            .await?;
        Ok(revision)
    }

    fn rollback_harness_component(
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
        let _ = (revisions, manifest);
        Ok(HarnessRollbackResponse {
            component,
            active_revision,
            history_event_id: history.event.id,
        })
    }

    pub async fn rollback_harness_component_async(
        &self,
        tenant_id: &str,
        component_id: &str,
        req: RollbackHarnessComponentRequest,
    ) -> Result<HarnessRollbackResponse, ApiError> {
        let (response, _) = self
            .execute_staged_mutation(
                tenant_id,
                "harness_component.rollback",
                None,
                None,
                MutationPrimary::HarnessComponents,
                |staged| staged.rollback_harness_component(tenant_id, component_id, req),
            )
            .await?;
        Ok(response)
    }

    fn create_harness_verdict(
        &self,
        tenant_id: &str,
        change_id: &str,
        req: CreateHarnessChangeVerdictRequest,
    ) -> Result<HarnessChangeVerdict, ApiError> {
        let created_by = req.created_by.unwrap_or_else(|| "admin".to_string());
        let delta = self
            .compare_harness_change(change_id, None, req.eval_run_id.clone())
            .ok();
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
        let (predicted_fixes_confirmed, risk_cases_regressed, verdict, evidence) = if let Some(
            delta,
        ) = delta
        {
            let risk_cases_regressed = delta
                .risk_matrix
                .iter()
                .filter(|risk| risk.regressed)
                .map(|risk| risk.case_id.clone())
                .collect::<Vec<_>>();
            let predicted_fixes_confirmed =
                predicted_fix_confirmations(&change.predicted_fixes, &delta);
            let verdict = if !risk_cases_regressed.is_empty() {
                if predicted_fixes_confirmed.is_empty() {
                    "rollback_and_pivot"
                } else {
                    "rollback"
                }
            } else if !predicted_fixes_confirmed.is_empty()
                && predicted_fixes_confirmed.len() >= change.predicted_fixes.len().max(1)
            {
                "keep"
            } else {
                "improve"
            }
            .to_string();
            (
                predicted_fixes_confirmed,
                risk_cases_regressed,
                verdict,
                json!({ "delta": delta }),
            )
        } else {
            let evidence_text = verdict_evidence_text(run.as_ref(), overview.as_ref());
            let risk_cases_regressed = change
                .risk_cases
                .iter()
                .filter(|risk| contains_folded(&evidence_text, risk))
                .cloned()
                .collect::<Vec<_>>();
            let predicted_fixes_confirmed =
                if run.as_ref().is_some_and(|run| run.status == "passed") {
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
            (
                predicted_fixes_confirmed,
                risk_cases_regressed,
                verdict,
                json!({
                    "change_failure_pattern": change.failure_pattern,
                    "eval_run_status": run.as_ref().map(|run| run.status.clone()),
                    "overview": overview.as_ref().map(|overview| overview.overview_markdown.clone())
                }),
            )
        };
        let verdict_record = HarnessChangeVerdict {
            id: new_id("hverdict"),
            tenant_id: tenant_id.to_string(),
            change_id: change_id.to_string(),
            eval_run_id: run.as_ref().map(|run| run.id.clone()),
            verdict,
            predicted_fixes_confirmed,
            risk_cases_regressed,
            observed_metric_deltas,
            evidence,
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
        Ok(verdict_record)
    }

    pub async fn create_harness_verdict_async(
        &self,
        tenant_id: &str,
        change_id: &str,
        req: CreateHarnessChangeVerdictRequest,
    ) -> Result<HarnessChangeVerdict, ApiError> {
        let (verdict, _) = self
            .execute_staged_mutation(
                tenant_id,
                "harness_verdict.create",
                None,
                None,
                MutationPrimary::HarnessVerdicts,
                |staged| staged.create_harness_verdict(tenant_id, change_id, req),
            )
            .await?;
        Ok(verdict)
    }

    pub fn compare_harness_change(
        &self,
        change_id: &str,
        baseline_eval_run_id: Option<String>,
        candidate_eval_run_id: Option<String>,
    ) -> Result<EvalDeltaReport, ApiError> {
        let data = self.read()?;
        let change = data
            .harness_changes
            .get(change_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("harness change not found"))?;
        let baseline_run_id = baseline_eval_run_id
            .or(change.baseline_eval_run_id.clone())
            .ok_or_else(|| ApiError::bad_request("baseline_eval_run_id is required"))?;
        let candidate_run_id = candidate_eval_run_id
            .or(change.candidate_eval_run_id.clone())
            .or_else(|| latest_eval_run_for_change(&data, change_id).map(|run| run.id))
            .ok_or_else(|| ApiError::bad_request("candidate_eval_run_id is required"))?;
        let baseline_run = data
            .eval_runs
            .get(&baseline_run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("baseline eval run not found"))?;
        let candidate_run = data
            .eval_runs
            .get(&candidate_run_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("candidate eval run not found"))?;
        let baseline_results = eval_results_by_case(&data, &baseline_run);
        let candidate_results = eval_results_by_case(&data, &candidate_run);
        let mut case_ids = baseline_results
            .keys()
            .chain(candidate_results.keys())
            .cloned()
            .collect::<HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        case_ids.sort();

        let mut fixed_cases = Vec::new();
        let mut regressed_cases = Vec::new();
        let mut unchanged_failed_cases = Vec::new();
        let mut unchanged_passed_cases = Vec::new();
        for case_id in &case_ids {
            let baseline_status = baseline_results
                .get(case_id)
                .map(|result| result.status.as_str())
                .unwrap_or("missing");
            let candidate_status = candidate_results
                .get(case_id)
                .map(|result| result.status.as_str())
                .unwrap_or("missing");
            match (baseline_status, candidate_status) {
                ("failed", "passed") => fixed_cases.push(case_id.clone()),
                ("passed", "failed") => regressed_cases.push(case_id.clone()),
                ("failed", "failed") => unchanged_failed_cases.push(case_id.clone()),
                ("passed", "passed") => unchanged_passed_cases.push(case_id.clone()),
                _ => {}
            }
        }

        let risk_matrix = risk_matrix_for_change(
            &change,
            &baseline_results,
            &candidate_results,
            &regressed_cases,
        );

        Ok(EvalDeltaReport {
            change_id: change_id.to_string(),
            baseline_run_id,
            candidate_run_id,
            fixed_cases,
            regressed_cases,
            unchanged_failed_cases,
            unchanged_passed_cases,
            metric_deltas: metric_deltas(&baseline_run.metrics, &candidate_run.metrics),
            risk_matrix,
            generated_at: now(),
        })
    }

    pub fn create_eval_case(
        &self,
        tenant_id: &str,
        req: CreateRagEvalCaseRequest,
    ) -> Result<RagEvalCase, ApiError> {
        let id = match req.id {
            Some(id) => require_string(Some(id), "id")?,
            None => new_id("evalcase"),
        };
        let question = require_string(req.question, "question")?;
        let case = RagEvalCase {
            id,
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

    pub async fn create_eval_case_async(
        &self,
        tenant_id: &str,
        req: CreateRagEvalCaseRequest,
    ) -> Result<RagEvalCase, ApiError> {
        let (case, _) = self
            .execute_staged_mutation(
                tenant_id,
                "eval_case.create",
                None,
                None,
                MutationPrimary::EvalCase,
                |staged| staged.create_eval_case(tenant_id, req),
            )
            .await?;
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
                    false,
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
        let run = RagEvalRun {
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
        let overview = build_eval_overview(&run, &results);
        let run_for_stage = run.clone();
        let overview_for_stage = overview.clone();
        let results_for_stage = results.clone();
        let (persisted_run, _) = self
            .execute_staged_mutation(
                tenant_id,
                "eval_run.create",
                None,
                None,
                MutationPrimary::EvalRun,
                move |staged| {
                    let mut run = run_for_stage;
                    let mut overview = overview_for_stage;
                    let mut data = staged.write()?;
                    staged.write_eval_reports_locked(
                        &mut data,
                        tenant_id,
                        &mut run,
                        &mut overview,
                        &results_for_stage,
                    );
                    for result in &results_for_stage {
                        data.eval_case_results
                            .insert(result.id.clone(), result.clone());
                    }
                    data.eval_overviews.insert(run.id.clone(), overview);
                    data.eval_runs.insert(run.id.clone(), run.clone());
                    Ok(run)
                },
            )
            .await?;
        Ok(persisted_run)
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
}
