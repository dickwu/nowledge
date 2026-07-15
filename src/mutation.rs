use std::collections::{BTreeMap, BTreeSet, HashSet};

use chrono::{DateTime, Utc};
use thiserror::Error;

use crate::models::{
    CompanySourceDeleteTarget, OperationActorScope, OperationIndexingState, OperationListItem,
    OperationPlan, OperationProgress, OperationRecord, OperationResource, OperationStatus,
    OperationStep, OperationStepProgress, OperationStepRole, OperationStepStatus, OperationSummary,
    PersistenceMetadata, RepositoryWriteReceipt,
};

pub const OPERATION_PLAN_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum MutationPlanError {
    #[error("invalid operation plan: {0}")]
    InvalidPlan(String),
    #[error("operation step `{0}` does not exist")]
    UnknownStep(String),
    #[error("invalid transition for operation step `{step_id}`: {reason}")]
    InvalidTransition { step_id: String, reason: String },
}

impl OperationStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::PrimaryCommitted => "primary_committed",
            Self::EffectsSubmitted => "effects_submitted",
            Self::PartiallyFailed => "partially_failed",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl OperationIndexingState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }
}

impl RepositoryWriteReceipt {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_task_uid(task_uid: Option<String>) -> Self {
        Self {
            task_uids: task_uid.into_iter().collect(),
        }
    }

    pub fn extend(&mut self, other: Self) {
        let mut seen = self.task_uids.iter().cloned().collect::<HashSet<_>>();
        self.task_uids.extend(
            other
                .task_uids
                .into_iter()
                .filter(|task_uid| !task_uid.is_empty() && seen.insert(task_uid.clone())),
        );
    }
}

pub fn validate_operation_plan(plan: &OperationPlan) -> Result<(), MutationPlanError> {
    if plan.schema_version != OPERATION_PLAN_SCHEMA_VERSION {
        return invalid(format!(
            "unsupported schema version {}; expected {OPERATION_PLAN_SCHEMA_VERSION}",
            plan.schema_version
        ));
    }
    require_non_empty("operation id", &plan.id)?;
    require_non_empty("tenant id", &plan.tenant_id)?;
    require_non_empty("operation kind", &plan.operation_kind)?;
    if let Some(hash) = &plan.idempotency_key_hash {
        require_non_empty("idempotency key hash", hash)?;
    }
    if let Some(request_id) = &plan.actor.request_id {
        require_non_empty("request id", request_id)?;
    }
    if plan.actor.scope == OperationActorScope::Owner {
        require_non_empty(
            "owner actor hash",
            plan.actor.owner_user_id_hash.as_deref().unwrap_or_default(),
        )?;
    }
    validate_unique_non_empty("actor role", &plan.actor.roles)?;
    if !plan.redacted_metadata.is_null() && !plan.redacted_metadata.is_object() {
        return invalid("redacted metadata must be an object or null");
    }

    if plan.primary.role != OperationStepRole::Primary {
        return invalid("primary step must have the primary role");
    }
    let mut step_ids = HashSet::new();
    validate_step(&plan.tenant_id, &plan.primary, &mut step_ids)?;
    for step in &plan.side_effects {
        if step.role != OperationStepRole::SideEffect {
            return invalid(format!(
                "side-effect step `{}` must have the side_effect role",
                step.id
            ));
        }
        validate_step(&plan.tenant_id, step, &mut step_ids)?;
    }
    validate_company_source_delete_steps(plan)?;
    validate_analysis_materialization_steps(plan)?;
    Ok(())
}

fn validate_analysis_materialization_steps(plan: &OperationPlan) -> Result<(), MutationPlanError> {
    if plan.operation_kind != "analysis.materialize" {
        return Ok(());
    }

    let expected_owner_hash = plan
        .redacted_metadata
        .get("target_owner_user_id_hash")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            MutationPlanError::InvalidPlan(
                "analysis materialization is missing its target owner commitment".to_string(),
            )
        })?;
    require_non_empty("analysis target owner hash", expected_owner_hash)?;

    let mut target_owner = None;
    for step in operation_steps(plan) {
        match &step.resource {
            OperationResource::EnsureUserEventIndex { index } => {
                if index.owner_user_id_hash != expected_owner_hash {
                    return invalid(
                        "analysis user-index resource does not match its target owner commitment",
                    );
                }
            }
            OperationResource::HistoryEvents { events } => {
                for event in events {
                    if event.owner_user_id_hash != expected_owner_hash {
                        return invalid(
                            "analysis history resource does not match its target owner commitment",
                        );
                    }
                    bind_analysis_owner(&mut target_owner, &event.owner_user_id)?;
                }
            }
            OperationResource::ContextNodes { nodes, .. } => {
                for node in nodes {
                    let owner = node.owner_user_id.as_deref().ok_or_else(|| {
                        MutationPlanError::InvalidPlan(
                            "analysis context resource must be owner scoped".to_string(),
                        )
                    })?;
                    bind_analysis_owner(&mut target_owner, owner)?;
                }
            }
            OperationResource::Insight { insight } => {
                bind_analysis_owner(&mut target_owner, &insight.owner_user_id)?;
            }
            OperationResource::Links { links } => {
                for link in links {
                    let owner = link.owner_user_id.as_deref().ok_or_else(|| {
                        MutationPlanError::InvalidPlan(
                            "analysis link resource must be owner scoped".to_string(),
                        )
                    })?;
                    bind_analysis_owner(&mut target_owner, owner)?;
                }
            }
            _ => {
                return invalid(
                    "analysis materialization contains an unsupported operation resource",
                );
            }
        }
    }
    if target_owner.is_none() {
        return invalid("analysis materialization contains no owner-scoped resource");
    }
    Ok(())
}

fn bind_analysis_owner<'a>(
    target_owner: &mut Option<&'a str>,
    owner: &'a str,
) -> Result<(), MutationPlanError> {
    require_non_empty("analysis resource owner", owner)?;
    match target_owner {
        Some(expected) if *expected != owner => {
            invalid("analysis materialization mixes target owners")
        }
        Some(_) => Ok(()),
        None => {
            *target_owner = Some(owner);
            Ok(())
        }
    }
}

fn validate_company_source_delete_steps(plan: &OperationPlan) -> Result<(), MutationPlanError> {
    let mut next_target_by_source = BTreeMap::<&str, usize>::new();
    for step in operation_steps(plan) {
        let OperationResource::DeleteCompanySourceIndex { source_id, target } = &step.resource
        else {
            continue;
        };
        let target_index = match target {
            CompanySourceDeleteTarget::Fragments => 0,
            CompanySourceDeleteTarget::Revisions => 1,
            CompanySourceDeleteTarget::Source => 2,
            CompanySourceDeleteTarget::SourceDocuments => 3,
            CompanySourceDeleteTarget::ParseArtifacts => 4,
            CompanySourceDeleteTarget::IngestTasks => 5,
            CompanySourceDeleteTarget::IngestResults => 6,
            CompanySourceDeleteTarget::Links { .. } => 7,
        };
        let expected = next_target_by_source.entry(source_id.as_str()).or_default();
        if target_index != *expected {
            return invalid(format!(
                "company source `{source_id}` deletion target is out of order at step `{}`",
                step.id
            ));
        }
        *expected += 1;
    }
    for (source_id, next_target) in next_target_by_source {
        if next_target < 7 {
            return invalid(format!(
                "company source `{source_id}` deletion omits a required managed-index step"
            ));
        }
    }
    Ok(())
}

pub fn validate_operation_step_for_tenant(
    tenant_id: &str,
    step: &OperationStep,
) -> Result<(), MutationPlanError> {
    require_non_empty("tenant id", tenant_id)?;
    require_non_empty("step id", &step.id)?;
    validate_resource(tenant_id, &step.resource)
}

pub fn validate_operation_record(record: &OperationRecord) -> Result<(), MutationPlanError> {
    validate_operation_plan(&record.plan)?;
    if record.id != record.plan.id
        || record.tenant_id != record.plan.tenant_id
        || record.operation_kind != record.plan.operation_kind
        || record.actor_scope != record.plan.actor.scope
        || record.idempotency_key_hash != record.plan.idempotency_key_hash
        || record.created_at != record.plan.created_at
    {
        return invalid("record identity does not match its immutable plan");
    }
    if record.updated_at < record.created_at {
        return invalid("record updated_at precedes created_at");
    }

    let expected_step_ids = operation_steps(&record.plan)
        .map(|step| step.id.as_str())
        .collect::<BTreeSet<_>>();
    let actual_step_ids = record
        .progress
        .steps
        .keys()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if expected_step_ids != actual_step_ids {
        return invalid("record progress does not cover exactly the planned steps");
    }
    for (step_id, progress) in &record.progress.steps {
        if progress.step_id != *step_id {
            return invalid(format!(
                "progress key `{step_id}` does not match embedded step id"
            ));
        }
        if progress.updated_at < record.created_at || progress.updated_at > record.updated_at {
            return invalid(format!("step `{step_id}` has an invalid update timestamp"));
        }
        validate_task_uids(step_id, &progress.task_uids)?;
        if progress.status == OperationStepStatus::Submitted && progress.task_uids.is_empty() {
            return invalid(format!(
                "submitted step `{step_id}` must retain at least one task UID"
            ));
        }
    }

    let (expected_status, expected_indexing_state) = derive_operation_state(record)?;
    if record.status != expected_status || record.indexing_state != expected_indexing_state {
        return invalid("record status does not match step progress");
    }
    if (record.status == OperationStatus::Completed) != record.completed_at.is_some() {
        return invalid("completed_at must be present only for completed operations");
    }
    Ok(())
}

pub fn operation_record_from_plan(
    plan: OperationPlan,
) -> Result<OperationRecord, MutationPlanError> {
    validate_operation_plan(&plan)?;
    let created_at = plan.created_at;
    let steps = operation_steps(&plan)
        .map(|step| {
            (
                step.id.clone(),
                OperationStepProgress {
                    step_id: step.id.clone(),
                    status: OperationStepStatus::Pending,
                    attempts: 0,
                    task_uids: Vec::new(),
                    last_error_category: None,
                    last_error_fingerprint: None,
                    updated_at: created_at,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    Ok(OperationRecord {
        id: plan.id.clone(),
        tenant_id: plan.tenant_id.clone(),
        operation_kind: plan.operation_kind.clone(),
        actor_scope: plan.actor.scope,
        idempotency_key_hash: plan.idempotency_key_hash.clone(),
        plan,
        status: OperationStatus::Pending,
        indexing_state: OperationIndexingState::Pending,
        progress: OperationProgress {
            attempt_count: 0,
            steps,
        },
        created_at,
        updated_at: created_at,
        completed_at: None,
        last_error_category: None,
        last_error_fingerprint: None,
    })
}

pub fn operation_step_submitted(
    record: &OperationRecord,
    step_id: &str,
    task_uids: Vec<String>,
    at: DateTime<Utc>,
) -> Result<OperationRecord, MutationPlanError> {
    validate_operation_record(record)?;
    validate_task_uids(step_id, &task_uids)?;
    if task_uids.is_empty() {
        return invalid_transition(step_id, "submission requires at least one task UID");
    }
    validate_step_sequence(record, step_id)?;

    let mut next = record.clone();
    let progress = next
        .progress
        .steps
        .get_mut(step_id)
        .ok_or_else(|| MutationPlanError::UnknownStep(step_id.to_string()))?;
    match progress.status {
        OperationStepStatus::Pending | OperationStepStatus::Failed => {}
        OperationStepStatus::Submitted => {
            return invalid_transition(step_id, "step is already submitted")
        }
        OperationStepStatus::Completed => {
            return invalid_transition(step_id, "completed steps are immutable")
        }
    }
    progress.status = OperationStepStatus::Submitted;
    progress.attempts = progress.attempts.saturating_add(1);
    progress.task_uids = dedup_non_empty(task_uids);
    progress.last_error_category = None;
    progress.last_error_fingerprint = None;
    progress.updated_at = at;
    next.progress.attempt_count = next.progress.attempt_count.saturating_add(1);
    refresh_operation_state(&mut next, at)?;
    Ok(next)
}

/// Record that a backend durably accepted this step. For task-based Meili
/// writes this is the read-your-writes commit boundary: callers may publish
/// the cache while `indexing_state` remains pending. Confirm the task UIDs by
/// calling `operation_step_completed` after the backend wait succeeds.
pub fn operation_step_accepted(
    record: &OperationRecord,
    step_id: &str,
    task_uids: Vec<String>,
    at: DateTime<Utc>,
) -> Result<OperationRecord, MutationPlanError> {
    operation_step_submitted(record, step_id, task_uids, at)
}

pub fn operation_step_completed(
    record: &OperationRecord,
    step_id: &str,
    at: DateTime<Utc>,
) -> Result<OperationRecord, MutationPlanError> {
    validate_operation_record(record)?;
    validate_step_sequence(record, step_id)?;

    let mut next = record.clone();
    let progress = next
        .progress
        .steps
        .get_mut(step_id)
        .ok_or_else(|| MutationPlanError::UnknownStep(step_id.to_string()))?;
    match progress.status {
        OperationStepStatus::Completed => {
            return invalid_transition(step_id, "completed steps are immutable")
        }
        OperationStepStatus::Failed => {
            return invalid_transition(step_id, "failed steps must be retried before completion")
        }
        OperationStepStatus::Pending => {
            progress.attempts = progress.attempts.saturating_add(1);
            next.progress.attempt_count = next.progress.attempt_count.saturating_add(1);
        }
        OperationStepStatus::Submitted => {}
    }
    progress.status = OperationStepStatus::Completed;
    progress.last_error_category = None;
    progress.last_error_fingerprint = None;
    progress.updated_at = at;
    refresh_operation_state(&mut next, at)?;
    Ok(next)
}

pub fn operation_step_failed(
    record: &OperationRecord,
    step_id: &str,
    category: impl Into<String>,
    fingerprint: impl Into<String>,
    at: DateTime<Utc>,
) -> Result<OperationRecord, MutationPlanError> {
    validate_operation_record(record)?;
    validate_step_sequence(record, step_id)?;
    let category = category.into();
    let fingerprint = fingerprint.into();
    require_non_empty("error category", &category)?;
    require_non_empty("error fingerprint", &fingerprint)?;

    let mut next = record.clone();
    let progress = next
        .progress
        .steps
        .get_mut(step_id)
        .ok_or_else(|| MutationPlanError::UnknownStep(step_id.to_string()))?;
    if progress.status == OperationStepStatus::Completed {
        return invalid_transition(step_id, "completed steps are immutable");
    }
    if progress.status != OperationStepStatus::Submitted {
        progress.attempts = progress.attempts.saturating_add(1);
        next.progress.attempt_count = next.progress.attempt_count.saturating_add(1);
    }
    progress.status = OperationStepStatus::Failed;
    progress.last_error_category = Some(category);
    progress.last_error_fingerprint = Some(fingerprint);
    progress.updated_at = at;
    refresh_operation_state(&mut next, at)?;
    Ok(next)
}

pub fn persistence_metadata(record: &OperationRecord) -> PersistenceMetadata {
    let primary_task_uids = record
        .progress
        .steps
        .get(&record.plan.primary.id)
        .map(|progress| dedup_non_empty(progress.task_uids.clone()))
        .unwrap_or_default();
    let mut task_uids = primary_task_uids.clone();
    let mut seen = task_uids.iter().cloned().collect::<HashSet<_>>();
    for step in &record.plan.side_effects {
        if let Some(progress) = record.progress.steps.get(&step.id) {
            task_uids.extend(
                progress
                    .task_uids
                    .iter()
                    .filter(|task_uid| !task_uid.is_empty() && seen.insert((*task_uid).clone()))
                    .cloned(),
            );
        }
    }
    PersistenceMetadata {
        operation_id: record.id.clone(),
        status: record.status,
        indexing_state: record.indexing_state,
        primary_task_uids,
        task_uids,
    }
}

pub fn operation_summary(record: &OperationRecord) -> OperationSummary {
    OperationSummary {
        id: record.id.clone(),
        tenant_id: record.tenant_id.clone(),
        operation_kind: record.operation_kind.clone(),
        actor_scope: record.actor_scope,
        idempotency_key_hash: record.idempotency_key_hash.clone(),
        status: record.status,
        indexing_state: record.indexing_state,
        attempt_count: record.progress.attempt_count,
        pending_steps: record
            .progress
            .steps
            .values()
            .filter(|progress| {
                matches!(
                    progress.status,
                    OperationStepStatus::Pending | OperationStepStatus::Submitted
                )
            })
            .count(),
        failed_steps: record
            .progress
            .steps
            .values()
            .filter(|progress| progress.status == OperationStepStatus::Failed)
            .count(),
        created_at: record.created_at,
        updated_at: record.updated_at,
        completed_at: record.completed_at,
        last_error_category: record.last_error_category.clone(),
        last_error_fingerprint: record.last_error_fingerprint.clone(),
    }
}

pub fn operation_list_item(record: &OperationRecord, include_plan: bool) -> OperationListItem {
    OperationListItem {
        summary: operation_summary(record),
        plan: include_plan.then(|| record.plan.clone()),
    }
}

fn validate_step(
    tenant_id: &str,
    step: &OperationStep,
    step_ids: &mut HashSet<String>,
) -> Result<(), MutationPlanError> {
    require_non_empty("step id", &step.id)?;
    if !step_ids.insert(step.id.clone()) {
        return invalid(format!("duplicate step id `{}`", step.id));
    }
    validate_resource(tenant_id, &step.resource)
}

fn validate_resource(
    tenant_id: &str,
    resource: &OperationResource,
) -> Result<(), MutationPlanError> {
    match resource {
        OperationResource::EnsureUserEventIndex { index } => {
            tenant_matches(tenant_id, &index.tenant_id, "event index")?;
            require_non_empty("event index id", &index.id)?;
        }
        OperationResource::HistoryEvents { events } => {
            require_non_empty_slice("history events", events)?;
            let event_index_uid = &events[0].event_index_uid;
            let owner_hash = &events[0].owner_user_id_hash;
            require_non_empty("event index UID", event_index_uid)?;
            require_non_empty("event owner hash", owner_hash)?;
            for event in events {
                tenant_matches(tenant_id, &event.tenant_id, "history event")?;
                if event.event_index_uid != *event_index_uid
                    || event.owner_user_id_hash != *owner_hash
                {
                    return invalid(
                        "a history-event batch must use one event index and owner hash",
                    );
                }
            }
        }
        OperationResource::ContextNodes { index_uid, nodes } => {
            require_non_empty("context index UID", index_uid)?;
            require_non_empty_slice("context nodes", nodes)?;
            for node in nodes {
                tenant_matches(tenant_id, &node.tenant_id, "context node")?;
                if node.index_uid != *index_uid {
                    return invalid("context node index UID does not match the step index UID");
                }
            }
        }
        OperationResource::StateItem { item } => {
            tenant_matches(tenant_id, &item.tenant_id, "state item")?;
        }
        OperationResource::Insight { insight } => {
            tenant_matches(tenant_id, &insight.tenant_id, "insight")?;
        }
        OperationResource::CompanySource { source } => {
            tenant_matches(tenant_id, &source.tenant_id, "company source")?;
        }
        OperationResource::SourceRevision { revision } => {
            tenant_matches(tenant_id, &revision.tenant_id, "source revision")?;
        }
        OperationResource::DeleteCompanySourceIndex { source_id, target } => {
            require_non_empty("company source id", source_id)?;
            if let CompanySourceDeleteTarget::Links {
                link_ids,
                related_uris,
            } = target
            {
                if link_ids.is_empty() && related_uris.is_empty() {
                    return invalid(
                        "company source link deletion requires link ids or related URIs",
                    );
                }
                validate_unique_non_empty("company source link id", link_ids)?;
                validate_unique_non_empty("company source related URI", related_uris)?;
            }
        }
        OperationResource::SourceDocuments { documents } => {
            require_non_empty_slice("source documents", documents)?;
            for document in documents {
                tenant_matches(tenant_id, &document.tenant_id, "source document")?;
            }
        }
        OperationResource::ParseArtifacts { artifacts } => {
            require_non_empty_slice("parse artifacts", artifacts)?;
            for artifact in artifacts {
                tenant_matches(tenant_id, &artifact.tenant_id, "parse artifact")?;
            }
        }
        OperationResource::StructuredSnapshot { snapshot } => {
            tenant_matches(tenant_id, &snapshot.tenant_id, "structured snapshot")?;
        }
        OperationResource::Dataset { dataset } => {
            tenant_matches(tenant_id, &dataset.tenant_id, "dataset")?;
        }
        OperationResource::StructuredRows { rows } => {
            require_non_empty_slice("structured rows", rows)?;
        }
        OperationResource::StructuredSummary { summary } => {
            if summary.is_null() {
                return invalid("structured summary must not be null");
            }
        }
        OperationResource::Session { session } => {
            tenant_matches(tenant_id, &session.tenant_id, "session")?;
        }
        OperationResource::Trace { trace } => {
            tenant_matches(tenant_id, &trace.tenant_id, "trace")?;
        }
        OperationResource::Links { links } => {
            require_non_empty_slice("links", links)?;
            for link in links {
                tenant_matches(tenant_id, &link.tenant_id, "link")?;
            }
        }
        OperationResource::HarnessComponents {
            components,
            revisions,
        } => {
            if components.is_empty() && revisions.is_empty() {
                return invalid("harness component step has no resources");
            }
            for component in components {
                tenant_matches(tenant_id, &component.tenant_id, "harness component")?;
            }
            for revision in revisions {
                tenant_matches(tenant_id, &revision.tenant_id, "harness revision")?;
            }
        }
        OperationResource::HarnessChanges { changes } => {
            require_non_empty_slice("harness changes", changes)?;
            for change in changes {
                tenant_matches(tenant_id, &change.tenant_id, "harness change")?;
            }
        }
        OperationResource::HarnessVerdicts { verdicts } => {
            require_non_empty_slice("harness verdicts", verdicts)?;
            for verdict in verdicts {
                tenant_matches(tenant_id, &verdict.tenant_id, "harness verdict")?;
            }
        }
        OperationResource::IngestTask { task } => {
            tenant_matches(tenant_id, &task.tenant_id, "ingest task")?;
        }
        OperationResource::IngestTasks { tasks } => {
            require_non_empty_slice("ingest tasks", tasks)?;
            for task in tasks {
                tenant_matches(tenant_id, &task.tenant_id, "ingest task")?;
            }
        }
        OperationResource::DeleteIngestTasks { task_ids } => {
            validate_unique_non_empty("ingest task id", task_ids)?;
            if task_ids.is_empty() {
                return invalid("ingest task ids must not be empty");
            }
        }
        OperationResource::IngestResult { result } => {
            tenant_matches(tenant_id, &result.task.tenant_id, "ingest result")?;
            for artifact in &result.parse_artifacts {
                tenant_matches(tenant_id, &artifact.tenant_id, "ingest result artifact")?;
            }
        }
        OperationResource::EvalCase { case } => {
            tenant_matches(tenant_id, &case.tenant_id, "evaluation case")?;
        }
        OperationResource::EvalRun { run } => {
            tenant_matches(tenant_id, &run.tenant_id, "evaluation run")?;
        }
        OperationResource::EvalCaseResults { results } => {
            require_non_empty_slice("evaluation case results", results)?;
            for result in results {
                tenant_matches(tenant_id, &result.tenant_id, "evaluation case result")?;
            }
        }
        OperationResource::EvalOverview { overview } => {
            tenant_matches(tenant_id, &overview.tenant_id, "evaluation overview")?;
        }
    }
    Ok(())
}

fn validate_step_sequence(
    record: &OperationRecord,
    step_id: &str,
) -> Result<(), MutationPlanError> {
    let step = operation_steps(&record.plan)
        .find(|step| step.id == step_id)
        .ok_or_else(|| MutationPlanError::UnknownStep(step_id.to_string()))?;
    if step.role == OperationStepRole::SideEffect {
        let primary = record
            .progress
            .steps
            .get(&record.plan.primary.id)
            .ok_or_else(|| MutationPlanError::UnknownStep(record.plan.primary.id.clone()))?;
        if !matches!(
            primary.status,
            OperationStepStatus::Submitted | OperationStepStatus::Completed
        ) {
            return invalid_transition(step_id, "primary step has not been durably accepted");
        }
    }
    Ok(())
}

fn refresh_operation_state(
    record: &mut OperationRecord,
    at: DateTime<Utc>,
) -> Result<(), MutationPlanError> {
    if at < record.updated_at {
        return invalid("operation transition timestamp moved backwards");
    }
    record.updated_at = at;
    let (status, indexing_state) = derive_operation_state(record)?;
    record.status = status;
    record.indexing_state = indexing_state;
    record.completed_at = if status == OperationStatus::Completed {
        record.completed_at.or(Some(at))
    } else {
        None
    };

    let failed = record
        .progress
        .steps
        .values()
        .find(|progress| progress.status == OperationStepStatus::Failed);
    record.last_error_category = failed.and_then(|value| value.last_error_category.clone());
    record.last_error_fingerprint = failed.and_then(|value| value.last_error_fingerprint.clone());
    validate_operation_record(record)
}

fn derive_operation_state(
    record: &OperationRecord,
) -> Result<(OperationStatus, OperationIndexingState), MutationPlanError> {
    let primary = record
        .progress
        .steps
        .get(&record.plan.primary.id)
        .ok_or_else(|| MutationPlanError::UnknownStep(record.plan.primary.id.clone()))?;
    match primary.status {
        OperationStepStatus::Pending => {
            Ok((OperationStatus::Pending, OperationIndexingState::Pending))
        }
        OperationStepStatus::Failed => {
            Ok((OperationStatus::Failed, OperationIndexingState::Failed))
        }
        OperationStepStatus::Submitted | OperationStepStatus::Completed => {
            let side_effects = record.plan.side_effects.iter().filter_map(|step| {
                record
                    .progress
                    .steps
                    .get(&step.id)
                    .map(|progress| progress.status)
            });
            let side_statuses = side_effects.collect::<Vec<_>>();
            if side_statuses.contains(&OperationStepStatus::Failed) {
                Ok((
                    OperationStatus::PartiallyFailed,
                    OperationIndexingState::Failed,
                ))
            } else if side_statuses.contains(&OperationStepStatus::Pending) {
                if side_statuses
                    .iter()
                    .any(|status| *status != OperationStepStatus::Pending)
                {
                    Ok((
                        OperationStatus::EffectsSubmitted,
                        OperationIndexingState::Pending,
                    ))
                } else {
                    Ok((
                        OperationStatus::PrimaryCommitted,
                        OperationIndexingState::Pending,
                    ))
                }
            } else {
                let indexing_state = if primary.status == OperationStepStatus::Completed
                    && side_statuses
                        .iter()
                        .all(|status| *status == OperationStepStatus::Completed)
                {
                    OperationIndexingState::Completed
                } else {
                    OperationIndexingState::Pending
                };
                Ok((OperationStatus::Completed, indexing_state))
            }
        }
    }
}

fn operation_steps(plan: &OperationPlan) -> impl Iterator<Item = &OperationStep> {
    std::iter::once(&plan.primary).chain(plan.side_effects.iter())
}

fn require_non_empty(label: &str, value: &str) -> Result<(), MutationPlanError> {
    if value.trim().is_empty() {
        invalid(format!("{label} must not be empty"))
    } else {
        Ok(())
    }
}

fn require_non_empty_slice<T>(label: &str, values: &[T]) -> Result<(), MutationPlanError> {
    if values.is_empty() {
        invalid(format!("{label} must not be empty"))
    } else {
        Ok(())
    }
}

fn validate_unique_non_empty(label: &str, values: &[String]) -> Result<(), MutationPlanError> {
    let mut unique = HashSet::new();
    for value in values {
        require_non_empty(label, value)?;
        if !unique.insert(value) {
            return invalid(format!("duplicate {label} `{value}`"));
        }
    }
    Ok(())
}

fn validate_task_uids(step_id: &str, task_uids: &[String]) -> Result<(), MutationPlanError> {
    let mut unique = HashSet::new();
    for task_uid in task_uids {
        require_non_empty("task UID", task_uid)?;
        if !unique.insert(task_uid) {
            return invalid(format!("step `{step_id}` contains a duplicate task UID"));
        }
    }
    Ok(())
}

fn tenant_matches(expected: &str, actual: &str, resource: &str) -> Result<(), MutationPlanError> {
    if actual == expected {
        Ok(())
    } else {
        invalid(format!(
            "{resource} tenant does not match the operation tenant"
        ))
    }
}

fn dedup_non_empty(values: Vec<String>) -> Vec<String> {
    let mut seen = HashSet::new();
    values
        .into_iter()
        .filter(|value| !value.is_empty() && seen.insert(value.clone()))
        .collect()
}

fn invalid<T>(message: impl Into<String>) -> Result<T, MutationPlanError> {
    Err(MutationPlanError::InvalidPlan(message.into()))
}

fn invalid_transition<T>(step_id: &str, reason: impl Into<String>) -> Result<T, MutationPlanError> {
    Err(MutationPlanError::InvalidTransition {
        step_id: step_id.to_string(),
        reason: reason.into(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{OperationActor, OperationResource};
    use serde_json::json;

    fn plan() -> OperationPlan {
        let at = DateTime::parse_from_rfc3339("2026-07-14T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        OperationPlan {
            schema_version: OPERATION_PLAN_SCHEMA_VERSION,
            id: "op-1".to_string(),
            tenant_id: "tenant-a".to_string(),
            operation_kind: "state_upsert".to_string(),
            actor: OperationActor {
                scope: OperationActorScope::TenantService,
                owner_user_id_hash: None,
                roles: vec!["writer".to_string()],
                request_id: Some("request-1".to_string()),
            },
            idempotency_key_hash: Some("idem-hash".to_string()),
            primary: OperationStep {
                id: "primary".to_string(),
                role: OperationStepRole::Primary,
                resource: OperationResource::StructuredSummary {
                    summary: json!({"id": "summary-1"}),
                },
            },
            side_effects: vec![OperationStep {
                id: "effect".to_string(),
                role: OperationStepRole::SideEffect,
                resource: OperationResource::StructuredSummary {
                    summary: json!({"id": "summary-2"}),
                },
            }],
            redacted_metadata: json!({"resource_count": 2}),
            response_snapshot: json!({"ok": true}),
            created_at: at,
        }
    }

    #[test]
    fn plan_is_immutable_while_progress_transitions() {
        let record = operation_record_from_plan(plan()).unwrap();
        let serialized_plan = serde_json::to_value(&record.plan).unwrap();
        let primary_at = record.created_at + chrono::Duration::seconds(1);
        let record =
            operation_step_submitted(&record, "primary", vec!["11".to_string()], primary_at)
                .unwrap();
        let record = operation_step_completed(
            &record,
            "primary",
            primary_at + chrono::Duration::seconds(1),
        )
        .unwrap();
        assert_eq!(record.status, OperationStatus::PrimaryCommitted);
        assert_eq!(serde_json::to_value(&record.plan).unwrap(), serialized_plan);

        let effect_at = primary_at + chrono::Duration::seconds(2);
        let record = operation_step_completed(&record, "effect", effect_at).unwrap();
        assert_eq!(record.status, OperationStatus::Completed);
        assert_eq!(record.indexing_state, OperationIndexingState::Completed);
        assert_eq!(record.completed_at, Some(effect_at));
        assert_eq!(serde_json::to_value(&record.plan).unwrap(), serialized_plan);
    }

    #[test]
    fn failed_side_effect_is_retryable() {
        let record = operation_record_from_plan(plan()).unwrap();
        let at = record.created_at + chrono::Duration::seconds(1);
        let record = operation_step_completed(&record, "primary", at).unwrap();
        let record = operation_step_failed(
            &record,
            "effect",
            "upstream",
            "hmac:fingerprint",
            at + chrono::Duration::seconds(1),
        )
        .unwrap();
        assert_eq!(record.status, OperationStatus::PartiallyFailed);

        let record = operation_step_submitted(
            &record,
            "effect",
            vec!["22".to_string()],
            at + chrono::Duration::seconds(2),
        )
        .unwrap();
        assert_eq!(record.status, OperationStatus::Completed);
        assert_eq!(record.indexing_state, OperationIndexingState::Pending);
        assert_eq!(record.last_error_category, None);
    }

    #[test]
    fn accepted_steps_commit_before_async_indexing_finishes() {
        let record = operation_record_from_plan(plan()).unwrap();
        let primary_at = record.created_at + chrono::Duration::seconds(1);
        let record =
            operation_step_accepted(&record, "primary", vec!["11".to_string()], primary_at)
                .unwrap();
        assert_eq!(record.status, OperationStatus::PrimaryCommitted);
        assert_eq!(record.indexing_state, OperationIndexingState::Pending);

        let accepted_at = primary_at + chrono::Duration::seconds(1);
        let record =
            operation_step_accepted(&record, "effect", vec!["12".to_string()], accepted_at)
                .unwrap();
        assert_eq!(record.status, OperationStatus::Completed);
        assert_eq!(record.indexing_state, OperationIndexingState::Pending);
        assert_eq!(record.completed_at, Some(accepted_at));

        let record = operation_step_completed(
            &record,
            "primary",
            accepted_at + chrono::Duration::seconds(1),
        )
        .unwrap();
        let record = operation_step_completed(
            &record,
            "effect",
            accepted_at + chrono::Duration::seconds(2),
        )
        .unwrap();
        assert_eq!(record.indexing_state, OperationIndexingState::Completed);
        assert_eq!(record.completed_at, Some(accepted_at));
    }

    #[test]
    fn persistence_metadata_preserves_primary_task_order() {
        let record = operation_record_from_plan(plan()).unwrap();
        let at = record.created_at + chrono::Duration::seconds(1);
        let record = operation_step_accepted(
            &record,
            "primary",
            vec!["20".to_string(), "10".to_string()],
            at,
        )
        .unwrap();
        let record = operation_step_accepted(
            &record,
            "effect",
            vec!["30".to_string(), "10".to_string()],
            at + chrono::Duration::seconds(1),
        )
        .unwrap();

        let metadata = persistence_metadata(&record);
        assert_eq!(metadata.primary_task_uids, ["20", "10"]);
        assert_eq!(metadata.task_uids, ["20", "10", "30"]);
    }

    #[test]
    fn side_effect_cannot_run_before_primary_commit() {
        let record = operation_record_from_plan(plan()).unwrap();
        let error = operation_step_completed(
            &record,
            "effect",
            record.created_at + chrono::Duration::seconds(1),
        )
        .unwrap_err();
        assert!(matches!(error, MutationPlanError::InvalidTransition { .. }));
    }

    #[test]
    fn validation_rejects_cross_tenant_payloads() {
        let mut plan = plan();
        plan.primary.resource = OperationResource::StructuredSnapshot {
            snapshot: crate::models::StructuredSnapshot {
                id: "snapshot-1".to_string(),
                tenant_id: "tenant-b".to_string(),
                dataset_key: "daily".to_string(),
                owner_user_id: "owner".to_string(),
                period_key: "2026-07-14".to_string(),
                period_start: plan.created_at,
                period_end: plan.created_at,
                row_count: 1,
                status: "active".to_string(),
            },
        };
        assert!(validate_operation_plan(&plan).is_err());
    }

    #[test]
    fn summary_omits_the_replay_plan_by_default() {
        let record = operation_record_from_plan(plan()).unwrap();
        let item = operation_list_item(&record, false);
        assert!(item.plan.is_none());
        assert_eq!(item.summary.tenant_id, "tenant-a");
    }
}
