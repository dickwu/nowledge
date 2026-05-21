use std::sync::Arc;

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Map, Value};

use crate::{
    config::Config,
    error::ApiError,
    meili::{MeiliAdmin, SearchResponse},
    models::*,
    resolver::EventIndexResolver,
    util::{hmac_hex, text_score, truncate_chars},
};

#[derive(Debug, Clone)]
pub struct RepositoryContextSearch {
    pub nodes: Vec<ContextNode>,
    pub stages: Vec<Value>,
}

pub struct RepositoryContextSearchQuery<'a> {
    pub tenant_id: &'a str,
    pub owner_user_id: Option<&'a str>,
    pub query: &'a str,
    pub mode: &'a str,
    pub limit: usize,
    pub filters: &'a ContextStructuredFilters,
    pub resolver: &'a EventIndexResolver,
}

#[async_trait]
pub trait KnowledgeRepository: Send + Sync {
    fn backend_name(&self) -> &'static str;

    async fn ensure_user_event_index(
        &self,
        index: &UserEventIndex,
    ) -> Result<Vec<String>, ApiError>;

    async fn append_event(&self, event: &HistoryEvent) -> Result<Option<String>, ApiError>;

    async fn upsert_context_nodes(
        &self,
        index_uid: &str,
        nodes: &[ContextNode],
    ) -> Result<Option<String>, ApiError>;

    /// Load every company-scoped context node persisted for `tenant_id`.
    /// Returns `Ok(None)` for backends that do not keep their own copy
    /// (e.g. the in-memory backend); callers should then fall back to the
    /// in-process state. The Meili backend rehydrates from
    /// `rag_company_context`.
    async fn list_company_context_nodes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<ContextNode>>, ApiError>;

    async fn upsert_state_item(&self, item: &StateItem) -> Result<Option<String>, ApiError>;

    async fn upsert_company_source(
        &self,
        source: &CompanySource,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_source_revision(
        &self,
        revision: &SourceRevision,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_source_documents(
        &self,
        documents: &[SourceDocument],
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_parse_artifacts(
        &self,
        artifacts: &[ParseArtifact],
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_structured_snapshot(
        &self,
        snapshot: &StructuredSnapshot,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_structured_rows(&self, rows: &[Value]) -> Result<Option<String>, ApiError>;

    async fn upsert_structured_summary(&self, summary: &Value) -> Result<Option<String>, ApiError>;

    async fn upsert_trace(&self, trace: &TraceRecord) -> Result<Option<String>, ApiError>;

    async fn upsert_links(&self, links: &[KnowledgeLink]) -> Result<Option<String>, ApiError>;

    async fn upsert_harness_components(
        &self,
        components: &[HarnessComponent],
        revisions: &[HarnessComponentRevision],
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_harness_changes(
        &self,
        changes: &[HarnessChangeManifest],
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_harness_verdicts(
        &self,
        verdicts: &[HarnessChangeVerdict],
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_ingest_task(&self, task: &IngestTask) -> Result<Option<String>, ApiError>;

    async fn upsert_ingest_result(
        &self,
        result: &IngestTaskResult,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_eval_case(&self, case: &RagEvalCase) -> Result<Option<String>, ApiError>;

    async fn upsert_eval_run(&self, run: &RagEvalRun) -> Result<Option<String>, ApiError>;

    async fn upsert_eval_case_results(
        &self,
        results: &[RagEvalCaseResult],
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_eval_overview(
        &self,
        overview: &RagEvalOverview,
    ) -> Result<Option<String>, ApiError>;

    async fn list_harness_components(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<HarnessComponent>>, ApiError>;

    async fn list_harness_component_revisions(
        &self,
        tenant_id: &str,
        component_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessComponentRevision>>, ApiError>;

    async fn get_harness_change(
        &self,
        tenant_id: &str,
        change_id: &str,
    ) -> Result<Option<HarnessChangeManifest>, ApiError>;

    async fn list_harness_changes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<HarnessChangeManifest>>, ApiError>;

    async fn list_harness_verdicts(
        &self,
        tenant_id: &str,
        change_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessChangeVerdict>>, ApiError>;

    async fn get_ingest_task(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<Option<IngestTask>, ApiError>;

    async fn get_ingest_result(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<Option<IngestTaskResult>, ApiError>;

    async fn list_ingest_tasks(&self, tenant_id: &str)
        -> Result<Option<Vec<IngestTask>>, ApiError>;

    async fn list_ingest_results(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<IngestTaskResult>>, ApiError>;

    async fn list_eval_cases(&self, tenant_id: &str) -> Result<Option<Vec<RagEvalCase>>, ApiError>;

    async fn get_eval_run(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<RagEvalRun>, ApiError>;

    async fn list_eval_runs(&self, tenant_id: &str) -> Result<Option<Vec<RagEvalRun>>, ApiError>;

    async fn get_eval_overview(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<RagEvalOverview>, ApiError>;

    async fn list_eval_case_results(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<Vec<RagEvalCaseResult>>, ApiError>;

    async fn list_parse_artifacts(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        source_id: Option<&str>,
        revision_id: Option<&str>,
    ) -> Result<Option<Vec<ParseArtifact>>, ApiError>;

    async fn search_user_events(
        &self,
        routing: &EventIndexRouting,
        req: &HistorySearchRequest,
    ) -> Result<Option<Vec<HistoryEvent>>, ApiError>;

    async fn search_context(
        &self,
        request: RepositoryContextSearchQuery<'_>,
    ) -> Result<Option<RepositoryContextSearch>, ApiError>;

    async fn get_event(
        &self,
        routing: &EventIndexRouting,
        event_id: &str,
    ) -> Result<Option<HistoryEvent>, ApiError>;

    async fn read_context_node(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        uri: &str,
        layer: Option<u8>,
        resolver: &EventIndexResolver,
    ) -> Result<Option<ContextNode>, ApiError>;

    async fn read_source_document(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        uri: &str,
    ) -> Result<Option<SourceDocument>, ApiError>;

    async fn get_trace(&self, trace_id: &str) -> Result<Option<TraceRecord>, ApiError>;

    async fn get_snapshot(&self, snapshot_id: &str)
        -> Result<Option<StructuredSnapshot>, ApiError>;

    async fn list_rows(&self, snapshot_id: &str) -> Result<Option<Vec<Value>>, ApiError>;

    async fn debug_search(&self, index_uid: &str, query: &str) -> Result<Option<Value>, ApiError>;
}

#[derive(Debug)]
pub struct MemoryRepository;

#[async_trait]
impl KnowledgeRepository for MemoryRepository {
    fn backend_name(&self) -> &'static str {
        "memory"
    }

    async fn ensure_user_event_index(
        &self,
        _index: &UserEventIndex,
    ) -> Result<Vec<String>, ApiError> {
        Ok(Vec::new())
    }

    async fn append_event(&self, _event: &HistoryEvent) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_context_nodes(
        &self,
        _index_uid: &str,
        _nodes: &[ContextNode],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_company_context_nodes(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<ContextNode>>, ApiError> {
        Ok(None)
    }

    async fn upsert_state_item(&self, _item: &StateItem) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_company_source(
        &self,
        _source: &CompanySource,
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_source_revision(
        &self,
        _revision: &SourceRevision,
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_source_documents(
        &self,
        _documents: &[SourceDocument],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_parse_artifacts(
        &self,
        _artifacts: &[ParseArtifact],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_structured_snapshot(
        &self,
        _snapshot: &StructuredSnapshot,
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_structured_rows(&self, _rows: &[Value]) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_structured_summary(
        &self,
        _summary: &Value,
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_trace(&self, _trace: &TraceRecord) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_links(&self, _links: &[KnowledgeLink]) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_harness_components(
        &self,
        _components: &[HarnessComponent],
        _revisions: &[HarnessComponentRevision],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_harness_changes(
        &self,
        _changes: &[HarnessChangeManifest],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_harness_verdicts(
        &self,
        _verdicts: &[HarnessChangeVerdict],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_ingest_task(&self, _task: &IngestTask) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_ingest_result(
        &self,
        _result: &IngestTaskResult,
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_eval_case(&self, _case: &RagEvalCase) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_eval_run(&self, _run: &RagEvalRun) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_eval_case_results(
        &self,
        _results: &[RagEvalCaseResult],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_eval_overview(
        &self,
        _overview: &RagEvalOverview,
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_harness_components(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<HarnessComponent>>, ApiError> {
        Ok(None)
    }

    async fn list_harness_component_revisions(
        &self,
        _tenant_id: &str,
        _component_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessComponentRevision>>, ApiError> {
        Ok(None)
    }

    async fn get_harness_change(
        &self,
        _tenant_id: &str,
        _change_id: &str,
    ) -> Result<Option<HarnessChangeManifest>, ApiError> {
        Ok(None)
    }

    async fn list_harness_changes(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<HarnessChangeManifest>>, ApiError> {
        Ok(None)
    }

    async fn list_harness_verdicts(
        &self,
        _tenant_id: &str,
        _change_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessChangeVerdict>>, ApiError> {
        Ok(None)
    }

    async fn get_ingest_task(
        &self,
        _tenant_id: &str,
        _task_id: &str,
    ) -> Result<Option<IngestTask>, ApiError> {
        Ok(None)
    }

    async fn get_ingest_result(
        &self,
        _tenant_id: &str,
        _task_id: &str,
    ) -> Result<Option<IngestTaskResult>, ApiError> {
        Ok(None)
    }

    async fn list_ingest_tasks(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<IngestTask>>, ApiError> {
        Ok(None)
    }

    async fn list_ingest_results(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<IngestTaskResult>>, ApiError> {
        Ok(None)
    }

    async fn list_eval_cases(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<RagEvalCase>>, ApiError> {
        Ok(None)
    }

    async fn get_eval_run(
        &self,
        _tenant_id: &str,
        _run_id: &str,
    ) -> Result<Option<RagEvalRun>, ApiError> {
        Ok(None)
    }

    async fn list_eval_runs(&self, _tenant_id: &str) -> Result<Option<Vec<RagEvalRun>>, ApiError> {
        Ok(None)
    }

    async fn get_eval_overview(
        &self,
        _tenant_id: &str,
        _run_id: &str,
    ) -> Result<Option<RagEvalOverview>, ApiError> {
        Ok(None)
    }

    async fn list_eval_case_results(
        &self,
        _tenant_id: &str,
        _run_id: &str,
    ) -> Result<Option<Vec<RagEvalCaseResult>>, ApiError> {
        Ok(None)
    }

    async fn list_parse_artifacts(
        &self,
        _tenant_id: &str,
        _owner_user_id: Option<&str>,
        _source_id: Option<&str>,
        _revision_id: Option<&str>,
    ) -> Result<Option<Vec<ParseArtifact>>, ApiError> {
        Ok(None)
    }

    async fn search_user_events(
        &self,
        _routing: &EventIndexRouting,
        _req: &HistorySearchRequest,
    ) -> Result<Option<Vec<HistoryEvent>>, ApiError> {
        Ok(None)
    }

    async fn search_context(
        &self,
        _request: RepositoryContextSearchQuery<'_>,
    ) -> Result<Option<RepositoryContextSearch>, ApiError> {
        Ok(None)
    }

    async fn get_event(
        &self,
        _routing: &EventIndexRouting,
        _event_id: &str,
    ) -> Result<Option<HistoryEvent>, ApiError> {
        Ok(None)
    }

    async fn read_context_node(
        &self,
        _tenant_id: &str,
        _owner_user_id: Option<&str>,
        _uri: &str,
        _layer: Option<u8>,
        _resolver: &EventIndexResolver,
    ) -> Result<Option<ContextNode>, ApiError> {
        Ok(None)
    }

    async fn read_source_document(
        &self,
        _tenant_id: &str,
        _owner_user_id: Option<&str>,
        _uri: &str,
    ) -> Result<Option<SourceDocument>, ApiError> {
        Ok(None)
    }

    async fn get_trace(&self, _trace_id: &str) -> Result<Option<TraceRecord>, ApiError> {
        Ok(None)
    }

    async fn get_snapshot(
        &self,
        _snapshot_id: &str,
    ) -> Result<Option<StructuredSnapshot>, ApiError> {
        Ok(None)
    }

    async fn list_rows(&self, _snapshot_id: &str) -> Result<Option<Vec<Value>>, ApiError> {
        Ok(None)
    }

    async fn debug_search(
        &self,
        _index_uid: &str,
        _query: &str,
    ) -> Result<Option<Value>, ApiError> {
        Ok(None)
    }
}

#[derive(Debug, Clone)]
pub struct MeiliRepository {
    admin: MeiliAdmin,
    wait_for_tasks: bool,
}

impl MeiliRepository {
    pub fn new(admin: MeiliAdmin, wait_for_tasks: bool) -> Self {
        Self {
            admin,
            wait_for_tasks,
        }
    }

    async fn maybe_wait(&self, task_uid: &Option<String>) -> Result<(), ApiError> {
        if self.wait_for_tasks {
            if let Some(task_uid) = task_uid {
                self.admin.wait_for_task(task_uid).await?;
            }
        }
        Ok(())
    }

    async fn upsert_values(
        &self,
        index_uid: &str,
        documents: &[Value],
    ) -> Result<Option<String>, ApiError> {
        if documents.is_empty() {
            return Ok(None);
        }
        let task_uid = self.admin.add_documents(index_uid, documents).await?;
        self.maybe_wait(&task_uid).await?;
        Ok(task_uid)
    }
}

#[async_trait]
impl KnowledgeRepository for MeiliRepository {
    fn backend_name(&self) -> &'static str {
        "meili"
    }

    async fn ensure_user_event_index(
        &self,
        index: &UserEventIndex,
    ) -> Result<Vec<String>, ApiError> {
        let mut task_uids = self
            .admin
            .ensure_index(&index.event_index_uid, "id", true)
            .await?;
        task_uids.extend(
            self.admin
                .ensure_index(&index.personal_context_index_uid, "id", true)
                .await?,
        );
        let registry_task = self
            .upsert_values("rag_user_event_indexes", &[to_document(index, &index.id)?])
            .await?;
        if let Some(task_uid) = registry_task {
            task_uids.push(task_uid);
        }
        if self.wait_for_tasks {
            self.admin.wait_for_tasks(&task_uids).await?;
        }
        Ok(task_uids)
    }

    async fn append_event(&self, event: &HistoryEvent) -> Result<Option<String>, ApiError> {
        let task_uid = self
            .upsert_values(&event.event_index_uid, &[to_document(event, &event.id)?])
            .await?;
        Ok(task_uid)
    }

    async fn upsert_context_nodes(
        &self,
        index_uid: &str,
        nodes: &[ContextNode],
    ) -> Result<Option<String>, ApiError> {
        let documents = nodes
            .iter()
            .map(|node| to_document(node, &context_document_id(&node.uri)))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values(index_uid, &documents).await
    }

    async fn list_company_context_nodes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<ContextNode>>, ApiError> {
        self.search_many(
            "rag_company_context",
            &format!("tenant_id = {}", meili_string(tenant_id)?),
            1000,
            Some(&["updated_at:desc"]),
        )
        .await
    }

    async fn upsert_state_item(&self, item: &StateItem) -> Result<Option<String>, ApiError> {
        self.upsert_values("rag_state_items", &[to_document(item, &item.id)?])
            .await
    }

    async fn upsert_company_source(
        &self,
        source: &CompanySource,
    ) -> Result<Option<String>, ApiError> {
        self.upsert_values("rag_sources", &[to_document(source, &source.id)?])
            .await
    }

    async fn upsert_source_revision(
        &self,
        revision: &SourceRevision,
    ) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_source_revisions",
            &[to_document(revision, &revision.id)?],
        )
        .await
    }

    async fn upsert_source_documents(
        &self,
        documents: &[SourceDocument],
    ) -> Result<Option<String>, ApiError> {
        let documents = documents
            .iter()
            .map(|document| to_document(document, &document.id))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_source_documents", &documents).await
    }

    async fn upsert_parse_artifacts(
        &self,
        artifacts: &[ParseArtifact],
    ) -> Result<Option<String>, ApiError> {
        let documents = artifacts
            .iter()
            .map(|artifact| to_document(artifact, &artifact.id))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_parse_artifacts", &documents).await
    }

    async fn upsert_structured_snapshot(
        &self,
        snapshot: &StructuredSnapshot,
    ) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_structured_snapshots",
            &[to_document(snapshot, &snapshot.id)?],
        )
        .await
    }

    async fn upsert_structured_rows(&self, rows: &[Value]) -> Result<Option<String>, ApiError> {
        let documents = rows
            .iter()
            .map(|row| {
                let id = row
                    .get("id")
                    .and_then(Value::as_str)
                    .map(ToString::to_string)
                    .unwrap_or_else(|| context_document_id(&row.to_string()));
                to_document(row, &id)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_structured_rows", &documents).await
    }

    async fn upsert_structured_summary(&self, summary: &Value) -> Result<Option<String>, ApiError> {
        let id = summary
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::Internal("structured summary is missing id".to_string()))?;
        self.upsert_values("rag_structured_summaries", &[to_document(summary, id)?])
            .await
    }

    async fn upsert_trace(&self, trace: &TraceRecord) -> Result<Option<String>, ApiError> {
        self.upsert_values("rag_traces", &[to_document(trace, &trace.id)?])
            .await
    }

    async fn upsert_links(&self, links: &[KnowledgeLink]) -> Result<Option<String>, ApiError> {
        let documents = links
            .iter()
            .map(|link| to_document(link, &link.id))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_links", &documents).await
    }

    async fn upsert_harness_components(
        &self,
        components: &[HarnessComponent],
        revisions: &[HarnessComponentRevision],
    ) -> Result<Option<String>, ApiError> {
        let mut documents = components
            .iter()
            .map(|component| to_document_with_kind(component, &component.id, "component"))
            .collect::<Result<Vec<_>, _>>()?;
        documents.extend(
            revisions
                .iter()
                .map(|revision| to_document_with_kind(revision, &revision.id, "revision"))
                .collect::<Result<Vec<_>, _>>()?,
        );
        self.upsert_values("rag_harness_components", &documents)
            .await
    }

    async fn upsert_harness_changes(
        &self,
        changes: &[HarnessChangeManifest],
    ) -> Result<Option<String>, ApiError> {
        let documents = changes
            .iter()
            .map(|change| to_document(change, &change.id))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_harness_changes", &documents).await
    }

    async fn upsert_harness_verdicts(
        &self,
        verdicts: &[HarnessChangeVerdict],
    ) -> Result<Option<String>, ApiError> {
        let documents = verdicts
            .iter()
            .map(|verdict| to_document(verdict, &verdict.id))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_harness_verdicts", &documents).await
    }

    async fn upsert_ingest_task(&self, task: &IngestTask) -> Result<Option<String>, ApiError> {
        self.upsert_values("rag_ingest_tasks", &[to_document(task, &task.task_id)?])
            .await
    }

    async fn upsert_ingest_result(
        &self,
        result: &IngestTaskResult,
    ) -> Result<Option<String>, ApiError> {
        let mut document = to_document(result, &result.task.task_id)?;
        if let Value::Object(map) = &mut document {
            map.insert(
                "tenant_id".to_string(),
                Value::String(result.task.tenant_id.clone()),
            );
            map.insert(
                "task_id".to_string(),
                Value::String(result.task.task_id.clone()),
            );
            if let Some(owner) = &result.task.owner_user_id {
                map.insert("owner_user_id".to_string(), Value::String(owner.clone()));
            }
        }
        self.upsert_values("rag_ingest_results", &[document]).await
    }

    async fn upsert_eval_case(&self, case: &RagEvalCase) -> Result<Option<String>, ApiError> {
        self.upsert_values("rag_eval_cases", &[to_document(case, &case.id)?])
            .await
    }

    async fn upsert_eval_run(&self, run: &RagEvalRun) -> Result<Option<String>, ApiError> {
        self.upsert_values("rag_eval_runs", &[to_document(run, &run.id)?])
            .await
    }

    async fn upsert_eval_case_results(
        &self,
        results: &[RagEvalCaseResult],
    ) -> Result<Option<String>, ApiError> {
        let documents = results
            .iter()
            .map(|result| to_document(result, &result.id))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_eval_case_results", &documents)
            .await
    }

    async fn upsert_eval_overview(
        &self,
        overview: &RagEvalOverview,
    ) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_eval_overviews",
            &[to_document(overview, &overview.run_id)?],
        )
        .await
    }

    async fn list_harness_components(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<HarnessComponent>>, ApiError> {
        self.search_many(
            "rag_harness_components",
            &format!(
                "tenant_id = {} AND doc_kind = \"component\"",
                meili_string(tenant_id)?
            ),
            1000,
            Some(&["id:asc"]),
        )
        .await
    }

    async fn list_harness_component_revisions(
        &self,
        tenant_id: &str,
        component_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessComponentRevision>>, ApiError> {
        let mut filters = vec![
            format!("tenant_id = {}", meili_string(tenant_id)?),
            "doc_kind = \"revision\"".to_string(),
        ];
        if let Some(component_id) = component_id {
            filters.push(format!("component_id = {}", meili_string(component_id)?));
        }
        self.search_many(
            "rag_harness_components",
            &filters.join(" AND "),
            1000,
            Some(&["iteration:asc"]),
        )
        .await
    }

    async fn get_harness_change(
        &self,
        tenant_id: &str,
        change_id: &str,
    ) -> Result<Option<HarnessChangeManifest>, ApiError> {
        self.search_one(
            "rag_harness_changes",
            &format!(
                "tenant_id = {} AND id = {}",
                meili_string(tenant_id)?,
                meili_string(change_id)?
            ),
        )
        .await
    }

    async fn list_harness_changes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<HarnessChangeManifest>>, ApiError> {
        self.search_many(
            "rag_harness_changes",
            &format!("tenant_id = {}", meili_string(tenant_id)?),
            1000,
            Some(&["created_at:desc"]),
        )
        .await
    }

    async fn list_harness_verdicts(
        &self,
        tenant_id: &str,
        change_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessChangeVerdict>>, ApiError> {
        let mut filters = vec![format!("tenant_id = {}", meili_string(tenant_id)?)];
        if let Some(change_id) = change_id {
            filters.push(format!("change_id = {}", meili_string(change_id)?));
        }
        self.search_many(
            "rag_harness_verdicts",
            &filters.join(" AND "),
            1000,
            Some(&["created_at:desc"]),
        )
        .await
    }

    async fn get_ingest_task(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<Option<IngestTask>, ApiError> {
        self.search_one(
            "rag_ingest_tasks",
            &format!(
                "tenant_id = {} AND task_id = {}",
                meili_string(tenant_id)?,
                meili_string(task_id)?
            ),
        )
        .await
    }

    async fn get_ingest_result(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<Option<IngestTaskResult>, ApiError> {
        self.search_one(
            "rag_ingest_results",
            &format!(
                "tenant_id = {} AND task_id = {}",
                meili_string(tenant_id)?,
                meili_string(task_id)?
            ),
        )
        .await
    }

    async fn list_ingest_tasks(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<IngestTask>>, ApiError> {
        self.search_many(
            "rag_ingest_tasks",
            &format!("tenant_id = {}", meili_string(tenant_id)?),
            1000,
            Some(&["created_at:desc"]),
        )
        .await
    }

    async fn list_ingest_results(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<IngestTaskResult>>, ApiError> {
        self.search_many(
            "rag_ingest_results",
            &format!("tenant_id = {}", meili_string(tenant_id)?),
            1000,
            None,
        )
        .await
    }

    async fn list_eval_cases(&self, tenant_id: &str) -> Result<Option<Vec<RagEvalCase>>, ApiError> {
        self.search_many(
            "rag_eval_cases",
            &format!("tenant_id = {}", meili_string(tenant_id)?),
            1000,
            Some(&["created_at:asc"]),
        )
        .await
    }

    async fn get_eval_run(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<RagEvalRun>, ApiError> {
        self.search_one(
            "rag_eval_runs",
            &format!(
                "tenant_id = {} AND id = {}",
                meili_string(tenant_id)?,
                meili_string(run_id)?
            ),
        )
        .await
    }

    async fn list_eval_runs(&self, tenant_id: &str) -> Result<Option<Vec<RagEvalRun>>, ApiError> {
        self.search_many(
            "rag_eval_runs",
            &format!("tenant_id = {}", meili_string(tenant_id)?),
            1000,
            Some(&["created_at:desc"]),
        )
        .await
    }

    async fn get_eval_overview(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<RagEvalOverview>, ApiError> {
        let _ = tenant_id;
        self.search_one(
            "rag_eval_overviews",
            &format!("run_id = {}", meili_string(run_id)?),
        )
        .await
    }

    async fn list_eval_case_results(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<Vec<RagEvalCaseResult>>, ApiError> {
        let _ = tenant_id;
        self.search_many(
            "rag_eval_case_results",
            &format!("run_id = {}", meili_string(run_id)?),
            1000,
            None,
        )
        .await
    }

    async fn list_parse_artifacts(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        source_id: Option<&str>,
        revision_id: Option<&str>,
    ) -> Result<Option<Vec<ParseArtifact>>, ApiError> {
        let mut filters = vec![format!("tenant_id = {}", meili_string(tenant_id)?)];
        if let Some(owner) = owner_user_id {
            filters.push(format!("owner_user_id = {}", meili_string(owner)?));
        } else {
            filters.push("owner_user_id IS NULL".to_string());
        }
        if let Some(source_id) = source_id {
            filters.push(format!("source_id = {}", meili_string(source_id)?));
        }
        if let Some(revision_id) = revision_id {
            filters.push(format!("revision_id = {}", meili_string(revision_id)?));
        }
        self.search_many(
            "rag_parse_artifacts",
            &filters.join(" AND "),
            1000,
            Some(&["created_at:asc"]),
        )
        .await
    }

    async fn search_user_events(
        &self,
        routing: &EventIndexRouting,
        req: &HistorySearchRequest,
    ) -> Result<Option<Vec<HistoryEvent>>, ApiError> {
        let mut filters = vec![format!(
            "owner_user_id_hash = {}",
            meili_string(&routing.owner_user_id_hash)?
        )];
        if !req.event_types.is_empty() {
            filters.push(format!(
                "event_type IN {}",
                meili_string_array(&req.event_types)?
            ));
        }
        if let Some(entity_type) = &req.entity_type {
            filters.push(format!("entity_type = {}", meili_string(entity_type)?));
        }
        if let Some(entity_id) = &req.entity_id {
            filters.push(format!("entity_id = {}", meili_string(entity_id)?));
        }
        if let Some(from) = req.from {
            filters.push(format!(
                "occurred_at >= {}",
                meili_string(&from.to_rfc3339())?
            ));
        }
        if let Some(to) = req.to {
            filters.push(format!(
                "occurred_at <= {}",
                meili_string(&to.to_rfc3339())?
            ));
        }

        let response: SearchResponse<HistoryEvent> = self
            .admin
            .search(
                &routing.event_index_uid,
                json!({
                    "q": req.query.clone().unwrap_or_default(),
                    "limit": req.limit.max(1),
                    "filter": filters.join(" AND "),
                    "sort": ["occurred_at:desc"]
                }),
            )
            .await?;
        Ok(Some(response.hits))
    }

    async fn search_context(
        &self,
        request: RepositoryContextSearchQuery<'_>,
    ) -> Result<Option<RepositoryContextSearch>, ApiError> {
        let mut stages = Vec::new();
        let mut all_nodes = Vec::new();
        let search_limit = if request.filters.requires_post_filter() {
            request
                .limit
                .saturating_mul(20)
                .max(request.limit)
                .min(1000)
        } else {
            request.limit
        };

        let company_filter = context_filter(request.tenant_id, None, request.filters)?;
        let company = self
            .search_context_index(
                "rag_company_context",
                request.query,
                &company_filter,
                search_limit,
            )
            .await?;
        let company_hits = company
            .hits
            .into_iter()
            .filter(|node| request.filters.matches_node(node))
            .collect::<Vec<_>>();
        stages.push(context_stage(
            "fragments_company",
            "rag_company_context",
            request.query,
            &company_filter,
            company.processing_time_ms,
            &company_hits,
        ));
        all_nodes.extend(company_hits);

        if let Some(owner) = request.owner_user_id {
            let routing = request
                .resolver
                .resolve(request.tenant_id, owner, false, true)?;
            let personal_filter = context_filter(request.tenant_id, Some(owner), request.filters)?;
            let personal = self
                .search_context_index(
                    &routing.personal_context_index_uid,
                    request.query,
                    &personal_filter,
                    search_limit,
                )
                .await?;
            let personal_hits = personal
                .hits
                .into_iter()
                .filter(|node| request.filters.matches_node(node))
                .collect::<Vec<_>>();
            stages.push(context_stage(
                "fragments_personal",
                &routing.personal_context_index_uid,
                request.query,
                &personal_filter,
                personal.processing_time_ms,
                &personal_hits,
            ));
            all_nodes.extend(personal_hits);
        }

        all_nodes.sort_by(|a, b| {
            text_score(&format!("{} {}", b.title, b.body), request.query)
                .partial_cmp(&text_score(
                    &format!("{} {}", a.title, a.body),
                    request.query,
                ))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all_nodes.truncate(request.limit);
        stages.push(json!({
            "stage": "selection",
            "mode": request.mode,
            "selected_uris": all_nodes.iter().map(|node| &node.uri).collect::<Vec<_>>()
        }));

        Ok(Some(RepositoryContextSearch {
            nodes: all_nodes,
            stages,
        }))
    }

    async fn get_event(
        &self,
        routing: &EventIndexRouting,
        event_id: &str,
    ) -> Result<Option<HistoryEvent>, ApiError> {
        let response: SearchResponse<HistoryEvent> = self
            .admin
            .search(
                &routing.event_index_uid,
                json!({
                    "q": "",
                    "limit": 1,
                    "filter": format!(
                        "id = {} AND owner_user_id_hash = {}",
                        meili_string(event_id)?,
                        meili_string(&routing.owner_user_id_hash)?
                    )
                }),
            )
            .await?;
        Ok(response.hits.into_iter().next())
    }

    async fn read_context_node(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        uri: &str,
        layer: Option<u8>,
        resolver: &EventIndexResolver,
    ) -> Result<Option<ContextNode>, ApiError> {
        let target = strip_context_layer_suffix(uri);
        let mut indexes = vec![("rag_company_context".to_string(), None)];
        if let Some(owner) = owner_user_id {
            let routing = resolver.resolve(tenant_id, owner, false, true)?;
            indexes.push((routing.personal_context_index_uid, Some(owner)));
        }

        for (index_uid, owner) in indexes {
            let mut filters = vec![
                format!("tenant_id = {}", meili_string(tenant_id)?),
                "status = \"active\"".to_string(),
            ];
            if let Some(layer) = layer {
                filters.push(format!("layer = {layer}"));
            }
            if let Some(owner) = owner {
                filters.push(format!("owner_user_id = {}", meili_string(owner)?));
                filters.push("privacy = \"private\"".to_string());
            } else {
                filters.push("privacy = \"company\"".to_string());
            }
            let response: SearchResponse<ContextNode> = self
                .admin
                .search(
                    &index_uid,
                    json!({
                        "q": target,
                        "limit": 20,
                        "filter": filters.join(" AND ")
                    }),
                )
                .await?;
            if let Some(node) = response
                .hits
                .into_iter()
                .find(|node| node.uri == uri || strip_context_layer_suffix(&node.uri) == target)
            {
                return Ok(Some(node));
            }
        }
        Ok(None)
    }

    async fn read_source_document(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        uri: &str,
    ) -> Result<Option<SourceDocument>, ApiError> {
        let mut filters = vec![
            format!("tenant_id = {}", meili_string(tenant_id)?),
            format!("uri = {}", meili_string(uri)?),
            "status = \"active\"".to_string(),
        ];
        if let Some(owner) = owner_user_id {
            filters.push(format!(
                "(owner_user_id = {} OR owner_user_id IS NULL)",
                meili_string(owner)?
            ));
        } else {
            filters.push("owner_user_id IS NULL".to_string());
        }
        let response: SearchResponse<SourceDocument> = self
            .admin
            .search(
                "rag_source_documents",
                json!({
                    "q": "",
                    "limit": 1,
                    "filter": filters.join(" AND ")
                }),
            )
            .await?;
        Ok(response.hits.into_iter().next())
    }

    async fn get_trace(&self, trace_id: &str) -> Result<Option<TraceRecord>, ApiError> {
        let response: SearchResponse<TraceRecord> = self
            .admin
            .search(
                "rag_traces",
                json!({
                    "q": "",
                    "limit": 1,
                    "filter": format!("id = {}", meili_string(trace_id)?)
                }),
            )
            .await?;
        Ok(response.hits.into_iter().next())
    }

    async fn get_snapshot(
        &self,
        snapshot_id: &str,
    ) -> Result<Option<StructuredSnapshot>, ApiError> {
        let response: SearchResponse<StructuredSnapshot> = self
            .admin
            .search(
                "rag_structured_snapshots",
                json!({
                    "q": "",
                    "limit": 1,
                    "filter": format!("id = {}", meili_string(snapshot_id)?)
                }),
            )
            .await?;
        Ok(response.hits.into_iter().next())
    }

    async fn list_rows(&self, snapshot_id: &str) -> Result<Option<Vec<Value>>, ApiError> {
        let response: SearchResponse<Value> = self
            .admin
            .search(
                "rag_structured_rows",
                json!({
                    "q": "",
                    "limit": 1000,
                    "filter": format!("snapshot_id = {}", meili_string(snapshot_id)?)
                }),
            )
            .await?;
        Ok(Some(response.hits))
    }

    async fn debug_search(&self, index_uid: &str, query: &str) -> Result<Option<Value>, ApiError> {
        Ok(Some(
            self.admin
                .search_value(
                    index_uid,
                    json!({
                        "q": query,
                        "limit": 20
                    }),
                )
                .await?,
        ))
    }
}

impl MeiliRepository {
    async fn search_one<T: DeserializeOwned>(
        &self,
        index_uid: &str,
        filter: &str,
    ) -> Result<Option<T>, ApiError> {
        let response: SearchResponse<T> = self
            .admin
            .search(
                index_uid,
                json!({
                    "q": "",
                    "limit": 1,
                    "filter": filter
                }),
            )
            .await?;
        Ok(response.hits.into_iter().next())
    }

    async fn search_many<T: DeserializeOwned>(
        &self,
        index_uid: &str,
        filter: &str,
        limit: usize,
        sort: Option<&[&str]>,
    ) -> Result<Option<Vec<T>>, ApiError> {
        let mut body = json!({
            "q": "",
            "limit": limit.max(1),
            "filter": filter
        });
        if let Some(sort) = sort {
            body["sort"] = json!(sort);
        }
        let response: SearchResponse<T> = self.admin.search(index_uid, body).await?;
        Ok(Some(response.hits))
    }

    async fn search_context_index(
        &self,
        index_uid: &str,
        query: &str,
        filter: &str,
        limit: usize,
    ) -> Result<SearchResponse<ContextNode>, ApiError> {
        self.admin
            .search(
                index_uid,
                json!({
                    "q": query,
                    "limit": limit.max(1),
                    "filter": filter
                }),
            )
            .await
    }
}

pub fn repository_from_config(config: &Config) -> Arc<dyn KnowledgeRepository> {
    if config.store_backend == "meili" && config.meili_url.is_some() {
        Arc::new(MeiliRepository::new(
            MeiliAdmin::from_config(config),
            config.meili_wait_for_tasks,
        ))
    } else {
        Arc::new(MemoryRepository)
    }
}

fn to_document<T: Serialize + ?Sized>(value: &T, id: &str) -> Result<Value, ApiError> {
    let mut document =
        match serde_json::to_value(value).map_err(|e| ApiError::Internal(e.to_string()))? {
            Value::Object(map) => map,
            other => {
                let mut map = Map::new();
                map.insert("value".to_string(), other);
                map
            }
        };
    document.insert("id".to_string(), Value::String(id.to_string()));
    Ok(Value::Object(document))
}

fn to_document_with_kind<T: Serialize + ?Sized>(
    value: &T,
    id: &str,
    doc_kind: &str,
) -> Result<Value, ApiError> {
    let mut document = to_document(value, id)?;
    if let Value::Object(map) = &mut document {
        map.insert("doc_kind".to_string(), Value::String(doc_kind.to_string()));
    }
    Ok(document)
}

fn context_document_id(uri: &str) -> String {
    format!("ctx_{}", hmac_hex(b"nowledge-context-doc", "uri", uri, 24))
}

fn meili_string(value: &str) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(|e| ApiError::Internal(e.to_string()))
}

fn meili_string_array(values: &[String]) -> Result<String, ApiError> {
    serde_json::to_string(values).map_err(|e| ApiError::Internal(e.to_string()))
}

fn context_filter(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    structured: &ContextStructuredFilters,
) -> Result<String, ApiError> {
    let mut filters = vec![
        format!("tenant_id = {}", meili_string(tenant_id)?),
        "status = \"active\"".to_string(),
        "retrieval_enabled = true".to_string(),
        "retrieval_role = \"fragment\"".to_string(),
    ];
    if let Some(owner) = owner_user_id {
        filters.push(format!("owner_user_id = {}", meili_string(owner)?));
        filters.push("privacy = \"private\"".to_string());
    } else {
        filters.push("privacy = \"company\"".to_string());
    }
    filters.extend(context_filter_clauses(structured)?);
    Ok(filters.join(" AND "))
}

fn context_filter_clauses(filters: &ContextStructuredFilters) -> Result<Vec<String>, ApiError> {
    let mut clauses = Vec::new();
    if let Some(source_id) = filters.source_id.as_deref() {
        clauses.push(format!("source_id = {}", meili_string(source_id)?));
    }
    if let Some(revision_id) = filters.revision_id.as_deref() {
        clauses.push(format!("revision_id = {}", meili_string(revision_id)?));
    }
    if let Some(uri) = filters.source_document_uri.as_deref() {
        clauses.push(format!("source_document_uri = {}", meili_string(uri)?));
    }
    if let Some(block_type) = filters.block_type.as_deref() {
        clauses.push(format!("block_type = {}", meili_string(block_type)?));
    }
    if let Some(page_idx) = filters.page_idx {
        clauses.push(format!("page_idx = {page_idx}"));
    }
    if let Some(page_idx_gte) = filters.page_idx_gte {
        clauses.push(format!("page_idx >= {page_idx_gte}"));
    }
    if let Some(page_idx_lte) = filters.page_idx_lte {
        clauses.push(format!("page_idx <= {page_idx_lte}"));
    }
    Ok(clauses)
}

fn context_stage(
    stage: &str,
    index_uid: &str,
    query: &str,
    filter: &str,
    latency_ms: u64,
    hits: &[ContextNode],
) -> Value {
    json!({
        "stage": stage,
        "index_uid": index_uid,
        "query": query,
        "filter": filter,
        "hits": hits.len(),
        "latency_ms": latency_ms,
        "selected_uris": hits
            .iter()
            .map(|node| truncate_chars(&node.uri, 240))
            .collect::<Vec<_>>()
    })
}

fn strip_context_layer_suffix(uri: &str) -> String {
    uri.strip_suffix("/.abstract")
        .or_else(|| uri.strip_suffix("/.overview"))
        .or_else(|| uri.strip_suffix("/detail"))
        .or_else(|| uri.strip_suffix("/chunks/0001"))
        .unwrap_or(uri)
        .to_string()
}
