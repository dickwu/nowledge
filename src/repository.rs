use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Map, Value};

use crate::{
    config::{Config, DEFAULT_MEILI_SCAN_MAX_DOCUMENTS, DEFAULT_MEILI_SCAN_PAGE_SIZE},
    error::{safe_cause_diagnostic, safe_value_fingerprint, ApiError},
    meili::{MeiliAdmin, SearchResponse},
    models::*,
    resolver::EventIndexResolver,
    tenant_scope::{
        is_tenant_document, owner_scoped_storage_identity, restore_logical_id,
        scoped_storage_identity, tenant_document, tenant_document_with_storage_identity,
        tenant_structured_row_document, TenantFilter,
    },
    util::{hmac_hex, text_score},
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

    /// Reconcile a registry-owned dynamic index during startup without
    /// creating a missing index. Backends without an asynchronous index
    /// lifecycle may use the ordinary ensure behavior.
    async fn reconcile_registered_user_event_index(
        &self,
        index: &UserEventIndex,
    ) -> Result<Vec<String>, ApiError> {
        self.ensure_user_event_index(index).await
    }

    async fn list_user_event_indexes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<UserEventIndex>>, ApiError>;

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

    async fn list_personal_context_nodes(
        &self,
        tenant_id: &str,
        index_uid: &str,
    ) -> Result<Option<Vec<ContextNode>>, ApiError>;

    /// Rehydrate every persisted CompanySource for `tenant_id` so the
    /// in-memory `sources` table survives a restart without crossing tenants.
    async fn list_company_sources(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<CompanySource>>, ApiError>;

    /// Rehydrate every persisted SourceRevision for `tenant_id` so revision
    /// history and edit/reindex flows keep working after a restart. Returned
    /// rows are regrouped into `source_revisions` by source_id in the store.
    async fn list_source_revisions(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<SourceRevision>>, ApiError>;

    async fn upsert_state_item(&self, item: &StateItem) -> Result<Option<String>, ApiError>;

    async fn list_state_items(&self, tenant_id: &str) -> Result<Option<Vec<StateItem>>, ApiError>;

    async fn upsert_insight(&self, insight: &InsightRecord) -> Result<Option<String>, ApiError>;

    async fn list_insights(&self, tenant_id: &str) -> Result<Option<Vec<InsightRecord>>, ApiError>;

    async fn upsert_company_source(
        &self,
        source: &CompanySource,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_source_revision(
        &self,
        revision: &SourceRevision,
    ) -> Result<Option<String>, ApiError>;

    /// Remove a company source and every Meili row that references it
    /// (fragments, revisions, source pointer, plus operational auxiliary
    /// tracking rows). In-memory backends return an empty report.
    async fn delete_company_source(
        &self,
        tenant_id: &str,
        source_id: &str,
    ) -> Result<DeleteSourceReport, ApiError>;

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

    async fn upsert_dataset(&self, dataset: &DatasetRecord) -> Result<Option<String>, ApiError>;

    async fn list_datasets(&self, tenant_id: &str) -> Result<Option<Vec<DatasetRecord>>, ApiError>;

    async fn list_structured_snapshots(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<StructuredSnapshot>>, ApiError>;

    async fn upsert_structured_rows(
        &self,
        tenant_id: &str,
        rows: &[Value],
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_structured_summary(
        &self,
        tenant_id: &str,
        summary: &Value,
    ) -> Result<Option<String>, ApiError>;

    async fn list_structured_summaries(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<Value>>, ApiError>;

    async fn upsert_session(&self, session: &SessionRecord) -> Result<Option<String>, ApiError>;

    async fn list_sessions(&self, tenant_id: &str) -> Result<Option<Vec<SessionRecord>>, ApiError>;

    async fn upsert_trace(&self, trace: &TraceRecord) -> Result<Option<String>, ApiError>;

    async fn list_traces(&self, tenant_id: &str) -> Result<Option<Vec<TraceRecord>>, ApiError>;

    async fn upsert_links(&self, links: &[KnowledgeLink]) -> Result<Option<String>, ApiError>;

    async fn list_links(&self, tenant_id: &str) -> Result<Option<Vec<KnowledgeLink>>, ApiError>;

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

    async fn upsert_ingest_tasks(&self, tasks: &[IngestTask]) -> Result<Vec<String>, ApiError> {
        let mut task_uids = Vec::new();
        for task in tasks {
            if let Some(task_uid) = self.upsert_ingest_task(task).await? {
                task_uids.push(task_uid);
            }
        }
        Ok(task_uids)
    }

    async fn upsert_ingest_result(
        &self,
        result: &IngestTaskResult,
    ) -> Result<Option<String>, ApiError>;

    /// Confirm that previously accepted backend tasks have completed
    /// successfully. Memory-backed repositories have no asynchronous commit
    /// boundary, so the default is a no-op. Startup recovery uses this hook
    /// unconditionally before publishing recovered state as ready.
    async fn wait_for_tasks(&self, _task_uids: &[String]) -> Result<(), ApiError> {
        Ok(())
    }

    /// Remove expired ingest tasks (and their stored results) from the
    /// backing store, keyed by task id. The memory backend is a no-op — the
    /// in-memory maps are canonical there and the Store prunes them itself.
    async fn delete_ingest_tasks(
        &self,
        tenant_id: &str,
        task_ids: &[String],
    ) -> Result<(), ApiError>;

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

    /// Load every persisted parse artifact for `tenant_id`, across company
    /// scope and every private owner scope. Startup hydration uses this
    /// tenant-wide contract because retained artifacts outlive operational
    /// ingest task/result rows.
    async fn list_tenant_parse_artifacts(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<ParseArtifact>>, ApiError> {
        Ok(None)
    }

    /// Load parse artifacts for one exact owner scope. `owner_user_id = None`
    /// means company scope only; use `list_tenant_parse_artifacts` when all
    /// private owners must be included.
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

    /// Load every active tenant document matching a public ContextFS URI.
    /// This is reserved for administrator reads without an explicit owner so
    /// the Store can reject ambiguous private matches instead of selecting an
    /// arbitrary owner.
    async fn list_source_documents_by_uri(
        &self,
        tenant_id: &str,
        uri: &str,
    ) -> Result<Option<Vec<SourceDocument>>, ApiError>;

    async fn get_trace(
        &self,
        tenant_id: &str,
        trace_id: &str,
    ) -> Result<Option<TraceRecord>, ApiError>;

    async fn get_snapshot(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<Option<StructuredSnapshot>, ApiError>;

    async fn list_rows(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<Option<Vec<Value>>, ApiError>;

    async fn debug_search(
        &self,
        tenant_id: &str,
        index_uid: &str,
        query: &str,
    ) -> Result<Option<Value>, ApiError>;
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

    async fn list_user_event_indexes(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<UserEventIndex>>, ApiError> {
        Ok(None)
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

    async fn list_personal_context_nodes(
        &self,
        _tenant_id: &str,
        _index_uid: &str,
    ) -> Result<Option<Vec<ContextNode>>, ApiError> {
        Ok(None)
    }

    async fn list_company_sources(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<CompanySource>>, ApiError> {
        Ok(None)
    }

    async fn list_source_revisions(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<SourceRevision>>, ApiError> {
        Ok(None)
    }

    async fn upsert_state_item(&self, _item: &StateItem) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_state_items(&self, _tenant_id: &str) -> Result<Option<Vec<StateItem>>, ApiError> {
        Ok(None)
    }

    async fn upsert_insight(&self, _insight: &InsightRecord) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_insights(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<InsightRecord>>, ApiError> {
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

    async fn delete_company_source(
        &self,
        _tenant_id: &str,
        _source_id: &str,
    ) -> Result<DeleteSourceReport, ApiError> {
        Ok(DeleteSourceReport::default())
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

    async fn upsert_dataset(&self, _dataset: &DatasetRecord) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_datasets(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<DatasetRecord>>, ApiError> {
        Ok(None)
    }

    async fn list_structured_snapshots(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<StructuredSnapshot>>, ApiError> {
        Ok(None)
    }

    async fn upsert_structured_rows(
        &self,
        _tenant_id: &str,
        _rows: &[Value],
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn upsert_structured_summary(
        &self,
        _tenant_id: &str,
        _summary: &Value,
    ) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_structured_summaries(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<Value>>, ApiError> {
        Ok(None)
    }

    async fn upsert_session(&self, _session: &SessionRecord) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_sessions(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<SessionRecord>>, ApiError> {
        Ok(None)
    }

    async fn upsert_trace(&self, _trace: &TraceRecord) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_traces(&self, _tenant_id: &str) -> Result<Option<Vec<TraceRecord>>, ApiError> {
        Ok(None)
    }

    async fn upsert_links(&self, _links: &[KnowledgeLink]) -> Result<Option<String>, ApiError> {
        Ok(None)
    }

    async fn list_links(&self, _tenant_id: &str) -> Result<Option<Vec<KnowledgeLink>>, ApiError> {
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

    async fn delete_ingest_tasks(
        &self,
        _tenant_id: &str,
        _task_ids: &[String],
    ) -> Result<(), ApiError> {
        Ok(())
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

    async fn list_source_documents_by_uri(
        &self,
        _tenant_id: &str,
        _uri: &str,
    ) -> Result<Option<Vec<SourceDocument>>, ApiError> {
        Ok(None)
    }

    async fn get_trace(
        &self,
        _tenant_id: &str,
        _trace_id: &str,
    ) -> Result<Option<TraceRecord>, ApiError> {
        Ok(None)
    }

    async fn get_snapshot(
        &self,
        _tenant_id: &str,
        _snapshot_id: &str,
    ) -> Result<Option<StructuredSnapshot>, ApiError> {
        Ok(None)
    }

    async fn list_rows(
        &self,
        _tenant_id: &str,
        _snapshot_id: &str,
    ) -> Result<Option<Vec<Value>>, ApiError> {
        Ok(None)
    }

    async fn debug_search(
        &self,
        _tenant_id: &str,
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
    scan_page_size: usize,
    scan_max_documents: usize,
}

impl MeiliRepository {
    pub fn new(admin: MeiliAdmin, wait_for_tasks: bool) -> Self {
        Self::new_with_scan_limits(
            admin,
            wait_for_tasks,
            DEFAULT_MEILI_SCAN_PAGE_SIZE,
            DEFAULT_MEILI_SCAN_MAX_DOCUMENTS,
        )
    }

    pub fn new_with_scan_limits(
        admin: MeiliAdmin,
        wait_for_tasks: bool,
        scan_page_size: usize,
        scan_max_documents: usize,
    ) -> Self {
        Self {
            admin,
            wait_for_tasks,
            scan_page_size,
            scan_max_documents,
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
        let legacy_documents = self
            .load_same_tenant_legacy_documents(index_uid, documents)
            .await?;
        let mut write_documents = documents.to_vec();
        write_documents.extend(legacy_mirror_documents(
            index_uid,
            documents,
            &legacy_documents,
        ));
        let task_uid = self
            .admin
            .add_documents(index_uid, &write_documents)
            .await?;
        self.maybe_wait(&task_uid).await?;
        Ok(task_uid)
    }

    async fn load_same_tenant_legacy_documents(
        &self,
        index_uid: &str,
        documents: &[Value],
    ) -> Result<Vec<Value>, ApiError> {
        let mut candidates: HashMap<String, HashSet<String>> = HashMap::new();
        for document in documents {
            if !is_tenant_document(index_uid, document) {
                continue;
            }
            let Some(tenant_id) = document.get("tenant_id").and_then(Value::as_str) else {
                continue;
            };
            let Some(legacy_id) = legacy_document_id(index_uid, document) else {
                continue;
            };
            candidates
                .entry(tenant_id.to_string())
                .or_default()
                .insert(legacy_id);
        }

        let mut legacy_documents = Vec::new();
        for (tenant_id, ids) in candidates {
            let ids = ids.into_iter().collect::<Vec<_>>();
            for chunk in ids.chunks(128) {
                let response: SearchResponse<Value> = self
                    .admin
                    .search(
                        index_uid,
                        json!({
                            "q": "",
                            "limit": chunk.len().max(1),
                            "filter": TenantFilter::new(&tenant_id)?
                                .in_strings("id", chunk)?
                                .finish()
                        }),
                    )
                    .await?;
                legacy_documents.extend(
                    response
                        .hits
                        .into_iter()
                        .filter(|document| !is_tenant_document(index_uid, document)),
                );
            }
        }
        Ok(legacy_documents)
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
            .upsert_values(
                "rag_user_event_indexes",
                &[tenant_document(
                    &index.tenant_id,
                    "rag_user_event_indexes",
                    &index.id,
                    index,
                )?],
            )
            .await?;
        if let Some(task_uid) = registry_task {
            task_uids.push(task_uid);
        }
        if self.wait_for_tasks {
            self.admin.wait_for_tasks(&task_uids).await?;
        }
        Ok(task_uids)
    }

    async fn reconcile_registered_user_event_index(
        &self,
        index: &UserEventIndex,
    ) -> Result<Vec<String>, ApiError> {
        let mut task_uids = self
            .admin
            .reconcile_existing_index(&index.event_index_uid, true)
            .await?;
        task_uids.extend(
            self.admin
                .reconcile_existing_index(&index.personal_context_index_uid, true)
                .await?,
        );
        let registry_task = self
            .upsert_values(
                "rag_user_event_indexes",
                &[tenant_document(
                    &index.tenant_id,
                    "rag_user_event_indexes",
                    &index.id,
                    index,
                )?],
            )
            .await?;
        if let Some(task_uid) = registry_task {
            task_uids.push(task_uid);
        }
        if self.wait_for_tasks {
            self.admin.wait_for_tasks(&task_uids).await?;
        }
        Ok(task_uids)
    }

    async fn list_user_event_indexes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<UserEventIndex>>, ApiError> {
        self.search_tenant_many(
            "rag_user_event_indexes",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:asc"]),
        )
        .await
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
            .map(|node| tenant_document(&node.tenant_id, index_uid, &node.uri, node))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values(index_uid, &documents).await
    }

    async fn list_company_context_nodes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<ContextNode>>, ApiError> {
        self.search_tenant_many(
            "rag_company_context",
            TenantFilter::new(tenant_id)?,
            Some(&["updated_at:desc"]),
        )
        .await
    }

    async fn list_personal_context_nodes(
        &self,
        tenant_id: &str,
        index_uid: &str,
    ) -> Result<Option<Vec<ContextNode>>, ApiError> {
        self.search_tenant_many(
            index_uid,
            TenantFilter::new(tenant_id)?,
            Some(&["updated_at:desc"]),
        )
        .await
    }

    async fn list_company_sources(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<CompanySource>>, ApiError> {
        self.search_tenant_many("rag_sources", TenantFilter::new(tenant_id)?, None)
            .await
    }

    async fn list_source_revisions(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<SourceRevision>>, ApiError> {
        self.search_tenant_many("rag_source_revisions", TenantFilter::new(tenant_id)?, None)
            .await
    }

    async fn upsert_state_item(&self, item: &StateItem) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_state_items",
            &[tenant_document(
                &item.tenant_id,
                "rag_state_items",
                &item.id,
                item,
            )?],
        )
        .await
    }

    async fn list_state_items(&self, tenant_id: &str) -> Result<Option<Vec<StateItem>>, ApiError> {
        self.search_tenant_many(
            "rag_state_items",
            TenantFilter::new(tenant_id)?,
            Some(&["updated_at:desc"]),
        )
        .await
    }

    async fn upsert_insight(&self, insight: &InsightRecord) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_insights",
            &[tenant_document(
                &insight.tenant_id,
                "rag_insights",
                &insight.id,
                insight,
            )?],
        )
        .await
    }

    async fn list_insights(&self, tenant_id: &str) -> Result<Option<Vec<InsightRecord>>, ApiError> {
        self.search_tenant_many(
            "rag_insights",
            TenantFilter::new(tenant_id)?,
            Some(&["updated_at:desc"]),
        )
        .await
    }

    async fn upsert_company_source(
        &self,
        source: &CompanySource,
    ) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_sources",
            &[tenant_document(
                &source.tenant_id,
                "rag_sources",
                &source.id,
                source,
            )?],
        )
        .await
    }

    async fn upsert_source_revision(
        &self,
        revision: &SourceRevision,
    ) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_source_revisions",
            &[tenant_document(
                &revision.tenant_id,
                "rag_source_revisions",
                &revision.id,
                revision,
            )?],
        )
        .await
    }

    async fn delete_company_source(
        &self,
        tenant_id: &str,
        source_id: &str,
    ) -> Result<DeleteSourceReport, ApiError> {
        let source_filter = TenantFilter::new(tenant_id)?
            .logical_id(source_id)?
            .finish();
        let related_filter = TenantFilter::new(tenant_id)?
            .eq("source_id", source_id)?
            .finish();
        let company_auxiliary_filter = TenantFilter::new(tenant_id)?
            .eq("source_id", source_id)?
            .is_null("owner_user_id")
            .finish();

        // 1. Fragments — stop search hits immediately.
        let mut report = DeleteSourceReport {
            fragments_task: self
                .admin
                .delete_documents_by_filter("rag_company_context", &related_filter)
                .await?,
            ..Default::default()
        };
        self.maybe_wait(&report.fragments_task).await?;

        // 2. Revision content blobs.
        report.revisions_task = self
            .admin
            .delete_documents_by_filter("rag_source_revisions", &related_filter)
            .await?;
        self.maybe_wait(&report.revisions_task).await?;

        // 3. Source pointer. Legacy rows without proven tenant ownership are
        // deliberately retained for tenant_scope_v1 quarantine/verification.
        report.source_task = self
            .admin
            .delete_documents_by_filter("rag_sources", &source_filter)
            .await?;
        self.maybe_wait(&report.source_task).await?;

        // 4. Auxiliary indices — best-effort. Orphan rows here are
        //    harmless once the canonical source is gone, but cleaning
        //    them keeps Meili lean. Errors are logged, not fatal.
        for aux_uid in [
            "rag_source_documents",
            "rag_parse_artifacts",
            "rag_ingest_tasks",
            "rag_ingest_results",
        ] {
            match self
                .admin
                .delete_documents_by_filter(aux_uid, &company_auxiliary_filter)
                .await
            {
                Ok(Some(task)) => {
                    let wait_task = Some(task.clone());
                    let _ = self.maybe_wait(&wait_task).await;
                    report.auxiliary_tasks.push(task);
                }
                Ok(None) => {}
                Err(e) => {
                    let diagnostic = safe_cause_diagnostic(&e);
                    let source_fingerprint = safe_value_fingerprint("company_source_id", source_id);
                    tracing::warn!(
                        target: "nowledge::delete_company_source",
                        index_kind = aux_uid,
                        %source_fingerprint,
                        cause_category = diagnostic.category,
                        cause_fingerprint = %diagnostic.fingerprint,
                        "auxiliary cleanup failed; continuing"
                    );
                }
            }
        }

        Ok(report)
    }

    async fn upsert_source_documents(
        &self,
        documents: &[SourceDocument],
    ) -> Result<Option<String>, ApiError> {
        let documents = documents
            .iter()
            .map(|document| {
                tenant_document(
                    &document.tenant_id,
                    "rag_source_documents",
                    &document.id,
                    document,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_source_documents", &documents).await
    }

    async fn upsert_parse_artifacts(
        &self,
        artifacts: &[ParseArtifact],
    ) -> Result<Option<String>, ApiError> {
        let documents = artifacts
            .iter()
            .map(|artifact| {
                let storage_identity =
                    owner_scoped_storage_identity(artifact.owner_user_id.as_deref(), &artifact.id)?;
                tenant_document_with_storage_identity(
                    &artifact.tenant_id,
                    "rag_parse_artifacts",
                    &artifact.id,
                    &storage_identity,
                    artifact,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_parse_artifacts", &documents).await
    }

    async fn upsert_structured_snapshot(
        &self,
        snapshot: &StructuredSnapshot,
    ) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_structured_snapshots",
            &[tenant_document(
                &snapshot.tenant_id,
                "rag_structured_snapshots",
                &snapshot.id,
                snapshot,
            )?],
        )
        .await
    }

    async fn upsert_dataset(&self, dataset: &DatasetRecord) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_structured_datasets",
            &[tenant_document(
                &dataset.tenant_id,
                "rag_structured_datasets",
                &dataset.id,
                dataset,
            )?],
        )
        .await
    }

    async fn list_datasets(&self, tenant_id: &str) -> Result<Option<Vec<DatasetRecord>>, ApiError> {
        self.search_tenant_many(
            "rag_structured_datasets",
            TenantFilter::new(tenant_id)?,
            None,
        )
        .await
    }

    async fn list_structured_snapshots(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<StructuredSnapshot>>, ApiError> {
        self.search_tenant_many(
            "rag_structured_snapshots",
            TenantFilter::new(tenant_id)?,
            None,
        )
        .await
    }

    async fn upsert_structured_rows(
        &self,
        tenant_id: &str,
        rows: &[Value],
    ) -> Result<Option<String>, ApiError> {
        let documents = rows
            .iter()
            .map(|row| tenant_structured_row_document(tenant_id, row))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_structured_rows", &documents).await
    }

    async fn upsert_structured_summary(
        &self,
        tenant_id: &str,
        summary: &Value,
    ) -> Result<Option<String>, ApiError> {
        let id = summary
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::Internal("structured summary is missing id".to_string()))?;
        self.upsert_values(
            "rag_structured_summaries",
            &[tenant_document(
                tenant_id,
                "rag_structured_summaries",
                id,
                summary,
            )?],
        )
        .await
    }

    async fn list_structured_summaries(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<Value>>, ApiError> {
        self.search_tenant_many(
            "rag_structured_summaries",
            TenantFilter::new(tenant_id)?,
            None,
        )
        .await
    }

    async fn upsert_session(&self, session: &SessionRecord) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_sessions",
            &[tenant_document(
                &session.tenant_id,
                "rag_sessions",
                &session.id,
                session,
            )?],
        )
        .await
    }

    async fn list_sessions(&self, tenant_id: &str) -> Result<Option<Vec<SessionRecord>>, ApiError> {
        self.search_tenant_many(
            "rag_sessions",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:asc"]),
        )
        .await
    }

    async fn upsert_trace(&self, trace: &TraceRecord) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_traces",
            &[tenant_document(
                &trace.tenant_id,
                "rag_traces",
                &trace.id,
                trace,
            )?],
        )
        .await
    }

    async fn list_traces(&self, tenant_id: &str) -> Result<Option<Vec<TraceRecord>>, ApiError> {
        self.search_tenant_many(
            "rag_traces",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:asc"]),
        )
        .await
    }

    async fn upsert_links(&self, links: &[KnowledgeLink]) -> Result<Option<String>, ApiError> {
        let documents = links
            .iter()
            .map(|link| tenant_document(&link.tenant_id, "rag_links", &link.id, link))
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_links", &documents).await
    }

    async fn list_links(&self, tenant_id: &str) -> Result<Option<Vec<KnowledgeLink>>, ApiError> {
        self.search_tenant_many(
            "rag_links",
            TenantFilter::new(tenant_id)?,
            Some(&["updated_at:desc"]),
        )
        .await
    }

    async fn upsert_harness_components(
        &self,
        components: &[HarnessComponent],
        revisions: &[HarnessComponentRevision],
    ) -> Result<Option<String>, ApiError> {
        let mut documents = components
            .iter()
            .map(|component| {
                tenant_document(
                    &component.tenant_id,
                    "rag_harness_components:component",
                    &component.id,
                    component,
                )
                .map(|mut document| {
                    document["doc_kind"] = Value::String("component".to_string());
                    document
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        documents.extend(
            revisions
                .iter()
                .map(|revision| {
                    tenant_document(
                        &revision.tenant_id,
                        "rag_harness_components:revision",
                        &revision.id,
                        revision,
                    )
                    .map(|mut document| {
                        document["doc_kind"] = Value::String("revision".to_string());
                        document
                    })
                })
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
            .map(|change| {
                tenant_document(&change.tenant_id, "rag_harness_changes", &change.id, change)
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_harness_changes", &documents).await
    }

    async fn upsert_harness_verdicts(
        &self,
        verdicts: &[HarnessChangeVerdict],
    ) -> Result<Option<String>, ApiError> {
        let documents = verdicts
            .iter()
            .map(|verdict| {
                tenant_document(
                    &verdict.tenant_id,
                    "rag_harness_verdicts",
                    &verdict.id,
                    verdict,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.upsert_values("rag_harness_verdicts", &documents).await
    }

    async fn upsert_ingest_task(&self, task: &IngestTask) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_ingest_tasks",
            &[tenant_document(
                &task.tenant_id,
                "rag_ingest_tasks",
                &task.task_id,
                task,
            )?],
        )
        .await
    }

    async fn upsert_ingest_tasks(&self, tasks: &[IngestTask]) -> Result<Vec<String>, ApiError> {
        let documents = tasks
            .iter()
            .map(|task| tenant_document(&task.tenant_id, "rag_ingest_tasks", &task.task_id, task))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self
            .upsert_values("rag_ingest_tasks", &documents)
            .await?
            .into_iter()
            .collect())
    }

    async fn upsert_ingest_result(
        &self,
        result: &IngestTaskResult,
    ) -> Result<Option<String>, ApiError> {
        let mut document = tenant_document(
            &result.task.tenant_id,
            "rag_ingest_results",
            &result.task.task_id,
            result,
        )?;
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

    async fn wait_for_tasks(&self, task_uids: &[String]) -> Result<(), ApiError> {
        self.admin.wait_for_tasks(task_uids).await
    }

    async fn delete_ingest_tasks(
        &self,
        tenant_id: &str,
        task_ids: &[String],
    ) -> Result<(), ApiError> {
        if task_ids.is_empty() {
            return Ok(());
        }
        // Best-effort on both indexes: a failed delete only delays cleanup
        // until the next sweep, so log and continue rather than abort.
        for index_uid in ["rag_ingest_tasks", "rag_ingest_results"] {
            let filter = TenantFilter::new(tenant_id)?
                .in_strings("task_id", task_ids)?
                .finish();
            match self
                .admin
                .delete_documents_by_filter(index_uid, &filter)
                .await
            {
                Ok(task) => {
                    let _ = self.maybe_wait(&task).await;
                }
                Err(e) => {
                    let diagnostic = safe_cause_diagnostic(&e);
                    tracing::warn!(
                        target: "nowledge::ingest_cleanup",
                        index_kind = index_uid,
                        document_count = task_ids.len(),
                        cause_category = diagnostic.category,
                        cause_fingerprint = %diagnostic.fingerprint,
                        "failed to delete expired ingest documents"
                    );
                }
            }
        }
        Ok(())
    }

    async fn upsert_eval_case(&self, case: &RagEvalCase) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_eval_cases",
            &[tenant_document(
                &case.tenant_id,
                "rag_eval_cases",
                &case.id,
                case,
            )?],
        )
        .await
    }

    async fn upsert_eval_run(&self, run: &RagEvalRun) -> Result<Option<String>, ApiError> {
        self.upsert_values(
            "rag_eval_runs",
            &[tenant_document(
                &run.tenant_id,
                "rag_eval_runs",
                &run.id,
                run,
            )?],
        )
        .await
    }

    async fn upsert_eval_case_results(
        &self,
        results: &[RagEvalCaseResult],
    ) -> Result<Option<String>, ApiError> {
        let documents = results
            .iter()
            .map(|result| {
                tenant_document(
                    &result.tenant_id,
                    "rag_eval_case_results",
                    &result.id,
                    result,
                )
            })
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
            &[tenant_document(
                &overview.tenant_id,
                "rag_eval_overviews",
                &overview.run_id,
                overview,
            )?],
        )
        .await
    }

    async fn list_harness_components(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<HarnessComponent>>, ApiError> {
        self.search_tenant_many(
            "rag_harness_components",
            TenantFilter::new(tenant_id)?.eq("doc_kind", "component")?,
            Some(&["logical_id:asc"]),
        )
        .await
    }

    async fn list_harness_component_revisions(
        &self,
        tenant_id: &str,
        component_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessComponentRevision>>, ApiError> {
        let mut filter = TenantFilter::new(tenant_id)?.eq("doc_kind", "revision")?;
        if let Some(component_id) = component_id {
            filter = filter.eq("component_id", component_id)?;
        }
        self.search_tenant_many("rag_harness_components", filter, Some(&["iteration:asc"]))
            .await
    }

    async fn get_harness_change(
        &self,
        tenant_id: &str,
        change_id: &str,
    ) -> Result<Option<HarnessChangeManifest>, ApiError> {
        self.search_tenant_one(
            "rag_harness_changes",
            TenantFilter::new(tenant_id)?.logical_id(change_id)?,
        )
        .await
    }

    async fn list_harness_changes(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<HarnessChangeManifest>>, ApiError> {
        self.search_tenant_many(
            "rag_harness_changes",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:desc"]),
        )
        .await
    }

    async fn list_harness_verdicts(
        &self,
        tenant_id: &str,
        change_id: Option<&str>,
    ) -> Result<Option<Vec<HarnessChangeVerdict>>, ApiError> {
        let mut filter = TenantFilter::new(tenant_id)?;
        if let Some(change_id) = change_id {
            filter = filter.eq("change_id", change_id)?;
        }
        self.search_tenant_many("rag_harness_verdicts", filter, Some(&["created_at:desc"]))
            .await
    }

    async fn get_ingest_task(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<Option<IngestTask>, ApiError> {
        self.search_tenant_one(
            "rag_ingest_tasks",
            TenantFilter::new(tenant_id)?.eq("task_id", task_id)?,
        )
        .await
    }

    async fn get_ingest_result(
        &self,
        tenant_id: &str,
        task_id: &str,
    ) -> Result<Option<IngestTaskResult>, ApiError> {
        self.search_tenant_one(
            "rag_ingest_results",
            TenantFilter::new(tenant_id)?.eq("task_id", task_id)?,
        )
        .await
    }

    async fn list_ingest_tasks(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<IngestTask>>, ApiError> {
        self.search_tenant_many(
            "rag_ingest_tasks",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:desc"]),
        )
        .await
    }

    async fn list_ingest_results(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<IngestTaskResult>>, ApiError> {
        self.search_tenant_many("rag_ingest_results", TenantFilter::new(tenant_id)?, None)
            .await
    }

    async fn list_eval_cases(&self, tenant_id: &str) -> Result<Option<Vec<RagEvalCase>>, ApiError> {
        self.search_tenant_many(
            "rag_eval_cases",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:asc"]),
        )
        .await
    }

    async fn get_eval_run(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<RagEvalRun>, ApiError> {
        self.search_tenant_one(
            "rag_eval_runs",
            TenantFilter::new(tenant_id)?.logical_id(run_id)?,
        )
        .await
    }

    async fn list_eval_runs(&self, tenant_id: &str) -> Result<Option<Vec<RagEvalRun>>, ApiError> {
        self.search_tenant_many(
            "rag_eval_runs",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:desc"]),
        )
        .await
    }

    async fn get_eval_overview(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<RagEvalOverview>, ApiError> {
        self.search_tenant_one(
            "rag_eval_overviews",
            TenantFilter::new(tenant_id)?.eq("run_id", run_id)?,
        )
        .await
    }

    async fn list_eval_case_results(
        &self,
        tenant_id: &str,
        run_id: &str,
    ) -> Result<Option<Vec<RagEvalCaseResult>>, ApiError> {
        self.search_tenant_many(
            "rag_eval_case_results",
            TenantFilter::new(tenant_id)?.eq("run_id", run_id)?,
            None,
        )
        .await
    }

    async fn list_tenant_parse_artifacts(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<ParseArtifact>>, ApiError> {
        self.search_tenant_many(
            "rag_parse_artifacts",
            TenantFilter::new(tenant_id)?,
            Some(&["created_at:asc"]),
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
        let mut filter = TenantFilter::new(tenant_id)?;
        if let Some(owner) = owner_user_id {
            filter = filter.eq("owner_user_id", owner)?;
        } else {
            filter = filter.is_null("owner_user_id");
        }
        if let Some(source_id) = source_id {
            filter = filter.eq("source_id", source_id)?;
        }
        if let Some(revision_id) = revision_id {
            filter = filter.eq("revision_id", revision_id)?;
        }
        self.search_tenant_many("rag_parse_artifacts", filter, Some(&["created_at:asc"]))
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

        let company_filter = context_filter(
            request.tenant_id,
            None,
            "rag_company_context",
            request.filters,
        )?;
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
            let personal_filter = context_filter(
                request.tenant_id,
                Some(owner),
                &routing.personal_context_index_uid,
                request.filters,
            )?;
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
        let indexes = if let Some(owner) = owner_user_id {
            let routing = resolver.resolve(tenant_id, owner, false, true)?;
            vec![
                (routing.personal_context_index_uid, Some(owner)),
                ("rag_company_context".to_string(), None),
            ]
        } else {
            vec![("rag_company_context".to_string(), None)]
        };

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
                filters.push(format!("index_uid = {}", meili_string(&index_uid)?));
                filters.push("index_kind = \"personal\"".to_string());
            } else {
                filters.push("(owner_user_id IS NULL OR owner_user_id NOT EXISTS)".to_string());
                filters.push("privacy = \"company\"".to_string());
                filters.push("index_uid = \"rag_company_context\"".to_string());
                filters.push("index_kind = \"company\"".to_string());
            }
            let response = self
                .search_context_index(&index_uid, &target, &filters.join(" AND "), 20)
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
        if let Some(owner) = owner_user_id {
            if let Some(document) = self
                .search_tenant_one(
                    "rag_source_documents",
                    TenantFilter::new(tenant_id)?
                        .eq("uri", uri)?
                        .eq("status", "active")?
                        .eq("owner_user_id", owner)?,
                )
                .await?
            {
                return Ok(Some(document));
            }
        }
        self.search_tenant_one(
            "rag_source_documents",
            TenantFilter::new(tenant_id)?
                .eq("uri", uri)?
                .eq("status", "active")?
                .is_null("owner_user_id"),
        )
        .await
    }

    async fn list_source_documents_by_uri(
        &self,
        tenant_id: &str,
        uri: &str,
    ) -> Result<Option<Vec<SourceDocument>>, ApiError> {
        self.search_tenant_many(
            "rag_source_documents",
            TenantFilter::new(tenant_id)?
                .eq("uri", uri)?
                .eq("status", "active")?,
            Some(&["updated_at:desc"]),
        )
        .await
    }

    async fn get_trace(
        &self,
        tenant_id: &str,
        trace_id: &str,
    ) -> Result<Option<TraceRecord>, ApiError> {
        self.search_tenant_one(
            "rag_traces",
            TenantFilter::new(tenant_id)?.logical_id(trace_id)?,
        )
        .await
    }

    async fn get_snapshot(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<Option<StructuredSnapshot>, ApiError> {
        self.search_tenant_one(
            "rag_structured_snapshots",
            TenantFilter::new(tenant_id)?.logical_id(snapshot_id)?,
        )
        .await
    }

    async fn list_rows(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<Option<Vec<Value>>, ApiError> {
        let documents = self
            .scan_tenant_documents(
                "rag_structured_rows",
                TenantFilter::new(tenant_id)?.eq("snapshot_id", snapshot_id)?,
                None,
            )
            .await?;
        Ok(Some(
            prefer_tenant_documents("rag_structured_rows", documents)
                .into_iter()
                .map(|document| restore_logical_id("rag_structured_rows", document))
                .collect(),
        ))
    }

    async fn debug_search(
        &self,
        tenant_id: &str,
        index_uid: &str,
        query: &str,
    ) -> Result<Option<Value>, ApiError> {
        let mut response = self
            .admin
            .search_value(
                index_uid,
                json!({
                    "q": query,
                    "limit": 20,
                    "filter": TenantFilter::new(tenant_id)?.finish()
                }),
            )
            .await?;
        if let Some(hits) = response.get_mut("hits").and_then(Value::as_array_mut) {
            let preferred = prefer_tenant_documents(index_uid, std::mem::take(hits));
            *hits = preferred
                .into_iter()
                .map(|document| restore_logical_id(index_uid, document))
                .collect();
        }
        Ok(Some(response))
    }
}

impl MeiliRepository {
    async fn search_tenant_one<T: DeserializeOwned>(
        &self,
        index_uid: &str,
        filter: TenantFilter,
    ) -> Result<Option<T>, ApiError> {
        let response: SearchResponse<Value> = self
            .admin
            .search(
                index_uid,
                json!({
                    "q": "",
                    "limit": 20,
                    "filter": filter.finish()
                }),
            )
            .await?;
        prefer_tenant_documents(index_uid, response.hits)
            .into_iter()
            .next()
            .map(|document| restore_logical_id(index_uid, document))
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| {
                ApiError::Internal(format!(
                    "failed to decode tenant-scoped {index_uid} document: {error}"
                ))
            })
    }

    async fn search_tenant_many<T: DeserializeOwned>(
        &self,
        index_uid: &str,
        filter: TenantFilter,
        sort: Option<&[&str]>,
    ) -> Result<Option<Vec<T>>, ApiError> {
        let documents = self.scan_tenant_documents(index_uid, filter, sort).await?;
        let hits = prefer_tenant_documents(index_uid, documents)
            .into_iter()
            .map(|document| restore_logical_id(index_uid, document))
            .map(serde_json::from_value)
            .collect::<Result<Vec<T>, _>>()
            .map_err(|error| {
                ApiError::Internal(format!(
                    "failed to decode tenant-scoped {index_uid} documents: {error}"
                ))
            })?;
        Ok(Some(hits))
    }

    async fn scan_tenant_documents(
        &self,
        index_uid: &str,
        filter: TenantFilter,
        sort: Option<&[&str]>,
    ) -> Result<Vec<Value>, ApiError> {
        if self.scan_page_size == 0 || self.scan_max_documents == 0 {
            return Err(ApiError::Internal(
                "Meilisearch scan limits must be greater than zero".to_string(),
            ));
        }
        if self.scan_page_size > self.scan_max_documents {
            return Err(ApiError::Internal(
                "Meilisearch scan page size exceeds its document safety ceiling".to_string(),
            ));
        }

        let filter = filter.finish();
        let mut stable_sort = sort
            .unwrap_or_default()
            .iter()
            .map(|value| (*value).to_string())
            .collect::<Vec<_>>();
        if !stable_sort
            .iter()
            .any(|value| value.split(':').next() == Some("id"))
        {
            stable_sort.push("id:asc".to_string());
        }

        let mut documents = Vec::new();
        let mut physical_ids = HashSet::new();
        let mut expected_total = None;
        let mut offset = 0usize;

        loop {
            let remaining = self.scan_max_documents.checked_sub(offset).ok_or_else(|| {
                ApiError::Upstream(format!(
                    "Meilisearch {index_uid} scan exceeded its configured document ceiling"
                ))
            })?;
            if remaining == 0 {
                return Err(ApiError::Upstream(format!(
                    "refusing to truncate Meilisearch {index_uid} scan at {} documents",
                    self.scan_max_documents
                )));
            }
            let requested_limit = self.scan_page_size.min(remaining);
            let page = self
                .admin
                .fetch_filtered_documents_page(
                    index_uid,
                    &filter,
                    &stable_sort,
                    offset,
                    requested_limit,
                )
                .await?;

            if page.offset != offset {
                return Err(ApiError::Upstream(format!(
                    "Meilisearch {index_uid} scan returned offset {} while {} was requested",
                    page.offset, offset
                )));
            }
            if page.limit != requested_limit {
                return Err(ApiError::Upstream(format!(
                    "Meilisearch {index_uid} scan returned limit {} while {} was requested",
                    page.limit, requested_limit
                )));
            }
            if let Some(expected_total) = expected_total {
                if page.total != expected_total {
                    return Err(ApiError::Upstream(format!(
                        "Meilisearch {index_uid} scan total changed from {expected_total} to {}",
                        page.total
                    )));
                }
            } else {
                expected_total = Some(page.total);
            }
            if page.total > self.scan_max_documents {
                return Err(ApiError::Upstream(format!(
                    "refusing to truncate Meilisearch {index_uid} scan: total {} exceeds configured ceiling {}",
                    page.total, self.scan_max_documents
                )));
            }
            if page.results.len() > requested_limit {
                return Err(ApiError::Upstream(format!(
                    "Meilisearch {index_uid} scan returned more documents than requested"
                )));
            }

            let next_offset = offset.checked_add(page.results.len()).ok_or_else(|| {
                ApiError::Upstream(format!("Meilisearch {index_uid} scan offset overflowed"))
            })?;
            if next_offset > page.total {
                return Err(ApiError::Upstream(format!(
                    "Meilisearch {index_uid} scan returned more documents than its reported total"
                )));
            }
            if page.results.is_empty() && offset < page.total {
                return Err(ApiError::Upstream(format!(
                    "Meilisearch {index_uid} scan returned an empty page before its reported total"
                )));
            }

            for document in &page.results {
                let physical_id = match document.get("id") {
                    Some(Value::String(id)) => format!("string:{id}"),
                    Some(Value::Number(id)) => format!("number:{id}"),
                    _ => {
                        return Err(ApiError::Upstream(format!(
                            "Meilisearch {index_uid} scan returned a document without a valid physical id"
                        )))
                    }
                };
                if !physical_ids.insert(physical_id) {
                    return Err(ApiError::Upstream(format!(
                        "Meilisearch {index_uid} scan returned a duplicate physical document id"
                    )));
                }
            }
            documents.extend(page.results);

            if next_offset == page.total {
                break;
            }
            offset = next_offset;
        }

        Ok(documents)
    }

    async fn search_context_index(
        &self,
        index_uid: &str,
        query: &str,
        filter: &str,
        limit: usize,
    ) -> Result<SearchResponse<ContextNode>, ApiError> {
        let response: SearchResponse<Value> = self
            .admin
            .search(
                index_uid,
                json!({
                    "q": query,
                    "limit": limit.max(1),
                    "filter": filter
                }),
            )
            .await?;
        let hits = prefer_tenant_documents(index_uid, response.hits)
            .into_iter()
            .map(|document| restore_logical_id(index_uid, document))
            .map(serde_json::from_value)
            .collect::<Result<Vec<ContextNode>, _>>()
            .map_err(|error| {
                ApiError::Internal(format!(
                    "failed to decode tenant-scoped {index_uid} context documents: {error}"
                ))
            })?;
        Ok(SearchResponse {
            hits,
            processing_time_ms: response.processing_time_ms,
        })
    }
}

pub fn repository_from_config(config: &Config) -> Arc<dyn KnowledgeRepository> {
    if config.store_backend == "meili" && config.meili_url.is_some() {
        Arc::new(MeiliRepository::new_with_scan_limits(
            MeiliAdmin::from_config(config),
            config.meili_wait_for_tasks,
            config.meili_scan_page_size,
            config.meili_scan_max_documents,
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

fn legacy_mirror_documents(
    index_uid: &str,
    new_documents: &[Value],
    legacy_documents: &[Value],
) -> Vec<Value> {
    let mut mirrored_ids = HashSet::new();
    let mut mirrors = Vec::new();
    for legacy in legacy_documents {
        let Some(legacy_id) = legacy.get("id").and_then(Value::as_str) else {
            continue;
        };
        if mirrored_ids.contains(legacy_id) {
            continue;
        }
        let Some(current) = new_documents
            .iter()
            .rev()
            .find(|current| legacy_mirror_matches(index_uid, legacy, current))
        else {
            continue;
        };
        mirrored_ids.insert(legacy_id.to_string());
        let mut mirror = restore_logical_id(index_uid, current.clone());
        let Some(object) = mirror.as_object_mut() else {
            continue;
        };
        object.insert("id".to_string(), Value::String(legacy_id.to_string()));
        mirrors.push(mirror);
    }
    mirrors
}

fn legacy_mirror_matches(index_uid: &str, legacy: &Value, current: &Value) -> bool {
    if !is_tenant_document(index_uid, current) || is_tenant_document(index_uid, legacy) {
        return false;
    }
    if legacy.get("tenant_id").and_then(Value::as_str)
        != current.get("tenant_id").and_then(Value::as_str)
    {
        return false;
    }
    if legacy.get("id").and_then(Value::as_str) != legacy_document_id(index_uid, current).as_deref()
    {
        return false;
    }
    match index_uid {
        "rag_harness_components" => {
            legacy.get("doc_kind").and_then(Value::as_str)
                == current.get("doc_kind").and_then(Value::as_str)
        }
        "rag_structured_rows" => {
            legacy.get("snapshot_id").and_then(Value::as_str)
                == current.get("snapshot_id").and_then(Value::as_str)
        }
        "rag_parse_artifacts" => {
            optional_string(legacy, "owner_user_id") == optional_string(current, "owner_user_id")
        }
        uid if uid == "rag_company_context" || uid.starts_with("rag_context__") => {
            legacy.get("uri").and_then(Value::as_str) == current.get("uri").and_then(Value::as_str)
        }
        _ => true,
    }
}

fn legacy_document_id(index_uid: &str, document: &Value) -> Option<String> {
    let logical_id = document.get("logical_id").and_then(Value::as_str)?;
    if index_uid == "rag_company_context" || index_uid.starts_with("rag_context__") {
        Some(format!(
            "ctx_{}",
            hmac_hex(b"nowledge-context-doc", "uri", logical_id, 24)
        ))
    } else {
        Some(logical_id.to_string())
    }
}

fn optional_string<'a>(document: &'a Value, field: &str) -> Option<&'a str> {
    document.get(field).and_then(Value::as_str)
}

fn meili_string(value: &str) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(|e| ApiError::Internal(e.to_string()))
}

fn meili_string_array(values: &[String]) -> Result<String, ApiError> {
    serde_json::to_string(values).map_err(|e| ApiError::Internal(e.to_string()))
}

fn prefer_tenant_documents(index_uid: &str, documents: Vec<Value>) -> Vec<Value> {
    let mut positions: HashMap<String, usize> = HashMap::new();
    let mut preferred: Vec<Value> = Vec::with_capacity(documents.len());
    for document in documents {
        let Some(key) = tenant_document_key(index_uid, &document) else {
            preferred.push(document);
            continue;
        };
        if let Some(position) = positions.get(&key).copied() {
            let candidate_is_migrated = is_tenant_document(index_uid, &document);
            let current_is_migrated = is_tenant_document(index_uid, &preferred[position]);
            if candidate_is_migrated && !current_is_migrated {
                preferred[position] = document;
            }
            continue;
        }
        positions.insert(key, preferred.len());
        preferred.push(document);
    }
    preferred
}

fn tenant_document_key(index_uid: &str, document: &Value) -> Option<String> {
    let migrated = is_tenant_document(index_uid, document);
    let logical_id = match index_uid {
        uid if uid == "rag_company_context" || uid.starts_with("rag_context__") => {
            document.get("uri").and_then(Value::as_str)
        }
        "rag_eval_overviews" => document.get("run_id").and_then(Value::as_str),
        "rag_ingest_tasks" | "rag_ingest_results" => {
            document.get("task_id").and_then(Value::as_str)
        }
        _ if migrated => document.get("logical_id").and_then(Value::as_str),
        _ => document.get("id").and_then(Value::as_str),
    }?;
    if index_uid == "rag_structured_rows" {
        let snapshot_id = document.get("snapshot_id").and_then(Value::as_str)?;
        return scoped_storage_identity(snapshot_id, logical_id).ok();
    }
    if index_uid == "rag_parse_artifacts" {
        let owner_user_id = match document.get("owner_user_id") {
            Some(Value::String(owner_user_id)) => Some(owner_user_id.as_str()),
            Some(Value::Null) | None => None,
            Some(_) => return None,
        };
        return owner_scoped_storage_identity(owner_user_id, logical_id).ok();
    }
    if index_uid == "rag_harness_components" {
        let kind = document
            .get("doc_kind")
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        Some(format!("{kind}\0{logical_id}"))
    } else {
        Some(logical_id.to_string())
    }
}

fn context_filter(
    tenant_id: &str,
    owner_user_id: Option<&str>,
    index_uid: &str,
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
        filters.push("index_kind = \"personal\"".to_string());
    } else {
        filters.push("(owner_user_id IS NULL OR owner_user_id NOT EXISTS)".to_string());
        filters.push("privacy = \"company\"".to_string());
        filters.push("index_kind = \"company\"".to_string());
    }
    filters.push(format!("index_uid = {}", meili_string(index_uid)?));
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
            .map(|node| node.uri.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_filters_pin_company_and_personal_index_identity() {
        let filters = ContextStructuredFilters::default();
        let company = context_filter("tenant-a", None, "rag_company_context", &filters).unwrap();
        assert!(company.contains("owner_user_id IS NULL"), "{company}");
        assert!(company.contains("owner_user_id NOT EXISTS"), "{company}");
        assert!(company.contains("index_kind = \"company\""), "{company}");
        assert!(
            company.contains("index_uid = \"rag_company_context\""),
            "{company}"
        );

        let personal = context_filter(
            "tenant-a",
            Some("owner-a"),
            "rag_context__tenant__owner",
            &filters,
        )
        .unwrap();
        assert!(
            personal.contains("owner_user_id = \"owner-a\""),
            "{personal}"
        );
        assert!(personal.contains("index_kind = \"personal\""), "{personal}");
        assert!(
            personal.contains("index_uid = \"rag_context__tenant__owner\""),
            "{personal}"
        );
    }

    #[test]
    fn compatibility_mirror_updates_only_an_existing_same_tenant_legacy_row() {
        let current = tenant_document(
            "tenant-a",
            "rag_source_documents",
            "document-1",
            &json!({
                "tenant_id": "tenant-a",
                "status": "superseded",
                "content": "current"
            }),
        )
        .unwrap();
        let mirrors = legacy_mirror_documents(
            "rag_source_documents",
            std::slice::from_ref(&current),
            &[
                json!({
                    "id": "document-1",
                    "tenant_id": "tenant-b",
                    "status": "active",
                    "content": "other tenant"
                }),
                json!({
                    "id": "document-1",
                    "tenant_id": "tenant-a",
                    "status": "active",
                    "content": "stale"
                }),
            ],
        );

        assert_eq!(mirrors.len(), 1);
        assert_eq!(mirrors[0]["id"], "document-1");
        assert_eq!(mirrors[0]["tenant_id"], "tenant-a");
        assert_eq!(mirrors[0]["status"], "superseded");
        assert_eq!(mirrors[0]["content"], "current");
        assert!(mirrors[0].get("logical_id").is_none());
    }

    #[test]
    fn compatibility_mirror_preserves_structured_business_fields_and_snapshot_scope() {
        let current = tenant_structured_row_document(
            "tenant-a",
            &json!({
                "id": "row-1",
                "tenant_id": "tenant-a",
                "snapshot_id": "snapshot-a",
                "logical_id": "business-value",
                "value": "current"
            }),
        )
        .unwrap();
        let mirrors = legacy_mirror_documents(
            "rag_structured_rows",
            &[current],
            &[
                json!({
                    "id": "row-1",
                    "tenant_id": "tenant-a",
                    "snapshot_id": "snapshot-b",
                    "value": "other snapshot"
                }),
                json!({
                    "id": "row-1",
                    "tenant_id": "tenant-a",
                    "snapshot_id": "snapshot-a",
                    "value": "stale"
                }),
            ],
        );

        assert_eq!(mirrors.len(), 1);
        assert_eq!(mirrors[0]["snapshot_id"], "snapshot-a");
        assert_eq!(mirrors[0]["logical_id"], "business-value");
        assert_eq!(mirrors[0]["value"], "current");
    }

    #[test]
    fn parse_artifact_storage_and_legacy_mirroring_are_owner_scoped() {
        let current_for = |owner_user_id: Option<&str>| {
            let storage_identity =
                owner_scoped_storage_identity(owner_user_id, "artifact-1").unwrap();
            tenant_document_with_storage_identity(
                "tenant-a",
                "rag_parse_artifacts",
                "artifact-1",
                &storage_identity,
                &json!({
                    "tenant_id": "tenant-a",
                    "owner_user_id": owner_user_id,
                    "checksum": "current"
                }),
            )
            .unwrap()
        };
        let owner_a = current_for(Some("owner-a"));
        let owner_b = current_for(Some("owner-b"));
        let company = current_for(None);

        assert_ne!(owner_a["id"], owner_b["id"]);
        assert_ne!(owner_a["id"], company["id"]);
        assert_ne!(owner_b["id"], company["id"]);

        let mirrors = legacy_mirror_documents(
            "rag_parse_artifacts",
            &[owner_a, owner_b, company],
            &[json!({
                "id": "artifact-1",
                "tenant_id": "tenant-a",
                "owner_user_id": "owner-b",
                "checksum": "stale"
            })],
        );
        assert_eq!(mirrors.len(), 1);
        assert_eq!(mirrors[0]["id"], "artifact-1");
        assert_eq!(mirrors[0]["owner_user_id"], "owner-b");
        assert_eq!(mirrors[0]["checksum"], "current");
    }

    #[test]
    fn dual_read_prefers_tenant_safe_copy_and_preserves_public_id() {
        let migrated = tenant_document(
            "tenant-a",
            "rag_sources",
            "source-1",
            &json!({"title": "current"}),
        )
        .unwrap();
        let documents = prefer_tenant_documents(
            "rag_sources",
            vec![
                json!({
                    "id": "source-1",
                    "tenant_id": "tenant-a",
                    "title": "legacy"
                }),
                migrated,
            ],
        );

        assert_eq!(documents.len(), 1);
        let restored = restore_logical_id("rag_sources", documents.into_iter().next().unwrap());
        assert_eq!(restored["id"], "source-1");
        assert_eq!(restored["title"], "current");
        assert!(restored.get("logical_id").is_none());
    }

    #[test]
    fn dual_read_deduplicates_legacy_company_context_by_uri() {
        let migrated = tenant_document(
            "tenant-a",
            "rag_company_context",
            "ctx://company/shared",
            &json!({"uri": "ctx://company/shared"}),
        )
        .unwrap();
        let migrated_id = migrated["id"].clone();
        let documents = prefer_tenant_documents(
            "rag_company_context",
            vec![
                json!({
                    "id": "ctx_legacy_hash",
                    "tenant_id": "tenant-a",
                    "uri": "ctx://company/shared"
                }),
                migrated,
            ],
        );

        assert_eq!(documents.len(), 1);
        assert_eq!(documents[0]["id"], migrated_id);
    }

    #[test]
    fn dual_read_keeps_distinct_harness_document_kinds() {
        let documents = prefer_tenant_documents(
            "rag_harness_components",
            vec![
                json!({
                    "id": "same-id",
                    "tenant_id": "tenant-a",
                    "doc_kind": "component"
                }),
                json!({
                    "id": "same-id",
                    "tenant_id": "tenant-a",
                    "doc_kind": "revision"
                }),
            ],
        );

        assert_eq!(documents.len(), 2);
    }

    #[test]
    fn dual_read_deduplicates_structured_rows() {
        let current = tenant_structured_row_document(
            "tenant-a",
            &json!({
                "id": "row-1",
                "tenant_id": "tenant-a",
                "snapshot_id": "snapshot-1",
                "logical_id": "business-current",
                "value": "current"
            }),
        )
        .unwrap();
        let other = tenant_structured_row_document(
            "tenant-a",
            &json!({
                "id": "row-1",
                "tenant_id": "tenant-a",
                "snapshot_id": "snapshot-2",
                "logical_id": "business-other",
                "value": "other"
            }),
        )
        .unwrap();
        let documents = prefer_tenant_documents(
            "rag_structured_rows",
            vec![
                json!({
                    "id": "row-1",
                    "tenant_id": "tenant-a",
                    "snapshot_id": "snapshot-1",
                    "value": "legacy"
                }),
                current,
                other,
            ],
        );

        assert_eq!(documents.len(), 2);
        let restored = documents
            .into_iter()
            .map(|row| restore_logical_id("rag_structured_rows", row))
            .collect::<Vec<_>>();
        assert!(restored
            .iter()
            .any(|row| { row["value"] == "current" && row["logical_id"] == "business-current" }));
        assert!(restored
            .iter()
            .any(|row| { row["value"] == "other" && row["logical_id"] == "business-other" }));
    }
}
