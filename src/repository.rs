use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{
    config::{Config, DEFAULT_MEILI_SCAN_MAX_DOCUMENTS, DEFAULT_MEILI_SCAN_PAGE_SIZE},
    error::ApiError,
    meili::{MeiliAdmin, SearchResponse},
    models::*,
    mutation::{
        operation_list_item, validate_operation_record, validate_operation_step_for_tenant,
    },
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

pub struct RepositoryOperationListQuery<'a> {
    pub tenant_id: &'a str,
    pub statuses: &'a [OperationStatus],
    pub operation_kinds: &'a [String],
    pub offset: usize,
    pub previous_operation_id: Option<&'a str>,
    pub limit: usize,
    pub include_plan: bool,
}

#[derive(Debug, Clone)]
pub struct RepositoryOperationPage {
    pub operations: Vec<OperationListItem>,
    pub has_more: bool,
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

    async fn append_events(
        &self,
        events: &[HistoryEvent],
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        let mut receipt = RepositoryWriteReceipt::empty();
        for event in events {
            receipt.extend(RepositoryWriteReceipt::from_task_uid(
                self.append_event(event).await?,
            ));
        }
        Ok(receipt)
    }

    async fn upsert_operation(
        &self,
        operation: &OperationRecord,
    ) -> Result<RepositoryWriteReceipt, ApiError>;

    async fn upsert_audit_record(
        &self,
        record: &AuditRecord,
    ) -> Result<RepositoryWriteReceipt, ApiError>;

    async fn get_operation(
        &self,
        tenant_id: &str,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>, ApiError>;

    /// Return all matching tenant-scoped operation records. Empty `statuses`
    /// means all statuses. Meili-backed implementations page through the full
    /// bounded tenant scan rather than relying on a single search result page.
    async fn list_operations(
        &self,
        tenant_id: &str,
        statuses: &[OperationStatus],
    ) -> Result<Option<Vec<OperationRecord>>, ApiError>;

    /// Return at most `limit` tenant-scoped operation records matching the
    /// supplied logical IDs and statuses. Implementations should issue one
    /// bounded repository query rather than one query per operation ID.
    async fn list_operations_by_ids(
        &self,
        tenant_id: &str,
        operation_ids: &[String],
        statuses: &[OperationStatus],
        limit: usize,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError>;

    /// Return one filtered, deterministically ordered operation-summary page.
    /// Backends that cannot page at the repository boundary return `None` so
    /// the store can apply the same cursor semantics to its local journal.
    async fn list_operation_page(
        &self,
        _query: RepositoryOperationListQuery<'_>,
    ) -> Result<Option<RepositoryOperationPage>, ApiError> {
        Ok(None)
    }

    /// Return only records that still need write or indexing reconciliation.
    /// Startup uses this bounded working set instead of retained history.
    async fn list_reconcilable_operations(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        Ok(self.list_operations(tenant_id, &[]).await?.map(|records| {
            records
                .into_iter()
                .filter(|record| {
                    record.status != OperationStatus::Completed
                        || record.indexing_state != OperationIndexingState::Completed
                })
                .collect()
        }))
    }

    /// Return the oldest bounded set of tenant-scoped operation records that
    /// still need write or indexing reconciliation. Empty `statuses` means all
    /// statuses. Backends without a durable journal return `None`.
    async fn list_oldest_reconcilable_operations(
        &self,
        tenant_id: &str,
        statuses: &[OperationStatus],
        limit: usize,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError>;

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
        source_document_uris: &[String],
        link_ids: &[String],
    ) -> Result<DeleteSourceReport, ApiError>;

    /// Apply one ordered company-source deletion step. Journaled mutations
    /// use this narrow surface so every accepted backend task is checkpointed
    /// before the following managed index is touched.
    async fn delete_company_source_index(
        &self,
        tenant_id: &str,
        source_id: &str,
        target: &CompanySourceDeleteTarget,
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
    ) -> Result<RepositoryWriteReceipt, ApiError>;

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

    /// Apply one validated, typed step from an immutable operation plan. This
    /// method deliberately delegates to the ordinary aggregate persistence
    /// methods so reconciliation and request-time writes share one code path.
    async fn apply_operation_step(
        &self,
        tenant_id: &str,
        step: &OperationStep,
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        validate_operation_step_for_tenant(tenant_id, step).map_err(|error| {
            ApiError::Internal(format!("invalid persisted operation step: {error}"))
        })?;
        let receipt = match &step.resource {
            OperationResource::EnsureUserEventIndex { index } => RepositoryWriteReceipt {
                task_uids: self.ensure_user_event_index(index).await?,
            },
            OperationResource::HistoryEvents { events } => self.append_events(events).await?,
            OperationResource::ContextNodes { index_uid, nodes } => {
                RepositoryWriteReceipt::from_task_uid(
                    self.upsert_context_nodes(index_uid, nodes).await?,
                )
            }
            OperationResource::StateItem { item } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_state_item(item).await?)
            }
            OperationResource::Insight { insight } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_insight(insight).await?)
            }
            OperationResource::CompanySource { source } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_company_source(source).await?)
            }
            OperationResource::SourceRevision { revision } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_source_revision(revision).await?)
            }
            OperationResource::DeleteCompanySourceIndex { source_id, target } => {
                RepositoryWriteReceipt::from_task_uid(
                    self.delete_company_source_index(tenant_id, source_id, target)
                        .await?,
                )
            }
            OperationResource::SourceDocuments { documents } => {
                RepositoryWriteReceipt::from_task_uid(
                    self.upsert_source_documents(documents).await?,
                )
            }
            OperationResource::ParseArtifacts { artifacts } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_parse_artifacts(artifacts).await?)
            }
            OperationResource::StructuredSnapshot { snapshot } => {
                RepositoryWriteReceipt::from_task_uid(
                    self.upsert_structured_snapshot(snapshot).await?,
                )
            }
            OperationResource::Dataset { dataset } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_dataset(dataset).await?)
            }
            OperationResource::StructuredRows { rows } => RepositoryWriteReceipt::from_task_uid(
                self.upsert_structured_rows(tenant_id, rows).await?,
            ),
            OperationResource::StructuredSummary { summary } => {
                RepositoryWriteReceipt::from_task_uid(
                    self.upsert_structured_summary(tenant_id, summary).await?,
                )
            }
            OperationResource::Session { session } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_session(session).await?)
            }
            OperationResource::Trace { trace } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_trace(trace).await?)
            }
            OperationResource::Links { links } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_links(links).await?)
            }
            OperationResource::HarnessComponents {
                components,
                revisions,
            } => RepositoryWriteReceipt::from_task_uid(
                self.upsert_harness_components(components, revisions)
                    .await?,
            ),
            OperationResource::HarnessChanges { changes } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_harness_changes(changes).await?)
            }
            OperationResource::HarnessVerdicts { verdicts } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_harness_verdicts(verdicts).await?)
            }
            OperationResource::IngestTask { task } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_ingest_task(task).await?)
            }
            OperationResource::IngestTasks { tasks } => RepositoryWriteReceipt {
                task_uids: self.upsert_ingest_tasks(tasks).await?,
            },
            OperationResource::DeleteIngestTasks { task_ids } => {
                self.delete_ingest_tasks(tenant_id, task_ids).await?
            }
            OperationResource::IngestResult { result } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_ingest_result(result).await?)
            }
            OperationResource::EvalCase { case } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_eval_case(case).await?)
            }
            OperationResource::EvalRun { run } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_eval_run(run).await?)
            }
            OperationResource::EvalCaseResults { results } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_eval_case_results(results).await?)
            }
            OperationResource::EvalOverview { overview } => {
                RepositoryWriteReceipt::from_task_uid(self.upsert_eval_overview(overview).await?)
            }
        };
        Ok(receipt)
    }

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

    /// Load the complete company-owned source-document set for one source.
    /// Destructive source operations use this read-through boundary so their
    /// immutable replay plan includes URIs that were not startup-hydrated.
    async fn list_company_source_documents(
        &self,
        tenant_id: &str,
        source_id: &str,
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

    async fn upsert_operation(
        &self,
        operation: &OperationRecord,
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        validate_operation_record(operation).map_err(|error| {
            ApiError::Internal(format!("invalid operation journal record: {error}"))
        })?;
        Ok(RepositoryWriteReceipt::empty())
    }

    async fn upsert_audit_record(
        &self,
        record: &AuditRecord,
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        record
            .validate()
            .map_err(|error| ApiError::Internal(format!("invalid audit record: {error}")))?;
        Ok(RepositoryWriteReceipt::empty())
    }

    async fn get_operation(
        &self,
        _tenant_id: &str,
        _operation_id: &str,
    ) -> Result<Option<OperationRecord>, ApiError> {
        Ok(None)
    }

    async fn list_operations(
        &self,
        _tenant_id: &str,
        _statuses: &[OperationStatus],
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        Ok(None)
    }

    async fn list_operations_by_ids(
        &self,
        _tenant_id: &str,
        _operation_ids: &[String],
        _statuses: &[OperationStatus],
        _limit: usize,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        Ok(None)
    }

    async fn list_reconcilable_operations(
        &self,
        _tenant_id: &str,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        Ok(None)
    }

    async fn list_oldest_reconcilable_operations(
        &self,
        _tenant_id: &str,
        _statuses: &[OperationStatus],
        _limit: usize,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
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
        _source_document_uris: &[String],
        _link_ids: &[String],
    ) -> Result<DeleteSourceReport, ApiError> {
        Ok(DeleteSourceReport::default())
    }

    async fn delete_company_source_index(
        &self,
        _tenant_id: &str,
        _source_id: &str,
        _target: &CompanySourceDeleteTarget,
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
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        Ok(RepositoryWriteReceipt::empty())
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

    async fn list_company_source_documents(
        &self,
        _tenant_id: &str,
        _source_id: &str,
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
    index_admin: MeiliAdmin,
    wait_for_tasks: bool,
    scan_page_size: usize,
    scan_max_documents: usize,
}

const OPERATION_IDENTITY_FIELDS: &[&str] = &["id", "logical_id", "tenant_id"];
const MAX_OPERATION_CANDIDATE_LIMIT: usize = 1_000;
const OPERATION_SUMMARY_FIELDS: &[&str] = &[
    "id",
    "logical_id",
    "tenant_id",
    "operation_kind",
    "actor_scope",
    "idempotency_key_hash",
    "status",
    "indexing_state",
    "progress",
    "created_at",
    "updated_at",
    "completed_at",
    "last_error_category",
    "last_error_fingerprint",
];
const OPERATION_WITH_PLAN_FIELDS: &[&str] = &[
    "id",
    "logical_id",
    "tenant_id",
    "operation_kind",
    "actor_scope",
    "idempotency_key_hash",
    "plan",
    "status",
    "indexing_state",
    "progress",
    "created_at",
    "updated_at",
    "completed_at",
    "last_error_category",
    "last_error_fingerprint",
];

#[derive(Debug, Deserialize)]
struct OperationSummaryDocument {
    id: String,
    tenant_id: String,
    operation_kind: String,
    actor_scope: OperationActorScope,
    #[serde(default)]
    idempotency_key_hash: Option<String>,
    status: OperationStatus,
    indexing_state: OperationIndexingState,
    progress: OperationProgress,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    #[serde(default)]
    completed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    last_error_category: Option<String>,
    #[serde(default)]
    last_error_fingerprint: Option<String>,
}

impl OperationSummaryDocument {
    fn into_summary(self) -> OperationSummary {
        OperationSummary {
            id: self.id,
            tenant_id: self.tenant_id,
            operation_kind: self.operation_kind,
            actor_scope: self.actor_scope,
            idempotency_key_hash: self.idempotency_key_hash,
            status: self.status,
            indexing_state: self.indexing_state,
            attempt_count: self.progress.attempt_count,
            pending_steps: self
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
            failed_steps: self
                .progress
                .steps
                .values()
                .filter(|progress| progress.status == OperationStepStatus::Failed)
                .count(),
            created_at: self.created_at,
            updated_at: self.updated_at,
            completed_at: self.completed_at,
            last_error_category: self.last_error_category,
            last_error_fingerprint: self.last_error_fingerprint,
        }
    }
}

fn operation_document_logical_id<'a>(
    tenant_id: &str,
    document: &'a Value,
) -> Result<&'a str, ApiError> {
    if !is_tenant_document("rag_operations", document) {
        return Err(ApiError::Internal(
            "operation journal returned an invalid tenant-scoped document".to_string(),
        ));
    }
    if document.get("tenant_id").and_then(Value::as_str) != Some(tenant_id) {
        return Err(ApiError::Internal(
            "operation journal returned a cross-tenant document".to_string(),
        ));
    }
    document
        .get("logical_id")
        .and_then(Value::as_str)
        .filter(|operation_id| !operation_id.trim().is_empty())
        .ok_or_else(|| {
            ApiError::Internal(
                "operation journal returned a document without a logical id".to_string(),
            )
        })
}

fn decode_operation_page_item(
    tenant_id: &str,
    document: Value,
    include_plan: bool,
) -> Result<OperationListItem, ApiError> {
    operation_document_logical_id(tenant_id, &document)?;
    let document = restore_logical_id("rag_operations", document);
    if include_plan {
        let record = serde_json::from_value::<OperationRecord>(document).map_err(|error| {
            ApiError::Internal(format!(
                "failed to decode tenant-scoped rag_operations document: {error}"
            ))
        })?;
        if record.tenant_id != tenant_id {
            return Err(ApiError::Internal(
                "operation journal returned a cross-tenant record".to_string(),
            ));
        }
        validate_operation_record(&record).map_err(|error| {
            ApiError::Internal(format!(
                "operation journal returned an invalid record: {error}"
            ))
        })?;
        return Ok(operation_list_item(&record, true));
    }

    let summary =
        serde_json::from_value::<OperationSummaryDocument>(document).map_err(|error| {
            ApiError::Internal(format!(
                "failed to decode tenant-scoped rag_operations summary: {error}"
            ))
        })?;
    if summary.tenant_id != tenant_id
        || summary.id.trim().is_empty()
        || summary.operation_kind.trim().is_empty()
    {
        return Err(ApiError::Internal(
            "operation journal returned an invalid summary record".to_string(),
        ));
    }
    Ok(OperationListItem {
        summary: summary.into_summary(),
        plan: None,
    })
}

fn validate_operation_candidate_limit(limit: usize) -> Result<(), ApiError> {
    if !(1..=MAX_OPERATION_CANDIDATE_LIMIT).contains(&limit) {
        return Err(ApiError::bad_request(format!(
            "limit must be between 1 and {MAX_OPERATION_CANDIDATE_LIMIT}"
        )));
    }
    Ok(())
}

fn normalize_operation_candidate_ids(operation_ids: &[String]) -> Result<Vec<String>, ApiError> {
    if operation_ids.len() > MAX_OPERATION_CANDIDATE_LIMIT {
        return Err(ApiError::bad_request(format!(
            "operation_ids must contain at most {MAX_OPERATION_CANDIDATE_LIMIT} values"
        )));
    }
    let mut seen = HashSet::with_capacity(operation_ids.len());
    let mut normalized = Vec::with_capacity(operation_ids.len());
    for operation_id in operation_ids {
        if operation_id.trim().is_empty() {
            return Err(ApiError::bad_request(
                "operation_ids must not contain empty values",
            ));
        }
        if seen.insert(operation_id.clone()) {
            normalized.push(operation_id.clone());
        }
    }
    Ok(normalized)
}

fn operation_candidate_document_logical_id<'a>(
    tenant_id: &str,
    document: &'a Value,
) -> Result<&'a str, ApiError> {
    if document.get("tenant_id").and_then(Value::as_str) != Some(tenant_id) {
        return Err(ApiError::Internal(
            "operation candidate query returned a cross-tenant document".to_string(),
        ));
    }
    match document.get("logical_id") {
        Some(Value::String(logical_id)) => {
            if logical_id.trim().is_empty() || !is_tenant_document("rag_operations", document) {
                return Err(ApiError::Internal(
                    "operation candidate query returned an invalid tenant-scoped document"
                        .to_string(),
                ));
            }
            Ok(logical_id)
        }
        Some(Value::Null) | None => document
            .get("id")
            .and_then(Value::as_str)
            .filter(|operation_id| !operation_id.trim().is_empty())
            .ok_or_else(|| {
                ApiError::Internal(
                    "operation candidate query returned a legacy document without an id"
                        .to_string(),
                )
            }),
        Some(_) => Err(ApiError::Internal(
            "operation candidate query returned an invalid logical id".to_string(),
        )),
    }
}

fn decode_operation_candidate_documents(
    tenant_id: &str,
    documents: Vec<Value>,
    statuses: &[OperationStatus],
    expected_ids: Option<&HashSet<String>>,
    reconcilable_only: bool,
    limit: usize,
) -> Result<Vec<OperationRecord>, ApiError> {
    if documents.len() > limit {
        return Err(ApiError::Upstream(
            "Meilisearch rag_operations candidate query returned more documents than requested"
                .to_string(),
        ));
    }

    let mut seen_ids = HashSet::with_capacity(documents.len());
    let mut records = Vec::with_capacity(documents.len());
    for document in prefer_tenant_documents("rag_operations", documents) {
        let logical_id = operation_candidate_document_logical_id(tenant_id, &document)?.to_string();
        let document = restore_logical_id("rag_operations", document);
        let record = serde_json::from_value::<OperationRecord>(document).map_err(|error| {
            ApiError::Internal(format!(
                "failed to decode tenant-scoped rag_operations candidate: {error}"
            ))
        })?;
        if record.tenant_id != tenant_id || record.id != logical_id {
            return Err(ApiError::Internal(
                "operation candidate query returned a record with invalid tenant or logical identity"
                    .to_string(),
            ));
        }
        validate_operation_record(&record).map_err(|error| {
            ApiError::Internal(format!(
                "operation candidate query returned an invalid record: {error}"
            ))
        })?;
        if expected_ids.is_some_and(|operation_ids| !operation_ids.contains(&record.id)) {
            return Err(ApiError::Internal(
                "operation candidate query returned an unrequested operation".to_string(),
            ));
        }
        if !statuses.is_empty() && !statuses.contains(&record.status) {
            return Err(ApiError::Internal(
                "operation candidate query returned an operation with an unrequested status"
                    .to_string(),
            ));
        }
        if reconcilable_only
            && record.status == OperationStatus::Completed
            && record.indexing_state == OperationIndexingState::Completed
        {
            return Err(ApiError::Internal(
                "operation candidate query returned a fully reconciled operation".to_string(),
            ));
        }
        if !seen_ids.insert(record.id.clone()) {
            return Err(ApiError::Internal(
                "operation candidate query returned a duplicate logical id".to_string(),
            ));
        }
        records.push(record);
    }
    records.sort_by(|left, right| {
        left.created_at
            .cmp(&right.created_at)
            .then_with(|| left.id.cmp(&right.id))
    });
    Ok(records)
}

impl MeiliRepository {
    pub fn new(admin: MeiliAdmin, wait_for_tasks: bool) -> Self {
        Self::new_with_admins_and_scan_limits(
            admin.clone(),
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
        Self::new_with_admins_and_scan_limits(
            admin.clone(),
            admin,
            wait_for_tasks,
            scan_page_size,
            scan_max_documents,
        )
    }

    pub(crate) fn new_with_admins_and_scan_limits(
        admin: MeiliAdmin,
        index_admin: MeiliAdmin,
        wait_for_tasks: bool,
        scan_page_size: usize,
        scan_max_documents: usize,
    ) -> Self {
        Self {
            admin,
            index_admin,
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
        let durable_fixed_index = matches!(index_uid, "rag_operations" | "rag_audit_records");
        if durable_fixed_index {
            self.admin.verify_durable_index_for_write(index_uid).await?;
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
        let accepted_task_uid = task_uid.as_deref().ok_or_else(|| {
            ApiError::Upstream(format!(
                "Meilisearch accepted a non-empty {index_uid} document mutation without a task UID"
            ))
        })?;
        if durable_fixed_index {
            self.admin.wait_for_task(accepted_task_uid).await?;
            self.admin.verify_durable_index_for_write(index_uid).await?;
        } else {
            self.maybe_wait(&task_uid).await?;
        }
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
            .index_admin
            .ensure_index(&index.event_index_uid, "id", true)
            .await?;
        task_uids.extend(
            self.index_admin
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
            .index_admin
            .reconcile_existing_index(&index.event_index_uid, true)
            .await?;
        task_uids.extend(
            self.index_admin
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

    async fn list_operations_by_ids(
        &self,
        tenant_id: &str,
        operation_ids: &[String],
        statuses: &[OperationStatus],
        limit: usize,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        validate_operation_candidate_limit(limit)?;
        let operation_ids = normalize_operation_candidate_ids(operation_ids)?;
        if operation_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }

        let expected_ids = operation_ids.iter().cloned().collect::<HashSet<_>>();
        let mut filter = TenantFilter::new(tenant_id)?.logical_ids(&operation_ids)?;
        if !statuses.is_empty() {
            let statuses = statuses
                .iter()
                .map(|status| status.as_str().to_string())
                .collect::<Vec<_>>();
            filter = filter.in_strings("status", &statuses)?;
        }
        let response: SearchResponse<Value> = self
            .admin
            .search(
                "rag_operations",
                json!({
                    "q": "",
                    "limit": limit,
                    "filter": filter.finish(),
                    "sort": ["created_at:asc", "id:asc"]
                }),
            )
            .await?;
        Ok(Some(decode_operation_candidate_documents(
            tenant_id,
            response.hits,
            statuses,
            Some(&expected_ids),
            false,
            limit,
        )?))
    }

    async fn append_event(&self, event: &HistoryEvent) -> Result<Option<String>, ApiError> {
        let task_uid = self
            .upsert_values(&event.event_index_uid, &[to_document(event, &event.id)?])
            .await?;
        Ok(task_uid)
    }

    async fn append_events(
        &self,
        events: &[HistoryEvent],
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        let Some(first) = events.first() else {
            return Ok(RepositoryWriteReceipt::empty());
        };
        if events.iter().any(|event| {
            event.tenant_id != first.tenant_id
                || event.owner_user_id_hash != first.owner_user_id_hash
                || event.event_index_uid != first.event_index_uid
        }) {
            return Err(ApiError::Internal(
                "history-event batch crosses tenant, owner, or event-index scope".to_string(),
            ));
        }
        let documents = events
            .iter()
            .map(|event| to_document(event, &event.id))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(RepositoryWriteReceipt::from_task_uid(
            self.upsert_values(&first.event_index_uid, &documents)
                .await?,
        ))
    }

    async fn upsert_operation(
        &self,
        operation: &OperationRecord,
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        validate_operation_record(operation).map_err(|error| {
            ApiError::Internal(format!("invalid operation journal record: {error}"))
        })?;
        let document = tenant_document(
            &operation.tenant_id,
            "rag_operations",
            &operation.id,
            operation,
        )?;
        Ok(RepositoryWriteReceipt::from_task_uid(
            self.upsert_values("rag_operations", &[document]).await?,
        ))
    }

    async fn upsert_audit_record(
        &self,
        record: &AuditRecord,
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        record
            .validate()
            .map_err(|error| ApiError::Internal(format!("invalid audit record: {error}")))?;
        let document = tenant_document(&record.tenant_id, "rag_audit_records", &record.id, record)?;
        Ok(RepositoryWriteReceipt::from_task_uid(
            self.upsert_values("rag_audit_records", &[document]).await?,
        ))
    }

    async fn get_operation(
        &self,
        tenant_id: &str,
        operation_id: &str,
    ) -> Result<Option<OperationRecord>, ApiError> {
        self.search_tenant_one(
            "rag_operations",
            TenantFilter::new(tenant_id)?.logical_id(operation_id)?,
        )
        .await
    }

    async fn list_operations(
        &self,
        tenant_id: &str,
        statuses: &[OperationStatus],
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        let mut filter = TenantFilter::new(tenant_id)?;
        if !statuses.is_empty() {
            let statuses = statuses
                .iter()
                .map(|status| status.as_str().to_string())
                .collect::<Vec<_>>();
            filter = filter.in_strings("status", &statuses)?;
        }
        self.search_tenant_many(
            "rag_operations",
            filter,
            Some(&["created_at:asc", "updated_at:asc"]),
        )
        .await
    }

    async fn list_operation_page(
        &self,
        query: RepositoryOperationListQuery<'_>,
    ) -> Result<Option<RepositoryOperationPage>, ApiError> {
        if query.limit == 0 || query.limit > 500 {
            return Err(ApiError::bad_request("limit must be between 1 and 500"));
        }
        match (query.offset, query.previous_operation_id) {
            (0, None) => {}
            (0, Some(_)) | (_, None) => {
                return Err(ApiError::bad_request("cursor is invalid or stale"));
            }
            (_, Some(operation_id)) if operation_id.trim().is_empty() => {
                return Err(ApiError::bad_request("cursor is invalid or stale"));
            }
            _ => {}
        }

        let mut filter = TenantFilter::new(query.tenant_id)?;
        if !query.statuses.is_empty() {
            let statuses = query
                .statuses
                .iter()
                .map(|status| status.as_str().to_string())
                .collect::<Vec<_>>();
            filter = filter.in_strings("status", &statuses)?;
        }
        if !query.operation_kinds.is_empty() {
            filter = filter.in_strings("operation_kind", query.operation_kinds)?;
        }
        let filter = filter.finish();
        let sort = vec!["created_at:desc".to_string(), "id:desc".to_string()];

        if let Some(previous_operation_id) = query.previous_operation_id {
            let previous_offset = query
                .offset
                .checked_sub(1)
                .ok_or_else(|| ApiError::bad_request("cursor is invalid or stale"))?;
            let previous = self
                .admin
                .fetch_projected_documents_page(
                    "rag_operations",
                    &filter,
                    &sort,
                    previous_offset,
                    1,
                    OPERATION_IDENTITY_FIELDS,
                )
                .await?;
            if previous.offset != previous_offset
                || previous.limit != 1
                || previous.results.len() != 1
            {
                return Err(ApiError::bad_request("cursor is invalid or stale"));
            }
            let actual_operation_id =
                operation_document_logical_id(query.tenant_id, &previous.results[0])?;
            if actual_operation_id != previous_operation_id {
                return Err(ApiError::bad_request("cursor is invalid or stale"));
            }
        }

        let fields = if query.include_plan {
            OPERATION_WITH_PLAN_FIELDS
        } else {
            OPERATION_SUMMARY_FIELDS
        };
        let page = self
            .admin
            .fetch_projected_documents_page(
                "rag_operations",
                &filter,
                &sort,
                query.offset,
                query.limit,
                fields,
            )
            .await?;
        if page.offset != query.offset || page.limit != query.limit {
            return Err(ApiError::Upstream(format!(
                "Meilisearch rag_operations page returned offset {} and limit {} while offset {} and limit {} were requested",
                page.offset, page.limit, query.offset, query.limit
            )));
        }
        if page.results.len() > query.limit {
            return Err(ApiError::Upstream(
                "Meilisearch rag_operations page returned more documents than requested"
                    .to_string(),
            ));
        }
        let end = query
            .offset
            .checked_add(page.results.len())
            .ok_or_else(|| ApiError::bad_request("cursor is invalid or stale"))?;
        if end > page.total || (page.results.is_empty() && query.offset < page.total) {
            return Err(ApiError::Upstream(
                "Meilisearch rag_operations page returned inconsistent pagination metadata"
                    .to_string(),
            ));
        }
        let operations = page
            .results
            .into_iter()
            .map(|document| {
                decode_operation_page_item(query.tenant_id, document, query.include_plan)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Some(RepositoryOperationPage {
            operations,
            has_more: end < page.total,
        }))
    }

    async fn list_reconcilable_operations(
        &self,
        tenant_id: &str,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        let filter = TenantFilter::new(tenant_id)?.any_not_eq(&[
            ("status", OperationStatus::Completed.as_str()),
            ("indexing_state", OperationIndexingState::Completed.as_str()),
        ])?;
        self.search_tenant_many(
            "rag_operations",
            filter,
            Some(&["created_at:asc", "updated_at:asc"]),
        )
        .await
    }

    async fn list_oldest_reconcilable_operations(
        &self,
        tenant_id: &str,
        statuses: &[OperationStatus],
        limit: usize,
    ) -> Result<Option<Vec<OperationRecord>>, ApiError> {
        validate_operation_candidate_limit(limit)?;
        let mut filter = TenantFilter::new(tenant_id)?.any_not_eq(&[
            ("status", OperationStatus::Completed.as_str()),
            ("indexing_state", OperationIndexingState::Completed.as_str()),
        ])?;
        if !statuses.is_empty() {
            let statuses = statuses
                .iter()
                .map(|status| status.as_str().to_string())
                .collect::<Vec<_>>();
            filter = filter.in_strings("status", &statuses)?;
        }
        let filter = filter.finish();
        let sort = vec!["created_at:asc".to_string(), "id:asc".to_string()];
        let page = self
            .admin
            .fetch_filtered_documents_page("rag_operations", &filter, &sort, 0, limit)
            .await?;
        if page.offset != 0 || page.limit != limit {
            return Err(ApiError::Upstream(format!(
                "Meilisearch rag_operations candidate page returned offset {} and limit {} while offset 0 and limit {limit} were requested",
                page.offset, page.limit
            )));
        }
        if page.results.len() > limit {
            return Err(ApiError::Upstream(
                "Meilisearch rag_operations candidate page returned more documents than requested"
                    .to_string(),
            ));
        }
        if page.results.len() > page.total || (page.results.is_empty() && page.total > 0) {
            return Err(ApiError::Upstream(
                "Meilisearch rag_operations candidate page returned inconsistent pagination metadata"
                    .to_string(),
            ));
        }
        Ok(Some(decode_operation_candidate_documents(
            tenant_id,
            page.results,
            statuses,
            None,
            true,
            limit,
        )?))
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
        source_document_uris: &[String],
        link_ids: &[String],
    ) -> Result<DeleteSourceReport, ApiError> {
        let mut report = DeleteSourceReport::default();
        let mut targets = vec![
            CompanySourceDeleteTarget::Fragments,
            CompanySourceDeleteTarget::Revisions,
            CompanySourceDeleteTarget::Source,
            CompanySourceDeleteTarget::SourceDocuments,
            CompanySourceDeleteTarget::ParseArtifacts,
            CompanySourceDeleteTarget::IngestTasks,
            CompanySourceDeleteTarget::IngestResults,
        ];
        if !link_ids.is_empty() || !source_document_uris.is_empty() {
            targets.push(CompanySourceDeleteTarget::Links {
                link_ids: link_ids.to_vec(),
                related_uris: source_document_uris.to_vec(),
            });
        }
        for target in targets {
            let task = self
                .delete_company_source_index(tenant_id, source_id, &target)
                .await?;
            match target {
                CompanySourceDeleteTarget::Fragments => report.fragments_task = task,
                CompanySourceDeleteTarget::Revisions => report.revisions_task = task,
                CompanySourceDeleteTarget::Source => report.source_task = task,
                CompanySourceDeleteTarget::SourceDocuments
                | CompanySourceDeleteTarget::ParseArtifacts
                | CompanySourceDeleteTarget::IngestTasks
                | CompanySourceDeleteTarget::IngestResults
                | CompanySourceDeleteTarget::Links { .. } => {
                    if let Some(task) = task {
                        report.auxiliary_tasks.push(task);
                    }
                }
            }
        }
        Ok(report)
    }

    async fn delete_company_source_index(
        &self,
        tenant_id: &str,
        source_id: &str,
        target: &CompanySourceDeleteTarget,
    ) -> Result<Option<String>, ApiError> {
        if matches!(
            target,
            CompanySourceDeleteTarget::Links {
                link_ids,
                related_uris,
            } if link_ids.is_empty() && related_uris.is_empty()
        ) {
            return Ok(None);
        }
        let (index_uid, filter) = match target {
            CompanySourceDeleteTarget::Fragments => (
                "rag_company_context",
                TenantFilter::new(tenant_id)?
                    .eq("source_id", source_id)?
                    .finish(),
            ),
            CompanySourceDeleteTarget::Revisions => (
                "rag_source_revisions",
                TenantFilter::new(tenant_id)?
                    .eq("source_id", source_id)?
                    .finish(),
            ),
            CompanySourceDeleteTarget::Source => (
                "rag_sources",
                TenantFilter::new(tenant_id)?
                    .logical_id(source_id)?
                    .finish(),
            ),
            CompanySourceDeleteTarget::SourceDocuments => (
                "rag_source_documents",
                TenantFilter::new(tenant_id)?
                    .eq("source_id", source_id)?
                    .is_null("owner_user_id")
                    .finish(),
            ),
            CompanySourceDeleteTarget::ParseArtifacts => (
                "rag_parse_artifacts",
                TenantFilter::new(tenant_id)?
                    .eq("source_id", source_id)?
                    .is_null("owner_user_id")
                    .finish(),
            ),
            CompanySourceDeleteTarget::IngestTasks => (
                "rag_ingest_tasks",
                TenantFilter::new(tenant_id)?
                    .eq("source_id", source_id)?
                    .is_null("owner_user_id")
                    .finish(),
            ),
            CompanySourceDeleteTarget::IngestResults => (
                "rag_ingest_results",
                TenantFilter::new(tenant_id)?
                    .eq("source_id", source_id)?
                    .is_null("owner_user_id")
                    .finish(),
            ),
            CompanySourceDeleteTarget::Links {
                link_ids,
                related_uris,
            } => {
                let mut conditions = Vec::<(&'static str, &[String])>::new();
                if !link_ids.is_empty() {
                    conditions.push(("logical_id", link_ids));
                    conditions.push(("id", link_ids));
                }
                if !related_uris.is_empty() {
                    conditions.push(("source_uri", related_uris));
                    conditions.push(("target_uri", related_uris));
                }
                (
                    "rag_links",
                    TenantFilter::new(tenant_id)?
                        .any_in_strings(&conditions)?
                        .finish(),
                )
            }
        };
        let task = self
            .admin
            .delete_documents_by_filter(index_uid, &filter)
            .await?;
        if task.is_none() && self.admin.configured() {
            return Err(ApiError::Upstream(format!(
                "Meilisearch did not return a task UID for required company-source deletion index {index_uid}"
            )));
        }
        self.maybe_wait(&task).await?;
        Ok(task)
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
    ) -> Result<RepositoryWriteReceipt, ApiError> {
        if task_ids.is_empty() {
            return Ok(RepositoryWriteReceipt::empty());
        }
        let mut receipt = RepositoryWriteReceipt::empty();
        for index_uid in ["rag_ingest_tasks", "rag_ingest_results"] {
            let filter = TenantFilter::new(tenant_id)?
                .in_strings("task_id", task_ids)?
                .finish();
            let task_uid = self
                .admin
                .delete_documents_by_filter(index_uid, &filter)
                .await?;
            self.maybe_wait(&task_uid).await?;
            if let Some(task_uid) = task_uid {
                receipt.task_uids.push(task_uid);
            }
        }
        let mut ordered = RepositoryWriteReceipt::empty();
        ordered.extend(receipt);
        Ok(ordered)
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
                    "sort": ["occurred_at:desc", "id:desc"]
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

    async fn list_company_source_documents(
        &self,
        tenant_id: &str,
        source_id: &str,
    ) -> Result<Option<Vec<SourceDocument>>, ApiError> {
        self.search_tenant_many(
            "rag_source_documents",
            TenantFilter::new(tenant_id)?
                .eq("source_id", source_id)?
                .is_null("owner_user_id"),
            Some(&["updated_at:asc"]),
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
        let (runtime, index_admin) = MeiliAdmin::pair_from_config(config);
        Arc::new(MeiliRepository::new_with_admins_and_scan_limits(
            runtime,
            index_admin,
            config.meili_wait_for_tasks,
            config.meili_scan_page_size,
            config.meili_scan_max_documents,
        ))
    } else {
        Arc::new(MemoryRepository)
    }
}

pub(crate) fn repository_from_meili_admins(
    config: &Config,
    runtime: MeiliAdmin,
    index_admin: MeiliAdmin,
) -> Arc<dyn KnowledgeRepository> {
    if config.store_backend != "meili" || config.meili_url.is_none() {
        return Arc::new(MemoryRepository);
    }
    Arc::new(MeiliRepository::new_with_admins_and_scan_limits(
        runtime,
        index_admin,
        config.meili_wait_for_tasks,
        config.meili_scan_page_size,
        config.meili_scan_max_documents,
    ))
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
    use std::sync::Mutex;

    use axum::{
        body::to_bytes,
        extract::{Request as AxumRequest, State},
        http::{Method, StatusCode},
        response::{IntoResponse, Response},
        Json, Router,
    };

    use crate::{
        config::Config,
        mutation::{
            operation_record_from_plan, operation_step_completed, OPERATION_PLAN_SCHEMA_VERSION,
        },
    };

    #[derive(Clone)]
    struct OperationQueryStub {
        response: Value,
        requests: Arc<Mutex<Vec<(Method, String, Value)>>>,
    }

    async fn operation_query_stub(
        State(stub): State<OperationQueryStub>,
        request: AxumRequest,
    ) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_string();
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let body = serde_json::from_slice(&bytes).unwrap();
        stub.requests.lock().unwrap().push((method, path, body));
        (StatusCode::OK, Json(stub.response)).into_response()
    }

    async fn spawn_operation_query_repository(
        response: Value,
    ) -> (MeiliRepository, Arc<Mutex<Vec<(Method, String, Value)>>>) {
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stub = OperationQueryStub {
            response,
            requests: Arc::clone(&requests),
        };
        let app = Router::new()
            .fallback(operation_query_stub)
            .with_state(stub);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut config = Config::test();
        config.meili_url = Some(format!("http://{address}"));
        (
            MeiliRepository::new(MeiliAdmin::from_config(&config), false),
            requests,
        )
    }

    fn operation_candidate_record(
        tenant_id: &str,
        operation_id: &str,
        created_at: &str,
    ) -> OperationRecord {
        let created_at = DateTime::parse_from_rfc3339(created_at)
            .unwrap()
            .with_timezone(&Utc);
        operation_record_from_plan(OperationPlan {
            schema_version: OPERATION_PLAN_SCHEMA_VERSION,
            id: operation_id.to_string(),
            tenant_id: tenant_id.to_string(),
            operation_kind: "state_upsert".to_string(),
            actor: OperationActor {
                scope: OperationActorScope::TenantService,
                owner_user_id_hash: None,
                roles: Vec::new(),
                request_id: None,
            },
            idempotency_key_hash: None,
            primary: OperationStep {
                id: "primary".to_string(),
                role: OperationStepRole::Primary,
                resource: OperationResource::StructuredSummary {
                    summary: json!({"id": operation_id}),
                },
            },
            side_effects: Vec::new(),
            redacted_metadata: json!({}),
            response_snapshot: Value::Null,
            created_at,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn operation_id_batch_uses_one_scoped_search_and_restores_legacy_ids() {
        let newer = operation_candidate_record("tenant-a", "operation-2", "2026-07-14T02:00:00Z");
        let older = operation_candidate_record("tenant-a", "operation-1", "2026-07-14T01:00:00Z");
        let newer = tenant_document("tenant-a", "rag_operations", "operation-2", &newer).unwrap();
        let older = serde_json::to_value(older).unwrap();
        let (repository, requests) = spawn_operation_query_repository(json!({
            "hits": [newer, older]
        }))
        .await;

        let operations = repository
            .list_operations_by_ids(
                "tenant-a",
                &["operation-2".to_string(), "operation-1".to_string()],
                &[OperationStatus::Pending],
                2,
            )
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            operations
                .iter()
                .map(|operation| operation.id.as_str())
                .collect::<Vec<_>>(),
            vec!["operation-1", "operation-2"]
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "batch query must use one Meili call");
        assert_eq!(requests[0].0, Method::POST);
        assert_eq!(requests[0].1, "/indexes/rag_operations/search");
        assert_eq!(requests[0].2["limit"], 2);
        assert_eq!(requests[0].2["sort"], json!(["created_at:asc", "id:asc"]));
        let filter = requests[0].2["filter"].as_str().unwrap();
        assert!(filter.contains("tenant_id = \"tenant-a\""), "{filter}");
        assert!(filter.contains("logical_id IN"), "{filter}");
        assert!(filter.contains("id IN"), "{filter}");
        assert!(filter.contains("status IN [\"pending\"]"), "{filter}");
    }

    #[tokio::test]
    async fn oldest_reconcilable_query_uses_one_bounded_document_page() {
        let newer = operation_candidate_record("tenant-a", "operation-2", "2026-07-14T02:00:00Z");
        let older = operation_candidate_record("tenant-a", "operation-1", "2026-07-14T01:00:00Z");
        let newer = tenant_document("tenant-a", "rag_operations", "operation-2", &newer).unwrap();
        let older = serde_json::to_value(older).unwrap();
        let (repository, requests) = spawn_operation_query_repository(json!({
            "results": [newer, older],
            "offset": 0,
            "limit": 2,
            "total": 7
        }))
        .await;

        let operations = repository
            .list_oldest_reconcilable_operations("tenant-a", &[OperationStatus::Pending], 2)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            operations
                .iter()
                .map(|operation| operation.id.as_str())
                .collect::<Vec<_>>(),
            vec!["operation-1", "operation-2"]
        );
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 1, "bounded query must use one Meili call");
        assert_eq!(requests[0].0, Method::POST);
        assert_eq!(requests[0].1, "/indexes/rag_operations/documents/fetch");
        assert_eq!(requests[0].2["offset"], 0);
        assert_eq!(requests[0].2["limit"], 2);
        assert_eq!(requests[0].2["sort"], json!(["created_at:asc", "id:asc"]));
        let filter = requests[0].2["filter"].as_str().unwrap();
        assert!(filter.contains("tenant_id = \"tenant-a\""), "{filter}");
        assert!(filter.contains("status != \"completed\""), "{filter}");
        assert!(
            filter.contains("indexing_state != \"completed\""),
            "{filter}"
        );
        assert!(filter.contains("status IN [\"pending\"]"), "{filter}");
    }

    #[tokio::test]
    async fn memory_operation_candidate_queries_are_unsupported() {
        let repository = MemoryRepository;
        assert!(repository
            .list_operations_by_ids("tenant-a", &["operation-1".to_string()], &[], 1)
            .await
            .unwrap()
            .is_none());
        assert!(repository
            .list_oldest_reconcilable_operations("tenant-a", &[], 1)
            .await
            .unwrap()
            .is_none());
    }

    #[test]
    fn operation_candidate_inputs_are_hard_bounded() {
        assert!(matches!(
            validate_operation_candidate_limit(0),
            Err(ApiError::BadRequest(_))
        ));
        assert!(validate_operation_candidate_limit(MAX_OPERATION_CANDIDATE_LIMIT).is_ok());
        assert!(matches!(
            validate_operation_candidate_limit(MAX_OPERATION_CANDIDATE_LIMIT + 1),
            Err(ApiError::BadRequest(_))
        ));
        let too_many_ids = (0..=MAX_OPERATION_CANDIDATE_LIMIT)
            .map(|index| format!("operation-{index}"))
            .collect::<Vec<_>>();
        assert!(matches!(
            normalize_operation_candidate_ids(&too_many_ids),
            Err(ApiError::BadRequest(_))
        ));
    }

    #[test]
    fn operation_candidate_decoder_enforces_returned_scope_and_state() {
        let pending = operation_candidate_record("tenant-a", "operation-1", "2026-07-14T01:00:00Z");
        let expected_ids = HashSet::from(["operation-1".to_string()]);

        assert!(decode_operation_candidate_documents(
            "tenant-b",
            vec![serde_json::to_value(&pending).unwrap()],
            &[],
            Some(&expected_ids),
            false,
            1,
        )
        .is_err());
        assert!(decode_operation_candidate_documents(
            "tenant-a",
            vec![serde_json::to_value(&pending).unwrap()],
            &[OperationStatus::Failed],
            Some(&expected_ids),
            false,
            1,
        )
        .is_err());

        let completed = operation_step_completed(
            &pending,
            "primary",
            pending.created_at + chrono::Duration::seconds(1),
        )
        .unwrap();
        assert!(decode_operation_candidate_documents(
            "tenant-a",
            vec![serde_json::to_value(completed).unwrap()],
            &[],
            Some(&expected_ids),
            true,
            1,
        )
        .is_err());
    }

    #[tokio::test]
    async fn generic_operation_step_application_is_typed_and_memory_deterministic() {
        let repository = MemoryRepository;
        let step = OperationStep {
            id: "summary".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::StructuredSummary {
                summary: json!({"id": "summary-1"}),
            },
        };
        let receipt = repository
            .apply_operation_step("tenant-a", &step)
            .await
            .unwrap();
        assert_eq!(receipt, RepositoryWriteReceipt::empty());
    }

    #[tokio::test]
    async fn generic_operation_step_rejects_cross_tenant_resources() {
        let repository = MemoryRepository;
        let step = OperationStep {
            id: "dataset".to_string(),
            role: OperationStepRole::Primary,
            resource: OperationResource::Dataset {
                dataset: DatasetRecord {
                    id: "dataset-1".to_string(),
                    tenant_id: "tenant-b".to_string(),
                    dataset_key: "daily".to_string(),
                    title: "Daily".to_string(),
                    schema_version: 1,
                    status: "active".to_string(),
                    columns: Vec::new(),
                },
            },
        };
        assert!(repository
            .apply_operation_step("tenant-a", &step)
            .await
            .is_err());
    }

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
