use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use serde_json::{json, Value};

use crate::{
    config::Config,
    error::ApiError,
    models::*,
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
    preflight_decisions: HashMap<String, CompanyDocPreflightResponse>,
    datasets: HashMap<String, DatasetRecord>,
    snapshots: HashMap<String, StructuredSnapshot>,
    snapshot_idempotency: HashMap<String, String>,
    rows_by_snapshot: HashMap<String, Vec<Value>>,
    row_idempotency: HashSet<(String, String)>,
    structured_summaries: HashMap<String, Value>,
    sessions: HashMap<String, SessionRecord>,
    traces: HashMap<String, TraceRecord>,
}

#[derive(Debug, Clone)]
pub struct ContextSearchOutcome {
    pub response: ContextSearchResponse,
    pub trace: TraceRecord,
    pub nodes: Vec<ContextNode>,
}

impl Store {
    pub fn new(config: &Config) -> Self {
        Self {
            inner: Arc::new(RwLock::new(StoreData::default())),
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
        if let Some(history_event_id) = &response.history_event_id {
            let _ = self.persist_history_event_by_id(history_event_id).await?;
        }
        Ok(response)
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
                .map(|node| ContextHit {
                    uri: node.uri.clone(),
                    title: node.title.clone(),
                    layer: node.layer,
                    score: text_score(&format!("{} {}", node.title, node.body), &query),
                    source_id: node.source_id.clone(),
                    revision_id: node.revision_id.clone(),
                    snippet: truncate_chars(&node.body, 240),
                })
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
            let item = if let Some(mut item) = existing {
                item.title = title;
                item.statement = statement;
                item.value = req.value;
                item.confidence = req.confidence;
                item.salience = req.salience;
                item.valid_from = req.valid_from;
                item.valid_to = req.valid_to;
                item.source_refs = req.source_refs.clone();
                item.status = "active".to_string();
                item.current_version += 1;
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
                    value: req.value,
                    status: "active".to_string(),
                    confidence: req.confidence,
                    salience: req.salience,
                    valid_from: req.valid_from,
                    valid_to: req.valid_to,
                    source_refs: req.source_refs.clone(),
                    context_uri: context_uri.clone(),
                    current_version: 1,
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
            let routing = self
                .resolver
                .resolve(tenant_id, &owner_user_id, false, true)?;
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

        for node in &mut data.company_context {
            if node.source_id.as_deref() == Some(source_id) {
                node.status = "superseded".to_string();
            }
        }
        let context_uris =
            self.write_company_revision_context_locked(&mut data, tenant_id, &revision);

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
            context_uris,
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
                    source_id: None,
                    revision_id: None,
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
        self.context_scope_for_acl_locked(&data, tenant_id, owner_user_id, include_all_private)?
            .into_iter()
            .find(|node| node.uri == uri && node.status == "active")
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

        let mut l0 = rank_nodes(
            nodes
                .iter()
                .filter(|node| node.layer == 0 && node.status == "active")
                .cloned(),
            &query,
            limit,
        );
        if l0.is_empty() {
            l0 = rank_nodes(
                nodes.iter().filter(|node| node.status == "active").cloned(),
                &query,
                limit,
            );
        }
        let bases: HashSet<String> = l0
            .iter()
            .map(|(node, _)| strip_layer_suffix(&node.uri))
            .collect();
        let l1 = rank_nodes(
            nodes
                .iter()
                .filter(|node| node.layer == 1 && bases.contains(&strip_layer_suffix(&node.uri)))
                .cloned(),
            &query,
            limit,
        );
        let l2 = rank_nodes(
            nodes
                .iter()
                .filter(|node| node.layer == 2 && bases.contains(&strip_layer_suffix(&node.uri)))
                .cloned(),
            &query,
            limit,
        );
        drop(data);

        let selected_nodes: Vec<_> = l0
            .iter()
            .chain(l1.iter())
            .chain(l2.iter())
            .map(|(node, _)| node.clone())
            .take(limit)
            .collect();
        let hits: Vec<_> = selected_nodes
            .iter()
            .map(|node| ContextHit {
                uri: node.uri.clone(),
                title: node.title.clone(),
                layer: node.layer,
                score: text_score(&format!("{} {}", node.title, node.body), &query),
                source_id: node.source_id.clone(),
                revision_id: node.revision_id.clone(),
                snippet: truncate_chars(&node.body, 240),
            })
            .collect();
        let stages = vec![
            stage_value("L0", &l0, owner_user_id.as_deref()),
            stage_value("L1", &l1, owner_user_id.as_deref()),
            stage_value("L2", &l2, owner_user_id.as_deref()),
        ];
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
                    source_id: None,
                    revision_id: None,
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
                "stages": ["L0", "L1", "L2"]
            }),
        }
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
    ) -> Vec<String> {
        let base = format!(
            "ctx://company/docs/{}/{}",
            sanitize_slug(&revision.source_id),
            sanitize_slug(&revision.title)
        );
        let nodes = vec![
            self.context_node(
                &format!("{base}/.abstract"),
                &revision.title,
                0,
                &truncate_chars(&revision.content, 500),
                "company",
                "rag_company_context",
                tenant_id,
                None,
                Some(revision.source_id.clone()),
                Some(revision.id.clone()),
            ),
            self.context_node(
                &format!("{base}/.overview"),
                &revision.title,
                1,
                &truncate_chars(&revision.content, 2000),
                "company",
                "rag_company_context",
                tenant_id,
                None,
                Some(revision.source_id.clone()),
                Some(revision.id.clone()),
            ),
            self.context_node(
                &format!("{base}/chunks/0001"),
                &revision.title,
                2,
                &revision.content,
                "company",
                "rag_company_context",
                tenant_id,
                None,
                Some(revision.source_id.clone()),
                Some(revision.id.clone()),
            ),
        ];
        let uris = nodes.iter().map(|node| node.uri.clone()).collect();
        upsert_context_nodes(&mut data.company_context, nodes);
        uris
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
            source_id,
            revision_id,
            status: "active".to_string(),
            privacy: if index_kind == "company" {
                "company".to_string()
            } else {
                "private".to_string()
            },
            updated_at: now(),
        }
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
