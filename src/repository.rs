use std::sync::Arc;

use async_trait::async_trait;
use serde::Serialize;
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

    async fn upsert_state_item(&self, item: &StateItem) -> Result<Option<String>, ApiError>;

    async fn upsert_company_source(
        &self,
        source: &CompanySource,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_source_revision(
        &self,
        revision: &SourceRevision,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_structured_snapshot(
        &self,
        snapshot: &StructuredSnapshot,
    ) -> Result<Option<String>, ApiError>;

    async fn upsert_structured_rows(&self, rows: &[Value]) -> Result<Option<String>, ApiError>;

    async fn upsert_structured_summary(&self, summary: &Value) -> Result<Option<String>, ApiError>;

    async fn upsert_trace(&self, trace: &TraceRecord) -> Result<Option<String>, ApiError>;

    async fn search_user_events(
        &self,
        routing: &EventIndexRouting,
        req: &HistorySearchRequest,
    ) -> Result<Option<Vec<HistoryEvent>>, ApiError>;

    async fn search_context(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        query: &str,
        mode: &str,
        limit: usize,
        resolver: &EventIndexResolver,
    ) -> Result<Option<RepositoryContextSearch>, ApiError>;

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

    async fn search_user_events(
        &self,
        _routing: &EventIndexRouting,
        _req: &HistorySearchRequest,
    ) -> Result<Option<Vec<HistoryEvent>>, ApiError> {
        Ok(None)
    }

    async fn search_context(
        &self,
        _tenant_id: &str,
        _owner_user_id: Option<&str>,
        _query: &str,
        _mode: &str,
        _limit: usize,
        _resolver: &EventIndexResolver,
    ) -> Result<Option<RepositoryContextSearch>, ApiError> {
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
        tenant_id: &str,
        owner_user_id: Option<&str>,
        query: &str,
        mode: &str,
        limit: usize,
        resolver: &EventIndexResolver,
    ) -> Result<Option<RepositoryContextSearch>, ApiError> {
        let mut stages = Vec::new();
        let mut l0_nodes = Vec::new();
        let mut all_nodes = Vec::new();

        for layer in [0_u8, 1, 2] {
            let mut layer_nodes = Vec::new();
            let company_filter = context_filter(tenant_id, None, layer)?;
            let company = self
                .search_context_index("rag_company_context", query, &company_filter, limit)
                .await?;
            stages.push(context_stage(
                &format!("l{layer}_company"),
                "rag_company_context",
                query,
                &company_filter,
                company.processing_time_ms,
                &company.hits,
            ));
            layer_nodes.extend(company.hits);

            if let Some(owner) = owner_user_id {
                let routing = resolver.resolve(tenant_id, owner, false, true)?;
                let personal_filter = context_filter(tenant_id, Some(owner), layer)?;
                let personal = self
                    .search_context_index(
                        &routing.personal_context_index_uid,
                        query,
                        &personal_filter,
                        limit,
                    )
                    .await?;
                stages.push(context_stage(
                    &format!("l{layer}_personal"),
                    &routing.personal_context_index_uid,
                    query,
                    &personal_filter,
                    personal.processing_time_ms,
                    &personal.hits,
                ));
                layer_nodes.extend(personal.hits);
            }

            layer_nodes.sort_by(|a, b| {
                text_score(&format!("{} {}", b.title, b.body), query)
                    .partial_cmp(&text_score(&format!("{} {}", a.title, a.body), query))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            layer_nodes.truncate(limit);
            if layer == 0 {
                l0_nodes = layer_nodes.clone();
            }
            all_nodes.extend(layer_nodes);
        }

        if !l0_nodes.is_empty() {
            let bases = l0_nodes
                .iter()
                .map(|node| strip_context_layer_suffix(&node.uri))
                .collect::<Vec<_>>();
            all_nodes.retain(|node| {
                node.layer == 0
                    || bases
                        .iter()
                        .any(|base| strip_context_layer_suffix(&node.uri) == *base)
            });
        }

        all_nodes.sort_by(|a, b| {
            text_score(&format!("{} {}", b.title, b.body), query)
                .partial_cmp(&text_score(&format!("{} {}", a.title, a.body), query))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        all_nodes.truncate(limit);
        stages.push(json!({
            "stage": "selection",
            "mode": mode,
            "selected_uris": all_nodes.iter().map(|node| &node.uri).collect::<Vec<_>>()
        }));

        Ok(Some(RepositoryContextSearch {
            nodes: all_nodes,
            stages,
        }))
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
    layer: u8,
) -> Result<String, ApiError> {
    let mut filters = vec![
        format!("tenant_id = {}", meili_string(tenant_id)?),
        format!("layer = {layer}"),
        "status = \"active\"".to_string(),
    ];
    if let Some(owner) = owner_user_id {
        filters.push(format!("owner_user_id = {}", meili_string(owner)?));
        filters.push("privacy = \"private\"".to_string());
    } else {
        filters.push("privacy = \"company\"".to_string());
    }
    Ok(filters.join(" AND "))
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
