use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};

use serde_json::{json, Value};

use crate::{
    config::Config,
    error::ApiError,
    models::*,
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
        }
    }

    pub fn resolver(&self) -> &EventIndexResolver {
        &self.resolver
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

        let mut task_uids = Vec::new();
        if req.force_reapply_settings {
            task_uids.push(format!("settings-{}", routing.settings_hash));
        }
        if req.create_personal_context_index {
            task_uids.push(format!("context-{}", routing.owner_user_id_hash));
        }

        Ok(UserEventIndexResponse {
            index,
            routing,
            meili_task_uids: task_uids,
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
        indexes.sort_by(|a, b| a.created_at.cmp(&b.created_at));
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
                indexes.push(UserEventIndex {
                    id: format!("{}:{}", routing.tenant_id, routing.owner_user_id_hash),
                    tenant_id: routing.tenant_id.clone(),
                    tenant_hash: self.resolver.tenant_hash(tenant_id),
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

        hits.sort_by(|a, b| b.occurred_at.cmp(&a.occurred_at));
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
        events.sort_by(|a, b| a.occurred_at.cmp(&b.occurred_at));
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
        hits.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
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
        self.write_insight_context_locked(&mut data, &routing, &insight, req.evidence_text.clone());
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
            self.write_insight_context_locked(&mut data, &routing, &insight, None);
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
        hits.sort_by(|a, b| b.updated_at.cmp(&a.updated_at));
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

        Ok(CreateRevisionResponse {
            source_id: source_id.to_string(),
            revision_id: revision.id,
            status: "staged".to_string(),
            history_event_id: None,
            ingest_job_id: if req.ingest {
                Some(new_id("ingest"))
            } else {
                None
            },
        })
    }

    pub fn activate_revision(
        &self,
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
        let context_uris = self.write_company_revision_context_locked(&mut data, &revision);

        Ok(ActivateRevisionResponse {
            source_id: source_id.to_string(),
            active_revision_id: revision_id.to_string(),
            previous_revision_id,
            history_event_id: None,
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
        for row in req.rows {
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
        let (snapshot, rows) = {
            let data = self.read()?;
            (
                data.snapshots
                    .get(&snapshot_id)
                    .cloned()
                    .ok_or_else(|| ApiError::not_found("snapshot not found"))?,
                data.rows_by_snapshot
                    .get(&snapshot_id)
                    .cloned()
                    .unwrap_or_default(),
            )
        };
        if snapshot.dataset_key != dataset_key {
            return Err(ApiError::bad_request(
                "snapshot dataset_key does not match path dataset_key",
            ));
        }

        let stats = deterministic_stats(&rows);
        let summary_id = new_id("summary");
        let context_uri = format!(
            "ctx://user/structured/{}/snapshots/{}/trend/.overview",
            sanitize_slug(dataset_key),
            sanitize_slug(&snapshot.period_key)
        );
        let summary = json!({
            "id": summary_id,
            "snapshot_id": snapshot_id,
            "dataset_key": dataset_key,
            "owner_user_id": snapshot.owner_user_id,
            "stats": stats,
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

        Ok(ApplySnapshotResponse {
            snapshot_id,
            summary_ids: vec![summary["id"].as_str().unwrap().to_string()],
            state_item_ids: Vec::new(),
            insight_candidate_ids: Vec::new(),
            context_uris: vec![context_uri],
            job_id: new_id("job"),
        })
    }

    pub fn current_structured_state(&self) -> Result<CurrentStructuredStateResponse, ApiError> {
        let data = self.read()?;
        Ok(CurrentStructuredStateResponse {
            items: data
                .state_items
                .values()
                .filter(|item| item.state_type == "structured_summary")
                .cloned()
                .collect(),
            summaries: data.structured_summaries.values().cloned().collect(),
        })
    }

    pub fn fs_ls(&self, uri: Option<&str>) -> Result<Value, ApiError> {
        let data = self.read()?;
        let prefix = uri.unwrap_or("ctx://");
        let nodes = self.all_context_nodes_locked(&data);
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

    pub fn fs_tree(&self, uri: Option<&str>, depth: Option<usize>) -> Result<Value, ApiError> {
        let mut tree = self.fs_ls(uri)?;
        tree["depth"] = json!(depth.unwrap_or(2));
        Ok(tree)
    }

    pub fn fs_read(&self, uri: &str) -> Result<ContextNode, ApiError> {
        let data = self.read()?;
        self.all_context_nodes_locked(&data)
            .into_iter()
            .find(|node| node.uri == uri && node.status == "active")
            .ok_or_else(|| ApiError::not_found("context uri not found"))
    }

    pub fn fs_layer(&self, uri: &str, layer: u8) -> Result<ContextNode, ApiError> {
        let target = strip_layer_suffix(uri);
        let data = self.read()?;
        self.all_context_nodes_locked(&data)
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
        req: ContextRevealRequest,
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
        let node = self.fs_layer(&uri, layer)?;
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

        Ok(RagAnswerResponse {
            answer_id: new_id("answer"),
            trace_id: outcome.response.trace_id,
            answer,
            citations,
            usage: json!({
                "provider": "none",
                "stages": ["L0", "L1", "L2"]
            }),
        })
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
            let index = UserEventIndex {
                id: format!("{}:{}", tenant_id, routing.owner_user_id_hash),
                tenant_id: tenant_id.to_string(),
                tenant_hash: self.resolver.tenant_hash(tenant_id),
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
                None,
                Some(revision.source_id.clone()),
                Some(revision.id.clone()),
            ),
        ];
        let uris = nodes.iter().map(|node| node.uri.clone()).collect();
        upsert_context_nodes(&mut data.company_context, nodes);
        uris
    }

    fn context_node(
        &self,
        uri: &str,
        title: &str,
        layer: u8,
        body: &str,
        index_kind: &str,
        index_uid: &str,
        owner_user_id: Option<String>,
        source_id: Option<String>,
        revision_id: Option<String>,
    ) -> ContextNode {
        ContextNode {
            uri: uri.to_string(),
            title: title.to_string(),
            layer,
            body: body.to_string(),
            tenant_id: "default".to_string(),
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

fn deterministic_stats(rows: &[Value]) -> Value {
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
    let metrics = numeric
        .into_iter()
        .map(|(key, values)| {
            let count = values.len();
            let sum: f64 = values.iter().sum();
            let mean = if count == 0 { 0.0 } else { sum / count as f64 };
            let min = values.iter().copied().fold(f64::INFINITY, f64::min);
            let max = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
            json!({
                "metric": key,
                "count": count,
                "mean": mean,
                "min": min,
                "max": max,
                "slope": simple_slope(&values)
            })
        })
        .collect::<Vec<_>>();
    json!({
        "row_count": rows.len(),
        "metrics": metrics
    })
}

fn simple_slope(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    (values[values.len() - 1] - values[0]) / (values.len() - 1) as f64
}
