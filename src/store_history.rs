use super::*;

impl Store {
    pub fn ensure_user_index(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        req: EnsureUserEventIndexRequest,
    ) -> Result<UserEventIndexResponse, ApiError> {
        if req
            .schema_version
            .is_some_and(|version| version != EVENT_INDEX_SCHEMA_VERSION)
        {
            return Err(ApiError::bad_request(format!(
                "schema_version must be {EVENT_INDEX_SCHEMA_VERSION}"
            )));
        }
        if !req.create_personal_context_index {
            return Err(ApiError::bad_request(
                "create_personal_context_index must be true",
            ));
        }
        let mut data = self.write()?;
        let (index, routing) = self.ensure_user_index_locked(
            &mut data,
            tenant_id,
            owner_user_id,
            EVENT_INDEX_SCHEMA_VERSION,
        )?;

        let _ = req.force_reapply_settings;

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

    pub fn list_user_indexes(
        &self,
        tenant_id: &str,
    ) -> Result<ListUserEventIndexesResponse, ApiError> {
        let data = self.read()?;
        let mut indexes: Vec<_> = data
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .cloned()
            .collect();
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
        if req.owner_user_ids.is_empty() {
            indexes = data
                .user_indexes
                .iter()
                .filter(|((tenant, _), _)| tenant == tenant_id)
                .map(|(_, index)| {
                    let mut index = index.clone();
                    if req.dry_run {
                        index.status = "dry_run".to_string();
                    }
                    index
                })
                .collect();
            indexes.sort_by_key(|index| index.created_at);
            if req.reapply_settings && !req.dry_run {
                updated_settings = indexes.len();
            }
            return Ok(ReconcileUserEventIndexesResponse {
                checked: indexes.len(),
                created,
                updated_settings,
                errors: Vec::new(),
                indexes,
            });
        }

        for owner in req.owner_user_ids {
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

            let owner_hash = self.resolver.user_hash(&owner);
            let existed = data
                .user_indexes
                .contains_key(&(tenant_id.to_string(), owner_hash));
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

    pub async fn reconcile_user_indexes_async(
        &self,
        tenant_id: &str,
        req: ReconcileUserEventIndexesRequest,
    ) -> Result<ReconcileUserEventIndexesResponse, ApiError> {
        if req.dry_run {
            return self.reconcile_user_indexes(tenant_id, req);
        }
        let reapply_settings = req.reapply_settings;
        let existing_ids = self
            .read()?
            .user_indexes
            .values()
            .filter(|index| index.tenant_id == tenant_id)
            .map(|index| index.id.clone())
            .collect::<HashSet<_>>();
        let (response, _) = self
            .execute_staged_mutation(
                tenant_id,
                "user_event_indexes.reconcile",
                None,
                None,
                MutationPrimary::UserIndex,
                |staged| staged.reconcile_user_indexes(tenant_id, req),
            )
            .await?;
        // Existing registry rows produce no mutation delta, but this admin
        // endpoint also re-applies their dynamic-index settings. Newly
        // created rows were already applied by their journal steps.
        for index in &response.indexes {
            if reapply_settings && existing_ids.contains(&index.id) {
                self.execute_explicit_resource_operation(
                    tenant_id,
                    "user_event_indexes.settings_reapply",
                    None,
                    MutationPrimary::UserIndex,
                    OperationResource::EnsureUserEventIndex {
                        index: index.clone(),
                    },
                )
                .await?;
            }
        }
        Ok(response)
    }

    pub fn append_event(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: AppendHistoryEventRequest,
    ) -> Result<HistoryEventResponse, ApiError> {
        let owner_user_id = self.validate_append_event_request(path_owner_user_id, &req)?;

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
        let text = req.text.unwrap_or_default();
        let payload = req.payload;
        let tags = req.tags;
        let privacy = req.privacy;

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
                    let same_request = event.event_type == event_type
                        && event.entity_type == entity_type
                        && event.entity_id == entity_id
                        && event.occurred_at == occurred_at
                        && event.observed_at == observed_at
                        && event.source_kind == source_kind
                        && event.source_ref == source_ref
                        && event.text == text
                        && event.payload == payload
                        && event.tags == tags
                        && event.privacy == privacy
                        && event.tenant_id == tenant_id
                        && event.owner_user_id == owner_user_id;
                    if !same_request {
                        return Err(ApiError::conflict(
                            "idempotency key was already used for a different request",
                        ));
                    }
                    return Ok(HistoryEventResponse {
                        event,
                        duplicate: true,
                        materialization_job_id: None,
                        routing,
                        meili_task_uid: None,
                        persistence: None,
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
            text,
            payload,
            tags,
            privacy,
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
            persistence: None,
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
        if req
            .events
            .iter()
            .any(|event| event.idempotency_key.is_some())
        {
            return Err(ApiError::bad_request(
                "events[].idempotency_key is not supported; use the batch idempotency_key",
            ));
        }

        let owner = self
            .owner_from_path_or_body(path_owner_user_id, req.events[0].owner_user_id.as_deref())?;
        for event in &req.events {
            self.validate_append_event_request(Some(&owner), event)?;
        }
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
            persistence: None,
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
}

impl Store {
    pub async fn ensure_user_index_async(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        req: EnsureUserEventIndexRequest,
    ) -> Result<UserEventIndexResponse, ApiError> {
        let force_reapply = req.force_reapply_settings;
        let (mut response, persistence) = self
            .execute_staged_mutation(
                tenant_id,
                "user_event_index.ensure",
                Some(owner_user_id),
                None,
                MutationPrimary::UserIndex,
                |staged| staged.ensure_user_index(tenant_id, owner_user_id, req),
            )
            .await?;
        if let Some(persistence) = persistence {
            response.meili_task_uids = persistence.task_uids;
        } else if force_reapply {
            let persistence = self
                .execute_explicit_resource_operation(
                    tenant_id,
                    "user_event_index.settings_reapply",
                    Some(owner_user_id),
                    MutationPrimary::UserIndex,
                    OperationResource::EnsureUserEventIndex {
                        index: response.index.clone(),
                    },
                )
                .await?;
            response.meili_task_uids = persistence.primary_task_uids;
        }
        Ok(response)
    }

    pub async fn append_event_async(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: AppendHistoryEventRequest,
    ) -> Result<HistoryEventResponse, ApiError> {
        let owner = self.validate_append_event_request(path_owner_user_id, &req)?;
        // A history event is the primary aggregate, but its dynamic physical
        // indexes are infrastructure prerequisites. Provision and confirm
        // them in their own journaled operation before submitting the event;
        // otherwise Meilisearch may auto-create an unconfigured index.
        self.ensure_user_index_async(tenant_id, &owner, EnsureUserEventIndexRequest::default())
            .await?;
        let idempotency_key = req.idempotency_key.clone();
        let mut canonical_request = req.clone();
        canonical_request.owner_user_id = Some(owner.clone());
        let request_fingerprint = self.mutation_request_fingerprint(
            tenant_id,
            "history_event.append",
            &canonical_request,
        )?;
        let (mut response, persistence) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "history_event.append",
                Some(&owner),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::HistoryEvents,
                |staged| staged.append_event(tenant_id, path_owner_user_id, req),
            )
            .await?;
        if let Some(persistence) = persistence {
            response.meili_task_uid = persistence.primary_task_uids.last().cloned();
            response.persistence = Some(persistence);
        }
        Ok(response)
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
        if req.events.len() > 500 {
            return Err(ApiError::bad_request(
                "events must contain at most 500 entries",
            ));
        }
        if req
            .events
            .iter()
            .any(|event| event.idempotency_key.is_some())
        {
            return Err(ApiError::bad_request(
                "events[].idempotency_key is not supported; use the batch idempotency_key",
            ));
        }
        let owner = self
            .owner_from_path_or_body(path_owner_user_id, req.events[0].owner_user_id.as_deref())?;
        for event in &req.events {
            self.validate_append_event_request(Some(&owner), event)?;
        }
        self.ensure_user_index_async(tenant_id, &owner, EnsureUserEventIndexRequest::default())
            .await?;
        let idempotency_key = req.idempotency_key.clone();
        let mut canonical_request = req.clone();
        for event in &mut canonical_request.events {
            event.owner_user_id = Some(owner.clone());
        }
        let request_fingerprint = self.mutation_request_fingerprint(
            tenant_id,
            "history_event.append_bulk",
            &canonical_request,
        )?;
        let (mut response, persistence) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "history_event.append_bulk",
                Some(&owner),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::HistoryEvents,
                |staged| staged.append_bulk_events(tenant_id, Some(&owner), req),
            )
            .await?;
        if let Some(persistence) = persistence {
            response.meili_task_uid = persistence.primary_task_uids.last().cloned();
            response.persistence = Some(persistence);
        }
        Ok(response)
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
        if let Some(mut hits) = self.repository.search_user_events(&routing, &req).await? {
            if self.redaction_config.write_consistency == WriteConsistency::ReadYourWrites {
                let local = self
                    .search_events(tenant_id, path_owner_user_id, req.clone())?
                    .hits;
                let mut merged = hits
                    .drain(..)
                    .map(|event| (event.id.clone(), event))
                    .collect::<HashMap<_, _>>();
                for event in local {
                    merged.insert(event.id.clone(), event);
                }
                hits = merged.into_values().collect();
                hits.sort_by(|left, right| {
                    right
                        .occurred_at
                        .cmp(&left.occurred_at)
                        .then_with(|| right.id.cmp(&left.id))
                });
                hits.truncate(req.limit.max(1));
            }
            return Ok(HistorySearchResponse { hits, routing });
        }
        self.search_events(tenant_id, path_owner_user_id, req)
    }

    pub async fn timeline_async(
        &self,
        tenant_id: &str,
        path_owner_user_id: Option<&str>,
        req: TimelineQueryRequest,
    ) -> Result<TimelineResponse, ApiError> {
        let owner_user_id =
            self.owner_from_path_or_body(path_owner_user_id, req.owner_user_id.as_deref())?;
        let search = HistorySearchRequest {
            owner_user_id: Some(owner_user_id.clone()),
            from: req.from,
            to: req.to,
            limit: req.limit,
            ..HistorySearchRequest::default()
        };
        let mut events = self
            .search_events_async(tenant_id, Some(&owner_user_id), search)
            .await?
            .hits;
        events.sort_by_key(|event| event.occurred_at);
        Ok(TimelineResponse { events })
    }
}
