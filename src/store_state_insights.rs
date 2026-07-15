use super::*;

impl Store {
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

    pub(super) fn materialize_analysis(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        req: AnalysisMaterializationRequest,
    ) -> Result<AnalysisMaterializationResponse, ApiError> {
        let mut link_natural_keys = HashSet::with_capacity(req.links.len());
        for candidate in &req.links {
            let source_uri = canonical_link_uri(&require_string(
                Some(candidate.source_uri.clone()),
                "source_uri",
            )?);
            let target_uri = canonical_link_uri(&require_string(
                Some(candidate.target_uri.clone()),
                "target_uri",
            )?);
            if source_uri == target_uri {
                return Err(ApiError::bad_request(
                    "source_uri and target_uri must refer to different context nodes",
                ));
            }
            let relation = normalize_relation(&candidate.relation);
            if !link_natural_keys.insert(link_natural_key(
                tenant_id,
                Some(owner_user_id),
                &source_uri,
                &target_uri,
                &relation,
            )) {
                return Err(ApiError::bad_request(
                    "analysis materialization contains a duplicate link natural identity",
                ));
            }
        }

        let mut insight_context_uris = HashSet::with_capacity(req.insights.len());
        for candidate in &req.insights {
            require_string(Some(candidate.insight_type.clone()), "insight_type")?;
            require_string(Some(candidate.title.clone()), "title")?;
            require_string(Some(candidate.statement.clone()), "statement")?;
            if !insight_context_uris.insert(crate::analysis::analysis_insight_context_uri(
                &candidate.insight_type,
                &candidate.title,
            )) {
                return Err(ApiError::bad_request(
                    "analysis materialization contains a duplicate insight context identity",
                ));
            }
        }

        let mut created_links = Vec::with_capacity(req.links.len());
        for candidate in req.links {
            let source_uri = canonical_link_uri(&candidate.source_uri);
            let target_uri = canonical_link_uri(&candidate.target_uri);
            let relation = normalize_relation(&candidate.relation);
            let natural_key = link_natural_key(
                tenant_id,
                Some(owner_user_id),
                &source_uri,
                &target_uri,
                &relation,
            );
            let existing = {
                let data = self.read()?;
                let mut matches = data.links.values().filter(|link| {
                    link_natural_key(
                        &link.tenant_id,
                        link.owner_user_id.as_deref(),
                        &link.source_uri,
                        &link.target_uri,
                        &link.relation,
                    ) == natural_key
                });
                let existing = matches.next().cloned();
                if matches.next().is_some() {
                    return Err(ApiError::conflict(
                        "analysis link natural identity is ambiguous",
                    ));
                }
                if let Some(existing) = existing.as_ref() {
                    ensure_link_not_pending_company_source_delete(
                        &data,
                        tenant_id,
                        Some(&existing.id),
                        &existing.source_uri,
                        &existing.target_uri,
                    )?;
                }
                existing
            };
            if let Some(existing) = existing {
                if existing.status != "active" {
                    return Err(ApiError::conflict(
                        "analysis link natural identity is not active",
                    ));
                }
                created_links.push(existing);
                continue;
            }

            let candidate_key = self.mutation_request_fingerprint(
                tenant_id,
                "analysis.materialize.link",
                &(owner_user_id, &candidate),
            )?;
            let mut tags = candidate.tags;
            if !tags.iter().any(|tag| tag == "analysis")
                && tags.len() < crate::analysis::MAX_TAGS_PER_CANDIDATE
            {
                tags.push("analysis".to_string());
            }
            let response = self.upsert_link(
                tenant_id,
                LinkUpsertRequest {
                    owner_user_id: Some(owner_user_id.to_string()),
                    source_uri: Some(source_uri),
                    target_uri: Some(target_uri),
                    source_title: candidate.source_title,
                    target_title: candidate.target_title,
                    relation,
                    rationale: candidate.rationale,
                    evidence_text: None,
                    confidence: candidate.confidence,
                    created_by: "analysis_api".to_string(),
                    tags,
                    idempotency_key: Some(candidate_key),
                },
            )?;
            created_links.push(response.link);
        }

        let mut insights = Vec::with_capacity(req.insights.len());
        for candidate in req.insights {
            let context_uri = crate::analysis::analysis_insight_context_uri(
                &candidate.insight_type,
                &candidate.title,
            );
            let existing = {
                let data = self.read()?;
                let mut matches = data.insights.values().filter(|insight| {
                    insight.tenant_id == tenant_id
                        && insight.owner_user_id == owner_user_id
                        && insight.context_uri == context_uri
                });
                let existing = matches.next().cloned();
                if matches.next().is_some() {
                    return Err(ApiError::conflict(
                        "analysis insight context identity is ambiguous",
                    ));
                }
                existing
            };
            if let Some(existing) = existing {
                if existing.status != "active" {
                    return Err(ApiError::conflict(
                        "analysis insight context identity is not active",
                    ));
                }
                if existing.insight_type != candidate.insight_type
                    || existing.title != candidate.title
                {
                    return Err(ApiError::conflict(
                        "analysis insight context identity collides with an existing insight",
                    ));
                }
                insights.push(existing);
                continue;
            }

            let candidate_key = self.mutation_request_fingerprint(
                tenant_id,
                "analysis.materialize.insight",
                &(owner_user_id, &candidate),
            )?;
            let source_refs = candidate
                .source_uris
                .into_iter()
                .map(|uri| crate::analysis::canonicalize_analysis_uri(&uri).unwrap_or(uri))
                .map(|uri| SourceRef {
                    kind: "context_uri".to_string(),
                    id: uri.clone(),
                    uri: Some(uri),
                    meta: None,
                })
                .collect();
            let response = self.upsert_insight(
                tenant_id,
                InsightUpsertRequest {
                    owner_user_id: Some(owner_user_id.to_string()),
                    insight_type: Some(candidate.insight_type),
                    title: Some(candidate.title),
                    statement: Some(candidate.statement),
                    evidence_text: None,
                    source_refs,
                    confidence: candidate.confidence,
                    salience: candidate.salience,
                    privacy: "private".to_string(),
                    merge_policy: "merge".to_string(),
                    idempotency_key: Some(candidate_key),
                },
            )?;
            insights.push(response.insight);
        }

        Ok(AnalysisMaterializationResponse {
            created_links,
            insights,
            persistence: None,
        })
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
                .get(&(tenant_id.to_string(), owner_user_id.clone(), hash.clone()))
                .cloned()
            {
                id
            } else {
                let id = new_id("insight");
                data.insight_idempotency.insert(
                    (tenant_id.to_string(), owner_user_id.clone(), hash),
                    id.clone(),
                );
                id
            }
        } else {
            new_id("insight")
        };

        let context_uri = crate::analysis::analysis_insight_context_uri(&insight_type, &title);
        let created_at = data
            .insights
            .get(&id)
            .map_or(now, |existing| existing.created_at);
        let insight = InsightRecord {
            id: id.clone(),
            tenant_id: tenant_id.to_string(),
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
            created_at,
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
            if insight.tenant_id != tenant_id {
                return Err(ApiError::not_found("insight not found"));
            }
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
                    .get(&(tenant_id.to_string(), owner_scope.clone(), hash.clone()))
                    .cloned()
                {
                    if let Some(link) = data.links.get(&existing_id).cloned() {
                        ensure_link_not_pending_company_source_delete(
                            &data,
                            tenant_id,
                            Some(&existing_id),
                            &link.source_uri,
                            &link.target_uri,
                        )?;
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
                ensure_link_not_pending_company_source_delete(
                    &data,
                    tenant_id,
                    Some(&existing_id),
                    &source_uri,
                    &target_uri,
                )?;
                let created_at = data
                    .links
                    .get(&existing_id)
                    .map(|link| link.created_at)
                    .unwrap_or(now);
                (existing_id, created_at, "updated".to_string())
            } else {
                ensure_link_not_pending_company_source_delete(
                    &data,
                    tenant_id,
                    None,
                    &source_uri,
                    &target_uri,
                )?;
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
                    .insert((tenant_id.to_string(), owner_scope, hash), id.clone());
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
                        link.source_uri, link.relation, link.target_uri
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
}

impl Store {
    pub async fn upsert_state_fact_async(
        &self,
        tenant_id: &str,
        fact_key: &str,
        req: UpsertStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        let owner = require_string(req.owner_user_id.clone(), "owner_user_id")?;
        let state_type = require_string(req.state_type.clone(), "state_type")?;
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint =
            self.mutation_request_fingerprint(tenant_id, "state_fact.upsert", &(fact_key, &req))?;
        let _mutation_guard = self.mutation_gate.lock().await;
        self.ensure_state_aggregate_identity_available(tenant_id, &owner, &state_type, fact_key)?;
        let current_operation_id = idempotency_key.as_deref().map(|key| {
            let idempotency_key_hash = self.resolver.idempotency_hash(key);
            let owner_user_id_hash = self.resolver.user_hash(&owner);
            self.idempotent_operation_id(
                tenant_id,
                "state_fact.upsert",
                Some(&owner_user_id_hash),
                &idempotency_key_hash,
            )
        });
        self.ensure_state_operation_generation_ready(
            tenant_id,
            &owner,
            &state_type,
            fact_key,
            current_operation_id.as_deref(),
        )
        .await?;
        self.ensure_state_document_aggregate_loaded(tenant_id, &owner, &state_type, fact_key)
            .await?;
        let (response, _) = self
            .execute_staged_mutation_guarded(
                tenant_id,
                "state_fact.upsert",
                Some(&owner),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::StateItem,
                |staged| staged.upsert_state_fact(tenant_id, fact_key, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn patch_state_fact_async(
        &self,
        tenant_id: &str,
        fact_key: &str,
        req: PatchStateFactRequest,
    ) -> Result<StateItemResponse, ApiError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        let state_key =
            self.resolve_state_key(tenant_id, fact_key, req.owner_user_id.as_deref())?;
        let owner = state_key.1.clone();
        let state_type = self
            .read()?
            .state_items
            .get(&state_key)
            .map(|item| item.state_type.clone())
            .ok_or_else(|| ApiError::not_found("state item not found"))?;
        self.ensure_state_aggregate_identity_available(tenant_id, &owner, &state_type, fact_key)?;
        self.ensure_state_operation_generation_ready(
            tenant_id,
            &owner,
            &state_type,
            fact_key,
            None,
        )
        .await?;
        self.ensure_state_document_aggregate_loaded(tenant_id, &owner, &state_type, fact_key)
            .await?;
        let (response, _) = self
            .execute_staged_mutation_guarded(
                tenant_id,
                "state_fact.patch",
                Some(&owner),
                MutationIdempotency {
                    key: None,
                    request_fingerprint: None,
                },
                MutationPrimary::StateItem,
                |staged| staged.patch_state_fact(tenant_id, fact_key, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn upsert_insight_async(
        &self,
        tenant_id: &str,
        req: InsightUpsertRequest,
    ) -> Result<InsightResponse, ApiError> {
        let owner = require_string(req.owner_user_id.clone(), "owner_user_id")?;
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint =
            self.mutation_request_fingerprint(tenant_id, "insight.upsert", &req)?;
        let (response, _) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "insight.upsert",
                Some(&owner),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::Insight,
                |staged| staged.upsert_insight(tenant_id, req),
            )
            .await?;
        Ok(response)
    }

    /// Admit already-authorized analysis candidates as one durable operation
    /// when the batch creates any new records. Exact existing identities are
    /// returned unchanged; a pure-reuse batch is a no-op with no journal entry.
    ///
    /// The request deliberately carries no tenant, owner, or idempotency
    /// fields. The trusted server boundary supplies the scope, and this method
    /// derives bounded HMAC fingerprints for the operation and each newly
    /// persisted candidate before staging any cache changes.
    pub async fn materialize_analysis_async(
        &self,
        tenant_id: &str,
        owner_user_id: &str,
        req: AnalysisMaterializationRequest,
    ) -> Result<AnalysisMaterializationResponse, ApiError> {
        let owner_user_id = require_string(Some(owner_user_id.to_string()), "owner_user_id")?;
        if req.links.len() > MAX_ANALYSIS_MATERIALIZATION_LINKS {
            return Err(ApiError::bad_request(
                "analysis materialization link limit exceeded",
            ));
        }
        if req.insights.len() > MAX_ANALYSIS_MATERIALIZATION_INSIGHTS {
            return Err(ApiError::bad_request(
                "analysis materialization insight limit exceeded",
            ));
        }
        validate_analysis_materialization_request(&req)?;

        let request_fingerprint = self.mutation_request_fingerprint(
            tenant_id,
            "analysis.materialize",
            &(owner_user_id.as_str(), &req),
        )?;
        let (mut response, persistence) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "analysis.materialize",
                Some(&owner_user_id),
                MutationIdempotency {
                    key: Some(&request_fingerprint),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::AnalysisMaterialization,
                |staged| staged.materialize_analysis(tenant_id, &owner_user_id, req),
            )
            .await?;
        response.persistence = persistence;
        Ok(response)
    }

    pub async fn patch_insight_async(
        &self,
        tenant_id: &str,
        insight_id: &str,
        req: InsightPatchRequest,
    ) -> Result<InsightResponse, ApiError> {
        let owner = self.insight_owner(tenant_id, insight_id)?;
        let (response, _) = self
            .execute_staged_mutation(
                tenant_id,
                "insight.patch",
                Some(&owner),
                None,
                MutationPrimary::Insight,
                |staged| staged.patch_insight(tenant_id, insight_id, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn upsert_link_async(
        &self,
        tenant_id: &str,
        req: LinkUpsertRequest,
    ) -> Result<LinkResponse, ApiError> {
        let owner = req.owner_user_id.clone();
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint =
            self.mutation_request_fingerprint(tenant_id, "link.upsert", &req)?;
        let (response, _) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "link.upsert",
                owner.as_deref(),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::Links,
                |staged| staged.upsert_link(tenant_id, req),
            )
            .await?;
        Ok(response)
    }
}
