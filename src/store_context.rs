use super::*;

impl Store {
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
        if let Some(node) = self.context_node_for_acl_locked(
            &data,
            tenant_id,
            owner_user_id,
            include_all_private,
            |node| node.uri == uri && node.status == "active",
        )? {
            return Ok(node);
        }
        self.source_document_for_acl_locked(
            &data,
            tenant_id,
            uri,
            owner_user_id,
            include_all_private,
        )?
        .map(source_document_context_node)
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
        self.context_node_for_acl_locked(
            &data,
            tenant_id,
            owner_user_id,
            include_all_private,
            |node| {
                strip_layer_suffix(&node.uri) == target
                    && node.layer == layer
                    && node.status == "active"
            },
        )?
        .ok_or_else(|| ApiError::not_found("context layer not found"))
    }

    pub fn traceback(
        &self,
        tenant_id: &str,
        req: ContextTracebackRequest,
        include_all_private: bool,
    ) -> Result<ContextTracebackResponse, ApiError> {
        let uri = require_string(req.uri, "uri")?;
        let owner_user_id = req.owner_user_id.as_deref();
        let data = self.read()?;
        let fragment = self
            .context_node_for_acl_locked(
                &data,
                tenant_id,
                owner_user_id,
                include_all_private,
                |node| node.uri == uri && node.status == "active",
            )?
            .ok_or_else(|| ApiError::not_found("fragment uri not found"))?;
        if fragment.node_kind != "fragment" {
            return Err(ApiError::bad_request(
                "traceback uri must identify a fragment",
            ));
        }

        let part_of = data
            .links
            .values()
            .find(|link| {
                link.tenant_id == tenant_id
                    && link.status == "active"
                    && link.relation == "part_of"
                    && link.source_uri == fragment.uri
            })
            .cloned();
        let source_document_uri = fragment
            .source_document_uri
            .clone()
            .or_else(|| part_of.map(|link| link.target_uri))
            .ok_or_else(|| ApiError::not_found("source document link not found"))?;
        let source_document = self
            .source_document_for_acl_locked(
                &data,
                tenant_id,
                &source_document_uri,
                owner_user_id,
                include_all_private,
            )?
            .ok_or_else(|| ApiError::not_found("source document not found"))?;

        Ok(ContextTracebackResponse {
            fragment_uri: fragment.uri,
            fragment_title: fragment.title,
            fragment_index: fragment.fragment_index,
            checksum: fragment.checksum,
            token_estimate: fragment.token_estimate,
            source_document_uri: source_document.uri,
            source_id: source_document.source_id,
            revision_id: source_document.revision_id,
            page_idx: fragment.page_idx,
            bbox: fragment.bbox,
            block_type: fragment.block_type,
            section_path: fragment.section_path,
            asset_refs: fragment.asset_refs,
            artifact_refs: fragment.artifact_refs,
            char_start: fragment.char_start,
            char_end: fragment.char_end,
            source_title: source_document.title,
        })
    }

    pub fn search_context(
        &self,
        tenant_id: &str,
        req: ContextSearchRequest,
        is_admin: bool,
    ) -> Result<ContextSearchOutcome, ApiError> {
        let query = require_string(req.query, "query")?;
        let owner_user_id = resolve_context_owner(req.owner_user_id, &req.filters)?;
        let limit = req.limit.max(1);
        let structured_filters = parse_context_filters(&req.filters)?;
        let include = ContextIncludeSet::from_request(&req.include)?;
        let profile = ContextReturnProfile::from_request(&req.return_profile)?;
        let data = self.read()?;
        let nodes = self.context_scope_locked(&data, tenant_id, owner_user_id.as_deref())?;

        let candidates: Vec<ContextNode> = nodes
            .iter()
            .filter(|node| retrieval_candidate(node))
            .filter(|node| structured_filters.matches_node(node))
            .cloned()
            .collect();
        let vector_scores = self.vector_score_map(&query, &candidates);
        let doc_scores =
            self.vector_doc_score_map(&query, doc_candidates_locked(&data, &candidates));
        let fragments = rank_nodes(
            candidates.into_iter(),
            &query,
            limit,
            &vector_scores,
            &doc_scores,
        );

        let selected_nodes: Vec<_> = fragments
            .iter()
            .map(|(node, _)| node.clone())
            .take(limit)
            .collect();
        let redaction_secrets = self.redaction_config.configured_secret_values();
        let mut hits: Vec<_> = fragments
            .iter()
            .take(limit)
            .map(|(node, score)| {
                let mut hit = context_hit_from_node(node, *score, &redaction_secrets);
                hit.score_breakdown = Some(score_breakdown_value(
                    node,
                    &query,
                    &vector_scores,
                    &doc_scores,
                    *score,
                ));
                hit
            })
            .collect();
        enrich_context_hits_locked(
            &data,
            tenant_id,
            owner_user_id.as_deref(),
            &selected_nodes,
            &mut hits,
            &include,
            profile,
        );
        drop(data);

        let stages = sanitize_context_stages(
            vec![stage_value(
                "fragments",
                &fragments,
                owner_user_id.as_deref(),
            )],
            req.debug,
            is_admin,
        );
        let groups = context_source_groups(profile, &hits);
        let hits = shape_context_hits(hits, profile, &include);
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
            groups,
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
            false,
        )?;
        Ok(self.answer_from_context(outcome))
    }
}

impl Store {
    pub async fn search_context_async(
        &self,
        tenant_id: &str,
        req: ContextSearchRequest,
        is_admin: bool,
    ) -> Result<ContextSearchOutcome, ApiError> {
        let query = require_string(req.query.clone(), "query")?;
        let owner_user_id = resolve_context_owner(req.owner_user_id.clone(), &req.filters)?;
        let limit = req.limit.max(1);
        let structured_filters = parse_context_filters(&req.filters)?;
        let include = ContextIncludeSet::from_request(&req.include)?;
        let profile = ContextReturnProfile::from_request(&req.return_profile)?;
        if let Some(mut result) = self
            .repository
            .search_context(RepositoryContextSearchQuery {
                tenant_id,
                owner_user_id: owner_user_id.as_deref(),
                query: &query,
                mode: &req.mode,
                limit,
                filters: &structured_filters,
                resolver: &self.resolver,
            })
            .await?
        {
            result.nodes.retain(|node| {
                self.validate_repository_context_node(node, tenant_id, owner_user_id.as_deref())
                    .is_ok()
            });
            if self.redaction_config.write_consistency == WriteConsistency::ReadYourWrites {
                let local_scope = {
                    let data = self.read()?;
                    self.context_scope_locked(&data, tenant_id, owner_user_id.as_deref())?
                };
                let mut local_by_uri = HashMap::new();
                for node in local_scope {
                    local_by_uri.insert((node.uri.clone(), node.layer), node);
                }
                result
                    .nodes
                    .retain(|node| !local_by_uri.contains_key(&(node.uri.clone(), node.layer)));
                result
                    .nodes
                    .extend(local_by_uri.into_values().filter(|node| {
                        retrieval_candidate(node) && structured_filters.matches_node(node)
                    }));
            }
            // Use the same hybrid ranker as the memory path after merging
            // repository and local RYW candidates. This retains vector-only
            // local matches without admitting unrelated zero-score rows.
            let doc_candidates = {
                let data = self.read()?;
                doc_candidates_locked(&data, &result.nodes)
            };
            let vector_scores = self.vector_score_map(&query, &result.nodes);
            let doc_scores = self.vector_doc_score_map(&query, doc_candidates);
            let scored_nodes = rank_nodes(
                result.nodes.into_iter(),
                &query,
                limit,
                &vector_scores,
                &doc_scores,
            );
            let nodes: Vec<ContextNode> =
                scored_nodes.iter().map(|(node, _)| node.clone()).collect();
            let redaction_secrets = self.redaction_config.configured_secret_values();
            let mut hits = scored_nodes
                .iter()
                .map(|(node, score)| {
                    let mut hit = context_hit_from_node(node, *score, &redaction_secrets);
                    hit.score_breakdown = Some(score_breakdown_value(
                        node,
                        &query,
                        &vector_scores,
                        &doc_scores,
                        *score,
                    ));
                    hit
                })
                .collect::<Vec<_>>();
            self.enrich_context_hits(
                tenant_id,
                owner_user_id.as_deref(),
                &nodes,
                &mut hits,
                &include,
                profile,
            )?;
            let groups = context_source_groups(profile, &hits);
            let hits = shape_context_hits(hits, profile, &include);
            let stages = sanitize_context_stages(result.stages, req.debug, is_admin);
            let trace = TraceRecord {
                id: new_id("trace"),
                tenant_id: tenant_id.to_string(),
                owner_user_id: owner_user_id.clone(),
                query,
                mode: req.mode,
                stages: stages.clone(),
                context_uris: hits.iter().map(|hit| hit.uri.clone()).collect(),
                created_at: now(),
            };
            let response = ContextSearchResponse {
                trace_id: trace.id.clone(),
                hits,
                groups,
                stages,
            };
            let outcome = ContextSearchOutcome {
                response,
                trace,
                nodes,
            };
            let outcome_for_stage = outcome.clone();
            let (outcome, _) = self
                .execute_staged_mutation(
                    tenant_id,
                    "trace.create",
                    owner_user_id.as_deref(),
                    None,
                    MutationPrimary::Trace,
                    move |staged| {
                        staged.insert_trace(outcome_for_stage.trace.clone())?;
                        Ok(outcome_for_stage)
                    },
                )
                .await?;
            return Ok(outcome);
        }
        let owner = owner_user_id;
        let (outcome, _) = self
            .execute_staged_mutation(
                tenant_id,
                "trace.create",
                owner.as_deref(),
                None,
                MutationPrimary::Trace,
                |staged| staged.search_context(tenant_id, req, is_admin),
            )
            .await?;
        Ok(outcome)
    }

    pub async fn answer_rag_async(
        &self,
        tenant_id: &str,
        req: RagAnswerRequest,
        is_admin: bool,
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
                is_admin,
            )
            .await?;
        Ok(self.answer_from_context(outcome))
    }

    fn enrich_context_hits(
        &self,
        tenant_id: &str,
        owner_user_id: Option<&str>,
        nodes: &[ContextNode],
        hits: &mut [ContextHit],
        include: &ContextIncludeSet,
        profile: ContextReturnProfile,
    ) -> Result<(), ApiError> {
        let data = self.read()?;
        enrich_context_hits_locked(
            &data,
            tenant_id,
            owner_user_id,
            nodes,
            hits,
            include,
            profile,
        );
        Ok(())
    }
}
