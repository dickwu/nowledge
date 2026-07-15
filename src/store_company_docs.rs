use super::*;

impl Store {
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
        require_string(Some(source_id.to_string()), "source_id")?;
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
            tenant_id: tenant_id.to_string(),
            source_id: source_id.to_string(),
            title: title.clone(),
            source_uri: source_uri.clone(),
            checksum,
            content,
            status: "staged".to_string(),
            created_at: now(),
        };

        let mut data = self.write()?;
        ensure_company_source_not_deleting(&data, tenant_id, source_id)?;
        data.sources
            .entry(source_id.to_string())
            .or_insert_with(|| CompanySource {
                id: source_id.to_string(),
                tenant_id: tenant_id.to_string(),
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
        ensure_company_source_not_deleting(&data, tenant_id, source_id)?;
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

        let ingest = self.write_company_revision_context_locked(&mut data, tenant_id, &revision);

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
            source_document_uri: ingest.source_document_uri,
            fragment_uris: ingest.fragment_uris.clone(),
            context_uris: ingest.fragment_uris,
        })
    }

    pub fn list_revisions(&self, source_id: &str) -> Result<Value, ApiError> {
        let data = self.read()?;
        Ok(json!({
            "source_id": source_id,
            "revisions": data.source_revisions.get(source_id).cloned().unwrap_or_default()
        }))
    }

    pub fn list_company_docs(&self) -> Result<Value, ApiError> {
        let data = self.read()?;
        let mut sources: Vec<Value> = data
            .sources
            .values()
            .map(|s| {
                let revisions = data.source_revisions.get(&s.id);
                let revision_count = revisions.map(|v| v.len()).unwrap_or(0);
                let active_rev = s.active_revision_id.as_deref().and_then(|active_id| {
                    revisions.and_then(|revs| revs.iter().find(|r| r.id == active_id))
                });
                json!({
                    "source_id": s.id,
                    "title": s.title,
                    "source_uri": s.source_uri,
                    "active_revision_id": active_rev.map(|r| &r.id),
                    "active_revision_created_at": active_rev.map(|r| r.created_at),
                    "active_revision_status": active_rev.map(|r| &r.status),
                    "revision_count": revision_count
                })
            })
            .collect();
        // Sort by active_revision_created_at descending; sources without an active revision sort last.
        sources.sort_by(|a, b| {
            let ta = a["active_revision_created_at"].as_str().unwrap_or("");
            let tb = b["active_revision_created_at"].as_str().unwrap_or("");
            tb.cmp(ta)
        });
        Ok(json!({ "sources": sources }))
    }

    pub fn get_company_doc(&self, source_id: &str) -> Result<Value, ApiError> {
        let data = self.read()?;
        let source = data
            .sources
            .get(source_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("source not found"))?;
        let revisions = data
            .source_revisions
            .get(source_id)
            .cloned()
            .unwrap_or_default();
        if revisions.is_empty() {
            return Err(ApiError::not_found("no revisions for source"));
        }
        // The canonical "active" pointer lives on CompanySource — individual
        // SourceRevision rows can carry a stale status="active" from prior
        // activations (the activation flow updates the source pointer but
        // doesn't demote prior revisions' status). Follow the source pointer
        // first, fall back to the most recent revision if the source doesn't
        // name an active one yet.
        let rev = source
            .active_revision_id
            .as_deref()
            .and_then(|active_id| revisions.iter().find(|r| r.id == active_id))
            .or_else(|| revisions.last())
            .unwrap(); // safe: revisions non-empty
        Ok(json!({
            "source_id": source.id,
            "title": rev.title,
            "content": rev.content,
            "revision_id": rev.id,
            "status": rev.status,
            "created_at": rev.created_at,
            "source_uri": rev.source_uri,
            "active_revision_id": source.active_revision_id
        }))
    }

    pub async fn delete_company_doc(
        &self,
        tenant_id: &str,
        source_id: &str,
    ) -> Result<Value, ApiError> {
        let _mutation_guard = self.mutation_gate.lock().await;
        {
            let data = self.read()?;
            ensure_company_source_not_deleting(&data, tenant_id, source_id)?;
            if data
                .sources
                .get(source_id)
                .is_none_or(|source| source.tenant_id != tenant_id)
            {
                return Err(ApiError::not_found("source not found"));
            }
        }
        self.ensure_company_source_documents_loaded(tenant_id, source_id)
            .await?;
        let source_id_for_stage = source_id.to_string();
        let (mut response, persistence) = self
            .execute_staged_mutation_guarded(
                tenant_id,
                "company_doc.delete",
                None,
                MutationIdempotency {
                    key: None,
                    request_fingerprint: None,
                },
                MutationPrimary::DeleteCompanySource,
                move |staged| {
                    let mut data = staged.write()?;
                    ensure_company_source_not_deleting(&data, tenant_id, &source_id_for_stage)?;
                    if data
                        .sources
                        .get(&source_id_for_stage)
                        .is_none_or(|source| source.tenant_id != tenant_id)
                    {
                        return Err(ApiError::not_found("source not found"));
                    }
                    let removed_document_keys = data
                        .source_documents
                        .iter()
                        .filter(|(_, document)| {
                            document.tenant_id == tenant_id
                                && document.owner_user_id.is_none()
                                && document.source_id == source_id_for_stage
                        })
                        .map(|(key, _)| key.clone())
                        .collect::<HashSet<_>>();
                    let mut removed_document_uris = removed_document_keys
                        .iter()
                        .map(|key| key.uri.clone())
                        .collect::<HashSet<_>>();
                    removed_document_uris.extend(company_source_related_uris(
                        &data,
                        tenant_id,
                        &source_id_for_stage,
                    ));
                    ensure_company_source_mutations_reconciled_before_delete(
                        &data,
                        tenant_id,
                        &source_id_for_stage,
                        &removed_document_uris,
                    )?;
                    let removed_task_ids = data
                        .ingest_tasks
                        .values()
                        .filter(|task| {
                            task.tenant_id == tenant_id
                                && task.owner_user_id.is_none()
                                && task.source_id == source_id_for_stage
                        })
                        .map(|task| task.task_id.clone())
                        .collect::<HashSet<_>>();
                    let removed_link_ids = data
                        .links
                        .values()
                        .filter(|link| {
                            link.tenant_id == tenant_id
                                && (removed_document_uris.contains(&link.source_uri)
                                    || removed_document_uris.contains(&link.target_uri))
                        })
                        .map(|link| link.id.clone())
                        .collect::<HashSet<_>>();

                    data.sources.remove(&source_id_for_stage);
                    let remove_revision_entry = if let Some(revisions) =
                        data.source_revisions.get_mut(&source_id_for_stage)
                    {
                        revisions.retain(|revision| revision.tenant_id != tenant_id);
                        revisions.is_empty()
                    } else {
                        false
                    };
                    if remove_revision_entry {
                        data.source_revisions.remove(&source_id_for_stage);
                    }
                    data.company_context.retain(|node| {
                        node.tenant_id != tenant_id
                            || node.source_id.as_deref() != Some(&source_id_for_stage)
                    });
                    data.source_documents
                        .retain(|key, _| !removed_document_keys.contains(key));
                    data.parse_artifacts.retain(|_, artifact| {
                        artifact.tenant_id != tenant_id
                            || artifact.owner_user_id.is_some()
                            || artifact.source_id != source_id_for_stage
                    });
                    data.parsed_blocks
                        .retain(|key, _| !removed_document_keys.contains(key));
                    data.ingest_tasks
                        .retain(|task_id, _| !removed_task_ids.contains(task_id));
                    data.ingest_results.retain(|task_id, result| {
                        !removed_task_ids.contains(task_id)
                            && (result.task.tenant_id != tenant_id
                                || result.task.owner_user_id.is_some()
                                || result.source_id != source_id_for_stage)
                    });
                    data.links
                        .retain(|link_id, _| !removed_link_ids.contains(link_id));
                    data.link_idempotency
                        .retain(|_, link_id| !removed_link_ids.contains(link_id));
                    Ok(json!({
                        "source_id": source_id_for_stage,
                        "deleted": true,
                        "fragments_task": null,
                        "revisions_task": null,
                        "source_task": null,
                        "auxiliary_tasks": [],
                    }))
                },
            )
            .await?;
        if let Some(persistence) = persistence {
            response["deleted"] = json!(
                persistence.status == OperationStatus::Completed
                    && persistence.indexing_state == OperationIndexingState::Completed
            );
            response["fragments_task"] = json!(persistence.task_uids.first());
            response["revisions_task"] = json!(persistence.task_uids.get(1));
            response["source_task"] = json!(persistence.task_uids.get(2));
            response["auxiliary_tasks"] =
                json!(persistence.task_uids.iter().skip(3).collect::<Vec<_>>());
            response["persistence"] = json!(persistence);
        }
        Ok(response)
    }
}

impl Store {
    pub async fn create_revision_async(
        &self,
        tenant_id: &str,
        source_id: &str,
        req: CreateRevisionRequest,
    ) -> Result<CreateRevisionResponse, ApiError> {
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint = self.mutation_request_fingerprint(
            tenant_id,
            "company_revision.create",
            &(source_id, &req),
        )?;
        let (response, _) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "company_revision.create",
                None,
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::SourceRevision,
                |staged| staged.create_revision(tenant_id, source_id, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn activate_revision_async(
        &self,
        tenant_id: &str,
        source_id: &str,
        revision_id: &str,
        req: ActivateRevisionRequest,
    ) -> Result<ActivateRevisionResponse, ApiError> {
        let (response, _) = self
            .execute_staged_mutation(
                tenant_id,
                "company_revision.activate",
                None,
                None,
                MutationPrimary::SourceRevision,
                |staged| staged.activate_revision(tenant_id, source_id, revision_id, req),
            )
            .await?;
        Ok(response)
    }
}
