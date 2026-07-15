use super::*;

impl Store {
    pub async fn create_ingest_task_async(
        &self,
        tenant_id: &str,
        req: IngestTaskRequest,
        config: &Config,
    ) -> Result<IngestTask, ApiError> {
        let task = self
            .create_ingest_task_record_async(tenant_id, &req, config, false, 0)
            .await?;
        Box::pin(self.run_ingest_task_async(tenant_id, &task.task_id, req, None, config))
            .await
            .map(|result| result.task)
    }

    /// Prune terminal ingest tasks (`completed` / `failed`) whose lifecycle
    /// ended more than `retention_seconds` ago, together with their stored
    /// results — both in the in-memory maps and the backing repository, so
    /// pruned tasks do not resurrect from Meilisearch on restart.
    /// Non-terminal tasks are never pruned regardless of age. Returns the
    /// pruned task ids.
    pub async fn cleanup_ingest_tasks_async(
        &self,
        tenant_id: &str,
        retention_seconds: u64,
    ) -> Result<Vec<String>, ApiError> {
        let cutoff = chrono::Utc::now()
            - chrono::Duration::seconds(retention_seconds.min(i64::MAX as u64) as i64);
        let expired: Vec<String> = {
            let data = self.read()?;
            data.ingest_tasks
                .values()
                .filter(|task| task.tenant_id == tenant_id)
                .filter(|task| matches!(task.state.as_str(), "completed" | "failed"))
                .filter(|task| task.completed_at.unwrap_or(task.updated_at) < cutoff)
                .map(|task| task.task_id.clone())
                .collect()
        };
        if expired.is_empty() {
            return Ok(expired);
        }
        let expired_for_stage = expired.clone();
        let (_, _) = self
            .execute_staged_mutation(
                tenant_id,
                "ingest_tasks.cleanup",
                None,
                None,
                MutationPrimary::DeleteIngestTasks,
                move |staged| {
                    let mut data = staged.write()?;
                    for task_id in &expired_for_stage {
                        data.ingest_tasks.remove(task_id);
                        if let Some(result) = data.ingest_results.remove(task_id) {
                            data.parsed_blocks.remove(&SourceDocumentKey::new(
                                &result.task.tenant_id,
                                result.task.owner_user_id.as_deref(),
                                &result.source_document_uri,
                            ));
                        }
                    }
                    Ok(())
                },
            )
            .await?;
        Ok(expired)
    }

    pub async fn ingest_file_sync_async<F>(
        &self,
        tenant_id: &str,
        req: IngestTaskRequest,
        staged_upload: Option<StagedUpload>,
        config: &Config,
        on_task_created: F,
    ) -> Result<IngestTaskResult, ApiError>
    where
        F: FnOnce(&str),
    {
        let has_staged_upload = staged_upload.is_some();
        let task = self
            .create_ingest_task_record_async(tenant_id, &req, config, has_staged_upload, 0)
            .await?;
        on_task_created(&task.task_id);
        Box::pin(self.run_ingest_task_async(tenant_id, &task.task_id, req, staged_upload, config))
            .await
    }

    pub async fn create_ingest_task_record_async(
        &self,
        tenant_id: &str,
        req: &IngestTaskRequest,
        config: &Config,
        has_staged_upload: bool,
        queued_ahead: usize,
    ) -> Result<IngestTask, ApiError> {
        if req.idempotency_key.is_some() {
            return Err(ApiError::bad_request(
                "idempotency_key is not supported for ingest tasks",
            ));
        }
        let mut parser_config = config.clone();
        if let Some(provider) = req.parser_provider.as_deref() {
            parser_config.parser_provider = provider.to_string();
        }
        if let Some(backend) = req.parser_backend.as_deref() {
            parser_config.mineru_backend = backend.to_string();
        }
        validate_parser_config(&parser_config)?;

        let has_content = req
            .content
            .as_deref()
            .is_some_and(|content| !content.trim().is_empty());
        if !has_content
            && req.bytes.as_ref().is_none_or(Vec::is_empty)
            && !has_staged_upload
            && req.content_list.is_none()
            && req.content_list_v2.is_none()
        {
            return Err(ApiError::bad_request(
                "content, file bytes, or MinerU content_list output is required",
            ));
        }

        let title = req
            .title
            .clone()
            .or_else(|| req.file_name.clone())
            .or_else(|| req.source_uri.clone())
            .unwrap_or_else(|| "Parsed document".to_string());
        let source_id = req.source_id.clone().unwrap_or_else(|| {
            format!(
                "ingest:{}",
                sanitize_slug(
                    req.source_uri
                        .as_deref()
                        .or(req.file_name.as_deref())
                        .unwrap_or(&title)
                )
            )
        });
        let revision_id = req.revision_id.clone().unwrap_or_else(|| new_id("rev"));
        let source_document_uri = req.source_document_uri.clone().unwrap_or_else(|| {
            let scope = if req.owner_user_id.is_some() {
                "user"
            } else {
                "company"
            };
            format!(
                "ctx://{scope}/ingest/{}/source/{}",
                sanitize_slug(&source_id),
                sanitize_slug(&revision_id)
            )
        });
        let parser_provider = parser_config.parser_provider.clone();
        let parser_backend = if parser_provider == "mineru" {
            parser_config.mineru_backend.clone()
        } else {
            "text".to_string()
        };
        let now = now();
        let task_id = new_id("ingest");
        let task = IngestTask {
            task_id: task_id.clone(),
            tenant_id: tenant_id.to_string(),
            owner_user_id: req.owner_user_id.clone(),
            source_id: source_id.clone(),
            revision_id: revision_id.clone(),
            source_document_uri: Some(source_document_uri.clone()),
            parser_provider: parser_provider.clone(),
            parser_backend: parser_backend.clone(),
            state: "queued".to_string(),
            error: None,
            created_at: now,
            updated_at: now,
            completed_at: None,
            status_url: Some(format!("/v1/ingest/tasks/{task_id}")),
            result_url: Some(format!("/v1/ingest/tasks/{task_id}/result")),
            queued_ahead: Some(queued_ahead),
        };
        let owner = task.owner_user_id.clone();
        let task_for_stage = task.clone();
        let (task, _) = self
            .execute_staged_mutation(
                tenant_id,
                "ingest_task.create",
                owner.as_deref(),
                None,
                MutationPrimary::IngestTask,
                move |staged| {
                    let mut data = staged.write()?;
                    if task_for_stage.owner_user_id.is_none() {
                        ensure_company_source_not_deleting(
                            &data,
                            tenant_id,
                            &task_for_stage.source_id,
                        )?;
                    }
                    data.ingest_tasks
                        .insert(task_for_stage.task_id.clone(), task_for_stage.clone());
                    Ok(task_for_stage)
                },
            )
            .await?;
        Ok(task)
    }

    pub async fn run_ingest_task_async(
        &self,
        tenant_id: &str,
        task_id: &str,
        mut req: IngestTaskRequest,
        staged_upload: Option<StagedUpload>,
        config: &Config,
    ) -> Result<IngestTaskResult, ApiError> {
        let mut parser_config = config.clone();
        if let Some(provider) = req.parser_provider.as_deref() {
            parser_config.parser_provider = provider.to_string();
        }
        if let Some(backend) = req.parser_backend.as_deref() {
            parser_config.mineru_backend = backend.to_string();
        }
        validate_parser_config(&parser_config)?;
        let task = self.ingest_task_for_run(task_id)?;
        let uses_builtin_parser = parser_config.parser_provider == "builtin";
        let staged_builtin_upload = uses_builtin_parser && staged_upload.is_some();
        let staged_original = (!uses_builtin_parser)
            .then(|| staged_upload.clone())
            .flatten();
        let original_content = if uses_builtin_parser {
            String::new()
        } else {
            req.content
                .clone()
                .or_else(|| {
                    req.bytes
                        .as_ref()
                        .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
                })
                .unwrap_or_default()
        };
        let parser_content = if uses_builtin_parser {
            req.content.take()
        } else {
            req.content.clone()
        };
        let parser_bytes = if uses_builtin_parser {
            req.bytes.take()
        } else {
            req.bytes.clone()
        };

        let parsing_observation = self.metrics.begin_ingest_stage("parsing");
        self.transition_ingest_task_async(task_id, "parsing", None)
            .await?;
        let parser = match self.parser_registry.parser_for_config(&parser_config) {
            Ok(parser) => parser,
            Err(err) => {
                let _ = self
                    .transition_ingest_task_async(
                        task_id,
                        "failed",
                        Some(INGEST_ERROR_PARSER_FAILED.to_string()),
                    )
                    .await;
                return Err(err);
            }
        };
        let mut parsed = match parser
            .parse(ParserInput {
                content: parser_content,
                bytes: parser_bytes,
                staged_upload,
                content_type: req.content_type.clone(),
                file_name: req.file_name.clone(),
                content_list: req.content_list.clone(),
                content_list_v2: req.content_list_v2.clone(),
                middle_json: req.middle_json.clone(),
                model_json: req.model_json.clone(),
            })
            .await
        {
            Ok(parsed) => parsed,
            Err(err) => {
                let _ = self
                    .transition_ingest_task_async(
                        task_id,
                        "failed",
                        Some(INGEST_ERROR_PARSER_FAILED.to_string()),
                    )
                    .await;
                return Err(err);
            }
        };
        if let Err(err) = validate_parser_output(&parsed, parser_config.parser_max_response_bytes) {
            let _ = self
                .transition_ingest_task_async(
                    task_id,
                    "failed",
                    Some(INGEST_ERROR_PARSER_FAILED.to_string()),
                )
                .await;
            return Err(err);
        }

        let staged_original_content = if original_content.is_empty() {
            match staged_original {
                Some(upload) => upload.read_utf8().await?,
                None => None,
            }
        } else {
            None
        };

        self.transition_ingest_task_async(task_id, "parsed", None)
            .await?;
        parsing_observation.complete();
        let fragmenting_observation = self.metrics.begin_ingest_stage("fragmenting");
        // The built-in parser already performs the single bounded staged-file read.
        // Reuse its markdown rather than materializing the upload a second time.
        let original_content_for_artifacts = if !original_content.is_empty() {
            original_content.as_str()
        } else if staged_builtin_upload {
            parsed.markdown.as_deref().unwrap_or_default()
        } else {
            staged_original_content.as_deref().unwrap_or_default()
        };
        let artifacts = build_parse_artifacts(
            tenant_id,
            req.owner_user_id.clone(),
            task.source_document_uri.as_deref().unwrap_or_default(),
            &task.source_id,
            &task.revision_id,
            &parsed,
            original_content_for_artifacts,
        )?;
        let artifact_refs = artifacts
            .iter()
            .map(|artifact| ParseArtifactRef {
                id: artifact.id.clone(),
                artifact_kind: artifact.artifact_kind.clone(),
                uri: artifact.uri.clone(),
                checksum: Some(artifact.checksum.clone()),
            })
            .collect::<Vec<_>>();
        let document_content = parsed
            .markdown
            .take()
            .filter(|content| !content.trim().is_empty())
            .unwrap_or_else(|| {
                if !original_content.trim().is_empty() {
                    original_content
                } else if let Some(content) = staged_original_content {
                    content
                } else {
                    parsed
                        .blocks
                        .iter()
                        .filter_map(parsed_block_text)
                        .collect::<Vec<_>>()
                        .join("\n\n")
                }
            });
        let checksum = req
            .checksum
            .clone()
            .unwrap_or_else(|| sha256_hex(document_content.as_bytes()));

        self.transition_ingest_task_async(task_id, "fragmenting", None)
            .await?;
        let (index_kind, index_uid) = if let Some(owner) = req.owner_user_id.as_deref() {
            let routing = self.resolver.resolve(tenant_id, owner, false, true)?;
            ("personal".to_string(), routing.personal_context_index_uid)
        } else {
            ("company".to_string(), "rag_company_context".to_string())
        };
        let owner = req.owner_user_id.clone();
        let artifacts_for_stage = artifacts.clone();
        let blocks_for_stage = parsed.blocks.clone();
        let artifact_refs_for_stage = artifact_refs.clone();
        let source_id = task.source_id.clone();
        let revision_id = task.revision_id.clone();
        let source_document_uri = task.source_document_uri.clone().unwrap_or_default();
        let title = req
            .title
            .clone()
            .or_else(|| req.file_name.clone())
            .or_else(|| req.source_uri.clone())
            .unwrap_or_else(|| "Parsed document".to_string());
        let fragment_policy = req.fragment_policy.clone();
        let (ingest, _) = Box::pin(self.execute_staged_mutation(
            tenant_id,
            "ingest.outputs.persist",
            owner.as_deref(),
            None,
            MutationPrimary::SourceDocuments,
            |staged| {
                let mut data = staged.write()?;
                if let Some(owner_user_id) = owner.as_deref() {
                    staged.ensure_user_index_locked(
                        &mut data,
                        tenant_id,
                        owner_user_id,
                        EVENT_INDEX_SCHEMA_VERSION,
                    )?;
                } else {
                    ensure_company_source_not_deleting(&data, tenant_id, &source_id)?;
                }
                for artifact in artifacts_for_stage.iter().cloned() {
                    data.parse_artifacts
                        .insert(ParseArtifactKey::from_artifact(&artifact), artifact);
                }
                Ok(staged.write_source_document_fragments_locked(
                    &mut data,
                    tenant_id,
                    owner.clone(),
                    "parsed_doc",
                    &source_id,
                    &revision_id,
                    &source_document_uri,
                    &title,
                    &document_content,
                    &checksum,
                    &index_kind,
                    &index_uid,
                    fragment_policy.as_ref(),
                    &blocks_for_stage,
                    &artifact_refs_for_stage,
                ))
            },
        ))
        .await?;
        fragmenting_observation.complete();

        let indexing_observation = self.metrics.begin_ingest_stage("indexing");
        self.transition_ingest_task_async(task_id, "indexing", None)
            .await?;

        let mut task = self.ingest_task_for_run(task_id)?;
        apply_ingest_task_transition(&mut task, "completed", None);
        let result = IngestTaskResult {
            task: task.clone(),
            source_document_uri: ingest.source_document_uri,
            source_id: ingest.source_id,
            revision_id: task.revision_id.clone(),
            parse_artifacts: artifacts,
            parsed_blocks: parsed.blocks,
            context_uris: ingest.fragment_uris.clone(),
            fragment_uris: ingest.fragment_uris,
        };
        let owner = task.owner_user_id.clone();
        let result_for_stage = result.clone();
        let (result, _) = Box::pin(self.execute_staged_mutation(
            tenant_id,
            "ingest.complete",
            owner.as_deref(),
            None,
            MutationPrimary::IngestResult,
            move |staged| {
                let mut data = staged.write()?;
                if result_for_stage.task.owner_user_id.is_none() {
                    ensure_company_source_not_deleting(
                        &data,
                        tenant_id,
                        &result_for_stage.source_id,
                    )?;
                }
                let current = data
                    .ingest_tasks
                    .get(task_id)
                    .ok_or_else(|| ApiError::not_found("ingest task not found"))?;
                if !is_nonterminal_ingest_state(&current.state) {
                    return Err(ApiError::conflict(
                        "ingest task was terminalized before completion",
                    ));
                }
                data.ingest_tasks.insert(
                    result_for_stage.task.task_id.clone(),
                    result_for_stage.task.clone(),
                );
                data.ingest_results.insert(
                    result_for_stage.task.task_id.clone(),
                    result_for_stage.clone(),
                );
                Ok(result_for_stage)
            },
        ))
        .await?;
        indexing_observation.complete();
        Ok(result)
    }

    pub fn get_ingest_task(
        &self,
        task_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<IngestTask, ApiError> {
        let data = self.read()?;
        data.ingest_tasks
            .get(task_id)
            .filter(|task| ingest_task_visible(task, owner_user_id, include_all_private))
            .cloned()
            .map(sanitize_ingest_task)
            .ok_or_else(|| ApiError::not_found("ingest task not found"))
    }

    pub fn get_ingest_task_result(
        &self,
        task_id: &str,
        owner_user_id: Option<&str>,
        include_all_private: bool,
    ) -> Result<IngestTaskResult, ApiError> {
        let data = self.read()?;
        let visible_completed_task = data
            .ingest_tasks
            .get(task_id)
            .filter(|task| ingest_task_visible(task, owner_user_id, include_all_private))
            .filter(|task| task.state == "completed");
        visible_completed_task
            .and_then(|_| data.ingest_results.get(task_id))
            .cloned()
            .map(|mut result| {
                result.task = sanitize_ingest_task(result.task);
                result
            })
            .map(Ok)
            .unwrap_or_else(|| {
                let Some(task) = data
                    .ingest_tasks
                    .get(task_id)
                    .filter(|task| ingest_task_visible(task, owner_user_id, include_all_private))
                else {
                    return Err(ApiError::not_found("ingest result not found"));
                };
                if task.state == "failed" {
                    Err(ApiError::conflict("ingest task failed"))
                } else {
                    Err(ApiError::conflict("ingest result is not ready"))
                }
            })
    }
}
