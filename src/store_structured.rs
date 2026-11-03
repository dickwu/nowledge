use super::*;

impl Store {
    pub fn upsert_dataset(
        &self,
        tenant_id: &str,
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
            tenant_id: tenant_id.to_string(),
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
            let key = (tenant_id.to_string(), hash);
            if let Some(id) = data.snapshot_idempotency.get(&key).cloned() {
                id
            } else {
                let id = new_id("snapshot");
                data.snapshot_idempotency.insert(key, id.clone());
                id
            }
        } else {
            new_id("snapshot")
        };

        let snapshot = StructuredSnapshot {
            id: id.clone(),
            tenant_id: tenant_id.to_string(),
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

    pub fn get_snapshot(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
    ) -> Result<StructuredSnapshot, ApiError> {
        let data = self.read()?;
        data.snapshots
            .get(snapshot_id)
            .filter(|snapshot| snapshot.tenant_id == tenant_id)
            .cloned()
            .ok_or_else(|| ApiError::not_found("snapshot not found"))
    }

    pub fn bulk_rows(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
        req: BulkStructuredRowsRequest,
    ) -> Result<BulkStructuredRowsResponse, ApiError> {
        for row in &req.rows {
            if let Some(id) = row.get("id").and_then(Value::as_str) {
                require_string(Some(id.to_string()), "id")?;
            }
        }
        let mut data = self.write()?;
        let owner_user_id = data
            .snapshots
            .get(snapshot_id)
            .filter(|snapshot| snapshot.tenant_id == tenant_id)
            .map(|snapshot| snapshot.owner_user_id.clone())
            .ok_or_else(|| ApiError::not_found("snapshot not found"))?;

        let mut inserted = 0;
        let mut duplicates = 0;
        let mut invalid = 0;
        let mut row_ids = Vec::new();
        let mut rows_to_add = Vec::new();
        let batch_idempotency_hash = req
            .idempotency_key
            .as_deref()
            .map(|key| self.resolver.idempotency_hash(key));
        for (row_index, mut row) in req.rows.into_iter().enumerate() {
            if !row.is_object() {
                invalid += 1;
                continue;
            }
            let row_id = row
                .get("id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or_else(|| {
                    batch_idempotency_hash.as_deref().map(|batch_hash| {
                        format!(
                            "row_{}",
                            hmac_hex(
                                &self.redaction_config.index_hash_secret,
                                "structured-row-id-v1",
                                &format!("{snapshot_id}\0{batch_hash}\0{row_index}"),
                                32,
                            )
                        )
                    })
                })
                .unwrap_or_else(|| new_id("row"));
            let key = (snapshot_id.to_string(), row_id.clone());
            if data.row_idempotency.contains(&key) {
                duplicates += 1;
            } else {
                if let Some(obj) = row.as_object_mut() {
                    obj.insert("id".to_string(), Value::String(row_id.clone()));
                    obj.insert(
                        "snapshot_id".to_string(),
                        Value::String(snapshot_id.to_string()),
                    );
                    obj.insert(
                        "tenant_id".to_string(),
                        Value::String(tenant_id.to_string()),
                    );
                    obj.insert(
                        "owner_user_id".to_string(),
                        Value::String(owner_user_id.clone()),
                    );
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
        if inserted == 0 {
            let history_event_id = data
                .event_by_id
                .values()
                .filter(|event| {
                    event.tenant_id == tenant_id
                        && event.owner_user_id == owner_user_id
                        && event.event_type == "structured.rows.bulk_inserted"
                        && event.entity_id == snapshot_id
                })
                .max_by(|left, right| {
                    left.observed_at
                        .cmp(&right.observed_at)
                        .then_with(|| left.id.cmp(&right.id))
                })
                .map(|event| event.id.clone())
                .unwrap_or_default();
            return Ok(BulkStructuredRowsResponse {
                snapshot_id: snapshot_id.to_string(),
                inserted,
                duplicates,
                invalid,
                row_ids,
                history_event_id,
            });
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
                .filter(|snapshot| snapshot.tenant_id == tenant_id)
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
                        && candidate.tenant_id == tenant_id
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
            "tenant_id": tenant_id,
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
                    node_kind: "overview".to_string(),
                    retrieval_role: "overview".to_string(),
                    retrieval_enabled: false,
                    parent_uri: None,
                    source_document_uri: None,
                    fragment_index: None,
                    char_start: None,
                    char_end: None,
                    token_estimate: None,
                    checksum: None,
                    source_id: None,
                    revision_id: None,
                    block_type: None,
                    page_idx: None,
                    bbox: None,
                    section_path: Vec::new(),
                    heading_level: None,
                    asset_refs: Vec::new(),
                    artifact_refs: Vec::new(),
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
                    summary.get("tenant_id").and_then(Value::as_str) == Some(tenant_id)
                        && summary
                            .get("owner_user_id")
                            .and_then(Value::as_str)
                            .is_some_and(private_allowed)
                })
                .cloned()
                .collect(),
        })
    }
}

const ROWS_CURSOR_PREFIX: &str = "rows1.";
const ROWS_CURSOR_MAX_BYTES: usize = 4_096;

/// Optional keyset page over a snapshot's rows. Pages are ordered by the
/// logical row `id` ascending, so they are stable regardless of whether the
/// rows came from the in-process cache (insertion order) or a Meilisearch
/// scan, and concurrent appends never shift already-returned rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RowsPageRequest {
    pub(crate) limit: usize,
    pub(crate) after_row_id: Option<String>,
}

pub(crate) fn encode_rows_cursor(row_id: &str) -> String {
    format!("{ROWS_CURSOR_PREFIX}{}", hex::encode(row_id))
}

/// The cursor is an opaque, unsigned encoding of the last returned row id.
/// It only narrows rows inside a snapshot the caller is already authorized
/// for, so tampering cannot widen access.
pub(crate) fn decode_rows_cursor(cursor: &str) -> Result<String, ApiError> {
    let invalid = || ApiError::validation("cursor", "is invalid");
    if cursor.len() > ROWS_CURSOR_MAX_BYTES {
        return Err(invalid());
    }
    let payload = cursor
        .strip_prefix(ROWS_CURSOR_PREFIX)
        .ok_or_else(invalid)?;
    let decoded = hex::decode(payload).map_err(|_| invalid())?;
    let row_id = String::from_utf8(decoded).map_err(|_| invalid())?;
    if row_id.is_empty() {
        return Err(invalid());
    }
    Ok(row_id)
}

fn row_sort_id(row: &Value) -> &str {
    row.get("id").and_then(Value::as_str).unwrap_or_default()
}

/// Select one keyset page from `rows` and build the response body. Only the
/// selected rows are cloned.
fn rows_page_response(snapshot_id: &str, rows: &[Value], page: &RowsPageRequest) -> Value {
    let mut candidates = rows
        .iter()
        .filter(|row| {
            page.after_row_id
                .as_deref()
                .is_none_or(|after| row_sort_id(row) > after)
        })
        .collect::<Vec<_>>();
    candidates.sort_by(|left, right| row_sort_id(left).cmp(row_sort_id(right)));
    let has_more = candidates.len() > page.limit;
    candidates.truncate(page.limit);
    let next_cursor = has_more
        .then(|| {
            candidates
                .last()
                .map(|row| encode_rows_cursor(row_sort_id(row)))
        })
        .flatten();
    json!({
        "snapshot_id": snapshot_id,
        "rows": candidates.into_iter().cloned().collect::<Vec<_>>(),
        "next_cursor": next_cursor,
    })
}

impl Store {
    /// Read rows for a snapshot, optionally as one keyset page.
    ///
    /// When the rows are already resident (they were written or applied in
    /// this process) the cached vector is used, preserving read-your-writes.
    /// Otherwise the rows are read through the repository for this request
    /// only and are *not* inserted into `rows_by_snapshot`: a read-only list
    /// must not grow resident memory. Write and apply paths still load the
    /// cache because row idempotency and deterministic stats depend on it.
    pub(crate) async fn list_rows_page_async(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
        page: Option<RowsPageRequest>,
    ) -> Result<Value, ApiError> {
        self.get_snapshot_async(tenant_id, snapshot_id).await?;
        {
            let data = self.read()?;
            if let Some(rows) = data.rows_by_snapshot.get(snapshot_id) {
                self.metrics.record_cache_access("structured_rows", "hit");
                return Ok(match page.as_ref() {
                    Some(page) => rows_page_response(snapshot_id, rows, page),
                    None => json!({ "snapshot_id": snapshot_id, "rows": rows }),
                });
            }
        }
        self.metrics.record_cache_access("structured_rows", "miss");
        let rows = match self.repository.list_rows(tenant_id, snapshot_id).await {
            Ok(rows) => rows.unwrap_or_default(),
            Err(error) => {
                self.metrics
                    .record_read_through("structured_rows", "failure", 1);
                return Err(error);
            }
        };
        if rows.is_empty() {
            self.metrics
                .record_read_through("structured_rows", "not_found", 1);
        } else {
            self.metrics
                .record_read_through("structured_rows", "loaded", rows.len());
        }
        Ok(match page.as_ref() {
            Some(page) => rows_page_response(snapshot_id, &rows, page),
            None => json!({ "snapshot_id": snapshot_id, "rows": rows }),
        })
    }

    pub async fn create_snapshot_async(
        &self,
        tenant_id: &str,
        req: CreateStructuredSnapshotRequest,
    ) -> Result<StructuredSnapshotResponse, ApiError> {
        let owner = require_string(req.owner_user_id.clone(), "owner_user_id")?;
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint =
            self.mutation_request_fingerprint(tenant_id, "structured_snapshot.create", &req)?;
        let (response, _) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "structured_snapshot.create",
                Some(&owner),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::StructuredSnapshot,
                |staged| staged.create_snapshot(tenant_id, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn upsert_dataset_async(
        &self,
        tenant_id: &str,
        dataset_key: &str,
        req: DatasetSchemaUpsertRequest,
    ) -> Result<DatasetSchemaResponse, ApiError> {
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint =
            self.mutation_request_fingerprint(tenant_id, "dataset.upsert", &(dataset_key, &req))?;
        let (response, _) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "dataset.upsert",
                None,
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::Dataset,
                |staged| staged.upsert_dataset(tenant_id, dataset_key, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn bulk_rows_async(
        &self,
        tenant_id: &str,
        snapshot_id: &str,
        req: BulkStructuredRowsRequest,
    ) -> Result<BulkStructuredRowsResponse, ApiError> {
        let snapshot = self
            .ensure_snapshot_rows_loaded(tenant_id, snapshot_id)
            .await?;
        let owner = snapshot.owner_user_id;
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint = self.mutation_request_fingerprint(
            tenant_id,
            "structured_rows.bulk",
            &(snapshot_id, &req),
        )?;
        let (response, _) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "structured_rows.bulk",
                Some(&owner),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::StructuredRows,
                |staged| staged.bulk_rows(tenant_id, snapshot_id, req),
            )
            .await?;
        Ok(response)
    }

    pub async fn apply_snapshot_async(
        &self,
        tenant_id: &str,
        dataset_key: &str,
        req: ApplySnapshotRequest,
    ) -> Result<ApplySnapshotResponse, ApiError> {
        let snapshot_id = req
            .snapshot_id
            .as_deref()
            .ok_or_else(|| ApiError::bad_request("snapshot_id is required"))?;
        let snapshot = self
            .ensure_snapshot_rows_loaded(tenant_id, snapshot_id)
            .await?;
        let prior_snapshot_ids = {
            let data = self.read()?;
            let mut prior = data
                .snapshots
                .values()
                .filter(|candidate| {
                    candidate.id != snapshot.id
                        && candidate.tenant_id == tenant_id
                        && candidate.dataset_key == snapshot.dataset_key
                        && candidate.owner_user_id == snapshot.owner_user_id
                        && candidate.period_start < snapshot.period_start
                })
                .cloned()
                .collect::<Vec<_>>();
            prior.sort_by_key(|candidate| Reverse(candidate.period_start));
            prior
                .into_iter()
                .take(4)
                .map(|candidate| candidate.id)
                .collect::<Vec<_>>()
        };
        for prior_snapshot_id in prior_snapshot_ids {
            self.ensure_snapshot_rows_loaded(tenant_id, &prior_snapshot_id)
                .await?;
        }
        let owner = snapshot.owner_user_id.clone();
        let idempotency_key = req.idempotency_key.clone();
        let request_fingerprint = self.mutation_request_fingerprint(
            tenant_id,
            "structured_snapshot.apply",
            &(dataset_key, &req),
        )?;
        let (response, _) = self
            .execute_staged_mutation_with_idempotency(
                tenant_id,
                "structured_snapshot.apply",
                Some(&owner),
                MutationIdempotency {
                    key: idempotency_key.as_deref(),
                    request_fingerprint: Some(&request_fingerprint),
                },
                MutationPrimary::StructuredSummary,
                |staged| staged.apply_snapshot(tenant_id, dataset_key, req),
            )
            .await?;
        Ok(response)
    }
}
