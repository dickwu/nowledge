use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeSet, time::Instant};

use tokio::time::{sleep, timeout, Duration};

use crate::{config::Config, error::ApiError};

pub const FIXED_INDEXES: &[&str] = &[
    "rag_company_context",
    "rag_state_items",
    "rag_user_event_indexes",
    "rag_sources",
    "rag_source_revisions",
    "rag_source_documents",
    "rag_parse_artifacts",
    "rag_doc_candidates",
    "rag_structured_datasets",
    "rag_structured_snapshots",
    "rag_structured_rows",
    "rag_structured_summaries",
    "rag_insights",
    "rag_links",
    "rag_sessions",
    "rag_memory_diffs",
    "rag_feedback",
    "rag_traces",
    "rag_harness_components",
    "rag_harness_changes",
    "rag_harness_verdicts",
    "rag_ingest_tasks",
    "rag_ingest_results",
    "rag_eval_cases",
    "rag_eval_runs",
    "rag_eval_case_results",
    "rag_eval_overviews",
];

const MEILI_TASK_WAIT_ATTEMPTS: usize = 600;
const MEILI_TASK_WAIT_INTERVAL_MS: u64 = 100;

#[derive(Debug, Clone)]
pub struct MeiliAdmin {
    url: Option<String>,
    api_key: Option<String>,
    client: reqwest::Client,
}

#[derive(Debug, Serialize)]
pub struct BootstrapResult {
    pub indexes: Vec<String>,
    pub tasks: Vec<Value>,
    pub dry_run: bool,
}

#[derive(Debug, Deserialize)]
#[serde(bound(deserialize = "T: Deserialize<'de>"))]
pub struct SearchResponse<T> {
    #[serde(default)]
    pub hits: Vec<T>,
    #[serde(rename = "processingTimeMs", default)]
    pub processing_time_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DocumentPage {
    #[serde(default)]
    pub results: Vec<Value>,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub limit: usize,
    #[serde(default)]
    pub total: usize,
}

impl MeiliAdmin {
    pub fn from_config(config: &Config) -> Self {
        Self {
            url: config.meili_url.clone(),
            api_key: config.meili_api_key.clone(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn bootstrap(&self, reset: bool) -> Result<BootstrapResult, ApiError> {
        let Some(url) = &self.url else {
            return Ok(BootstrapResult {
                indexes: FIXED_INDEXES.iter().map(|s| s.to_string()).collect(),
                tasks: Vec::new(),
                dry_run: true,
            });
        };

        let provision = if reset {
            true
        } else {
            let mut existing = Vec::new();
            let mut missing = Vec::new();
            for uid in FIXED_INDEXES {
                if self.index_exists(uid).await? {
                    existing.push(*uid);
                } else {
                    missing.push(*uid);
                }
            }
            if !existing.is_empty() && !missing.is_empty() {
                return Err(ApiError::Upstream(format!(
                    "managed Meilisearch index set is incomplete; refusing automatic recreation of: {}",
                    missing.join(", ")
                )));
            }
            existing.is_empty()
        };

        let mut task_uids = Vec::new();
        for uid in FIXED_INDEXES {
            if reset {
                let response = self
                    .client
                    .delete(format!("{}/indexes/{}", url.trim_end_matches('/'), uid))
                    .headers(self.headers()?)
                    .send()
                    .await
                    .map_err(|e| ApiError::Upstream(e.to_string()))?;
                let status = response.status();
                if !status.is_success() && status.as_u16() != 404 {
                    return Err(ApiError::Upstream(format!(
                        "failed to delete Meilisearch index {uid}: {}",
                        status
                    )));
                }
                if status.is_success() {
                    let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
                    if let Some(task_uid) = task_uid(&body) {
                        self.wait_for_task(&task_uid).await?;
                        task_uids.push(task_uid);
                    }
                }
            }

            if provision {
                task_uids.extend(self.ensure_index(uid, "id", true).await?);
            } else {
                task_uids.extend(self.reconcile_existing_index(uid, true).await?);
            }
        }
        self.wait_for_tasks(&task_uids).await?;
        let tasks = task_uids
            .into_iter()
            .map(|task_uid| json!({ "taskUid": task_uid }))
            .collect();

        Ok(BootstrapResult {
            indexes: FIXED_INDEXES.iter().map(|s| s.to_string()).collect(),
            tasks,
            dry_run: false,
        })
    }

    pub fn configured(&self) -> bool {
        self.url.is_some()
    }

    pub async fn health_status(&self) -> Value {
        let Some(url) = &self.url else {
            return json!({
                "status": "memory",
                "healthy": true,
                "configured": false,
                "latency_ms": 0
            });
        };
        let started = Instant::now();
        let request = self
            .client
            .get(format!("{}/health", url.trim_end_matches('/')))
            .headers(match self.headers() {
                Ok(headers) => headers,
                Err(err) => {
                    return json!({
                        "status": "unhealthy",
                        "healthy": false,
                        "configured": true,
                        "error": err.to_string(),
                        "latency_ms": started.elapsed().as_millis() as u64
                    })
                }
            });
        let response = timeout(Duration::from_secs(2), request.send()).await;
        match response {
            Ok(Ok(response)) if response.status().is_success() => json!({
                "status": "ok",
                "healthy": true,
                "configured": true,
                "latency_ms": started.elapsed().as_millis() as u64
            }),
            Ok(Ok(response)) => json!({
                "status": "unhealthy",
                "healthy": false,
                "configured": true,
                "http_status": response.status().as_u16(),
                "latency_ms": started.elapsed().as_millis() as u64
            }),
            Ok(Err(err)) => json!({
                "status": "unhealthy",
                "healthy": false,
                "configured": true,
                "error": err.to_string(),
                "latency_ms": started.elapsed().as_millis() as u64
            }),
            Err(_) => json!({
                "status": "unhealthy",
                "healthy": false,
                "configured": true,
                "error": "Meilisearch health check timed out",
                "latency_ms": started.elapsed().as_millis() as u64
            }),
        }
    }

    pub async fn ensure_index(
        &self,
        uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(Vec::new());
        };

        let mut task_uids = Vec::new();
        if !self.index_exists(uid).await? {
            let response = self
                .client
                .post(format!("{}/indexes", url.trim_end_matches('/')))
                .headers(self.headers()?)
                .json(&json!({ "uid": uid, "primaryKey": primary_key }))
                .send()
                .await
                .map_err(|e| ApiError::Upstream(e.to_string()))?;
            if !response.status().is_success() {
                let status = response.status();
                if !self.index_exists(uid).await? {
                    return Err(ApiError::Upstream(format!(
                        "failed to create Meilisearch index {uid}: {status}"
                    )));
                }
            } else {
                let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
                if let Some(task_uid) = task_uid(&body) {
                    match self.wait_for_task(&task_uid).await {
                        Ok(()) => task_uids.push(task_uid),
                        Err(error) => {
                            if !self.index_exists(uid).await? {
                                return Err(error);
                            }
                            tracing::info!(
                                index_uid = uid,
                                "concurrent Meilisearch index creation already satisfied"
                            );
                        }
                    }
                }
            }
        }

        if apply_settings {
            if let Some(uid) = self.apply_settings(uid).await? {
                self.wait_for_task(&uid).await?;
                task_uids.push(uid);
            }
        }
        Ok(task_uids)
    }

    /// Reconcile settings for an index that must already exist.
    ///
    /// Startup recovery uses this instead of [`Self::ensure_index`] so a
    /// missing registered dynamic index is treated as data loss, not silently
    /// recreated as an empty index.
    pub async fn reconcile_existing_index(
        &self,
        uid: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>, ApiError> {
        if !self.index_exists(uid).await? {
            return Err(ApiError::Upstream(format!(
                "registered Meilisearch index {uid} is missing; refusing empty recreation"
            )));
        }
        let mut task_uids = Vec::new();
        if apply_settings {
            if let Some(task_uid) = self.apply_settings(uid).await? {
                self.wait_for_task(&task_uid).await?;
                task_uids.push(task_uid);
            }
        }
        Ok(task_uids)
    }

    pub async fn index_exists(&self, uid: &str) -> Result<bool, ApiError> {
        let Some(url) = &self.url else {
            return Ok(false);
        };
        let response = self
            .client
            .get(format!("{}/indexes/{}", url.trim_end_matches('/'), uid))
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;
        if response.status().is_success() {
            Ok(true)
        } else if response.status().as_u16() == 404 {
            Ok(false)
        } else {
            Err(ApiError::Upstream(format!(
                "failed to inspect Meilisearch index {uid}: {}",
                response.status()
            )))
        }
    }

    pub async fn add_documents<T: Serialize + ?Sized>(
        &self,
        index_uid: &str,
        documents: &T,
    ) -> Result<Option<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(None);
        };
        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/documents",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .json(documents)
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;

        if response.status().is_success() {
            let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            Ok(task_uid(&body))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to add Meilisearch documents into {index_uid}: {}",
                response.status()
            )))
        }
    }

    pub async fn delete_documents_by_filter(
        &self,
        index_uid: &str,
        filter: &str,
    ) -> Result<Option<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(None);
        };
        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/documents/delete",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .json(&json!({ "filter": filter }))
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;

        if response.status().as_u16() == 404 {
            return Ok(None);
        }

        if response.status().is_success() {
            let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            Ok(task_uid(&body))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to delete Meilisearch documents from {index_uid}: {}",
                response.status()
            )))
        }
    }

    pub async fn delete_documents_by_ids(
        &self,
        index_uid: &str,
        ids: &[String],
    ) -> Result<Option<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(None);
        };
        if ids.is_empty() {
            return Ok(None);
        }
        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/documents/delete-batch",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .json(ids)
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;

        if response.status().as_u16() == 404 {
            return Ok(None);
        }

        if response.status().is_success() {
            let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            Ok(task_uid(&body))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to delete Meilisearch documents from {index_uid}: {}",
                response.status()
            )))
        }
    }

    pub async fn search<T: DeserializeOwned>(
        &self,
        index_uid: &str,
        body: Value,
    ) -> Result<SearchResponse<T>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(SearchResponse {
                hits: Vec::new(),
                processing_time_ms: 0,
            });
        };
        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/search",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .json(&body)
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;

        if response.status().as_u16() == 404 {
            return Ok(SearchResponse {
                hits: Vec::new(),
                processing_time_ms: 0,
            });
        }

        if response.status().is_success() {
            response
                .json::<SearchResponse<T>>()
                .await
                .map_err(|e| ApiError::Upstream(e.to_string()))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to search Meilisearch index {index_uid}: {}",
                response.status()
            )))
        }
    }

    pub async fn fetch_documents_page(
        &self,
        index_uid: &str,
        offset: usize,
        limit: usize,
    ) -> Result<DocumentPage, ApiError> {
        let limit = limit.clamp(1, 1000);
        let Some(url) = &self.url else {
            return Ok(DocumentPage {
                results: Vec::new(),
                offset,
                limit,
                total: 0,
            });
        };
        let response = self
            .client
            .get(format!(
                "{}/indexes/{}/documents",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .query(&[("offset", offset), ("limit", limit)])
            .send()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;

        if response.status().as_u16() == 404 {
            return Ok(DocumentPage {
                results: Vec::new(),
                offset,
                limit,
                total: 0,
            });
        }
        if response.status().is_success() {
            response
                .json::<DocumentPage>()
                .await
                .map_err(|error| ApiError::Upstream(error.to_string()))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to list Meilisearch documents from {index_uid}: {}",
                response.status()
            )))
        }
    }

    /// Fetch one filtered, deterministically sorted document page.
    ///
    /// This deliberately uses Meilisearch's documents fetch endpoint instead
    /// of `/search`: repository hydration must be able to walk the complete
    /// filtered corpus without `maxTotalHits` truncation. The unfiltered GET
    /// reader above remains available to the tenant-scope migration tool.
    pub async fn fetch_filtered_documents_page(
        &self,
        index_uid: &str,
        filter: &str,
        sort: &[String],
        offset: usize,
        limit: usize,
    ) -> Result<DocumentPage, ApiError> {
        let Some(url) = &self.url else {
            return Ok(DocumentPage {
                results: Vec::new(),
                offset,
                limit,
                total: 0,
            });
        };
        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/documents/fetch",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .json(&json!({
                "offset": offset,
                "limit": limit,
                "filter": filter,
                "sort": sort,
            }))
            .send()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;

        if response.status().as_u16() == 404 {
            return Err(ApiError::Upstream(format!(
                "required Meilisearch index {index_uid} is missing during filtered scan"
            )));
        }
        if response.status().is_success() {
            response
                .json::<DocumentPage>()
                .await
                .map_err(|error| ApiError::Upstream(error.to_string()))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to fetch filtered Meilisearch documents from {index_uid}: {}",
                response.status()
            )))
        }
    }

    pub async fn search_value(&self, index_uid: &str, body: Value) -> Result<Value, ApiError> {
        let Some(url) = &self.url else {
            return Ok(json!({ "hits": [] }));
        };
        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/search",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .json(&body)
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;

        if response.status().as_u16() == 404 {
            return Ok(json!({ "hits": [] }));
        }
        if response.status().is_success() {
            response
                .json::<Value>()
                .await
                .map_err(|e| ApiError::Upstream(e.to_string()))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to search Meilisearch index {index_uid}: {}",
                response.status()
            )))
        }
    }

    pub async fn wait_for_task(&self, task_uid: &str) -> Result<(), ApiError> {
        let Some(url) = &self.url else {
            return Ok(());
        };
        for _ in 0..MEILI_TASK_WAIT_ATTEMPTS {
            let response = self
                .client
                .get(format!("{}/tasks/{}", url.trim_end_matches('/'), task_uid))
                .headers(self.headers()?)
                .send()
                .await
                .map_err(|e| ApiError::Upstream(e.to_string()))?;
            if !response.status().is_success() {
                return Err(ApiError::Upstream(format!(
                    "failed to read Meilisearch task {task_uid}: {}",
                    response.status()
                )));
            }
            let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            match body.get("status").and_then(Value::as_str) {
                Some("succeeded") => return Ok(()),
                Some("failed") | Some("canceled") => {
                    return Err(ApiError::Upstream(format!(
                        "Meilisearch task {task_uid} did not succeed: {body}"
                    )));
                }
                _ => sleep(Duration::from_millis(MEILI_TASK_WAIT_INTERVAL_MS)).await,
            }
        }
        Err(ApiError::Upstream(format!(
            "timed out waiting for Meilisearch task {task_uid}"
        )))
    }

    pub async fn wait_for_tasks(&self, task_uids: &[String]) -> Result<(), ApiError> {
        for task_uid in task_uids {
            self.wait_for_task(task_uid).await?;
        }
        Ok(())
    }

    pub async fn apply_settings(&self, uid: &str) -> Result<Option<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(None);
        };
        let settings = settings_for(uid);
        if self.managed_settings_match(uid, &settings).await? {
            return Ok(None);
        }
        let response = self
            .client
            .patch(format!(
                "{}/indexes/{}/settings",
                url.trim_end_matches('/'),
                uid
            ))
            .headers(self.headers()?)
            .json(&settings)
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;

        if response.status().is_success() {
            let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            Ok(task_uid(&body))
        } else {
            Err(ApiError::Upstream(format!(
                "failed to apply Meilisearch settings for {uid}: {}",
                response.status()
            )))
        }
    }

    async fn managed_settings_match(&self, uid: &str, desired: &Value) -> Result<bool, ApiError> {
        let Some(url) = &self.url else {
            return Ok(true);
        };
        let response = self
            .client
            .get(format!(
                "{}/indexes/{}/settings",
                url.trim_end_matches('/'),
                uid
            ))
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;
        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "failed to read Meilisearch settings for {uid}: {}",
                response.status()
            )));
        }
        let current = response
            .json::<Value>()
            .await
            .map_err(|e| ApiError::Upstream(e.to_string()))?;
        Ok(managed_settings_equal(&current, desired))
    }

    fn headers(&self) -> Result<HeaderMap, ApiError> {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(key) = &self.api_key {
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {key}"))
                    .map_err(|e| ApiError::Internal(e.to_string()))?,
            );
        }
        Ok(headers)
    }
}

pub fn task_uid(value: &Value) -> Option<String> {
    value
        .get("taskUid")
        .or_else(|| value.get("uid"))
        .and_then(Value::as_u64)
        .map(|uid| uid.to_string())
        .or_else(|| {
            value
                .get("taskUid")
                .or_else(|| value.get("uid"))
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
}

pub fn settings_for(uid: &str) -> Value {
    let mut settings = if uid.contains("context") {
        json!({
            "searchableAttributes": ["title", "body", "uri"],
            "filterableAttributes": ["id", "uri", "tenant_id", "owner_user_id", "index_uid", "index_kind", "layer", "ancestor_uris", "status", "privacy", "source_id", "revision_id", "node_kind", "retrieval_role", "retrieval_enabled", "parent_uri", "source_document_uri", "fragment_index", "block_type", "page_idx", "heading_level"],
            "sortableAttributes": ["updated_at", "layer"]
        })
    } else if uid == "rag_source_documents" {
        json!({
            "searchableAttributes": ["title", "source_id", "revision_id", "uri"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id", "source_kind", "source_id", "revision_id", "uri", "status", "retrieval_enabled"],
            "sortableAttributes": ["created_at", "updated_at"]
        })
    } else if uid == "rag_parse_artifacts" {
        json!({
            "searchableAttributes": ["source_document_uri", "source_id", "revision_id", "artifact_kind", "uri"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id", "source_document_uri", "source_id", "revision_id", "parser_provider", "parser_backend", "artifact_kind", "uri"],
            "sortableAttributes": ["created_at"]
        })
    } else if uid.contains("events") {
        json!({
            "searchableAttributes": ["text", "event_type", "entity_type", "entity_id", "tags"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id_hash", "event_type", "entity_type", "entity_id", "privacy", "occurred_at", "observed_at"],
            "sortableAttributes": ["occurred_at", "observed_at"]
        })
    } else if uid.contains("links") {
        json!({
            "searchableAttributes": ["source_uri", "target_uri", "source_title", "target_title", "relation", "rationale", "evidence_text", "tags"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id", "source_uri", "target_uri", "relation", "status", "created_by"],
            "sortableAttributes": ["created_at", "updated_at"]
        })
    } else if uid == "rag_harness_components" {
        json!({
            "searchableAttributes": ["id", "display_name", "description", "component_id", "manifest_id", "files", "created_by"],
            "filterableAttributes": ["id", "tenant_id", "doc_kind", "component_id", "status", "component_kind", "manifest_id", "created_by"],
            "sortableAttributes": ["logical_id", "created_at", "updated_at", "iteration"]
        })
    } else if uid == "rag_harness_changes" {
        json!({
            "searchableAttributes": ["id", "component_id", "failure_pattern", "root_cause", "targeted_fix", "predicted_fixes", "risk_cases", "why_this_component", "created_by"],
            "filterableAttributes": ["id", "tenant_id", "component_id", "type", "status", "created_by"],
            "sortableAttributes": ["created_at", "iteration"]
        })
    } else if uid == "rag_harness_verdicts" {
        json!({
            "searchableAttributes": ["id", "change_id", "verdict", "predicted_fixes_confirmed", "risk_cases_regressed", "created_by"],
            "filterableAttributes": ["id", "tenant_id", "change_id", "eval_run_id", "verdict", "created_by"],
            "sortableAttributes": ["created_at"]
        })
    } else if uid == "rag_ingest_tasks" {
        json!({
            "searchableAttributes": ["task_id", "source_id", "revision_id", "parser_provider", "parser_backend", "error"],
            "filterableAttributes": ["id", "task_id", "tenant_id", "owner_user_id", "source_id", "revision_id", "state", "parser_provider", "parser_backend"],
            "sortableAttributes": ["created_at", "updated_at", "completed_at"]
        })
    } else if uid == "rag_ingest_results" {
        json!({
            "searchableAttributes": ["task_id", "source_id", "revision_id", "source_document_uri", "fragment_uris", "context_uris"],
            "filterableAttributes": ["id", "task_id", "tenant_id", "owner_user_id", "source_id", "revision_id"],
            "sortableAttributes": ["created_at", "updated_at"]
        })
    } else if uid == "rag_eval_cases" {
        json!({
            "searchableAttributes": ["id", "question", "expected_context_uris", "expected_source_document_uris", "expected_answer_contains", "tags"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id", "tags"],
            "sortableAttributes": ["created_at"]
        })
    } else if uid == "rag_eval_runs" {
        json!({
            "searchableAttributes": ["id", "change_id", "case_ids", "status", "created_by"],
            "filterableAttributes": ["id", "tenant_id", "change_id", "status", "created_by"],
            "sortableAttributes": ["created_at", "completed_at"]
        })
    } else if uid == "rag_eval_case_results" {
        json!({
            "searchableAttributes": ["id", "run_id", "case_id", "status", "question", "answer", "failures", "guard_failures"],
            "filterableAttributes": ["id", "run_id", "case_id", "owner_user_id", "status", "failures", "guard_failures"],
            "sortableAttributes": ["created_at", "latency_ms"]
        })
    } else if uid == "rag_eval_overviews" {
        json!({
            "searchableAttributes": ["run_id", "status", "overview_markdown", "suggested_target_component"],
            "filterableAttributes": ["id", "run_id", "status", "suggested_target_component"],
            "sortableAttributes": ["generated_at"]
        })
    } else {
        json!({
            "searchableAttributes": ["title", "statement", "text", "content", "id"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id", "snapshot_id", "dataset_key", "status", "privacy", "source_id", "revision_id"],
            "sortableAttributes": ["created_at", "updated_at", "occurred_at"]
        })
    };
    for required in ["id", "logical_id", "tenant_id"] {
        ensure_filterable_attribute(&mut settings, required);
    }
    ensure_searchable_attribute(&mut settings, "logical_id");
    ensure_sortable_attribute(&mut settings, "id");
    settings
}

fn managed_settings_equal(current: &Value, desired: &Value) -> bool {
    current.get("searchableAttributes") == desired.get("searchableAttributes")
        && unordered_string_arrays_equal(
            current.get("filterableAttributes"),
            desired.get("filterableAttributes"),
        )
        && unordered_string_arrays_equal(
            current.get("sortableAttributes"),
            desired.get("sortableAttributes"),
        )
}

fn unordered_string_arrays_equal(left: Option<&Value>, right: Option<&Value>) -> bool {
    let (Some(left), Some(right)) = (
        left.and_then(Value::as_array),
        right.and_then(Value::as_array),
    ) else {
        return false;
    };
    if left.len() != right.len() {
        return false;
    }
    let Some(left_set) = left
        .iter()
        .map(Value::as_str)
        .collect::<Option<BTreeSet<_>>>()
    else {
        return false;
    };
    let Some(right_set) = right
        .iter()
        .map(Value::as_str)
        .collect::<Option<BTreeSet<_>>>()
    else {
        return false;
    };
    left_set.len() == left.len() && right_set.len() == right.len() && left_set == right_set
}

fn ensure_filterable_attribute(settings: &mut Value, attribute: &str) {
    let Some(filterable) = settings
        .get_mut("filterableAttributes")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if !filterable.iter().any(|value| value == attribute) {
        filterable.push(Value::String(attribute.to_string()));
    }
}

fn ensure_searchable_attribute(settings: &mut Value, attribute: &str) {
    let Some(searchable) = settings
        .get_mut("searchableAttributes")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if !searchable.iter().any(|value| value == attribute) {
        searchable.push(Value::String(attribute.to_string()));
    }
}

fn ensure_sortable_attribute(settings: &mut Value, attribute: &str) {
    let Some(sortable) = settings
        .get_mut("sortableAttributes")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if !sortable.iter().any(|value| value == attribute) {
        sortable.push(Value::String(attribute.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn managed_settings_treat_only_set_valued_attributes_as_unordered() {
        let desired = json!({
            "searchableAttributes": ["title", "body"],
            "filterableAttributes": ["tenant_id", "status"],
            "sortableAttributes": ["created_at", "id"]
        });
        let canonicalized = json!({
            "searchableAttributes": ["title", "body"],
            "filterableAttributes": ["status", "tenant_id"],
            "sortableAttributes": ["id", "created_at"]
        });
        assert!(managed_settings_equal(&canonicalized, &desired));

        let reordered_searchable = json!({
            "searchableAttributes": ["body", "title"],
            "filterableAttributes": ["status", "tenant_id"],
            "sortableAttributes": ["id", "created_at"]
        });
        assert!(!managed_settings_equal(&reordered_searchable, &desired));

        let duplicated_filter = json!({
            "searchableAttributes": ["title", "body"],
            "filterableAttributes": ["tenant_id", "tenant_id"],
            "sortableAttributes": ["id", "created_at"]
        });
        assert!(!managed_settings_equal(&duplicated_filter, &desired));
    }

    #[test]
    fn every_fixed_index_can_filter_by_tenant_and_logical_identity() {
        for uid in FIXED_INDEXES {
            let settings = settings_for(uid);
            let filterable = settings["filterableAttributes"]
                .as_array()
                .expect("filterable attributes");
            for required in ["id", "logical_id", "tenant_id"] {
                assert!(
                    filterable.iter().any(|value| value == required),
                    "{uid} is missing {required}"
                );
            }
            let searchable = settings["searchableAttributes"]
                .as_array()
                .expect("searchable attributes");
            assert!(
                searchable.iter().any(|value| value == "logical_id"),
                "{uid} is missing searchable logical_id"
            );
        }
    }

    #[test]
    fn every_fixed_index_supports_a_stable_physical_id_tie_breaker() {
        for uid in FIXED_INDEXES {
            let settings = settings_for(uid);
            let sortable = settings["sortableAttributes"]
                .as_array()
                .expect("sortable attributes");
            assert!(
                sortable.iter().any(|value| value == "id"),
                "{uid} is missing stable id sorting"
            );
        }
    }

    #[test]
    fn harness_components_sort_by_public_logical_identity_and_physical_tie_breaker() {
        let settings = settings_for("rag_harness_components");
        let sortable = settings["sortableAttributes"]
            .as_array()
            .expect("sortable attributes");
        assert!(sortable.iter().any(|value| value == "logical_id"));
        assert!(sortable.iter().any(|value| value == "id"));
    }
}
