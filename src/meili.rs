use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;

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

        let mut tasks = Vec::new();
        for uid in FIXED_INDEXES {
            if reset {
                let response = self
                    .client
                    .delete(format!("{}/indexes/{}", url.trim_end_matches('/'), uid))
                    .headers(self.headers()?)
                    .send()
                    .await
                    .map_err(|e| ApiError::Upstream(e.to_string()))?;
                if !response.status().is_success() && response.status().as_u16() != 404 {
                    return Err(ApiError::Upstream(format!(
                        "failed to delete Meilisearch index {uid}: {}",
                        response.status()
                    )));
                }
            }

            for task_uid in self.ensure_index(uid, "id", true).await? {
                tasks.push(json!({ "taskUid": task_uid }));
            }
        }

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
                return Err(ApiError::Upstream(format!(
                    "failed to create Meilisearch index {uid}: {}",
                    response.status()
                )));
            }
            let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            if let Some(uid) = task_uid(&body) {
                task_uids.push(uid);
            }
        }

        if apply_settings {
            if let Some(uid) = self.apply_settings(uid).await? {
                task_uids.push(uid);
            }
        }
        Ok(task_uids)
    }

    async fn index_exists(&self, uid: &str) -> Result<bool, ApiError> {
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
    if uid.contains("context") {
        json!({
            "searchableAttributes": ["title", "body", "uri"],
            "filterableAttributes": ["id", "uri", "tenant_id", "owner_user_id", "layer", "ancestor_uris", "status", "privacy", "source_id", "revision_id", "node_kind", "retrieval_role", "retrieval_enabled", "parent_uri", "source_document_uri", "fragment_index", "block_type", "page_idx", "heading_level"],
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
    } else {
        json!({
            "searchableAttributes": ["title", "statement", "text", "content", "id"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id", "snapshot_id", "dataset_key", "status", "privacy", "source_id", "revision_id"],
            "sortableAttributes": ["created_at", "updated_at", "occurred_at"]
        })
    }
}
