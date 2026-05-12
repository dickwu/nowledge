use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{config::Config, error::ApiError};

pub const FIXED_INDEXES: &[&str] = &[
    "rag_company_context",
    "rag_state_items",
    "rag_user_event_indexes",
    "rag_sources",
    "rag_source_revisions",
    "rag_doc_candidates",
    "rag_structured_datasets",
    "rag_structured_snapshots",
    "rag_structured_rows",
    "rag_structured_summaries",
    "rag_insights",
    "rag_sessions",
    "rag_memory_diffs",
    "rag_feedback",
    "rag_traces",
];

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

            let response = self
                .client
                .post(format!("{}/indexes", url.trim_end_matches('/')))
                .headers(self.headers()?)
                .json(&json!({ "uid": uid, "primaryKey": "id" }))
                .send()
                .await
                .map_err(|e| ApiError::Upstream(e.to_string()))?;

            if response.status().is_success() || response.status().as_u16() == 409 {
                let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
                tasks.push(body);
                self.apply_settings(uid).await?;
            } else {
                return Err(ApiError::Upstream(format!(
                    "failed to create Meilisearch index {uid}: {}",
                    response.status()
                )));
            }
        }

        Ok(BootstrapResult {
            indexes: FIXED_INDEXES.iter().map(|s| s.to_string()).collect(),
            tasks,
            dry_run: false,
        })
    }

    async fn apply_settings(&self, uid: &str) -> Result<(), ApiError> {
        let Some(url) = &self.url else {
            return Ok(());
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
            Ok(())
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

pub fn settings_for(uid: &str) -> Value {
    if uid.contains("context") {
        json!({
            "searchableAttributes": ["title", "body", "uri"],
            "filterableAttributes": ["tenant_id", "owner_user_id", "layer", "ancestor_uris", "status", "privacy", "source_id", "revision_id"],
            "sortableAttributes": ["updated_at", "layer"]
        })
    } else if uid.contains("events") {
        json!({
            "searchableAttributes": ["text", "event_type", "entity_type", "entity_id", "tags"],
            "filterableAttributes": ["tenant_id", "owner_user_id_hash", "event_type", "entity_type", "entity_id", "privacy", "occurred_at", "observed_at"],
            "sortableAttributes": ["occurred_at", "observed_at"]
        })
    } else {
        json!({
            "searchableAttributes": ["title", "statement", "text", "content", "id"],
            "filterableAttributes": ["tenant_id", "owner_user_id", "status", "privacy", "source_id", "revision_id"],
            "sortableAttributes": ["created_at", "updated_at", "occurred_at"]
        })
    }
}
