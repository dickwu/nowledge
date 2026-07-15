use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::Instant,
};

use tokio::time::{sleep, timeout, Duration};

use crate::{config::Config, error::ApiError};

pub const FIXED_INDEXES: &[&str] = &[
    "rag_company_context",
    "rag_state_items",
    "rag_user_event_indexes",
    "rag_operations",
    "rag_audit_records",
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
const DURABLE_FIXED_INDEXES: [&str; 2] = ["rag_operations", "rag_audit_records"];

#[derive(Debug, Clone, PartialEq, Eq)]
struct DurableIndexIdentity {
    uid: String,
    primary_key: String,
    created_at: String,
}

#[derive(Clone)]
pub struct MeiliAdmin {
    url: Option<String>,
    api_key: Option<String>,
    client: reqwest::Client,
    durable_index_identities: Arc<RwLock<BTreeMap<String, DurableIndexIdentity>>>,
    require_captured_durable_identities: bool,
    allow_initial_provision: bool,
    expected_durable_created_at: BTreeMap<String, String>,
}

impl std::fmt::Debug for MeiliAdmin {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MeiliAdmin")
            .field("configured", &self.url.is_some())
            .field("api_key", &self.api_key.as_ref().map(|_| "<redacted>"))
            .finish_non_exhaustive()
    }
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
        Self::with_key(
            config,
            config.meili_api_key.clone(),
            Arc::new(RwLock::new(BTreeMap::new())),
        )
    }

    pub fn from_admin_config(config: &Config) -> Self {
        Self::with_key(
            config,
            Self::admin_api_key(config),
            Arc::new(RwLock::new(BTreeMap::new())),
        )
    }

    pub(crate) fn pair_from_config(config: &Config) -> (Self, Self) {
        let identities = Arc::new(RwLock::new(BTreeMap::new()));
        let runtime = Self::with_key(config, config.meili_api_key.clone(), identities.clone());
        let index_admin = Self::with_key(config, Self::admin_api_key(config), identities);
        (runtime, index_admin)
    }

    fn admin_api_key(config: &Config) -> Option<String> {
        match (
            config.meili_admin_api_key.as_ref(),
            config.run_mode.as_str(),
        ) {
            (Some(admin_key), _) => Some(admin_key.clone()),
            (None, "development" | "test") => config.meili_api_key.clone(),
            (None, _) => None,
        }
    }

    fn with_key(
        config: &Config,
        api_key: Option<String>,
        durable_index_identities: Arc<RwLock<BTreeMap<String, DurableIndexIdentity>>>,
    ) -> Self {
        let expected_durable_created_at = [
            (
                "rag_operations",
                config.meili_operations_index_created_at.as_ref(),
            ),
            (
                "rag_audit_records",
                config.meili_audit_index_created_at.as_ref(),
            ),
        ]
        .into_iter()
        .filter_map(|(uid, created_at)| {
            created_at.map(|created_at| (uid.to_string(), created_at.clone()))
        })
        .collect();
        Self {
            url: config.meili_url.clone(),
            api_key,
            client: reqwest::Client::new(),
            durable_index_identities,
            require_captured_durable_identities: config.run_mode == "production",
            allow_initial_provision: config.meili_allow_initial_provision,
            expected_durable_created_at,
        }
    }

    /// Reconcile the managed index set without mutating a fully provisioned
    /// backend until its durable deployment identities have been validated.
    pub async fn prepare_for_startup(&self) -> Result<BootstrapResult, ApiError> {
        self.preflight_durable_index_pins().await?;
        let bootstrap = self.bootstrap(false).await?;
        self.capture_durable_index_identities().await?;
        Ok(bootstrap)
    }

    async fn preflight_durable_index_pins(&self) -> Result<(), ApiError> {
        if self.url.is_none() {
            return Ok(());
        }

        // Production defaults to requiring durable identity pins. The sole
        // exception is an explicitly approved first provision after every
        // managed index has been observed missing through read-only requests.
        if self.allow_initial_provision
            && self.expected_durable_created_at.is_empty()
            && self.meili_instance_is_empty().await?
        {
            return Ok(());
        }

        if !self.require_captured_durable_identities && self.expected_durable_created_at.is_empty()
        {
            return Ok(());
        }

        for uid in DURABLE_FIXED_INDEXES {
            let Some(expected_created_at) = self.expected_durable_created_at.get(uid) else {
                if self.require_captured_durable_identities {
                    return Err(ApiError::Upstream(format!(
                        "Meilisearch durable index {uid} deployment identity pin is missing; refusing startup reconciliation"
                    )));
                }
                continue;
            };
            let identity = self.read_durable_index_identity(uid).await?;
            if &identity.created_at != expected_created_at {
                return Err(ApiError::Upstream(format!(
                    "Meilisearch durable index {uid} does not match the pinned deployment identity"
                )));
            }
        }
        Ok(())
    }

    async fn meili_instance_is_empty(&self) -> Result<bool, ApiError> {
        let Some(url) = &self.url else {
            return Ok(true);
        };
        let response = self
            .client
            .get(format!("{}/indexes", url.trim_end_matches('/')))
            .headers(self.headers()?)
            .query(&[("limit", 1_usize)])
            .send()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "failed to prove that the Meilisearch instance is empty: {}",
                response.status()
            )));
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        let total = body.get("total").and_then(Value::as_u64).ok_or_else(|| {
            ApiError::Upstream(
                "Meilisearch index listing omitted a valid total; refusing initial provision"
                    .to_string(),
            )
        })?;
        let results = body
            .get("results")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                ApiError::Upstream(
                    "Meilisearch index listing omitted valid results; refusing initial provision"
                        .to_string(),
                )
            })?;
        Ok(total == 0 && results.is_empty())
    }

    pub async fn capture_durable_index_identities(&self) -> Result<(), ApiError> {
        if self.url.is_none() {
            return Ok(());
        }
        let mut captured = BTreeMap::new();
        for uid in DURABLE_FIXED_INDEXES {
            let identity = self.read_stable_durable_index_identity(uid).await?;
            if let Some(expected_created_at) = self.expected_durable_created_at.get(uid) {
                if &identity.created_at != expected_created_at {
                    return Err(ApiError::Upstream(format!(
                        "Meilisearch durable index {uid} does not match the pinned deployment identity"
                    )));
                }
            }
            captured.insert(uid.to_string(), identity);
        }
        *self
            .durable_index_identities
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = captured;
        Ok(())
    }

    pub async fn verify_durable_index_for_write(&self, uid: &str) -> Result<(), ApiError> {
        if !DURABLE_FIXED_INDEXES.contains(&uid) || self.url.is_none() {
            return Ok(());
        }
        let current = self.read_stable_durable_index_identity(uid).await?;
        let mut identities = self
            .durable_index_identities
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match identities.get(uid) {
            Some(expected) if expected != &current => Err(ApiError::Upstream(format!(
                "Meilisearch durable index {uid} generation changed; refusing persistence"
            ))),
            Some(_) => Ok(()),
            None if !self.require_captured_durable_identities => {
                identities.insert(uid.to_string(), current);
                Ok(())
            }
            None => Err(ApiError::Upstream(format!(
                "Meilisearch durable index {uid} startup identity was not captured; refusing persistence"
            ))),
        }
    }

    async fn verify_captured_durable_index_identities(&self) -> Result<(), ApiError> {
        let expected = self
            .durable_index_identities
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if self.require_captured_durable_identities && expected.len() != DURABLE_FIXED_INDEXES.len()
        {
            return Err(ApiError::Upstream(
                "Meilisearch durable index startup identities are incomplete".to_string(),
            ));
        }
        for (uid, expected_identity) in expected {
            let current = self.read_stable_durable_index_identity(&uid).await?;
            if current != expected_identity {
                return Err(ApiError::Upstream(format!(
                    "Meilisearch durable index {uid} continuity check failed"
                )));
            }
        }
        Ok(())
    }

    async fn read_stable_durable_index_identity(
        &self,
        uid: &str,
    ) -> Result<DurableIndexIdentity, ApiError> {
        let before = self.read_durable_index_identity(uid).await?;
        if !self.index_settings_match(uid).await? {
            return Err(ApiError::Upstream(format!(
                "Meilisearch durable index {uid} does not match managed settings"
            )));
        }
        let after = self.read_durable_index_identity(uid).await?;
        if before != after {
            return Err(ApiError::Upstream(format!(
                "Meilisearch durable index {uid} changed during continuity validation"
            )));
        }
        Ok(after)
    }

    async fn read_durable_index_identity(
        &self,
        uid: &str,
    ) -> Result<DurableIndexIdentity, ApiError> {
        let Some(url) = &self.url else {
            return Err(ApiError::Upstream(format!(
                "Meilisearch durable index {uid} is unavailable"
            )));
        };
        let response = self
            .client
            .get(format!("{}/indexes/{uid}", url.trim_end_matches('/')))
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "Meilisearch durable index {uid} is unavailable; refusing persistence"
            )));
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        let returned_uid = body.get("uid").and_then(Value::as_str);
        let primary_key = body.get("primaryKey").and_then(Value::as_str);
        let created_at = body.get("createdAt").and_then(Value::as_str);
        if returned_uid != Some(uid) || primary_key != Some("id") || created_at.is_none() {
            return Err(ApiError::Upstream(format!(
                "Meilisearch durable index {uid} returned invalid identity metadata"
            )));
        }
        let created_at = created_at.ok_or_else(|| {
            ApiError::Upstream(format!(
                "Meilisearch durable index {uid} returned invalid identity metadata"
            ))
        })?;
        Ok(DurableIndexIdentity {
            uid: uid.to_string(),
            primary_key: "id".to_string(),
            created_at: created_at.to_string(),
        })
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
                    if matches!(*uid, "rag_operations" | "rag_audit_records") {
                        self.require_index_primary_key(uid, "id").await?;
                    }
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
            if existing.is_empty() && !self.allow_initial_provision {
                return Err(ApiError::Upstream(
                    "all managed Meilisearch indexes are missing; refusing implicit initial provision"
                        .to_string(),
                ));
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
                    let body = response
                        .json::<Value>()
                        .await
                        .map_err(|error| ApiError::Upstream(error.to_string()))?;
                    let task_uid =
                        required_task_uid(&body, &format!("delete Meilisearch index {uid}"))?;
                    self.wait_for_task(&task_uid).await?;
                    task_uids.push(task_uid);
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
        let health_check = async {
            match request.send().await {
                Ok(response) if response.status().is_success() => {
                    match self.verify_captured_durable_index_identities().await {
                        Ok(()) => json!({
                            "status": "ok",
                            "healthy": true,
                            "configured": true,
                            "latency_ms": started.elapsed().as_millis() as u64
                        }),
                        Err(_) => json!({
                            "status": "unhealthy",
                            "healthy": false,
                            "configured": true,
                            "error": "Meilisearch durable index continuity check failed",
                            "latency_ms": started.elapsed().as_millis() as u64
                        }),
                    }
                }
                Ok(response) => json!({
                    "status": "unhealthy",
                    "healthy": false,
                    "configured": true,
                    "http_status": response.status().as_u16(),
                    "latency_ms": started.elapsed().as_millis() as u64
                }),
                Err(err) => json!({
                    "status": "unhealthy",
                    "healthy": false,
                    "configured": true,
                    "error": err.to_string(),
                    "latency_ms": started.elapsed().as_millis() as u64
                }),
            }
        };
        match timeout(Duration::from_secs(2), health_check).await {
            Ok(status) => status,
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
                let body = response
                    .json::<Value>()
                    .await
                    .map_err(|error| ApiError::Upstream(error.to_string()))?;
                let task_uid =
                    required_task_uid(&body, &format!("create Meilisearch index {uid}"))?;
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

        // The index may have been created by another actor between the
        // existence check and the create task. Never apply managed settings
        // until the resulting index proves it has the expected identity.
        self.require_index_primary_key(uid, primary_key).await?;

        if apply_settings {
            if let Some(uid) = self.apply_settings(uid).await? {
                self.wait_for_task(&uid).await?;
                task_uids.push(uid);
            }
        }
        Ok(task_uids)
    }

    /// Create an index only when this request is the sole creator.
    ///
    /// Migration plans that observed a missing durable index use this path so
    /// a concurrently-created same-UID index is never adopted or reconciled.
    /// Unlike [`Self::ensure_index`], any rejected or failed create task is a
    /// hard error, even when an index with the requested UID is visible later.
    pub async fn create_index_strict(
        &self,
        uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(Vec::new());
        };

        let response = self
            .client
            .post(format!("{}/indexes", url.trim_end_matches('/')))
            .headers(self.headers()?)
            .json(&json!({ "uid": uid, "primaryKey": primary_key }))
            .send()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "failed to strictly create Meilisearch index {uid}: {}; refusing to adopt any concurrently-created index",
                response.status()
            )));
        }

        let body = response
            .json::<Value>()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        let create_task_uid =
            required_task_uid(&body, &format!("strictly create Meilisearch index {uid}"))?;
        self.wait_for_task(&create_task_uid).await.map_err(|error| {
            ApiError::Upstream(format!(
                "strict creation of Meilisearch index {uid} did not succeed; refusing to adopt any concurrently-created index: {error}"
            ))
        })?;
        let mut task_uids = vec![create_task_uid];

        self.require_index_primary_key(uid, primary_key).await?;
        let created_at = self.index_created_at(uid).await?.ok_or_else(|| {
            ApiError::Upstream(format!(
                "strictly-created Meilisearch index {uid} returned no createdAt generation"
            ))
        })?;

        if apply_settings {
            if let Some(settings_task_uid) = self.apply_settings(uid).await? {
                self.wait_for_task(&settings_task_uid).await?;
                task_uids.push(settings_task_uid);
            }
        }

        self.require_index_primary_key(uid, primary_key).await?;
        if self.index_created_at(uid).await?.as_deref() != Some(created_at.as_str()) {
            return Err(ApiError::Upstream(format!(
                "Meilisearch index {uid} generation changed during strict creation"
            )));
        }
        Ok(task_uids)
    }

    /// Reconcile an existing index only after confirming its primary key.
    /// Unlike [`Self::ensure_index`], this path never recreates a disappeared
    /// index and is therefore suitable for non-destructive migration retries.
    pub async fn reconcile_existing_index_with_primary_key(
        &self,
        uid: &str,
        primary_key: &str,
        apply_settings: bool,
    ) -> Result<Vec<String>, ApiError> {
        if !self.index_exists(uid).await? {
            return Err(ApiError::Upstream(format!(
                "registered Meilisearch index {uid} is missing; refusing empty recreation"
            )));
        }
        self.require_index_primary_key(uid, primary_key).await?;
        self.reconcile_existing_index(uid, apply_settings).await
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

    /// Read the primary key declared by an existing Meilisearch index.
    ///
    /// A missing index and an index with no primary key both return `None`;
    /// callers that must distinguish them should check [`Self::index_exists`]
    /// first. Malformed successful responses fail closed.
    pub async fn index_primary_key(&self, uid: &str) -> Result<Option<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(None);
        };
        let response = self
            .client
            .get(format!("{}/indexes/{}", url.trim_end_matches('/'), uid))
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "failed to inspect Meilisearch primary key for {uid}: {}",
                response.status()
            )));
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        match body.get("primaryKey") {
            Some(Value::String(primary_key)) => Ok(Some(primary_key.clone())),
            Some(Value::Null) => Ok(None),
            _ => Err(ApiError::Upstream(format!(
                "Meilisearch index {uid} returned an invalid primaryKey field"
            ))),
        }
    }

    /// Return the immutable Meilisearch creation timestamp used to identify
    /// one concrete generation of a same-UID index.
    pub async fn index_created_at(&self, uid: &str) -> Result<Option<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(None);
        };
        let response = self
            .client
            .get(format!("{}/indexes/{}", url.trim_end_matches('/'), uid))
            .headers(self.headers()?)
            .send()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(ApiError::Upstream(format!(
                "failed to inspect Meilisearch createdAt for {uid}: {}",
                response.status()
            )));
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|error| ApiError::Upstream(error.to_string()))?;
        if body.get("uid").and_then(Value::as_str) != Some(uid) {
            return Err(ApiError::Upstream(format!(
                "Meilisearch index {uid} returned mismatched identity metadata"
            )));
        }
        match body.get("createdAt") {
            Some(Value::String(created_at)) if !created_at.trim().is_empty() => {
                Ok(Some(created_at.clone()))
            }
            _ => Err(ApiError::Upstream(format!(
                "Meilisearch index {uid} returned an invalid createdAt field"
            ))),
        }
    }

    pub async fn require_index_primary_key(
        &self,
        uid: &str,
        expected: &str,
    ) -> Result<(), ApiError> {
        let actual = self.index_primary_key(uid).await?;
        if actual.as_deref() == Some(expected) {
            return Ok(());
        }
        Err(ApiError::Upstream(format!(
            "Meilisearch index {uid} has incompatible primary key {}; expected {expected}; refusing non-destructive reconciliation",
            actual.as_deref().unwrap_or("<unset>")
        )))
    }

    /// Read-only verification that an existing index has the exact managed
    /// searchable, filterable, and sortable settings defined by this build.
    pub async fn index_settings_match(&self, uid: &str) -> Result<bool, ApiError> {
        if !self.index_exists(uid).await? {
            return Ok(false);
        }
        self.managed_settings_match(uid, &settings_for(uid)).await
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
            let body = response
                .json::<Value>()
                .await
                .map_err(|error| ApiError::Upstream(error.to_string()))?;
            Ok(Some(required_task_uid(
                &body,
                &format!("add documents to Meilisearch index {index_uid}"),
            )?))
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
            let body = response
                .json::<Value>()
                .await
                .map_err(|error| ApiError::Upstream(error.to_string()))?;
            Ok(Some(required_task_uid(
                &body,
                &format!("delete documents from Meilisearch index {index_uid}"),
            )?))
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
            let body = response
                .json::<Value>()
                .await
                .map_err(|error| ApiError::Upstream(error.to_string()))?;
            Ok(Some(required_task_uid(
                &body,
                &format!("delete documents from Meilisearch index {index_uid}"),
            )?))
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
        self.fetch_projected_documents_page(index_uid, filter, sort, offset, limit, &[])
            .await
    }

    /// Fetch one filtered document page while asking Meilisearch to return
    /// only the fields needed by the caller. An empty projection preserves the
    /// full-document behavior used by repository hydration.
    pub async fn fetch_projected_documents_page(
        &self,
        index_uid: &str,
        filter: &str,
        sort: &[String],
        offset: usize,
        limit: usize,
        fields: &[&str],
    ) -> Result<DocumentPage, ApiError> {
        let Some(url) = &self.url else {
            return Ok(DocumentPage {
                results: Vec::new(),
                offset,
                limit,
                total: 0,
            });
        };
        let mut request = json!({
            "offset": offset,
            "limit": limit,
            "filter": filter,
            "sort": sort,
        });
        if !fields.is_empty() {
            request["fields"] = json!(fields);
        }
        let response = self
            .client
            .post(format!(
                "{}/indexes/{}/documents/fetch",
                url.trim_end_matches('/'),
                index_uid
            ))
            .headers(self.headers()?)
            .json(&request)
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
            let body = response
                .json::<Value>()
                .await
                .map_err(|error| ApiError::Upstream(error.to_string()))?;
            Ok(Some(required_task_uid(
                &body,
                &format!("apply settings for Meilisearch index {uid}"),
            )?))
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

fn required_task_uid(value: &Value, action: &str) -> Result<String, ApiError> {
    task_uid(value).ok_or_else(|| {
        ApiError::Upstream(format!(
            "Meilisearch accepted {action} without returning a task UID"
        ))
    })
}

pub fn settings_for(uid: &str) -> Value {
    let mut settings = if uid == "rag_audit_records" {
        json!({
            "searchableAttributes": ["action", "reason_code", "error_kind"],
            "filterableAttributes": ["id", "logical_id", "tenant_id", "request_id", "principal_scope", "principal_owner_user_id_hash", "resource_id_hash", "action", "reason_code", "outcome", "error_kind", "operation_id", "occurred_at", "updated_at"],
            "sortableAttributes": ["occurred_at", "updated_at", "id"]
        })
    } else if uid == "rag_operations" {
        json!({
            "searchableAttributes": ["operation_kind", "idempotency_key_hash", "last_error_category", "last_error_fingerprint"],
            "filterableAttributes": ["id", "tenant_id", "operation_kind", "actor_scope", "status", "indexing_state", "idempotency_key_hash"],
            "sortableAttributes": ["created_at", "updated_at", "completed_at"]
        })
    } else if uid.contains("context") {
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
    use axum::{
        extract::Request,
        http::StatusCode,
        response::{IntoResponse, Response},
        Router,
    };

    use super::*;

    async fn healthy_then_stalled_meili(request: Request) -> Response {
        if request.uri().path() == "/health" {
            return StatusCode::OK.into_response();
        }
        std::future::pending::<Response>().await
    }

    #[tokio::test]
    async fn health_deadline_covers_durable_continuity_requests() {
        let app = Router::new().fallback(healthy_then_stalled_meili);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut config = Config::test();
        config.meili_url = Some(format!("http://{address}"));
        let admin = MeiliAdmin::from_config(&config);
        admin.durable_index_identities.write().unwrap().insert(
            "rag_operations".to_string(),
            DurableIndexIdentity {
                uid: "rag_operations".to_string(),
                primary_key: "id".to_string(),
                created_at: "2026-07-15T00:00:00Z".to_string(),
            },
        );

        let status = tokio::time::timeout(Duration::from_secs(3), admin.health_status())
            .await
            .expect("health status exceeded its full-operation deadline");
        assert_eq!(status["healthy"], false, "{status}");
        assert_eq!(
            status["error"], "Meilisearch health check timed out",
            "{status}"
        );
    }

    #[test]
    fn debug_output_never_exposes_meilisearch_credentials() {
        let mut config = Config::test();
        config.meili_url = Some("https://meili.internal".to_string());
        config.meili_api_key = Some("runtime-super-secret".to_string());
        let rendered = format!("{:?}", MeiliAdmin::from_config(&config));

        assert!(rendered.contains("configured: true"), "{rendered}");
        assert!(rendered.contains("<redacted>"), "{rendered}");
        assert!(!rendered.contains("runtime-super-secret"), "{rendered}");
        assert!(!rendered.contains("meili.internal"), "{rendered}");
    }

    #[test]
    fn production_admin_clients_never_fall_back_to_the_runtime_key() {
        let mut config = Config::test();
        config.run_mode = "production".to_string();
        config.meili_api_key = Some("runtime-only-key".to_string());

        let standalone = MeiliAdmin::from_admin_config(&config);
        let (_, paired) = MeiliAdmin::pair_from_config(&config);

        assert_eq!(standalone.api_key, None);
        assert_eq!(paired.api_key, None);
    }

    #[test]
    fn development_admin_clients_preserve_the_runtime_key_fallback() {
        let mut config = Config::test();
        config.run_mode = "development".to_string();
        config.meili_api_key = Some("development-key".to_string());

        let standalone = MeiliAdmin::from_admin_config(&config);
        let (_, paired) = MeiliAdmin::pair_from_config(&config);

        assert_eq!(standalone.api_key.as_deref(), Some("development-key"));
        assert_eq!(paired.api_key.as_deref(), Some("development-key"));
    }

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

    #[test]
    fn operations_support_reconciliation_filters_and_stable_pagination() {
        let settings = settings_for("rag_operations");
        let filterable = settings["filterableAttributes"]
            .as_array()
            .expect("filterable attributes");
        for required in [
            "tenant_id",
            "operation_kind",
            "actor_scope",
            "status",
            "indexing_state",
            "idempotency_key_hash",
        ] {
            assert!(
                filterable.iter().any(|value| value == required),
                "rag_operations is missing {required}"
            );
        }
        let sortable = settings["sortableAttributes"]
            .as_array()
            .expect("sortable attributes");
        for required in ["created_at", "updated_at", "completed_at", "id"] {
            assert!(sortable.iter().any(|value| value == required));
        }
    }

    #[test]
    fn audit_records_support_bounded_operational_filters_and_timeline_ordering() {
        let settings = settings_for("rag_audit_records");
        let filterable = settings["filterableAttributes"]
            .as_array()
            .expect("filterable attributes");
        for required in [
            "tenant_id",
            "request_id",
            "principal_scope",
            "resource_id_hash",
            "action",
            "reason_code",
            "outcome",
            "error_kind",
            "operation_id",
            "occurred_at",
            "updated_at",
        ] {
            assert!(
                filterable.iter().any(|value| value == required),
                "rag_audit_records is missing {required}"
            );
        }
        let sortable = settings["sortableAttributes"]
            .as_array()
            .expect("sortable attributes");
        for required in ["occurred_at", "updated_at", "id"] {
            assert!(sortable.iter().any(|value| value == required));
        }
    }
}
