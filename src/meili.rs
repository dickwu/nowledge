use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::Instant,
};

use tokio::time::{sleep, timeout, Duration};

use crate::{config::Config, error::ApiError, util::ENGLISH_STOP_WORDS};

/// Derived, filter-only field on context documents: the distinct ASCII-
/// lowercased character trigrams of every `section_path` part. It lets
/// `section_path_contains` push a necessary-condition prefilter down to
/// Meilisearch; the exact substring check still runs locally.
pub const CONTEXT_SECTION_PATH_TRIGRAMS_FIELD: &str = "section_path_trigrams";

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

/// Total time budget for one task wait. This matches the previous fixed
/// schedule (600 polls x 100 ms) so callers keep the same timeout semantics.
const MEILI_TASK_WAIT_TIMEOUT: Duration = Duration::from_secs(60);
/// Most Meilisearch tasks on a small index finish within a few milliseconds,
/// so polling starts quickly and backs off exponentially to the cap.
const MEILI_TASK_POLL_INITIAL: Duration = Duration::from_millis(5);
const MEILI_TASK_POLL_MAX: Duration = Duration::from_millis(200);
/// Upper bound on task UIDs per `GET /tasks?uids=...` status request.
const MEILI_TASK_BATCH_MAX_UIDS: usize = 100;
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
            client: meili_http_client(config),
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
            .map_err(meili_transport_error)?;
        if !response.status().is_success() {
            return Err(self
                .status_error(
                    "failed to prove that the Meilisearch instance is empty".to_string(),
                    response,
                )
                .await);
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(meili_transport_error)?;
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
            .map_err(meili_transport_error)?;
        if !response.status().is_success() {
            return Err(self
                .status_error(
                    format!("Meilisearch durable index {uid} is unavailable; refusing persistence"),
                    response,
                )
                .await);
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(meili_transport_error)?;
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
                    .map_err(meili_transport_error)?;
                let status = response.status();
                if !status.is_success() && status.as_u16() != 404 {
                    return Err(self
                        .status_error(
                            format!("failed to delete Meilisearch index {uid}"),
                            response,
                        )
                        .await);
                }
                if status.is_success() {
                    let body = response
                        .json::<Value>()
                        .await
                        .map_err(meili_transport_error)?;
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
                .map_err(meili_transport_error)?;
            if !response.status().is_success() {
                let status = response.status();
                let details = self.error_details(response).await;
                if !self.index_exists(uid).await? {
                    return Err(meili_status_error(
                        format!("failed to create Meilisearch index {uid}"),
                        status,
                        &details,
                    ));
                }
            } else {
                let body = response
                    .json::<Value>()
                    .await
                    .map_err(meili_transport_error)?;
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
            .map_err(meili_transport_error)?;
        if !response.status().is_success() {
            return Err(self
                .status_error(
                    format!(
                        "failed to strictly create Meilisearch index {uid}; refusing to adopt any concurrently-created index"
                    ),
                    response,
                )
                .await);
        }

        let body = response
            .json::<Value>()
            .await
            .map_err(meili_transport_error)?;
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
            .map_err(meili_transport_error)?;
        if response.status().is_success() {
            Ok(true)
        } else if response.status().as_u16() == 404 {
            Ok(false)
        } else {
            Err(self
                .status_error(
                    format!("failed to inspect Meilisearch index {uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(self
                .status_error(
                    format!("failed to inspect Meilisearch primary key for {uid}"),
                    response,
                )
                .await);
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(meili_transport_error)?;
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
            .map_err(meili_transport_error)?;
        if response.status().as_u16() == 404 {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(self
                .status_error(
                    format!("failed to inspect Meilisearch createdAt for {uid}"),
                    response,
                )
                .await);
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(meili_transport_error)?;
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
            .map_err(meili_transport_error)?;

        if response.status().is_success() {
            let body = response
                .json::<Value>()
                .await
                .map_err(meili_transport_error)?;
            Ok(Some(required_task_uid(
                &body,
                &format!("add documents to Meilisearch index {index_uid}"),
            )?))
        } else {
            Err(self
                .status_error(
                    format!("failed to add Meilisearch documents into {index_uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;

        if response.status().as_u16() == 404 {
            return Ok(None);
        }

        if response.status().is_success() {
            let body = response
                .json::<Value>()
                .await
                .map_err(meili_transport_error)?;
            Ok(Some(required_task_uid(
                &body,
                &format!("delete documents from Meilisearch index {index_uid}"),
            )?))
        } else {
            Err(self
                .status_error(
                    format!("failed to delete Meilisearch documents from {index_uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;

        if response.status().as_u16() == 404 {
            return Ok(None);
        }

        if response.status().is_success() {
            let body = response
                .json::<Value>()
                .await
                .map_err(meili_transport_error)?;
            Ok(Some(required_task_uid(
                &body,
                &format!("delete documents from Meilisearch index {index_uid}"),
            )?))
        } else {
            Err(self
                .status_error(
                    format!("failed to delete Meilisearch documents from {index_uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;

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
                .map_err(meili_transport_error)
        } else {
            Err(self
                .status_error(
                    format!("failed to search Meilisearch index {index_uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;

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
                .map_err(meili_transport_error)
        } else {
            Err(self
                .status_error(
                    format!("failed to list Meilisearch documents from {index_uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;

        if response.status().as_u16() == 404 {
            return Err(self
                .status_error(
                    format!(
                        "required Meilisearch index {index_uid} is missing during filtered scan"
                    ),
                    response,
                )
                .await);
        }
        if response.status().is_success() {
            response
                .json::<DocumentPage>()
                .await
                .map_err(meili_transport_error)
        } else {
            Err(self
                .status_error(
                    format!("failed to fetch filtered Meilisearch documents from {index_uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;

        if response.status().as_u16() == 404 {
            return Ok(json!({ "hits": [] }));
        }
        if response.status().is_success() {
            response
                .json::<Value>()
                .await
                .map_err(meili_transport_error)
        } else {
            Err(self
                .status_error(
                    format!("failed to search Meilisearch index {index_uid}"),
                    response,
                )
                .await)
        }
    }

    pub async fn wait_for_task(&self, task_uid: &str) -> Result<(), ApiError> {
        let Some(url) = &self.url else {
            return Ok(());
        };
        let deadline = Instant::now() + MEILI_TASK_WAIT_TIMEOUT;
        let mut delay = MEILI_TASK_POLL_INITIAL;
        loop {
            let response = self
                .client
                .get(format!("{}/tasks/{}", url.trim_end_matches('/'), task_uid))
                .headers(self.headers()?)
                .send()
                .await
                .map_err(meili_transport_error)?;
            if !response.status().is_success() {
                return Err(self
                    .status_error(
                        format!("failed to read Meilisearch task {task_uid}"),
                        response,
                    )
                    .await);
            }
            let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
            match body.get("status").and_then(Value::as_str) {
                Some("succeeded") => return Ok(()),
                Some("failed") | Some("canceled") => {
                    return Err(self.task_error(task_uid, &body));
                }
                _ => {}
            }
            if !sleep_before_next_poll(&mut delay, deadline).await {
                return Err(ApiError::Upstream(format!(
                    "timed out waiting for Meilisearch task {task_uid}"
                )));
            }
        }
    }

    /// Wait for every task in `task_uids` to succeed.
    ///
    /// A single task keeps the established `GET /tasks/{uid}` probe. Several
    /// numeric task UIDs are polled together through `GET /tasks?uids=...`
    /// so a multi-step write costs one status round trip per poll instead of
    /// one sequential wait per task. If the batch listing is unavailable or
    /// does not report a requested UID, the remaining tasks fall back to the
    /// per-task probe, which preserves its exact error semantics.
    pub async fn wait_for_tasks(&self, task_uids: &[String]) -> Result<(), ApiError> {
        let mut unique = Vec::with_capacity(task_uids.len());
        let mut seen = BTreeSet::new();
        for task_uid in task_uids {
            if seen.insert(task_uid.as_str()) {
                unique.push(task_uid.clone());
            }
        }
        let batchable = unique.len() > 1 && unique.iter().all(|uid| uid.parse::<u64>().is_ok());
        if self.url.is_none() || !batchable {
            for task_uid in &unique {
                self.wait_for_task(task_uid).await?;
            }
            return Ok(());
        }
        for chunk in unique.chunks(MEILI_TASK_BATCH_MAX_UIDS) {
            let remaining = self.wait_for_task_batch(chunk).await?;
            for task_uid in remaining {
                self.wait_for_task(&task_uid).await?;
            }
        }
        Ok(())
    }

    /// Poll one batch of task UIDs until each one has succeeded, returning
    /// the UIDs that must be confirmed through the per-task probe instead.
    async fn wait_for_task_batch(&self, task_uids: &[String]) -> Result<Vec<String>, ApiError> {
        let Some(url) = &self.url else {
            return Ok(Vec::new());
        };
        let mut pending = task_uids.iter().cloned().collect::<BTreeSet<_>>();
        let deadline = Instant::now() + MEILI_TASK_WAIT_TIMEOUT;
        let mut delay = MEILI_TASK_POLL_INITIAL;
        loop {
            let uids = pending.iter().cloned().collect::<Vec<_>>().join(",");
            let response = self
                .client
                .get(format!("{}/tasks", url.trim_end_matches('/')))
                .query(&[
                    ("uids", uids.as_str()),
                    ("limit", &pending.len().to_string()),
                ])
                .headers(self.headers()?)
                .send()
                .await
                .map_err(meili_transport_error)?;
            if !response.status().is_success() {
                return Ok(pending.into_iter().collect());
            }
            let Ok(body) = response.json::<Value>().await else {
                return Ok(pending.into_iter().collect());
            };
            let Some(results) = body.get("results").and_then(Value::as_array) else {
                return Ok(pending.into_iter().collect());
            };
            let mut reported = BTreeSet::new();
            for task in results {
                let Some(uid) = task_uid(task) else {
                    continue;
                };
                if !pending.contains(&uid) {
                    continue;
                }
                match task.get("status").and_then(Value::as_str) {
                    Some("succeeded") => {
                        pending.remove(&uid);
                    }
                    Some("failed") | Some("canceled") => {
                        return Err(self.task_error(&uid, task));
                    }
                    _ => {}
                }
                reported.insert(uid);
            }
            if pending.is_empty() {
                return Ok(Vec::new());
            }
            if pending.iter().any(|uid| !reported.contains(uid)) {
                // A task missing from a successful listing is unknown to
                // this server (or filtered by key scope); let the single-task
                // probe produce its established error instead of spinning.
                return Ok(pending.into_iter().collect());
            }
            if !sleep_before_next_poll(&mut delay, deadline).await {
                return Err(ApiError::Upstream(format!(
                    "timed out waiting for {} Meilisearch tasks",
                    pending.len()
                )));
            }
        }
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
            .map_err(meili_transport_error)?;

        if response.status().is_success() {
            let body = response
                .json::<Value>()
                .await
                .map_err(meili_transport_error)?;
            Ok(Some(required_task_uid(
                &body,
                &format!("apply settings for Meilisearch index {uid}"),
            )?))
        } else {
            Err(self
                .status_error(
                    format!("failed to apply Meilisearch settings for {uid}"),
                    response,
                )
                .await)
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
            .map_err(meili_transport_error)?;
        if !response.status().is_success() {
            return Err(self
                .status_error(
                    format!("failed to read Meilisearch settings for {uid}"),
                    response,
                )
                .await);
        }
        let current = response
            .json::<Value>()
            .await
            .map_err(meili_transport_error)?;
        Ok(managed_settings_equal(&current, desired))
    }

    /// Read a failed response's bounded Meilisearch error object.
    async fn error_details(&self, response: reqwest::Response) -> MeiliErrorDetails {
        let body = read_bounded_error_body(response).await;
        MeiliErrorDetails::from_body(&body, &self.known_secrets())
    }

    /// Private cause for a failed Meilisearch HTTP response, keeping the
    /// bounded structured error object instead of only the status line.
    async fn status_error(&self, context: String, response: reqwest::Response) -> ApiError {
        let status = response.status();
        let details = self.error_details(response).await;
        meili_status_error(context, status, &details)
    }

    /// Private cause for a failed or canceled task. Only the task's bounded
    /// error object and type are kept; the full task body (which can echo
    /// filters and settings) is not copied into the cause.
    fn task_error(&self, task_uid: &str, task: &Value) -> ApiError {
        let status = match task.get("status").and_then(Value::as_str) {
            Some("canceled") => "canceled",
            _ => "failed",
        };
        let details = task
            .get("error")
            .map(|error| MeiliErrorDetails::from_value(error, &self.known_secrets()))
            .unwrap_or_default();
        let mut cause = format!("Meilisearch task {task_uid} did not succeed: status={status}");
        if let Some(task_type) = meili_task_type(task.get("type")) {
            cause.push_str(" task_type=");
            cause.push_str(task_type);
        }
        if !details.is_empty() {
            cause.push_str("; ");
            cause.push_str(&details.to_string());
        }
        log_meili_error(None, &details, &cause);
        ApiError::Upstream(cause)
    }

    fn known_secrets(&self) -> Vec<String> {
        self.api_key.iter().cloned().collect()
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

/// Build the shared Meilisearch HTTP client with bounded connect and request
/// times, so a stalled server cannot hold a startup, worker, or task wait
/// forever. The request timeout covers one HTTP exchange including its body;
/// task waits poll with separate requests under their own total budget.
fn meili_http_client(config: &Config) -> reqwest::Client {
    reqwest::Client::builder()
        .connect_timeout(Duration::from_millis(config.meili_connect_timeout_ms))
        .timeout(Duration::from_millis(config.meili_request_timeout_ms))
        .build()
        // `reqwest::Client::new()` panics on the same backend initialization
        // failure; never fall back to a client without timeouts.
        .expect("Meilisearch HTTP client initialization failed")
}

/// Map a Meilisearch transport or body failure to a private upstream cause.
///
/// The cause names the failure class (timeout, connection, decode) so it can
/// be classified, and carries only the request path: the configured base URL
/// may embed credentials and is never echoed.
fn meili_transport_error(error: reqwest::Error) -> ApiError {
    let path = error
        .url()
        .map(|url| url.path().to_string())
        .unwrap_or_else(|| "<unknown>".to_string());
    let failure = match (error.is_connect(), error.is_timeout()) {
        (true, true) => "connect timed out",
        (false, true) => "timed out",
        (true, false) => "connection failed",
        _ if error.is_decode() => "response could not be decoded",
        _ if error.is_body() => "body transfer failed",
        _ => "request failed",
    };
    ApiError::Upstream(format!("Meilisearch request {path} {failure}"))
}

/// Most Meilisearch error bodies are a few hundred bytes; anything past this
/// is never read, so a misbehaving server or proxy cannot inflate a cause.
const MEILI_ERROR_BODY_MAX_BYTES: usize = 16 * 1024;
const MEILI_ERROR_MESSAGE_MAX_CHARS: usize = 512;
const MEILI_ERROR_BODY_SNIPPET_MAX_CHARS: usize = 256;
const MEILI_ERROR_TOKEN_MAX_CHARS: usize = 64;
const MEILI_ERROR_LINK_MAX_CHARS: usize = 256;

/// The structured error object Meilisearch returns for failed requests and
/// failed tasks (`message`, `code`, `type`, `link`). Every field is bounded
/// and sanitized on parse; `body` keeps a short snippet of a non-JSON error
/// (for example a proxy page) instead.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct MeiliErrorDetails {
    code: Option<String>,
    error_type: Option<String>,
    message: Option<String>,
    link: Option<String>,
    body: Option<String>,
}

impl MeiliErrorDetails {
    fn from_body(body: &str, known_secrets: &[String]) -> Self {
        if let Ok(value) = serde_json::from_str::<Value>(body) {
            let details = Self::from_value(&value, known_secrets);
            if !details.is_empty() {
                return details;
            }
        }
        let body =
            sanitize_meili_error_text(body, known_secrets, MEILI_ERROR_BODY_SNIPPET_MAX_CHARS);
        Self {
            body: (!body.is_empty()).then_some(body),
            ..Self::default()
        }
    }

    fn from_value(value: &Value, known_secrets: &[String]) -> Self {
        let field = |name: &str| value.get(name).and_then(Value::as_str);
        Self {
            code: field("code").and_then(meili_error_token),
            error_type: field("type").and_then(meili_error_token),
            message: field("message")
                .map(|message| {
                    sanitize_meili_error_text(message, known_secrets, MEILI_ERROR_MESSAGE_MAX_CHARS)
                })
                .filter(|message| !message.is_empty()),
            link: field("link").and_then(meili_error_link),
            body: None,
        }
    }

    fn is_empty(&self) -> bool {
        self == &Self::default()
    }
}

impl std::fmt::Display for MeiliErrorDetails {
    /// The code and type markers come first so the error classifier reads
    /// them before any free text (which is neutralized to never contain a
    /// marker).
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if let Some(code) = &self.code {
            parts.push(format!("{}{code}", crate::error::MEILI_CODE_MARKER));
        }
        if let Some(error_type) = &self.error_type {
            parts.push(format!("{}{error_type}", crate::error::MEILI_TYPE_MARKER));
        }
        if let Some(message) = &self.message {
            parts.push(format!("message={message:?}"));
        }
        if let Some(link) = &self.link {
            parts.push(format!("link={link}"));
        }
        if let Some(body) = &self.body {
            parts.push(format!("body={body:?}"));
        }
        formatter.write_str(&parts.join(" "))
    }
}

/// Read at most [`MEILI_ERROR_BODY_MAX_BYTES`] of a failed response. Read
/// failures keep whatever arrived; the request timeout bounds the wait.
async fn read_bounded_error_body(mut response: reqwest::Response) -> String {
    let mut body = Vec::new();
    while body.len() < MEILI_ERROR_BODY_MAX_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                let remaining = MEILI_ERROR_BODY_MAX_BYTES - body.len();
                body.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
            }
            Ok(None) | Err(_) => break,
        }
    }
    String::from_utf8_lossy(&body).into_owned()
}

/// Redact configured secrets first, then flatten control characters,
/// neutralize classifier markers, and truncate to `max_chars`.
fn sanitize_meili_error_text(text: &str, known_secrets: &[String], max_chars: usize) -> String {
    let redacted = crate::util::redact_string(text, known_secrets);
    let flattened = redacted
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .collect::<String>()
        .replace(crate::error::MEILI_CODE_MARKER, "meili_code:")
        .replace(crate::error::MEILI_TYPE_MARKER, "meili_type:");
    crate::util::truncate_chars(flattened.trim(), max_chars)
}

/// Meilisearch codes and types are short snake_case identifiers; anything
/// else is dropped rather than copied into a cause or log field.
fn meili_error_token(value: &str) -> Option<String> {
    let valid = !value.is_empty()
        && value.len() <= MEILI_ERROR_TOKEN_MAX_CHARS
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_');
    valid.then(|| value.to_string())
}

fn meili_error_link(value: &str) -> Option<String> {
    let valid = value.starts_with("https://")
        && value.len() <= MEILI_ERROR_LINK_MAX_CHARS
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b":/._#?=&%+-~".contains(&byte));
    valid.then(|| value.to_string())
}

/// Task types are short camelCase identifiers such as `indexCreation`.
fn meili_task_type(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str).filter(|task_type| {
        !task_type.is_empty()
            && task_type.len() <= MEILI_ERROR_TOKEN_MAX_CHARS
            && task_type.bytes().all(|byte| byte.is_ascii_alphanumeric())
    })
}

/// Private cause for a non-2xx Meilisearch response: the caller's context,
/// the HTTP status, and the bounded structured error object.
fn meili_status_error(
    context: String,
    status: reqwest::StatusCode,
    details: &MeiliErrorDetails,
) -> ApiError {
    let mut cause = format!("{context}: status {status}");
    if !details.is_empty() {
        cause.push_str("; ");
        cause.push_str(&details.to_string());
    }
    log_meili_error(Some(status.as_u16()), details, &cause);
    ApiError::Upstream(cause)
}

/// Log only allowlisted, bounded diagnostics: the HTTP status, Meilisearch's
/// code and type (sanitized identifiers), and the same category and keyed
/// fingerprint the error envelope logs for this cause. The message, body,
/// request headers, and API keys never reach the log.
fn log_meili_error(http_status: Option<u16>, details: &MeiliErrorDetails, cause: &str) {
    let diagnostic = crate::error::safe_cause_diagnostic(cause);
    tracing::warn!(
        target: "nowledge::meili",
        http_status,
        meili_code = details.code.as_deref().unwrap_or("none"),
        meili_type = details.error_type.as_deref().unwrap_or("none"),
        cause_category = diagnostic.category,
        cause_fingerprint = %diagnostic.fingerprint,
        "Meilisearch request failed"
    );
}

/// Sleep for the current poll delay and double it up to the cap. Returns
/// `false` without sleeping when the next poll would start past `deadline`.
async fn sleep_before_next_poll(delay: &mut Duration, deadline: Instant) -> bool {
    let now = Instant::now();
    if now >= deadline {
        return false;
    }
    let wait = (*delay).min(deadline - now);
    sleep(wait).await;
    *delay = next_task_poll_delay(*delay);
    true
}

fn next_task_poll_delay(delay: Duration) -> Duration {
    delay.saturating_mul(2).min(MEILI_TASK_POLL_MAX)
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
            "filterableAttributes": ["id", "uri", "tenant_id", "owner_user_id", "index_uid", "index_kind", "layer", "ancestor_uris", "status", "privacy", "source_id", "revision_id", "node_kind", "retrieval_role", "retrieval_enabled", "parent_uri", "source_document_uri", "fragment_index", "block_type", "page_idx", "heading_level", "artifact_refs.artifact_kind", CONTEXT_SECTION_PATH_TRIGRAMS_FIELD],
            "sortableAttributes": ["updated_at", "layer"],
            "stopWords": ENGLISH_STOP_WORDS
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
    } else if uid == "rag_traces" {
        // `created_at_ms` is a numeric mirror of `created_at` written by the
        // trace persister so retention can delete by a numeric range filter.
        json!({
            "searchableAttributes": ["title", "statement", "text", "content", "id"],
            "filterableAttributes": ["id", "tenant_id", "owner_user_id", "created_at", "created_at_ms"],
            "sortableAttributes": ["created_at", "created_at_ms"]
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
        // Only indexes whose managed settings declare stop words pin them;
        // Meilisearch reports stop words as a set.
        && desired.get("stopWords").is_none_or(|stop_words| {
            unordered_string_arrays_equal(current.get("stopWords"), Some(stop_words))
        })
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

    #[tokio::test]
    async fn search_write_and_task_requests_time_out_against_a_stalled_server() {
        let app = Router::new().fallback(healthy_then_stalled_meili);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let mut config = Config::test();
        config.meili_url = Some(format!("http://{address}"));
        config.meili_connect_timeout_ms = 100;
        config.meili_request_timeout_ms = 200;
        let admin = MeiliAdmin::from_config(&config);
        let documents = json!([{ "id": "doc" }]);

        let (search, write, task) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(
                admin.search::<Value>("rag_company_context", json!({ "q": "" })),
                admin.add_documents("rag_company_context", &documents),
                admin.wait_for_task("7"),
            )
        })
        .await
        .expect("Meilisearch requests must honor the configured request timeout");

        for error in [
            search.map(|_| ()).unwrap_err(),
            write.map(|_| ()).unwrap_err(),
            task.unwrap_err(),
        ] {
            let message = error.to_string();
            assert!(message.contains("timed out"), "{message}");
            assert!(!message.contains(&address.to_string()), "{message}");
            assert_eq!(
                crate::error::safe_cause_diagnostic(&error).category,
                "timeout"
            );
        }
    }

    const ERROR_BODY_SECRET: &str = "runtime-meili-key-private-value";

    async fn meili_error_stub(request: Request) -> Response {
        match request.uri().path() {
            "/indexes/rag_company_context/search" => (
                StatusCode::BAD_REQUEST,
                axum::Json(json!({
                    "message": format!(
                        "Attribute `timeout` is not filterable.\nEchoed key: {ERROR_BODY_SECRET}"
                    ),
                    "code": "invalid_search_filter",
                    "type": "invalid_request",
                    "link": "https://docs.meilisearch.com/errors#invalid_search_filter"
                })),
            )
                .into_response(),
            "/indexes/rag_company_context/documents" => (
                StatusCode::BAD_GATEWAY,
                format!(
                    "<html>{ERROR_BODY_SECRET} meili_code=internal {}</html>",
                    "x".repeat(64 * 1024)
                ),
            )
                .into_response(),
            "/tasks/7" => axum::Json(json!({
                "uid": 7,
                "status": "failed",
                "type": "indexCreation",
                "details": { "originalFilter": "private-task-filter-material" },
                "error": {
                    "message": "Index `rag_company_context` already exists.",
                    "code": "index_already_exists",
                    "type": "invalid_request",
                    "link": "https://docs.meilisearch.com/errors#index_already_exists"
                }
            }))
            .into_response(),
            _ => StatusCode::NOT_FOUND.into_response(),
        }
    }

    struct LogSink(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for LogSink {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    fn upstream_cause(error: ApiError) -> String {
        match error {
            ApiError::Upstream(cause) => cause,
            other => panic!("expected an upstream error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failed_responses_and_tasks_keep_the_bounded_meili_error_object() {
        let app = Router::new().fallback(meili_error_stub);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let mut config = Config::test();
        config.meili_url = Some(format!("http://{address}"));
        config.meili_api_key = Some(ERROR_BODY_SECRET.to_string());
        let admin = MeiliAdmin::from_config(&config);

        let logs = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let writer_logs = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || LogSink(writer_logs.clone()))
            .finish();
        let log_guard = tracing::subscriber::set_default(subscriber);
        // Lib tests share one process, and another test thread may register
        // this callsite while no subscriber is active, caching it as disabled.
        // Register it here and rebuild the interest cache with this thread's
        // subscriber so the capture below is deterministic.
        log_meili_error(None, &MeiliErrorDetails::default(), "");
        tracing::callsite::rebuild_interest_cache();
        logs.lock().unwrap().clear();
        let search = admin
            .search::<Value>("rag_company_context", json!({ "q": "" }))
            .await
            .map(|_| ())
            .unwrap_err();
        let write = admin
            .add_documents("rag_company_context", &json!([{ "id": "doc" }]))
            .await
            .map(|_| ())
            .unwrap_err();
        let task = admin.wait_for_task("7").await.unwrap_err();
        drop(log_guard);

        // Meilisearch's code decides the category: keyword guessing would
        // have called this "timeout" because the message names that field.
        assert_eq!(
            crate::error::safe_cause_diagnostic(&search).category,
            "invalid_request"
        );
        let search = upstream_cause(search);
        for expected in [
            "failed to search Meilisearch index rag_company_context: status 400 Bad Request",
            "meili_code=invalid_search_filter",
            "meili_type=invalid_request",
            "Attribute `timeout` is not filterable.",
            "link=https://docs.meilisearch.com/errors#invalid_search_filter",
        ] {
            assert!(search.contains(expected), "missing {expected}: {search}");
        }
        assert!(!search.contains('\n'), "{search}");

        // A non-JSON body keeps only a short snippet, and text that imitates
        // a classifier marker cannot pick the category.
        assert_eq!(
            crate::error::safe_cause_diagnostic(&write).category,
            "unspecified"
        );
        let write = upstream_cause(write);
        assert!(
            write.contains("status 502 Bad Gateway; body=\"<html>"),
            "{write}"
        );
        assert!(!write.contains("meili_code="), "{write}");
        assert!(write.len() < 1_024, "error body was not truncated: {write}");

        assert_eq!(
            crate::error::safe_cause_diagnostic(&task).category,
            "conflict"
        );
        let task = upstream_cause(task);
        for expected in [
            "Meilisearch task 7 did not succeed: status=failed task_type=indexCreation",
            "meili_code=index_already_exists",
            "already exists.",
        ] {
            assert!(task.contains(expected), "missing {expected}: {task}");
        }
        assert!(!task.contains("private-task-filter-material"), "{task}");

        for cause in [&search, &write, &task] {
            assert!(!cause.contains(ERROR_BODY_SECRET), "{cause}");
            assert!(!cause.contains(&address.to_string()), "{cause}");
        }

        let rendered = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
        for expected in [
            "nowledge::meili",
            "\"http_status\":400",
            "invalid_search_filter",
            "index_already_exists",
            "cause_fingerprint",
        ] {
            assert!(
                rendered.contains(expected),
                "missing {expected}: {rendered}"
            );
        }
        for private in [
            ERROR_BODY_SECRET,
            "not filterable",
            "<html>",
            "already exists.",
            "private-task-filter-material",
            "Bearer",
            "authorization",
        ] {
            assert!(!rendered.contains(private), "leaked {private}: {rendered}");
        }
    }

    async fn spawn_task_stub(
        batch_supported: bool,
        failing_uid: Option<u64>,
    ) -> (String, Arc<std::sync::Mutex<Vec<String>>>) {
        let requests = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recorded = requests.clone();
        let app = Router::new().fallback(move |request: Request| {
            let recorded = recorded.clone();
            let polls = polls.clone();
            async move {
                let path = request.uri().path().to_string();
                let query = request.uri().query().unwrap_or_default().to_string();
                recorded.lock().unwrap().push(if query.is_empty() {
                    path.clone()
                } else {
                    format!("{path}?{query}")
                });
                let status_for = |uid: u64, poll: usize| {
                    if Some(uid) == failing_uid {
                        "failed"
                    } else if poll == 0 {
                        "processing"
                    } else {
                        "succeeded"
                    }
                };
                if path == "/tasks" && batch_supported {
                    let poll = polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let uids = query
                        .split('&')
                        .find_map(|pair| pair.strip_prefix("uids="))
                        .unwrap_or_default()
                        .replace("%2C", ",");
                    let results = uids
                        .split(',')
                        .filter_map(|uid| uid.parse::<u64>().ok())
                        .map(|uid| json!({ "uid": uid, "status": status_for(uid, poll) }))
                        .collect::<Vec<_>>();
                    return axum::Json(json!({ "results": results })).into_response();
                }
                if let Some(uid) = path.strip_prefix("/tasks/") {
                    let uid = uid.parse::<u64>().unwrap();
                    return axum::Json(json!({ "uid": uid, "status": status_for(uid, 1) }))
                        .into_response();
                }
                StatusCode::NOT_FOUND.into_response()
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://{address}"), requests)
    }

    fn admin_for(url: String) -> MeiliAdmin {
        let mut config = Config::test();
        config.meili_url = Some(url);
        MeiliAdmin::from_config(&config)
    }

    #[tokio::test]
    async fn multi_task_waits_poll_one_batched_status_request() {
        let (url, requests) = spawn_task_stub(true, None).await;
        let admin = admin_for(url);
        let uids = ["7", "8", "9", "8"].map(String::from);
        admin.wait_for_tasks(&uids).await.unwrap();

        let requests = requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2, "{requests:?}");
        assert!(requests
            .iter()
            .all(|request| request.starts_with("/tasks?uids=7")));
        assert!(!requests
            .iter()
            .any(|request| request.starts_with("/tasks/")));
    }

    #[tokio::test]
    async fn multi_task_waits_fall_back_to_single_task_probes() {
        let (url, requests) = spawn_task_stub(false, None).await;
        let admin = admin_for(url);
        admin
            .wait_for_tasks(&["3".to_string(), "4".to_string()])
            .await
            .unwrap();

        let requests = requests.lock().unwrap().clone();
        assert!(requests.contains(&"/tasks/3".to_string()), "{requests:?}");
        assert!(requests.contains(&"/tasks/4".to_string()), "{requests:?}");
    }

    #[tokio::test]
    async fn batched_task_waits_surface_failed_tasks() {
        let (url, _) = spawn_task_stub(true, Some(12)).await;
        let admin = admin_for(url);
        let error = admin
            .wait_for_tasks(&["11".to_string(), "12".to_string()])
            .await
            .unwrap_err();
        assert!(error.to_string().contains("12"), "{error}");
    }

    #[test]
    fn task_poll_delay_backs_off_exponentially_to_the_cap() {
        let mut delay = MEILI_TASK_POLL_INITIAL;
        let mut observed = vec![delay.as_millis()];
        for _ in 0..8 {
            delay = next_task_poll_delay(delay);
            observed.push(delay.as_millis());
        }
        assert_eq!(observed, vec![5, 10, 20, 40, 80, 160, 200, 200, 200]);
    }

    #[test]
    fn trace_settings_support_time_based_retention() {
        let settings = settings_for("rag_traces");
        let filterable = settings["filterableAttributes"].as_array().unwrap();
        for required in ["tenant_id", "logical_id", "created_at", "created_at_ms"] {
            assert!(
                filterable.iter().any(|value| value == required),
                "{required}"
            );
        }
        let sortable = settings["sortableAttributes"].as_array().unwrap();
        assert!(sortable.iter().any(|value| value == "created_at"));
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
    fn context_indexes_pin_english_stop_words_and_pushdown_filters() {
        for uid in ["rag_company_context", "rag_context__t_abc__u_def"] {
            let settings = settings_for(uid);
            let filterable = settings["filterableAttributes"]
                .as_array()
                .expect("filterable attributes");
            for required in [
                "artifact_refs.artifact_kind",
                CONTEXT_SECTION_PATH_TRIGRAMS_FIELD,
            ] {
                assert!(
                    filterable.iter().any(|value| value == required),
                    "{uid} is missing {required}"
                );
            }
            assert_eq!(
                settings["stopWords"].as_array().map(Vec::len),
                Some(ENGLISH_STOP_WORDS.len())
            );

            let mut drifted = settings.clone();
            drifted["stopWords"] = json!([]);
            assert!(!managed_settings_equal(&drifted, &settings));
            let mut reordered = settings.clone();
            reordered["stopWords"]
                .as_array_mut()
                .expect("stop words")
                .reverse();
            assert!(managed_settings_equal(&reordered, &settings));
        }
        // Indexes without managed stop words do not pin them.
        let events = settings_for("rag_events__t_abc__u_def");
        assert!(events.get("stopWords").is_none());
        let mut with_stop_words = events.clone();
        with_stop_words["stopWords"] = json!(["the"]);
        assert!(managed_settings_equal(&with_stop_words, &events));
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
