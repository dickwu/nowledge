use std::{
    collections::BTreeMap,
    fs,
    net::{TcpStream, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use anyhow::{anyhow, ensure, Result as AnyResult};
use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{
    build_router,
    config::{AuthUserConfig, AuthUserScope},
    meili::{task_uid, MeiliAdmin, FIXED_INDEXES},
    models::{
        AnalysisInsightMaterialization, AnalysisLinkMaterialization,
        AnalysisMaterializationRequest, ContextNode, HarnessChangeManifest, IngestTask,
        IngestTaskResult, OperationIndexingState, OperationStatus, OperationStepStatus,
        RagEvalCaseResult, RagEvalMetrics, RagEvalOverview, RagEvalRun, SourceDocument,
        StructuredSnapshot, TraceRecord, UserEventIndex,
    },
    repository::{KnowledgeRepository, MeiliRepository},
    tenant_scope::{
        owner_scoped_storage_identity, tenant_document, tenant_document_with_storage_identity,
        tenant_structured_row_document, TenantFilter,
    },
    tenant_scope_v1::{
        apply_plan, create_plan, create_rollback_plan, verify_plan, FileCheckpointStore,
        LegacyTenantAssignment, LegacyTenantMapping, MIGRATION_NAME,
    },
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

#[path = "../src/audit_records_v1.rs"]
#[allow(dead_code)]
mod audit_records_v1;

static LIVE_MEILI_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
type AuxiliaryInventory = (&'static str, Vec<Value>, Vec<Value>);

async fn live_meili_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    LIVE_MEILI_TEST_LOCK.lock().await
}

fn meili_available(url: &str) -> bool {
    let authority = url
        .trim_start_matches("http://")
        .trim_start_matches("https://")
        .split('/')
        .next()
        .unwrap_or_default();
    let address = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:7700")
    };
    let Ok(addrs) = address.to_socket_addrs() else {
        return false;
    };
    addrs
        .into_iter()
        .any(|addr| TcpStream::connect_timeout(&addr, Duration::from_millis(200)).is_ok())
}

async fn meili_config_with_tenant(tenant_id: String) -> Option<Config> {
    let url = std::env::var("RAG_TEST_MEILI_URL").ok()?;
    if !meili_available(&url) {
        eprintln!("skipping Meilisearch integration test; {url} is not reachable");
        return None;
    }
    let api_key = std::env::var("RAG_TEST_MEILI_API_KEY").ok();
    if api_key.is_none() {
        let response = reqwest::Client::new()
            .get(format!("{}/indexes", url.trim_end_matches('/')))
            .send()
            .await;
        if response
            .as_ref()
            .is_ok_and(|response| response.status().as_u16() == 401)
        {
            eprintln!(
                "skipping Meilisearch integration test; server requires a key, set RAG_TEST_MEILI_API_KEY"
            );
            return None;
        }
    }
    let mut config = Config::test();
    config.tenant_id = tenant_id;
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.meili_api_key = api_key;
    config.meili_wait_for_tasks = true;
    // These tests exercise repository semantics through an SSH tunnel. Give
    // task polling enough headroom that the HTTP boundary does not turn normal
    // remote indexing latency into a persistence-test failure; timeout behavior
    // has dedicated deterministic coverage in http_boundary_characterization.
    config.request_timeout_ms = 180_000;
    config.sync_ingest_timeout_ms = 180_000;
    Some(config)
}

async fn bootstrapped_meili_admin(config: &Config) -> MeiliAdmin {
    let admin = MeiliAdmin::from_config(config);
    admin
        .bootstrap(false)
        .await
        .expect("Meilisearch bootstrap should succeed");
    admin
}

async fn prepare_audit_records_index(config: &Config) -> Result<(), String> {
    let admin = MeiliAdmin::from_config(config);
    match admin.bootstrap(false).await {
        Ok(_) => return Ok(()),
        Err(error) if error.to_string().contains("rag_audit_records") => {}
        Err(error) => return Err(format!("managed-index bootstrap failed: {error}")),
    }

    let plan = audit_records_v1::create_plan(&admin)
        .await
        .map_err(|error| format!("audit migration planning failed: {error}"))?;
    audit_records_v1::apply_plan(&admin, &plan, false)
        .await
        .map_err(|error| format!("audit migration apply failed: {error}"))?;
    let verification = audit_records_v1::verify_plan(&admin, &plan)
        .await
        .map_err(|error| format!("audit migration verify failed: {error}"))?;
    if !verification.ready {
        return Err(format!(
            "audit migration did not verify: {}",
            verification.failures.join("; ")
        ));
    }
    admin
        .bootstrap(false)
        .await
        .map_err(|error| format!("post-migration bootstrap failed: {error}"))?;
    Ok(())
}

async fn start_meili_app(config: &Config) -> (AppState, Value, Router) {
    let state = AppState::new(Arc::new(config.clone()));
    state
        .meili
        .bootstrap(false)
        .await
        .expect("startup Meilisearch reconciliation should succeed");
    let hydrated = state
        .store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .expect("startup repository hydration should succeed");
    let app = build_router(state.clone());
    (state, hydrated, app)
}

async fn try_start_meili_app(config: &Config) -> Result<(AppState, Value, Router), String> {
    let state = AppState::new(Arc::new(config.clone()));
    state
        .meili
        .bootstrap(false)
        .await
        .map_err(|error| format!("startup Meilisearch reconciliation failed: {error}"))?;
    let hydrated = state
        .store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .map_err(|error| format!("startup repository hydration failed: {error}"))?;
    let app = build_router(state.clone());
    Ok((state, hydrated, app))
}

async fn meili_fixture() -> Option<(Config, MeiliAdmin, AppState, Router)> {
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let config = meili_config_with_tenant(tenant_id).await?;
    let (state, _, app) = start_meili_app(&config).await;
    let admin = state.meili.clone();
    Some((config, admin, state, app))
}

async fn two_tenant_meili_fixture() -> Option<(
    Config,
    Config,
    MeiliAdmin,
    AppState,
    Router,
    AppState,
    Router,
)> {
    let fixture_id = uuid::Uuid::now_v7();
    let config_a = meili_config_with_tenant(format!("test-tenant-a-{fixture_id}")).await?;
    let mut config_b = config_a.clone();
    config_b.tenant_id = format!("test-tenant-b-{fixture_id}");
    let (state_a, _, app_a) = start_meili_app(&config_a).await;
    let admin = state_a.meili.clone();
    let (state_b, _, app_b) = start_meili_app(&config_b).await;
    Some((config_a, config_b, admin, state_a, app_a, state_b, app_b))
}

fn equality_filter(field: &str, value: &str) -> String {
    format!(
        "{field} = {}",
        serde_json::to_string(value).expect("filter value should serialize")
    )
}

fn tenant_resource_filter(tenant_id: &str, field: &str, value: &str) -> String {
    format!(
        "{} AND {}",
        equality_filter("tenant_id", tenant_id),
        equality_filter(field, value)
    )
}

fn tenant_logical_filter(tenant_id: &str, logical_id: &str) -> String {
    let logical_id =
        serde_json::to_string(logical_id).expect("logical ID filter value should serialize");
    format!(
        "{} AND (logical_id = {logical_id} OR ((logical_id IS NULL OR logical_id NOT EXISTS) AND id = {logical_id}))",
        equality_filter("tenant_id", tenant_id)
    )
}

fn auxiliary_document(
    index_uid: &str,
    tenant_id: &str,
    logical_id: &str,
    owner_user_id: Option<&str>,
    source_id: &str,
) -> Value {
    let value = json!({
        "id": logical_id,
        "tenant_id": tenant_id,
        "owner_user_id": owner_user_id,
        "source_id": source_id,
        "task_id": logical_id,
        "source_document_uri": format!("ctx://company/{source_id}/{logical_id}"),
        "status": "active",
        "marker": logical_id
    });
    if index_uid == "rag_parse_artifacts" {
        let storage_identity = owner_scoped_storage_identity(owner_user_id, logical_id)
            .expect("test owner scope should be valid");
        tenant_document_with_storage_identity(
            tenant_id,
            index_uid,
            logical_id,
            &storage_identity,
            &value,
        )
    } else {
        tenant_document(tenant_id, index_uid, logical_id, &value)
    }
    .expect("test auxiliary document should serialize")
}

fn two_tenant_logical_filter(tenant_a: &str, tenant_b: &str, logical_id: &str) -> String {
    let tenant_a = serde_json::to_string(tenant_a).expect("tenant filter value should serialize");
    let tenant_b = serde_json::to_string(tenant_b).expect("tenant filter value should serialize");
    let logical_id =
        serde_json::to_string(logical_id).expect("logical ID filter value should serialize");
    format!("(tenant_id = {tenant_a} OR tenant_id = {tenant_b}) AND logical_id = {logical_id}")
}

async fn wait_for_optional_task(
    admin: &MeiliAdmin,
    task_uid: Option<String>,
) -> Result<(), String> {
    if let Some(task_uid) = task_uid {
        admin
            .wait_for_task(&task_uid)
            .await
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

async fn delete_by_filter_and_wait(
    admin: &MeiliAdmin,
    index_uid: &str,
    filter: &str,
) -> Result<(), String> {
    let task_uid = admin
        .delete_documents_by_filter(index_uid, filter)
        .await
        .map_err(|error| error.to_string())?;
    wait_for_optional_task(admin, task_uid).await
}

async fn delete_index_and_wait(
    config: &Config,
    admin: &MeiliAdmin,
    index_uid: &str,
) -> Result<(), String> {
    let Some(url) = config.meili_url.as_deref() else {
        return Ok(());
    };
    let client = reqwest::Client::new();
    let endpoint = format!("{}/indexes/{index_uid}", url.trim_end_matches('/'));
    let mut inspect = client.get(&endpoint);
    if let Some(api_key) = config.meili_api_key.as_deref() {
        inspect = inspect.bearer_auth(api_key);
    }
    let inspection = inspect.send().await.map_err(|error| error.to_string())?;
    if inspection.status().as_u16() == 404 {
        return Ok(());
    }
    if !inspection.status().is_success() {
        return Err(format!(
            "test-owned Meilisearch index cleanup inspection should succeed: {}",
            inspection.status()
        ));
    }

    let mut request = client.delete(endpoint);
    if let Some(api_key) = config.meili_api_key.as_deref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    if response.status().as_u16() == 404 {
        return Ok(());
    }
    if !response.status().is_success() {
        return Err(format!(
            "test-owned Meilisearch index cleanup should be accepted: {}",
            response.status()
        ));
    }
    let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
    wait_for_optional_task(admin, task_uid(&body)).await
}

async fn replace_index_settings_and_wait(
    config: &Config,
    admin: &MeiliAdmin,
    index_uid: &str,
    settings: Value,
) -> Result<(), String> {
    let Some(url) = config.meili_url.as_deref() else {
        return Err("Meilisearch URL is unavailable".to_string());
    };
    let endpoint = format!("{}/indexes/{index_uid}/settings", url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut request = client.patch(endpoint).json(&settings);
    if let Some(api_key) = config.meili_api_key.as_deref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "legacy settings write for {index_uid} should be accepted: {}",
            response.status()
        ));
    }
    let body = response.json::<Value>().await.unwrap_or_else(|_| json!({}));
    wait_for_optional_task(admin, task_uid(&body)).await
}

async fn read_index_settings(config: &Config, index_uid: &str) -> Result<Value, String> {
    let Some(url) = config.meili_url.as_deref() else {
        return Err("Meilisearch URL is unavailable".to_string());
    };
    let endpoint = format!("{}/indexes/{index_uid}/settings", url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let mut request = client.get(endpoint);
    if let Some(api_key) = config.meili_api_key.as_deref() {
        request = request.bearer_auth(api_key);
    }
    let response = request.send().await.map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "settings read for {index_uid} should succeed: {}",
            response.status()
        ));
    }
    response
        .json::<Value>()
        .await
        .map_err(|error| error.to_string())
}

async fn delete_tenant_rows_and_wait(
    admin: &MeiliAdmin,
    tenant_id: &str,
    index_uids: &[&str],
) -> Result<(), String> {
    let tenant_filter = equality_filter("tenant_id", tenant_id);
    let mut failures = Vec::new();
    for index_uid in index_uids {
        if let Err(error) = delete_by_filter_and_wait(admin, index_uid, &tenant_filter).await {
            failures.push(format!("{index_uid}: {error}"));
        }
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

fn assert_cleanup_results(results: Vec<(&'static str, Result<(), String>)>) {
    let failures = results
        .into_iter()
        .filter_map(|(scope, result)| result.err().map(|error| format!("{scope}: {error}")))
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "live Meilisearch fixture cleanup failed: {}",
        failures.join("; ")
    );
}

fn isolated_migration_test_enabled() -> bool {
    std::env::var("RAG_TEST_MEILI_MIGRATION_ISOLATED").as_deref() == Ok("true")
}

async fn read_all_fixed_index_documents(
    admin: &MeiliAdmin,
) -> AnyResult<BTreeMap<String, Vec<Value>>> {
    let mut inventory = BTreeMap::new();
    for index_uid in FIXED_INDEXES {
        let mut offset = 0;
        let mut expected_total = None;
        let mut documents = Vec::new();
        loop {
            let page = admin
                .fetch_documents_page(index_uid, offset, 1_000)
                .await
                .map_err(|error| anyhow!(error.to_string()))?;
            ensure!(
                page.offset == offset,
                "{index_uid} returned offset {} while {offset} was requested",
                page.offset
            );
            if let Some(total) = expected_total {
                ensure!(
                    page.total == total,
                    "{index_uid} changed while its test inventory was read"
                );
            } else {
                expected_total = Some(page.total);
            }
            if page.results.is_empty() {
                ensure!(
                    offset >= page.total,
                    "{index_uid} returned an incomplete document page"
                );
                break;
            }
            offset += page.results.len();
            documents.extend(page.results);
            if offset >= page.total {
                break;
            }
        }
        ensure!(
            documents.len() == expected_total.unwrap_or_default(),
            "{index_uid} inventory count changed while it was read"
        );
        documents.sort_by_key(|document| {
            (
                document["id"].as_str().unwrap_or_default().to_string(),
                document.to_string(),
            )
        });
        inventory.insert(index_uid.to_string(), documents);
    }
    Ok(inventory)
}

async fn require_empty_fixed_indexes(admin: &MeiliAdmin) -> AnyResult<()> {
    for index_uid in FIXED_INDEXES {
        let page = admin
            .fetch_documents_page(index_uid, 0, 1)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        ensure!(
            page.total == 0,
            "isolated migration test refused to use {index_uid}: found {} existing documents",
            page.total
        );
    }
    Ok(())
}

fn company_context_document(tenant_id: &str, run_id: &str, ordinal: usize) -> Value {
    let parent_uri = format!("ctx://company/pr1-cap-{run_id}");
    let uri = format!("{parent_uri}/node-{ordinal:04}");
    let node = ContextNode {
        uri: uri.clone(),
        title: format!("PR1 cap node {ordinal}"),
        layer: 2,
        body: format!("PR1 company-context cap characterization node {ordinal}"),
        tenant_id: tenant_id.to_string(),
        owner_user_id: None,
        index_uid: "rag_company_context".to_string(),
        index_kind: "company".to_string(),
        ancestor_uris: vec!["ctx://company".to_string(), parent_uri.clone()],
        node_kind: "fragment".to_string(),
        retrieval_role: "fragment".to_string(),
        retrieval_enabled: true,
        parent_uri: Some(parent_uri),
        source_document_uri: None,
        fragment_index: Some(ordinal as u32),
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
        privacy: "company".to_string(),
        updated_at: chrono::Utc::now(),
    };
    let mut document = serde_json::to_value(node).expect("ContextNode should serialize");
    document
        .as_object_mut()
        .expect("ContextNode should serialize as an object")
        .insert(
            "id".to_string(),
            json!(format!("pr1-cap-{run_id}-{ordinal}")),
        );
    document
}

fn tenant_context_node(tenant_id: &str, uri: &str, source_id: &str, marker: &str) -> ContextNode {
    ContextNode {
        uri: uri.to_string(),
        title: format!("Tenant fixture {marker}"),
        layer: 2,
        body: marker.to_string(),
        tenant_id: tenant_id.to_string(),
        owner_user_id: None,
        index_uid: "rag_company_context".to_string(),
        index_kind: "company".to_string(),
        ancestor_uris: vec!["ctx://company".to_string()],
        node_kind: "fragment".to_string(),
        retrieval_role: "fragment".to_string(),
        retrieval_enabled: true,
        parent_uri: Some("ctx://company".to_string()),
        source_document_uri: None,
        fragment_index: Some(0),
        char_start: Some(0),
        char_end: Some(marker.len()),
        token_estimate: Some(1),
        checksum: Some(format!("checksum-{marker}")),
        source_id: Some(source_id.to_string()),
        revision_id: Some("shared-revision".to_string()),
        block_type: Some("paragraph".to_string()),
        page_idx: Some(1),
        bbox: None,
        section_path: Vec::new(),
        heading_level: None,
        asset_refs: Vec::new(),
        artifact_refs: Vec::new(),
        status: "active".to_string(),
        privacy: "company".to_string(),
        updated_at: chrono::Utc::now(),
    }
}

fn personal_context_node(
    tenant_id: &str,
    index_uid: &str,
    owner_user_id: &str,
    base_uri: &str,
    ordinal: usize,
    marker: &str,
    updated_at: chrono::DateTime<chrono::Utc>,
) -> ContextNode {
    ContextNode {
        uri: format!("{base_uri}/node-{ordinal}"),
        title: format!("Legacy personal node {ordinal}"),
        layer: 2,
        body: format!("{marker} node {ordinal}"),
        tenant_id: tenant_id.to_string(),
        owner_user_id: Some(owner_user_id.to_string()),
        index_uid: index_uid.to_string(),
        index_kind: "personal".to_string(),
        ancestor_uris: vec![
            "ctx://user".to_string(),
            "ctx://user/upgrade".to_string(),
            base_uri.to_string(),
        ],
        node_kind: "fragment".to_string(),
        retrieval_role: "fragment".to_string(),
        retrieval_enabled: true,
        parent_uri: Some(base_uri.to_string()),
        source_document_uri: None,
        fragment_index: Some(ordinal as u32),
        char_start: None,
        char_end: None,
        token_estimate: None,
        checksum: Some(format!("legacy-personal-checksum-{ordinal}")),
        source_id: None,
        revision_id: None,
        block_type: Some("paragraph".to_string()),
        page_idx: Some(ordinal as u32 + 1),
        bbox: None,
        section_path: Vec::new(),
        heading_level: None,
        asset_refs: Vec::new(),
        artifact_refs: Vec::new(),
        status: "active".to_string(),
        privacy: "private".to_string(),
        updated_at,
    }
}

async fn try_call(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
) -> Result<(StatusCode, Value), String> {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .map_err(|error| format!("request construction failed: {error}"))?;
    let response = app
        .oneshot(request)
        .await
        .map_err(|error| format!("router request failed: {error}"))?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|error| format!("response body failed: {error}"))?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).map_err(|error| format!("response JSON failed: {error}"))?
    };
    Ok((status, value))
}

async fn try_call_with_token(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
    token: &str,
) -> Result<(StatusCode, Value), String> {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .map_err(|error| format!("request construction failed: {error}"))?;
    let response = app
        .oneshot(request)
        .await
        .map_err(|error| format!("router request failed: {error}"))?;
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .map_err(|error| format!("response body failed: {error}"))?;
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).map_err(|error| format!("response JSON failed: {error}"))?
    };
    Ok((status, value))
}

#[tokio::test]
async fn meili_persists_final_shared_mutation_and_auth_denial_audits_without_raw_inputs() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-audit-{}", uuid::Uuid::now_v7());
    let Some(mut config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let writer_token = format!("audit-writer-token-{}", uuid::Uuid::now_v7());
    let raw_owner_id = format!("raw-audit-owner-{}", uuid::Uuid::now_v7());
    let raw_source_id = format!("raw-audit-source-{}", uuid::Uuid::now_v7());
    let raw_body_marker = format!("raw-audit-body-{}", uuid::Uuid::now_v7());
    config.allow_unsafe_unauthenticated = false;
    config.auth_users = vec![AuthUserConfig {
        token: writer_token.clone(),
        scope: AuthUserScope::Owner {
            owner_user_id: raw_owner_id.clone(),
        },
        roles: vec!["user".to_string(), "company_writer".to_string()],
    }];

    prepare_audit_records_index(&config)
        .await
        .expect("audit migration prerequisite should converge");
    let (state, _, app) = start_meili_app(&config).await;
    let admin = state.meili.clone();

    let exercise = async {
        let (success_status, success_body) = try_call_with_token(
            app.clone(),
            Method::POST,
            "/v1/state/company-docs/preflight",
            json!({
                "title": raw_body_marker.clone(),
                "text_preview": "shared audit persistence integration",
                "checksum": "audit-persistence-checksum"
            }),
            &writer_token,
        )
        .await?;
        if success_status != StatusCode::OK {
            return Err(format!("audited mutation failed: {success_body}"));
        }

        let (denied_status, denied_body) = try_call(
            app.clone(),
            Method::POST,
            &format!("/v1/state/company-docs/{raw_source_id}/revisions"),
            json!({ "content": raw_body_marker.clone(), "ingest": false }),
        )
        .await?;
        if denied_status != StatusCode::UNAUTHORIZED {
            return Err(format!(
                "unauthenticated mutation returned {denied_status}: {denied_body}"
            ));
        }

        let response: nowledge::meili::SearchResponse<Value> = admin
            .search(
                "rag_audit_records",
                json!({
                    "q": "",
                    "limit": 10,
                    "filter": TenantFilter::new(&tenant_id)
                        .map_err(|error| format!("audit tenant filter failed: {error}"))?
                        .finish(),
                    "sort": ["occurred_at:asc", "id:asc"]
                }),
            )
            .await
            .map_err(|error| format!("audit search failed: {error}"))?;
        if response.hits.len() != 2 {
            return Err(format!(
                "expected one finalized mutation and one denial, got {:?}",
                response.hits
            ));
        }

        let success = response
            .hits
            .iter()
            .find(|hit| hit["outcome"] == "success")
            .ok_or_else(|| format!("missing success audit: {:?}", response.hits))?;
        let denial = response
            .hits
            .iter()
            .find(|hit| hit["outcome"] == "denied")
            .ok_or_else(|| format!("missing denial audit: {:?}", response.hits))?;
        if success["action"] != "company_doc.preflight"
            || success["principal_scope"] != "owner"
            || !success["principal_owner_user_id_hash"]
                .as_str()
                .is_some_and(|value| value.starts_with("hmac:"))
        {
            return Err(format!("unexpected success audit: {success}"));
        }
        if denial["action"] != "company_doc.create_revision"
            || denial["principal_scope"] != "unauthenticated"
            || denial["reason_code"] != "authentication_failed"
            || denial["error_kind"] != "unauthorized"
        {
            return Err(format!("unexpected denial audit: {denial}"));
        }
        for hit in &response.hits {
            if hit["outcome"] == "attempted"
                || !hit["logical_id"]
                    .as_str()
                    .is_some_and(|value| value.starts_with("audit_"))
                || uuid::Uuid::parse_str(hit["request_id"].as_str().unwrap_or_default()).is_err()
                || !hit["resource_id_hash"]
                    .as_str()
                    .is_some_and(|value| value.starts_with("hmac:"))
            {
                return Err(format!("invalid persisted audit record: {hit}"));
            }
        }
        let encoded = serde_json::to_string(&response.hits)
            .map_err(|error| format!("audit serialization failed: {error}"))?;
        for forbidden in [
            raw_owner_id.as_str(),
            raw_source_id.as_str(),
            raw_body_marker.as_str(),
            writer_token.as_str(),
            "/v1/state/company-docs/",
        ] {
            if encoded.contains(forbidden) {
                return Err(format!(
                    "raw audit input leaked into durable records: {forbidden}"
                ));
            }
        }
        Ok::<(), String>(())
    }
    .await;

    let cleanup = delete_tenant_rows_and_wait(&admin, &tenant_id, &["rag_audit_records"]).await;
    assert_cleanup_results(vec![("durable audit records", cleanup)]);
    exercise.expect("durable shared-mutation audit integration should pass");
}

#[tokio::test]
async fn meili_company_context_hydration_reads_all_2001_documents() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let admin = bootstrapped_meili_admin(&config).await;
    let repository = MeiliRepository::new(admin.clone(), true);
    let run_id = uuid::Uuid::now_v7().to_string();
    let documents = (0..2001)
        .map(|ordinal| company_context_document(&tenant_id, &run_id, ordinal))
        .collect::<Vec<_>>();

    let add_task_result = admin.add_documents("rag_company_context", &documents).await;
    let wait_result = match &add_task_result {
        Ok(task_uid) => wait_for_optional_task(&admin, task_uid.clone()).await,
        Err(error) => Err(format!("write was not accepted: {error}")),
    };
    let loaded = if wait_result.is_ok() {
        Some(repository.list_company_context_nodes(&tenant_id).await)
    } else {
        None
    };
    let cleanup = delete_by_filter_and_wait(
        &admin,
        "rag_company_context",
        &equality_filter("tenant_id", &tenant_id),
    )
    .await;

    assert_cleanup_results(vec![("company context cap fixture", cleanup)]);
    add_task_result.expect("company ContextNode documents should be accepted");
    wait_result.expect("company ContextNode write should complete");

    let loaded = loaded
        .expect("company ContextNode scan should run after a completed write")
        .expect("company ContextNode scan should succeed")
        .expect("Meili repository should return persisted company ContextNodes");
    assert_eq!(
        loaded.len(),
        2001,
        "paginated hydration should return every persisted company ContextNode"
    );
    assert!(loaded.iter().all(|node| node.tenant_id == tenant_id));
}

#[tokio::test]
async fn meili_backend_creates_dynamic_user_indexes_and_searches_events() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");
    let ensured_result = try_call(
        app.clone(),
        Method::PUT,
        &format!("/v1/history/users/{owner}/event-index"),
        json!({ "create_personal_context_index": true }),
    )
    .await;

    let text = format!("meili-only-keyword-{}", uuid::Uuid::now_v7());
    let event_result = try_call(
        app.clone(),
        Method::POST,
        &format!("/v1/history/users/{owner}/events"),
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "n1",
            "owner_user_id": owner,
            "occurred_at": "2026-05-12T00:00:00Z",
            "observed_at": "2026-05-12T00:01:00Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "n1" },
            "text": text,
            "privacy": "private"
        }),
    )
    .await;
    let search_result = try_call(
        app,
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "query": text, "limit": 5 }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (ensure_status, ensured) = ensured_result.expect("index ensure call should finish");
    let (event_status, event) = event_result.expect("event write call should finish");
    let (search_status, search) = search_result.expect("event search call should finish");
    assert_eq!(ensure_status, StatusCode::OK, "{ensured}");
    assert!(!ensured["routing"]["event_index_uid"]
        .as_str()
        .unwrap()
        .contains(&owner));
    assert!(!ensured["meili_task_uids"].as_array().unwrap().is_empty());
    assert!(ensured["meili_task_uids"]
        .as_array()
        .unwrap()
        .iter()
        .all(|uid| uid.as_str().unwrap().chars().all(|c| c.is_ascii_digit())));
    assert_eq!(event_status, StatusCode::OK, "{event}");
    assert!(event["meili_task_uid"]
        .as_str()
        .is_some_and(|uid| uid.chars().all(|character| character.is_ascii_digit())));
    assert_eq!(search_status, StatusCode::OK, "{search}");
    assert_eq!(search["hits"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn meili_backend_indexes_internal_state_events() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let fact_key = format!("state-{}", uuid::Uuid::now_v7());
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");
    let state_result = try_call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "preference",
            "title": "Meili state event",
            "statement": "state changed should be indexed in Meili"
        }),
    )
    .await;
    let history_result = try_call(
        app,
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "event_types": ["state.changed"], "limit": 10 }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "state items",
            delete_by_filter_and_wait(&admin, "rag_state_items", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (state_status, state_body) = state_result.expect("state write call should finish");
    let (history_status, history) = history_result.expect("history search call should finish");
    assert_eq!(state_status, StatusCode::OK, "{state_body}");
    assert_eq!(history_status, StatusCode::OK, "{history}");
    assert_eq!(history["hits"].as_array().unwrap().len(), 1);
    assert_eq!(history["hits"][0]["event_type"], "state.changed");
}

#[tokio::test]
async fn meili_backend_context_search_retrieves_fragments_only() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let fact_key = format!("doc-{}", uuid::Uuid::now_v7());
    let keyword = format!("meili-fragment-keyword-{}", uuid::Uuid::now_v7());
    let content = format!("# Meili Source\n\n{}", format!("{keyword} ").repeat(180));
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");
    let state_result = try_call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "status",
            "title": "Meili source document",
            "statement": "Short current-state summary",
            "document": {
                "content": content,
                "content_type": "text/markdown",
                "source_uri": "https://example.test/meili/source"
            }
        }),
    )
    .await;
    let source_document_uri = state_result
        .as_ref()
        .ok()
        .and_then(|(_, state)| {
            state["item"]["source_refs"][0]["meta"]["source_document_uri"].as_str()
        })
        .unwrap_or("ctx://missing-source-document")
        .to_string();

    let source_docs_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/debug/meili/search",
        json!({ "index_uid": "rag_source_documents", "query": source_document_uri }),
    )
    .await;

    let search_result = try_call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({ "owner_user_id": owner, "query": keyword, "limit": 5 }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "state items",
            delete_by_filter_and_wait(&admin, "rag_state_items", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (state_status, state_body) = state_result.expect("state document call should finish");
    let (source_docs_status, source_docs) =
        source_docs_result.expect("source document search should finish");
    let (search_status, search) = search_result.expect("context search should finish");
    assert_eq!(state_status, StatusCode::OK, "{state_body}");
    assert_ne!(source_document_uri, "ctx://missing-source-document");
    assert_eq!(source_docs_status, StatusCode::OK, "{source_docs}");
    assert!(source_docs["hits"].as_array().unwrap().iter().any(|hit| {
        hit["uri"].as_str() == Some(source_document_uri.as_str())
            && hit["retrieval_enabled"] == false
    }));
    assert_eq!(search_status, StatusCode::OK, "{search}");
    assert!(!search["hits"].as_array().unwrap().is_empty(), "{search}");
    assert!(search["hits"].as_array().unwrap().iter().all(|hit| {
        hit["node_kind"] == "fragment"
            && hit["retrieval_role"] == "fragment"
            && hit["uri"].as_str() != Some(source_document_uri.as_str())
    }));
}

#[tokio::test]
async fn meili_backend_context_search_applies_structured_filters() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let owner = format!("u-{}", uuid::Uuid::now_v7());
    let source_a = format!("meili-filter-a-{}", uuid::Uuid::now_v7());
    let source_b = format!("meili-filter-b-{}", uuid::Uuid::now_v7());
    let keyword = format!("meili-filter-keyword-{}", uuid::Uuid::now_v7());
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");

    let ingest_a_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": owner,
            "source_id": source_a,
            "revision_id": "v1",
            "title": "Meili Filter A",
            "content": "source a",
            "content_list_v2": [
                {
                    "type": "table",
                    "html": format!("<table><tr><td>{keyword} table row</td></tr></table>"),
                    "page_idx": 1,
                    "bbox": [0, 0, 10, 10],
                    "reading_order": 0
                },
                {
                    "type": "paragraph",
                    "text": format!("{keyword} paragraph page three"),
                    "page_idx": 3,
                    "bbox": [1, 1, 11, 11],
                    "reading_order": 1
                }
            ]
        }),
    )
    .await;

    let ingest_b_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "owner_user_id": owner,
            "source_id": source_b,
            "revision_id": "v1",
            "title": "Meili Filter B",
            "content": "source b",
            "content_list_v2": [
                {
                    "type": "table",
                    "html": format!("<table><tr><td>{keyword} other table</td></tr></table>"),
                    "page_idx": 1,
                    "bbox": [2, 2, 12, 12],
                    "reading_order": 0
                }
            ]
        }),
    )
    .await;

    let table_search_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": owner,
            "query": keyword,
            "filters": { "block_type": "table", "source_id": source_a },
            "limit": 10
        }),
    )
    .await;

    let page_search_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": owner,
            "query": keyword,
            "filters": { "page_idx_gte": 3, "page_idx_lte": 3 },
            "limit": 10
        }),
    )
    .await;

    let debug_search_result = try_call(
        app,
        Method::POST,
        "/v1/context/search",
        json!({
            "owner_user_id": owner,
            "query": keyword,
            "debug": true,
            "limit": 5
        }),
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "ingest tasks",
            delete_by_filter_and_wait(&admin, "rag_ingest_tasks", &tenant_filter).await,
        ),
        (
            "ingest results",
            delete_by_filter_and_wait(&admin, "rag_ingest_results", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (ingest_a_status, ingest_a) = ingest_a_result.expect("first ingest call should finish");
    let (ingest_b_status, ingest_b) = ingest_b_result.expect("second ingest call should finish");
    let (table_status, table_search) =
        table_search_result.expect("table-filter search should finish");
    let (page_status, page_search) = page_search_result.expect("page-filter search should finish");
    let (debug_status, debug_search) =
        debug_search_result.expect("debug context search should finish");
    assert_eq!(ingest_a_status, StatusCode::OK, "{ingest_a}");
    assert_eq!(ingest_b_status, StatusCode::OK, "{ingest_b}");
    assert_eq!(table_status, StatusCode::OK, "{table_search}");
    assert_eq!(table_search["hits"].as_array().unwrap().len(), 1);
    assert_eq!(table_search["hits"][0]["block_type"], "table");
    assert_eq!(table_search["hits"][0]["source_id"], source_a);
    assert!(!table_search.to_string().contains("index_uid"));
    assert!(!table_search.to_string().contains("\"filter\""));
    assert_eq!(page_status, StatusCode::OK, "{page_search}");
    assert!(!page_search["hits"].as_array().unwrap().is_empty());
    assert!(page_search["hits"]
        .as_array()
        .unwrap()
        .iter()
        .all(|hit| hit["page_idx"] == 3));
    assert_eq!(debug_status, StatusCode::OK, "{debug_search}");
    assert!(debug_search["stages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|stage| stage.get("raw_stage_debug").is_some()));
}

#[tokio::test]
async fn meili_reconciled_harness_indexes_accept_and_search_changes() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, _state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();

    let change_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/admin/harness/evolution/changes",
        json!({
            "iteration": 1,
            "type": "improvement",
            "component_id": "retrieval.context_search",
            "files": ["src/store.rs"],
            "failure_pattern": "meili_harness_change_pattern",
            "root_cause": "test",
            "targeted_fix": "test",
            "predicted_fixes": ["meili_harness_change_pattern"],
            "risk_cases": [],
            "expected_metric_deltas": { "pass_rate": 1.0 },
            "why_this_component": "test",
            "created_by": "test"
        }),
    )
    .await;

    let search_result = try_call(
        app,
        Method::POST,
        "/v1/debug/meili/search",
        json!({
            "index_uid": "rag_harness_changes",
            "query": "meili_harness_change_pattern"
        }),
    )
    .await;

    let cleanup = delete_by_filter_and_wait(
        &admin,
        "rag_harness_changes",
        &equality_filter("tenant_id", &tenant_id),
    )
    .await;
    assert_cleanup_results(vec![("harness changes", cleanup)]);

    let (change_status, change) = change_result.expect("harness change call should finish");
    let (search_status, search) = search_result.expect("harness search call should finish");
    assert_eq!(change_status, StatusCode::OK, "{change}");
    assert_eq!(search_status, StatusCode::OK, "{search}");
    assert!(search["hits"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| { hit["id"].as_str() == change["id"].as_str() }));
}

#[tokio::test]
async fn meili_hydrates_harness_eval_and_ingest_metadata_into_fresh_app() {
    let _guard = live_meili_test_guard().await;
    let Some((config, admin, state, app)) = meili_fixture().await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_id = config.tenant_id.clone();
    let fixture_id = uuid::Uuid::now_v7().to_string();
    let eval_case_id = format!("hydration-eval-case-{fixture_id}");
    let source_id = format!("hydration-ingest-fixture-{fixture_id}");
    let parsed_block_marker = format!("hydration-parsed-block-{fixture_id}");
    let company_routing = state
        .store
        .resolver()
        .resolve(&tenant_id, "company", false, true)
        .expect("company routing should resolve");

    let change_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/admin/harness/evolution/changes",
        json!({
            "iteration": 1,
            "type": "improvement",
            "component_id": "retrieval.context_search",
            "files": ["src/store.rs"],
            "failure_pattern": "hydration_failure_pattern",
            "root_cause": "test",
            "targeted_fix": "test",
            "predicted_fixes": [eval_case_id],
            "risk_cases": [],
            "expected_metric_deltas": { "pass_rate": 1.0 },
            "why_this_component": "test",
            "created_by": "test"
        }),
    )
    .await;
    let change_id = change_result
        .as_ref()
        .ok()
        .and_then(|(_, change)| change["id"].as_str())
        .unwrap_or("missing-change")
        .to_string();

    let eval_case_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/eval/cases",
        json!({
            "id": eval_case_id,
            "question": "hydration eval should persist",
            "expected_answer_contains": ["unlikely-token"]
        }),
    )
    .await;

    let eval_run_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/eval/runs",
        json!({ "case_ids": [eval_case_id], "change_id": change_id }),
    )
    .await;
    let run_id = eval_run_result
        .as_ref()
        .ok()
        .and_then(|(_, run)| run["id"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("missing-run-{fixture_id}"));

    let ingest_result = try_call(
        app,
        Method::POST,
        "/v1/ingest/files:sync",
        json!({
            "source_id": source_id,
            "revision_id": "v1",
            "title": "Hydration Ingest Fixture",
            "content": "hydration ingest metadata should persist",
            "content_list_v2": [{
                "type": "paragraph",
                "text": parsed_block_marker,
                "page_idx": 1,
                "reading_order": 0
            }]
        }),
    )
    .await;
    let task_id = ingest_result
        .as_ref()
        .ok()
        .and_then(|(_, ingest)| ingest["task"]["task_id"].as_str())
        .map(str::to_string)
        .unwrap_or_else(|| format!("missing-task-{fixture_id}"));

    let (_, hydrated, fresh) = start_meili_app(&config).await;

    let hydrated_change_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/admin/harness/evolution/changes/{change_id}"),
        Value::Null,
    )
    .await;

    let hydrated_report_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/eval/runs/{run_id}/report"),
        Value::Null,
    )
    .await;

    let hydrated_ingest_result = try_call(
        fresh,
        Method::GET,
        &format!("/v1/ingest/tasks/{task_id}/result"),
        Value::Null,
    )
    .await;

    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let persisted_run_inventory_result = admin
        .search::<Value>(
            "rag_eval_runs",
            json!({ "q": "", "filter": tenant_filter, "limit": 100 }),
        )
        .await;
    let case_filter = tenant_resource_filter(&tenant_id, "case_id", &eval_case_id);
    let persisted_case_result_inventory_result = admin
        .search::<Value>(
            "rag_eval_case_results",
            json!({ "q": "", "filter": case_filter, "limit": 100 }),
        )
        .await;
    let mut cleanup_run_ids = persisted_run_inventory_result
        .as_ref()
        .map(|response| {
            response
                .hits
                .iter()
                .filter_map(|run| {
                    run["logical_id"]
                        .as_str()
                        .or_else(|| run["id"].as_str())
                        .map(str::to_string)
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if let Ok(response) = &persisted_case_result_inventory_result {
        cleanup_run_ids.extend(
            response
                .hits
                .iter()
                .filter_map(|result| result["run_id"].as_str().map(str::to_string)),
        );
    }
    if !cleanup_run_ids.contains(&run_id) {
        cleanup_run_ids.push(run_id.clone());
    }
    cleanup_run_ids.sort();
    cleanup_run_ids.dedup();

    let mut cleanup_results = vec![
        (
            "eval run inventory",
            persisted_run_inventory_result
                .as_ref()
                .map(|_| ())
                .map_err(ToString::to_string),
        ),
        (
            "eval case-result inventory",
            persisted_case_result_inventory_result
                .as_ref()
                .map(|_| ())
                .map_err(ToString::to_string),
        ),
        (
            "harness changes",
            delete_by_filter_and_wait(&admin, "rag_harness_changes", &tenant_filter).await,
        ),
        (
            "harness verdicts",
            delete_by_filter_and_wait(&admin, "rag_harness_verdicts", &tenant_filter).await,
        ),
        (
            "company context",
            delete_by_filter_and_wait(&admin, "rag_company_context", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "ingest tasks",
            delete_by_filter_and_wait(&admin, "rag_ingest_tasks", &tenant_filter).await,
        ),
        (
            "ingest results",
            delete_by_filter_and_wait(&admin, "rag_ingest_results", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "company event index",
            delete_index_and_wait(&config, &admin, &company_routing.event_index_uid).await,
        ),
        (
            "company context index",
            delete_index_and_wait(&config, &admin, &company_routing.personal_context_index_uid)
                .await,
        ),
    ];
    let inventory_is_recoverable = persisted_run_inventory_result.is_ok()
        || persisted_case_result_inventory_result.is_ok()
        || !run_id.starts_with("missing-run-");
    let mut eval_children_clean = inventory_is_recoverable;
    for cleanup_run_id in &cleanup_run_ids {
        let run_filter = tenant_resource_filter(&tenant_id, "run_id", cleanup_run_id);
        let overview_cleanup =
            delete_by_filter_and_wait(&admin, "rag_eval_overviews", &run_filter).await;
        eval_children_clean &= overview_cleanup.is_ok();
        cleanup_results.push(("eval overviews", overview_cleanup));
    }
    let case_results_cleanup =
        delete_by_filter_and_wait(&admin, "rag_eval_case_results", &case_filter).await;
    eval_children_clean &= case_results_cleanup.is_ok();
    cleanup_results.push(("eval case results", case_results_cleanup));

    let eval_runs_cleanup = if eval_children_clean {
        delete_by_filter_and_wait(&admin, "rag_eval_runs", &tenant_filter).await
    } else {
        Err("preserved parent eval runs because child cleanup or inventory failed".to_string())
    };
    let eval_runs_clean = eval_runs_cleanup.is_ok();
    cleanup_results.push(("eval runs", eval_runs_cleanup));

    let eval_cases_cleanup = if eval_runs_clean {
        delete_by_filter_and_wait(&admin, "rag_eval_cases", &tenant_filter).await
    } else {
        Err("preserved eval case because its run cleanup was incomplete".to_string())
    };
    cleanup_results.push(("eval cases", eval_cases_cleanup));
    assert_cleanup_results(cleanup_results);

    let (change_status, change) = change_result.expect("harness change call should finish");
    let (eval_case_status, eval_case) = eval_case_result.expect("eval case call should finish");
    let (eval_run_status, eval_run) = eval_run_result.expect("eval run call should finish");
    let (ingest_status, ingest) = ingest_result.expect("ingest call should finish");
    let (hydrated_change_status, hydrated_change) =
        hydrated_change_result.expect("hydrated change call should finish");
    let (hydrated_report_status, hydrated_report) =
        hydrated_report_result.expect("hydrated report call should finish");
    let (hydrated_ingest_status, hydrated_ingest) =
        hydrated_ingest_result.expect("hydrated ingest result call should finish");

    assert_eq!(change_status, StatusCode::OK, "{change}");
    assert_ne!(change_id, "missing-change");
    assert_eq!(eval_case_status, StatusCode::OK, "{eval_case}");
    assert_eq!(eval_run_status, StatusCode::OK, "{eval_run}");
    assert_eq!(ingest_status, StatusCode::OK, "{ingest}");
    assert!(hydrated["harness_changes"].as_u64().unwrap_or_default() >= 1);
    assert_eq!(hydrated_change_status, StatusCode::OK, "{hydrated_change}");
    assert_eq!(hydrated_change["id"], change["id"]);
    assert_eq!(hydrated_report_status, StatusCode::OK, "{hydrated_report}");
    assert_eq!(hydrated_report["run"]["id"], eval_run["id"]);
    assert_eq!(hydrated_report["case_results"][0]["case_id"], eval_case_id);
    assert_eq!(hydrated_ingest_status, StatusCode::OK, "{hydrated_ingest}");
    assert_eq!(hydrated_ingest["task"]["task_id"], task_id);
    assert!(hydrated_ingest["parse_artifacts"]
        .as_array()
        .is_some_and(|artifacts| artifacts.iter().any(|artifact| {
            artifact["artifact_kind"] == "content_list_v2" && artifact["source_id"] == source_id
        })));
    assert!(hydrated_ingest["parsed_blocks"]
        .as_array()
        .is_some_and(|blocks| blocks.iter().any(|block| {
            block["type"] == "paragraph" && block["text"] == parsed_block_marker
        })));
}

#[tokio::test]
async fn meili_restart_hydrates_company_context_sources_and_revisions() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let (state, _, app) = start_meili_app(&config).await;
    let admin = state.meili.clone();
    let run_id = uuid::Uuid::now_v7().to_string();
    let source_id = format!("pr1-company-restart-{run_id}");
    let marker = format!("pr1-company-restart-marker-{run_id}");
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, "company", false, true)
        .expect("company history routing should resolve");

    let revision_result = try_call(
        app.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "PR1 company restart fixture",
            "source_uri": format!("https://example.test/pr1-company-restart/{run_id}"),
            "content": format!("# Restart fixture\n\n{marker}"),
            "checksum": format!("pr1-company-restart-checksum-{run_id}")
        }),
    )
    .await;
    let revision_id = revision_result
        .as_ref()
        .ok()
        .and_then(|(_, revision)| revision["revision_id"].as_str())
        .unwrap_or("missing-revision")
        .to_string();

    let activation_result = try_call(
        app,
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_id}/activate"),
        json!({ "reason": "PR1 restart characterization" }),
    )
    .await;
    let source_document_uri = activation_result
        .as_ref()
        .ok()
        .and_then(|(_, activation)| activation["source_document_uri"].as_str())
        .unwrap_or("ctx://missing-source-document")
        .to_string();

    let (fresh_state, hydration, fresh) = start_meili_app(&config).await;
    let document_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let revisions_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/history/company-docs/{source_id}/revisions"),
        Value::Null,
    )
    .await;
    let context_result = try_call(
        fresh.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker, "limit": 10 }),
    )
    .await;
    let delete_result = try_call(
        fresh.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let immediate_source_document_result = fresh_state
        .store
        .fs_read_async(&tenant_id, &source_document_uri, None, false)
        .await;
    let immediate_document_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let immediate_context_result = try_call(
        fresh,
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker, "limit": 10 }),
    )
    .await;
    fresh_state.shutdown().await;

    let (deleted_state, deleted_hydration, deleted_app) = start_meili_app(&config).await;
    let restarted_source_document_result = deleted_state
        .store
        .fs_read_async(&tenant_id, &source_document_uri, None, false)
        .await;
    let restarted_document_result = try_call(
        deleted_app.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let restarted_context_result = try_call(
        deleted_app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker, "limit": 10 }),
    )
    .await;
    let restarted_links_result = try_call(
        deleted_app,
        Method::POST,
        "/v1/links/search",
        json!({ "uri": source_document_uri, "limit": 100 }),
    )
    .await;
    // Teardown is independent of the behavior under test. Every operation is
    // attempted before any assertion so a failed hydration/read contract does
    // not strand shared-backend fixtures.
    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let source_filter = tenant_resource_filter(&tenant_id, "source_id", &source_id);
    let cleanup_results = vec![
        (
            "company context",
            delete_by_filter_and_wait(&admin, "rag_company_context", &source_filter).await,
        ),
        (
            "source revisions",
            delete_by_filter_and_wait(&admin, "rag_source_revisions", &source_filter).await,
        ),
        (
            "source pointer",
            delete_by_filter_and_wait(
                &admin,
                "rag_sources",
                &tenant_logical_filter(&tenant_id, &source_id),
            )
            .await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &source_filter).await,
        ),
        (
            "parse artifacts",
            delete_by_filter_and_wait(&admin, "rag_parse_artifacts", &source_filter).await,
        ),
        (
            "ingest tasks",
            delete_by_filter_and_wait(&admin, "rag_ingest_tasks", &source_filter).await,
        ),
        (
            "ingest results",
            delete_by_filter_and_wait(&admin, "rag_ingest_results", &source_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "operations",
            delete_by_filter_and_wait(&admin, "rag_operations", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "company event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "company context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (revision_status, revision) = revision_result.expect("company revision call should finish");
    assert_eq!(revision_status, StatusCode::OK, "{revision}");
    assert_ne!(revision_id, "missing-revision");
    let (activation_status, activation) =
        activation_result.expect("company activation call should finish");
    assert_eq!(activation_status, StatusCode::OK, "{activation}");
    assert_ne!(source_document_uri, "ctx://missing-source-document");
    let (document_status, document) = document_result.expect("company document call should finish");
    let (revisions_status, revisions) =
        revisions_result.expect("company revisions call should finish");
    let (context_status, context) = context_result.expect("company context search should finish");
    for key in [
        "company_context_nodes",
        "company_sources",
        "source_revisions",
    ] {
        assert!(
            hydration[key].as_u64().is_some_and(|count| count >= 1),
            "current company hydration should report {key}: {hydration}"
        );
    }
    assert_eq!(document_status, StatusCode::OK, "{document}");
    assert_eq!(document["source_id"], source_id);
    assert_eq!(document["revision_id"], revision_id);
    assert_eq!(revisions_status, StatusCode::OK, "{revisions}");
    assert!(revisions["revisions"]
        .as_array()
        .unwrap()
        .iter()
        .any(|candidate| candidate["id"] == revision_id));
    assert_eq!(context_status, StatusCode::OK, "{context}");
    assert!(context["hits"]
        .as_array()
        .unwrap()
        .iter()
        .any(|hit| hit["source_id"] == source_id));
    let (delete_status, deleted) = delete_result.expect("company delete call should finish");
    assert_eq!(delete_status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["deleted"], true, "{deleted}");
    assert!(
        immediate_source_document_result.is_err(),
        "deleted source document remained in the live ContextFS cache"
    );
    let (immediate_document_status, immediate_document) =
        immediate_document_result.expect("immediate company read should finish");
    assert_eq!(
        immediate_document_status,
        StatusCode::NOT_FOUND,
        "{immediate_document}"
    );
    let (immediate_context_status, immediate_context) =
        immediate_context_result.expect("immediate company context search should finish");
    assert_eq!(
        immediate_context_status,
        StatusCode::OK,
        "{immediate_context}"
    );
    assert!(
        immediate_context["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|hit| hit["source_id"] != source_id),
        "{immediate_context}"
    );

    for key in [
        "company_context_nodes",
        "company_sources",
        "source_revisions",
    ] {
        assert_eq!(
            deleted_hydration[key], 0,
            "deleted company hydration should report no {key}: {deleted_hydration}"
        );
    }
    assert!(
        restarted_source_document_result.is_err(),
        "deleted source document reappeared through ContextFS after restart"
    );
    let (restarted_document_status, restarted_document) =
        restarted_document_result.expect("restarted company read should finish");
    assert_eq!(
        restarted_document_status,
        StatusCode::NOT_FOUND,
        "{restarted_document}"
    );
    let (restarted_context_status, restarted_context) =
        restarted_context_result.expect("restarted company context search should finish");
    assert_eq!(
        restarted_context_status,
        StatusCode::OK,
        "{restarted_context}"
    );
    assert!(
        restarted_context["hits"]
            .as_array()
            .unwrap()
            .iter()
            .all(|hit| hit["source_id"] != source_id),
        "{restarted_context}"
    );
    let (restarted_links_status, restarted_links) =
        restarted_links_result.expect("restarted company link search should finish");
    assert_eq!(restarted_links_status, StatusCode::OK, "{restarted_links}");
    for field in ["links", "outbound", "backlinks"] {
        assert_eq!(
            restarted_links[field].as_array().map(Vec::len),
            Some(0),
            "deleted company links survived restart: {restarted_links}"
        );
    }
    deleted_state.shutdown().await;
}

#[tokio::test]
async fn meili_analysis_materialization_confirms_tasks_and_dedupes_after_restart() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let (state, _, _) = start_meili_app(&config).await;
    let admin = state.meili.clone();
    let run_id = uuid::Uuid::now_v7().to_string();
    let owner = format!("pr7-analysis-owner-{run_id}");
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("analysis owner routing should resolve");
    let stable_candidate = AnalysisInsightMaterialization {
        insight_type: "analysis".to_string(),
        title: format!("PR7 stable insight {run_id}"),
        statement: "The first authorized context supports this insight.".to_string(),
        confidence: 0.9,
        salience: 0.7,
        source_uris: vec![format!("ctx://user/pr7-analysis/{run_id}/source-a")],
    };
    let first = state
        .store
        .materialize_analysis_async(
            &tenant_id,
            &owner,
            AnalysisMaterializationRequest {
                links: vec![AnalysisLinkMaterialization {
                    source_uri: format!("ctx://user/pr7-analysis/{run_id}/source-a"),
                    target_uri: format!("ctx://user/pr7-analysis/{run_id}/source-b"),
                    source_title: Some("Source A".to_string()),
                    target_title: Some("Source B".to_string()),
                    relation: "supports".to_string(),
                    rationale: Some("Both locators were authorized before persistence".to_string()),
                    confidence: 0.8,
                    tags: vec!["analysis".to_string()],
                }],
                insights: vec![
                    stable_candidate.clone(),
                    AnalysisInsightMaterialization {
                        insight_type: "analysis".to_string(),
                        title: format!("PR7 old companion {run_id}"),
                        statement: "The second authorized context supports a companion insight."
                            .to_string(),
                        confidence: 0.75,
                        salience: 0.5,
                        source_uris: vec![format!("ctx://user/pr7-analysis/{run_id}/source-b")],
                    },
                ],
            },
        )
        .await
        .expect("Meili analysis materialization should complete");
    let stable_id = first.insights[0].id.clone();
    let first_persistence = first
        .persistence
        .clone()
        .expect("analysis response should expose persistence confirmation");
    let repository = MeiliRepository::new(admin.clone(), true);
    let first_operations = repository
        .list_operations(&tenant_id, &[])
        .await
        .expect("analysis operation lookup should succeed")
        .expect("Meili should return durable operations");
    let first_operation = first_operations
        .iter()
        .find(|operation| operation.id == first_persistence.operation_id)
        .cloned()
        .expect("analysis operation should be persisted");
    state.shutdown().await;

    let (restarted, hydration, _) = start_meili_app(&config).await;
    let overlap = restarted
        .store
        .materialize_analysis_async(
            &tenant_id,
            &owner,
            AnalysisMaterializationRequest {
                links: Vec::new(),
                insights: vec![
                    stable_candidate,
                    AnalysisInsightMaterialization {
                        insight_type: "analysis".to_string(),
                        title: format!("PR7 new companion {run_id}"),
                        statement: "A post-restart context supports a new companion insight."
                            .to_string(),
                        confidence: 0.8,
                        salience: 0.6,
                        source_uris: vec![format!("ctx://user/pr7-analysis/{run_id}/source-c")],
                    },
                ],
            },
        )
        .await
        .expect("overlapping post-restart materialization should complete");
    let persisted_insights = repository
        .list_insights(&tenant_id)
        .await
        .expect("persisted insight lookup should succeed")
        .expect("Meili should return durable insights");
    restarted.shutdown().await;

    let fixed_cleanup = delete_tenant_rows_and_wait(
        &admin,
        &tenant_id,
        &[
            "rag_insights",
            "rag_links",
            "rag_operations",
            "rag_user_event_indexes",
        ],
    )
    .await;
    let event_cleanup = delete_index_and_wait(&config, &admin, &routing.event_index_uid).await;
    let context_cleanup =
        delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await;
    assert_cleanup_results(vec![
        ("analysis fixed rows", fixed_cleanup),
        ("analysis event index", event_cleanup),
        ("analysis context index", context_cleanup),
    ]);

    assert_eq!(first_persistence.status, OperationStatus::Completed);
    assert_eq!(
        first_persistence.indexing_state,
        OperationIndexingState::Completed
    );
    assert_eq!(first_operation.status, OperationStatus::Completed);
    assert_eq!(
        first_operation.indexing_state,
        OperationIndexingState::Completed
    );
    assert!(
        first_operation.progress.steps.values().all(|progress| {
            progress.status == OperationStepStatus::Completed && !progress.task_uids.is_empty()
        }),
        "every materialization step must retain and confirm its Meili task UID: {:?}",
        first_operation.progress.steps
    );
    assert_eq!(hydration["status"], "complete", "{hydration}");
    assert_eq!(overlap.insights[0].id, stable_id);
    assert_eq!(
        persisted_insights
            .iter()
            .filter(|insight| insight.owner_user_id == owner)
            .count(),
        3
    );
    assert_eq!(
        persisted_insights
            .iter()
            .filter(|insight| insight.id == stable_id)
            .count(),
        1
    );
}

#[tokio::test]
async fn meili_restart_restores_registry_state_links_and_sessions() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let (state, _, app) = start_meili_app(&config).await;
    let admin = state.meili.clone();
    let run_id = uuid::Uuid::now_v7().to_string();
    let owner = format!("pr1-restart-owner-{run_id}");
    let fact_key = format!("pr1-restart-fact-{run_id}");
    let marker = format!("pr1-restart-marker-{run_id}");
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");

    let state_created_result = try_call(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/profile/facts/{fact_key}"),
        json!({
            "owner_user_id": owner,
            "state_type": "status",
            "title": "PR1 restart state fact",
            "statement": marker,
            "document": {
                "content": format!("# PR1 restart source\n\n{marker}"),
                "content_type": "text/markdown",
                "source_uri": format!("https://example.test/pr1-state-restart/{run_id}")
            }
        }),
    )
    .await;
    let state_id = state_created_result
        .as_ref()
        .ok()
        .and_then(|(_, state)| state["item"]["id"].as_str())
        .unwrap_or("missing-state")
        .to_string();
    let personal_source_document_uri = state_created_result
        .as_ref()
        .ok()
        .and_then(|(_, state)| {
            state["item"]["source_refs"][0]["meta"]["source_document_uri"].as_str()
        })
        .unwrap_or("ctx://missing-personal-source-document")
        .to_string();

    let source_uri = format!("ctx://company/pr1-restart/{run_id}/source");
    let target_uri = format!("ctx://company/pr1-restart/{run_id}/target");
    let link_created_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/links",
        json!({
            "owner_user_id": owner,
            "source_uri": source_uri,
            "target_uri": target_uri,
            "relation": "supports",
            "rationale": marker
        }),
    )
    .await;
    let link_id = link_created_result
        .as_ref()
        .ok()
        .and_then(|(_, link)| link["link"]["id"].as_str())
        .unwrap_or("missing-link")
        .to_string();

    let session_created_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/sessions",
        json!({ "owner_user_id": owner, "title": "PR1 restart session" }),
    )
    .await;
    let session_id = session_created_result
        .as_ref()
        .ok()
        .and_then(|(_, session)| session["session_id"].as_str())
        .unwrap_or("missing-session")
        .to_string();

    let live_state_result = try_call(
        app.clone(),
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}?owner_user_id={owner}"),
        Value::Null,
    )
    .await;
    let live_links_result = try_call(
        app.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "owner_user_id": owner, "query": marker }),
    )
    .await;
    let live_session_result = try_call(
        app,
        Method::POST,
        &format!("/v1/sessions/{session_id}/messages"),
        json!({
            "role": "user",
            "content": marker,
            "write_history_event": false
        }),
    )
    .await;

    let state_filter = tenant_logical_filter(&tenant_id, &state_id);
    let persisted_state_result = admin
        .search::<Value>(
            "rag_state_items",
            json!({ "q": "", "filter": state_filter, "limit": 10 }),
        )
        .await;
    let link_filter = tenant_logical_filter(&tenant_id, &link_id);
    let persisted_link_result = admin
        .search::<Value>(
            "rag_links",
            json!({ "q": "", "filter": link_filter, "limit": 10 }),
        )
        .await;
    let session_filter = tenant_logical_filter(&tenant_id, &session_id);
    let persisted_session_result = admin
        .search::<Value>(
            "rag_sessions",
            json!({ "q": "", "filter": session_filter, "limit": 10 }),
        )
        .await;

    let (fresh_state, hydration, fresh) = start_meili_app(&config).await;
    let personal_source_document_result = fresh_state
        .store
        .fs_read_async(
            &tenant_id,
            &personal_source_document_uri,
            Some(&owner),
            false,
        )
        .await;
    let fresh_registry_result = try_call(
        fresh.clone(),
        Method::GET,
        "/v1/admin/history/user-event-indexes",
        Value::Null,
    )
    .await;
    let fresh_events_result = try_call(
        fresh.clone(),
        Method::POST,
        &format!("/v1/history/users/{owner}/search"),
        json!({ "event_types": ["state.changed"], "limit": 10 }),
    )
    .await;
    let fresh_personal_context_result = try_call(
        fresh.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "owner_user_id": owner, "query": marker, "limit": 10 }),
    )
    .await;
    let fresh_state_result = try_call(
        fresh.clone(),
        Method::GET,
        &format!("/v1/state/profile/facts/{fact_key}?owner_user_id={owner}"),
        Value::Null,
    )
    .await;
    let fresh_links_result = try_call(
        fresh.clone(),
        Method::POST,
        "/v1/links/search",
        json!({ "owner_user_id": owner, "query": marker }),
    )
    .await;
    let fresh_session_result = try_call(
        fresh,
        Method::POST,
        &format!("/v1/sessions/{session_id}/messages"),
        json!({
            "role": "user",
            "content": "message after restart",
            "write_history_event": false
        }),
    )
    .await;

    // Attempt every scoped cleanup before asserting any observed behavior.
    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "state items",
            delete_by_filter_and_wait(&admin, "rag_state_items", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "source documents",
            delete_by_filter_and_wait(&admin, "rag_source_documents", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "sessions",
            delete_by_filter_and_wait(&admin, "rag_sessions", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (state_created_status, state_created) =
        state_created_result.expect("state creation call should finish");
    assert_eq!(state_created_status, StatusCode::OK, "{state_created}");
    assert_ne!(state_id, "missing-state");
    let (link_created_status, link_created) =
        link_created_result.expect("link creation call should finish");
    assert_eq!(link_created_status, StatusCode::OK, "{link_created}");
    assert_ne!(link_id, "missing-link");
    let (session_created_status, session_created) =
        session_created_result.expect("session creation call should finish");
    assert_eq!(session_created_status, StatusCode::OK, "{session_created}");
    assert_ne!(session_id, "missing-session");
    let (live_state_status, live_state) = live_state_result.expect("live state read should finish");
    let (live_link_status, live_links) = live_links_result.expect("live link search should finish");
    let (live_session_status, live_session) =
        live_session_result.expect("live session write should finish");
    let persisted_state_count = persisted_state_result
        .expect("persisted state item lookup should succeed")
        .hits
        .len();
    let persisted_link_count = persisted_link_result
        .expect("persisted link lookup should succeed")
        .hits
        .len();
    let persisted_session_count = persisted_session_result
        .expect("persisted session lookup should succeed")
        .hits
        .len();
    let personal_source_document = personal_source_document_result
        .expect("personal source document should read through after restart");
    let (fresh_registry_status, fresh_registry) =
        fresh_registry_result.expect("fresh registry call should finish");
    let (fresh_event_status, fresh_events) =
        fresh_events_result.expect("fresh event search should finish");
    let (fresh_personal_context_status, fresh_personal_context) =
        fresh_personal_context_result.expect("fresh context search should finish");
    let (fresh_state_status, fresh_state_body) =
        fresh_state_result.expect("fresh state read should finish");
    let (fresh_link_status, fresh_links) =
        fresh_links_result.expect("fresh link search should finish");
    let (fresh_session_status, fresh_session_body) =
        fresh_session_result.expect("fresh session write should finish");

    assert_eq!(live_state_status, StatusCode::OK, "{live_state}");
    assert_eq!(live_link_status, StatusCode::OK, "{live_links}");
    assert_eq!(live_links["links"].as_array().unwrap().len(), 1);
    assert_eq!(live_session_status, StatusCode::OK, "{live_session}");

    assert_eq!(fresh_registry_status, StatusCode::OK, "{fresh_registry}");
    assert_eq!(fresh_registry["indexes"].as_array().unwrap().len(), 1);
    assert_eq!(
        fresh_registry["indexes"][0]["owner_user_id_hash"],
        routing.owner_user_id_hash
    );
    assert_eq!(fresh_event_status, StatusCode::OK, "{fresh_events}");
    assert_eq!(fresh_events["hits"].as_array().unwrap().len(), 1);
    assert_eq!(
        fresh_personal_context_status,
        StatusCode::OK,
        "{fresh_personal_context}"
    );
    assert!(
        !fresh_personal_context["hits"]
            .as_array()
            .unwrap()
            .is_empty(),
        "{fresh_personal_context}"
    );
    assert_eq!(personal_source_document.uri, personal_source_document_uri);
    assert_eq!(personal_source_document.node_kind, "source_doc");

    assert_eq!(persisted_state_count, 1);
    assert_eq!(persisted_link_count, 1);
    assert_eq!(persisted_session_count, 1);
    for hydrated_domain in ["user_event_indexes", "state_items", "links", "sessions"] {
        assert!(
            hydration[hydrated_domain].as_u64().unwrap_or_default() >= 1,
            "hydration did not report {hydrated_domain}: {hydration}"
        );
    }
    assert_eq!(hydration["status"], "complete", "{hydration}");
    assert_eq!(hydration["ready"], true, "{hydration}");
    assert_eq!(fresh_state_status, StatusCode::OK, "{fresh_state_body}");
    assert_eq!(fresh_state_body["item"]["id"], state_id);
    assert_eq!(fresh_link_status, StatusCode::OK, "{fresh_links}");
    assert_eq!(fresh_links["links"].as_array().unwrap().len(), 1);
    assert_eq!(fresh_links["links"][0]["id"], link_id);
    assert_eq!(fresh_session_status, StatusCode::OK, "{fresh_session_body}");
    assert_eq!(fresh_session_body["session_id"], session_id);
}

#[tokio::test]
async fn meili_restart_restores_insights_structured_rows_summaries_and_trace_acl() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(mut config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let run_id = uuid::Uuid::now_v7().to_string();
    let owner_a = format!("pr5-durable-owner-a-{run_id}");
    let owner_b = format!("pr5-durable-owner-b-{run_id}");
    let owner_a_token = "pr5-durable-owner-a-token";
    let owner_b_token = "pr5-durable-owner-b-token";
    let admin_token = "pr5-durable-admin-token";
    config.allow_unsafe_unauthenticated = false;
    config.auth_users = vec![
        AuthUserConfig {
            token: owner_a_token.to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: owner_a.clone(),
            },
            roles: vec!["user".to_string(), "company_writer".to_string()],
        },
        AuthUserConfig {
            token: owner_b_token.to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: owner_b,
            },
            roles: vec!["user".to_string()],
        },
        AuthUserConfig {
            token: admin_token.to_string(),
            scope: AuthUserScope::Admin,
            roles: vec!["admin".to_string()],
        },
    ];

    let (state, _, app) = start_meili_app(&config).await;
    let admin = state.meili.clone();
    let routing = state
        .store
        .resolver()
        .resolve(&tenant_id, &owner_a, false, true)
        .expect("owner routing should resolve");
    let dataset_key = format!("pr5-durable-dataset-{}", uuid::Uuid::now_v7().simple());
    // Keep the durability marker lexically distinct from bearer tokens: the
    // egress sanitizer intentionally masks every secret substring window.
    let marker = format!("restart-insight-evidence-{run_id}");
    let existing_row_id = format!("pr5-existing-row-{run_id}");
    let new_row_id = format!("pr5-new-row-{run_id}");

    let dataset_created_result = try_call_with_token(
        app.clone(),
        Method::PUT,
        &format!("/v1/state/structured/datasets/{dataset_key}"),
        json!({
            "title": "PR5 durable dataset",
            "description": "schema must survive a fresh AppState",
            "granularity": "weekly",
            "subject_type": "person",
            "columns": [{
                "name": "stress_score",
                "kind": "number",
                "required": true,
                "semantic_role": "metric",
                "trend_direction": "higher_is_worse"
            }]
        }),
        owner_a_token,
    )
    .await;
    let dataset_id = dataset_created_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["dataset"]["id"].as_str())
        .unwrap_or("missing-dataset")
        .to_string();

    let insight_created_result = try_call_with_token(
        app.clone(),
        Method::POST,
        "/v1/state/insights",
        json!({
            "insight_type": "durability",
            "title": "PR5 durable insight",
            "statement": marker,
            "evidence_text": marker,
            "confidence": 0.91,
            "salience": 0.73,
            "privacy": "private"
        }),
        owner_a_token,
    )
    .await;
    let insight_id = insight_created_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["insight"]["id"].as_str())
        .unwrap_or("missing-insight")
        .to_string();

    let snapshot_created_result = try_call_with_token(
        app.clone(),
        Method::POST,
        "/v1/history/structured/snapshots",
        json!({
            "dataset_key": dataset_key,
            "period_key": "2026-W29",
            "period_start": "2026-07-13T00:00:00Z",
            "period_end": "2026-07-19T23:59:59Z",
            "granularity": "weekly",
            "source_ref": { "kind": "test", "id": run_id }
        }),
        owner_a_token,
    )
    .await;
    let snapshot_id = snapshot_created_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["snapshot"]["id"].as_str())
        .unwrap_or("missing-snapshot")
        .to_string();

    let initial_rows_result = try_call_with_token(
        app.clone(),
        Method::POST,
        &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
        json!({ "rows": [{ "id": existing_row_id, "stress_score": 5.0 }] }),
        owner_a_token,
    )
    .await;
    let initial_apply_result = try_call_with_token(
        app.clone(),
        Method::POST,
        &format!("/v1/state/structured/datasets/{dataset_key}/apply-snapshot"),
        json!({ "snapshot_id": snapshot_id, "materialize_context": true }),
        owner_a_token,
    )
    .await;
    let initial_summary_id = initial_apply_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["summary_ids"][0].as_str())
        .unwrap_or("missing-summary")
        .to_string();

    let context_search_result = try_call_with_token(
        app,
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker, "limit": 5 }),
        owner_a_token,
    )
    .await;
    let trace_id = context_search_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["trace_id"].as_str())
        .unwrap_or("missing-trace")
        .to_string();

    let persisted_insight_result = admin
        .search::<Value>(
            "rag_insights",
            json!({
                "q": "",
                "filter": tenant_logical_filter(&tenant_id, &insight_id),
                "limit": 10
            }),
        )
        .await;
    let persisted_dataset_result = admin
        .search::<Value>(
            "rag_structured_datasets",
            json!({
                "q": "",
                "filter": tenant_logical_filter(&tenant_id, &dataset_id),
                "limit": 10
            }),
        )
        .await;

    // Use the fallible startup path after fixture creation so teardown still
    // runs if hydration itself is the regression under test.
    let fresh_start_result = try_start_meili_app(&config).await;
    let fresh_results = if let Ok((_fresh_state, _hydration, fresh)) = &fresh_start_result {
        let current_before = try_call_with_token(
            fresh.clone(),
            Method::GET,
            "/v1/state/structured/current",
            Value::Null,
            owner_a_token,
        )
        .await;
        let insight_owner_a = try_call_with_token(
            fresh.clone(),
            Method::POST,
            "/v1/state/insights/search",
            json!({ "query": marker }),
            owner_a_token,
        )
        .await;
        let insight_owner_b = try_call_with_token(
            fresh.clone(),
            Method::POST,
            "/v1/state/insights/search",
            json!({ "query": marker }),
            owner_b_token,
        )
        .await;
        let cross_owner_patch = try_call_with_token(
            fresh.clone(),
            Method::PATCH,
            &format!("/v1/state/insights/{insight_id}"),
            json!({ "statement": "cross-owner mutation must be rejected" }),
            owner_b_token,
        )
        .await;
        let dataset_updated = try_call_with_token(
            fresh.clone(),
            Method::PUT,
            &format!("/v1/state/structured/datasets/{dataset_key}"),
            json!({
                "title": "PR5 durable dataset after restart",
                "granularity": "weekly",
                "subject_type": "person",
                "columns": [
                    { "name": "stress_score", "kind": "number", "required": true },
                    { "name": "energy_score", "kind": "number", "required": false }
                ]
            }),
            owner_a_token,
        )
        .await;
        let snapshot = try_call_with_token(
            fresh.clone(),
            Method::GET,
            &format!("/v1/history/structured/snapshots/{snapshot_id}"),
            Value::Null,
            owner_a_token,
        )
        .await;
        // No row read precedes this write. The duplicate result therefore
        // proves bulk mutation lazily reloaded the pre-restart row IDs.
        let rows = try_call_with_token(
            fresh.clone(),
            Method::POST,
            &format!("/v1/history/structured/snapshots/{snapshot_id}/rows:bulk"),
            json!({
                "rows": [
                    { "id": existing_row_id, "stress_score": 5.0 },
                    { "id": new_row_id, "stress_score": 7.0 }
                ]
            }),
            owner_a_token,
        )
        .await;
        let applied = try_call_with_token(
            fresh.clone(),
            Method::POST,
            &format!("/v1/state/structured/datasets/{dataset_key}/apply-snapshot"),
            json!({ "snapshot_id": snapshot_id, "materialize_context": true }),
            owner_a_token,
        )
        .await;
        let current_after = try_call_with_token(
            fresh.clone(),
            Method::GET,
            "/v1/state/structured/current",
            Value::Null,
            owner_a_token,
        )
        .await;
        let reveal_owner_a = try_call_with_token(
            fresh.clone(),
            Method::POST,
            "/v1/context/reveal",
            json!({ "trace_id": trace_id, "next_layer": 2 }),
            owner_a_token,
        )
        .await;
        let reveal_owner_b = try_call_with_token(
            fresh.clone(),
            Method::POST,
            "/v1/context/reveal",
            json!({ "trace_id": trace_id, "next_layer": 2 }),
            owner_b_token,
        )
        .await;
        let debug_trace = try_call_with_token(
            fresh.clone(),
            Method::GET,
            &format!("/v1/debug/traces/{trace_id}"),
            Value::Null,
            admin_token,
        )
        .await;
        Some((
            current_before,
            insight_owner_a,
            insight_owner_b,
            cross_owner_patch,
            dataset_updated,
            snapshot,
            rows,
            applied,
            current_after,
            reveal_owner_a,
            reveal_owner_b,
            debug_trace,
        ))
    } else {
        None
    };

    // Every test-owned fixed row and dynamic index is removed before any
    // behavioral assertion, including on a failed fresh-start attempt.
    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "insights",
            delete_by_filter_and_wait(&admin, "rag_insights", &tenant_filter).await,
        ),
        (
            "structured datasets",
            delete_by_filter_and_wait(&admin, "rag_structured_datasets", &tenant_filter).await,
        ),
        (
            "structured snapshots",
            delete_by_filter_and_wait(&admin, "rag_structured_snapshots", &tenant_filter).await,
        ),
        (
            "structured rows",
            delete_by_filter_and_wait(&admin, "rag_structured_rows", &tenant_filter).await,
        ),
        (
            "structured summaries",
            delete_by_filter_and_wait(&admin, "rag_structured_summaries", &tenant_filter).await,
        ),
        (
            "state items",
            delete_by_filter_and_wait(&admin, "rag_state_items", &tenant_filter).await,
        ),
        (
            "links",
            delete_by_filter_and_wait(&admin, "rag_links", &tenant_filter).await,
        ),
        (
            "traces",
            delete_by_filter_and_wait(&admin, "rag_traces", &tenant_filter).await,
        ),
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (dataset_created_status, dataset_created) =
        dataset_created_result.expect("dataset creation call should finish");
    let (insight_created_status, insight_created) =
        insight_created_result.expect("insight creation call should finish");
    let (snapshot_created_status, snapshot_created) =
        snapshot_created_result.expect("snapshot creation call should finish");
    let (initial_rows_status, initial_rows) =
        initial_rows_result.expect("initial row write should finish");
    let (initial_apply_status, initial_apply) =
        initial_apply_result.expect("initial apply should finish");
    let (context_search_status, context_search) =
        context_search_result.expect("context search should finish");
    assert_eq!(dataset_created_status, StatusCode::OK, "{dataset_created}");
    assert_eq!(dataset_created["dataset"]["schema_version"], 1);
    assert_ne!(dataset_id, "missing-dataset");
    assert_eq!(insight_created_status, StatusCode::OK, "{insight_created}");
    assert_ne!(insight_id, "missing-insight");
    assert_eq!(
        snapshot_created_status,
        StatusCode::OK,
        "{snapshot_created}"
    );
    assert_ne!(snapshot_id, "missing-snapshot");
    assert_eq!(initial_rows_status, StatusCode::OK, "{initial_rows}");
    assert_eq!(initial_rows["inserted"], 1);
    assert_eq!(initial_apply_status, StatusCode::OK, "{initial_apply}");
    assert_ne!(initial_summary_id, "missing-summary");
    assert_eq!(context_search_status, StatusCode::OK, "{context_search}");
    assert_ne!(trace_id, "missing-trace");
    assert!(!context_search["hits"].as_array().unwrap().is_empty());
    assert_eq!(
        persisted_insight_result
            .expect("persisted insight lookup should succeed")
            .hits
            .len(),
        1
    );
    assert_eq!(
        persisted_dataset_result
            .expect("persisted dataset lookup should succeed")
            .hits
            .len(),
        1
    );

    let (_, hydration, _) = fresh_start_result.expect("fresh AppState startup should succeed");
    for hydrated_domain in [
        "insights",
        "datasets",
        "structured_snapshots",
        "structured_summaries",
        "traces",
    ] {
        assert!(
            hydration[hydrated_domain].as_u64().unwrap_or_default() >= 1,
            "hydration did not report {hydrated_domain}: {hydration}"
        );
    }
    assert_eq!(hydration["status"], "complete", "{hydration}");
    assert_eq!(hydration["ready"], true, "{hydration}");

    let (
        current_before_result,
        insight_owner_a_result,
        insight_owner_b_result,
        cross_owner_patch_result,
        dataset_updated_result,
        fresh_snapshot_result,
        fresh_rows_result,
        fresh_apply_result,
        current_after_result,
        reveal_owner_a_result,
        reveal_owner_b_result,
        debug_trace_result,
    ) = fresh_results.expect("fresh router calls should run");
    let (current_before_status, current_before) =
        current_before_result.expect("current structured read should finish");
    let (insight_owner_a_status, insight_owner_a) =
        insight_owner_a_result.expect("owner insight search should finish");
    let (insight_owner_b_status, insight_owner_b) =
        insight_owner_b_result.expect("cross-owner insight search should finish");
    let (cross_owner_patch_status, cross_owner_patch) =
        cross_owner_patch_result.expect("cross-owner insight patch should finish");
    let (dataset_updated_status, dataset_updated) =
        dataset_updated_result.expect("dataset update should finish");
    let (fresh_snapshot_status, fresh_snapshot) =
        fresh_snapshot_result.expect("snapshot metadata read should finish");
    let (fresh_rows_status, fresh_rows) =
        fresh_rows_result.expect("post-restart row write should finish");
    let (fresh_apply_status, fresh_apply) =
        fresh_apply_result.expect("post-restart apply should finish");
    let (current_after_status, current_after) =
        current_after_result.expect("post-apply structured read should finish");
    let (reveal_owner_a_status, reveal_owner_a) =
        reveal_owner_a_result.expect("trace owner reveal should finish");
    let (reveal_owner_b_status, reveal_owner_b) =
        reveal_owner_b_result.expect("cross-owner trace reveal should finish");
    let (debug_trace_status, debug_trace) =
        debug_trace_result.expect("admin trace read should finish");

    assert_eq!(current_before_status, StatusCode::OK, "{current_before}");
    assert!(current_before["summaries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|summary| summary["id"] == initial_summary_id));
    assert_eq!(insight_owner_a_status, StatusCode::OK, "{insight_owner_a}");
    assert_eq!(insight_owner_a["hits"].as_array().unwrap().len(), 1);
    assert_eq!(insight_owner_a["hits"][0]["id"], insight_id);
    assert_eq!(insight_owner_b_status, StatusCode::OK, "{insight_owner_b}");
    assert!(insight_owner_b["hits"].as_array().unwrap().is_empty());
    assert_eq!(
        cross_owner_patch_status,
        StatusCode::FORBIDDEN,
        "{cross_owner_patch}"
    );

    assert_eq!(dataset_updated_status, StatusCode::OK, "{dataset_updated}");
    assert_eq!(dataset_updated["dataset"]["schema_version"], 2);
    assert_eq!(
        dataset_updated["dataset"]["columns"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(fresh_snapshot_status, StatusCode::OK, "{fresh_snapshot}");
    assert_eq!(fresh_snapshot["dataset_key"], dataset_key);
    assert_eq!(fresh_snapshot["row_count"], 1);
    assert_eq!(fresh_rows_status, StatusCode::OK, "{fresh_rows}");
    assert_eq!(fresh_rows["inserted"], 1);
    assert_eq!(fresh_rows["duplicates"], 1);
    assert_eq!(fresh_apply_status, StatusCode::OK, "{fresh_apply}");
    let fresh_summary_id = fresh_apply["summary_ids"][0]
        .as_str()
        .expect("post-restart apply should return a summary id");
    assert_eq!(current_after_status, StatusCode::OK, "{current_after}");
    assert!(current_after["summaries"]
        .as_array()
        .unwrap()
        .iter()
        .any(|summary| {
            summary["id"] == fresh_summary_id && summary["stats"]["row_count"] == 2
        }));

    assert_eq!(reveal_owner_a_status, StatusCode::OK, "{reveal_owner_a}");
    assert!(reveal_owner_a["content"]
        .as_str()
        .is_some_and(|content| content.contains(&marker)));
    assert_eq!(
        reveal_owner_b_status,
        StatusCode::FORBIDDEN,
        "{reveal_owner_b}"
    );
    assert_eq!(debug_trace_status, StatusCode::OK, "{debug_trace}");
    assert_eq!(debug_trace["id"], trace_id);
    assert_eq!(debug_trace["owner_user_id"], owner_a);
}

#[tokio::test]
async fn meili_restart_upgrades_legacy_dynamic_settings_before_paginated_fs_reads() {
    let _guard = live_meili_test_guard().await;
    let tenant_id = format!("test-tenant-{}", uuid::Uuid::now_v7());
    let Some(mut config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    // Three documents over two-document pages proves the first lazy owner
    // load takes the stable paginated scan path instead of a single fetch.
    config.meili_scan_page_size = 2;
    config.meili_scan_max_documents = 20;

    let admin = bootstrapped_meili_admin(&config).await;
    let seed_state = AppState::new(Arc::new(config.clone()));
    let run_id = uuid::Uuid::now_v7().to_string();
    let owner = format!("pr5-upgrade-owner-{run_id}");
    let marker = format!("pr5-upgrade-marker-{run_id}");
    let base_uri = format!("ctx://user/upgrade/{run_id}");
    let routing = seed_state
        .store
        .resolver()
        .resolve(&tenant_id, &owner, false, true)
        .expect("owner routing should resolve");
    let tenant_hash = seed_state.store.resolver().tenant_hash(&tenant_id);
    let registry = UserEventIndex {
        id: format!("uei__t_{tenant_hash}__u_{}", routing.owner_user_id_hash),
        tenant_id: tenant_id.clone(),
        tenant_hash,
        owner_user_id_hash: routing.owner_user_id_hash.clone(),
        event_index_uid: routing.event_index_uid.clone(),
        personal_context_index_uid: routing.personal_context_index_uid.clone(),
        schema_version: routing.schema_version,
        settings_hash: routing.settings_hash.clone(),
        status: "active".to_string(),
        created_at: chrono::Utc::now(),
        last_event_at: None,
        event_count_estimate: 0,
    };
    let shared_updated_at = chrono::Utc::now();
    let context_nodes = (0..3)
        .map(|ordinal| {
            personal_context_node(
                &tenant_id,
                &routing.personal_context_index_uid,
                &owner,
                &base_uri,
                ordinal,
                &marker,
                shared_updated_at,
            )
        })
        .collect::<Vec<_>>();

    let setup_result: Result<(Value, Value), String> = async {
        admin
            .ensure_index(&routing.event_index_uid, "id", false)
            .await
            .map_err(|error| error.to_string())?;
        admin
            .ensure_index(&routing.personal_context_index_uid, "id", false)
            .await
            .map_err(|error| error.to_string())?;

        // These are the pre-PR5 dynamic-index shapes: filtering works, but
        // neither index permits the new stable physical `id` tie-breaker.
        replace_index_settings_and_wait(
            &config,
            &admin,
            &routing.event_index_uid,
            json!({
                "searchableAttributes": ["text", "event_type", "entity_type", "entity_id", "tags"],
                "filterableAttributes": ["id", "tenant_id", "owner_user_id_hash", "event_type", "entity_type", "entity_id", "privacy", "occurred_at", "observed_at"],
                "sortableAttributes": ["occurred_at", "observed_at"]
            }),
        )
        .await?;
        replace_index_settings_and_wait(
            &config,
            &admin,
            &routing.personal_context_index_uid,
            json!({
                "searchableAttributes": ["title", "body", "uri"],
                "filterableAttributes": ["id", "uri", "tenant_id", "owner_user_id", "layer", "ancestor_uris", "status", "privacy", "source_id", "revision_id", "node_kind", "retrieval_role", "retrieval_enabled", "parent_uri", "source_document_uri", "fragment_index", "block_type", "page_idx", "heading_level"],
                "sortableAttributes": ["updated_at", "layer"]
            }),
        )
        .await?;

        let registry_document = tenant_document(
            &tenant_id,
            "rag_user_event_indexes",
            &registry.id,
            &registry,
        )
        .map_err(|error| error.to_string())?;
        let registry_task = admin
            .add_documents("rag_user_event_indexes", &[registry_document])
            .await
            .map_err(|error| error.to_string())?;
        wait_for_optional_task(&admin, registry_task).await?;

        let context_documents = context_nodes
            .iter()
            .map(|node| {
                tenant_document(
                    &tenant_id,
                    &routing.personal_context_index_uid,
                    &node.uri,
                    node,
                )
                .map_err(|error| error.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let context_task = admin
            .add_documents(&routing.personal_context_index_uid, &context_documents)
            .await
            .map_err(|error| error.to_string())?;
        wait_for_optional_task(&admin, context_task).await?;

        let event_settings = read_index_settings(&config, &routing.event_index_uid).await?;
        let context_settings =
            read_index_settings(&config, &routing.personal_context_index_uid).await?;
        Ok((event_settings, context_settings))
    }
    .await;

    let fresh_start_result = if setup_result.is_ok() {
        try_start_meili_app(&config).await
    } else {
        Err("legacy dynamic-index fixture setup failed".to_string())
    };
    let post_start_settings_result: Result<(Value, Value), String> = if fresh_start_result.is_ok() {
        async {
            let event_settings = read_index_settings(&config, &routing.event_index_uid).await?;
            let context_settings =
                read_index_settings(&config, &routing.personal_context_index_uid).await?;
            Ok((event_settings, context_settings))
        }
        .await
    } else {
        Err("fresh startup did not complete".to_string())
    };

    let fs_results = if let Ok((fresh_state, _, _)) = &fresh_start_result {
        let listing = fresh_state
            .store
            .fs_ls_async(&tenant_id, Some(&base_uri), Some(&owner), false)
            .await;
        let tree = fresh_state
            .store
            .fs_tree_async(&tenant_id, Some(&base_uri), Some(3), Some(&owner), false)
            .await;
        let read = fresh_state
            .store
            .fs_read_async(
                &tenant_id,
                &format!("{base_uri}/node-1"),
                Some(&owner),
                false,
            )
            .await;
        Some((listing, tree, read))
    } else {
        None
    };

    // Teardown remains independent of startup reconciliation and lazy-read
    // behavior. Both dynamic indexes and the registry row are attempted even
    // when fixture setup or hydration returns an error.
    let tenant_filter = equality_filter("tenant_id", &tenant_id);
    let cleanup_results = vec![
        (
            "user index registry",
            delete_by_filter_and_wait(&admin, "rag_user_event_indexes", &tenant_filter).await,
        ),
        (
            "legacy owner event index",
            delete_index_and_wait(&config, &admin, &routing.event_index_uid).await,
        ),
        (
            "legacy owner context index",
            delete_index_and_wait(&config, &admin, &routing.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let (legacy_event_settings, legacy_context_settings) =
        setup_result.expect("legacy dynamic-index fixture setup should succeed");
    for (index_kind, settings) in [
        ("event", legacy_event_settings),
        ("personal context", legacy_context_settings),
    ] {
        assert!(
            settings["sortableAttributes"]
                .as_array()
                .is_some_and(|sortable| !sortable.iter().any(|value| value == "id")),
            "legacy {index_kind} settings unexpectedly permitted id sorting: {settings}"
        );
    }

    let (_, hydration, _) = fresh_start_result
        .expect("fresh startup should reconcile legacy dynamic-index settings before reads");
    assert_eq!(hydration["status"], "complete", "{hydration}");
    assert_eq!(hydration["ready"], true, "{hydration}");
    assert_eq!(hydration["user_event_indexes"], 1, "{hydration}");

    let (upgraded_event_settings, upgraded_context_settings) = post_start_settings_result
        .expect("startup should wait until upgraded dynamic settings are observable");
    for (index_kind, settings) in [
        ("event", upgraded_event_settings),
        ("personal context", upgraded_context_settings),
    ] {
        assert!(
            settings["sortableAttributes"]
                .as_array()
                .is_some_and(|sortable| sortable.iter().any(|value| value == "id")),
            "startup did not reconcile {index_kind} id sorting: {settings}"
        );
    }

    let (listing_result, tree_result, read_result) =
        fs_results.expect("first owner filesystem reads should run after startup");
    let listing = listing_result.expect("first owner fs_ls should paginate successfully");
    let tree = tree_result.expect("first owner fs_tree should succeed from hydrated context");
    let read = read_result.expect("first owner fs_read should succeed from hydrated context");
    assert_eq!(
        listing["children"].as_array().unwrap().len(),
        3,
        "{listing}"
    );
    assert_eq!(tree["children"].as_array().unwrap().len(), 3, "{tree}");
    assert_eq!(tree["depth"], 3, "{tree}");
    assert_eq!(read.uri, format!("{base_uri}/node-1"));
    assert!(read.body.contains(&marker));
    assert_eq!(read.owner_user_id.as_deref(), Some(owner.as_str()));
}

#[tokio::test]
async fn meili_same_company_identity_is_tenant_isolated_end_to_end() {
    let _guard = live_meili_test_guard().await;
    let Some((config_a, config_b, admin, state_a, app_a, state_b, app_b)) =
        two_tenant_meili_fixture().await
    else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_a = config_a.tenant_id.clone();
    let tenant_b = config_b.tenant_id.clone();
    let fixture_id = uuid::Uuid::now_v7();
    let source_id = format!("tenant-scope-company-{fixture_id}");
    let shared_source_uri = format!("https://example.test/tenant-scope/{fixture_id}");
    let marker_a = format!("tenantamarker{}", uuid::Uuid::now_v7().simple());
    let marker_a_updated = format!("tenantaupdated{}", uuid::Uuid::now_v7().simple());
    let marker_b = format!("tenantbmarker{}", uuid::Uuid::now_v7().simple());
    let routing_a = state_a
        .store
        .resolver()
        .resolve(&tenant_a, "company", false, true)
        .expect("tenant A company routing should resolve");
    let routing_b = state_b
        .store
        .resolver()
        .resolve(&tenant_b, "company", false, true)
        .expect("tenant B company routing should resolve");

    let revision_a_result = try_call(
        app_a.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Shared logical company source",
            "source_uri": shared_source_uri,
            "content": format!("# Tenant A\n\n{marker_a}"),
            "checksum": format!("checksum-{marker_a}"),
            "ingest": false
        }),
    )
    .await;
    let revision_b_result = try_call(
        app_b.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Shared logical company source",
            "source_uri": shared_source_uri,
            "content": format!("# Tenant B\n\n{marker_b}"),
            "checksum": format!("checksum-{marker_b}"),
            "ingest": false
        }),
    )
    .await;
    let revision_a = revision_a_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["revision_id"].as_str())
        .unwrap_or("missing-revision-a")
        .to_string();
    let revision_b = revision_b_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["revision_id"].as_str())
        .unwrap_or("missing-revision-b")
        .to_string();

    let activation_a_result = try_call(
        app_a.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_a}/activate"),
        json!({ "reason": "tenant isolation fixture A" }),
    )
    .await;
    let activation_b_result = try_call(
        app_b.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_b}/activate"),
        json!({ "reason": "tenant isolation fixture B" }),
    )
    .await;

    let revision_a_updated_result = try_call(
        app_a.clone(),
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions"),
        json!({
            "title": "Shared logical company source updated only in A",
            "source_uri": shared_source_uri,
            "content": format!("# Tenant A updated\n\n{marker_a_updated}"),
            "checksum": format!("checksum-{marker_a_updated}"),
            "ingest": false
        }),
    )
    .await;
    let revision_a_updated = revision_a_updated_result
        .as_ref()
        .ok()
        .and_then(|(_, body)| body["revision_id"].as_str())
        .unwrap_or("missing-revision-a-updated")
        .to_string();
    let activation_a_updated_result = try_call(
        app_a,
        Method::POST,
        &format!("/v1/state/company-docs/{source_id}/revisions/{revision_a_updated}/activate"),
        json!({ "reason": "tenant A update isolation fixture" }),
    )
    .await;

    let raw_sources_before_delete = admin
        .search::<Value>(
            "rag_sources",
            json!({
                "q": "",
                "filter": two_tenant_logical_filter(&tenant_a, &tenant_b, &source_id),
                "limit": 10
            }),
        )
        .await;

    let (_, hydration_a, fresh_app_a) = start_meili_app(&config_a).await;
    let (_, hydration_b, fresh_app_b) = start_meili_app(&config_b).await;

    let document_a_result = try_call(
        fresh_app_a.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let document_b_result = try_call(
        fresh_app_b.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let revisions_a_result = try_call(
        fresh_app_a.clone(),
        Method::GET,
        &format!("/v1/history/company-docs/{source_id}/revisions"),
        Value::Null,
    )
    .await;
    let revisions_b_result = try_call(
        fresh_app_b.clone(),
        Method::GET,
        &format!("/v1/history/company-docs/{source_id}/revisions"),
        Value::Null,
    )
    .await;
    let context_a_result = try_call(
        fresh_app_a.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker_a_updated, "limit": 10 }),
    )
    .await;
    let context_b_result = try_call(
        fresh_app_b.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": marker_b, "limit": 10 }),
    )
    .await;
    let debug_a_result = try_call(
        fresh_app_a.clone(),
        Method::POST,
        "/v1/debug/meili/search",
        json!({ "index_uid": "rag_sources", "query": "" }),
    )
    .await;
    let debug_b_result = try_call(
        fresh_app_b.clone(),
        Method::POST,
        "/v1/debug/meili/search",
        json!({ "index_uid": "rag_sources", "query": "" }),
    )
    .await;

    let delete_a_result = try_call(
        fresh_app_a.clone(),
        Method::DELETE,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let document_a_after_delete_result = try_call(
        fresh_app_a,
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let document_b_after_delete_result = try_call(
        fresh_app_b.clone(),
        Method::GET,
        &format!("/v1/state/company-docs/{source_id}"),
        Value::Null,
    )
    .await;
    let debug_b_after_delete_result = try_call(
        fresh_app_b,
        Method::POST,
        "/v1/debug/meili/search",
        json!({ "index_uid": "rag_sources", "query": "" }),
    )
    .await;
    let raw_source_a_after_delete = admin
        .search::<Value>(
            "rag_sources",
            json!({
                "q": "",
                "filter": tenant_logical_filter(&tenant_a, &source_id),
                "limit": 10
            }),
        )
        .await;
    let raw_source_b_after_delete = admin
        .search::<Value>(
            "rag_sources",
            json!({
                "q": "",
                "filter": tenant_logical_filter(&tenant_b, &source_id),
                "limit": 10
            }),
        )
        .await;

    let fixed_indexes = [
        "rag_sources",
        "rag_source_revisions",
        "rag_company_context",
        "rag_source_documents",
        "rag_links",
        "rag_traces",
        "rag_user_event_indexes",
    ];
    let cleanup_results = vec![
        (
            "tenant A fixed rows",
            delete_tenant_rows_and_wait(&admin, &tenant_a, &fixed_indexes).await,
        ),
        (
            "tenant B fixed rows",
            delete_tenant_rows_and_wait(&admin, &tenant_b, &fixed_indexes).await,
        ),
        (
            "tenant A company event index",
            delete_index_and_wait(&config_a, &admin, &routing_a.event_index_uid).await,
        ),
        (
            "tenant A company context index",
            delete_index_and_wait(&config_a, &admin, &routing_a.personal_context_index_uid).await,
        ),
        (
            "tenant B company event index",
            delete_index_and_wait(&config_b, &admin, &routing_b.event_index_uid).await,
        ),
        (
            "tenant B company context index",
            delete_index_and_wait(&config_b, &admin, &routing_b.personal_context_index_uid).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    for (status, body) in [
        revision_a_result.expect("tenant A revision request should finish"),
        revision_b_result.expect("tenant B revision request should finish"),
        activation_a_result.expect("tenant A activation request should finish"),
        activation_b_result.expect("tenant B activation request should finish"),
        revision_a_updated_result.expect("tenant A update request should finish"),
        activation_a_updated_result.expect("tenant A updated activation should finish"),
    ] {
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    assert_ne!(revision_a, "missing-revision-a");
    assert_ne!(revision_b, "missing-revision-b");
    assert_ne!(revision_a_updated, "missing-revision-a-updated");

    let raw_sources = raw_sources_before_delete.expect("raw source inventory should succeed");
    assert_eq!(raw_sources.hits.len(), 2, "{raw_sources:?}");
    assert_ne!(raw_sources.hits[0]["id"], raw_sources.hits[1]["id"]);
    assert!(raw_sources.hits.iter().all(|source| {
        source["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("ts1_"))
            && source["logical_id"] == source_id
    }));

    for hydration in [&hydration_a, &hydration_b] {
        assert!(hydration["company_sources"]
            .as_u64()
            .is_some_and(|count| count >= 1));
        assert!(hydration["source_revisions"]
            .as_u64()
            .is_some_and(|count| count >= 1));
        assert!(hydration["company_context_nodes"]
            .as_u64()
            .is_some_and(|count| count >= 1));
    }

    let (document_a_status, document_a) =
        document_a_result.expect("tenant A document read should finish");
    let (document_b_status, document_b) =
        document_b_result.expect("tenant B document read should finish");
    assert_eq!(document_a_status, StatusCode::OK, "{document_a}");
    assert_eq!(document_b_status, StatusCode::OK, "{document_b}");
    assert_eq!(document_a["source_id"], source_id);
    assert_eq!(document_b["source_id"], source_id);
    assert_eq!(document_a["source_uri"], shared_source_uri);
    assert_eq!(document_b["source_uri"], shared_source_uri);
    assert_eq!(document_a["revision_id"], revision_a_updated);
    assert_eq!(document_b["revision_id"], revision_b);
    assert!(
        document_a["content"].as_str().is_some_and(
            |content| content.contains(&marker_a_updated) && !content.contains(&marker_b)
        )
    );
    assert!(
        document_b["content"].as_str().is_some_and(
            |content| content.contains(&marker_b) && !content.contains(&marker_a_updated)
        )
    );

    let (revisions_a_status, revisions_a) =
        revisions_a_result.expect("tenant A revisions read should finish");
    let (revisions_b_status, revisions_b) =
        revisions_b_result.expect("tenant B revisions read should finish");
    assert_eq!(revisions_a_status, StatusCode::OK, "{revisions_a}");
    assert_eq!(revisions_b_status, StatusCode::OK, "{revisions_b}");
    assert_eq!(revisions_a["revisions"].as_array().unwrap().len(), 2);
    assert_eq!(revisions_b["revisions"].as_array().unwrap().len(), 1);

    for (result, marker, tenant_id) in [
        (
            context_a_result,
            marker_a_updated.as_str(),
            tenant_a.as_str(),
        ),
        (context_b_result, marker_b.as_str(), tenant_b.as_str()),
    ] {
        let (status, body) = result.expect("tenant context search should finish");
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["hits"].as_array().is_some_and(|hits| {
                !hits.is_empty()
                    && hits.iter().all(|hit| {
                        hit["snippet"]
                            .as_str()
                            .is_some_and(|snippet| snippet.contains(marker))
                    })
            }),
            "tenant {tenant_id} context search leaked or missed its marker: {body}"
        );
    }
    for (result, tenant_id) in [(debug_a_result, &tenant_a), (debug_b_result, &tenant_b)] {
        let (status, body) = result.expect("tenant debug search should finish");
        assert_eq!(status, StatusCode::OK, "{body}");
        assert!(
            body["hits"].as_array().is_some_and(|hits| {
                !hits.is_empty()
                    && hits
                        .iter()
                        .all(|hit| hit["tenant_id"].as_str() == Some(tenant_id.as_str()))
            }),
            "debug search was not tenant scoped: {body}"
        );
    }

    let (delete_status, delete_body) = delete_a_result.expect("tenant A delete should finish");
    assert_eq!(delete_status, StatusCode::OK, "{delete_body}");
    let (document_a_after_status, document_a_after) =
        document_a_after_delete_result.expect("tenant A post-delete read should finish");
    assert_eq!(
        document_a_after_status,
        StatusCode::NOT_FOUND,
        "{document_a_after}"
    );
    let (document_b_after_status, document_b_after) =
        document_b_after_delete_result.expect("tenant B post-delete read should finish");
    assert_eq!(
        document_b_after_status,
        StatusCode::OK,
        "{document_b_after}"
    );
    assert_eq!(document_b_after["revision_id"], revision_b);
    let (debug_b_after_status, debug_b_after) =
        debug_b_after_delete_result.expect("tenant B post-delete debug should finish");
    assert_eq!(debug_b_after_status, StatusCode::OK, "{debug_b_after}");
    assert!(debug_b_after["hits"].as_array().is_some_and(|hits| {
        hits.iter()
            .any(|hit| hit["tenant_id"] == tenant_b && hit["id"] == source_id)
    }));
    assert!(raw_source_a_after_delete
        .expect("tenant A post-delete inventory should succeed")
        .hits
        .is_empty());
    assert_eq!(
        raw_source_b_after_delete
            .expect("tenant B post-delete inventory should succeed")
            .hits
            .len(),
        1
    );
}

#[tokio::test]
async fn meili_same_point_ids_are_isolated_across_fixed_repository_paths() {
    let _guard = live_meili_test_guard().await;
    let Some((config_a, config_b, admin, state_a, _app_a, state_b, _app_b)) =
        two_tenant_meili_fixture().await
    else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_a = config_a.tenant_id.clone();
    let tenant_b = config_b.tenant_id.clone();
    let repository = MeiliRepository::new(admin.clone(), true);
    let fixture_id = uuid::Uuid::now_v7();
    let logical_id = format!("tenant-scope-point-{fixture_id}");
    let source_id = format!("tenant-scope-source-{fixture_id}");
    let source_document_uri = format!("ctx://company/tenant-scope/{fixture_id}/source");
    let context_uri = format!("{source_document_uri}/chunks/0001");
    let marker_a = format!("point-a-{}", uuid::Uuid::now_v7());
    let marker_a_updated = format!("point-a-updated-{}", uuid::Uuid::now_v7());
    let marker_b = format!("point-b-{}", uuid::Uuid::now_v7());
    let now = chrono::Utc::now();

    for (tenant_id, marker) in [(&tenant_a, &marker_a), (&tenant_b, &marker_b)] {
        let mut context = tenant_context_node(tenant_id, &context_uri, &source_id, marker);
        context.source_document_uri = Some(source_document_uri.clone());
        repository
            .upsert_context_nodes("rag_company_context", &[context])
            .await
            .expect("tenant context fixture should persist");
        repository
            .upsert_source_documents(&[SourceDocument {
                id: logical_id.clone(),
                tenant_id: tenant_id.clone(),
                owner_user_id: None,
                source_kind: "tenant_scope_test".to_string(),
                source_id: source_id.clone(),
                revision_id: "shared-revision".to_string(),
                uri: source_document_uri.clone(),
                title: format!("Source {marker}"),
                content: marker.clone(),
                checksum: format!("checksum-{marker}"),
                status: "active".to_string(),
                retrieval_enabled: false,
                created_at: now,
                updated_at: now,
            }])
            .await
            .expect("tenant source-document fixture should persist");
        repository
            .upsert_trace(&TraceRecord {
                id: logical_id.clone(),
                tenant_id: tenant_id.clone(),
                owner_user_id: None,
                query: marker.clone(),
                mode: "tenant-scope-test".to_string(),
                stages: vec![json!({ "marker": marker })],
                context_uris: vec![context_uri.clone()],
                created_at: now,
            })
            .await
            .expect("tenant trace fixture should persist");
        repository
            .upsert_structured_snapshot(&StructuredSnapshot {
                id: logical_id.clone(),
                tenant_id: tenant_id.clone(),
                dataset_key: format!("dataset-{marker}"),
                owner_user_id: "shared-owner".to_string(),
                period_key: "2026-07".to_string(),
                period_start: now,
                period_end: now + chrono::Duration::hours(1),
                row_count: 1,
                status: marker.clone(),
            })
            .await
            .expect("tenant snapshot fixture should persist");
        repository
            .upsert_structured_rows(
                tenant_id,
                &[json!({
                    "id": logical_id,
                    "tenant_id": tenant_id,
                    "snapshot_id": logical_id,
                    "marker": marker
                })],
            )
            .await
            .expect("tenant row fixture should persist");
        repository
            .upsert_eval_run(&RagEvalRun {
                id: logical_id.clone(),
                tenant_id: tenant_id.clone(),
                change_id: None,
                case_ids: vec![logical_id.clone()],
                result_ids: vec![logical_id.clone()],
                trace_ids: vec![logical_id.clone()],
                status: marker.clone(),
                metrics: RagEvalMetrics::default(),
                guard_results: Vec::new(),
                overview_source_document_uri: None,
                report_source_document_uris: Vec::new(),
                created_by: marker.clone(),
                created_at: now,
                completed_at: Some(now),
            })
            .await
            .expect("tenant eval-run fixture should persist");
        repository
            .upsert_eval_case_results(&[RagEvalCaseResult {
                id: logical_id.clone(),
                tenant_id: tenant_id.clone(),
                run_id: logical_id.clone(),
                case_id: logical_id.clone(),
                owner_user_id: None,
                status: marker.clone(),
                question: marker.clone(),
                trace_id: logical_id.clone(),
                answer: marker.clone(),
                citations: Vec::new(),
                retrieved_uris: Vec::new(),
                source_document_uris: vec![source_document_uri.clone()],
                failures: Vec::new(),
                guard_failures: Vec::new(),
                metrics: json!({ "marker": marker }),
                latency_ms: 1,
                created_at: now,
            }])
            .await
            .expect("tenant eval-result fixture should persist");
        repository
            .upsert_eval_overview(&RagEvalOverview {
                tenant_id: tenant_id.clone(),
                run_id: logical_id.clone(),
                status: marker.clone(),
                metrics: RagEvalMetrics::default(),
                failure_patterns: Vec::new(),
                suggested_target_component: marker.clone(),
                root_cause_notes: vec![marker.clone()],
                overview_markdown: marker.clone(),
                case_report_uris: Vec::new(),
                overview_source_document_uri: None,
                generated_at: now,
            })
            .await
            .expect("tenant eval-overview fixture should persist");
        let task = IngestTask {
            task_id: logical_id.clone(),
            tenant_id: tenant_id.clone(),
            owner_user_id: None,
            source_id: source_id.clone(),
            revision_id: "shared-revision".to_string(),
            source_document_uri: Some(source_document_uri.clone()),
            parser_provider: "builtin".to_string(),
            parser_backend: marker.clone(),
            state: "completed".to_string(),
            error: None,
            created_at: now,
            updated_at: now,
            completed_at: Some(now),
            status_url: None,
            result_url: None,
            queued_ahead: None,
        };
        repository
            .upsert_ingest_task(&task)
            .await
            .expect("tenant ingest-task fixture should persist");
        repository
            .upsert_ingest_result(&IngestTaskResult {
                task,
                source_document_uri: source_document_uri.clone(),
                source_id: source_id.clone(),
                revision_id: "shared-revision".to_string(),
                parse_artifacts: Vec::new(),
                parsed_blocks: Vec::new(),
                fragment_uris: vec![context_uri.clone()],
                context_uris: vec![context_uri.clone()],
            })
            .await
            .expect("tenant ingest-result fixture should persist");
        repository
            .upsert_harness_changes(&[HarnessChangeManifest {
                id: logical_id.clone(),
                tenant_id: tenant_id.clone(),
                iteration: 1,
                change_type: "tenant_scope_test".to_string(),
                component_id: "retrieval.context_search".to_string(),
                files: Vec::new(),
                failure_pattern: marker.clone(),
                root_cause: marker.clone(),
                targeted_fix: marker.clone(),
                predicted_fixes: Vec::new(),
                risk_cases: Vec::new(),
                expected_metric_deltas: json!({}),
                baseline_eval_run_id: None,
                candidate_eval_run_id: None,
                why_this_component: marker.clone(),
                created_by: marker.clone(),
                created_at: now,
                status: "proposed".to_string(),
            }])
            .await
            .expect("tenant harness-change fixture should persist");
    }

    let mut updated_context =
        tenant_context_node(&tenant_a, &context_uri, &source_id, &marker_a_updated);
    updated_context.source_document_uri = Some(source_document_uri.clone());
    repository
        .upsert_context_nodes("rag_company_context", &[updated_context])
        .await
        .expect("tenant A context update should persist");
    repository
        .upsert_source_documents(&[SourceDocument {
            id: logical_id.clone(),
            tenant_id: tenant_a.clone(),
            owner_user_id: None,
            source_kind: "tenant_scope_test".to_string(),
            source_id: source_id.clone(),
            revision_id: "shared-revision".to_string(),
            uri: source_document_uri.clone(),
            title: format!("Source {marker_a_updated}"),
            content: marker_a_updated.clone(),
            checksum: format!("checksum-{marker_a_updated}"),
            status: "active".to_string(),
            retrieval_enabled: false,
            created_at: now,
            updated_at: chrono::Utc::now(),
        }])
        .await
        .expect("tenant A source-document update should persist");
    repository
        .upsert_trace(&TraceRecord {
            id: logical_id.clone(),
            tenant_id: tenant_a.clone(),
            owner_user_id: None,
            query: marker_a_updated.clone(),
            mode: "tenant-scope-test".to_string(),
            stages: vec![json!({ "marker": marker_a_updated })],
            context_uris: vec![context_uri.clone()],
            created_at: now,
        })
        .await
        .expect("tenant A trace update should persist");
    repository
        .upsert_structured_rows(
            &tenant_a,
            &[json!({
                "id": logical_id,
                "tenant_id": tenant_a,
                "snapshot_id": logical_id,
                "marker": marker_a_updated
            })],
        )
        .await
        .expect("tenant A row update should persist");

    let context_a_result = repository
        .read_context_node(
            &tenant_a,
            None,
            &context_uri,
            None,
            state_a.store.resolver(),
        )
        .await;
    let context_b_result = repository
        .read_context_node(
            &tenant_b,
            None,
            &context_uri,
            None,
            state_b.store.resolver(),
        )
        .await;
    let source_a_result = repository
        .read_source_document(&tenant_a, None, &source_document_uri)
        .await;
    let source_b_result = repository
        .read_source_document(&tenant_b, None, &source_document_uri)
        .await;
    let trace_a_result = repository.get_trace(&tenant_a, &logical_id).await;
    let trace_b_result = repository.get_trace(&tenant_b, &logical_id).await;
    let snapshot_a_result = repository.get_snapshot(&tenant_a, &logical_id).await;
    let snapshot_b_result = repository.get_snapshot(&tenant_b, &logical_id).await;
    let rows_a_result = repository.list_rows(&tenant_a, &logical_id).await;
    let rows_b_result = repository.list_rows(&tenant_b, &logical_id).await;
    let run_a_result = repository.get_eval_run(&tenant_a, &logical_id).await;
    let run_b_result = repository.get_eval_run(&tenant_b, &logical_id).await;
    let eval_results_a_result = repository
        .list_eval_case_results(&tenant_a, &logical_id)
        .await;
    let eval_results_b_result = repository
        .list_eval_case_results(&tenant_b, &logical_id)
        .await;
    let overview_a_result = repository.get_eval_overview(&tenant_a, &logical_id).await;
    let overview_b_result = repository.get_eval_overview(&tenant_b, &logical_id).await;
    let task_a_result = repository.get_ingest_task(&tenant_a, &logical_id).await;
    let task_b_result = repository.get_ingest_task(&tenant_b, &logical_id).await;
    let ingest_result_a_result = repository.get_ingest_result(&tenant_a, &logical_id).await;
    let ingest_result_b_result = repository.get_ingest_result(&tenant_b, &logical_id).await;
    let harness_a_result = repository.get_harness_change(&tenant_a, &logical_id).await;
    let harness_b_result = repository.get_harness_change(&tenant_b, &logical_id).await;
    let debug_a_result = repository.debug_search(&tenant_a, "rag_traces", "").await;
    let debug_b_result = repository.debug_search(&tenant_b, "rag_traces", "").await;

    let raw_trace_inventory = admin
        .search::<Value>(
            "rag_traces",
            json!({
                "q": "",
                "filter": two_tenant_logical_filter(&tenant_a, &tenant_b, &logical_id),
                "limit": 10
            }),
        )
        .await;
    let delete_ingest_a_result = repository
        .delete_ingest_tasks(&tenant_a, std::slice::from_ref(&logical_id))
        .await;
    let task_a_after_delete_result = repository.get_ingest_task(&tenant_a, &logical_id).await;
    let task_b_after_delete_result = repository.get_ingest_task(&tenant_b, &logical_id).await;

    let fixed_indexes = [
        "rag_company_context",
        "rag_source_documents",
        "rag_structured_snapshots",
        "rag_structured_rows",
        "rag_traces",
        "rag_harness_changes",
        "rag_ingest_tasks",
        "rag_ingest_results",
        "rag_eval_runs",
        "rag_eval_case_results",
        "rag_eval_overviews",
    ];
    let cleanup_results = vec![
        (
            "tenant A direct fixed rows",
            delete_tenant_rows_and_wait(&admin, &tenant_a, &fixed_indexes).await,
        ),
        (
            "tenant B direct fixed rows",
            delete_tenant_rows_and_wait(&admin, &tenant_b, &fixed_indexes).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);

    let context_a = context_a_result
        .expect("tenant A context read should succeed")
        .expect("tenant A context should exist");
    let context_b = context_b_result
        .expect("tenant B context read should succeed")
        .expect("tenant B context should exist");
    assert_eq!(context_a.uri, context_uri);
    assert_eq!(context_b.uri, context_uri);
    assert!(context_a.body.contains(&marker_a_updated));
    assert!(context_b.body.contains(&marker_b));
    assert!(!context_b.body.contains(&marker_a_updated));

    let source_a = source_a_result
        .expect("tenant A source read should succeed")
        .expect("tenant A source should exist");
    let source_b = source_b_result
        .expect("tenant B source read should succeed")
        .expect("tenant B source should exist");
    assert_eq!(source_a.id, logical_id);
    assert_eq!(source_b.id, logical_id);
    assert_eq!(source_a.uri, source_document_uri);
    assert_eq!(source_b.uri, source_document_uri);
    assert_eq!(source_a.content, marker_a_updated);
    assert_eq!(source_b.content, marker_b);

    let trace_a = trace_a_result
        .expect("tenant A trace read should succeed")
        .expect("tenant A trace should exist");
    let trace_b = trace_b_result
        .expect("tenant B trace read should succeed")
        .expect("tenant B trace should exist");
    assert_eq!(trace_a.id, logical_id);
    assert_eq!(trace_b.id, logical_id);
    assert_eq!(trace_a.query, marker_a_updated);
    assert_eq!(trace_b.query, marker_b);

    let snapshot_a = snapshot_a_result
        .expect("tenant A snapshot read should succeed")
        .expect("tenant A snapshot should exist");
    let snapshot_b = snapshot_b_result
        .expect("tenant B snapshot read should succeed")
        .expect("tenant B snapshot should exist");
    assert_eq!(snapshot_a.id, logical_id);
    assert_eq!(snapshot_b.id, logical_id);
    assert_eq!(snapshot_a.tenant_id, tenant_a);
    assert_eq!(snapshot_b.tenant_id, tenant_b);

    let rows_a = rows_a_result
        .expect("tenant A row read should succeed")
        .expect("tenant A rows should be supported");
    let rows_b = rows_b_result
        .expect("tenant B row read should succeed")
        .expect("tenant B rows should be supported");
    assert_eq!(rows_a.len(), 1, "{rows_a:?}");
    assert_eq!(rows_b.len(), 1, "{rows_b:?}");
    assert_eq!(rows_a[0]["id"], logical_id);
    assert_eq!(rows_b[0]["id"], logical_id);
    assert_eq!(rows_a[0]["marker"], marker_a_updated);
    assert_eq!(rows_b[0]["marker"], marker_b);

    let run_a = run_a_result
        .expect("tenant A eval-run read should succeed")
        .expect("tenant A eval run should exist");
    let run_b = run_b_result
        .expect("tenant B eval-run read should succeed")
        .expect("tenant B eval run should exist");
    assert_eq!(run_a.created_by, marker_a);
    assert_eq!(run_b.created_by, marker_b);
    let eval_results_a = eval_results_a_result
        .expect("tenant A eval-results read should succeed")
        .expect("tenant A eval results should be supported");
    let eval_results_b = eval_results_b_result
        .expect("tenant B eval-results read should succeed")
        .expect("tenant B eval results should be supported");
    assert_eq!(eval_results_a.len(), 1);
    assert_eq!(eval_results_b.len(), 1);
    assert_eq!(eval_results_a[0].answer, marker_a);
    assert_eq!(eval_results_b[0].answer, marker_b);
    let overview_a = overview_a_result
        .expect("tenant A overview read should succeed")
        .expect("tenant A overview should exist");
    let overview_b = overview_b_result
        .expect("tenant B overview read should succeed")
        .expect("tenant B overview should exist");
    assert_eq!(overview_a.overview_markdown, marker_a);
    assert_eq!(overview_b.overview_markdown, marker_b);

    let task_a = task_a_result
        .expect("tenant A task read should succeed")
        .expect("tenant A task should exist before deletion");
    let task_b = task_b_result
        .expect("tenant B task read should succeed")
        .expect("tenant B task should exist");
    assert_eq!(task_a.parser_backend, marker_a);
    assert_eq!(task_b.parser_backend, marker_b);
    let ingest_result_a = ingest_result_a_result
        .expect("tenant A ingest-result read should succeed")
        .expect("tenant A ingest result should exist");
    let ingest_result_b = ingest_result_b_result
        .expect("tenant B ingest-result read should succeed")
        .expect("tenant B ingest result should exist");
    assert_eq!(ingest_result_a.task.parser_backend, marker_a);
    assert_eq!(ingest_result_b.task.parser_backend, marker_b);

    let harness_a = harness_a_result
        .expect("tenant A harness read should succeed")
        .expect("tenant A harness change should exist");
    let harness_b = harness_b_result
        .expect("tenant B harness read should succeed")
        .expect("tenant B harness change should exist");
    assert_eq!(harness_a.root_cause, marker_a);
    assert_eq!(harness_b.root_cause, marker_b);

    for (result, tenant_id) in [(debug_a_result, &tenant_a), (debug_b_result, &tenant_b)] {
        let body = result
            .expect("tenant debug search should succeed")
            .expect("tenant debug search should be supported");
        assert!(
            body["hits"].as_array().is_some_and(|hits| {
                !hits.is_empty()
                    && hits
                        .iter()
                        .all(|hit| hit["tenant_id"].as_str() == Some(tenant_id.as_str()))
            }),
            "direct debug search was not tenant scoped: {body}"
        );
    }

    let raw_traces = raw_trace_inventory.expect("raw trace inventory should succeed");
    assert_eq!(raw_traces.hits.len(), 2, "{raw_traces:?}");
    assert_ne!(raw_traces.hits[0]["id"], raw_traces.hits[1]["id"]);
    assert!(raw_traces.hits.iter().all(|trace| {
        trace["id"]
            .as_str()
            .is_some_and(|id| id.starts_with("ts1_"))
            && trace["logical_id"] == logical_id
    }));

    delete_ingest_a_result.expect("tenant A ingest cleanup should succeed");
    assert!(task_a_after_delete_result
        .expect("tenant A post-delete task read should succeed")
        .is_none());
    assert!(task_b_after_delete_result
        .expect("tenant B post-delete task read should succeed")
        .is_some());
}

#[tokio::test]
async fn tenant_scope_v1_live_migration_preserves_legacy_rows_across_tenants() {
    let _guard = live_meili_test_guard().await;
    if !isolated_migration_test_enabled() {
        eprintln!(
            "skipping destructive migration integration test; set \
             RAG_TEST_MEILI_MIGRATION_ISOLATED=true only for an isolated empty Meilisearch"
        );
        return;
    }

    let fixture_id = uuid::Uuid::now_v7();
    let tenant_a = format!("test-tenant-migration-a-{fixture_id}");
    let tenant_b = format!("test-tenant-migration-b-{fixture_id}");
    let Some(config) = meili_config_with_tenant(tenant_a.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let admin = MeiliAdmin::from_config(&config);

    // Inspect before bootstrap so an accidentally shared endpoint is rejected
    // before this test changes settings or writes a document. The explicit
    // opt-in plus this empty-inventory precondition make the direct production
    // migration adapter safe to exercise end to end.
    require_empty_fixed_indexes(&admin)
        .await
        .expect("migration integration endpoint must be isolated and empty");
    admin
        .bootstrap(false)
        .await
        .expect("isolated migration integration bootstrap should succeed");
    require_empty_fixed_indexes(&admin)
        .await
        .expect("bootstrap must not create documents");

    let legacy_a_id = format!("legacy-company-context-a-{fixture_id}");
    let legacy_b_id = format!("legacy-company-context-b-{fixture_id}");
    let shared_uri = format!("ctx://company/tenant-scope-v1-live/{fixture_id}");
    let legacy_a = json!({
        "id": legacy_a_id,
        "uri": shared_uri,
        "title": "Legacy tenant A copy",
        "body": format!("legacy-a-{fixture_id}"),
        "privacy": "company",
        "retrieval_enabled": true,
        "retrieval_role": "fragment"
    });
    let legacy_b = json!({
        "id": legacy_b_id,
        "uri": shared_uri,
        "title": "Legacy tenant B copy",
        "body": format!("legacy-b-{fixture_id}"),
        "privacy": "company",
        "retrieval_enabled": true,
        "retrieval_role": "fragment"
    });
    let migrated_a = tenant_document(&tenant_a, "rag_company_context", &shared_uri, &legacy_a)
        .expect("tenant A migrated fixture should serialize");
    let migrated_b = tenant_document(&tenant_b, "rag_company_context", &shared_uri, &legacy_b)
        .expect("tenant B migrated fixture should serialize");
    let migrated_a_id = migrated_a["id"]
        .as_str()
        .expect("tenant A fixture should have a physical ID")
        .to_string();
    let migrated_b_id = migrated_b["id"]
        .as_str()
        .expect("tenant B fixture should have a physical ID")
        .to_string();
    let fixture_ids = vec![
        legacy_a_id.clone(),
        legacy_b_id.clone(),
        migrated_a_id.clone(),
        migrated_b_id.clone(),
    ];
    let artifact_dir =
        std::env::temp_dir().join(format!("nowledge-tenant-scope-v1-live-{fixture_id}"));
    fs::create_dir_all(&artifact_dir).expect("migration test artifact directory should be created");
    let checkpoint_path = artifact_dir.join("checkpoint.json");

    let outcome: AnyResult<_> = async {
        let seed_task = admin
            .add_documents(
                "rag_company_context",
                &[legacy_a.clone(), legacy_b.clone(), migrated_a.clone()],
            )
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        wait_for_optional_task(&admin, seed_task)
            .await
            .map_err(anyhow::Error::msg)?;

        let mapping = LegacyTenantMapping {
            migration: MIGRATION_NAME.to_string(),
            documents: vec![
                LegacyTenantAssignment {
                    index_uid: "rag_company_context".to_string(),
                    legacy_id: legacy_a_id.clone(),
                    tenant_id: tenant_a.clone(),
                },
                LegacyTenantAssignment {
                    index_uid: "rag_company_context".to_string(),
                    legacy_id: legacy_b_id.clone(),
                    tenant_id: tenant_b.clone(),
                },
            ],
        };
        let plan = create_plan(&admin, &mapping, 2).await?;
        ensure!(plan.quarantined.is_empty(), "{:?}", plan.quarantined);
        ensure!(
            plan.unused_mappings.is_empty(),
            "{:?}",
            plan.unused_mappings
        );
        ensure!(
            plan.operations.len() == 1,
            "pre-migrated tenant A should leave only tenant B to write: {:?}",
            plan.operations
        );
        let operation = &plan.operations[0];
        ensure!(operation.tenant_id == tenant_b);
        ensure!(operation.legacy_id == legacy_b_id);
        ensure!(operation.logical_id == shared_uri);
        ensure!(operation.target_id == migrated_b_id);
        ensure!(operation.document == migrated_b);
        let company_inventory = plan
            .indexes
            .get("rag_company_context")
            .ok_or_else(|| anyhow!("company-context inventory is missing"))?;
        ensure!(
            company_inventory
                .tenants
                .get(&tenant_a)
                .is_some_and(|tenant| {
                    tenant.expected_count == 1 && tenant.already_migrated_count == 1
                }),
            "tenant A pre-migrated copy was not inventoried"
        );
        ensure!(
            company_inventory
                .tenants
                .get(&tenant_b)
                .is_some_and(|tenant| { tenant.expected_count == 1 && tenant.planned_count == 1 }),
            "tenant B legacy copy was not planned"
        );

        let rollback = create_rollback_plan(&plan)?;
        let acknowledgement = rollback.acknowledgement.clone();
        let before_dry_run = read_all_fixed_index_documents(&admin).await?;
        let mut checkpoints = FileCheckpointStore::new(checkpoint_path.clone());
        let dry_run = apply_plan(
            &admin,
            &plan,
            &rollback,
            &acknowledgement,
            &mut checkpoints,
            true,
        )
        .await?;
        let after_dry_run = read_all_fixed_index_documents(&admin).await?;
        ensure!(dry_run.dry_run);
        ensure!(dry_run.mutation_free);
        ensure!(dry_run.remote_batches_written == 0);
        ensure!(dry_run.checkpoint_writes == 0);
        ensure!(!checkpoint_path.exists());
        ensure!(
            before_dry_run == after_dry_run,
            "dry-run changed the isolated Meilisearch inventory"
        );

        let applied = apply_plan(
            &admin,
            &plan,
            &rollback,
            &acknowledgement,
            &mut checkpoints,
            false,
        )
        .await?;
        ensure!(!applied.dry_run);
        ensure!(!applied.mutation_free);
        ensure!(applied.completed_operations == 1);
        ensure!(applied.remote_batches_written == 1);
        ensure!(applied.checkpoint_writes == 1);
        ensure!(applied.ready_to_verify);

        let verification = verify_plan(&admin, &plan).await?;
        ensure!(verification.writes_verified, "{:?}", verification.failures);
        ensure!(
            verification.legacy_rows_preserved,
            "{:?}",
            verification.failures
        );
        ensure!(verification.unresolved_quarantine == 0);
        ensure!(verification.ready_to_cutover, "{:?}", verification.failures);

        let inventory = read_all_fixed_index_documents(&admin).await?;
        let company_documents = inventory
            .get("rag_company_context")
            .ok_or_else(|| anyhow!("company-context inventory is missing after apply"))?;
        ensure!(company_documents.len() == 4, "{company_documents:?}");
        let by_id = company_documents
            .iter()
            .filter_map(|document| document["id"].as_str().map(|id| (id.to_string(), document)))
            .collect::<BTreeMap<_, _>>();
        ensure!(by_id.get(&legacy_a_id).copied() == Some(&legacy_a));
        ensure!(by_id.get(&legacy_b_id).copied() == Some(&legacy_b));
        ensure!(by_id.get(&migrated_a_id).copied() == Some(&migrated_a));
        ensure!(by_id.get(&migrated_b_id).copied() == Some(&migrated_b));
        ensure!(migrated_a_id != migrated_b_id);
        for migrated in [&migrated_a, &migrated_b] {
            ensure!(migrated["logical_id"] == shared_uri);
            ensure!(migrated["uri"] == shared_uri);
        }

        Ok(verification)
    }
    .await;

    let cleanup_result: AnyResult<()> = async {
        let task_uid = admin
            .delete_documents_by_ids("rag_company_context", &fixture_ids)
            .await
            .map_err(|error| anyhow!(error.to_string()))?;
        wait_for_optional_task(&admin, task_uid)
            .await
            .map_err(anyhow::Error::msg)?;
        require_empty_fixed_indexes(&admin).await
    }
    .await;
    let artifact_cleanup = fs::remove_dir_all(&artifact_dir);
    cleanup_result.expect("isolated migration fixtures should be removed by exact document IDs");
    artifact_cleanup.expect("migration test artifacts should be removed");

    let verification = outcome.expect("live tenant-scope migration should succeed");
    assert!(verification.ready_to_cutover);
}

#[tokio::test]
async fn meili_source_document_compatibility_mirror_supersedes_legacy_copy() {
    let _guard = live_meili_test_guard().await;
    let fixture_id = uuid::Uuid::now_v7();
    let tenant_id = format!("test-tenant-compat-mirror-{fixture_id}");
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let admin = bootstrapped_meili_admin(&config).await;
    let repository = MeiliRepository::new(admin.clone(), false);
    let logical_id = format!("compat-source-document-{fixture_id}");
    let source_id = format!("compat-source-{fixture_id}");
    let uri = format!("ctx://company/compat-mirror/{fixture_id}");
    let legacy_marker = format!("legacy-active-{fixture_id}");
    let current_marker = format!("current-superseded-{fixture_id}");
    let now = chrono::Utc::now();

    let outcome: Result<(Vec<Value>, Option<SourceDocument>), String> = async {
        let legacy_task = admin
            .add_documents(
                "rag_source_documents",
                &[json!({
                    "id": logical_id,
                    "tenant_id": tenant_id,
                    "owner_user_id": null,
                    "source_kind": "compatibility_test",
                    "source_id": source_id,
                    "revision_id": "legacy-revision",
                    "uri": uri,
                    "title": "Legacy active copy",
                    "content": legacy_marker,
                    "checksum": "legacy-checksum",
                    "status": "active",
                    "retrieval_enabled": true,
                    "created_at": now,
                    "updated_at": now
                })],
            )
            .await
            .map_err(|error| format!("legacy preseed failed: {error}"))?;
        wait_for_optional_task(&admin, legacy_task)
            .await
            .map_err(|error| format!("legacy preseed task failed: {error}"))?;

        let upsert_task = repository
            .upsert_source_documents(&[SourceDocument {
                id: logical_id.clone(),
                tenant_id: tenant_id.clone(),
                owner_user_id: None,
                source_kind: "compatibility_test".to_string(),
                source_id: source_id.clone(),
                revision_id: "current-revision".to_string(),
                uri: uri.clone(),
                title: "Current superseded copy".to_string(),
                content: current_marker.clone(),
                checksum: "current-checksum".to_string(),
                status: "superseded".to_string(),
                retrieval_enabled: false,
                created_at: now,
                updated_at: now,
            }])
            .await
            .map_err(|error| format!("tenant-scoped upsert failed: {error}"))?;
        wait_for_optional_task(&admin, upsert_task)
            .await
            .map_err(|error| format!("tenant-scoped upsert task failed: {error}"))?;

        let inventory = admin
            .search::<Value>(
                "rag_source_documents",
                json!({
                    "q": "",
                    "filter": tenant_logical_filter(&tenant_id, &logical_id),
                    "limit": 10
                }),
            )
            .await
            .map_err(|error| format!("post-upsert inventory failed: {error}"))?;
        let active = repository
            .read_source_document(&tenant_id, None, &uri)
            .await
            .map_err(|error| format!("active-filtered read failed: {error}"))?;
        Ok((inventory.hits, active))
    }
    .await;

    let cleanup = delete_tenant_rows_and_wait(&admin, &tenant_id, &["rag_source_documents"]).await;
    assert_cleanup_results(vec![("compatibility mirror rows", cleanup)]);

    let (hits, active) = outcome.expect("compatibility mirror live regression should complete");
    assert_eq!(hits.len(), 2, "expected legacy and ts1 copies: {hits:?}");
    let legacy = hits
        .iter()
        .find(|document| document["id"] == logical_id)
        .expect("same-tenant legacy copy should remain present");
    let current = hits
        .iter()
        .find(|document| {
            document["id"] != logical_id
                && document["id"]
                    .as_str()
                    .is_some_and(|id| id.starts_with("ts1_"))
        })
        .expect("tenant-safe current copy should remain present");
    for document in [legacy, current] {
        assert_eq!(document["status"], "superseded", "{document}");
        assert_eq!(document["content"], current_marker, "{document}");
    }
    assert!(
        active.is_none(),
        "active-filtered tenant read resurrected a stale legacy copy: {active:?}"
    );
}

#[tokio::test]
async fn meili_company_source_delete_preserves_personal_auxiliary_rows() {
    let _guard = live_meili_test_guard().await;
    let fixture_id = uuid::Uuid::now_v7();
    let tenant_id = format!("test-tenant-company-delete-{fixture_id}");
    let Some(config) = meili_config_with_tenant(tenant_id.clone()).await else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let admin = bootstrapped_meili_admin(&config).await;
    let repository = MeiliRepository::new(admin.clone(), true);
    let source_id = format!("shared-company-personal-source-{fixture_id}");
    let owner_user_id = format!("personal-owner-{fixture_id}");
    let auxiliary_indexes = [
        "rag_source_documents",
        "rag_parse_artifacts",
        "rag_ingest_tasks",
        "rag_ingest_results",
    ];
    let touched_indexes = [
        "rag_sources",
        "rag_source_revisions",
        "rag_company_context",
        "rag_source_documents",
        "rag_parse_artifacts",
        "rag_ingest_tasks",
        "rag_ingest_results",
    ];

    let outcome: Result<Vec<AuxiliaryInventory>, String> = async {
        for index_uid in auxiliary_indexes {
            let shared_parse_id = format!("shared-parse-artifact-{fixture_id}");
            let company_logical_id = if index_uid == "rag_parse_artifacts" {
                shared_parse_id.clone()
            } else {
                format!("{index_uid}-company-{fixture_id}")
            };
            let personal_logical_id = if index_uid == "rag_parse_artifacts" {
                shared_parse_id
            } else {
                format!("{index_uid}-personal-{fixture_id}")
            };
            let documents = [
                auxiliary_document(index_uid, &tenant_id, &company_logical_id, None, &source_id),
                auxiliary_document(
                    index_uid,
                    &tenant_id,
                    &personal_logical_id,
                    Some(&owner_user_id),
                    &source_id,
                ),
            ];
            let task = admin
                .add_documents(index_uid, &documents)
                .await
                .map_err(|error| format!("{index_uid} fixture write failed: {error}"))?;
            wait_for_optional_task(&admin, task)
                .await
                .map_err(|error| format!("{index_uid} fixture task failed: {error}"))?;
        }

        let mut before = Vec::new();
        for index_uid in auxiliary_indexes {
            let inventory = admin
                .search::<Value>(
                    index_uid,
                    json!({
                        "q": "",
                        "filter": tenant_resource_filter(&tenant_id, "source_id", &source_id),
                        "limit": 10
                    }),
                )
                .await
                .map_err(|error| format!("{index_uid} pre-delete inventory failed: {error}"))?;
            before.push((index_uid, inventory.hits));
        }

        let report = repository
            .delete_company_source(&tenant_id, &source_id, &[], &[])
            .await
            .map_err(|error| format!("company source delete failed: {error}"))?;
        let mut deletion_tasks = report.auxiliary_tasks.clone();
        deletion_tasks.extend(
            [
                report.fragments_task.clone(),
                report.revisions_task.clone(),
                report.source_task.clone(),
            ]
            .into_iter()
            .flatten(),
        );
        admin
            .wait_for_tasks(&deletion_tasks)
            .await
            .map_err(|error| format!("company source delete task failed: {error}"))?;

        let mut inventories = Vec::new();
        for (index_uid, before_hits) in before {
            let after = admin
                .search::<Value>(
                    index_uid,
                    json!({
                        "q": "",
                        "filter": tenant_resource_filter(&tenant_id, "source_id", &source_id),
                        "limit": 10
                    }),
                )
                .await
                .map_err(|error| format!("{index_uid} post-delete inventory failed: {error}"))?;
            inventories.push((index_uid, before_hits, after.hits));
        }
        Ok(inventories)
    }
    .await;

    let cleanup = delete_tenant_rows_and_wait(&admin, &tenant_id, &touched_indexes).await;
    assert_cleanup_results(vec![("company-delete auxiliary rows", cleanup)]);

    let inventories = outcome.expect("company-source auxiliary delete regression should complete");
    assert_eq!(inventories.len(), auxiliary_indexes.len());
    for (index_uid, before, after) in inventories {
        assert_eq!(
            before.len(),
            2,
            "{index_uid} fixture must contain company and personal rows: {before:?}"
        );
        assert!(before.iter().any(|row| row["owner_user_id"].is_null()));
        assert!(before
            .iter()
            .any(|row| row["owner_user_id"] == owner_user_id));
        assert_eq!(
            after.len(),
            1,
            "{index_uid} should retain only its personal row: {after:?}"
        );
        assert_eq!(after[0]["owner_user_id"], owner_user_id);
        assert_eq!(after[0]["source_id"], source_id);
    }
}

#[tokio::test]
async fn meili_all_fixed_indexes_enforce_tenant_safe_logical_identity_lifecycle() {
    let _guard = live_meili_test_guard().await;
    let Some((config_a, config_b, admin, _state_a, _app_a, _state_b, _app_b)) =
        two_tenant_meili_fixture().await
    else {
        eprintln!("skipping Meilisearch integration test; set RAG_TEST_MEILI_URL");
        return;
    };
    let tenant_a = config_a.tenant_id.clone();
    let tenant_b = config_b.tenant_id.clone();
    let repository = MeiliRepository::new(admin.clone(), true);
    let fixture_id = uuid::Uuid::now_v7();
    let shared_id = format!("tenant-scope-all-fixed-{fixture_id}");
    let shared_context_uri = format!("ctx://company/tenant-scope-all-fixed/{fixture_id}");
    let marker_a = format!("all-fixed-a-{}", uuid::Uuid::now_v7());
    let marker_a_updated = format!("all-fixed-a-updated-{}", uuid::Uuid::now_v7());
    let marker_b = format!("all-fixed-b-{}", uuid::Uuid::now_v7());
    let mut failures = Vec::new();

    for index_uid in FIXED_INDEXES {
        let logical_id = if *index_uid == "rag_company_context" {
            shared_context_uri.as_str()
        } else {
            shared_id.as_str()
        };
        let persistence_kind = if *index_uid == "rag_harness_components" {
            "rag_harness_components:component"
        } else {
            index_uid
        };
        let document = |tenant_id: &str, marker: &str| {
            let value = json!({
                "id": logical_id,
                "tenant_id": tenant_id,
                "uri": logical_id,
                "task_id": logical_id,
                "run_id": logical_id,
                "source_id": logical_id,
                "snapshot_id": logical_id,
                "doc_kind": "component",
                "status": "active",
                "privacy": "company",
                "retrieval_enabled": true,
                "retrieval_role": "fragment",
                "marker": marker,
                "title": marker,
                "body": marker
            });
            if *index_uid == "rag_structured_rows" {
                tenant_structured_row_document(tenant_id, &value)
            } else if *index_uid == "rag_parse_artifacts" {
                let storage_identity = owner_scoped_storage_identity(None, logical_id)
                    .expect("company parse-artifact scope should be valid");
                tenant_document_with_storage_identity(
                    tenant_id,
                    persistence_kind,
                    logical_id,
                    &storage_identity,
                    &value,
                )
            } else {
                tenant_document(tenant_id, persistence_kind, logical_id, &value)
            }
            .expect("tenant-scoped generic fixture should serialize")
        };

        let initial_write = match admin
            .add_documents(
                index_uid,
                &[
                    document(&tenant_a, &marker_a),
                    document(&tenant_b, &marker_b),
                ],
            )
            .await
        {
            Ok(task_uid) => wait_for_optional_task(&admin, task_uid).await,
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = initial_write {
            failures.push(format!("{index_uid}: initial write failed: {error}"));
            continue;
        }

        match admin
            .search::<Value>(
                index_uid,
                json!({
                    "q": "",
                    "filter": two_tenant_logical_filter(&tenant_a, &tenant_b, logical_id),
                    "limit": 10
                }),
            )
            .await
        {
            Ok(response) => {
                if response.hits.len() != 2 {
                    failures.push(format!(
                        "{index_uid}: expected two tenant rows, found {}",
                        response.hits.len()
                    ));
                } else {
                    let first_id = response.hits[0]["id"].as_str().unwrap_or_default();
                    let second_id = response.hits[1]["id"].as_str().unwrap_or_default();
                    if first_id == second_id
                        || !first_id.starts_with("ts1_")
                        || !second_id.starts_with("ts1_")
                    {
                        failures.push(format!(
                            "{index_uid}: internal IDs were not distinct tenant-safe ts1 IDs"
                        ));
                    }
                    if response
                        .hits
                        .iter()
                        .any(|hit| hit["logical_id"].as_str() != Some(logical_id))
                    {
                        failures.push(format!(
                            "{index_uid}: raw rows did not preserve the public logical ID"
                        ));
                    }
                }
            }
            Err(error) => failures.push(format!("{index_uid}: raw inventory failed: {error}")),
        }

        for tenant_id in [&tenant_a, &tenant_b] {
            match repository.debug_search(tenant_id, index_uid, "").await {
                Ok(Some(body)) => {
                    let hits = body["hits"].as_array();
                    if !hits.is_some_and(|hits| {
                        !hits.is_empty()
                            && hits.iter().all(|hit| {
                                hit["tenant_id"].as_str() == Some(tenant_id.as_str())
                                    && hit["id"].as_str() == Some(logical_id)
                            })
                    }) {
                        failures.push(format!(
                            "{index_uid}: tenant debug did not restore/scoped public IDs for {tenant_id}: {body}"
                        ));
                    }
                }
                Ok(None) => failures.push(format!(
                    "{index_uid}: tenant debug unexpectedly unsupported for {tenant_id}"
                )),
                Err(error) => failures.push(format!(
                    "{index_uid}: tenant debug failed for {tenant_id}: {error}"
                )),
            }
        }

        let update_a = match admin
            .add_documents(index_uid, &[document(&tenant_a, &marker_a_updated)])
            .await
        {
            Ok(task_uid) => wait_for_optional_task(&admin, task_uid).await,
            Err(error) => Err(error.to_string()),
        };
        if let Err(error) = update_a {
            failures.push(format!("{index_uid}: tenant A update failed: {error}"));
            continue;
        }

        for (tenant_id, expected_marker) in [
            (&tenant_a, marker_a_updated.as_str()),
            (&tenant_b, marker_b.as_str()),
        ] {
            match admin
                .search::<Value>(
                    index_uid,
                    json!({
                        "q": "",
                        "filter": tenant_logical_filter(tenant_id, logical_id),
                        "limit": 10
                    }),
                )
                .await
            {
                Ok(response)
                    if response.hits.len() == 1
                        && response.hits[0]["marker"].as_str() == Some(expected_marker) => {}
                Ok(response) => failures.push(format!(
                    "{index_uid}: update isolation failed for {tenant_id}: {:?}",
                    response.hits
                )),
                Err(error) => failures.push(format!(
                    "{index_uid}: post-update inventory failed for {tenant_id}: {error}"
                )),
            }
        }

        if let Err(error) = delete_by_filter_and_wait(
            &admin,
            index_uid,
            &tenant_logical_filter(&tenant_a, logical_id),
        )
        .await
        {
            failures.push(format!("{index_uid}: tenant A delete failed: {error}"));
            continue;
        }

        for (tenant_id, expected_count) in [(&tenant_a, 0usize), (&tenant_b, 1usize)] {
            match admin
                .search::<Value>(
                    index_uid,
                    json!({
                        "q": "",
                        "filter": tenant_logical_filter(tenant_id, logical_id),
                        "limit": 10
                    }),
                )
                .await
            {
                Ok(response) if response.hits.len() == expected_count => {}
                Ok(response) => failures.push(format!(
                    "{index_uid}: tenant A delete changed {tenant_id} count to {}, expected {expected_count}",
                    response.hits.len()
                )),
                Err(error) => failures.push(format!(
                    "{index_uid}: post-delete inventory failed for {tenant_id}: {error}"
                )),
            }
        }
    }

    let cleanup_results = vec![
        (
            "tenant A all-fixed rows",
            delete_tenant_rows_and_wait(&admin, &tenant_a, FIXED_INDEXES).await,
        ),
        (
            "tenant B all-fixed rows",
            delete_tenant_rows_and_wait(&admin, &tenant_b, FIXED_INDEXES).await,
        ),
    ];
    assert_cleanup_results(cleanup_results);
    assert!(
        failures.is_empty(),
        "fixed-index tenant lifecycle failures:\n{}",
        failures.join("\n")
    );
}
