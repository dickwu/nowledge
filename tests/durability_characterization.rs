use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use axum::{
    body::{to_bytes, Body},
    extract::{Request as AxumRequest, State},
    http::{Method, Request, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use nowledge::{
    build_router,
    meili::{settings_for, MeiliAdmin},
    models::HydrationStatus,
    repository::{KnowledgeRepository, MeiliRepository},
    store::Store,
    tenant_scope::tenant_document,
    AppState, Config,
};
use serde_json::{json, Value};
use tower::ServiceExt;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum DurabilityClass {
    DurableCanonical,
    DerivedDurable,
    Ephemeral,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum RestartStrategy {
    StartupHydrated,
    ReadThrough,
    RegistryHydratedReadThrough,
    NotRequired,
}

#[derive(Debug)]
struct DurabilityDomain {
    name: &'static str,
    store_fields: &'static [&'static str],
    durability: DurabilityClass,
    strategy: RestartStrategy,
    repository_writes: &'static [&'static str],
    repository_reads: &'static [&'static str],
    startup_methods: &'static [&'static str],
    report_domains: &'static [&'static str],
    evidence: &'static str,
}

const DURABILITY_MATRIX: &[DurabilityDomain] = &[
    DurabilityDomain {
        name: "shared_mutation_audit",
        store_fields: &["audit_records"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::NotRequired,
        repository_writes: &["upsert_audit_record"],
        repository_reads: &[],
        startup_methods: &[],
        report_domains: &[],
        evidence: "the durable repository is canonical while the cache only retains in-flight attempt state and has no query surface",
    },
    DurabilityDomain {
        name: "operation_journal",
        store_fields: &["operations"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_operation"],
        repository_reads: &[
            "get_operation",
            "list_operations_by_ids",
            "list_oldest_reconcilable_operations",
        ],
        startup_methods: &["list_oldest_reconcilable_operations"],
        report_domains: &["operations"],
        evidence: "immutable mutation plans and progress checkpoints are reconciled before domain hydration",
    },
    DurabilityDomain {
        name: "user_events",
        store_fields: &[
            "user_indexes",
            "events_by_index",
            "event_by_id",
            "event_idempotency",
        ],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::RegistryHydratedReadThrough,
        repository_writes: &["ensure_user_event_index", "append_event"],
        repository_reads: &["list_user_event_indexes", "search_user_events", "get_event"],
        startup_methods: &["list_user_event_indexes"],
        report_domains: &["user_event_indexes"],
        evidence: "the registry is hydrated while event bodies remain owner-index read-through",
    },
    DurabilityDomain {
        name: "personal_context",
        store_fields: &["personal_context", "personal_context_loaded"],
        durability: DurabilityClass::DerivedDurable,
        strategy: RestartStrategy::ReadThrough,
        repository_writes: &["upsert_context_nodes"],
        repository_reads: &[
            "search_context",
            "read_context_node",
            "list_personal_context_nodes",
        ],
        startup_methods: &[],
        report_domains: &["personal_context"],
        evidence: "high-volume personal context is repository canonical and lazily read",
    },
    DurabilityDomain {
        name: "company_context",
        store_fields: &["company_context"],
        durability: DurabilityClass::DerivedDurable,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_context_nodes"],
        repository_reads: &["search_context", "read_context_node"],
        startup_methods: &["list_company_context_nodes"],
        report_domains: &["company_context_nodes"],
        evidence: "company ContextFS listings require a complete startup projection",
    },
    DurabilityDomain {
        name: "state_items",
        store_fields: &["state_items"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_state_item"],
        repository_reads: &["list_state_items"],
        startup_methods: &["list_state_items"],
        report_domains: &["state_items"],
        evidence: "direct state get/search APIs are backed by a reconstructed canonical map",
    },
    DurabilityDomain {
        name: "insights",
        store_fields: &["insights", "insight_idempotency"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_insight"],
        repository_reads: &["list_insights"],
        startup_methods: &["list_insights"],
        report_domains: &["insights"],
        evidence: "insight IDs, owner ACLs, and search results survive process replacement",
    },
    DurabilityDomain {
        name: "links",
        store_fields: &["links", "link_idempotency"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_links"],
        repository_reads: &["list_links"],
        startup_methods: &["list_links"],
        report_domains: &["links"],
        evidence: "link graph queries must reconstruct the complete tenant-local graph",
    },
    DurabilityDomain {
        name: "company_sources_and_revisions",
        store_fields: &["sources", "source_revisions"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_company_source", "upsert_source_revision"],
        repository_reads: &["list_company_sources", "list_source_revisions"],
        startup_methods: &["list_company_sources", "list_source_revisions"],
        report_domains: &["company_sources", "source_revisions"],
        evidence: "source pointers and revision histories are required for edit and activation",
    },
    DurabilityDomain {
        name: "source_documents",
        store_fields: &["source_documents"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::ReadThrough,
        repository_writes: &["upsert_source_documents"],
        repository_reads: &[
            "read_source_document",
            "list_source_documents_by_uri",
            "list_company_source_documents",
        ],
        startup_methods: &[],
        report_domains: &["source_documents"],
        evidence: "source bodies are high-volume and are resolved through tenant-aware reads",
    },
    DurabilityDomain {
        name: "parse_artifacts",
        store_fields: &["parse_artifacts"],
        durability: DurabilityClass::DerivedDurable,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_parse_artifacts"],
        repository_reads: &["list_parse_artifacts", "list_tenant_parse_artifacts"],
        startup_methods: &["list_tenant_parse_artifacts"],
        report_domains: &["parse_artifacts"],
        evidence: "all tenant artifact rows are hydrated independently of task/result retention",
    },
    DurabilityDomain {
        name: "parsed_blocks",
        store_fields: &["parsed_blocks"],
        durability: DurabilityClass::Ephemeral,
        strategy: RestartStrategy::NotRequired,
        repository_writes: &[],
        repository_reads: &[],
        startup_methods: &[],
        report_domains: &[],
        evidence: "blocks are an opportunistic cache while retained ingest results exist",
    },
    DurabilityDomain {
        name: "ingest_tasks_and_results",
        store_fields: &["ingest_tasks", "ingest_results"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_ingest_task", "upsert_ingest_result"],
        repository_reads: &["list_ingest_tasks", "list_ingest_results"],
        startup_methods: &["list_ingest_tasks", "list_ingest_results"],
        report_domains: &["ingest_tasks", "ingest_results"],
        evidence: "task state is recovered before publish and nonterminal work is terminalized",
    },
    DurabilityDomain {
        name: "company_doc_preflight_decisions",
        store_fields: &["preflight_decisions"],
        durability: DurabilityClass::Ephemeral,
        strategy: RestartStrategy::NotRequired,
        repository_writes: &[],
        repository_reads: &[],
        startup_methods: &[],
        report_domains: &[],
        evidence: "preflight decisions are short-lived admission tokens, not knowledge records",
    },
    DurabilityDomain {
        name: "dataset_schemas",
        store_fields: &["datasets"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_dataset"],
        repository_reads: &["list_datasets"],
        startup_methods: &["list_datasets"],
        report_domains: &["datasets"],
        evidence: "snapshot validation requires schemas after restart",
    },
    DurabilityDomain {
        name: "structured_snapshots",
        store_fields: &["snapshots", "snapshot_idempotency"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_structured_snapshot"],
        repository_reads: &["get_snapshot", "list_structured_snapshots"],
        startup_methods: &["list_structured_snapshots"],
        report_domains: &["structured_snapshots"],
        evidence: "snapshot owner ACL and current-state reconstruction require metadata hydration",
    },
    DurabilityDomain {
        name: "structured_rows",
        store_fields: &["rows_by_snapshot", "row_idempotency"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::ReadThrough,
        repository_writes: &["upsert_structured_rows"],
        repository_reads: &["list_rows"],
        startup_methods: &[],
        report_domains: &["structured_rows"],
        evidence: "rows are tenant-scoped, paginated, and lazily loaded per snapshot",
    },
    DurabilityDomain {
        name: "structured_summaries",
        store_fields: &["structured_summaries"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_structured_summary"],
        repository_reads: &["list_structured_summaries"],
        startup_methods: &["list_structured_summaries"],
        report_domains: &["structured_summaries"],
        evidence: "current structured state must include previously materialized summaries",
    },
    DurabilityDomain {
        name: "sessions",
        store_fields: &["sessions"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_session"],
        repository_reads: &["list_sessions"],
        startup_methods: &["list_sessions"],
        report_domains: &["sessions"],
        evidence: "session messages and commit state are public durable API objects",
    },
    DurabilityDomain {
        name: "traces",
        store_fields: &["traces"],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &["upsert_trace"],
        repository_reads: &["get_trace", "list_traces"],
        startup_methods: &["list_traces"],
        report_domains: &["traces"],
        evidence: "trace ownership and reveal authorization require complete startup hydration",
    },
    DurabilityDomain {
        name: "eval_and_harness",
        store_fields: &[
            "harness_components",
            "harness_revisions",
            "harness_changes",
            "harness_verdicts",
            "eval_cases",
            "eval_runs",
            "eval_case_results",
            "eval_overviews",
        ],
        durability: DurabilityClass::DurableCanonical,
        strategy: RestartStrategy::StartupHydrated,
        repository_writes: &[
            "upsert_harness_components",
            "upsert_harness_changes",
            "upsert_harness_verdicts",
            "upsert_eval_case",
            "upsert_eval_run",
            "upsert_eval_case_results",
            "upsert_eval_overview",
        ],
        repository_reads: &[
            "list_harness_components",
            "list_harness_component_revisions",
            "list_harness_changes",
            "list_harness_verdicts",
            "list_eval_cases",
            "list_eval_runs",
            "list_eval_case_results",
            "get_eval_overview",
        ],
        startup_methods: &[
            "list_harness_components",
            "list_harness_component_revisions",
            "list_harness_changes",
            "list_harness_verdicts",
            "list_eval_cases",
            "list_eval_runs",
        ],
        report_domains: &[
            "harness_components",
            "harness_revisions",
            "harness_changes",
            "harness_verdicts",
            "eval_cases",
            "eval_runs",
            "eval_case_results",
            "eval_overviews",
        ],
        evidence: "harness and eval control-plane state is completely reloaded",
    },
    DurabilityDomain {
        name: "hydration_operational_state",
        store_fields: &["hydration_report"],
        durability: DurabilityClass::Ephemeral,
        strategy: RestartStrategy::NotRequired,
        repository_writes: &[],
        repository_reads: &[],
        startup_methods: &[],
        report_domains: &[],
        evidence: "the hydration report describes this process startup and is never canonical data",
    },
];

fn function_body<'a>(source: &'a str, signature: &str) -> &'a str {
    let start = source
        .find(signature)
        .unwrap_or_else(|| panic!("missing function signature: {signature}"));
    let open = source[start..]
        .find('{')
        .map(|offset| start + offset)
        .unwrap_or_else(|| panic!("missing opening brace for: {signature}"));
    let mut depth = 0usize;
    for (offset, byte) in source.as_bytes()[open..].iter().enumerate() {
        match byte {
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return &source[open + 1..open + offset];
                }
            }
            _ => {}
        }
    }
    panic!("missing closing brace for: {signature}");
}

fn declared_store_fields(store_data: &str) -> BTreeSet<&str> {
    store_data
        .lines()
        .filter_map(|line| line.trim().split_once(':').map(|(field, _)| field.trim()))
        .filter(|field| !field.is_empty())
        .collect()
}

#[test]
fn every_store_domain_has_an_explicit_restart_contract() {
    let repository = include_str!("../src/repository.rs");
    let repository_trait = function_body(repository, "pub trait KnowledgeRepository");
    let store = include_str!("../src/store.rs");
    let store_data = function_body(store, "struct StoreData");

    let mut classified_fields = BTreeSet::new();
    let mut domain_names = BTreeSet::new();
    let mut durability_classes = BTreeSet::new();
    let mut restart_strategies = BTreeSet::new();
    for domain in DURABILITY_MATRIX {
        assert!(
            domain_names.insert(domain.name),
            "duplicate domain: {}",
            domain.name
        );
        assert!(
            !domain.evidence.is_empty(),
            "{} lacks evidence",
            domain.name
        );
        durability_classes.insert(domain.durability);
        restart_strategies.insert(domain.strategy);

        for field in domain.store_fields {
            assert!(
                classified_fields.insert(*field),
                "StoreData field {field} is classified more than once"
            );
        }
        for method in domain
            .repository_writes
            .iter()
            .chain(domain.repository_reads)
        {
            assert!(
                repository_trait.contains(&format!("async fn {method}")),
                "{} requires explicit repository method {method}",
                domain.name
            );
        }
        for method in domain.startup_methods {
            assert!(
                store.contains(&format!(".{method}(")),
                "{} requires startup recovery through {method}",
                domain.name
            );
        }
    }

    assert_eq!(classified_fields, declared_store_fields(store_data));
    assert_eq!(
        durability_classes,
        [
            DurabilityClass::DurableCanonical,
            DurabilityClass::DerivedDurable,
            DurabilityClass::Ephemeral,
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        restart_strategies,
        [
            RestartStrategy::StartupHydrated,
            RestartStrategy::ReadThrough,
            RestartStrategy::RegistryHydratedReadThrough,
            RestartStrategy::NotRequired,
        ]
        .into_iter()
        .collect()
    );
}

#[test]
fn hydration_loads_artifact_rows_and_opportunistically_rebuilds_retained_blocks() {
    let store = include_str!("../src/store.rs");
    let load = function_body(store, "async fn load_hydration_stage");
    let publish = function_body(store, "fn publish_hydration_stage");

    assert!(load.contains(".list_tenant_parse_artifacts("));
    assert!(
        publish.contains("parse_artifacts"),
        "tenant artifact hydration must reconstruct the scoped artifact map"
    );
    assert!(load.contains(".list_ingest_results("));
    assert!(
        publish.contains("parsed_blocks"),
        "ingest-result hydration must reconstruct the parsed block map"
    );
}

#[derive(Clone, Copy, Debug)]
enum StubBehavior {
    Empty,
    MissingIndexes,
    FailLateHydration,
    RecoverTasks,
    FailRecoveryWrite,
    FailRecoveryTaskWait,
    FailRecoveryResultWriteOnce,
    FailRecoveryResultCommitOnce,
    PersistedHarnessOverride,
}

#[derive(Clone)]
struct MeiliStubState {
    behavior: StubBehavior,
    ingest_task_documents: Arc<Mutex<Vec<Value>>>,
    ingest_result_documents: Arc<Mutex<Vec<Value>>>,
    ingest_task_writes: Arc<AtomicUsize>,
    ingest_result_write_attempts: Arc<AtomicUsize>,
    ingest_result_writes: Arc<AtomicUsize>,
    result_write_failures_remaining: Arc<AtomicUsize>,
    result_commit_failures_remaining: Arc<AtomicUsize>,
    staged_component_served: Arc<AtomicBool>,
}

impl MeiliStubState {
    fn empty(behavior: StubBehavior) -> Self {
        Self {
            behavior,
            ingest_task_documents: Arc::new(Mutex::new(Vec::new())),
            ingest_result_documents: Arc::new(Mutex::new(Vec::new())),
            ingest_task_writes: Arc::new(AtomicUsize::new(0)),
            ingest_result_write_attempts: Arc::new(AtomicUsize::new(0)),
            ingest_result_writes: Arc::new(AtomicUsize::new(0)),
            result_write_failures_remaining: Arc::new(AtomicUsize::new(0)),
            result_commit_failures_remaining: Arc::new(AtomicUsize::new(0)),
            staged_component_served: Arc::new(AtomicBool::new(false)),
        }
    }

    fn with_recovery_tasks(behavior: StubBehavior) -> Self {
        let queued = tenant_document(
            "tenant-recovery",
            "rag_ingest_tasks",
            "task-queued",
            &json!({
                "task_id": "task-queued",
                "tenant_id": "tenant-recovery",
                "owner_user_id": "owner-a",
                "source_id": "source-a",
                "revision_id": "revision-a",
                "parser_provider": "builtin",
                "parser_backend": "text",
                "state": "queued",
                "created_at": "2026-07-14T00:00:00Z",
                "updated_at": "2026-07-14T00:00:00Z"
            }),
        )
        .expect("queued task fixture should be tenant-scoped");
        let completed = tenant_document(
            "tenant-recovery",
            "rag_ingest_tasks",
            "task-completed",
            &json!({
                "task_id": "task-completed",
                "tenant_id": "tenant-recovery",
                "owner_user_id": "owner-a",
                "source_id": "source-b",
                "revision_id": "revision-b",
                "parser_provider": "builtin",
                "parser_backend": "text",
                "state": "completed",
                "created_at": "2026-07-14T00:00:00Z",
                "updated_at": "2026-07-14T00:01:00Z",
                "completed_at": "2026-07-14T00:01:00Z"
            }),
        )
        .expect("completed task fixture should be tenant-scoped");
        Self {
            behavior,
            ingest_task_documents: Arc::new(Mutex::new(vec![queued, completed])),
            ingest_result_documents: Arc::new(Mutex::new(Vec::new())),
            ingest_task_writes: Arc::new(AtomicUsize::new(0)),
            ingest_result_write_attempts: Arc::new(AtomicUsize::new(0)),
            ingest_result_writes: Arc::new(AtomicUsize::new(0)),
            result_write_failures_remaining: Arc::new(AtomicUsize::new(0)),
            result_commit_failures_remaining: Arc::new(AtomicUsize::new(0)),
            staged_component_served: Arc::new(AtomicBool::new(false)),
        }
    }

    fn with_recovery_result(behavior: StubBehavior) -> Self {
        let task = json!({
            "task_id": "task-with-result",
            "tenant_id": "tenant-recovery",
            "owner_user_id": "owner-a",
            "source_id": "source-with-result",
            "revision_id": "revision-with-result",
            "source_document_uri": "ctx://users/owner-a/sources/source-with-result",
            "parser_provider": "builtin",
            "parser_backend": "text",
            "state": "queued",
            "created_at": "2026-07-14T00:00:00Z",
            "updated_at": "2026-07-14T00:00:00Z"
        });
        let task_document = tenant_document(
            "tenant-recovery",
            "rag_ingest_tasks",
            "task-with-result",
            &task,
        )
        .expect("queued task fixture should be tenant-scoped");
        let mut result_document = tenant_document(
            "tenant-recovery",
            "rag_ingest_results",
            "task-with-result",
            &json!({
                "task": task,
                "source_document_uri": "ctx://users/owner-a/sources/source-with-result",
                "source_id": "source-with-result",
                "revision_id": "revision-with-result",
                "parse_artifacts": [],
                "parsed_blocks": [],
                "fragment_uris": [],
                "context_uris": []
            }),
        )
        .expect("ingest-result fixture should be tenant-scoped");
        result_document["task_id"] = json!("task-with-result");
        result_document["owner_user_id"] = json!("owner-a");
        Self {
            behavior,
            ingest_task_documents: Arc::new(Mutex::new(vec![task_document])),
            ingest_result_documents: Arc::new(Mutex::new(vec![result_document])),
            ingest_task_writes: Arc::new(AtomicUsize::new(0)),
            ingest_result_write_attempts: Arc::new(AtomicUsize::new(0)),
            ingest_result_writes: Arc::new(AtomicUsize::new(0)),
            result_write_failures_remaining: Arc::new(AtomicUsize::new(usize::from(matches!(
                behavior,
                StubBehavior::FailRecoveryResultWriteOnce
            )))),
            result_commit_failures_remaining: Arc::new(AtomicUsize::new(usize::from(matches!(
                behavior,
                StubBehavior::FailRecoveryResultCommitOnce
            )))),
            staged_component_served: Arc::new(AtomicBool::new(false)),
        }
    }
}

async fn spawn_meili_stub(state: MeiliStubState) -> String {
    let app = Router::new().fallback(meili_stub).with_state(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("stub should bind");
    let address = listener.local_addr().expect("stub should have an address");
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("stub should serve");
    });
    format!("http://{address}")
}

async fn meili_stub(State(state): State<MeiliStubState>, request: AxumRequest) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();

    if method == Method::GET && path == "/health" {
        return (StatusCode::OK, Json(json!({ "status": "available" }))).into_response();
    }
    if method == Method::GET && path.ends_with("/settings") {
        let uid = path
            .strip_prefix("/indexes/")
            .and_then(|path| path.strip_suffix("/settings"))
            .expect("settings request should contain an index UID");
        return (StatusCode::OK, Json(settings_for(uid))).into_response();
    }
    if method == Method::GET && path.starts_with("/indexes/") {
        let uid = path.trim_start_matches("/indexes/");
        return (
            StatusCode::OK,
            Json(json!({
                "uid": uid,
                "primaryKey": "id",
                "createdAt": "2026-07-14T00:00:00Z"
            })),
        )
            .into_response();
    }
    if method == Method::GET
        && path.starts_with("/tasks/")
        && matches!(state.behavior, StubBehavior::FailRecoveryTaskWait)
    {
        return (
            StatusCode::OK,
            Json(json!({
                "status": "failed",
                "error": { "message": "injected recovery task failure" }
            })),
        )
            .into_response();
    }
    if method == Method::GET
        && path == "/tasks/42"
        && take_failure(&state.result_commit_failures_remaining)
    {
        return (
            StatusCode::OK,
            Json(json!({
                "status": "failed",
                "error": { "message": "injected ingest-result commit failure" }
            })),
        )
            .into_response();
    }
    if method == Method::GET && path.starts_with("/tasks/") {
        return (StatusCode::OK, Json(json!({ "status": "succeeded" }))).into_response();
    }

    let is_scan =
        method == Method::POST && (path.ends_with("/search") || path.ends_with("/documents/fetch"));
    if is_scan {
        let body = request_json(request).await;
        if matches!(state.behavior, StubBehavior::MissingIndexes) {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({ "code": "index_not_found" })),
            )
                .into_response();
        }

        let index_uid = index_uid(&path).unwrap_or_default();
        if matches!(state.behavior, StubBehavior::FailLateHydration)
            && index_uid == "rag_ingest_results"
        {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "message": "injected late hydration failure" })),
            )
                .into_response();
        }

        let filter = body.get("filter").and_then(Value::as_str).unwrap_or("");
        let rows = if matches!(state.behavior, StubBehavior::FailLateHydration)
            && index_uid == "rag_harness_components"
            && filter.contains("doc_kind = \"component\"")
        {
            state.staged_component_served.store(true, Ordering::SeqCst);
            vec![remote_harness_component_document()]
        } else if matches!(state.behavior, StubBehavior::PersistedHarnessOverride)
            && index_uid == "rag_harness_components"
            && filter.contains("doc_kind = \"component\"")
        {
            vec![persisted_harness_override_document()]
        } else if matches!(
            state.behavior,
            StubBehavior::RecoverTasks
                | StubBehavior::FailRecoveryWrite
                | StubBehavior::FailRecoveryTaskWait
                | StubBehavior::FailRecoveryResultWriteOnce
                | StubBehavior::FailRecoveryResultCommitOnce
        ) && index_uid == "rag_ingest_tasks"
            && !filter.contains("id IN")
        {
            state.ingest_task_documents.lock().unwrap().clone()
        } else if matches!(
            state.behavior,
            StubBehavior::FailRecoveryResultWriteOnce | StubBehavior::FailRecoveryResultCommitOnce
        ) && index_uid == "rag_ingest_results"
        {
            state.ingest_result_documents.lock().unwrap().clone()
        } else {
            Vec::new()
        };
        return scan_response(&path, &body, rows);
    }

    if method == Method::POST && path.ends_with("/documents") {
        let body = request_json(request).await;
        let index_uid = index_uid(&path).unwrap_or_default();
        if index_uid == "rag_ingest_tasks" {
            if matches!(state.behavior, StubBehavior::FailRecoveryWrite) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "message": "injected recovery write failure" })),
                )
                    .into_response();
            }
            let documents = body.as_array().cloned().unwrap_or_default();
            let mut stored = state.ingest_task_documents.lock().unwrap();
            for document in documents {
                let Some(task_id) = document.get("task_id").and_then(Value::as_str) else {
                    continue;
                };
                if let Some(existing) = stored
                    .iter_mut()
                    .find(|row| row.get("task_id").and_then(Value::as_str) == Some(task_id))
                {
                    *existing = document;
                } else {
                    stored.push(document);
                }
                state.ingest_task_writes.fetch_add(1, Ordering::SeqCst);
            }
            return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 41 }))).into_response();
        }
        if index_uid == "rag_ingest_results" {
            state
                .ingest_result_write_attempts
                .fetch_add(1, Ordering::SeqCst);
            if take_failure(&state.result_write_failures_remaining) {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "message": "injected ingest-result write failure" })),
                )
                    .into_response();
            }

            // A failed asynchronous Meilisearch task does not make its submitted
            // document durable. Keep the stale fixture until the next attempt.
            if state
                .result_commit_failures_remaining
                .load(Ordering::SeqCst)
                == 0
            {
                let documents = body.as_array().cloned().unwrap_or_default();
                let mut stored = state.ingest_result_documents.lock().unwrap();
                for document in documents {
                    let Some(task_id) = document.get("task_id").and_then(Value::as_str) else {
                        continue;
                    };
                    if let Some(existing) = stored
                        .iter_mut()
                        .find(|row| row.get("task_id").and_then(Value::as_str) == Some(task_id))
                    {
                        *existing = document;
                    } else {
                        stored.push(document);
                    }
                    state.ingest_result_writes.fetch_add(1, Ordering::SeqCst);
                }
            }
            return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 42 }))).into_response();
        }
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 41 }))).into_response();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected stub request: {method} {path}") })),
    )
        .into_response()
}

fn take_failure(remaining: &AtomicUsize) -> bool {
    remaining
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
            count.checked_sub(1)
        })
        .is_ok()
}

async fn request_json(request: AxumRequest) -> Value {
    let bytes = to_bytes(request.into_body(), usize::MAX)
        .await
        .expect("stub request body should be readable");
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

fn index_uid(path: &str) -> Option<&str> {
    path.strip_prefix("/indexes/")?.split('/').next()
}

fn scan_response(path: &str, body: &Value, rows: Vec<Value>) -> Response {
    let offset = body.get("offset").and_then(Value::as_u64).unwrap_or(0) as usize;
    let limit = body.get("limit").and_then(Value::as_u64).unwrap_or(500) as usize;
    let total = rows.len();
    let page = rows
        .into_iter()
        .skip(offset)
        .take(limit.max(1))
        .collect::<Vec<_>>();
    if path.ends_with("/documents/fetch") {
        (
            StatusCode::OK,
            Json(json!({
                "results": page,
                "offset": offset,
                "limit": limit,
                "total": total
            })),
        )
            .into_response()
    } else {
        (
            StatusCode::OK,
            Json(json!({
                "hits": page,
                "estimatedTotalHits": total,
                "processingTimeMs": 0
            })),
        )
            .into_response()
    }
}

fn remote_harness_component_document() -> Value {
    let mut document = tenant_document(
        "tenant-atomic",
        "rag_harness_components:component",
        "remote-only-component",
        &json!({
            "id": "remote-only-component",
            "tenant_id": "tenant-atomic",
            "display_name": "Remote-only component",
            "component_kind": "test",
            "description": "must remain staged if a later mandatory domain fails",
            "status": "active",
            "created_at": "2026-07-14T00:00:00Z",
            "updated_at": "2026-07-14T00:00:00Z"
        }),
    )
    .expect("component fixture should be tenant-scoped");
    document["doc_kind"] = json!("component");
    document
}

fn persisted_harness_override_document() -> Value {
    let mut document = tenant_document(
        "tenant-harness",
        "rag_harness_components:component",
        "retrieval.context_search",
        &json!({
            "id": "retrieval.context_search",
            "tenant_id": "tenant-harness",
            "display_name": "Persisted context search override",
            "component_kind": "retrieval",
            "description": "a persisted override of one built-in component",
            "status": "active",
            "current_revision_id": "hrev_bootstrap_retrieval-context_search",
            "created_at": "2026-07-14T00:00:00Z",
            "updated_at": "2026-07-14T00:01:00Z"
        }),
    )
    .expect("persisted component fixture should be tenant-scoped");
    document["doc_kind"] = json!("component");
    document
}

fn meili_config(url: String, tenant_id: &str) -> Config {
    let mut config = Config::test();
    config.store_backend = "meili".to_string();
    config.meili_url = Some(url);
    config.meili_wait_for_tasks = false;
    config.ingest_worker_enabled = false;
    config.tenant_id = tenant_id.to_string();
    config
}

async fn call_json(app: Router, method: Method, uri: &str) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request should build");
    let response = app.oneshot(request).await.expect("router should respond");
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("response body should be readable");
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, body)
}

#[tokio::test]
async fn existing_empty_mandatory_index_is_distinct_from_a_missing_index() {
    let empty_url = spawn_meili_stub(MeiliStubState::empty(StubBehavior::Empty)).await;
    let empty_config = meili_config(empty_url, "tenant-empty");
    let empty_repository = MeiliRepository::new(MeiliAdmin::from_config(&empty_config), false);
    let empty = empty_repository
        .list_harness_components("tenant-empty")
        .await
        .expect("an existing empty index is a successful mandatory scan")
        .expect("Meili has an authoritative empty result");
    assert!(empty.is_empty());

    let missing_url = spawn_meili_stub(MeiliStubState::empty(StubBehavior::MissingIndexes)).await;
    let missing_config = meili_config(missing_url, "tenant-missing");
    let missing_repository = MeiliRepository::new(MeiliAdmin::from_config(&missing_config), false);
    assert!(
        missing_repository
            .list_harness_components("tenant-missing")
            .await
            .is_err(),
        "a missing mandatory index must make hydration incomplete, not look empty"
    );
}

#[tokio::test]
async fn successful_empty_hydration_returns_complete_structured_diagnostics() {
    let url = spawn_meili_stub(MeiliStubState::empty(StubBehavior::Empty)).await;
    let config = meili_config(url, "tenant-empty-report");
    let store = Store::new(&config);

    let report = store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .expect("all existing empty mandatory indexes should hydrate completely");
    let report = serde_json::to_value(report).expect("hydration report should serialize");
    assert_eq!(report["tenant_id"], "tenant-empty-report", "{report}");
    assert_eq!(report["backend"], "meili", "{report}");
    assert_eq!(report["status"], "complete", "{report}");
    assert_eq!(report["ready"], true, "{report}");
    assert!(report["started_at"].is_string(), "{report}");
    assert!(report["completed_at"].is_string(), "{report}");

    let domains = report["domains"]
        .as_object()
        .expect("structured hydration report should contain domain diagnostics");
    let expected_report_domains = DURABILITY_MATRIX
        .iter()
        .flat_map(|domain| domain.report_domains.iter().copied())
        .collect::<BTreeSet<_>>();
    for domain_name in expected_report_domains {
        let domain = domains
            .get(domain_name)
            .unwrap_or_else(|| panic!("missing hydration diagnostics for {domain_name}: {report}"));
        assert!(domain["durability"].is_string(), "{domain_name}: {domain}");
        assert!(domain["strategy"].is_string(), "{domain_name}: {domain}");
        assert!(domain["mandatory"].is_boolean(), "{domain_name}: {domain}");
        assert!(domain["status"].is_string(), "{domain_name}: {domain}");
        for count in ["expected", "loaded", "skipped", "quarantined", "recovered"] {
            assert!(
                domain[count].is_u64(),
                "{domain_name}.{count} must be an explicit count: {domain}"
            );
        }
    }
}

#[tokio::test]
async fn a_late_mandatory_hydration_failure_publishes_nothing_and_closes_readiness() {
    let stub = MeiliStubState::empty(StubBehavior::FailLateHydration);
    let staged_component_served = stub.staged_component_served.clone();
    let url = spawn_meili_stub(stub).await;
    let config = Arc::new(meili_config(url, "tenant-atomic"));
    let state = AppState::new(config.clone());

    assert!(state
        .store
        .harness_component_detail("remote-only-component")
        .is_err());
    assert!(state
        .store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .is_err());
    assert!(
        staged_component_served.load(Ordering::SeqCst),
        "the fixture must serve valid mandatory data before the injected late failure"
    );
    assert!(
        state
            .store
            .harness_component_detail("remote-only-component")
            .is_err(),
        "failed hydration must not publish an earlier successfully loaded domain"
    );

    let (status, ready) = call_json(build_router(state), Method::GET, "/readyz").await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{ready}");
    assert_eq!(ready["ready"], false, "{ready}");
}

#[tokio::test]
async fn interrupted_ingest_recovery_is_persisted_once_and_is_restart_idempotent() {
    let stub = MeiliStubState::with_recovery_tasks(StubBehavior::RecoverTasks);
    let writes = stub.ingest_task_writes.clone();
    let url = spawn_meili_stub(stub).await;
    let config = meili_config(url, "tenant-recovery");

    let first = Store::new(&config);
    let first_hydration = first
        .hydrate_from_repository(&config.tenant_id)
        .await
        .expect("first recovery should persist the terminalized task before publish");
    assert_eq!(
        first_hydration["domains"]["operations"]["loaded"], 1,
        "startup repair must be represented by a completed journal operation: {first_hydration}"
    );
    let recovered = first
        .get_ingest_task("task-queued", Some("owner-a"), false)
        .expect("recovered task should be published");
    assert_eq!(recovered.state, "failed");
    assert_eq!(recovered.error.as_deref(), Some("ingest_interrupted"));
    assert!(recovered.completed_at.is_some());
    let completed = first
        .get_ingest_task("task-completed", Some("owner-a"), false)
        .expect("already terminal task should survive hydration");
    assert_eq!(completed.state, "completed");
    assert_eq!(writes.load(Ordering::SeqCst), 1);

    let second = Store::new(&config);
    second
        .hydrate_from_repository(&config.tenant_id)
        .await
        .expect("restarting after recovery should be idempotent");
    let recovered_again = second
        .get_ingest_task("task-queued", Some("owner-a"), false)
        .expect("persisted recovery should survive the next restart");
    assert_eq!(recovered_again.state, "failed");
    assert_eq!(recovered_again.error.as_deref(), Some("ingest_interrupted"));
    assert_eq!(writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn failed_interrupted_task_persistence_does_not_publish_recovered_state() {
    let stub = MeiliStubState::with_recovery_tasks(StubBehavior::FailRecoveryWrite);
    let url = spawn_meili_stub(stub).await;
    let config = meili_config(url, "tenant-recovery");
    let store = Store::new(&config);

    assert!(store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .is_err());
    assert!(
        store
            .get_ingest_task("task-queued", Some("owner-a"), false)
            .is_err(),
        "recovery write failure must leave the live store unchanged"
    );
}

#[tokio::test]
async fn failed_interrupted_task_commit_does_not_publish_recovered_state() {
    let stub = MeiliStubState::with_recovery_tasks(StubBehavior::FailRecoveryTaskWait);
    let url = spawn_meili_stub(stub).await;
    let config = meili_config(url, "tenant-recovery");
    let store = Store::new(&config);

    assert!(store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .is_err());
    assert!(
        store
            .get_ingest_task("task-queued", Some("owner-a"), false)
            .is_err(),
        "an asynchronously failed recovery task must leave the live store unchanged"
    );
    let report = store
        .hydration_report()
        .expect("failed hydration should publish diagnostics");
    assert!(!report.ready);
    assert_eq!(report.status, HydrationStatus::Incomplete);
    assert_eq!(
        report.domains["ingest_tasks"].status, "incomplete",
        "the failed commit must identify the recovery domain"
    );
}

async fn recovery_result_failure_retries_on_next_startup(behavior: StubBehavior) {
    let stub = MeiliStubState::with_recovery_result(behavior);
    let task_documents = stub.ingest_task_documents.clone();
    let result_documents = stub.ingest_result_documents.clone();
    let task_writes = stub.ingest_task_writes.clone();
    let result_write_attempts = stub.ingest_result_write_attempts.clone();
    let result_writes = stub.ingest_result_writes.clone();
    let url = spawn_meili_stub(stub).await;
    let config = meili_config(url, "tenant-recovery");

    let first = Store::new(&config);
    assert!(
        first
            .hydrate_from_repository(&config.tenant_id)
            .await
            .is_err(),
        "the first startup must fail closed when the embedded result cannot be corrected"
    );
    let first_report = first
        .hydration_report()
        .expect("the first startup should publish failure diagnostics");
    assert!(!first_report.ready);
    assert_eq!(first_report.status, HydrationStatus::Incomplete);
    assert_eq!(first_report.domains["ingest_results"].status, "incomplete");
    assert_eq!(task_writes.load(Ordering::SeqCst), 1);
    assert_eq!(result_write_attempts.load(Ordering::SeqCst), 1);
    assert_eq!(result_writes.load(Ordering::SeqCst), 0);
    assert_eq!(task_documents.lock().unwrap()[0]["state"], "failed");
    assert_eq!(
        result_documents.lock().unwrap()[0]["task"]["state"],
        "queued",
        "the injected result failure must leave the durable embedded task stale"
    );

    let second = Store::new(&config);
    second
        .hydrate_from_repository(&config.tenant_id)
        .await
        .expect("the second startup should retry and commit the stale embedded task");

    assert_eq!(
        task_writes.load(Ordering::SeqCst),
        1,
        "the already durable terminal task should not be rewritten"
    );
    assert_eq!(
        result_write_attempts.load(Ordering::SeqCst),
        2,
        "the stale embedded result task must be retried on the second startup"
    );
    assert_eq!(result_writes.load(Ordering::SeqCst), 1);
    let result_documents = result_documents.lock().unwrap();
    let recovered_task = &result_documents[0]["task"];
    assert_eq!(recovered_task["state"], "failed");
    assert_eq!(recovered_task["error"], "ingest_interrupted");
    assert!(recovered_task["completed_at"].is_string());
    drop(result_documents);

    let second_report = second
        .hydration_report()
        .expect("the successful retry should publish readiness");
    assert!(second_report.ready);
    assert_eq!(second_report.status, HydrationStatus::Complete);
}

#[tokio::test]
async fn failed_recovered_ingest_result_write_is_retried_before_next_readiness() {
    recovery_result_failure_retries_on_next_startup(StubBehavior::FailRecoveryResultWriteOnce)
        .await;
}

#[tokio::test]
async fn failed_recovered_ingest_result_commit_is_retried_before_next_readiness() {
    recovery_result_failure_retries_on_next_startup(StubBehavior::FailRecoveryResultCommitOnce)
        .await;
}

#[tokio::test]
async fn a_persisted_builtin_override_preserves_untouched_harness_defaults() {
    let url = spawn_meili_stub(MeiliStubState::empty(
        StubBehavior::PersistedHarnessOverride,
    ))
    .await;
    let config = meili_config(url, "tenant-harness");
    let store = Store::new(&config);

    store
        .hydrate_from_repository(&config.tenant_id)
        .await
        .expect("a valid persisted built-in override should hydrate");

    let expected_component_ids = [
        "retrieval.context_search",
        "retrieval.traceback",
        "ingestion.fragmenter",
        "ingestion.parser_adapter",
        "llm.rag_answer_prompt",
        "llm.analysis_prompt",
        "memory.insight_policy",
        "memory.state_materialization_policy",
        "safety.owner_acl",
        "safety.source_doc_retrieval_guard",
        "health.llm_probe",
    ];
    let components = store
        .list_harness_components()
        .expect("the hydrated harness registry should be readable");
    assert_eq!(components.len(), expected_component_ids.len());
    let component_ids = components
        .iter()
        .map(|component| component.id.as_str())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        component_ids,
        expected_component_ids.into_iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(
        store
            .harness_component_detail("retrieval.context_search")
            .expect("the persisted override should remain present")
            .component
            .display_name,
        "Persisted context search override"
    );

    for component_id in expected_component_ids {
        let detail = store
            .harness_component_detail(component_id)
            .unwrap_or_else(|_| panic!("missing built-in harness component {component_id}"));
        let bootstrap_revision_id = format!("hrev_bootstrap_{}", component_id.replace('.', "-"));
        assert!(
            detail
                .revisions
                .iter()
                .any(|revision| revision.id == bootstrap_revision_id),
            "missing bootstrap revision for {component_id}: {:?}",
            detail.revisions
        );
    }
}
