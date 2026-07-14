use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RestartBehavior {
    StartupHydrated,
    ReadThrough,
    MixedReadThrough,
    Missing,
}

#[derive(Debug)]
struct DurabilityDomain {
    name: &'static str,
    write_methods: &'static [&'static str],
    read_methods: &'static [&'static str],
    missing_repository_methods: &'static [&'static str],
    startup_methods: &'static [&'static str],
    report_keys: &'static [&'static str],
    restart_behavior: RestartBehavior,
    evidence: &'static str,
}

const AUDITED_DOMAINS: &[&str] = &[
    "user_events",
    "personal_context",
    "company_context",
    "state_items",
    "insights",
    "links",
    "company_sources_and_revisions",
    "source_documents",
    "parse_artifacts",
    "parsed_blocks",
    "dataset_schemas",
    "structured_snapshots",
    "structured_rows",
    "structured_summaries",
    "sessions",
    "traces",
    "eval_and_harness",
    "ingest_tasks_and_results",
];

const DURABILITY_MATRIX: &[DurabilityDomain] = &[
    DurabilityDomain {
        name: "user_events",
        write_methods: &["ensure_user_event_index", "append_event"],
        read_methods: &["search_user_events", "get_event"],
        missing_repository_methods: &["list_user_event_indexes"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::ReadThrough,
        evidence: "meili_restart_characterizes_state_link_and_session_durability_gaps",
    },
    DurabilityDomain {
        name: "personal_context",
        write_methods: &["upsert_context_nodes"],
        read_methods: &["search_context", "read_context_node"],
        missing_repository_methods: &[],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::ReadThrough,
        evidence: "meili_restart_characterizes_state_link_and_session_durability_gaps",
    },
    DurabilityDomain {
        name: "company_context",
        write_methods: &["upsert_context_nodes"],
        read_methods: &["search_context", "read_context_node"],
        missing_repository_methods: &[],
        startup_methods: &["list_company_context_nodes"],
        report_keys: &["company_context_nodes"],
        restart_behavior: RestartBehavior::StartupHydrated,
        evidence: "meili_restart_hydrates_company_context_sources_and_revisions",
    },
    DurabilityDomain {
        name: "state_items",
        write_methods: &["upsert_state_item"],
        read_methods: &[],
        missing_repository_methods: &["get_state_item", "list_state_items"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "meili_restart_characterizes_state_link_and_session_durability_gaps",
    },
    DurabilityDomain {
        name: "insights",
        write_methods: &[],
        read_methods: &[],
        missing_repository_methods: &["upsert_insight", "list_insights"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "repository surface has no canonical insight persistence",
    },
    DurabilityDomain {
        name: "links",
        write_methods: &["upsert_links"],
        read_methods: &[],
        missing_repository_methods: &["list_links", "search_links"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "meili_restart_characterizes_state_link_and_session_durability_gaps",
    },
    DurabilityDomain {
        name: "company_sources_and_revisions",
        write_methods: &["upsert_company_source", "upsert_source_revision"],
        read_methods: &[],
        missing_repository_methods: &[],
        startup_methods: &["list_company_sources", "list_source_revisions"],
        report_keys: &["company_sources", "source_revisions"],
        restart_behavior: RestartBehavior::StartupHydrated,
        evidence: "meili_restart_hydrates_company_context_sources_and_revisions",
    },
    DurabilityDomain {
        name: "source_documents",
        write_methods: &["upsert_source_documents"],
        read_methods: &["read_source_document"],
        missing_repository_methods: &[],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::MixedReadThrough,
        evidence: "read-through is mixed: explicit-owner personal documents resolve, while company documents omit owner_user_id and miss the IS NULL filter",
    },
    DurabilityDomain {
        name: "parse_artifacts",
        write_methods: &["upsert_parse_artifacts"],
        read_methods: &[],
        missing_repository_methods: &[],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "the canonical Store map has no restart/read-through path, while artifacts also survive inside startup-hydrated ingest results",
    },
    DurabilityDomain {
        name: "parsed_blocks",
        write_methods: &[],
        read_methods: &[],
        missing_repository_methods: &["upsert_parsed_blocks", "list_parsed_blocks"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "the canonical Store map has no repository or restart/read-through path, while blocks survive inside startup-hydrated ingest results",
    },
    DurabilityDomain {
        name: "dataset_schemas",
        write_methods: &[],
        read_methods: &[],
        missing_repository_methods: &["upsert_dataset", "get_dataset"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "repository surface has no dataset schema persistence",
    },
    DurabilityDomain {
        name: "structured_snapshots",
        write_methods: &["upsert_structured_snapshot"],
        read_methods: &["get_snapshot"],
        missing_repository_methods: &[],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::ReadThrough,
        evidence: "get_snapshot_async falls back to the repository",
    },
    DurabilityDomain {
        name: "structured_rows",
        write_methods: &["upsert_structured_rows"],
        read_methods: &["list_rows"],
        missing_repository_methods: &[],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::ReadThrough,
        evidence: "list_rows_async falls back to the repository",
    },
    DurabilityDomain {
        name: "structured_summaries",
        write_methods: &["upsert_structured_summary"],
        read_methods: &[],
        missing_repository_methods: &["get_structured_summary"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "repository surface has no structured-summary read path",
    },
    DurabilityDomain {
        name: "sessions",
        write_methods: &[],
        read_methods: &[],
        missing_repository_methods: &["upsert_session", "get_session", "list_sessions"],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::Missing,
        evidence: "meili_restart_characterizes_state_link_and_session_durability_gaps",
    },
    DurabilityDomain {
        name: "traces",
        write_methods: &["upsert_trace"],
        read_methods: &["get_trace"],
        missing_repository_methods: &[],
        startup_methods: &[],
        report_keys: &[],
        restart_behavior: RestartBehavior::ReadThrough,
        evidence: "get_trace_async falls back to the repository",
    },
    DurabilityDomain {
        name: "eval_and_harness",
        write_methods: &[
            "upsert_harness_components",
            "upsert_harness_changes",
            "upsert_harness_verdicts",
            "upsert_eval_case",
            "upsert_eval_run",
            "upsert_eval_case_results",
            "upsert_eval_overview",
        ],
        read_methods: &[],
        missing_repository_methods: &[],
        startup_methods: &[
            "list_harness_components",
            "list_harness_component_revisions",
            "list_harness_changes",
            "list_harness_verdicts",
            "list_eval_cases",
            "list_eval_runs",
            "list_eval_case_results",
            "get_eval_overview",
        ],
        report_keys: &[
            "harness_components",
            "harness_revisions",
            "harness_changes",
            "harness_verdicts",
            "eval_cases",
            "eval_runs",
            "eval_case_results",
            "eval_overviews",
        ],
        restart_behavior: RestartBehavior::StartupHydrated,
        evidence: "meili_hydrates_harness_eval_and_ingest_metadata_into_fresh_app",
    },
    DurabilityDomain {
        name: "ingest_tasks_and_results",
        write_methods: &["upsert_ingest_task", "upsert_ingest_result"],
        read_methods: &[],
        missing_repository_methods: &[],
        startup_methods: &["list_ingest_tasks", "list_ingest_results"],
        report_keys: &["ingest_tasks", "ingest_results"],
        restart_behavior: RestartBehavior::StartupHydrated,
        evidence: "meili_hydrates_harness_eval_and_ingest_metadata_into_fresh_app",
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

fn method_names(body: &str) -> BTreeSet<String> {
    let mut methods = BTreeSet::new();
    for marker in [".list_", ".get_eval_overview"] {
        let mut remainder = body;
        while let Some(index) = remainder.find(marker) {
            let candidate = &remainder[index + 1..];
            let end = candidate
                .find(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                .unwrap_or(candidate.len());
            methods.insert(candidate[..end].to_string());
            remainder = &candidate[end..];
        }
    }
    methods
}

fn report_keys(body: &str) -> BTreeSet<String> {
    let marker = "counts.insert(\"";
    let mut keys = BTreeSet::new();
    let mut remainder = body;
    while let Some(index) = remainder.find(marker) {
        let candidate = &remainder[index + marker.len()..];
        let end = candidate
            .find('"')
            .expect("hydration report key should close its string literal");
        keys.insert(candidate[..end].to_string());
        remainder = &candidate[end + 1..];
    }
    keys
}

#[test]
fn current_restart_durability_contract_covers_every_audited_domain() {
    let repository = include_str!("../src/repository.rs");
    let store = include_str!("../src/store.rs");
    let hydrate = function_body(store, "pub async fn hydrate_from_repository");

    let actual_domains = DURABILITY_MATRIX
        .iter()
        .map(|domain| domain.name)
        .collect::<BTreeSet<_>>();
    let expected_domains = AUDITED_DOMAINS.iter().copied().collect::<BTreeSet<_>>();
    assert_eq!(actual_domains, expected_domains);
    assert_eq!(DURABILITY_MATRIX.len(), AUDITED_DOMAINS.len());

    let mut expected_startup_methods = BTreeSet::new();
    let mut expected_report_keys = BTreeSet::new();
    let mut restart_behaviors = BTreeSet::new();
    for domain in DURABILITY_MATRIX {
        assert!(
            !domain.evidence.is_empty(),
            "{} lacks evidence",
            domain.name
        );
        restart_behaviors.insert(format!("{:?}", domain.restart_behavior));
        for method in domain.write_methods.iter().chain(domain.read_methods) {
            assert!(
                repository.contains(&format!("async fn {method}")),
                "{} expects repository method {method}",
                domain.name
            );
        }
        for method in domain.missing_repository_methods {
            assert!(
                !repository.contains(&format!("async fn {method}")),
                "{} currently characterizes {method} as missing",
                domain.name
            );
        }
        expected_startup_methods.extend(
            domain
                .startup_methods
                .iter()
                .map(|method| (*method).to_string()),
        );
        expected_report_keys.extend(domain.report_keys.iter().map(|key| (*key).to_string()));
    }

    assert_eq!(
        restart_behaviors,
        [
            "Missing",
            "MixedReadThrough",
            "ReadThrough",
            "StartupHydrated",
        ]
        .into_iter()
        .map(str::to_string)
        .collect()
    );
    assert_eq!(method_names(hydrate), expected_startup_methods);
    assert_eq!(report_keys(hydrate), expected_report_keys);
}

#[test]
fn parse_outputs_restart_only_inside_the_ingest_result_projection() {
    let models = include_str!("../src/models.rs");
    let repository = include_str!("../src/repository.rs");
    let store = include_str!("../src/store.rs");
    let store_data = function_body(store, "struct StoreData");
    let hydrate = function_body(store, "pub async fn hydrate_from_repository");
    let run_ingest = function_body(store, "pub async fn run_ingest_task_async");
    let ingest_result = function_body(models, "pub struct IngestTaskResult");

    assert!(store_data.contains("parse_artifacts: HashMap<String, ParseArtifact>"));
    assert!(store_data.contains("parsed_blocks: HashMap<String, Vec<ParsedBlock>>"));
    assert!(repository.contains("async fn list_parse_artifacts"));
    assert!(
        !store.contains(".list_parse_artifacts("),
        "the canonical parse-artifact map currently has no Store/API read-through path"
    );
    assert!(
        !repository.contains("async fn list_parsed_blocks"),
        "the canonical parsed-block map currently has no repository read path"
    );
    assert!(
        !hydrate.contains("data.parse_artifacts") && !hydrate.contains("data.parsed_blocks"),
        "startup hydration must not be mistaken for rebuilding the canonical parse-output maps"
    );

    assert!(ingest_result.contains("pub parse_artifacts: Vec<ParseArtifact>"));
    assert!(ingest_result.contains("pub parsed_blocks: Vec<ParsedBlock>"));
    assert!(run_ingest.contains("parse_artifacts: artifacts"));
    assert!(run_ingest.contains("parsed_blocks: parsed.blocks"));
    assert!(run_ingest.contains(".upsert_ingest_result(&result)"));
    assert!(hydrate.contains(".list_ingest_results(tenant_id)"));
    assert!(hydrate.contains("data.ingest_results"));
}

#[test]
fn source_document_read_through_is_mixed_by_owner_projection() {
    let models = include_str!("../src/models.rs");
    let repository = include_str!("../src/repository.rs");
    let store = include_str!("../src/store.rs");
    let source_document = function_body(models, "pub struct SourceDocument");
    let fs_read = function_body(store, "pub async fn fs_read_async");
    let meili_repository = repository
        .split_once("impl KnowledgeRepository for MeiliRepository")
        .expect("Meili repository implementation should exist")
        .1;
    let upsert_source_documents =
        function_body(meili_repository, "async fn upsert_source_documents");
    let read_source_document = function_body(meili_repository, "async fn read_source_document");

    assert!(source_document.contains(
        "#[serde(default, skip_serializing_if = \"Option::is_none\")]\n    pub owner_user_id"
    ));
    assert!(upsert_source_documents.contains("to_document(document, &document.id)"));
    assert!(fs_read.contains(".read_source_document(tenant_id, owner_user_id, uri)"));
    assert!(read_source_document.contains("owner_user_id = {} OR owner_user_id IS NULL"));
    assert!(read_source_document.contains("owner_user_id IS NULL"));
}
