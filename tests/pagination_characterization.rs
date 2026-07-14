use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use axum::{
    body::to_bytes,
    extract::{Request as AxumRequest, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use nowledge::{
    meili::MeiliAdmin,
    repository::{KnowledgeRepository, MeiliRepository, MemoryRepository},
    tenant_scope::tenant_document,
    Config,
};
use serde_json::{json, Value};

#[derive(Clone)]
struct StubResponse {
    status: StatusCode,
    body: Value,
}

#[derive(Clone, Debug, PartialEq)]
struct RecordedRequest {
    method: Method,
    path: String,
    body: Option<Value>,
}

#[derive(Clone, Default)]
struct FetchStub {
    responses: Arc<Mutex<VecDeque<StubResponse>>>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

impl FetchStub {
    fn new(responses: Vec<StubResponse>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

async fn fetch_stub(State(stub): State<FetchStub>, request: AxumRequest) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let body = if method == Method::POST {
        let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        Some(serde_json::from_slice(&bytes).unwrap())
    } else {
        None
    };
    stub.requests
        .lock()
        .unwrap()
        .push(RecordedRequest { method, path, body });
    let Some(response) = stub.responses.lock().unwrap().pop_front() else {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "message": "unexpected extra request" })),
        )
            .into_response();
    };
    (response.status, Json(response.body)).into_response()
}

async fn spawn_stub(responses: Vec<StubResponse>) -> (String, FetchStub) {
    let stub = FetchStub::new(responses);
    let app = Router::new().fallback(fetch_stub).with_state(stub.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), stub)
}

fn ok_page(results: Vec<Value>, offset: usize, limit: usize, total: usize) -> StubResponse {
    StubResponse {
        status: StatusCode::OK,
        body: json!({
            "results": results,
            "offset": offset,
            "limit": limit,
            "total": total,
        }),
    }
}

fn revision(id: &str, title: &str) -> Value {
    json!({
        "id": id,
        "tenant_id": "tenant-a",
        "source_id": "source-with-many-revisions",
        "title": title,
        "source_uri": "ctx://company/sources/many-revisions",
        "checksum": format!("checksum-{id}"),
        "content": format!("content {id}"),
        "status": "historical",
        "created_at": "2026-07-13T00:00:00Z"
    })
}

fn repository(url: &str, page_size: usize, max_documents: usize) -> MeiliRepository {
    let mut config = Config::test();
    config.meili_url = Some(url.to_string());
    MeiliRepository::new_with_scan_limits(
        MeiliAdmin::from_config(&config),
        false,
        page_size,
        max_documents,
    )
}

#[tokio::test]
async fn tenant_scan_fetches_all_2001_rows_without_search_result_truncation() {
    let documents = (0..2001)
        .map(|index| {
            revision(
                &format!("revision-{index:04}"),
                &format!("Revision {index}"),
            )
        })
        .collect::<Vec<_>>();
    let responses = (0..documents.len())
        .step_by(500)
        .map(|offset| {
            ok_page(
                documents[offset..documents.len().min(offset + 500)].to_vec(),
                offset,
                500,
                documents.len(),
            )
        })
        .collect();
    let (url, stub) = spawn_stub(responses).await;

    let revisions = repository(&url, 500, 3_000)
        .list_source_revisions("tenant-a")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(revisions.len(), 2001);
    assert_eq!(revisions.first().unwrap().id, "revision-0000");
    assert_eq!(revisions.last().unwrap().id, "revision-2000");
    let requests = stub.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 5);
    for (request, expected_offset) in requests.iter().zip([0, 500, 1000, 1500, 2000]) {
        assert_eq!(request.method, Method::POST);
        assert_eq!(
            request.path,
            "/indexes/rag_source_revisions/documents/fetch"
        );
        assert_eq!(request.body.as_ref().unwrap()["offset"], expected_offset);
        assert_eq!(request.body.as_ref().unwrap()["limit"], 500);
        assert_eq!(
            request.body.as_ref().unwrap()["filter"],
            "tenant_id = \"tenant-a\""
        );
        assert_eq!(request.body.as_ref().unwrap()["sort"], json!(["id:asc"]));
    }
}

#[tokio::test]
async fn tenant_scan_deduplicates_only_after_all_pages_and_prefers_migrated_copy() {
    let legacy = revision("revision-1", "Legacy");
    let migrated = tenant_document(
        "tenant-a",
        "rag_source_revisions",
        "revision-1",
        &revision("revision-1", "Migrated"),
    )
    .unwrap();
    let (url, _) = spawn_stub(vec![
        ok_page(vec![legacy], 0, 1, 2),
        ok_page(vec![migrated], 1, 1, 2),
    ])
    .await;

    let revisions = repository(&url, 1, 10)
        .list_source_revisions("tenant-a")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(revisions.len(), 1);
    assert_eq!(revisions[0].id, "revision-1");
    assert_eq!(revisions[0].title, "Migrated");
}

#[tokio::test]
async fn tenant_scan_preserves_domain_sort_and_adds_physical_id_tie_breaker() {
    let (url, stub) = spawn_stub(vec![ok_page(Vec::new(), 0, 10, 0)]).await;

    let changes = repository(&url, 10, 100)
        .list_harness_changes("tenant-a")
        .await
        .unwrap()
        .unwrap();

    assert!(changes.is_empty());
    assert_eq!(
        stub.requests.lock().unwrap()[0].body.as_ref().unwrap()["sort"],
        json!(["created_at:desc", "id:asc"])
    );
}

#[tokio::test]
async fn startup_durability_lists_all_use_the_tenant_paginated_fetch_contract() {
    let indexes = [
        "rag_user_event_indexes",
        "rag_state_items",
        "rag_insights",
        "rag_links",
        "rag_structured_datasets",
        "rag_structured_snapshots",
        "rag_structured_summaries",
        "rag_sessions",
        "rag_traces",
    ];
    let (url, stub) = spawn_stub(
        indexes
            .iter()
            .map(|_| ok_page(Vec::new(), 0, 10, 0))
            .collect(),
    )
    .await;
    let repository = repository(&url, 10, 100);

    assert!(repository
        .list_user_event_indexes("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_state_items("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_insights("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_links("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_datasets("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_structured_snapshots("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_structured_summaries("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_sessions("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());
    assert!(repository
        .list_traces("tenant-a")
        .await
        .unwrap()
        .unwrap()
        .is_empty());

    let requests = stub.requests.lock().unwrap();
    assert_eq!(requests.len(), indexes.len());
    for (request, index) in requests.iter().zip(indexes) {
        assert_eq!(request.path, format!("/indexes/{index}/documents/fetch"));
        assert_eq!(
            request.body.as_ref().unwrap()["filter"],
            "tenant_id = \"tenant-a\""
        );
        assert!(request.body.as_ref().unwrap()["sort"]
            .as_array()
            .unwrap()
            .iter()
            .any(|value| value == "id:asc"));
    }
}

#[tokio::test]
async fn memory_repository_marks_startup_durability_lists_not_required() {
    let repository = MemoryRepository;

    assert!(repository
        .list_user_event_indexes("tenant-a")
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_state_items("tenant-a")
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_insights("tenant-a")
        .await
        .unwrap()
        .is_none());
    assert!(repository.list_links("tenant-a").await.unwrap().is_none());
    assert!(repository
        .list_datasets("tenant-a")
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_structured_snapshots("tenant-a")
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_structured_summaries("tenant-a")
        .await
        .unwrap()
        .is_none());
    assert!(repository
        .list_sessions("tenant-a")
        .await
        .unwrap()
        .is_none());
    assert!(repository.list_traces("tenant-a").await.unwrap().is_none());
}

async fn scan_error(responses: Vec<StubResponse>, page_size: usize, max: usize) -> String {
    let (url, _) = spawn_stub(responses).await;
    repository(&url, page_size, max)
        .list_source_revisions("tenant-a")
        .await
        .unwrap_err()
        .to_string()
}

#[tokio::test]
async fn tenant_scan_rejects_changing_totals_wrong_offsets_and_wrong_limits() {
    let changed = scan_error(
        vec![
            ok_page(vec![revision("revision-0", "Zero")], 0, 1, 2),
            ok_page(vec![revision("revision-1", "One")], 1, 1, 3),
        ],
        1,
        10,
    )
    .await;
    assert!(changed.contains("total changed from 2 to 3"), "{changed}");

    let wrong_offset = scan_error(
        vec![ok_page(vec![revision("revision-0", "Zero")], 1, 1, 1)],
        1,
        10,
    )
    .await;
    assert!(
        wrong_offset.contains("returned offset 1 while 0 was requested"),
        "{wrong_offset}"
    );

    let wrong_limit = scan_error(vec![ok_page(Vec::new(), 0, 2, 0)], 1, 10).await;
    assert!(
        wrong_limit.contains("returned limit 2 while 1 was requested"),
        "{wrong_limit}"
    );
}

#[tokio::test]
async fn tenant_scan_rejects_premature_empty_pages_duplicate_ids_and_truncation() {
    let empty = scan_error(vec![ok_page(Vec::new(), 0, 1, 1)], 1, 10).await;
    assert!(empty.contains("empty page before"), "{empty}");

    let duplicate = scan_error(
        vec![
            ok_page(vec![revision("revision-0", "Zero")], 0, 1, 2),
            ok_page(vec![revision("revision-0", "Zero again")], 1, 1, 2),
        ],
        1,
        10,
    )
    .await;
    assert!(
        duplicate.contains("duplicate physical document id"),
        "{duplicate}"
    );

    let truncated = scan_error(
        vec![ok_page(vec![revision("revision-0", "Zero")], 0, 1, 3)],
        1,
        2,
    )
    .await;
    assert!(truncated.contains("refusing to truncate"), "{truncated}");
    assert!(truncated.contains("ceiling 2"), "{truncated}");
}

#[tokio::test]
async fn filtered_scan_reports_missing_index_but_migration_fetch_keeps_legacy_empty_behavior() {
    let missing = StubResponse {
        status: StatusCode::NOT_FOUND,
        body: json!({ "message": "index not found" }),
    };
    let (url, _) = spawn_stub(vec![missing.clone()]).await;
    let error = repository(&url, 10, 100)
        .list_source_revisions("tenant-a")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("required Meilisearch index"), "{error}");
    assert!(error.contains("missing"), "{error}");

    let (url, _) = spawn_stub(vec![missing]).await;
    let mut config = Config::test();
    config.meili_url = Some(url);
    let page = MeiliAdmin::from_config(&config)
        .fetch_documents_page("rag_source_revisions", 0, 100)
        .await
        .unwrap();
    assert!(page.results.is_empty());
    assert_eq!(page.total, 0);
}
