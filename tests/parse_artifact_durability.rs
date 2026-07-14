use std::sync::{Arc, Mutex};

use axum::{
    body::to_bytes,
    extract::{Request as AxumRequest, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use nowledge::{
    meili::MeiliAdmin,
    repository::{KnowledgeRepository, MeiliRepository},
    tenant_scope::{owner_scoped_storage_identity, tenant_document_with_storage_identity},
    Config,
};
use serde_json::{json, Value};

#[derive(Clone, Debug)]
struct RecordedRequest {
    method: Method,
    path: String,
    body: Value,
}

#[derive(Clone)]
struct RetentionStub {
    artifacts: Arc<Vec<Value>>,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
}

async fn retention_stub(State(stub): State<RetentionStub>, request: AxumRequest) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let bytes = to_bytes(request.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    stub.requests.lock().unwrap().push(RecordedRequest {
        method: method.clone(),
        path: path.clone(),
        body: body.clone(),
    });

    if method == Method::POST
        && matches!(
            path.as_str(),
            "/indexes/rag_ingest_tasks/documents/delete"
                | "/indexes/rag_ingest_results/documents/delete"
        )
    {
        return (StatusCode::ACCEPTED, Json(json!({ "taskUid": 7 }))).into_response();
    }

    if method == Method::POST && path == "/indexes/rag_parse_artifacts/documents/fetch" {
        let offset = body["offset"].as_u64().unwrap() as usize;
        let limit = body["limit"].as_u64().unwrap() as usize;
        let results = stub
            .artifacts
            .iter()
            .skip(offset)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        return (
            StatusCode::OK,
            Json(json!({
                "results": results,
                "offset": offset,
                "limit": limit,
                "total": stub.artifacts.len(),
            })),
        )
            .into_response();
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "message": format!("unexpected request: {method} {path}") })),
    )
        .into_response()
}

fn artifact_document(owner_user_id: Option<&str>, id: &str) -> Value {
    let storage_identity = owner_scoped_storage_identity(owner_user_id, id).unwrap();
    tenant_document_with_storage_identity(
        "tenant-a",
        "rag_parse_artifacts",
        id,
        &storage_identity,
        &json!({
            "id": id,
            "tenant_id": "tenant-a",
            "owner_user_id": owner_user_id,
            "source_document_uri": format!("ctx://documents/{id}"),
            "source_id": format!("source-{id}"),
            "revision_id": format!("revision-{id}"),
            "parser_provider": "builtin",
            "parser_backend": "text",
            "parser_version": null,
            "artifact_kind": "markdown",
            "uri": format!("ctx://documents/{id}/artifacts/markdown"),
            "checksum": format!("checksum-{id}"),
            "byte_size": 10,
            "created_at": "2026-07-14T00:00:00Z"
        }),
    )
    .unwrap()
}

fn repository(url: &str) -> MeiliRepository {
    let mut config = Config::test();
    config.meili_url = Some(url.to_string());
    MeiliRepository::new_with_scan_limits(MeiliAdmin::from_config(&config), false, 1, 10)
}

#[tokio::test]
async fn retained_parse_artifacts_remain_loadable_after_ingest_retention_cleanup_and_restart() {
    let artifacts = Arc::new(vec![
        artifact_document(None, "shared-artifact"),
        artifact_document(Some("owner-a"), "shared-artifact"),
        artifact_document(Some("owner-b"), "shared-artifact"),
    ]);
    let requests = Arc::new(Mutex::new(Vec::new()));
    let stub = RetentionStub {
        artifacts,
        requests: requests.clone(),
    };
    let app = Router::new().fallback(retention_stub).with_state(stub);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let url = format!("http://{address}");

    repository(&url)
        .delete_ingest_tasks("tenant-a", &["expired-task".to_string()])
        .await
        .unwrap();

    // A fresh repository instance models the process boundary after cleanup.
    let recovered = repository(&url)
        .list_tenant_parse_artifacts("tenant-a")
        .await
        .unwrap()
        .unwrap();

    assert_eq!(recovered.len(), 3);
    assert!(recovered
        .iter()
        .all(|artifact| artifact.id == "shared-artifact"));
    assert_eq!(recovered[0].owner_user_id, None);
    assert_eq!(recovered[1].owner_user_id.as_deref(), Some("owner-a"));
    assert_eq!(recovered[2].owner_user_id.as_deref(), Some("owner-b"));

    let requests = requests.lock().unwrap();
    assert_eq!(requests.len(), 5);
    assert_eq!(
        requests
            .iter()
            .map(|request| request.path.as_str())
            .collect::<Vec<_>>(),
        vec![
            "/indexes/rag_ingest_tasks/documents/delete",
            "/indexes/rag_ingest_results/documents/delete",
            "/indexes/rag_parse_artifacts/documents/fetch",
            "/indexes/rag_parse_artifacts/documents/fetch",
            "/indexes/rag_parse_artifacts/documents/fetch",
        ]
    );
    assert!(requests
        .iter()
        .all(|request| request.method == Method::POST));
    assert_eq!(
        requests[0].body["filter"],
        "tenant_id = \"tenant-a\" AND task_id IN [\"expired-task\"]"
    );
    for request in &requests[2..] {
        assert_eq!(request.body["filter"], "tenant_id = \"tenant-a\"");
        assert_eq!(request.body["sort"], json!(["created_at:asc", "id:asc"]));
    }
}
