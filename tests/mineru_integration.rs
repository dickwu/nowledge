use std::{sync::Arc, time::Duration};

use axum::{
    body::{to_bytes, Body},
    http::{header::CONTENT_TYPE, Method, Request, StatusCode},
    Router,
};
use nowledge::{build_router, AppState, Config};
use serde_json::{json, Value};
use tower::ServiceExt;

fn mineru_pdf_fixture() -> Vec<u8> {
    b"%PDF-1.4
1 0 obj
<< /Type /Catalog /Pages 2 0 R >>
endobj
2 0 obj
<< /Type /Pages /Kids [3 0 R] /Count 1 >>
endobj
3 0 obj
<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>
endobj
4 0 obj
<< /Length 74 >>
stream
BT /F1 16 Tf 36 96 Td (Nowledge MinerU smoke keyword) Tj ET
endstream
endobj
5 0 obj
<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>
endobj
xref
0 6
0000000000 65535 f 
0000000010 00000 n 
0000000059 00000 n 
0000000116 00000 n 
0000000245 00000 n 
0000000369 00000 n 
trailer
<< /Size 6 /Root 1 0 R >>
startxref
439
%%EOF
"
    .to_vec()
}

fn app(mineru_api_url: String) -> Router {
    let mut config = Config::test();
    config.parser_provider = "mineru".to_string();
    config.mineru_api_url = mineru_api_url;
    config.store_backend = "memory".to_string();
    build_router(AppState::new(Arc::new(config)))
}

async fn call_json(app: Router, method: Method, uri: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    read_response(app.oneshot(request).await.unwrap()).await
}

async fn upload_pdf(app: Router) -> (StatusCode, Value) {
    let boundary = format!("nowledge-mineru-{}", uuid::Uuid::now_v7());
    let mut body = Vec::new();
    for (name, value) in [
        ("source_id", "live-mineru-smoke"),
        ("revision_id", "v1"),
        ("title", "Live MinerU Smoke"),
        ("parser_provider", "mineru"),
    ] {
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{name}\"\r\n\r\n").as_bytes(),
        );
        body.extend_from_slice(value.as_bytes());
        body.extend_from_slice(b"\r\n");
    }
    body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
    body.extend_from_slice(
        b"Content-Disposition: form-data; name=\"file\"; filename=\"mineru_smoke.pdf\"\r\nContent-Type: application/pdf\r\n\r\n",
    );
    body.extend_from_slice(&mineru_pdf_fixture());
    body.extend_from_slice(b"\r\n");
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());

    let request = Request::builder()
        .method(Method::POST)
        .uri("/v1/ingest/uploads:sync")
        .header(
            CONTENT_TYPE,
            format!("multipart/form-data; boundary={boundary}"),
        )
        .body(Body::from(body))
        .unwrap();
    read_response(app.oneshot(request).await.unwrap()).await
}

async fn read_response(response: axum::response::Response) -> (StatusCode, Value) {
    let status = response.status();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes).to_string() }))
    };
    (status, value)
}

#[tokio::test]
async fn live_mineru_multipart_upload_parses_fixture() {
    let Ok(mineru_url) = std::env::var("RAG_TEST_MINERU_API_URL") else {
        eprintln!("skipping MinerU integration test; set RAG_TEST_MINERU_API_URL");
        return;
    };
    let health = tokio::time::timeout(
        Duration::from_secs(2),
        reqwest::Client::new()
            .get(format!("{}/health", mineru_url.trim_end_matches('/')))
            .send(),
    )
    .await;
    let Ok(Ok(response)) = health else {
        eprintln!("skipping MinerU integration test; MinerU /health is unreachable");
        return;
    };
    if !response.status().is_success() {
        eprintln!(
            "skipping MinerU integration test; MinerU /health returned {}",
            response.status()
        );
        return;
    }

    let app = app(mineru_url);
    let (status, result) = upload_pdf(app.clone()).await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["task"]["state"], "completed");
    assert!(result["parse_artifacts"]
        .as_array()
        .unwrap()
        .iter()
        .any(|artifact| artifact["artifact_kind"] == "markdown"
            || artifact["artifact_kind"] == "content_list"
            || artifact["artifact_kind"] == "content_list_v2"));
    assert!(
        !result["parsed_blocks"].as_array().unwrap().is_empty(),
        "{result}"
    );
    assert!(
        !result["fragment_uris"].as_array().unwrap().is_empty(),
        "{result}"
    );

    let (status, search) = call_json(
        app.clone(),
        Method::POST,
        "/v1/context/search",
        json!({ "query": "Nowledge MinerU smoke keyword", "limit": 5 }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{search}");
    assert!(!search["hits"].as_array().unwrap().is_empty(), "{search}");
    let fragment_uri = search["hits"][0]["uri"].as_str().unwrap();

    let (status, traceback) = call_json(
        app,
        Method::POST,
        "/v1/context/traceback",
        json!({ "uri": fragment_uri }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{traceback}");
    assert_eq!(
        traceback["source_document_uri"],
        result["source_document_uri"]
    );
}
