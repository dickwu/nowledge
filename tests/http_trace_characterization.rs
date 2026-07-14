use std::{
    collections::HashSet,
    io::{self, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use nowledge::{build_router, request_context::X_REQUEST_ID, AppState, Config};
use serde_json::Value;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{sleep, timeout},
};
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

static MULTIPART_TEMP_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Clone, Default)]
struct SharedLog(Arc<Mutex<Vec<u8>>>);

impl SharedLog {
    fn records(&self) -> Vec<Value> {
        let rendered = String::from_utf8(self.0.lock().unwrap().clone()).unwrap();
        rendered
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

struct SharedLogWriter(SharedLog);

impl Write for SharedLogWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 .0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'writer> MakeWriter<'writer> for SharedLog {
    type Writer = SharedLogWriter;

    fn make_writer(&'writer self) -> Self::Writer {
        SharedLogWriter(self.clone())
    }
}

fn staged_upload_paths() -> HashSet<PathBuf> {
    std::fs::read_dir(std::env::temp_dir())
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| {
            entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("nowledge-upload-"))
                .then(|| entry.path())
        })
        .collect()
}

#[tokio::test(flavor = "current_thread")]
async fn http_completion_log_is_info_and_uses_the_response_request_id() {
    let logs = SharedLog::default();
    let subscriber = tracing_subscriber::fmt()
        .json()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(logs.clone())
        .finish();
    let _subscriber_guard = tracing::subscriber::set_default(subscriber);

    let state = AppState::new(Arc::new(Config::test()));
    let response = build_router(state.clone())
        .oneshot(
            Request::builder()
                .uri("/livez")
                .header(X_REQUEST_ID, "untrusted-client-request-id")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response
        .headers()
        .get(X_REQUEST_ID)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    assert_ne!(request_id, "untrusted-client-request-id");
    assert!(uuid::Uuid::parse_str(&request_id).is_ok(), "{request_id}");
    to_bytes(response.into_body(), usize::MAX).await.unwrap();

    let records = logs.records();
    let completion = records
        .iter()
        .find(|record| {
            record["fields"]["message"] == "finished processing request"
                && record["span"]["name"] == "http_request"
        })
        .unwrap_or_else(|| panic!("missing HTTP completion log: {records:#?}"));

    assert_eq!(completion["level"], "INFO", "{completion:#}");
    assert_eq!(
        completion["span"]["request_id"], request_id,
        "{completion:#}"
    );

    state.shutdown().await;
}

#[tokio::test]
async fn disconnecting_mid_multipart_body_removes_the_incomplete_temp_file() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let state = AppState::new(Arc::new(Config::test()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn({
        let app = build_router(state.clone());
        async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        }
    });

    let boundary = "request-body-cancellation-boundary";
    let partial_body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"file\"; filename=\"partial.txt\"\r\n\
         Content-Type: text/plain\r\n\r\n{}",
        "x".repeat(32 * 1_024)
    );
    let mut connection = TcpStream::connect(address).await.unwrap();
    let headers = format!(
        "POST /v1/ingest/uploads:sync HTTP/1.1\r\n\
         Host: {address}\r\n\
         Content-Type: multipart/form-data; boundary={boundary}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        partial_body.len() + 4_096
    );
    connection.write_all(headers.as_bytes()).await.unwrap();
    connection.write_all(partial_body.as_bytes()).await.unwrap();
    connection.flush().await.unwrap();

    let created = timeout(Duration::from_secs(2), async {
        loop {
            let created = staged_upload_paths()
                .difference(&before)
                .cloned()
                .collect::<HashSet<_>>();
            if !created.is_empty() {
                break created;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("server never staged the partial multipart upload");

    drop(connection);

    timeout(Duration::from_secs(2), async {
        while created.iter().any(|path| path.exists()) {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("incomplete staged upload survived request-body cancellation");

    let _ = shutdown_tx.send(());
    server.await.unwrap();
    state.shutdown().await;
}

#[tokio::test]
async fn disallowed_mime_is_rejected_from_headers_without_reading_or_staging_the_file() {
    let _multipart_guard = MULTIPART_TEMP_TEST_LOCK.lock().await;
    let before = staged_upload_paths();
    let state = AppState::new(Arc::new(Config::test()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let server = tokio::spawn({
        let app = build_router(state.clone());
        async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .unwrap();
        }
    });

    let boundary = "disallowed-mime-early-rejection-boundary";
    let partial_body = format!(
        "--{boundary}\r\n\
         Content-Disposition: form-data; name=\"file\"; filename=\"blocked.bin\"\r\n\
         Content-Type: application/x-not-allowed\r\n\r\nx"
    );
    let mut connection = TcpStream::connect(address).await.unwrap();
    let headers = format!(
        "POST /v1/ingest/uploads:sync HTTP/1.1\r\n\
         Host: {address}\r\n\
         Content-Type: multipart/form-data; boundary={boundary}\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\r\n",
        partial_body.len() + 1_000_000
    );
    connection.write_all(headers.as_bytes()).await.unwrap();
    connection.write_all(partial_body.as_bytes()).await.unwrap();
    connection.flush().await.unwrap();

    let mut response = Vec::new();
    timeout(
        Duration::from_secs(2),
        connection.read_to_end(&mut response),
    )
    .await
    .expect("server waited for the disallowed file body")
    .unwrap();
    let response = String::from_utf8(response).unwrap();
    assert!(response.starts_with("HTTP/1.1 400"), "{response}");
    assert!(response.contains("validation_error"), "{response}");
    assert_eq!(staged_upload_paths(), before);

    let _ = shutdown_tx.send(());
    server.await.unwrap();
    state.shutdown().await;
}
