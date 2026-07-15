use std::{
    convert::Infallible,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
    time::Duration,
};

use axum::{
    body::{to_bytes, Body, Bytes},
    extract::State,
    http::{
        header::{
            ACCEPT, ACCEPT_ENCODING, ACCESS_CONTROL_EXPOSE_HEADERS, AUTHORIZATION,
            CONTENT_ENCODING, CONTENT_TYPE, LINK, RETRY_AFTER,
        },
        HeaderMap, HeaderValue, Method, Request, StatusCode,
    },
    response::Response,
    routing::post,
    Json, Router,
};
use futures_util::{stream, Stream, StreamExt};
use nowledge::{
    build_router,
    config::{AuthUserConfig, AuthUserScope},
    AppState, Config,
};
use serde_json::{json, Value};
use tokio::{
    sync::{oneshot, Notify},
    task::JoinHandle,
    time::timeout,
};
use tower::ServiceExt;

const PROVIDER_MODEL: &str = "stream-contract-model";

#[derive(Debug)]
struct ParsedSseEvent {
    name: String,
    data: Value,
}

#[derive(Default)]
struct SseParser {
    buffer: Vec<u8>,
}

impl SseParser {
    fn push(&mut self, chunk: &[u8]) -> Vec<ParsedSseEvent> {
        self.buffer.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(end) = self.buffer.windows(2).position(|window| window == b"\n\n") {
            let frame = self.buffer.drain(..end + 2).collect::<Vec<_>>();
            if let Some(event) = parse_sse_frame(&frame[..end]) {
                events.push(event);
            }
        }
        events
    }

    fn finish(self) {
        assert!(
            self.buffer.iter().all(u8::is_ascii_whitespace),
            "incomplete downstream SSE frame: {:?}",
            String::from_utf8_lossy(&self.buffer)
        );
    }
}

fn parse_sse_frame(frame: &[u8]) -> Option<ParsedSseEvent> {
    let frame = std::str::from_utf8(frame).expect("downstream SSE must be UTF-8");
    let mut name = None;
    let mut data = Vec::new();
    for line in frame.lines().map(|line| line.trim_end_matches('\r')) {
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => name = Some(value.to_string()),
            "data" => data.push(value),
            _ => {}
        }
    }
    if data.is_empty() {
        return None;
    }
    let data = serde_json::from_str(&data.join("\n")).expect("SSE data must be JSON");
    Some(ParsedSseEvent {
        name: name.expect("every application SSE frame must name its event"),
        data,
    })
}

fn parse_sse(body: &[u8]) -> Vec<ParsedSseEvent> {
    let mut parser = SseParser::default();
    let events = parser.push(body);
    parser.finish();
    events
}

fn assert_success_sequence(events: &[ParsedSseEvent]) {
    assert!(events.len() >= 4, "{events:?}");
    assert_eq!(events[0].name, "meta", "{events:?}");
    assert_eq!(events[events.len() - 2].name, "usage", "{events:?}");
    assert_eq!(events[events.len() - 1].name, "done", "{events:?}");

    let first_delta = events
        .iter()
        .position(|event| event.name == "delta")
        .expect("a successful answer must contain a delta");
    assert!(
        events[1..first_delta]
            .iter()
            .all(|event| event.name == "citation"),
        "{events:?}"
    );
    assert!(
        events[first_delta..events.len() - 2]
            .iter()
            .all(|event| event.name == "delta"),
        "{events:?}"
    );
    assert_eq!(
        events[0].data["answer_id"],
        events[events.len() - 1].data["answer_id"]
    );
    assert_eq!(
        events[0].data["trace_id"],
        events[events.len() - 1].data["trace_id"]
    );
}

fn plain_app() -> Router {
    let mut config = Config::test();
    config.ingest_task_retention_seconds = 0;
    build_router(AppState::new(Arc::new(config)))
}

fn json_request(
    method: Method,
    uri: &str,
    body: Value,
    token: Option<&str>,
    accept_gzip: bool,
) -> Request<Body> {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT, "text/event-stream");
    if let Some(token) = token {
        builder = builder.header(AUTHORIZATION, format!("Bearer {token}"));
    }
    if accept_gzip {
        builder = builder.header(ACCEPT_ENCODING, "gzip");
    }
    builder.body(Body::from(body.to_string())).unwrap()
}

async fn json_call(
    app: Router,
    method: Method,
    uri: &str,
    body: Value,
    token: Option<&str>,
) -> (StatusCode, HeaderMap, Value) {
    let response = app
        .oneshot(json_request(method, uri, body, token, false))
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| json!({ "raw": String::from_utf8_lossy(&bytes) }));
    (status, headers, body)
}

fn rag_body(owner: &str, question: &str) -> Value {
    json!({
        "owner_user_id": owner,
        "question": question
    })
}

fn provider_event(value: Value) -> Vec<u8> {
    format!("data: {value}\n\n").into_bytes()
}

fn provider_delta(text: &str) -> Vec<u8> {
    provider_event(json!({
        "type": "response.output_text.delta",
        "delta": text
    }))
}

fn provider_terminal(text: &str) -> Vec<u8> {
    let done = provider_event(json!({
        "type": "response.output_text.done",
        "text": text
    }));
    let completed = provider_event(json!({
        "type": "response.completed",
        "response": {
            "status": "completed",
            "output": [{
                "content": [{ "type": "output_text", "text": text }]
            }],
            "usage": {
                "input_tokens": 11,
                "output_tokens": 7,
                "total_tokens": 18,
                "input_tokens_details": { "cached_tokens": 2 },
                "output_tokens_details": { "reasoning_tokens": 1 }
            }
        }
    }));
    [done, completed].concat()
}

fn provider_success(deltas: &[&str]) -> Vec<Vec<u8>> {
    let text = deltas.concat();
    let mut chunks = deltas
        .iter()
        .map(|delta| provider_delta(delta))
        .collect::<Vec<_>>();
    chunks.push(provider_terminal(&text));
    chunks
}

struct AuthFile(PathBuf);

impl Drop for AuthFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[derive(Default)]
struct CancellationSignal {
    observed: AtomicBool,
    notify: Notify,
}

impl CancellationSignal {
    fn observe(&self) {
        self.observed.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.observed.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

enum StubMode {
    Static(Vec<Vec<u8>>),
    Gated {
        first: Vec<u8>,
        terminal: Vec<u8>,
        release: Mutex<Option<oneshot::Receiver<()>>>,
        cancellation: Arc<CancellationSignal>,
    },
}

#[derive(Clone)]
struct CodexStubState {
    token: String,
    requests: Arc<AtomicUsize>,
    mode: Arc<StubMode>,
}

struct CodexStub {
    base_url: String,
    requests: Arc<AtomicUsize>,
    server: JoinHandle<()>,
}

impl CodexStub {
    async fn spawn(token: &str, mode: StubMode) -> Self {
        async fn responses(
            State(state): State<CodexStubState>,
            headers: HeaderMap,
            Json(payload): Json<Value>,
        ) -> Response {
            state.requests.fetch_add(1, Ordering::SeqCst);
            let expected = format!("Bearer {}", state.token);
            assert_eq!(
                headers
                    .get(AUTHORIZATION)
                    .and_then(|value| value.to_str().ok()),
                Some(expected.as_str())
            );
            assert_eq!(payload["stream"], true, "{payload}");

            let body = match state.mode.as_ref() {
                StubMode::Static(chunks) => Body::from_stream(stream::iter(
                    chunks
                        .clone()
                        .into_iter()
                        .map(|chunk| Ok::<_, Infallible>(Bytes::from(chunk))),
                )),
                StubMode::Gated {
                    first,
                    terminal,
                    release,
                    cancellation,
                } => {
                    let release = release
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner())
                        .take()
                        .expect("gated Codex stub accepts one request");
                    Body::from_stream(GatedProviderBody {
                        first: Some(Bytes::from(first.clone())),
                        terminal: Some(Bytes::from(terminal.clone())),
                        release,
                        phase: 0,
                        completed: false,
                        cancellation: cancellation.clone(),
                    })
                }
            };
            let mut response = Response::new(body);
            response
                .headers_mut()
                .insert(CONTENT_TYPE, HeaderValue::from_static("text/event-stream"));
            response
        }

        let requests = Arc::new(AtomicUsize::new(0));
        let app = Router::new()
            .route("/responses", post(responses))
            .with_state(CodexStubState {
                token: token.to_string(),
                requests: requests.clone(),
                mode: Arc::new(mode),
            });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            base_url: format!("http://{address}"),
            requests,
            server,
        }
    }

    fn request_count(&self) -> usize {
        self.requests.load(Ordering::SeqCst)
    }
}

impl Drop for CodexStub {
    fn drop(&mut self) {
        self.server.abort();
    }
}

struct GatedProviderBody {
    first: Option<Bytes>,
    terminal: Option<Bytes>,
    release: oneshot::Receiver<()>,
    phase: u8,
    completed: bool,
    cancellation: Arc<CancellationSignal>,
}

impl Stream for GatedProviderBody {
    type Item = Result<Bytes, Infallible>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.phase {
                0 => {
                    self.phase = 1;
                    return Poll::Ready(self.first.take().map(Ok));
                }
                1 => match Pin::new(&mut self.release).poll(context) {
                    Poll::Ready(_) => self.phase = 2,
                    Poll::Pending => return Poll::Pending,
                },
                2 => {
                    self.phase = 3;
                    return Poll::Ready(self.terminal.take().map(Ok));
                }
                _ => {
                    self.completed = true;
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Drop for GatedProviderBody {
    fn drop(&mut self) {
        if !self.completed {
            self.cancellation.observe();
        }
    }
}

fn codex_config(token: &str, base_url: &str) -> (Config, AuthFile) {
    let path = std::env::temp_dir().join(format!(
        "nowledge-rag-stream-auth-{}.json",
        uuid::Uuid::now_v7()
    ));
    std::fs::write(&path, json!({ "access_token": token }).to_string()).unwrap();
    let mut config = Config::test();
    config.llm_provider = "codex_auth".to_string();
    config.llm_model = Some(PROVIDER_MODEL.to_string());
    config.codex_auth_path = Some(path.to_string_lossy().into_owned());
    config.codex_base_url = base_url.to_string();
    config.provider_proxy_mode = "direct".to_string();
    config.provider_connect_timeout_ms = 1_000;
    config.llm_timeout_ms = 5_000;
    config.request_timeout_ms = 5_000;
    config.health_llm_enabled = false;
    config.health_require_llm = false;
    config.ingest_task_retention_seconds = 0;
    config.refresh_configured_secret_values();
    (config, AuthFile(path))
}

#[tokio::test]
async fn default_stream_is_ordered_json_sse_and_never_gzipped() {
    let app = plain_app();
    let (status, _, event) = json_call(
        app.clone(),
        Method::POST,
        "/v1/history/users/u1/events",
        json!({
            "event_type": "note.created",
            "entity_type": "note",
            "entity_id": "stream-contract-source",
            "owner_user_id": "u1",
            "occurred_at": "2026-05-12T00:00:00Z",
            "observed_at": "2026-05-12T00:01:00Z",
            "source_kind": "test",
            "source_ref": { "kind": "test", "id": "stream-contract-source" },
            "text": "streaming-order-marker is available for retrieval",
            "privacy": "private"
        }),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{event}");

    let response = app
        .oneshot(json_request(
            Method::POST,
            "/v1/rag/stream",
            rag_body("u1", "streaming-order-marker"),
            None,
            true,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(response.headers()[CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("text/event-stream"));
    assert!(!response.headers().contains_key(CONTENT_ENCODING));
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let events = parse_sse(&body);
    assert_success_sequence(&events);
    assert!(
        events.iter().any(|event| event.name == "citation"),
        "{events:?}"
    );
    assert!(events.iter().all(|event| event.data.is_object()));
    assert_eq!(events[0].data["provider"], "none");
    assert_eq!(events[0].data["backend"], "memory");
}

#[tokio::test]
async fn json_format_delegates_to_answer_with_deprecation_headers() {
    let app = plain_app();
    let request = rag_body("u1", "compatibility question");
    let (answer_status, _, answer) = json_call(
        app.clone(),
        Method::POST,
        "/v1/rag/answer",
        request.clone(),
        None,
    )
    .await;
    assert_eq!(answer_status, StatusCode::OK, "{answer}");

    let (status, headers, compatibility) = json_call(
        app,
        Method::POST,
        "/v1/rag/stream?format=json",
        request,
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{compatibility}");
    assert_eq!(headers["deprecation"], "@1784073600");
    assert_eq!(headers[LINK], "</v1/rag/answer>; rel=\"successor-version\"");
    let exposed = headers[ACCESS_CONTROL_EXPOSE_HEADERS]
        .to_str()
        .unwrap()
        .to_ascii_lowercase();
    assert!(exposed.split(',').any(|name| name.trim() == "deprecation"));
    assert!(exposed.split(',').any(|name| name.trim() == "link"));
    assert!(headers[CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("application/json"));
    assert_eq!(compatibility["answer"], answer["answer"]);
    assert_eq!(compatibility["citations"], answer["citations"]);
    assert_eq!(compatibility["usage"], answer["usage"]);
    for field in ["answer_id", "trace_id", "answer", "citations", "usage"] {
        assert!(compatibility.get(field).is_some(), "{compatibility}");
    }
}

#[tokio::test]
async fn provider_delta_reaches_client_before_terminal_completion() {
    let token = "codex-stream-first-delta-private-token";
    let first = format!("early-provider-delta-{}", "x".repeat(512));
    let (release_tx, release_rx) = oneshot::channel();
    let cancellation = Arc::new(CancellationSignal::default());
    let stub = CodexStub::spawn(
        token,
        StubMode::Gated {
            first: provider_delta(&first),
            terminal: provider_terminal(&first),
            release: Mutex::new(Some(release_rx)),
            cancellation,
        },
    )
    .await;
    let (config, _auth_file) = codex_config(token, &stub.base_url);
    let app = build_router(AppState::new(Arc::new(config)));
    let response = app
        .oneshot(json_request(
            Method::POST,
            "/v1/rag/stream",
            rag_body("u1", "show an incremental answer"),
            None,
            false,
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let mut body = response.into_body().into_data_stream();
    let mut parser = SseParser::default();
    let mut events = Vec::new();
    while !events
        .iter()
        .any(|event: &ParsedSseEvent| event.name == "delta")
    {
        let chunk = timeout(Duration::from_secs(2), body.next())
            .await
            .expect("first downstream delta was buffered until terminal completion")
            .expect("stream ended before its first delta")
            .expect("downstream body failed");
        events.extend(parser.push(&chunk));
    }
    assert!(!events
        .iter()
        .any(|event| matches!(event.name.as_str(), "usage" | "done" | "error")));
    release_tx.send(()).unwrap();
    while let Some(chunk) = body.next().await {
        events.extend(parser.push(&chunk.unwrap()));
    }
    parser.finish();
    assert_success_sequence(&events);
    assert_eq!(stub.request_count(), 1);
}

#[tokio::test]
async fn malformed_and_truncated_provider_bodies_emit_one_error_without_retry() {
    let visible = format!("visible-partial-answer-{}", "y".repeat(512));
    for (name, broken_chunk) in [
        ("malformed", b"data: {not-json}\n\n".to_vec()),
        (
            "truncated",
            b"data: {\"type\":\"response.output_text.done\"".to_vec(),
        ),
    ] {
        let token = format!("codex-{name}-stream-private-token");
        let stub = CodexStub::spawn(
            &token,
            StubMode::Static(vec![provider_delta(&visible), broken_chunk]),
        )
        .await;
        let (mut config, _auth_file) = codex_config(&token, &stub.base_url);
        config.provider_max_retries = 2;
        let app = build_router(AppState::new(Arc::new(config)));
        let response = app
            .oneshot(json_request(
                Method::POST,
                "/v1/rag/stream",
                rag_body("u1", "exercise provider failure"),
                None,
                false,
            ))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{name}");
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let events = parse_sse(&body);
        let names = events
            .iter()
            .map(|event| event.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(names.iter().filter(|event| **event == "error").count(), 1);
        assert_eq!(names.last(), Some(&"error"), "{name}: {events:?}");
        assert!(!names.contains(&"usage"), "{name}: {events:?}");
        assert!(!names.contains(&"done"), "{name}: {events:?}");
        let error = &events.last().unwrap().data;
        assert_eq!(error["error"]["code"], "upstream_error", "{name}");
        assert_eq!(error["error"]["details"]["status"], 502, "{name}");
        assert_eq!(stub.request_count(), 1, "{name} body failure was retried");
    }
}

#[tokio::test]
async fn configured_secret_split_across_provider_deltas_cannot_be_reconstructed() {
    let token = "codex-split-stream-secret-private-token";
    let split_a = &token[..13];
    let split_b = &token[13..27];
    let split_c = &token[27..];
    let chunks = provider_success(&["safe prefix ", split_a, split_b, split_c, " safe suffix"]);
    let stub = CodexStub::spawn(token, StubMode::Static(chunks)).await;
    let (mut config, _auth_file) = codex_config(token, &stub.base_url);
    // Exercise metadata redaction too: `meta.model` and `usage.model` cross
    // the SSE boundary separately from provider text deltas.
    config.llm_model = Some(token.to_string());
    let app = build_router(AppState::new(Arc::new(config)));
    let response = app
        .oneshot(json_request(
            Method::POST,
            "/v1/rag/stream",
            rag_body("u1", "redact the provider answer"),
            None,
            false,
        ))
        .await
        .unwrap();
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let rendered = String::from_utf8(body.to_vec()).unwrap();
    let events = parse_sse(rendered.as_bytes());
    assert_success_sequence(&events);
    let answer = events
        .iter()
        .filter(|event| event.name == "delta")
        .map(|event| event.data["text"].as_str().unwrap())
        .collect::<String>();
    assert!(!rendered.contains(token), "{rendered}");
    assert!(!answer.contains(token), "{answer}");
    for fragment in [split_a, split_b, split_c] {
        assert!(
            !answer.contains(fragment),
            "leaked {fragment:?} in {answer:?}"
        );
    }
    assert!(answer.contains("safe prefix"), "{answer}");
    assert!(answer.contains("safe suffix"), "{answer}");
}

#[tokio::test]
async fn owner_mismatch_is_forbidden_before_the_provider_is_called() {
    let token = "codex-owner-guard-private-token";
    let stub = CodexStub::spawn(token, StubMode::Static(provider_success(&["unused"]))).await;
    let (mut config, _auth_file) = codex_config(token, &stub.base_url);
    config.auth_users = vec![
        AuthUserConfig {
            token: "u1-token".to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u1".to_string(),
            },
            roles: vec!["user".to_string()],
        },
        AuthUserConfig {
            token: "u2-token".to_string(),
            scope: AuthUserScope::Owner {
                owner_user_id: "u2".to_string(),
            },
            roles: vec!["user".to_string()],
        },
    ];
    let app = build_router(AppState::new(Arc::new(config)));
    let (status, headers, body) = json_call(
        app,
        Method::POST,
        "/v1/rag/stream",
        rag_body("u2", "cross-owner request"),
        Some("u1-token"),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    assert!(headers[CONTENT_TYPE]
        .to_str()
        .unwrap()
        .starts_with("application/json"));
    assert_eq!(body["error"]["code"], "forbidden");
    assert_eq!(stub.request_count(), 0);
}

#[tokio::test]
async fn unread_stream_holds_capacity_livez_bypasses_and_drop_releases_upstream() {
    let token = "codex-capacity-stream-private-token";
    let first = provider_delta(&format!("held-stream-{}", "z".repeat(512)));
    let (release_tx, release_rx) = oneshot::channel();
    let cancellation = Arc::new(CancellationSignal::default());
    let stub = CodexStub::spawn(
        token,
        StubMode::Gated {
            first,
            terminal: provider_terminal("unused"),
            release: Mutex::new(Some(release_rx)),
            cancellation: cancellation.clone(),
        },
    )
    .await;
    let (mut config, _auth_file) = codex_config(token, &stub.base_url);
    config.max_in_flight_requests = 1;
    let app = build_router(AppState::new(Arc::new(config)));
    let stream_response = app
        .clone()
        .oneshot(json_request(
            Method::POST,
            "/v1/rag/stream",
            rag_body("u1", "hold global capacity"),
            None,
            false,
        ))
        .await
        .unwrap();
    assert_eq!(stream_response.status(), StatusCode::OK);

    let (status, headers, body) = json_call(
        app.clone(),
        Method::POST,
        "/v1/state/search",
        json!({}),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    assert_eq!(headers[RETRY_AFTER], "1");

    let (status, _, livez) = json_call(app.clone(), Method::GET, "/livez", Value::Null, None).await;
    assert_eq!(status, StatusCode::OK, "{livez}");

    drop(stream_response);
    timeout(Duration::from_secs(2), cancellation.wait())
        .await
        .expect("dropping the client response did not cancel the provider body");
    let (status, _, body) = json_call(app, Method::POST, "/v1/state/search", json!({}), None).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(stub.request_count(), 1);
    drop(release_tx);
}
