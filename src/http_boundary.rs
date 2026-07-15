use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use axum::{
    body::{to_bytes, Body, Bytes, HttpBody},
    extract::{Request, State},
    http::{
        header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE, LINK, RETRY_AFTER},
        HeaderName, HeaderValue, Method,
    },
    middleware::Next,
    response::{IntoResponse, Response},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant as TokioInstant;
use tower_http::cors::{Any, CorsLayer};

use crate::{
    config::{validate_cors_allowed_origins, Config},
    error::ApiError,
};

const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(60);
const PRESSURE_RETRY_AFTER_SECONDS: u64 = 1;
const LIVE_PATH: &str = "/livez";
const READY_PATH: &str = "/readyz";
const PUBLIC_READY_RATE_KEY: &str = "public-readiness";
const SYNC_INGEST_PATHS: [&str; 2] = ["/v1/ingest/uploads:sync", "/v1/ingest/files:sync"];
const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");
const DEPRECATION: HeaderName = HeaderName::from_static("deprecation");

#[derive(Clone, Copy, Debug)]
pub struct RequestDeadline(TokioInstant);

impl RequestDeadline {
    pub fn new(deadline: TokioInstant) -> Self {
        Self(deadline)
    }

    pub fn instant(self) -> TokioInstant {
        self.0
    }
}

#[derive(Clone)]
pub struct HttpBoundaryState {
    max_json_bytes: usize,
    request_timeout: Duration,
    sync_ingest_timeout: Duration,
    in_flight: Arc<Semaphore>,
    sync_ingest_in_flight: Arc<Semaphore>,
    rate_limiter: FixedWindowRateLimiter,
}

impl HttpBoundaryState {
    pub fn new(config: &Config) -> Self {
        Self {
            max_json_bytes: config.max_json_bytes,
            request_timeout: Duration::from_millis(config.request_timeout_ms),
            sync_ingest_timeout: Duration::from_millis(config.sync_ingest_timeout_ms),
            in_flight: Arc::new(Semaphore::new(config.max_in_flight_requests)),
            sync_ingest_in_flight: Arc::new(Semaphore::new(config.ingest_max_concurrent_tasks)),
            rate_limiter: FixedWindowRateLimiter::new(config.rate_limit_requests_per_minute),
        }
    }

    pub fn max_json_bytes(&self) -> usize {
        self.max_json_bytes
    }

    pub fn timeout_for_path(&self, path: &str) -> Duration {
        if SYNC_INGEST_PATHS.contains(&path) {
            self.sync_ingest_timeout
        } else {
            self.request_timeout
        }
    }

    pub fn try_acquire(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        self.in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::service_unavailable(PRESSURE_RETRY_AFTER_SECONDS))
    }

    pub fn try_acquire_sync_ingest(&self) -> Result<OwnedSemaphorePermit, ApiError> {
        self.sync_ingest_in_flight
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::service_unavailable(PRESSURE_RETRY_AFTER_SECONDS))
    }

    /// Applies the shared fixed-window rate limit to an already-authenticated,
    /// caller-supplied logical principal key. This function does not parse,
    /// inspect, or log credentials; callers must never pass a raw token.
    pub fn check_rate_limit(&self, trusted_principal_key: &str) -> Result<(), ApiError> {
        self.rate_limiter.check(trusted_principal_key)
    }

    pub fn rate_limiter(&self) -> &FixedWindowRateLimiter {
        &self.rate_limiter
    }
}

#[derive(Clone)]
pub struct FixedWindowRateLimiter {
    max_requests: u64,
    window: Duration,
    state: Arc<Mutex<RateLimiterState>>,
}

struct RateLimiterState {
    windows: HashMap<String, RateWindow>,
    last_cleanup: Instant,
}

struct RateWindow {
    started_at: Instant,
    requests: u64,
}

impl FixedWindowRateLimiter {
    pub fn new(requests_per_minute: u64) -> Self {
        Self::with_window(requests_per_minute, RATE_LIMIT_WINDOW)
    }

    fn with_window(max_requests: u64, window: Duration) -> Self {
        Self {
            max_requests,
            window,
            state: Arc::new(Mutex::new(RateLimiterState {
                windows: HashMap::new(),
                last_cleanup: Instant::now(),
            })),
        }
    }

    /// The key must be a stable logical identity derived after authentication,
    /// such as tenant plus owner ID. Keys are never included in errors or logs.
    pub fn check(&self, trusted_principal_key: &str) -> Result<(), ApiError> {
        self.check_at(trusted_principal_key, Instant::now())
    }

    fn check_at(&self, trusted_principal_key: &str, now: Instant) -> Result<(), ApiError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ApiError::Internal("rate limiter lock poisoned".to_string()))?;

        if now
            .checked_duration_since(state.last_cleanup)
            .is_some_and(|elapsed| elapsed >= self.window)
        {
            state.windows.retain(|_, entry| {
                now.checked_duration_since(entry.started_at)
                    .is_some_and(|elapsed| elapsed < self.window)
            });
            state.last_cleanup = now;
        }

        let entry = state
            .windows
            .entry(trusted_principal_key.to_string())
            .or_insert(RateWindow {
                started_at: now,
                requests: 0,
            });
        let elapsed = now
            .checked_duration_since(entry.started_at)
            .unwrap_or_default();
        if elapsed >= self.window {
            entry.started_at = now;
            entry.requests = 0;
        }

        if entry.requests >= self.max_requests {
            let remaining = self
                .window
                .saturating_sub(now.saturating_duration_since(entry.started_at));
            let retry_after = remaining
                .as_secs()
                .saturating_add(u64::from(remaining.subsec_nanos() > 0))
                .max(1);
            return Err(ApiError::too_many_requests(retry_after));
        }

        entry.requests = entry.requests.saturating_add(1);
        Ok(())
    }
}

pub fn bypasses_global_limits(path: &str) -> bool {
    path == LIVE_PATH
}

pub fn store_owns_timeout(path: &str) -> bool {
    SYNC_INGEST_PATHS.contains(&path)
}

pub async fn load_shed(
    State(state): State<HttpBoundaryState>,
    request: Request,
    next: Next,
) -> Response {
    if bypasses_global_limits(request.uri().path()) {
        return next.run(request).await;
    }

    if request.uri().path() == READY_PATH {
        if let Err(error) = state.check_rate_limit(PUBLIC_READY_RATE_KEY) {
            return error.into_response();
        }
    }

    let permit = match state.try_acquire() {
        Ok(permit) => permit,
        Err(error) => return error.into_response(),
    };
    let sync_ingest_permit = if store_owns_timeout(request.uri().path()) {
        match state.try_acquire_sync_ingest() {
            Ok(permit) => Some(permit),
            Err(error) => return error.into_response(),
        }
    } else {
        None
    };
    let response = next.run(request).await;
    hold_capacity_until_body_completion(response, permit, sync_ingest_permit)
}

struct CapacityPermits {
    _global: OwnedSemaphorePermit,
    _sync_ingest: Option<OwnedSemaphorePermit>,
}

struct CapacityBody {
    inner: Body,
    permits: Option<CapacityPermits>,
}

impl HttpBody for CapacityBody {
    type Data = Bytes;
    type Error = axum::Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let poll = Pin::new(&mut self.inner).poll_frame(context);
        if matches!(&poll, Poll::Ready(None) | Poll::Ready(Some(Err(_)))) {
            self.permits.take();
        }
        poll
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

fn hold_capacity_until_body_completion(
    response: Response,
    global: OwnedSemaphorePermit,
    sync_ingest: Option<OwnedSemaphorePermit>,
) -> Response {
    let (parts, body) = response.into_parts();
    Response::from_parts(
        parts,
        Body::new(CapacityBody {
            inner: body,
            permits: Some(CapacityPermits {
                _global: global,
                _sync_ingest: sync_ingest,
            }),
        }),
    )
}

pub async fn enforce_timeout(
    State(state): State<HttpBoundaryState>,
    mut request: Request,
    next: Next,
) -> Response {
    if bypasses_global_limits(request.uri().path()) || store_owns_timeout(request.uri().path()) {
        return next.run(request).await;
    }

    let timeout = state.timeout_for_path(request.uri().path());
    let deadline = TokioInstant::now() + timeout;
    request
        .extensions_mut()
        .insert(RequestDeadline::new(deadline));
    match tokio::time::timeout_at(deadline, next.run(request)).await {
        Ok(response) => response,
        Err(_) => ApiError::timeout().into_response(),
    }
}

pub async fn enforce_non_multipart_body(
    State(state): State<HttpBoundaryState>,
    request: Request,
    next: Next,
) -> Response {
    if bypasses_global_limits(request.uri().path()) {
        return next.run(request).await;
    }
    if is_multipart(&request) {
        return next.run(request).await;
    }

    let (parts, body) = request.into_parts();
    match to_bytes(body, state.max_json_bytes()).await {
        Ok(bytes) => {
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
        Err(_) => ApiError::payload_too_large().into_response(),
    }
}

fn is_multipart(request: &Request) -> bool {
    request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| {
            media_type
                .trim()
                .eq_ignore_ascii_case("multipart/form-data")
        })
}

pub fn build_cors_layer(config: &Config) -> anyhow::Result<CorsLayer> {
    validate_cors_allowed_origins(
        &config.run_mode,
        &config.cors_allowed_origins,
        config.allow_wildcard_cors,
    )?;

    let layer = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::PATCH,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers([ACCEPT, AUTHORIZATION, CONTENT_TYPE, X_REQUEST_ID])
        .expose_headers([X_REQUEST_ID, RETRY_AFTER, DEPRECATION, LINK]);

    if config.cors_allowed_origins == ["*"] {
        Ok(layer.allow_origin(Any))
    } else {
        let origins = config
            .cors_allowed_origins
            .iter()
            .map(|origin| {
                origin.parse::<HeaderValue>().map_err(|_| {
                    anyhow::anyhow!("RAG_CORS_ALLOWED_ORIGINS contains an invalid origin")
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;
        Ok(layer.allow_origin(origins))
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use axum::{
        body::{to_bytes, Body},
        extract::Request,
        http::{header::RETRY_AFTER, StatusCode},
        response::IntoResponse,
    };
    use serde_json::Value;

    use super::*;

    #[tokio::test]
    async fn rate_limiter_rejects_with_retry_after_and_resets_next_window() {
        let limiter = FixedWindowRateLimiter::with_window(2, Duration::from_secs(10));
        let start = Instant::now();

        assert!(limiter.check_at("tenant:owner", start).is_ok());
        assert!(limiter
            .check_at("tenant:owner", start + Duration::from_secs(1))
            .is_ok());
        let response = limiter
            .check_at("tenant:owner", start + Duration::from_secs(2))
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "8");

        assert!(limiter
            .check_at("tenant:owner", start + Duration::from_secs(10))
            .is_ok());
    }

    #[test]
    fn rate_limits_are_isolated_by_trusted_logical_key() {
        let limiter = FixedWindowRateLimiter::new(1);

        assert!(limiter.check("tenant:owner-a").is_ok());
        assert!(limiter.check("tenant:owner-a").is_err());
        assert!(limiter.check("tenant:owner-b").is_ok());
    }

    #[test]
    fn capacity_is_load_shed_without_waiting() {
        let mut config = Config::test();
        config.max_in_flight_requests = 1;
        let state = HttpBoundaryState::new(&config);

        let permit = state.try_acquire().unwrap();
        let response = state.try_acquire().unwrap_err().into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "1");
        drop(permit);
        assert!(state.try_acquire().is_ok());
    }

    #[tokio::test]
    async fn capacity_is_held_until_response_body_eof_or_drop() {
        let mut config = Config::test();
        config.max_in_flight_requests = 1;
        config.ingest_max_concurrent_tasks = 1;
        let state = HttpBoundaryState::new(&config);

        let response = hold_capacity_until_body_completion(
            (StatusCode::OK, "chunk").into_response(),
            state.try_acquire().unwrap(),
            Some(state.try_acquire_sync_ingest().unwrap()),
        );
        let mut body = response.into_body();
        assert!(state.try_acquire().is_err());
        assert!(state.try_acquire_sync_ingest().is_err());

        let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
            .await
            .expect("body data frame")
            .expect("body frame succeeds");
        assert_eq!(frame.into_data().unwrap(), Bytes::from_static(b"chunk"));
        assert!(state.try_acquire().is_err());

        let end = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context)).await;
        assert!(end.is_none());
        assert!(state.try_acquire().is_ok());
        assert!(state.try_acquire_sync_ingest().is_ok());

        let response = hold_capacity_until_body_completion(
            StatusCode::NO_CONTENT.into_response(),
            state.try_acquire().unwrap(),
            None,
        );
        assert!(state.try_acquire().is_err());
        drop(response);
        assert!(state.try_acquire().is_ok());
    }

    #[tokio::test]
    async fn capacity_is_released_when_the_response_body_errors() {
        let mut config = Config::test();
        config.max_in_flight_requests = 1;
        let state = HttpBoundaryState::new(&config);
        let error_stream = futures_util::stream::once(async {
            Err::<Bytes, std::io::Error>(std::io::Error::other("body failed"))
        });
        let response = hold_capacity_until_body_completion(
            Response::new(Body::from_stream(error_stream)),
            state.try_acquire().unwrap(),
            None,
        );
        let mut body = response.into_body();

        let frame = std::future::poll_fn(|context| Pin::new(&mut body).poll_frame(context))
            .await
            .expect("body error frame");
        assert!(frame.is_err());
        assert!(state.try_acquire().is_ok());
    }

    #[test]
    fn sync_ingest_has_a_separate_memory_pressure_lane() {
        let mut config = Config::test();
        config.max_in_flight_requests = 8;
        config.ingest_max_concurrent_tasks = 1;
        let state = HttpBoundaryState::new(&config);

        let permit = state.try_acquire_sync_ingest().unwrap();
        let response = state.try_acquire_sync_ingest().unwrap_err().into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers().get(RETRY_AFTER).unwrap(), "1");
        drop(permit);
        assert!(state.try_acquire_sync_ingest().is_ok());
    }

    #[test]
    fn sync_ingest_has_its_own_timeout_and_liveness_bypasses_limits() {
        let mut config = Config::test();
        config.request_timeout_ms = 30;
        config.sync_ingest_timeout_ms = 120;
        let state = HttpBoundaryState::new(&config);

        assert_eq!(
            state.timeout_for_path("/v1/ingest/uploads:sync"),
            Duration::from_millis(120)
        );
        assert_eq!(
            state.timeout_for_path("/v1/ingest/files:sync"),
            Duration::from_millis(120)
        );
        assert_eq!(
            state.timeout_for_path("/v1/context/search"),
            Duration::from_millis(30)
        );
        assert!(bypasses_global_limits("/livez"));
        assert!(!bypasses_global_limits("/readyz"));
        assert!(store_owns_timeout("/v1/ingest/uploads:sync"));
        assert!(store_owns_timeout("/v1/ingest/files:sync"));
        assert!(!store_owns_timeout("/v1/context/search"));
    }

    #[tokio::test]
    async fn oversized_body_errors_use_the_stable_envelope() {
        let mut config = Config::test();
        config.max_json_bytes = 3;
        let state = HttpBoundaryState::new(&config);
        let request = Request::builder()
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("four"))
            .unwrap();
        let (parts, body) = request.into_parts();
        let response = match to_bytes(body, state.max_json_bytes()).await {
            Ok(_) => panic!("oversized body was accepted"),
            Err(_) => ApiError::payload_too_large().into_response(),
        };

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["error"]["code"], "payload_too_large");
        assert_eq!(body["error"]["details"]["status"], 413);
        drop(parts);
    }

    #[test]
    fn cors_builder_revalidates_origins() {
        let mut config = Config::test();
        config.run_mode = "production".to_string();
        config.cors_allowed_origins = vec!["*".to_string()];
        config.allow_wildcard_cors = false;
        assert!(build_cors_layer(&config).is_err());

        config.allow_wildcard_cors = true;
        assert!(build_cors_layer(&config).is_ok());

        config.cors_allowed_origins = vec!["https://app.example.com".to_string()];
        config.allow_wildcard_cors = false;
        assert!(build_cors_layer(&config).is_ok());
    }
}
