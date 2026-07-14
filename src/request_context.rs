use std::sync::{Arc, OnceLock};

use axum::{
    extract::{Request, State},
    http::{header::HeaderName, HeaderValue},
    middleware::Next,
    response::Response,
};

use crate::{config::Config, util::hmac_hex};

pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

#[derive(Debug, Clone)]
pub struct RequestId(String);

impl RequestId {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone)]
pub struct RequestContextState {
    config: Arc<Config>,
}

impl RequestContextState {
    pub fn from_config(config: &Config) -> Self {
        Self {
            config: Arc::new(config.clone()),
        }
    }

    pub fn from_shared_config(config: Arc<Config>) -> Self {
        Self { config }
    }
}

#[derive(Clone)]
struct RequestContext {
    request_id: RequestId,
    config: Arc<Config>,
}

tokio::task_local! {
    static CURRENT_REQUEST: RequestContext;
}

pub async fn assign(
    State(state): State<RequestContextState>,
    mut request: Request,
    next: Next,
) -> Response {
    let request_id = RequestId(uuid::Uuid::now_v7().to_string());
    let header_value = HeaderValue::from_str(request_id.as_str())
        .expect("UUID request IDs are valid HTTP header values");
    request
        .headers_mut()
        .insert(X_REQUEST_ID, header_value.clone());
    request.extensions_mut().insert(request_id.clone());

    let context = RequestContext {
        request_id,
        config: state.config,
    };
    let mut response = CURRENT_REQUEST
        .scope(context, async move { next.run(request).await })
        .await;
    response.headers_mut().insert(X_REQUEST_ID, header_value);
    response
}

pub fn current_id() -> Option<String> {
    CURRENT_REQUEST
        .try_with(|context| context.request_id.0.clone())
        .ok()
}

pub fn current_or_new_id() -> String {
    current_id().unwrap_or_else(|| uuid::Uuid::now_v7().to_string())
}

pub fn fingerprint_current(input: &str) -> String {
    CURRENT_REQUEST
        .try_with(|context| hmac_hex(&context.config.index_hash_secret, "error-cause", input, 16))
        .unwrap_or_else(|_| {
            static FALLBACK_KEY: OnceLock<String> = OnceLock::new();
            let key = FALLBACK_KEY.get_or_init(|| uuid::Uuid::now_v7().to_string());
            hmac_hex(key.as_bytes(), "error-cause", input, 16)
        })
}
