use std::convert::Infallible;

use axum::{
    extract::{Extension, Query, State},
    http::header::{HeaderValue, LINK},
    response::{sse::Event, IntoResponse, Response, Sse},
    Json,
};
use futures_util::stream;
use serde::Deserialize;
use serde_json::Value;

use crate::{
    app::AppState,
    auth::{AdminGuard, UserGuard},
    error::ApiError,
    models::RagAnswerRequest,
    rag_service::RagService,
    rag_stream_service::RagStreamService,
    request_context::RequestId,
};

const RAG_STREAM_JSON_DEPRECATION_DATE: &str = "@1784073600"; // 2026-07-15T00:00:00Z

#[derive(Debug, Default, Deserialize)]
pub(crate) struct RagStreamQuery {
    format: Option<String>,
}

pub(crate) async fn rag_answer(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut req.owner_user_id)?;
    let budget_key = user
        .principal
        .provider_budget_key(&state.config.index_hash_secret);
    Ok(Json(
        RagService::answer(&state, req, user.principal.is_admin(), &budget_key).await?,
    ))
}

pub(crate) async fn rag_stream(
    user: UserGuard,
    State(state): State<AppState>,
    Extension(request_id): Extension<RequestId>,
    Query(query): Query<RagStreamQuery>,
    Json(mut req): Json<RagAnswerRequest>,
) -> Result<Response, ApiError> {
    match query.format.as_deref().unwrap_or("sse") {
        "json" => {
            user.apply_owner_default(&mut req.owner_user_id)?;
            let budget_key = user
                .principal
                .provider_budget_key(&state.config.index_hash_secret);
            let mut response = Json(
                RagService::answer(&state, req, user.principal.is_admin(), &budget_key).await?,
            )
            .into_response();
            response.headers_mut().insert(
                "deprecation",
                HeaderValue::from_static(RAG_STREAM_JSON_DEPRECATION_DATE),
            );
            response.headers_mut().insert(
                LINK,
                HeaderValue::from_static("</v1/rag/answer>; rel=\"successor-version\""),
            );
            return Ok(response);
        }
        "sse" => {}
        _ => {
            return Err(ApiError::validation("format", "must be one of: sse, json"));
        }
    }

    user.apply_owner_default(&mut req.owner_user_id)?;
    let budget_key = user
        .principal
        .provider_budget_key(&state.config.index_hash_secret);
    let session = RagStreamService::open(
        &state,
        req,
        user.principal.is_admin(),
        &budget_key,
        request_id.as_str(),
    )
    .await?;
    let body = stream::unfold(session, |mut session| async move {
        session.next_event().await.map(|event| {
            let (name, data) = event.into_parts();
            (
                Ok::<_, Infallible>(Event::default().event(name).data(data.to_string())),
                session,
            )
        })
    });
    Ok(Sse::new(body).into_response())
}

pub(crate) async fn rag_debug(
    admin: AdminGuard,
    State(state): State<AppState>,
    Json(mut req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    admin
        .principal
        .apply_owner_default(&mut req.owner_user_id)?;
    let budget_key = admin
        .principal
        .provider_budget_key(&state.config.index_hash_secret);
    Ok(Json(RagService::debug(&state, req, &budget_key).await?))
}

pub(crate) async fn prompt_preview(
    admin: AdminGuard,
    State(state): State<AppState>,
    Json(req): Json<RagAnswerRequest>,
) -> Result<Json<Value>, ApiError> {
    let budget_key = admin
        .principal
        .provider_budget_key(&state.config.index_hash_secret);
    Ok(Json(
        RagService::prompt_preview(&state, req, &budget_key).await?,
    ))
}
