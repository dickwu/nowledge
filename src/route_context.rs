use axum::{extract::Query, extract::State, Json};
use serde::Deserialize;
use serde_json::Value;

use crate::{
    app::AppState,
    auth::UserGuard,
    context_service::ContextService,
    error::ApiError,
    models::{
        ContextNode, ContextRevealRequest, ContextRevealResponse, ContextSearchRequest,
        ContextSearchResponse, ContextTracebackRequest, ContextTracebackResponse,
    },
    request_validation::validate_search_limit,
    util::require_string,
};

#[derive(Debug, Deserialize)]
pub(crate) struct FsQuery {
    uri: Option<String>,
    depth: Option<usize>,
    owner_user_id: Option<String>,
}

pub(crate) async fn fs_ls(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    Ok(Json(
        context_service(&state)
            .list(
                query.uri.as_deref(),
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

pub(crate) async fn fs_tree(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<Value>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    Ok(Json(
        context_service(&state)
            .tree(
                query.uri.as_deref(),
                query.depth,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

pub(crate) async fn fs_read(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(
        context_service(&state)
            .read(
                &uri,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

pub(crate) async fn fs_abstract(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(
        context_service(&state)
            .layer(
                &uri,
                0,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

pub(crate) async fn fs_overview(
    user: UserGuard,
    State(state): State<AppState>,
    Query(mut query): Query<FsQuery>,
) -> Result<Json<ContextNode>, ApiError> {
    user.apply_owner_default(&mut query.owner_user_id)?;
    let uri = require_string(query.uri, "uri")?;
    Ok(Json(
        context_service(&state)
            .layer(
                &uri,
                1,
                query.owner_user_id.as_deref(),
                user.principal.is_admin(),
            )
            .await?,
    ))
}

pub(crate) async fn context_search(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<ContextSearchRequest>,
) -> Result<Json<ContextSearchResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    validate_search_limit("limit", request.limit, &state.config)?;
    Ok(Json(
        context_service(&state)
            .search(request, user.principal.is_admin())
            .await?,
    ))
}

pub(crate) async fn context_reveal(
    user: UserGuard,
    State(state): State<AppState>,
    Json(request): Json<ContextRevealRequest>,
) -> Result<Json<ContextRevealResponse>, ApiError> {
    let service = context_service(&state);
    let owner = if let Some(trace_id) = request.trace_id.as_deref() {
        service.trace_owner(trace_id).await?
    } else {
        None
    };
    if let Some(owner) = &owner {
        user.require_owner_access(owner)?;
    }
    let owner_scope = owner.or_else(|| user.principal.owner_user_id().map(ToString::to_string));
    Ok(Json(
        service
            .reveal(request, owner_scope.as_deref(), user.principal.is_admin())
            .await?,
    ))
}

pub(crate) async fn context_traceback(
    user: UserGuard,
    State(state): State<AppState>,
    Json(mut request): Json<ContextTracebackRequest>,
) -> Result<Json<ContextTracebackResponse>, ApiError> {
    user.apply_owner_default(&mut request.owner_user_id)?;
    Ok(Json(
        context_service(&state)
            .traceback(request, user.principal.is_admin())
            .await?,
    ))
}

fn context_service(state: &AppState) -> ContextService {
    ContextService::new(state.tenant_id().to_string(), state.store.clone())
}
