use std::{future::IntoFuture, net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use nowledge::{build_router, AppState, Config};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nowledge=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer().json())
        .init();

    let config = Arc::new(Config::from_env());
    config.validate_startup()?;
    let addr: SocketAddr = format!("{}:{}", config.host, config.port)
        .parse()
        .context("invalid bind address")?;

    let state = AppState::new(config.clone());
    if config.store_backend == "meili" && config.meili_url.is_some() {
        let bootstrap = state.meili.bootstrap(false).await?;
        tracing::info!(
            managed_indexes = bootstrap.indexes.len(),
            completed_tasks = bootstrap.tasks.len(),
            "reconciled Meilisearch settings before hydration"
        );
        let hydrated = state
            .store
            .hydrate_from_repository(&config.tenant_id)
            .await?;
        tracing::info!(%hydrated, "hydrated repository-backed metadata");
    }
    let app = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;

    tracing::info!(%addr, "nowledge service listening");
    let shutdown_grace = Duration::from_millis(config.shutdown_timeout_ms);
    let (shutdown_started, shutdown_observed) = tokio::sync::oneshot::channel();
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(
            state.clone(),
            shutdown_started,
            shutdown_grace,
        ))
        .into_future();
    tokio::pin!(server);
    let (result, shutdown_deadline) = tokio::select! {
        result = &mut server => (result.context("server failed"), None),
        observed = shutdown_observed => {
            let deadline = observed.unwrap_or_else(|_| tokio::time::Instant::now());
            let result = match tokio::time::timeout_at(deadline, &mut server).await {
                Ok(result) => result.context("server failed"),
                Err(_) => {
                    tracing::warn!("HTTP drain exceeded shutdown deadline; forcing close");
                    Ok(())
                }
            };
            (result, Some(deadline))
        }
    };
    if let Some(deadline) = shutdown_deadline {
        state.shutdown_until(deadline).await;
    } else {
        state.shutdown().await;
    }
    result
}

async fn shutdown_signal(
    state: AppState,
    shutdown_started: tokio::sync::oneshot::Sender<tokio::time::Instant>,
    shutdown_grace: Duration,
) {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .expect("failed to install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
    }

    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }

    tracing::info!("shutdown signal received; draining requests and ingest tasks");
    let deadline = tokio::time::Instant::now() + shutdown_grace;
    state.begin_shutdown();
    let _ = shutdown_started.send(deadline);
}
