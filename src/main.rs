use std::{net::SocketAddr, sync::Arc};

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
    // Probe the LLMs once in the background (periodically only when opted
    // in) so `/readyz`, which only reads the probe cache, has a verdict.
    state.start_llm_health_refresh();
    if config.store_backend == "meili" && config.meili_url.is_some() {
        let bootstrap = state.meili.prepare_for_startup().await?;
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
    state
        .serve_until_shutdown(listener, app, shutdown_signal())
        .await
}

async fn shutdown_signal() {
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
}
