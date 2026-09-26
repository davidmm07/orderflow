//! Orderflow server: loads configuration, wires the layers and runs the
//! HTTP server until a shutdown signal arrives.

mod app;
mod config;
mod instruments;
mod telemetry;

use std::process::ExitCode;

use anyhow::Context;

use crate::config::Settings;

#[tokio::main]
async fn main() -> ExitCode {
    // A local `.env` is a developer convenience. Deployments inject
    // variables directly and never ship the file.
    let _ = dotenvy::dotenv();

    let settings = match Settings::from_env() {
        Ok(settings) => settings,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::FAILURE;
        }
    };
    telemetry::init(settings.log_format);

    match run(settings).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = format!("{error:#}"), "server stopped with an error");
            ExitCode::FAILURE
        }
    }
}

async fn run(settings: Settings) -> anyhow::Result<()> {
    let catalog = instruments::load(&settings.instruments_file)?;
    let market_count = catalog.market_count();
    let app = app::build(&settings, catalog)?;

    let listener = tokio::net::TcpListener::bind(settings.bind_addr)
        .await
        .with_context(|| format!("binding {}", settings.bind_addr))?;
    tracing::info!(addr = %listener.local_addr()?, markets = market_count, "orderflow is listening");

    axum::serve(listener, app.router)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("http server failed")?;

    // The router, and with it every market handle, was dropped when `serve`
    // returned. Each engine now drains its queue and stops, which closes the
    // outbox, which lets the dispatcher publish what is left and stop.
    tracing::info!("http server stopped, draining market engines and events");
    let drain = async {
        for engine in app.engines {
            let _ = engine.await;
        }
        let _ = app.dispatcher.await;
    };
    if tokio::time::timeout(settings.shutdown_grace, drain)
        .await
        .is_err()
    {
        tracing::warn!(
            grace = ?settings.shutdown_grace,
            "grace period elapsed before every event was flushed"
        );
    }
    tracing::info!("shutdown complete");
    Ok(())
}

/// Resolves on Ctrl+C or SIGTERM, the signal container orchestrators send.
async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::error!(%error, "cannot listen for Ctrl+C");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(error) => {
                tracing::error!(%error, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received");
}
