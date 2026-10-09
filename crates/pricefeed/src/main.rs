//! Price feed binary: loads configuration, wires the role and serves until
//! a shutdown signal arrives.

use std::process::ExitCode;

use anyhow::Context;
use orderflow_pricefeed::{app, config::Settings, feeds, telemetry};

#[tokio::main]
async fn main() -> ExitCode {
    // A local `.env` is a developer convenience; deployments inject
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
            tracing::error!(
                error = format!("{error:#}"),
                "pricefeed stopped with an error"
            );
            ExitCode::FAILURE
        }
    }
}

async fn run(settings: Settings) -> anyhow::Result<()> {
    let feeds = feeds::load(&settings.feeds_file)?;
    let markets = feeds.markets.len();
    let app = app::build(&settings, feeds).await?;

    let listener = tokio::net::TcpListener::bind(settings.bind_addr)
        .await
        .with_context(|| format!("binding {}", settings.bind_addr))?;
    tracing::info!(
        addr = %listener.local_addr()?,
        role = settings.role.as_str(),
        markets,
        "pricefeed is listening"
    );

    let shutdown = app.shutdown.clone();
    axum::serve(listener, app.router)
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            // Stops ingestion and tells every WebSocket client to reconnect
            // elsewhere (close code 1001) while HTTP drains.
            shutdown.cancel();
        })
        .await
        .context("http server failed")?;

    tracing::info!("http server stopped, closing sessions and flushing history");
    app.sessions.close();
    let drain = async {
        app.sessions.wait().await;
        for task in app.tasks {
            let _ = task.await;
        }
    };
    if tokio::time::timeout(settings.shutdown_grace, drain)
        .await
        .is_err()
    {
        tracing::warn!(grace = ?settings.shutdown_grace, "grace period elapsed before shutdown finished");
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
