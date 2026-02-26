#![forbid(unsafe_code)]

use std::future::pending;
use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;
use vz_runtimed::{RuntimeDaemon, RuntimedConfig};

#[derive(Debug, Parser)]
#[command(
    name = "vz-runtimed",
    version,
    about = "Runtime V2 control-plane daemon"
)]
struct Cli {
    /// Reserved bind address for future daemon IPC transport.
    #[arg(long, default_value = "127.0.0.1:9191")]
    _bind: SocketAddr,

    /// SQLite state-store path for runtime entities/events/receipts.
    #[arg(long, default_value = "stack-state.db")]
    state_store_path: PathBuf,

    /// Runtime backend data directory.
    #[arg(long, default_value = ".vz-runtime")]
    runtime_data_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let daemon = RuntimeDaemon::start(RuntimedConfig {
        state_store_path: cli.state_store_path,
        runtime_data_dir: cli.runtime_data_dir,
    })
    .context("failed to start runtime daemon")?;

    let health = daemon.health();
    info!(
        backend = %health.backend_name,
        started_at = health.started_at_unix_secs,
        "runtime daemon ready"
    );

    shutdown_signal().await;
    info!("runtime daemon shutting down");
    Ok(())
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            tracing::warn!(%error, "failed to install Ctrl+C signal handler");
        }
    };

    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let terminate = async {
            match signal(SignalKind::terminate()) {
                Ok(mut stream) => {
                    stream.recv().await;
                }
                Err(error) => {
                    tracing::warn!(%error, "failed to install SIGTERM signal handler");
                    pending::<()>().await;
                }
            }
        };

        tokio::select! {
            _ = ctrl_c => {}
            _ = terminate => {}
        }
    }

    #[cfg(not(unix))]
    {
        ctrl_c.await;
    }
}
