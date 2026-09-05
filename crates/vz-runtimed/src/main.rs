#![forbid(unsafe_code)]

use std::future::pending;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};
use vz_stack::CheckpointRetentionPolicy;

#[derive(Debug, Parser)]
#[command(
    name = "vz-runtimed",
    version,
    about = "Runtime V2 control-plane daemon"
)]
struct Cli {
    /// SQLite state-store path for runtime entities/events/receipts.
    #[arg(long, default_value = "stack-state.db")]
    state_store_path: PathBuf,

    /// Runtime backend data directory.
    #[arg(long, default_value = ".vz-runtime")]
    runtime_data_dir: PathBuf,

    /// Unix domain socket path for Runtime V2 gRPC.
    #[arg(long, default_value = ".vz-runtime/runtimed.sock")]
    socket_path: PathBuf,

    /// Absolute path to the explicit Machine target artifact catalog.
    #[cfg(target_os = "macos")]
    #[arg(long, value_name = "ABSOLUTE_FILE")]
    machine_target_catalog: Option<PathBuf>,

    /// Offline installer operation: verify selected profile bundles and atomically write the catalog.
    #[cfg(target_os = "macos")]
    #[arg(long, requires_all = ["installed_release_version", "installed_linux_profile"], conflicts_with = "machine_target_catalog")]
    write_installed_machine_target_catalog: Option<PathBuf>,
    #[cfg(target_os = "macos")]
    #[arg(long, requires = "write_installed_machine_target_catalog")]
    installed_release_version: Option<String>,
    #[cfg(target_os = "macos")]
    #[arg(long, requires = "write_installed_machine_target_catalog", value_parser = ["developer", "container"])]
    installed_linux_profile: Vec<String>,

    /// Maximum retained untagged checkpoints in daemon GC loop.
    #[arg(long, default_value_t = 128)]
    checkpoint_retention_max_untagged_count: usize,

    /// Maximum age (seconds) for untagged checkpoints in daemon GC loop.
    #[arg(long, default_value_t = 30 * 24 * 3600)]
    checkpoint_retention_max_age_secs: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    #[cfg(target_os = "macos")]
    if let Some(prefix) = cli.write_installed_machine_target_catalog.as_deref() {
        let version = cli
            .installed_release_version
            .as_deref()
            .context("installed release version required")?;
        let path = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            vz_runtimed::installed_machine_catalog::write_installed_catalog(
                prefix,
                version,
                &cli.installed_linux_profile,
            ),
        )
        .await
        .context("installed catalog verification exceeded its 120-second deadline")??;
        use std::io::Write;
        writeln!(std::io::stdout().lock(), "{}", path.display())?;
        return Ok(());
    }

    #[cfg(target_os = "macos")]
    let machine_target_catalog = match cli.machine_target_catalog.as_deref() {
        Some(path) => vz_runtimed::machine_target_resolver::MachineTargetCatalog::from_file(path)
            .with_context(|| {
            format!(
                "failed to load explicit Machine target catalog {}",
                path.display()
            )
        })?,
        None => vz_runtimed::machine_target_resolver::MachineTargetCatalog::default(),
    };

    // Write logs to a file next to the socket for `vz logs` support.
    let log_file_path = cli.socket_path.with_extension("log");
    init_tracing(Some(&log_file_path));

    let config = RuntimedConfig {
        state_store_path: cli.state_store_path,
        runtime_data_dir: cli.runtime_data_dir,
        socket_path: cli.socket_path,
    };
    let checkpoint_retention_policy = CheckpointRetentionPolicy {
        max_untagged_count: cli.checkpoint_retention_max_untagged_count,
        max_age_secs: cli.checkpoint_retention_max_age_secs,
    };
    #[cfg(target_os = "macos")]
    let daemon = RuntimeDaemon::start_with_machine_target_catalog_and_checkpoint_retention_policy(
        config,
        machine_target_catalog,
        checkpoint_retention_policy,
    );
    #[cfg(not(target_os = "macos"))]
    let daemon =
        RuntimeDaemon::start_with_checkpoint_retention_policy(config, checkpoint_retention_policy);
    let daemon = Arc::new(daemon.context("failed to start runtime daemon")?);

    let health = daemon.health();
    info!(
        daemon_id = %health.daemon_id,
        daemon_version = %health.daemon_version,
        backend = %health.backend_name,
        socket_path = %daemon.socket_path().display(),
        started_at = health.started_at_unix_secs,
        "runtime daemon ready"
    );

    let socket_path = daemon.socket_path().to_path_buf();

    // Diagnostic process identity for the exact supervising owner. A PID file
    // is not authority for a client to replace a version-mismatched daemon.
    let pid_path = socket_path.with_extension("pid");
    std::fs::write(&pid_path, std::process::id().to_string())
        .context("failed to write daemon PID file")?;

    serve_runtime_uds_with_shutdown(daemon, socket_path, shutdown_signal())
        .await
        .context("runtime gRPC server failed")?;

    // Clean up PID file on graceful shutdown.
    let _ = std::fs::remove_file(&pid_path);

    info!("runtime daemon shutting down");
    Ok(())
}

fn init_tracing(log_file: Option<&std::path::Path>) {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    if let Some(path) = log_file {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            tracing_subscriber::fmt()
                .with_env_filter(env_filter)
                .with_target(false)
                .with_ansi(false)
                .compact()
                .with_writer(file)
                .init();
            return;
        }
    }

    // Fallback: write to stderr (for interactive use / debugging).
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
