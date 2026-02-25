#![forbid(unsafe_code)]

use std::future::pending;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::Parser;
use tracing::info;
use tracing_subscriber::EnvFilter;
use vz_api::{ApiConfig, router};
use vz_runtime_contract::RuntimeCapabilities;

const SUPPORTED_CAPABILITIES: &[&str] = &[
    "fs_quick_checkpoint",
    "vm_full_checkpoint",
    "checkpoint_fork",
    "docker_compat",
    "compose_adapter",
    "build_cache_export",
    "gpu_passthrough",
    "live_resize",
    "shared_vm",
    "stack_networking",
    "container_logs",
];

#[derive(Debug, Parser)]
#[command(
    name = "vz-api",
    version,
    about = "Runtime V2 OpenAPI/SSE adapter server"
)]
struct Cli {
    /// Socket address to bind the HTTP server.
    #[arg(long, default_value = "127.0.0.1:8181")]
    bind: SocketAddr,

    /// SQLite state-store path used by event endpoints.
    #[arg(long, default_value = "stack-state.db")]
    state_store_path: PathBuf,

    /// Poll interval for SSE/WebSocket event adapters in milliseconds.
    #[arg(long, default_value_t = 250)]
    event_poll_ms: u64,

    /// Default page size for `/v1/events/{stack_name}` reads.
    #[arg(long, default_value_t = 100)]
    default_event_page_size: usize,

    /// Start from stack baseline capabilities before applying explicit flags.
    #[arg(long)]
    stack_baseline: bool,

    /// Enable runtime capability flags (repeat or pass comma-separated values).
    #[arg(long = "capability", value_name = "NAME", value_delimiter = ',')]
    capabilities: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();

    let capabilities = parse_capabilities(&cli.capabilities, cli.stack_baseline)?;
    let config = ApiConfig {
        state_store_path: cli.state_store_path.clone(),
        capabilities,
        event_poll_interval: Duration::from_millis(cli.event_poll_ms.max(1)),
        default_event_page_size: cli.default_event_page_size,
    };

    let app = router(config);
    let listener = tokio::net::TcpListener::bind(cli.bind)
        .await
        .with_context(|| format!("failed to bind Runtime V2 API server on {}", cli.bind))?;
    let bound = listener
        .local_addr()
        .context("failed to resolve bound API listener address")?;

    info!(
        address = %bound,
        state_store = %cli.state_store_path.display(),
        "Runtime V2 API adapter listening"
    );

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Runtime V2 API server failed")
}

fn init_tracing() {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .init();
}

fn parse_capabilities(values: &[String], stack_baseline: bool) -> Result<RuntimeCapabilities> {
    let mut capabilities = if stack_baseline {
        RuntimeCapabilities::stack_baseline()
    } else {
        RuntimeCapabilities::default()
    };

    for value in values {
        match value.as_str() {
            "fs_quick_checkpoint" => capabilities.fs_quick_checkpoint = true,
            "vm_full_checkpoint" => capabilities.vm_full_checkpoint = true,
            "checkpoint_fork" => capabilities.checkpoint_fork = true,
            "docker_compat" => capabilities.docker_compat = true,
            "compose_adapter" => capabilities.compose_adapter = true,
            "build_cache_export" => capabilities.build_cache_export = true,
            "gpu_passthrough" => capabilities.gpu_passthrough = true,
            "live_resize" => capabilities.live_resize = true,
            "shared_vm" => capabilities.shared_vm = true,
            "stack_networking" => capabilities.stack_networking = true,
            "container_logs" => capabilities.container_logs = true,
            _ => {
                bail!(
                    "unknown capability `{value}` (supported: {})",
                    SUPPORTED_CAPABILITIES.join(", ")
                );
            }
        }
    }

    Ok(capabilities)
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

    tracing::info!("shutdown signal received");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_capabilities_accepts_known_values() {
        let values = vec![
            "fs_quick_checkpoint".to_string(),
            "checkpoint_fork".to_string(),
            "container_logs".to_string(),
        ];
        let capabilities = parse_capabilities(&values, false).expect("capabilities should parse");
        assert!(capabilities.fs_quick_checkpoint);
        assert!(capabilities.checkpoint_fork);
        assert!(capabilities.container_logs);
        assert!(!capabilities.compose_adapter);
    }

    #[test]
    fn parse_capabilities_rejects_unknown_values() {
        let values = vec!["not_real".to_string()];
        let error = parse_capabilities(&values, false).expect_err("parse should fail");
        assert!(error.to_string().contains("unknown capability"));
    }

    #[test]
    fn parse_capabilities_supports_stack_baseline() {
        let capabilities = parse_capabilities(&[], true).expect("baseline should parse");
        assert!(capabilities.compose_adapter);
        assert!(capabilities.shared_vm);
        assert!(capabilities.stack_networking);
        assert!(capabilities.container_logs);
    }
}
