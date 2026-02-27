//! Shared runtime-daemon client wiring for CLI commands.

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, bail};
use clap::ValueEnum;
use vz_runtimed_client::{DaemonClient, DaemonClientConfig};

/// Environment variable used to select the CLI control-plane transport.
const CONTROL_PLANE_TRANSPORT_ENV: &str = "VZ_CONTROL_PLANE_TRANSPORT";

static CONTROL_PLANE_TRANSPORT_OVERRIDE: OnceLock<ControlPlaneTransport> = OnceLock::new();

/// CLI control-plane transport for runtime mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum ControlPlaneTransport {
    /// Direct gRPC calls to `vz-runtimed` over UDS.
    #[value(name = "daemon-grpc", alias = "daemon")]
    DaemonGrpc,
    /// HTTP calls to a `vz-api` control-plane facade.
    #[value(name = "api-http", alias = "api")]
    ApiHttp,
}

impl ControlPlaneTransport {
    fn as_str(self) -> &'static str {
        match self {
            Self::DaemonGrpc => "daemon-grpc",
            Self::ApiHttp => "api-http",
        }
    }
}

/// Set a process-wide transport override from CLI flags.
pub(crate) fn set_control_plane_transport(transport: ControlPlaneTransport) -> anyhow::Result<()> {
    if let Some(existing) = CONTROL_PLANE_TRANSPORT_OVERRIDE.get().copied() {
        if existing != transport {
            bail!(
                "control-plane transport already set to `{}`; cannot override with `{}`",
                existing.as_str(),
                transport.as_str()
            );
        }
        return Ok(());
    }

    let _ = CONTROL_PLANE_TRANSPORT_OVERRIDE.set(transport);
    Ok(())
}

fn parse_env_control_plane_transport() -> anyhow::Result<ControlPlaneTransport> {
    let Some(raw) = std::env::var_os(CONTROL_PLANE_TRANSPORT_ENV) else {
        return Ok(ControlPlaneTransport::DaemonGrpc);
    };

    let value = raw.to_string_lossy().trim().to_ascii_lowercase();
    if value.is_empty() {
        return Ok(ControlPlaneTransport::DaemonGrpc);
    }

    match value.as_str() {
        "daemon" | "daemon-grpc" => Ok(ControlPlaneTransport::DaemonGrpc),
        "api" | "api-http" => Ok(ControlPlaneTransport::ApiHttp),
        other => bail!(
            "unsupported `{}` value `{other}`; expected one of: daemon-grpc, api-http",
            CONTROL_PLANE_TRANSPORT_ENV
        ),
    }
}

fn configured_control_plane_transport() -> anyhow::Result<ControlPlaneTransport> {
    if let Some(transport) = CONTROL_PLANE_TRANSPORT_OVERRIDE.get().copied() {
        return Ok(transport);
    }
    parse_env_control_plane_transport()
}

/// Build daemon client config scoped to a specific runtime state DB path.
pub(crate) fn daemon_client_config(state_db: &Path) -> DaemonClientConfig {
    let mut config = DaemonClientConfig::default();
    config.state_store_path = Some(state_db.to_path_buf());
    if let Some(parent) = state_db.parent()
        && !parent.as_os_str().is_empty()
    {
        let runtime_dir = parent.join(".vz-runtime");
        config.socket_path = runtime_dir.join("runtimed.sock");
        config.runtime_data_dir = Some(runtime_dir);
    }
    config
}

async fn connect_daemon_grpc_for_state_db(state_db: &Path) -> anyhow::Result<DaemonClient> {
    DaemonClient::connect_with_config(daemon_client_config(state_db))
        .await
        .with_context(|| {
            format!(
                "failed to connect to vz-runtimed for state db {}",
                state_db.display()
            )
        })
}

/// Connect to the configured runtime control-plane transport.
pub(crate) async fn connect_control_plane_for_state_db(
    state_db: &Path,
) -> anyhow::Result<DaemonClient> {
    match configured_control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => connect_daemon_grpc_for_state_db(state_db).await,
        ControlPlaneTransport::ApiHttp => {
            bail!("control-plane transport `api-http` is not implemented yet; use `daemon-grpc`")
        }
    }
}

/// Default CLI state DB path in user home.
pub(crate) fn default_state_db_path() -> PathBuf {
    std::env::var_os("HOME")
        .map(|home| PathBuf::from(home).join(".vz").join("stack-state.db"))
        .unwrap_or_else(|| PathBuf::from("stack-state.db"))
}
