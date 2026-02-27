//! Shared runtime-daemon client wiring for CLI commands.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use anyhow::{Context, bail};
use clap::ValueEnum;
use vz_runtimed_client::{DaemonClient, DaemonClientConfig};

/// Environment variable used to select the CLI control-plane transport.
const CONTROL_PLANE_TRANSPORT_ENV: &str = "VZ_CONTROL_PLANE_TRANSPORT";
/// Optional daemon socket override for CLI runtime commands.
const DAEMON_SOCKET_PATH_ENV: &str = "VZ_RUNTIME_DAEMON_SOCKET";
/// Optional daemon autostart policy override (`true/false`, `1/0`, etc.).
const DAEMON_AUTOSTART_ENV: &str = "VZ_RUNTIME_DAEMON_AUTOSTART";

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

fn parse_control_plane_transport(raw: Option<OsString>) -> anyhow::Result<ControlPlaneTransport> {
    let Some(raw) = raw else {
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

fn parse_env_control_plane_transport() -> anyhow::Result<ControlPlaneTransport> {
    parse_control_plane_transport(std::env::var_os(CONTROL_PLANE_TRANSPORT_ENV))
}

fn parse_daemon_autostart(raw: Option<OsString>) -> anyhow::Result<bool> {
    let Some(raw) = raw else {
        return Ok(true);
    };

    let value = raw.to_string_lossy().trim().to_ascii_lowercase();
    if value.is_empty() {
        return Ok(true);
    }

    match value.as_str() {
        "1" | "true" | "yes" | "on" => Ok(true),
        "0" | "false" | "no" | "off" => Ok(false),
        other => bail!(
            "unsupported `{}` value `{other}`; expected one of: 1,true,yes,on,0,false,no,off",
            DAEMON_AUTOSTART_ENV
        ),
    }
}

fn parse_env_daemon_autostart() -> anyhow::Result<bool> {
    parse_daemon_autostart(std::env::var_os(DAEMON_AUTOSTART_ENV))
}

fn parse_daemon_socket_override(raw: Option<OsString>) -> Option<PathBuf> {
    let value = raw?;
    if value.is_empty() {
        return None;
    }
    Some(PathBuf::from(value))
}

fn parse_env_daemon_socket_override() -> Option<PathBuf> {
    parse_daemon_socket_override(std::env::var_os(DAEMON_SOCKET_PATH_ENV))
}

fn daemon_client_config_with_overrides(
    state_db: &Path,
    socket_override: Option<PathBuf>,
    auto_spawn: bool,
) -> DaemonClientConfig {
    let mut config = DaemonClientConfig::default();
    config.auto_spawn = auto_spawn;
    config.state_store_path = Some(state_db.to_path_buf());

    if let Some(socket_path) = socket_override {
        config.socket_path = socket_path.clone();
        if let Some(parent) = socket_path.parent()
            && !parent.as_os_str().is_empty()
        {
            config.runtime_data_dir = Some(parent.to_path_buf());
        }
        return config;
    }

    if let Some(parent) = state_db.parent()
        && !parent.as_os_str().is_empty()
    {
        let runtime_dir = parent.join(".vz-runtime");
        config.socket_path = runtime_dir.join("runtimed.sock");
        config.runtime_data_dir = Some(runtime_dir);
    }

    config
}

fn configured_control_plane_transport() -> anyhow::Result<ControlPlaneTransport> {
    if let Some(transport) = CONTROL_PLANE_TRANSPORT_OVERRIDE.get().copied() {
        return Ok(transport);
    }
    parse_env_control_plane_transport()
}

/// Build daemon client config scoped to a specific runtime state DB path.
pub(crate) fn daemon_client_config(state_db: &Path) -> anyhow::Result<DaemonClientConfig> {
    let auto_spawn = parse_env_daemon_autostart()?;
    let socket_override = parse_env_daemon_socket_override();
    Ok(daemon_client_config_with_overrides(
        state_db,
        socket_override,
        auto_spawn,
    ))
}

async fn connect_daemon_grpc_for_state_db(state_db: &Path) -> anyhow::Result<DaemonClient> {
    let config = daemon_client_config(state_db)?;
    DaemonClient::connect_with_config(config)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::process::Command;

    #[test]
    fn parse_control_plane_transport_accepts_aliases() {
        let daemon = parse_control_plane_transport(Some(OsString::from("daemon"))).ok();
        assert_eq!(daemon, Some(ControlPlaneTransport::DaemonGrpc));

        let daemon_grpc = parse_control_plane_transport(Some(OsString::from("daemon-grpc"))).ok();
        assert_eq!(daemon_grpc, Some(ControlPlaneTransport::DaemonGrpc));

        let api = parse_control_plane_transport(Some(OsString::from("api"))).ok();
        assert_eq!(api, Some(ControlPlaneTransport::ApiHttp));

        let api_http = parse_control_plane_transport(Some(OsString::from("api-http"))).ok();
        assert_eq!(api_http, Some(ControlPlaneTransport::ApiHttp));
    }

    #[test]
    fn parse_control_plane_transport_rejects_invalid_value() {
        let result = parse_control_plane_transport(Some(OsString::from("invalid-value")));
        assert!(result.is_err());
    }

    #[test]
    fn parse_daemon_autostart_accepts_bool_and_numeric_values() {
        assert_eq!(parse_daemon_autostart(None).ok(), Some(true));
        assert_eq!(
            parse_daemon_autostart(Some(OsString::from("true"))).ok(),
            Some(true)
        );
        assert_eq!(
            parse_daemon_autostart(Some(OsString::from("1"))).ok(),
            Some(true)
        );
        assert_eq!(
            parse_daemon_autostart(Some(OsString::from("false"))).ok(),
            Some(false)
        );
        assert_eq!(
            parse_daemon_autostart(Some(OsString::from("0"))).ok(),
            Some(false)
        );
    }

    #[test]
    fn parse_daemon_autostart_rejects_invalid_values() {
        let result = parse_daemon_autostart(Some(OsString::from("sometimes")));
        assert!(result.is_err());
    }

    #[test]
    fn parse_daemon_socket_override_ignores_empty() {
        let override_path = parse_daemon_socket_override(Some(OsString::from("")));
        assert!(override_path.is_none());
    }

    #[test]
    fn daemon_client_config_defaults_to_state_db_runtime_dir() {
        let state_db = PathBuf::from("/tmp/vz/state/stack-state.db");
        let config = daemon_client_config_with_overrides(&state_db, None, true);

        assert!(config.auto_spawn);
        assert_eq!(config.state_store_path, Some(state_db.clone()));
        assert_eq!(
            config.socket_path,
            PathBuf::from("/tmp/vz/state/.vz-runtime/runtimed.sock")
        );
        assert_eq!(
            config.runtime_data_dir,
            Some(PathBuf::from("/tmp/vz/state/.vz-runtime"))
        );
    }

    #[test]
    fn daemon_client_config_uses_socket_override() {
        let state_db = PathBuf::from("/tmp/vz/state/stack-state.db");
        let socket_path = PathBuf::from("/tmp/custom-runtime/runtimed.sock");
        let config = daemon_client_config_with_overrides(&state_db, Some(socket_path), false);

        assert!(!config.auto_spawn);
        assert_eq!(config.state_store_path, Some(state_db));
        assert_eq!(
            config.socket_path,
            PathBuf::from("/tmp/custom-runtime/runtimed.sock")
        );
        assert_eq!(
            config.runtime_data_dir,
            Some(PathBuf::from("/tmp/custom-runtime"))
        );
    }

    #[test]
    fn daemon_only_guardrail_script_passes() {
        let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .and_then(|path| path.parent())
            .map(|path| path.to_path_buf())
            .expect("workspace root");
        let script_path = workspace_root.join("scripts/check-daemon-only-guardrails.sh");

        let output = Command::new("bash")
            .arg(&script_path)
            .current_dir(&workspace_root)
            .output()
            .expect("run daemon-only guardrail script");

        assert!(
            output.status.success(),
            "daemon-only guardrail script failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
