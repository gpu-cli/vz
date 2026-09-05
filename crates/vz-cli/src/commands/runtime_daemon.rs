//! Shared runtime-daemon client wiring for CLI commands.

use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use vz_runtimed_client::{DaemonClient, DaemonClientConfig};

/// Environment variable used to select the CLI control-plane transport.
const CONTROL_PLANE_TRANSPORT_ENV: &str = "VZ_CONTROL_PLANE_TRANSPORT";
/// Optional daemon socket override for CLI runtime commands.
const DAEMON_SOCKET_PATH_ENV: &str = "VZ_RUNTIME_DAEMON_SOCKET";
/// Optional runtime data directory override (socket/log/metrics parent).
const RUNTIME_DATA_DIR_ENV: &str = "VZ_RUNTIME_DATA_DIR";
/// Optional runtime state DB path override.
const RUNTIME_STATE_DB_ENV: &str = "VZ_RUNTIME_STATE_DB";
/// Explicit trusted operator catalog; never resolved relative to a project.
const MACHINE_TARGET_CATALOG_ENV: &str = "VZ_MACHINE_TARGET_CATALOG";

fn parse_machine_target_catalog(raw: Option<OsString>) -> anyhow::Result<Option<PathBuf>> {
    raw.map(|value| {
        let path = PathBuf::from(value);
        if !path.is_absolute()
            || path.components().any(|part| {
                matches!(
                    part,
                    std::path::Component::ParentDir | std::path::Component::CurDir
                )
            })
        {
            bail!(
                "{MACHINE_TARGET_CATALOG_ENV} requires an absolute catalog path without traversal"
            );
        }
        Ok(path)
    })
    .transpose()
}

/// CLI control-plane transport for runtime mutations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ControlPlaneTransport {
    /// Direct gRPC calls to `vz-runtimed` over UDS.
    DaemonGrpc,
    /// HTTP calls to a `vz-api` control-plane facade.
    ApiHttp,
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

fn parse_runtime_data_dir_override(raw: Option<OsString>) -> Option<PathBuf> {
    let value = raw?;
    if value.is_empty() {
        return None;
    }
    Some(PathBuf::from(value))
}

fn parse_env_runtime_data_dir_override() -> Option<PathBuf> {
    parse_runtime_data_dir_override(std::env::var_os(RUNTIME_DATA_DIR_ENV))
}

fn parse_state_db_override(raw: Option<OsString>) -> Option<PathBuf> {
    let value = raw?;
    if value.is_empty() {
        return None;
    }
    Some(PathBuf::from(value))
}

fn daemon_client_config_with_overrides(
    state_db: &Path,
    socket_override: Option<PathBuf>,
    runtime_data_dir_override: Option<PathBuf>,
    auto_spawn: bool,
) -> DaemonClientConfig {
    let mut config = DaemonClientConfig {
        auto_spawn,
        state_store_path: Some(state_db.to_path_buf()),
        ..DaemonClientConfig::default()
    };

    if let Some(socket_path) = socket_override {
        config.socket_path = socket_path.clone();
        if let Some(parent) = socket_path.parent()
            && !parent.as_os_str().is_empty()
        {
            config.runtime_data_dir = Some(parent.to_path_buf());
        }
        return config;
    }

    if let Some(runtime_dir) = runtime_data_dir_override {
        config.socket_path = runtime_dir.join("runtimed.sock");
        config.runtime_data_dir = Some(runtime_dir);
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

/// Up deliberately permits managed daemon startup. Read-only status and Stop/
/// Exec keep their existing-daemon-only path; there is no HTTP/runtime fallback.
pub async fn connect_up_daemon_for_state_db(state_db: &Path) -> anyhow::Result<DaemonClient> {
    if parse_env_control_plane_transport()? != ControlPlaneTransport::DaemonGrpc {
        bail!("Developer Environment Up requires daemon-grpc transport; no HTTP fallback");
    }
    let mut config = daemon_client_config_with_overrides(
        state_db,
        parse_env_daemon_socket_override(),
        parse_env_runtime_data_dir_override(),
        true,
    );
    config.machine_target_catalog =
        parse_machine_target_catalog(std::env::var_os(MACHINE_TARGET_CATALOG_ENV))?;
    config.discover_installed_machine_target_catalog = true;
    DaemonClient::connect_with_config(config)
        .await
        .context("connect managed Up daemon")
}

/// Connect to an already-running daemon without creating runtime state.
///
/// Read-only Developer Environment commands use this path so observing status
/// cannot spawn a daemon, create its runtime directory, or wait through the
/// normal cold-start retry budget.
pub(crate) async fn connect_existing_daemon_for_state_db(
    state_db: &Path,
) -> anyhow::Result<DaemonClient> {
    match parse_env_control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {
            let socket_override = parse_env_daemon_socket_override();
            let runtime_data_dir_override = parse_env_runtime_data_dir_override();
            let mut config = daemon_client_config_with_overrides(
                state_db,
                socket_override,
                runtime_data_dir_override,
                false,
            );
            config.startup_timeout = config.connect_timeout;
            DaemonClient::connect_with_config(config)
                .await
                .with_context(|| {
                    format!(
                        "failed to connect to an existing vz-runtimed for state db {}",
                        state_db.display()
                    )
                })
        }
        ControlPlaneTransport::ApiHttp => bail!(
            "api-http transport cannot use direct daemon gRPC connector; route through runtime API HTTP client helpers"
        ),
    }
}

/// Default CLI state DB path in user home.
pub(crate) fn default_state_db_path() -> PathBuf {
    if let Some(explicit) = parse_state_db_override(std::env::var_os(RUNTIME_STATE_DB_ENV)) {
        return explicit;
    }
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
    fn machine_catalog_override_never_treats_invalid_as_absent() {
        assert_eq!(parse_machine_target_catalog(None).expect("absent"), None);
        for value in ["", "relative.json", "/private/../catalog.json"] {
            assert!(parse_machine_target_catalog(Some(value.into())).is_err());
        }
        assert_eq!(
            parse_machine_target_catalog(Some("/private/catalog.json".into())).expect("absolute"),
            Some(PathBuf::from("/private/catalog.json"))
        );
    }

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
    fn parse_daemon_socket_override_ignores_empty() {
        let override_path = parse_daemon_socket_override(Some(OsString::from("")));
        assert!(override_path.is_none());
    }

    #[test]
    fn parse_runtime_data_dir_override_ignores_empty() {
        let override_path = parse_runtime_data_dir_override(Some(OsString::from("")));
        assert!(override_path.is_none());
    }

    #[test]
    fn parse_state_db_override_ignores_empty() {
        let override_path = parse_state_db_override(Some(OsString::from("")));
        assert!(override_path.is_none());
    }

    #[test]
    fn daemon_client_config_defaults_to_state_db_runtime_dir() {
        let state_db = PathBuf::from("/tmp/vz/state/stack-state.db");
        let config = daemon_client_config_with_overrides(&state_db, None, None, true);

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
        let config = daemon_client_config_with_overrides(&state_db, Some(socket_path), None, false);

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
    fn daemon_client_config_uses_runtime_data_dir_override() {
        let state_db = PathBuf::from("/tmp/vz/state/stack-state.db");
        let runtime_dir = PathBuf::from("/tmp/runtime-dir");
        let config = daemon_client_config_with_overrides(&state_db, None, Some(runtime_dir), false);

        assert!(!config.auto_spawn);
        assert_eq!(config.state_store_path, Some(state_db));
        assert_eq!(
            config.socket_path,
            PathBuf::from("/tmp/runtime-dir/runtimed.sock")
        );
        assert_eq!(
            config.runtime_data_dir,
            Some(PathBuf::from("/tmp/runtime-dir"))
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
