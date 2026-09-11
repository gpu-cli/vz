//! Typed gRPC observer for the Runtime V2 daemon.
//!
//! Criterion 15 requires the public CLI and the typed API to agree on
//! identities, state transitions, topology, capabilities and failures. Proving
//! that needs a second, independent speaker of the daemon's own channel: a
//! comparison the CLI performs against its own output proves only that the CLI
//! is self-consistent. This binary is that speaker. It shares no projection
//! code with `vz` -- it decodes the contract types off the wire and prints
//! them, so a field the CLI drops is visible here as a field the CLI drops.
//!
//! It never spawns, restarts, or reconfigures a daemon. A probe that started
//! its own daemon would report a different runtime's state and would agree
//! with nothing the CLI had done.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use vz_runtime_contract::ProjectDefinition;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClient, DaemonClientConfig};

const SCHEMA_VERSION: u32 = 1;
const USAGE: &str = "usage:\n  \
    vz-runtime-probe state --state-db <path> --project-id <id> [--socket <path>] [--request-id <id>]\n  \
    vz-runtime-probe up --state-db <path> --definition <path> [--socket <path>] [--environment <name>]\n                     \
    [--timeout-millis <n>] [--request-id <id>] [--idempotency-key <key>]";

/// Every failure leaves the same shape on stdout, so a caller comparing this
/// against the CLI's error envelope is comparing two documents rather than one
/// document and a line of prose.
fn emit_error(reason: &str, detail: &str) -> ExitCode {
    let document = serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "vz-runtime-probe-error",
        "reason": reason,
        "detail": detail,
    });
    let mut out = std::io::stdout().lock();
    // A probe that cannot write its own diagnosis still has to fail loudly.
    if writeln!(out, "{document}").is_err() {
        return ExitCode::from(3);
    }
    let _ = out.flush();
    ExitCode::FAILURE
}

fn emit(document: &serde_json::Value) -> Result<(), String> {
    let mut out = std::io::stdout().lock();
    writeln!(out, "{document}").map_err(|error| error.to_string())?;
    out.flush().map_err(|error| error.to_string())
}

#[derive(Debug)]
struct Options {
    values: std::collections::BTreeMap<String, String>,
}

impl Options {
    /// Unknown and valueless options are refused rather than ignored: a probe
    /// that silently drops `--socket` would connect somewhere else and report
    /// a disagreement that is entirely its own.
    fn parse(argv: &[String], allowed: &[&str]) -> Result<Self, String> {
        let mut values = std::collections::BTreeMap::new();
        let mut index = 0;
        while index < argv.len() {
            let name = argv[index]
                .strip_prefix("--")
                .ok_or_else(|| format!("expected an option, found {:?}", argv[index]))?;
            if !allowed.contains(&name) {
                return Err(format!("unknown option --{name}"));
            }
            let value = argv
                .get(index + 1)
                .ok_or_else(|| format!("--{name} requires a value"))?;
            if value.starts_with("--") {
                return Err(format!("--{name} requires a value"));
            }
            if values.insert(name.to_string(), value.clone()).is_some() {
                return Err(format!("--{name} given more than once"));
            }
            index += 2;
        }
        Ok(Self { values })
    }

    fn required(&self, name: &str) -> Result<&str, String> {
        self.values
            .get(name)
            .map(String::as_str)
            .ok_or_else(|| format!("missing required option --{name}"))
    }

    fn optional(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }
}

/// Mirrors the CLI's own socket derivation exactly. Diverging here would point
/// the probe at a socket the CLI never used, and the disagreement it reported
/// would be an artefact of this function.
fn client_config(state_db: &Path, socket: Option<&str>) -> DaemonClientConfig {
    let mut config = DaemonClientConfig {
        // Read-only observation of a daemon the CLI already owns.
        auto_spawn: false,
        state_store_path: Some(state_db.to_path_buf()),
        ..DaemonClientConfig::default()
    };
    if let Some(socket_path) = socket {
        let socket_path = PathBuf::from(socket_path);
        if let Some(parent) = socket_path.parent()
            && !parent.as_os_str().is_empty()
        {
            config.runtime_data_dir = Some(parent.to_path_buf());
        }
        config.socket_path = socket_path;
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

fn metadata(request_id: &str, idempotency_key: &str) -> runtime_v2::RequestMetadata {
    runtime_v2::RequestMetadata {
        request_id: request_id.to_string(),
        idempotency_key: idempotency_key.to_string(),
        trace_id: String::new(),
    }
}

/// Correlation identifiers the caller did not pin. Uniqueness matters because
/// an accidentally reused idempotency key would replay an earlier operation
/// instead of observing this one.
fn generated_id(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or_default();
    format!("{prefix}-{}-{nanos}", std::process::id())
}

async fn run_state(options: &Options) -> Result<(), (String, String)> {
    let state_db = PathBuf::from(
        options
            .required("state-db")
            .map_err(|detail| ("invalid_arguments".to_string(), detail))?,
    );
    let project_id = options
        .required("project-id")
        .map_err(|detail| ("invalid_arguments".to_string(), detail))?
        .to_string();
    let request_id = options
        .optional("request-id")
        .map(str::to_string)
        .unwrap_or_else(|| generated_id("probe-state"));
    let config = client_config(&state_db, options.optional("socket"));
    let mut client = DaemonClient::connect_with_config(config)
        .await
        .map_err(|error| ("daemon_unavailable".to_string(), error.to_string()))?;
    let snapshot = client
        .get_project_state(runtime_v2::GetProjectStateRequest {
            metadata: Some(metadata(&request_id, &generated_id("probe-state-key"))),
            project_id,
        })
        .await
        .map_err(|error| ("get_project_state_failed".to_string(), error.to_string()))?;
    let project = serde_json::to_value(&snapshot.project)
        .map_err(|error| ("encode_failed".to_string(), error.to_string()))?;
    emit(&serde_json::json!({
        "schema_version": SCHEMA_VERSION,
        "kind": "vz-runtime-probe-state",
        "request_id": snapshot.request_id,
        "project": project,
    }))
    .map_err(|detail| ("write_failed".to_string(), detail))
}

async fn run_up(options: &Options) -> Result<(), (String, String)> {
    let state_db = PathBuf::from(
        options
            .required("state-db")
            .map_err(|detail| ("invalid_arguments".to_string(), detail))?,
    );
    let definition_path = PathBuf::from(
        options
            .required("definition")
            .map_err(|detail| ("invalid_arguments".to_string(), detail))?,
    );
    let bytes = std::fs::read(&definition_path).map_err(|error| {
        (
            "definition_unreadable".to_string(),
            format!("{}: {error}", definition_path.display()),
        )
    })?;
    let definition: ProjectDefinition = serde_json::from_slice(&bytes).map_err(|error| {
        (
            "definition_invalid".to_string(),
            format!("{}: {error}", definition_path.display()),
        )
    })?;
    let timeout_millis: u64 = match options.optional("timeout-millis") {
        Some(raw) => raw.parse().map_err(|_| {
            (
                "invalid_arguments".to_string(),
                format!("--timeout-millis must be a positive integer, got {raw:?}"),
            )
        })?,
        None => 300_000,
    };
    let request_id = options
        .optional("request-id")
        .map(str::to_string)
        .unwrap_or_else(|| generated_id("probe-up"));
    let idempotency_key = options
        .optional("idempotency-key")
        .map(str::to_string)
        .unwrap_or_else(|| generated_id("probe-up-key"));
    let config = client_config(&state_db, options.optional("socket"));
    let mut client = DaemonClient::connect_with_config(config)
        .await
        .map_err(|error| ("daemon_unavailable".to_string(), error.to_string()))?;
    let request = runtime_v2::UpEnvironmentRequest {
        fork: None,
        // The probe drives agreement checks against definitions that declare no
        // SecretBinding, and it must never be a second way to plant a value.
        secret_values: Default::default(),
        metadata: Some(metadata(&request_id, &idempotency_key)),
        definition: Some(vz_runtime_translate::project_definition_to_proto(
            &definition,
        )),
        environment: options.optional("environment").map(str::to_string),
        process_environment_id: None,
        workspace_key: None,
        path_hint: None,
        timeout_millis,
        workspace_root: options.optional("workspace-root").map(str::to_string),
    };
    let mut stream = client
        .up_environment_stream(request)
        .await
        .map_err(|error| ("up_admission_failed".to_string(), error.to_string()))?;
    // Every event is printed as it arrives, including the terminal one. A
    // caller reconstructing the transition sequence needs the intermediate
    // phases, not just the outcome.
    loop {
        let event = stream
            .next_event()
            .await
            .map_err(|error| ("up_stream_failed".to_string(), error.to_string()))?;
        let Some(event) = event else { break };
        let encoded = serde_json::to_value(&event)
            .map_err(|error| ("encode_failed".to_string(), error.to_string()))?;
        emit(&serde_json::json!({
            "schema_version": SCHEMA_VERSION,
            "kind": "vz-runtime-probe-up-event",
            "event": encoded,
        }))
        .map_err(|detail| ("write_failed".to_string(), detail))?;
    }
    Ok(())
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(mode) = argv.first().cloned() else {
        return emit_error("invalid_arguments", USAGE);
    };
    let rest = &argv[1..];
    let parsed = match mode.as_str() {
        "state" => Options::parse(rest, &["state-db", "project-id", "socket", "request-id"]),
        "up" => Options::parse(
            rest,
            &[
                "state-db",
                "definition",
                "socket",
                "environment",
                "timeout-millis",
                "request-id",
                "idempotency-key",
                "workspace-root",
            ],
        ),
        other => {
            return emit_error(
                "invalid_arguments",
                &format!("unknown mode {other:?}; {USAGE}"),
            );
        }
    };
    let options = match parsed {
        Ok(options) => options,
        Err(detail) => return emit_error("invalid_arguments", &format!("{detail}; {USAGE}")),
    };
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => return emit_error("runtime_unavailable", &error.to_string()),
    };
    let outcome = runtime.block_on(async {
        match mode.as_str() {
            "state" => run_state(&options).await,
            _ => run_up(&options).await,
        }
    });
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err((reason, detail)) => emit_error(&reason, &detail),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    fn argv(items: &[&str]) -> Vec<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    #[test]
    fn unknown_and_valueless_options_are_refused() {
        let allowed = ["state-db", "project-id"];
        assert_eq!(
            Options::parse(&argv(&["--socket", "/s"]), &allowed).unwrap_err(),
            "unknown option --socket"
        );
        assert_eq!(
            Options::parse(&argv(&["--state-db"]), &allowed).unwrap_err(),
            "--state-db requires a value"
        );
        // The next option is not a value: `--state-db --project-id p` would
        // otherwise read "--project-id" as a database path and then report a
        // missing --project-id, which describes neither mistake.
        assert_eq!(
            Options::parse(&argv(&["--state-db", "--project-id", "p"]), &allowed).unwrap_err(),
            "--state-db requires a value"
        );
        assert_eq!(
            Options::parse(&argv(&["--state-db", "a", "--state-db", "b"]), &allowed).unwrap_err(),
            "--state-db given more than once"
        );
        assert_eq!(
            Options::parse(&argv(&["state-db", "a"]), &allowed).unwrap_err(),
            "expected an option, found \"state-db\""
        );
    }

    #[test]
    fn a_required_option_is_reported_by_name() {
        let options = Options::parse(&argv(&["--state-db", "/db"]), &["state-db", "project-id"])
            .expect("parse");
        assert_eq!(options.required("state-db"), Ok("/db"));
        assert_eq!(
            options.required("project-id").unwrap_err(),
            "missing required option --project-id"
        );
        assert_eq!(options.optional("project-id"), None);
    }

    /// The probe must land on the same socket the CLI used. These are the CLI's
    /// own rules from `daemon_client_config_with_overrides`; if that function
    /// changes, this test is where the divergence has to be noticed.
    #[test]
    fn the_socket_is_derived_exactly_as_the_cli_derives_it() {
        let derived = client_config(Path::new("/work/state.db"), None);
        assert_eq!(
            derived.socket_path,
            PathBuf::from("/work/.vz-runtime/runtimed.sock")
        );
        assert_eq!(
            derived.runtime_data_dir,
            Some(PathBuf::from("/work/.vz-runtime"))
        );
        let overridden = client_config(Path::new("/work/state.db"), Some("/tmp/r/d.sock"));
        assert_eq!(overridden.socket_path, PathBuf::from("/tmp/r/d.sock"));
        assert_eq!(overridden.runtime_data_dir, Some(PathBuf::from("/tmp/r")));
        assert_eq!(
            overridden.state_store_path,
            Some(PathBuf::from("/work/state.db"))
        );
    }

    /// A probe that spawns is a probe reporting on a daemon the CLI never used.
    #[test]
    fn the_probe_never_spawns_a_daemon() {
        assert!(!client_config(Path::new("/work/state.db"), None).auto_spawn);
        assert!(!client_config(Path::new("/work/state.db"), Some("/tmp/r/d.sock")).auto_spawn);
    }

    #[test]
    fn generated_correlation_ids_are_distinct() {
        let first = generated_id("probe-up");
        let second = generated_id("probe-up");
        assert!(first.starts_with("probe-up-"), "{first}");
        assert_ne!(first, second);
    }
}
