//! Real CLI/UDS Delete admission failures. No VM, deletion success, or physical cleanup claim.
#![cfg(target_os = "macos")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use vz_runtime_contract::{
    EnvironmentState, MachineState, ProjectDefinition, ProjectId, ProjectState,
};
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};
use vz_stack::StateStore;

#[path = "support/installed_stop_daemon.rs"]
mod installed_stop_daemon;
use installed_stop_daemon::{ExternalDaemon, installed_binaries};

enum Server {
    InProcess(Arc<tokio::sync::Notify>, tokio::task::JoinHandle<()>),
    External(ExternalDaemon),
}

impl Server {
    async fn shutdown(self) {
        match self {
            Self::InProcess(shutdown, task) => {
                shutdown.notify_one();
                task.await.unwrap();
            }
            Self::External(server) => server.shutdown().await,
        }
    }
}

struct Fixture {
    root: TempDir,
    project: ProjectState,
    socket: PathBuf,
    database: PathBuf,
}
impl Fixture {
    fn new(stopped: bool) -> Self {
        let root = tempfile::Builder::new()
            .prefix("vz-delete-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let definition: ProjectDefinition = serde_json::from_value(json!({
            "schema_version": 1, "project_id": ProjectId::generate(), "name": "delete-test",
            "environment": {"schema_version": 1, "machines": [{"schema_version": 1,
                "name": "dev", "profile": "developer", "target": {"os": "linux", "arch": "aarch64", "image": "test-appliance"}}]}
        })).unwrap();
        fs::write(
            root.path().join("vz.json"),
            serde_json::to_vec(&definition).unwrap(),
        )
        .unwrap();
        let mut environments = ["selected", "sibling"]
            .map(|name| definition.instantiate_environment(name, 1).unwrap())
            .to_vec();
        for environment in &mut environments {
            environment.state = if stopped {
                EnvironmentState::Stopped
            } else {
                EnvironmentState::Failed
            };
            for machine in &mut environment.machines {
                machine.state = if stopped {
                    MachineState::Stopped
                } else {
                    MachineState::Failed
                };
            }
        }
        let project = ProjectState {
            schema_version: 1,
            definition,
            environments,
        };
        let database = root.path().join("state.db");
        let socket = root.path().join("daemon.sock");
        StateStore::open(&database)
            .unwrap()
            .save_project_state(&project)
            .unwrap();
        let mut fixture = Self {
            root,
            project,
            socket,
            database,
        };
        fixture.project = fixture.snapshot();
        fixture
    }
    fn snapshot(&self) -> ProjectState {
        StateStore::open(&self.database)
            .unwrap()
            .load_project_state_snapshot(self.project.definition.project_id.as_str())
            .unwrap()
            .unwrap()
    }
    fn command(&self) -> Command {
        let cli = installed_binaries()
            .map(|(cli, _)| cli)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_vz")));
        let mut command = Command::new(cli);
        command
            .current_dir(self.root.path())
            .env("VZ_RUNTIME_STATE_DB", &self.database)
            .env("VZ_RUNTIME_DATA_DIR", self.root.path().join("runtime"))
            .env("VZ_RUNTIME_DAEMON_SOCKET", &self.socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "0")
            .env_remove("VZ_CONTROL_PLANE_TRANSPORT")
            .env_remove("RUST_LOG")
            .env_remove("VZ_ENVIRONMENT_ID")
            .env_remove("VZ_MACHINE_ID")
            .env_remove("VZ_TEST_INSTALLED_CLI")
            .env_remove("VZ_TEST_INSTALLED_DAEMON");
        command
    }
    async fn serve(&self) -> Server {
        if let Some((_, daemon)) = installed_binaries() {
            return Server::External(ExternalDaemon::start(self, &daemon).await);
        }
        let daemon = Arc::new(
            RuntimeDaemon::start(RuntimedConfig {
                state_store_path: self.database.clone(),
                runtime_data_dir: self.root.path().join("runtime"),
                socket_path: self.socket.clone(),
            })
            .unwrap(),
        );
        let shutdown = Arc::new(tokio::sync::Notify::new());
        let stopped = shutdown.clone();
        let socket = self.socket.clone();
        let task = tokio::spawn(async move {
            serve_runtime_uds_with_shutdown(
                daemon,
                socket,
                async move { stopped.notified().await },
            )
            .await
            .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if tokio::net::UnixStream::connect(&self.socket).await.is_ok() {
                    break;
                }
                assert!(!task.is_finished());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Server::InProcess(shutdown, task)
    }
}

async fn run(mut command: Command) -> Output {
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || command.output().unwrap()),
    )
    .await
    .unwrap()
    .unwrap()
}
fn records(output: &Output) -> Vec<Value> {
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn error_record(output: &Output, code: &str) -> Value {
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).expect("one structured CLI error");
    assert_eq!(error["schema_version"], 1);
    assert_eq!(error["error"]["code"], code);
    error
}

fn assert_correlated_failure(output: &Output, code: &str) -> Value {
    let start = records(output);
    assert_eq!(
        start.len(),
        1,
        "failure must not fabricate progress or tombstone"
    );
    assert_eq!(start[0]["record_type"], "request_started");
    assert_eq!(start[0]["operation"], "delete_environment");
    let error = error_record(output, code);
    assert_eq!(error["error"]["request_id"], start[0]["request_id"]);
    assert_eq!(
        error["error"]["idempotency_key"],
        start[0]["idempotency_key"]
    );
    error
}

fn assert_no_daemon_effects(fixture: &Fixture) {
    assert!(!fixture.socket.exists());
    assert!(!fixture.socket.with_extension("pid").exists());
    assert!(!fixture.socket.with_extension("log").exists());
    assert!(!fixture.root.path().join("runtime").exists());
    assert!(!fixture.root.path().join(".git").exists());
    assert_eq!(fixture.snapshot(), fixture.project);
}

#[test]
fn delete_help_is_read_only_and_documents_only_the_environment_boundary() {
    let fixture = Fixture::new(true);
    let output = fixture
        .command()
        .args(["delete", "--help"])
        .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
        .output()
        .unwrap();
    assert!(output.status.success());
    let help = String::from_utf8(output.stdout).unwrap();
    assert!(help.contains("--environment"));
    assert!(help.contains("--request-id"));
    assert!(help.contains("--idempotency-key"));
    assert!(!help.contains("--machine"));
    assert!(!help.contains("--force"));
    assert!(output.stderr.is_empty());
    assert_no_daemon_effects(&fixture);
}

#[test]
fn delete_parser_rejects_machine_force_legacy_selector_and_unpaired_ids_without_effects() {
    let fixture = Fixture::new(true);
    for arguments in [
        vec!["delete", "old-sandbox-id"],
        vec!["delete", "--machine", "dev"],
        vec!["delete", "--force"],
        vec!["delete", "--request-id", "request"],
        vec!["delete", "--idempotency-key", "key"],
        vec!["delete", "--timeout", "0"],
        vec!["delete", "--timeout", "301"],
    ] {
        let output = fixture
            .command()
            .args(&arguments)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{arguments:?}");
        assert!(output.stdout.is_empty(), "{arguments:?}");
        assert_no_daemon_effects(&fixture);
    }
}

#[test]
fn delete_rejects_malformed_replay_ids_before_selection_or_daemon_access() {
    let fixture = Fixture::new(true);
    for invalid in [
        String::new(),
        " req".into(),
        "req\ninjected".into(),
        "x".repeat(257),
    ] {
        for invalid_request in [true, false] {
            let (request, key) = if invalid_request {
                (invalid.as_str(), "valid-key")
            } else {
                ("valid-request", invalid.as_str())
            };
            let output = fixture
                .command()
                .args([
                    "--json",
                    "delete",
                    "--environment",
                    "selected",
                    "--request-id",
                    request,
                    "--idempotency-key",
                    key,
                ])
                .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
                .output()
                .unwrap();
            assert_eq!(output.status.code(), Some(2));
            assert!(output.stdout.is_empty());
            error_record(&output, "validation_error");
            assert_no_daemon_effects(&fixture);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_requires_prior_control_owner_before_managed_recovery() {
    let fixture = Fixture::new(true);
    let mut command = fixture.command();
    command
        .args([
            "--json",
            "delete",
            "--environment",
            "selected",
            "--request-id",
            "req-no-daemon",
            "--idempotency-key",
            "idem-no-daemon",
        ])
        .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1");
    let output = run(command).await;
    assert_eq!(output.status.code(), Some(5));
    let error = assert_correlated_failure(&output, "daemon_unavailable");
    assert_eq!(error["error"]["request_id"], "req-no-daemon");
    assert_eq!(error["error"]["idempotency_key"], "idem-no-daemon");
    assert_no_daemon_effects(&fixture);
}

#[test]
fn malformed_process_environment_is_not_ignored_or_replaced_by_a_machine_selector() {
    use std::os::unix::ffi::OsStringExt;
    let fixture = Fixture::new(true);
    for selector in [
        std::ffi::OsString::from("invalid selector with spaces"),
        std::ffi::OsString::from_vec(vec![0xff]),
    ] {
        let output = fixture
            .command()
            .args(["--json", "delete"])
            .env("VZ_ENVIRONMENT_ID", selector)
            .env("VZ_MACHINE_ID", "unrelated invalid machine")
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(output.stdout.is_empty());
        error_record(&output, "invalid_selector");
        assert_no_daemon_effects(&fixture);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_delete_outranks_bad_process_selectors_and_returns_exact_api_failure() {
    let fixture = Fixture::new(true);
    let server = fixture.serve().await;
    for selector in [
        "selected".to_string(),
        fixture.project.environments[0].environment_id.to_string(),
    ] {
        let mut command = fixture.command();
        command
            .args([
                "--json",
                "delete",
                "--environment",
                &selector,
                "--request-id",
                "req-api-delete",
                "--idempotency-key",
                "idem-api-delete",
            ])
            .env("VZ_ENVIRONMENT_ID", "invalid lower-priority environment")
            .env("VZ_MACHINE_ID", "invalid irrelevant Machine");
        let output = run(command).await;
        assert_eq!(output.status.code(), Some(2));
        // The fixture has no physical store/VM reservation. The real daemon
        // must reject this incomplete graph before creating a Delete journal.
        let error = assert_correlated_failure(&output, "unsupported_operation");
        assert_eq!(error["error"]["details"]["operation"], "delete_environment");
        assert_eq!(
            error["error"]["details"]["project_id"],
            fixture.project.definition.project_id.to_string()
        );
        assert_eq!(error["error"]["request_id"], "req-api-delete");
        assert_eq!(fixture.snapshot(), fixture.project);
        assert!(
            StateStore::open(&fixture.database)
                .unwrap()
                .load_environment_lifecycle_by_idempotency_key("idem-api-delete")
                .unwrap()
                .is_none()
        );
    }
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn process_environment_uses_exact_id_without_worktree_or_machine_fallback() {
    let fixture = Fixture::new(true);
    let server = fixture.serve().await;
    let mut command = fixture.command();
    command
        .args(["--json", "delete"])
        .env(
            "VZ_ENVIRONMENT_ID",
            fixture.project.environments[0].environment_id.as_str(),
        )
        .env("VZ_MACHINE_ID", "invalid irrelevant Machine");
    let output = run(command).await;
    let error = assert_correlated_failure(&output, "unsupported_operation");
    assert_eq!(error["error"]["details"]["operation"], "delete_environment");
    assert_eq!(fixture.snapshot(), fixture.project);
    assert!(!fixture.root.path().join(".git").exists());
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_explicit_or_process_environment_never_falls_back_to_another_environment() {
    let fixture = Fixture::new(true);
    let server = fixture.serve().await;
    // Process selectors are exact opaque IDs. A syntactically valid human
    // name must reach ID lookup and fail, never select the namesake instance.
    for (explicit, stale) in [
        (
            true,
            vz_runtime_contract::EnvironmentId::generate().to_string(),
        ),
        (
            false,
            vz_runtime_contract::EnvironmentId::generate().to_string(),
        ),
        (false, "selected".to_owned()),
    ] {
        let mut command = fixture.command();
        command.args(["--json", "delete"]);
        if explicit {
            command.args(["--environment", &stale]).env(
                "VZ_ENVIRONMENT_ID",
                fixture.project.environments[0].environment_id.as_str(),
            );
        } else {
            command.env("VZ_ENVIRONMENT_ID", &stale);
        }
        let output = run(command).await;
        assert_eq!(output.status.code(), Some(2));
        let error = assert_correlated_failure(&output, "state_conflict");
        assert_eq!(error["error"]["details"]["operation"], "delete_environment");
        assert!(error["error"]["message"].as_str().unwrap().contains(&stale));
        assert_eq!(fixture.snapshot(), fixture.project);
    }
    server.shutdown().await;
}
