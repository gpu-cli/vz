//! Real CLI/UDS Stop admission and journal replay. No VM or physical Stop claim.
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
            .prefix("vz-stop-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let definition: ProjectDefinition = serde_json::from_value(json!({
            "schema_version": 1, "project_id": ProjectId::generate(), "name": "stop-test",
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn selected_stop_and_exact_replay_use_real_rpc_and_preserve_sibling() {
    let fixture = Fixture::new(true);
    let server = fixture.serve().await;
    let mut command = fixture.command();
    command
        .args([
            "--json",
            "stop",
            "--environment",
            "selected",
            "--request-id",
            "req-cli-stop",
            "--idempotency-key",
            "idem-cli-stop",
        ])
        .env("VZ_ENVIRONMENT_ID", "invalid lower selector")
        .env("VZ_MACHINE_ID", "unrelated invalid machine selector");
    let output = run(command).await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let first = records(&output);
    assert_eq!(first.len(), 2);
    assert_eq!(first[0]["record_type"], "request_started");
    assert_eq!(first[0]["idempotency_key"], "idem-cli-stop");
    assert_eq!(first[1]["terminal"], true);
    assert_eq!(first[1]["operation"]["status"], "succeeded");
    let mut command = fixture.command();
    command.args([
        "--json",
        "stop",
        "--environment",
        "selected",
        "--request-id",
        "req-cli-stop",
        "--idempotency-key",
        "idem-cli-stop",
    ]);
    let output = run(command).await;
    assert!(output.status.success());
    assert_eq!(records(&output), first);
    let after = fixture.snapshot();
    assert_eq!(
        after.environments.iter().find(|env| env.name == "sibling"),
        fixture
            .project
            .environments
            .iter()
            .find(|env| env.name == "sibling")
    );
    let selected = after
        .environments
        .iter()
        .find(|env| env.name == "selected")
        .unwrap();
    assert_eq!(selected.lifecycle_generation, 1);
    assert_eq!(
        selected.machines,
        fixture
            .project
            .environments
            .iter()
            .find(|env| env.name == "selected")
            .unwrap()
            .machines
    );
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unknown_after_restart_fails_closed_with_correlated_error_and_no_mutation() {
    let fixture = Fixture::new(false);
    let server = fixture.serve().await;
    let mut command = fixture.command();
    command.args(["--json", "stop", "--environment", "selected"]);
    let output = run(command).await;
    assert_eq!(output.status.code(), Some(2));
    let start = records(&output);
    assert_eq!(start.len(), 1);
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "state_conflict");
    assert_eq!(error["error"]["request_id"], start[0]["request_id"]);
    assert_eq!(
        error["error"]["idempotency_key"],
        start[0]["idempotency_key"]
    );
    assert_eq!(fixture.snapshot(), fixture.project);
    server.shutdown().await;
}

#[test]
fn stop_rejects_legacy_positional_selector_and_control_character_ids_before_admission() {
    let fixture = Fixture::new(true);
    let output = fixture
        .command()
        .args(["stop", "vz-run-old-sandbox"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    let output = fixture
        .command()
        .args([
            "--json",
            "stop",
            "--environment",
            "selected",
            "--request-id",
            "req\ninjected",
            "--idempotency-key",
            "idem",
        ])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "validation_error");
    assert!(!fixture.socket.exists());
    assert_eq!(fixture.snapshot(), fixture.project);
}
