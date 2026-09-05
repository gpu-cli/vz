//! Actual CLI/UDS Up admission; an empty verified catalog cannot boot or certify Ready.
#![cfg(target_os = "macos")]
#![allow(clippy::unwrap_used, clippy::expect_used)]
use serde_json::{Value, json};
use std::{
    fs,
    path::PathBuf,
    process::{Command, Output},
    sync::Arc,
    time::Duration,
};
use tempfile::TempDir;
use vz_runtime_contract::{EnvironmentState, ProjectDefinition, ProjectId, ProjectState};
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};
use vz_stack::StateStore;
#[path = "support/installed_stop_daemon.rs"]
mod installed_stop_daemon;
use installed_stop_daemon::{ExternalDaemon, installed_binaries};

struct Fixture {
    root: TempDir,
    database: PathBuf,
    socket: PathBuf,
    definition: ProjectDefinition,
}
enum Server {
    InProcess(Arc<tokio::sync::Notify>, tokio::task::JoinHandle<()>),
    External(ExternalDaemon),
}
impl Server {
    async fn shutdown(self) {
        match self {
            Self::InProcess(notify, task) => {
                notify.notify_one();
                task.await.unwrap();
            }
            Self::External(server) => server.shutdown().await,
        }
    }
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("vz-up-cli-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let definition=serde_json::from_value(json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"up-cli","environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"developer","target":{"os":"linux","arch":"aarch64","image":"vz-linux-appliance","digest":format!("sha256:{}","a".repeat(64))}}]}})).unwrap();
        Self {
            database: root.path().join("state.db"),
            socket: root.path().join("d.sock"),
            root,
            definition,
        }
    }
    fn bootstrap(&self) {
        let output = Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(self.root.path())
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .output()
            .unwrap();
        assert!(output.status.success());
        fs::write(
            self.root.path().join("vz.json"),
            serde_json::to_vec(&self.definition).unwrap(),
        )
        .unwrap();
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
            .env_remove("VZ_CONTROL_PLANE_TRANSPORT")
            .env_remove("VZ_ENVIRONMENT_ID")
            .env_remove("VZ_MACHINE_ID")
            .env_remove("RUST_LOG")
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("VZ_TEST_INSTALLED_CLI")
            .env_remove("VZ_TEST_INSTALLED_DAEMON");
        command
    }
    fn snapshot(&self) -> ProjectState {
        StateStore::open(&self.database)
            .unwrap()
            .load_project_state_snapshot(self.definition.project_id.as_str())
            .unwrap()
            .unwrap()
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
        let notify = Arc::new(tokio::sync::Notify::new());
        let shutdown = Arc::clone(&notify);
        let socket = self.socket.clone();
        let task =
            tokio::spawn(async move {
                serve_runtime_uds_with_shutdown(daemon, socket, async move {
                    shutdown.notified().await
                })
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
        Server::InProcess(notify, task)
    }
}
async fn run(command: Command) -> Output {
    let mut command = tokio::process::Command::from(command);
    command.kill_on_drop(true);
    tokio::time::timeout(Duration::from_secs(15), command.output())
        .await
        .unwrap()
        .unwrap()
}
fn terminal(output: &Output) -> Value {
    assert!(!output.status.success());
    assert!(
        output.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records[0]["record_type"], "request_started");
    records.last().unwrap()["progress"]["completion"].clone()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_definition_has_zero_runtime_or_workspace_mutation() {
    let fixture = Fixture::new();
    let mut command = fixture.command();
    command.args(["--json", "up"]);
    let output = run(command).await;
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "definition_not_found");
    assert!(!fixture.database.exists());
    assert!(!fixture.socket.exists());
    assert!(!fixture.root.path().join(".git").exists());
    assert!(!fixture.root.path().join("runtime").exists());
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_up_rpc_creates_one_default_and_replays_exact_catalog_failure_without_false_ready() {
    let fixture = Fixture::new();
    fixture.bootstrap();
    let server = fixture.serve().await;
    let args = [
        "--json",
        "up",
        "--request-id",
        "req-up-cli",
        "--idempotency-key",
        "idem-up-cli",
    ];
    let mut command = fixture.command();
    command.args(args);
    let first = terminal(&run(command).await);
    assert!(!first["error"].is_null());
    assert!(first["operation"].is_null());
    let snapshot = fixture.snapshot();
    assert_eq!(snapshot.environments.len(), 1);
    assert_eq!(snapshot.environments[0].name, "default");
    assert_eq!(snapshot.environments[0].state, EnvironmentState::Creating);
    assert!(snapshot.environments[0].bindings.is_empty());
    let mut command = fixture.command();
    command.args(args);
    let replay = terminal(&run(command).await);
    assert_eq!(first, replay);
    assert_eq!(fixture.snapshot(), snapshot);
    server.shutdown().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_named_up_ignores_invalid_lower_selectors_and_preserves_sibling() {
    let fixture = Fixture::new();
    fixture.bootstrap();
    let server = fixture.serve().await;
    for name in ["alpha", "beta"] {
        let mut command = fixture.command();
        command
            .args([
                "--json",
                "up",
                "--environment",
                name,
                "--request-id",
                name,
                "--idempotency-key",
                name,
            ])
            .env("VZ_ENVIRONMENT_ID", "invalid lower tier")
            .env("VZ_MACHINE_ID", "unrelated invalid machine");
        terminal(&run(command).await);
    }
    let project = fixture.snapshot();
    assert_eq!(project.environments.len(), 2);
    assert_ne!(
        project.environments[0].environment_id,
        project.environments[1].environment_id
    );
    assert!(
        project
            .environments
            .iter()
            .all(
                |environment| environment.state == EnvironmentState::Creating
                    && environment.bindings.is_empty()
            )
    );
    server.shutdown().await;
}
