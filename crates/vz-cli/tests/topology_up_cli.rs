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
    /// The worktree the CLI runs in, kept out of the daemon's own state root so
    /// an inventory of it is an inventory of the user's files and nothing else.
    worktree: PathBuf,
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
        let worktree = root.path().join("worktree");
        fs::create_dir(&worktree).unwrap();
        Self {
            database: root.path().join("state.db"),
            socket: root.path().join("d.sock"),
            worktree,
            root,
            definition,
        }
    }
    fn bootstrap(&self) {
        self.bootstrap_definition(&self.definition);
    }
    fn bootstrap_definition(&self, definition: &ProjectDefinition) {
        let output = Command::new("git")
            .args(["init", "--quiet"])
            .current_dir(&self.worktree)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .output()
            .unwrap();
        assert!(output.status.success());
        fs::write(
            self.worktree.join("vz.json"),
            serde_json::to_vec(definition).unwrap(),
        )
        .unwrap();
    }
    fn command(&self) -> Command {
        let cli = installed_binaries()
            .map(|(cli, _)| cli)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_vz")));
        let mut command = Command::new(cli);
        command
            .current_dir(&self.worktree)
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
/// The terminal receipt of a failed `--json up`, with the error envelope the
/// same failure prints on stderr proved to agree with it.
///
/// This used to assert `output.stderr.is_empty()` — that a failure carried in
/// the Up stream's terminal receipt printed no envelope at all under `--json`.
/// That was the defect: it left `vz --json up` exiting nonzero with nothing on
/// stderr, so a caller could not tell a host export port collision from any
/// other refusal, while a refusal decided *before* the stream (see
/// `missing_definition_has_zero_runtime_or_workspace_mutation`, which reads its
/// envelope off stderr) printed one normally. Both now print the same envelope,
/// and this asserts they say the same thing the receipt says.
fn terminal(output: &Output) -> Value {
    assert!(!output.status.success());
    let envelope: Value = serde_json::from_slice(&output.stderr).unwrap_or_else(|error| {
        panic!(
            "a failed --json up must print one error envelope on stderr ({error}): {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    assert_eq!(envelope["schema_version"], 1);
    let records = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(records[0]["record_type"], "request_started");
    let completion = records.last().unwrap()["progress"]["completion"].clone();
    // CLI and receipt agree on the failure, not merely on the exit status.
    assert_eq!(envelope["error"]["code"], completion["error"]["code"]);
    assert_eq!(envelope["error"]["message"], completion["error"]["message"]);
    assert_eq!(envelope["error"]["details"], completion["error"]["details"]);
    completion
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
    assert!(!fixture.worktree.join(".git").exists());
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

/// Every path under `root`, `.git` included, with contents, so any appearance,
/// disappearance or edit is a difference.
fn inventory(root: &std::path::Path) -> std::collections::BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn walk(
        root: &std::path::Path,
        at: &std::path::Path,
        into: &mut std::collections::BTreeMap<PathBuf, Option<Vec<u8>>>,
    ) {
        let mut entries: Vec<_> = fs::read_dir(at)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        entries.sort();
        for path in entries {
            let relative = path.strip_prefix(root).unwrap().to_path_buf();
            let metadata = fs::symlink_metadata(&path).unwrap();
            if metadata.is_dir() {
                into.insert(relative, None);
                walk(root, &path, into);
            } else if metadata.is_symlink() {
                into.insert(
                    relative,
                    Some(
                        fs::read_link(&path)
                            .unwrap()
                            .into_os_string()
                            .into_encoded_bytes(),
                    ),
                );
            } else {
                into.insert(relative, Some(fs::read(&path).unwrap()));
            }
        }
    }
    let mut into = std::collections::BTreeMap::new();
    walk(root, root, &mut into);
    into
}

fn definition_with(machine: Value) -> ProjectDefinition {
    serde_json::from_value(json!({
        "schema_version": 1,
        "project_id": ProjectId::generate(),
        "name": "up-cli",
        "environment": {"schema_version": 1, "machines": [machine]},
    }))
    .unwrap()
}

fn linux_machine() -> Value {
    json!({
        "schema_version": 1,
        "name": "app",
        "profile": "developer",
        "target": {
            "os": "linux",
            "arch": "aarch64",
            "image": "vz-linux-appliance",
            "digest": format!("sha256:{}", "a".repeat(64)),
        },
    })
}

/// A refused Up must not write the worktree, whatever it was refused for.
///
/// "Rejected before mutation" is an ordering claim, so it is proved the only
/// way an ordering claim can be: inventory the worktree, issue an Up that is
/// refused, inventory it again, and require the two to be identical. The
/// workspace key is persistent identity, so a token minted for a refused Up and
/// left behind is a binding artifact that a later Up would find and adopt.
///
/// The cases are deliberately refusals of different kinds and from different
/// sides of the wire: one the CLI itself refuses before it ever connects, and
/// two the daemon refuses before `reserve_environment_up_admission` runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_refused_up_leaves_the_worktree_byte_identical() {
    // The daemon is never reached: the CLI has no definition to send.
    let fixture = Fixture::new();
    fixture.bootstrap_definition(&fixture.definition);
    fs::remove_file(fixture.worktree.join("vz.json")).unwrap();
    let before = inventory(&fixture.worktree);
    let mut command = fixture.command();
    command.args(["--json", "up"]);
    let output = run(command).await;
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "definition_not_found");
    assert_eq!(inventory(&fixture.worktree), before, "definition_not_found");

    // Refused by the daemon for a capability the checked-in matrix does not
    // advertise, and refused by it for a target no adapter serves. Both are
    // refusals before admission, so neither may leave a token behind either.
    let mut unadvertised = linux_machine();
    unadvertised["requested_capabilities"] = json!({"capabilities": ["posix_exec", "snapshot"]});
    let mut unserved = linux_machine();
    unserved["target"]["arch"] = json!("x86_64");
    for (label, definition) in [
        ("unadvertised capability", definition_with(unadvertised)),
        ("unserved target", definition_with(unserved)),
    ] {
        let fixture = Fixture::new();
        fixture.bootstrap_definition(&definition);
        let server = fixture.serve().await;
        let before = inventory(&fixture.worktree);
        let mut command = fixture.command();
        command.args(["--json", "up"]);
        let output = run(command).await;
        assert!(!output.status.success(), "{label}");
        let error: Value = serde_json::from_slice(&output.stderr).unwrap();
        assert_eq!(error["error"]["code"], "unsupported_operation", "{label}");
        assert_eq!(inventory(&fixture.worktree), before, "{label}");
        // No admission means no reserved identity to find later either.
        assert!(
            StateStore::open(&fixture.database)
                .unwrap()
                .load_project_state_snapshot(definition.project_id.as_str())
                .unwrap()
                .is_none(),
            "{label}"
        );
        server.shutdown().await;
    }
}

/// A Machine requesting a capability the matrix marks PLANNED is refused, and
/// the refusal names the capability in machine-readable detail.
///
/// `config/host-target-capabilities-v0.4.json` is the source of truth for what
/// this host × target × profile can negotiate, and it marks `snapshot` PLANNED
/// for Apple-silicon macOS hosting a Developer Linux Machine. Echoing the
/// request back as "negotiated" would make discovery, `vz status`, the help and
/// the site copy that all read it claim a capability the runtime does not have.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_capability_the_matrix_does_not_advertise_is_refused_by_name() {
    let mut machine = linux_machine();
    machine["requested_capabilities"] = json!({"capabilities": ["posix_exec", "snapshot"]});
    let definition = definition_with(machine);
    let fixture = Fixture::new();
    fixture.bootstrap_definition(&definition);
    let server = fixture.serve().await;

    let mut command = fixture.command();
    command.args(["--json", "up"]);
    let output = run(command).await;
    assert!(!output.status.success());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "unsupported_operation");
    assert_eq!(error["error"]["details"]["capability"], "snapshot");
    assert_eq!(error["error"]["details"]["capability_status"], "PLANNED");
    assert_eq!(error["error"]["details"]["machine"], "app");
    assert_eq!(error["error"]["details"]["host"], "macos-arm64");
    assert_eq!(error["error"]["details"]["target"], "linux");
    assert_eq!(error["error"]["details"]["profile"], "developer");
    let message = error["error"]["message"].as_str().unwrap();
    assert!(message.contains("snapshot"), "{message}");
    assert!(
        message.contains("config/host-target-capabilities-v0.4.json"),
        "{message}"
    );

    // A capability the matrix does advertise for this pair is still admitted:
    // the Up gets as far as the empty verified catalog, not a capability refusal.
    let mut advertised = linux_machine();
    advertised["requested_capabilities"] =
        json!({"capabilities": ["posix_exec", "docker_engine", "compose", "buildx"]});
    let advertised = definition_with(advertised);
    fs::write(
        fixture.worktree.join("vz.json"),
        serde_json::to_vec(&advertised).unwrap(),
    )
    .unwrap();
    let mut command = fixture.command();
    command.args(["--json", "up"]);
    let admitted = terminal(&run(command).await);
    let admitted = admitted["error"]["message"].as_str().unwrap();
    assert!(!admitted.contains("capability"), "{admitted}");
    server.shutdown().await;
}
