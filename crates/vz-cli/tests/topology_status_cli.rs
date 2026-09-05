#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;
use tempfile::TempDir;
use vz_runtime_contract::{
    Architecture, CapabilitySet, EnvironmentSpec, MachineProfile, MachineResources, MachineSpec,
    OperatingSystem, ProjectDefinition, ProjectId, ProjectState, TOPOLOGY_SCHEMA_VERSION,
    TargetSpec,
};
use vz_runtimed::{RuntimeDaemon, RuntimedConfig, serve_runtime_uds_with_shutdown};
use vz_stack::StateStore;

const STATUS_ERROR_EXIT_CODE: i32 = 2;

struct IsolatedStatusInvocation {
    _root: TempDir,
    project: PathBuf,
    state_db: PathBuf,
    runtime_dir: PathBuf,
    socket: PathBuf,
}

impl IsolatedStatusInvocation {
    fn new() -> Self {
        // Keep Unix socket paths below macOS's short sockaddr_un limit.
        let root = tempfile::Builder::new()
            .prefix("vz-status-")
            .tempdir_in("/tmp")
            .unwrap();
        let project = root.path().join("project");
        fs::create_dir(&project).unwrap();
        Self {
            state_db: root.path().join("state/stack-state.db"),
            runtime_dir: root.path().join("runtime"),
            socket: root.path().join("runtime/runtimed.sock"),
            _root: root,
            project,
        }
    }

    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_vz"))
            .args(args)
            .current_dir(&self.project)
            .env("VZ_RUNTIME_STATE_DB", &self.state_db)
            .env("VZ_RUNTIME_DATA_DIR", &self.runtime_dir)
            .env("VZ_RUNTIME_DAEMON_SOCKET", &self.socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
            .env_remove("VZ_ENVIRONMENT_ID")
            .env_remove("VZ_MACHINE_ID")
            .output()
            .unwrap()
    }

    fn write_definition(&self, definition: &ProjectDefinition) {
        fs::write(
            self.project.join("vz.json"),
            serde_json::to_vec_pretty(definition).unwrap(),
        )
        .unwrap();
    }

    fn assert_no_daemon_or_state_created(&self) {
        assert!(
            !self.state_db.exists(),
            "state database must not be created"
        );
        assert!(
            !self.runtime_dir.exists(),
            "runtime directory must not be created"
        );
        assert!(!self.socket.exists(), "daemon socket must not be created");
    }
}

fn definition(project_name: &str, image: &str) -> ProjectDefinition {
    ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: ProjectId::new("prj_status_cli").unwrap(),
        name: project_name.to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machines: vec![
                MachineSpec {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    name: "app".to_string(),
                    profile: MachineProfile::Developer,
                    target: TargetSpec {
                        os: OperatingSystem::Linux,
                        arch: Architecture::Aarch64,
                        image: image.to_string(),
                        version: None,
                        channel: None,
                        digest: None,
                    },
                    resources: MachineResources::default(),
                    requested_capabilities: CapabilitySet::default(),
                    workspace: None,
                },
                MachineSpec {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    name: "worker".to_string(),
                    profile: MachineProfile::Developer,
                    target: TargetSpec {
                        os: OperatingSystem::Linux,
                        arch: Architecture::Aarch64,
                        image: image.to_string(),
                        version: None,
                        channel: None,
                        digest: None,
                    },
                    resources: MachineResources::default(),
                    requested_capabilities: CapabilitySet::default(),
                    workspace: None,
                },
            ],
            networks: Vec::new(),
            endpoints: Vec::new(),
        },
    }
}

fn parse_error(output: &Output, expected_code: &str) -> Value {
    assert_eq!(output.status.code(), Some(STATUS_ERROR_EXIT_CODE));
    assert!(output.stdout.is_empty());
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], expected_code);
    error
}

#[test]
fn status_missing_or_invalid_definition_stops_before_daemon_state() {
    let missing = IsolatedStatusInvocation::new();
    let output = missing.run(&["status", "--environment", "dev"]);
    parse_error(&output, "definition_not_found");
    missing.assert_no_daemon_or_state_created();

    let invalid = IsolatedStatusInvocation::new();
    fs::write(invalid.project.join("vz.json"), b"not-json").unwrap();
    let output = invalid.run(&["status", "--environment", "dev"]);
    parse_error(&output, "invalid_definition");
    invalid.assert_no_daemon_or_state_created();
}

#[test]
fn status_never_autospawns_or_creates_state_when_daemon_is_absent() {
    let invocation = IsolatedStatusInvocation::new();
    invocation.write_definition(&definition("status-project", "fixture:v1"));

    let output = invocation.run(&["status", "--environment", "dev"]);
    let error = parse_error(&output, "daemon_unavailable");
    assert_eq!(
        error["error"]["message"],
        "no compatible runtime daemon is listening on the configured socket"
    );
    invocation.assert_no_daemon_or_state_created();
}

#[test]
fn status_all_rejects_process_selectors_before_daemon_access() {
    let invocation = IsolatedStatusInvocation::new();
    invocation.write_definition(&definition("status-project", "fixture:v1"));

    let output = Command::new(env!("CARGO_BIN_EXE_vz"))
        .args(["status", "--all"])
        .current_dir(&invocation.project)
        .env("VZ_RUNTIME_STATE_DB", &invocation.state_db)
        .env("VZ_RUNTIME_DATA_DIR", &invocation.runtime_dir)
        .env("VZ_RUNTIME_DAEMON_SOCKET", &invocation.socket)
        .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
        .env("VZ_ENVIRONMENT_ID", "env_process")
        .output()
        .unwrap();
    parse_error(&output, "selector_conflict");
    invocation.assert_no_daemon_or_state_created();
}

async fn wait_for_socket(
    path: &Path,
    server: &mut tokio::task::JoinHandle<Result<(), vz_runtimed::RuntimedServerError>>,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if path.exists() {
            return;
        }
        if server.is_finished() {
            let result = (&mut *server).await;
            panic!("daemon server exited before binding {path:?}: {result:?}");
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("daemon socket was not created: {}", path.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn status_reads_exact_topology_reports_definition_drift_and_does_not_mutate() {
    let invocation = IsolatedStatusInvocation::new();
    let persisted_definition = definition("persisted-project", "fixture:v1");
    let desired_definition = definition("desired-project", "fixture:v2");
    invocation.write_definition(&desired_definition);

    fs::create_dir_all(invocation.state_db.parent().unwrap()).unwrap();
    let environment = persisted_definition
        .instantiate_environment("dev", 100)
        .unwrap();
    let environment_id = environment.environment_id.clone();
    let app_id = environment
        .machines
        .iter()
        .find(|machine| machine.name == "app")
        .unwrap()
        .machine_id
        .clone();
    let project = ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition: persisted_definition.clone(),
        environments: vec![environment],
    };
    let store = StateStore::open(&invocation.state_db).unwrap();
    store.save_project_state(&project).unwrap();
    drop(store);

    let daemon = Arc::new(
        RuntimeDaemon::start(RuntimedConfig {
            state_store_path: invocation.state_db.clone(),
            runtime_data_dir: invocation.runtime_dir.clone(),
            socket_path: invocation.socket.clone(),
        })
        .unwrap(),
    );
    let shutdown = Arc::new(tokio::sync::Notify::new());
    let shutdown_task = shutdown.clone();
    let daemon_task = daemon.clone();
    let socket_path = invocation.socket.clone();
    let mut server = tokio::spawn(async move {
        serve_runtime_uds_with_shutdown(daemon_task, socket_path, async move {
            shutdown_task.notified().await;
        })
        .await
    });
    wait_for_socket(&invocation.socket, &mut server).await;

    let project_dir = invocation.project.clone();
    let state_db = invocation.state_db.clone();
    let runtime_dir = invocation.runtime_dir.clone();
    let socket = invocation.socket.clone();
    let output = tokio::task::spawn_blocking(move || {
        Command::new(env!("CARGO_BIN_EXE_vz"))
            .args([
                "--json",
                "status",
                "--environment",
                "dev",
                "--machine",
                "app",
            ])
            .current_dir(project_dir)
            .env("VZ_RUNTIME_STATE_DB", state_db)
            .env("VZ_RUNTIME_DATA_DIR", runtime_dir)
            .env("VZ_RUNTIME_DAEMON_SOCKET", socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "1")
            // Explicit selectors outrank even malformed lower-priority process values.
            .env("VZ_ENVIRONMENT_ID", "invalid environment id")
            .env("VZ_MACHINE_ID", "invalid machine id")
            .output()
            .unwrap()
    })
    .await
    .unwrap();

    assert!(
        output.status.success(),
        "status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let status: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(status["schema_version"], 1);
    assert_eq!(status["topology_state_source"], "persisted");
    assert_eq!(status["host"]["os"], std::env::consts::OS);
    assert_eq!(status["host"]["arch"], std::env::consts::ARCH);
    assert!(status["daemon"]["backend_name"].as_str().is_some());
    assert!(status["daemon"]["version"].as_str().is_some());
    assert_eq!(
        status["project_id"],
        persisted_definition.project_id.as_str()
    );
    assert_eq!(status["project_name"], "persisted-project");
    assert_eq!(status["selection_source"], "explicit");
    assert_eq!(status["definition_drift"], true);
    assert_eq!(
        status["desired_definition_digest"],
        desired_definition.digest().unwrap()
    );
    assert_eq!(
        status["persisted_definition_digest"],
        persisted_definition.digest().unwrap()
    );
    assert_eq!(status["environments"].as_array().unwrap().len(), 1);
    assert_eq!(
        status["environments"][0]["environment_id"],
        environment_id.as_str()
    );
    assert_eq!(
        status["environments"][0]["machines"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        status["environments"][0]["machines"][0]["machine_id"],
        app_id.as_str()
    );

    let reopened = StateStore::open(&invocation.state_db).unwrap();
    assert_eq!(
        reopened
            .load_project_state(persisted_definition.project_id.as_str())
            .unwrap(),
        Some(project)
    );

    shutdown.notify_waiters();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("daemon shutdown timed out")
        .unwrap()
        .unwrap();
}
