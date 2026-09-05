//! Actual CLI/UDS admission and seeded receipt replay; not physical guest evidence.
#![cfg(target_os = "macos")]
#![allow(clippy::unwrap_used)]
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
use vz_runtime_contract::*;
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
            Self::InProcess(signal, task) => {
                signal.notify_one();
                task.await.unwrap();
            }
            Self::External(server) => server.shutdown().await,
        }
    }
}

struct Fixture {
    root: tempfile::TempDir,
    database: PathBuf,
    socket: PathBuf,
    project: ProjectState,
    receipt: MachineExecutionReceipt,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::Builder::new()
            .prefix("vz-exec-")
            .tempdir_in("/private/tmp")
            .unwrap();
        let definition:ProjectDefinition=serde_json::from_value(json!({"schema_version":1,"project_id":ProjectId::generate(),"name":"exec-cli","environment":{"schema_version":1,"machines":[{"schema_version":1,"name":"app","profile":"hardened","target":{"os":"linux","arch":"aarch64","image":"fixture"},"requested_capabilities":{"capabilities":["posix_exec"]}}]}})).unwrap();
        std::fs::write(
            root.path().join("vz.json"),
            serde_json::to_vec(&definition).unwrap(),
        )
        .unwrap();
        let mut environment = definition.instantiate_environment("selected", 1).unwrap();
        environment.lifecycle_generation = 1;
        environment.state = EnvironmentState::Ready;
        let machine = &mut environment.machines[0];
        machine.state = MachineState::Ready;
        machine.backend = Some(MachineBackend::MacosVirtualizationLinux);
        machine.negotiated_capabilities = CapabilitySet::new([MachineCapability::PosixExec]);
        let incarnation = MachineIncarnation {
            schema_version: 1,
            incarnation_id: MachineIncarnationId::generate(),
            machine_id: machine.machine_id.clone(),
            generation: 1,
            created_at: 1,
        };
        let identity = MachineRuntimeIdentity {
            schema_version: 1,
            opaque_id: "seeded-no-live-runtime".into(),
        };
        machine.incarnation = Some(incarnation.clone());
        machine.runtime_identity = Some(identity.clone());
        let spec = MachineExecutionSpec {
            argv: vec!["/bin/sh".into(), "-c".into(), "exit 7".into()],
            environment: Default::default(),
            working_directory: None,
            user: None,
            terminal: None,
            timeout_millis: 1000,
        };
        let scope = MachineExecutionScope {
            schema_version: 1,
            execution_id: "mex_seeded".into(),
            request_id: "req-exec-cli".into(),
            idempotency_key: "idem-exec-cli".into(),
            request_hash: spec
                .request_hash(
                    &definition.project_id,
                    &environment.environment_id,
                    &machine.machine_id,
                )
                .unwrap(),
            project_id: definition.project_id.clone(),
            environment_id: environment.environment_id.clone(),
            machine_id: machine.machine_id.clone(),
            environment_generation: 1,
            incarnation: incarnation.clone(),
            runtime_identity: identity,
            definition_digest: environment.definition_digest.clone(),
        };
        environment.ownership.push(OwnershipRecord {
            schema_version: 1,
            resource_kind: OwnedResourceKind::Incarnation,
            resource_id: incarnation.incarnation_id.to_string(),
            environment_id: scope.environment_id.clone(),
            machine_id: Some(scope.machine_id.clone()),
        });
        let project = ProjectState {
            schema_version: 1,
            definition,
            environments: vec![environment],
        };
        let database = root.path().join("state.db");
        let socket = root.path().join("daemon.sock");
        StateStore::open(&database)
            .unwrap()
            .save_project_state(&project)
            .unwrap();
        // Compare the canonical database projection, whose ownership rows have
        // deterministic ordering independent of fixture insertion order.
        let project = StateStore::open(&database)
            .unwrap()
            .load_project_state_snapshot(project.definition.project_id.as_str())
            .unwrap()
            .unwrap();
        let receipt = MachineExecutionReceipt {
            scope,
            state: MachineExecutionState::Admitted,
            exit_code: None,
            failure: None,
            output_replay_available: false,
            created_at: 2,
            updated_at: 2,
        };
        Self {
            root,
            database,
            socket,
            project,
            receipt,
        }
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
        let signal = Arc::clone(&shutdown);
        let socket = self.socket.clone();
        let task = tokio::spawn(async move {
            serve_runtime_uds_with_shutdown(daemon, socket, async move { signal.notified().await })
                .await
                .unwrap();
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::net::UnixStream::connect(&self.socket).await.is_err() {
                assert!(!task.is_finished());
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        Server::InProcess(shutdown, task)
    }
    fn command(&self) -> tokio::process::Command {
        let binary = installed_binaries()
            .map(|(cli, _)| cli)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_vz")));
        let mut command = tokio::process::Command::new(binary);
        command
            .kill_on_drop(true)
            .current_dir(self.root.path())
            .env("VZ_RUNTIME_STATE_DB", &self.database)
            .env("VZ_RUNTIME_DATA_DIR", self.root.path().join("runtime"))
            .env("VZ_RUNTIME_DAEMON_SOCKET", &self.socket)
            .env("VZ_RUNTIME_DAEMON_AUTOSTART", "0")
            .env_remove("VZ_CONTROL_PLANE_TRANSPORT")
            .env_remove("RUST_LOG")
            .env_remove("VZ_ENVIRONMENT_ID")
            .env_remove("VZ_MACHINE_ID");
        command.args([
            "--json",
            "exec",
            "--environment",
            "selected",
            "--machine",
            "app",
            "--timeout",
            "1",
            "--request-id",
            "req-exec-cli",
            "--idempotency-key",
            "idem-exec-cli",
            "--",
            "/bin/sh",
            "-c",
            "exit 7",
        ]);
        command
    }
    fn assert_project_unchanged(&self) {
        assert_eq!(
            StateStore::open(&self.database)
                .unwrap()
                .load_project_state_snapshot(self.project.definition.project_id.as_str())
                .unwrap()
                .unwrap(),
            self.project
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_exec_uses_topology_and_explicit_selectors_without_runtime_fallback() {
    let fixture = Fixture::new();
    let server = fixture.serve().await;
    let output = tokio::time::timeout(
        Duration::from_secs(10),
        fixture
            .command()
            .env("VZ_ENVIRONMENT_ID", "malformed-lower-tier")
            .env("VZ_MACHINE_ID", "malformed-lower-tier")
            .output(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        output.status.code(),
        Some(2),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let records: Vec<Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["record_type"], "request_started");
    assert_eq!(records[0]["idempotency_key"], "idem-exec-cli");
    let error: Value = serde_json::from_slice(&output.stderr).unwrap();
    assert_eq!(error["error"]["code"], "state_conflict");
    assert_eq!(error["error"]["request_id"], "req-exec-cli");
    assert!(
        StateStore::open(&fixture.database)
            .unwrap()
            .load_machine_execution("idem-exec-cli")
            .unwrap()
            .is_none()
    );
    fixture.assert_project_unchanged();
    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_exec_replays_only_exact_seeded_terminal_receipt_and_nonzero_exit() {
    let fixture = Fixture::new();
    let store = StateStore::open(&fixture.database).unwrap();
    store.claim_machine_execution(&fixture.receipt).unwrap();
    let mut receipt = fixture.receipt.clone();
    receipt.state = MachineExecutionState::Completed;
    receipt.exit_code = Some(7);
    receipt.updated_at = 3;
    store
        .finish_machine_execution(&receipt.scope, &receipt)
        .unwrap();
    drop(store);
    let server = fixture.serve().await;
    let output = tokio::time::timeout(Duration::from_secs(10), fixture.command().output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(7),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let records: Vec<Value> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 2);
    assert_eq!(records[1]["record_type"], "execution_receipt");
    assert_eq!(records[1]["replayed"], true);
    assert_eq!(records[1]["receipt"]["exit_code"], 7);
    assert_eq!(records[1]["receipt"]["output_replay_available"], false);
    fixture.assert_project_unchanged();
    server.shutdown().await;
}
