//! End-to-end stack tests exercising real Linux VMs via the full pipeline:
//! compose YAML → parse → reconcile → execute → verify.
//!
//! These tests boot real Linux VMs, pull real OCI images, and execute
//! the complete stack control plane through the OCI container runtime.
//!
//! Requirements:
//! - Apple Silicon Mac (arm64)
//! - Linux kernel artifacts installed (`~/.vz/linux/`)
//! - Network access for image pulls (first run only; cached after)
//!
//! Run with: `./scripts/run-sandbox-vm-e2e.sh --suite stack`

#![cfg(target_os = "macos")]
#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::io::ErrorKind;
use std::path::Path;
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use sha2::{Digest, Sha256};
use vz_oci_macos::{
    MacosOciError, MacosRuntimeBackend, Runtime, RuntimeConfig, RuntimeLifecycleAdmissionKind,
    RuntimeLifecycleDiagnostics,
};
use vz_runtime_contract::{
    Architecture, CapabilitySet, Container, ContainerCreateReceipt, ContainerGenerationOwnership,
    ContainerState, ContractInvariantError, EnvironmentId, EnvironmentInstance,
    EnvironmentLifecycleKind, EnvironmentLifecycleOperation, EnvironmentLifecycleStatus,
    EnvironmentSpec, EnvironmentState, ExecConfig, GenerationCleanupOutcome, Lease, LeaseState,
    LifecycleStepResult, LifecycleStepStatus, MachineBackend, MachineCapability, MachineErrorCode,
    MachineId, MachineIncarnation, MachineIncarnationId, MachineInstance,
    MachineLifecycleStepAcknowledgement, MachineProfile, MachineResources, MachineSpec,
    MachineState, NetworkServiceConfig, OperatingSystem, OwnedCreateError, OwnedResourceKind,
    OwnershipCleanupStepAcknowledgement, OwnershipRecord, PortMapping, ProjectDefinition,
    ProjectId, ProjectState, ResourceOwner, RunConfig, RuntimeBackend, Sandbox, SandboxBackend,
    SandboxSpec, SandboxState, StackResourceHint, TOPOLOGY_SCHEMA_VERSION, TargetSpec,
};
use vz_stack::{
    Action, ContainerRuntime, ImagePolicy, OrchestrationConfig, ServicePhase, StackError,
    StackEvent, StackExecutor, StackOrchestrator, StateStore, apply, parse_compose,
    parse_compose_with_dir,
};

const EXPECTED_VM_FULL_UNSUPPORTED_REASON: &str = "vm_full_checkpoint=false: shared VM state depends on external VirtioFS/device state that is not captured atomically";

fn has_virtualization_entitlement() -> bool {
    let Ok(test_binary) = std::env::current_exe() else {
        return false;
    };
    let Ok(output) = Command::new("codesign")
        .arg("-d")
        .arg("--entitlements")
        .arg(":-")
        .arg(&test_binary)
        .output()
    else {
        return false;
    };

    let entitlements = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    entitlements.contains("com.apple.security.virtualization")
}

fn require_virtualization_entitlement() -> bool {
    if has_virtualization_entitlement() {
        return true;
    }

    eprintln!(
        "VZ_E2E_REQUIRED_SKIP: stack_e2e test binary is missing com.apple.security.virtualization entitlement; run ./scripts/run-sandbox-vm-e2e.sh --suite stack"
    );
    false
}

fn stack_e2e_oci_data_dir() -> std::path::PathBuf {
    let data_dir = std::env::var_os("VZ_STACK_E2E_OCI_DATA_DIR")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(std::env::var("HOME").expect("HOME must be set"))
                .join(".vz/oci")
        });
    std::fs::create_dir_all(&data_dir).expect("failed to create stack E2E OCI data directory");
    data_dir
}

/// Bridge the async [`MacosRuntimeBackend`] to the sync [`ContainerRuntime`] trait.
///
/// Uses `MacosRuntimeBackend` (which implements `RuntimeBackend` with contract types)
/// rather than `vz_oci_macos::Runtime` directly, avoiding manual type conversions.
#[derive(Clone)]
struct OciContainerRuntime {
    backend: MacosRuntimeBackend,
    handle: tokio::runtime::Handle,
    data_dir: std::path::PathBuf,
}

impl OciContainerRuntime {
    fn new(data_dir: &Path) -> Self {
        let config = RuntimeConfig {
            data_dir: data_dir.to_path_buf(),
            require_exact_agent_version: false,
            agent_ready_timeout: Duration::from_secs(15),
            exec_timeout: Duration::from_secs(30),
            ..RuntimeConfig::default()
        };
        let runtime = Runtime::new(config);
        Self::from_runtime(runtime, data_dir)
    }

    fn from_runtime(runtime: Runtime, data_dir: &Path) -> Self {
        Self {
            backend: MacosRuntimeBackend::new(runtime),
            handle: tokio::runtime::Handle::current(),
            data_dir: data_dir.to_path_buf(),
        }
    }

    /// Exec with full stdout/stderr capture (bypasses ContainerRuntime trait).
    /// Returns `(exit_code, stdout, stderr)`.
    fn exec_with_output(&self, container_id: &str, cmd: Vec<String>) -> (i32, String, String) {
        self.try_exec_with_output(container_id, cmd)
            .unwrap_or_else(|error| panic!("exec in container '{container_id}' failed: {error}"))
    }

    fn try_exec_with_output(
        &self,
        container_id: &str,
        cmd: Vec<String>,
    ) -> Result<(i32, String, String), String> {
        tokio::task::block_in_place(|| {
            let out = self
                .handle
                .block_on(self.backend.exec_container(
                    container_id,
                    ExecConfig {
                        cmd,
                        timeout: Some(Duration::from_secs(30)),
                        ..ExecConfig::default()
                    },
                ))
                .map_err(|error| error.to_string())?;
            Ok((out.exit_code, out.stdout, out.stderr))
        })
    }

    fn save_shared_vm_snapshot(
        &self,
        stack_id: &str,
        snapshot_path: &Path,
    ) -> Result<(), MacosOciError> {
        tokio::task::block_in_place(|| {
            self.handle.block_on(
                self.backend
                    .inner()
                    .save_shared_vm_snapshot(stack_id, snapshot_path),
            )
        })
    }

    fn restore_shared_vm_snapshot(
        &self,
        stack_id: &str,
        snapshot_path: &Path,
    ) -> Result<(), MacosOciError> {
        tokio::task::block_in_place(|| {
            self.handle.block_on(
                self.backend
                    .inner()
                    .restore_shared_vm_snapshot(stack_id, snapshot_path),
            )
        })
    }

    fn try_stack_guest_generation_evidence(
        &self,
        stack_id: &str,
        container_id: &str,
    ) -> Result<serde_json::Value, String> {
        tokio::task::block_in_place(|| {
            self.handle.block_on(try_stack_guest_generation_evidence(
                self.backend.inner(),
                stack_id,
                container_id,
            ))
        })
    }

    fn exec_in_shared_vm(
        &self,
        stack_id: &str,
        command: &str,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<(i32, String, String), StackError> {
        tokio::task::block_in_place(|| {
            let out = self
                .handle
                .block_on(self.backend.inner().exec_in_shared_vm(
                    stack_id,
                    command.to_string(),
                    args,
                    timeout,
                ))
                .map_err(|e| StackError::Network(format!("exec_in_shared_vm failed: {e}")))?;
            Ok((out.exit_code, out.stdout, out.stderr))
        })
    }

    fn lifecycle_diagnostics(&self) -> RuntimeLifecycleDiagnostics {
        self.try_lifecycle_diagnostics()
            .expect("lifecycle diagnostics should be available")
    }

    fn try_lifecycle_diagnostics(&self) -> Result<RuntimeLifecycleDiagnostics, String> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.inner().lifecycle_diagnostics())
                .map_err(|error| error.to_string())
        })
    }

    fn tracked_container_ids(&self) -> Vec<String> {
        self.try_tracked_container_ids()
            .expect("container metadata should be readable")
    }

    fn try_tracked_container_ids(&self) -> Result<Vec<String>, String> {
        let mut ids = self
            .backend
            .inner()
            .list_containers()
            .map_err(|error| error.to_string())?
            .into_iter()
            .map(|container| container.id)
            .collect::<Vec<_>>();
        ids.sort();
        Ok(ids)
    }
}

fn lifecycle_inventory(diagnostics: &RuntimeLifecycleDiagnostics) -> serde_json::Value {
    let mut generations = diagnostics
        .generations
        .iter()
        .map(|generation| {
            serde_json::json!({
                "container_id": generation.container_id,
                "generation": generation.generation.0,
                "reserved": generation.reserved,
                "owner_pid": generation.owner_pid,
                "scope": serde_json::to_value(&generation.scope)
                    .expect("generation scope should serialize"),
                "quarantined": generation.quarantined,
                // An unreserved generation has no active owner even when its
                // historical owner PID is this still-running test process.
                "owner_alive": generation.reserved && generation.owner_alive,
            })
        })
        .collect::<Vec<_>>();
    generations.sort_by(|left, right| {
        left["container_id"]
            .as_str()
            .cmp(&right["container_id"].as_str())
    });

    serde_json::json!({
        "generations": generations,
        "container_lock_slots": diagnostics.container_lock_slots,
        "stack_lock_slots": diagnostics.stack_lock_slots,
        "vm_handles": diagnostics.vm_handles,
        "vm_handle_ids": diagnostics.vm_handle_ids,
        "stack_vms": diagnostics.stack_vms,
        "stack_vm_ids": diagnostics.stack_vm_ids,
        "container_routes": diagnostics.container_routes,
        "container_route_pairs": diagnostics.container_route_pairs,
        "stack_port_forwards": diagnostics.stack_port_forwards,
        "stack_port_forward_ids": diagnostics.stack_port_forward_ids,
        "exec_bindings": diagnostics.exec_bindings,
        "active_lifecycles": diagnostics.active_lifecycles,
        "exec_sessions": diagnostics.exec_sessions,
        "setup_restore_entries": diagnostics.setup_restore_entries,
        "overlay_cleanup_pending": diagnostics.overlay_cleanup_pending,
        "rootfs_directories": diagnostics.rootfs_directories,
    })
}

fn write_stack_teardown_evidence(evidence: &serde_json::Value) {
    let Some(path) = std::env::var_os("VZ_STACK_TEARDOWN_EVIDENCE").map(std::path::PathBuf::from)
    else {
        return;
    };
    let temporary = path.with_extension("json.tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec_pretty(evidence).expect("stack teardown evidence should serialize"),
    )
    .expect("stack teardown evidence should be writable");
    std::fs::rename(&temporary, &path).expect("stack teardown evidence should publish atomically");
}

fn write_stack_container_ownership_evidence(evidence: &serde_json::Value) {
    let Some(path) =
        std::env::var_os("VZ_STACK_CONTAINER_OWNERSHIP_EVIDENCE").map(std::path::PathBuf::from)
    else {
        return;
    };
    let temporary = path.with_extension("json.tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec_pretty(evidence)
            .expect("stack container-ownership evidence should serialize"),
    )
    .expect("stack container-ownership evidence should be writable");
    std::fs::rename(&temporary, &path)
        .expect("stack container-ownership evidence should publish atomically");
}

#[allow(clippy::expect_used)]
fn write_environment_lifecycle_evidence(evidence: &serde_json::Value) {
    let Some(path) =
        std::env::var_os("VZ_ENVIRONMENT_LIFECYCLE_EVIDENCE").map(std::path::PathBuf::from)
    else {
        return;
    };
    let temporary = path.with_extension("json.tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec_pretty(evidence)
            .expect("Environment lifecycle evidence should serialize"),
    )
    .expect("Environment lifecycle evidence should be writable");
    std::fs::rename(&temporary, &path)
        .expect("Environment lifecycle evidence should publish atomically");
}

#[allow(clippy::expect_used)]
fn environment_lifecycle_plan_digest(operation: &EnvironmentLifecycleOperation) -> String {
    let machine_steps = operation
        .machine_steps
        .iter()
        .map(|step| {
            serde_json::json!({
                "machine_id": step.machine_id,
                "initial_state": step.initial_state,
                "target_state": step.target_state,
                "expected_incarnation": step.expected_incarnation,
            })
        })
        .collect::<Vec<_>>();
    let cleanup_ownership = operation
        .cleanup_steps
        .iter()
        .map(|step| &step.ownership)
        .collect::<Vec<_>>();
    let plan = serde_json::json!({
        "kind": operation.kind,
        "generation": operation.generation,
        "definition_digest": operation.definition_digest,
        "machine_steps": machine_steps,
        "cleanup_ownership": cleanup_ownership,
    });
    format!(
        "sha256:{:x}",
        Sha256::digest(serde_json::to_vec(&plan).expect("lifecycle plan should serialize"))
    )
}

fn environment_lifecycle_sha256(value: &str) -> String {
    format!("sha256:{:x}", Sha256::digest(value.as_bytes()))
}

fn environment_lifecycle_operation_evidence(
    label: &str,
    operation: &EnvironmentLifecycleOperation,
) -> serde_json::Value {
    serde_json::json!({
        "label": label,
        "kind": match operation.kind {
            EnvironmentLifecycleKind::Up => "up",
            EnvironmentLifecycleKind::Stop => "stop",
            EnvironmentLifecycleKind::Delete => "delete",
        },
        "operation_id": operation.operation_id,
        "generation": operation.generation,
        "request_id": operation.request_id,
        "idempotency_key": operation.idempotency_key,
        "request_hash": operation.request_hash,
        "definition_digest": operation.definition_digest,
        "plan_digest": environment_lifecycle_plan_digest(operation),
        "status": match operation.status {
            EnvironmentLifecycleStatus::Succeeded => "succeeded",
            EnvironmentLifecycleStatus::Failed => "failed",
            EnvironmentLifecycleStatus::Superseded => "superseded",
            EnvironmentLifecycleStatus::Planned => "planned",
            EnvironmentLifecycleStatus::Running => "running",
            EnvironmentLifecycleStatus::Blocked => "blocked",
        },
    })
}

#[allow(clippy::expect_used)]
fn environment_lifecycle_machine_ack(
    operation: &EnvironmentLifecycleOperation,
    resulting_incarnation: Option<MachineIncarnation>,
) -> MachineLifecycleStepAcknowledgement {
    let step = operation
        .machine_steps
        .first()
        .expect("single-Machine lifecycle plan should have one step");
    MachineLifecycleStepAcknowledgement {
        operation_id: operation.operation_id.clone(),
        generation: operation.generation,
        machine_id: step.machine_id.clone(),
        initial_state: step.initial_state,
        target_state: step.target_state,
        expected_incarnation: step.expected_incarnation.clone(),
        resulting_incarnation,
        result: LifecycleStepResult::Succeeded,
    }
}

#[allow(clippy::expect_used)]
fn environment_lifecycle_environment(
    store: &StateStore,
    project_id: &ProjectId,
    environment_id: &EnvironmentId,
) -> EnvironmentInstance {
    store
        .load_project_state(project_id.as_str())
        .expect("project state should load")
        .expect("project state should exist")
        .environments
        .into_iter()
        .find(|environment| environment.environment_id == *environment_id)
        .unwrap_or_else(|| panic!("Environment `{environment_id}` should exist"))
}

#[allow(clippy::expect_used)]
fn environment_lifecycle_guest_exec(
    runtime: &OciContainerRuntime,
    backend_key: &str,
    script: &str,
) -> String {
    let (exit_code, stdout, stderr) = runtime
        .exec_in_shared_vm(
            backend_key,
            "/bin/busybox",
            vec!["sh".to_string(), "-c".to_string(), script.to_string()],
            Duration::from_secs(30),
        )
        .expect("shared-VM lifecycle probe should execute");
    assert_eq!(
        exit_code, 0,
        "shared-VM lifecycle probe failed: stdout={stdout}, stderr={stderr}"
    );
    stdout.trim().to_string()
}

struct EnvironmentLifecyclePhysicalCleanup {
    runtime: OciContainerRuntime,
    backend_keys: Vec<String>,
    disk_paths: Vec<std::path::PathBuf>,
}

#[allow(clippy::print_stderr)]
impl Drop for EnvironmentLifecyclePhysicalCleanup {
    fn drop(&mut self) {
        for backend_key in &self.backend_keys {
            if self.runtime.has_sandbox(backend_key) {
                let _ = self.runtime.shutdown_sandbox(backend_key);
            }
        }
        for disk_path in &self.disk_paths {
            match std::fs::remove_file(disk_path) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => eprintln!(
                    "best-effort Environment lifecycle disk cleanup failed for {}: {error}",
                    disk_path.display()
                ),
            }
        }
    }
}

fn ownership_json(ownership: &ContainerGenerationOwnership) -> serde_json::Value {
    serde_json::json!({
        "container_id": ownership.container_id,
        "generation": ownership.generation,
        "stack_id": ownership.stack_id,
        "scope": serde_json::to_value(&ownership.scope)
            .expect("generation ownership scope should serialize"),
    })
}

fn stack_ownership_build_identity() -> serde_json::Value {
    let profile = std::env::var("VZ_STACK_OWNERSHIP_BUILD_PROFILE")
        .expect("strict ownership harness must provide the build profile");
    assert_eq!(
        profile, "release",
        "stack ownership evidence cannot be emitted by a non-release build"
    );
    let test_binary_sha256 = std::env::var("VZ_STACK_OWNERSHIP_TEST_BINARY_SHA256")
        .expect("strict ownership harness must provide the test-binary digest");
    assert_eq!(
        test_binary_sha256.len(),
        64,
        "test-binary digest must be lowercase SHA-256"
    );
    assert!(
        test_binary_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)),
        "test-binary digest must be lowercase SHA-256"
    );
    serde_json::json!({
        "profile": profile,
        "test_binary_sha256": test_binary_sha256,
    })
}

async fn try_stack_guest_generation_evidence(
    runtime: &Runtime,
    stack_id: &str,
    container_id: &str,
) -> Result<serde_json::Value, String> {
    let script = format!(
        r#"state=$(/run/vz-oci/bin/youki --root /run/vz-oci/state state {container_id}) || exit 1
pid=$(printf '%s\n' "$state" | sed -n 's/.*"pid"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p' | head -n1)
test -n "$pid" || exit 2
start=$(awk '{{print $22}}' /proc/$pid/stat)
cgroup_path=$(sed -n 's/^[^:]*:[^:]*://p' /proc/$pid/cgroup | head -n1)
owner=$(tr '\000' '\n' < /proc/$pid/environ | sed -n 's/^VZ_E2E_OWNER=//p' | head -n1)
printf '{{"owner":"%s","boot_id":"%s","guest_init_pid":%s,"start_time":"%s","cgroup_path":"%s","cgroup_identity":"%s","mnt_identity":"%s","net_identity":"%s","pid_identity":"%s","ipc_identity":"%s","uts_identity":"%s","root_identity":"%s"}}\n' \
  "$owner" "$(cat /proc/sys/kernel/random/boot_id)" "$pid" "$start" "$cgroup_path" \
  "$(stat -Lc '%d:%i' /sys/fs/cgroup$cgroup_path)" "$(stat -Lc '%d:%i' /proc/$pid/ns/mnt)" \
  "$(stat -Lc '%d:%i' /proc/$pid/ns/net)" "$(stat -Lc '%d:%i' /proc/$pid/ns/pid)" \
  "$(stat -Lc '%d:%i' /proc/$pid/ns/ipc)" "$(stat -Lc '%d:%i' /proc/$pid/ns/uts)" \
  "$(stat -Lc '%d:%i' /proc/$pid/root)""#
    );
    let output = runtime
        .exec_in_shared_vm(
            stack_id,
            "/bin/sh".to_string(),
            vec!["-c".to_string(), script],
            Duration::from_secs(15),
        )
        .await
        .map_err(|error| format!("guest generation probe failed: {error}"))?;
    if output.exit_code != 0 {
        return Err(format!(
            "guest generation probe exited {}: stdout={:?}, stderr={:?}",
            output.exit_code, output.stdout, output.stderr
        ));
    }
    serde_json::from_str(output.stdout.trim()).map_err(|error| {
        format!(
            "guest generation probe was not JSON: {error}; stdout={}",
            output.stdout
        )
    })
}

async fn stack_guest_generation_evidence(
    runtime: &Runtime,
    stack_id: &str,
    container_id: &str,
) -> serde_json::Value {
    try_stack_guest_generation_evidence(runtime, stack_id, container_id)
        .await
        .unwrap_or_else(|error| panic!("guest generation probe failed: {error}"))
}

#[derive(Debug, Default)]
struct StackOwnershipFaultState {
    inject_stack_id: Option<String>,
    injected: bool,
    injected_ownership: Option<ContainerGenerationOwnership>,
    failed_cgroup_path: Option<String>,
    cleanup_operations: Vec<serde_json::Value>,
    after_remove_before_recreate: Option<serde_json::Value>,
    unowned_failures: Vec<(String, MachineErrorCode)>,
    successful_ownership: Vec<ContainerGenerationOwnership>,
}

#[derive(Clone)]
struct StackOwnershipE2eRuntime {
    inner: OciContainerRuntime,
    faults: Arc<Mutex<StackOwnershipFaultState>>,
}

impl StackOwnershipE2eRuntime {
    fn new(inner: OciContainerRuntime) -> Self {
        Self {
            inner,
            faults: Arc::new(Mutex::new(StackOwnershipFaultState::default())),
        }
    }

    fn inject_once_after_publication(&self, stack_id: &str) {
        let mut faults = self.faults.lock().unwrap();
        faults.inject_stack_id = Some(stack_id.to_string());
        faults.injected = false;
        faults.injected_ownership = None;
    }

    fn set_failed_cgroup_path(&self, cgroup_path: &str) {
        self.faults.lock().unwrap().failed_cgroup_path = Some(cgroup_path.to_string());
    }

    fn injected_ownership(&self) -> ContainerGenerationOwnership {
        self.faults
            .lock()
            .unwrap()
            .injected_ownership
            .clone()
            .expect("owned fault did not retain the runtime-issued ownership proof")
    }

    fn cleanup_operations(&self) -> Vec<serde_json::Value> {
        self.faults.lock().unwrap().cleanup_operations.clone()
    }

    fn cleanup_checkpoint(&self) -> serde_json::Value {
        self.faults
            .lock()
            .unwrap()
            .after_remove_before_recreate
            .clone()
            .expect("generation cleanup omitted its pre-recreate checkpoint")
    }

    fn unowned_failure_code(&self, stack_id: &str) -> MachineErrorCode {
        self.faults
            .lock()
            .unwrap()
            .unowned_failures
            .iter()
            .rev()
            .find_map(|(observed_stack, code)| (observed_stack == stack_id).then_some(*code))
            .unwrap_or_else(|| {
                panic!("stack '{stack_id}' did not record an unowned create failure")
            })
    }

    fn latest_successful_ownership(&self, stack_id: &str) -> ContainerGenerationOwnership {
        self.faults
            .lock()
            .unwrap()
            .successful_ownership
            .iter()
            .rev()
            .find(|ownership| ownership.stack_id == stack_id)
            .cloned()
            .unwrap_or_else(|| panic!("stack '{stack_id}' did not return generation ownership"))
    }
}

impl ContainerRuntime for OciContainerRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.pull(image))
                .map_err(|e| StackError::Network(format!("pull failed: {e}")))
        })
    }

    fn create(&self, image: &str, config: RunConfig) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.create_container(image, config))
                .map_err(|e| StackError::Network(format!("create failed: {e}")))
        })
    }

    fn stop(
        &self,
        container_id: &str,
        signal: Option<&str>,
        grace_period: Option<std::time::Duration>,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .stop_container(container_id, false, signal, grace_period),
                )
                .map(|_| ())
                .map_err(|e| StackError::Network(format!("stop failed: {e}")))
        })
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.remove_container(container_id))
                .map_err(|e| StackError::Network(format!("remove failed: {e}")))
        })
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        tokio::task::block_in_place(|| {
            let exec_config = ExecConfig {
                cmd: command.to_vec(),
                ..ExecConfig::default()
            };
            self.handle
                .block_on(self.backend.exec_container(container_id, exec_config))
                .map(|output| output.exit_code)
                .map_err(|e| StackError::Network(format!("exec failed: {e}")))
        })
    }

    fn create_sandbox(
        &self,
        sandbox_id: &str,
        ports: Vec<PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.boot_shared_vm(sandbox_id, ports, resources))
                .map_err(|e| StackError::Network(format!("create_sandbox failed: {e}")))
        })
    }

    fn create_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<String, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .create_container_in_stack(sandbox_id, image, config),
                )
                .map_err(|e| StackError::Network(format!("create_in_sandbox failed: {e}")))
        })
    }

    fn create_in_sandbox_owned(
        &self,
        sandbox_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<ContainerCreateReceipt, OwnedCreateError<StackError>> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(
                    self.backend
                        .create_container_in_stack_owned_legacy(sandbox_id, image, config),
                )
                .map_err(|failure| failure.map_error(StackError::from))
        })
    }

    fn cleanup_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
    ) -> Result<GenerationCleanupOutcome, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.cleanup_container_generation(ownership))
                .map_err(StackError::from)
        })
    }

    fn stop_and_remove_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
        signal: Option<&str>,
        grace_period: Option<Duration>,
    ) -> Result<GenerationCleanupOutcome, StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.stop_and_remove_container_generation(
                    ownership,
                    signal.map(str::to_string),
                    grace_period,
                ))
                .map_err(StackError::from)
        })
    }

    fn setup_sandbox_network(
        &self,
        sandbox_id: &str,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.network_setup(sandbox_id, services))
                .map_err(|e| StackError::Network(format!("setup_sandbox_network failed: {e}")))
        })
    }

    fn teardown_sandbox_network(
        &self,
        sandbox_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.network_teardown(sandbox_id, service_names))
                .map_err(|e| StackError::Network(format!("teardown_sandbox_network failed: {e}")))
        })
    }

    fn shutdown_sandbox(&self, sandbox_id: &str) -> Result<(), StackError> {
        tokio::task::block_in_place(|| {
            self.handle
                .block_on(self.backend.shutdown_shared_vm(sandbox_id))
                .map_err(|e| StackError::Network(format!("shutdown_sandbox failed: {e}")))
        })
    }

    fn has_sandbox(&self, sandbox_id: &str) -> bool {
        self.backend.has_shared_vm(sandbox_id)
    }
}

impl ContainerRuntime for StackOwnershipE2eRuntime {
    fn pull(&self, image: &str) -> Result<String, StackError> {
        self.inner.pull(image)
    }

    fn create(&self, image: &str, config: RunConfig) -> Result<String, StackError> {
        self.inner.create(image, config)
    }

    fn stop(
        &self,
        container_id: &str,
        signal: Option<&str>,
        grace_period: Option<Duration>,
    ) -> Result<(), StackError> {
        self.inner.stop(container_id, signal, grace_period)
    }

    fn remove(&self, container_id: &str) -> Result<(), StackError> {
        self.inner.remove(container_id)
    }

    fn exec(&self, container_id: &str, command: &[String]) -> Result<i32, StackError> {
        self.inner.exec(container_id, command)
    }

    fn create_sandbox(
        &self,
        sandbox_id: &str,
        ports: Vec<PortMapping>,
        resources: vz_runtime_contract::StackResourceHint,
    ) -> Result<(), StackError> {
        self.inner.create_sandbox(sandbox_id, ports, resources)
    }

    fn create_in_sandbox(
        &self,
        sandbox_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<String, StackError> {
        self.inner.create_in_sandbox(sandbox_id, image, config)
    }

    fn create_in_sandbox_owned(
        &self,
        sandbox_id: &str,
        image: &str,
        config: RunConfig,
    ) -> Result<ContainerCreateReceipt, OwnedCreateError<StackError>> {
        let result = self
            .inner
            .create_in_sandbox_owned(sandbox_id, image, config);
        match result {
            Ok(receipt) => {
                if let Some(ownership) = receipt.ownership.clone() {
                    self.faults
                        .lock()
                        .unwrap()
                        .successful_ownership
                        .push(ownership);
                }
                let should_inject = {
                    let faults = self.faults.lock().unwrap();
                    !faults.injected && faults.inject_stack_id.as_deref() == Some(sandbox_id)
                };
                if should_inject {
                    let ownership = receipt.ownership.clone().unwrap_or_else(|| {
                        panic!("macOS runtime published a container without generation ownership")
                    });
                    let mut faults = self.faults.lock().unwrap();
                    faults.injected = true;
                    faults.injected_ownership = Some(ownership.clone());
                    return Err(OwnedCreateError {
                        error: StackError::Network(
                            "injected_post_publication: control-plane acknowledgement lost after runtime publication"
                                .to_string(),
                        ),
                        cleanup: Some(ownership),
                    });
                }
                Ok(receipt)
            }
            Err(failure) => {
                if failure.cleanup.is_none() {
                    self.faults
                        .lock()
                        .unwrap()
                        .unowned_failures
                        .push((sandbox_id.to_string(), failure.error.machine_code()));
                }
                Err(failure)
            }
        }
    }

    fn cleanup_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
    ) -> Result<GenerationCleanupOutcome, StackError> {
        let is_injected_failure =
            self.faults.lock().unwrap().injected_ownership.as_ref() == Some(&ownership);
        if !is_injected_failure {
            return self.inner.cleanup_container_generation(ownership);
        }

        let outcome = self.inner.cleanup_container_generation(ownership.clone())?;
        let outcome_name = match outcome {
            GenerationCleanupOutcome::Removed => "removed",
            GenerationCleanupOutcome::AlreadyAbsent => "already_absent",
        };

        let cgroup_path = self
            .faults
            .lock()
            .unwrap()
            .failed_cgroup_path
            .clone()
            .expect("owned failure did not record its guest cgroup path before cleanup");
        let probe = self
            .inner
            .exec_in_shared_vm(
                &ownership.stack_id,
                "/bin/sh",
                vec![
                    "-c".to_string(),
                    format!(
                        "printf 'overlay='; test -e /run/vz-oci/containers/{id} && echo present || echo absent; \
                         printf 'youki_state='; test -e /run/vz-oci/state/{id} && echo present || echo absent; \
                         printf 'cgroup='; test -e /sys/fs/cgroup{cgroup} && echo present || echo absent",
                        id = ownership.container_id,
                        cgroup = cgroup_path,
                    ),
                ],
                Duration::from_secs(15),
            )
            .expect("failed to inspect generation cleanup before replacement create");
        assert_eq!(probe.0, 0, "generation cleanup probe failed: {}", probe.2);
        let diagnostics = self.inner.lifecycle_diagnostics();
        let checkpoint = serde_json::json!({
            "metadata_absent": !self
                .inner
                .tracked_container_ids()
                .contains(&ownership.container_id),
            "rootfs_absent": !self
                .inner
                .data_dir
                .join("rootfs")
                .join(&ownership.container_id)
                .exists(),
            "guest_overlay_absent": probe.1.contains("overlay=absent"),
            "guest_youki_state_absent": probe.1.contains("youki_state=absent"),
            "guest_cgroup_absent": probe.1.contains("cgroup=absent"),
            "lifecycle": lifecycle_inventory(&diagnostics),
        });

        let mut faults = self.faults.lock().unwrap();
        faults.cleanup_operations.push(serde_json::json!({
            "operation": "cleanup_container_generation",
            "ownership": ownership_json(&ownership),
            "outcome": outcome_name,
        }));
        faults.after_remove_before_recreate = Some(checkpoint);
        Ok(outcome)
    }

    fn stop_and_remove_container_generation(
        &self,
        ownership: ContainerGenerationOwnership,
        signal: Option<&str>,
        grace_period: Option<Duration>,
    ) -> Result<GenerationCleanupOutcome, StackError> {
        let is_injected_failure =
            self.faults.lock().unwrap().injected_ownership.as_ref() == Some(&ownership);
        if is_injected_failure {
            return self.cleanup_container_generation(ownership);
        }
        self.inner
            .stop_and_remove_container_generation(ownership, signal, grace_period)
    }

    fn setup_sandbox_network(
        &self,
        sandbox_id: &str,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), StackError> {
        self.inner.setup_sandbox_network(sandbox_id, services)
    }

    fn teardown_sandbox_network(
        &self,
        sandbox_id: &str,
        service_names: Vec<String>,
    ) -> Result<(), StackError> {
        self.inner
            .teardown_sandbox_network(sandbox_id, service_names)
    }

    fn shutdown_sandbox(&self, sandbox_id: &str) -> Result<(), StackError> {
        self.inner.shutdown_sandbox(sandbox_id)
    }

    fn has_sandbox(&self, sandbox_id: &str) -> bool {
        self.inner.has_sandbox(sandbox_id)
    }
}

fn stack_ownership_spec(
    stack_id: &str,
    service_name: &str,
    owner: &str,
    container_name: Option<&str>,
) -> vz_stack::StackSpec {
    let container_name = container_name
        .map(|name| format!("    container_name: {name}\n"))
        .unwrap_or_default();
    parse_compose(
        &format!(
            r#"services:
  {service_name}:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      VZ_E2E_OWNER: {owner}
{container_name}"#
        ),
        stack_id,
    )
    .unwrap()
}

fn stack_ownership_orchestrator(
    runtime: StackOwnershipE2eRuntime,
    root: &Path,
    stack_id: &str,
) -> StackOrchestrator<StackOwnershipE2eRuntime> {
    let stack_dir = root.join(stack_id);
    std::fs::create_dir_all(&stack_dir).unwrap();
    let db_path = stack_dir.join("state.db");
    let executor = StackExecutor::new(runtime, StateStore::open(&db_path).unwrap(), &stack_dir);
    StackOrchestrator::new(
        executor,
        StateStore::open(&db_path).unwrap(),
        OrchestrationConfig {
            poll_interval: Some(0),
            max_rounds: 1,
            image_policy: ImagePolicy::AllowAll,
        },
    )
}

fn stop_stack_strict(
    orchestrator: &mut StackOrchestrator<StackOwnershipE2eRuntime>,
    stack_id: &str,
) {
    let down = orchestrator
        .run(
            &vz_stack::StackSpec {
                name: stack_id.to_string(),
                services: vec![],
                networks: vec![],
                volumes: vec![],
                secrets: vec![],
                disk_size_mb: None,
            },
            None,
        )
        .unwrap_or_else(|error| panic!("stack '{stack_id}' down failed: {error}"));
    assert!(down.converged, "stack '{stack_id}' down did not converge");
    assert_eq!(down.services_failed, 0, "stack '{stack_id}' down failed");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox(stack_id)
        .unwrap_or_else(|error| panic!("stack '{stack_id}' VM shutdown failed: {error}"));
}

/// Prove that stack-generated IDs are namespace-stable and that only an exact
/// runtime-issued generation token authorizes failed-create cleanup.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_container_generation_ownership() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    const IMAGE: &str = "alpine:latest";
    let oci_data = stack_e2e_oci_data_dir();
    let raw_runtime = Runtime::new(RuntimeConfig {
        data_dir: oci_data.clone(),
        require_exact_agent_version: false,
        agent_ready_timeout: Duration::from_secs(15),
        exec_timeout: Duration::from_secs(30),
        ..RuntimeConfig::default()
    });
    raw_runtime.pull(IMAGE).await.unwrap();
    let bridge = StackOwnershipE2eRuntime::new(OciContainerRuntime::from_runtime(
        raw_runtime.clone(),
        &oci_data,
    ));
    let tmp = tempfile::tempdir().unwrap();

    let mut evidence = serde_json::json!({
        "schema_version": 3,
        "scenario": "stack-container-ownership",
        "build_identity": stack_ownership_build_identity(),
        "scope_identity": {
            "kind": "synthetic_legacy_compatibility",
            "topology_authoritative": false,
        },
        "concurrent_same_service": serde_json::Value::Null,
        "owned_failure": serde_json::Value::Null,
        "foreign_collision": serde_json::Value::Null,
        "final": serde_json::Value::Null,
    });
    write_stack_container_ownership_evidence(&evidence);

    // Boot both stack VMs before installing the admission barrier. VM startup
    // is intentionally serialized by the macOS runtime, while container
    // admission is the ownership boundary this scenario must exercise
    // concurrently. Pre-booting also ensures the barrier cannot deadlock with
    // that unrelated VM-start serialization.
    bridge
        .create_sandbox(
            "same-a",
            vec![],
            vz_runtime_contract::StackResourceHint::default(),
        )
        .expect("failed to pre-boot same-a sandbox");
    bridge
        .create_sandbox(
            "same-b",
            vec![],
            vz_runtime_contract::StackResourceHint::default(),
        )
        .expect("failed to pre-boot same-b sandbox");
    for stack_id in ["same-a", "same-b"] {
        bridge
            .setup_sandbox_network(
                stack_id,
                vec![NetworkServiceConfig {
                    name: "db".to_string(),
                    addr: "172.20.0.2/24".to_string(),
                    network_name: "default".to_string(),
                }],
            )
            .unwrap_or_else(|error| panic!("failed to preconfigure {stack_id} network: {error}"));
    }

    // Both stack container creates must reach runtime admission before either
    // is released. Distinct generated IDs therefore exercise real concurrent
    // ownership in one Runtime rather than two independent fixture stores.
    let spec_a = stack_ownership_spec("same-a", "db", "same-a-db", None);
    let spec_b = stack_ownership_spec("same-b", "db", "same-b-db", None);
    let orchestrator_a = stack_ownership_orchestrator(bridge.clone(), tmp.path(), "same-a");
    let orchestrator_b = stack_ownership_orchestrator(bridge.clone(), tmp.path(), "same-b");
    let mut observer = raw_runtime.install_lifecycle_observer();
    let task_a = tokio::task::spawn_blocking(move || {
        let mut orchestrator = orchestrator_a;
        let result = orchestrator.run(&spec_a, None);
        (orchestrator, result)
    });
    let task_b = tokio::task::spawn_blocking(move || {
        let mut orchestrator = orchestrator_b;
        let result = orchestrator.run(&spec_b, None);
        (orchestrator, result)
    });
    let admission_a = tokio::time::timeout(Duration::from_secs(120), observer.recv())
        .await
        .expect("first concurrent stack never reached create admission")
        .expect("lifecycle observer closed before first concurrent create");
    let admission_b = tokio::time::timeout(Duration::from_secs(120), observer.recv())
        .await
        .expect("second concurrent stack never reached create admission")
        .expect("lifecycle observer closed before second concurrent create");
    assert_eq!(
        admission_a.kind(),
        RuntimeLifecycleAdmissionKind::CreateBeforeReservation
    );
    assert_eq!(
        admission_b.kind(),
        RuntimeLifecycleAdmissionKind::CreateBeforeReservation
    );
    let mut barrier_ids = vec![
        admission_a.container_id().to_string(),
        admission_b.container_id().to_string(),
    ];
    barrier_ids.sort();
    barrier_ids.dedup();
    assert_eq!(barrier_ids.len(), 2, "generated IDs collided at admission");
    drop(observer);
    admission_a.resume();
    admission_b.resume();

    let (mut orchestrator_a, result_a) = tokio::time::timeout(Duration::from_secs(180), task_a)
        .await
        .expect("same-a create timed out")
        .expect("same-a orchestration task panicked");
    let (mut orchestrator_b, result_b) = tokio::time::timeout(Duration::from_secs(180), task_b)
        .await
        .expect("same-b create timed out")
        .expect("same-b orchestration task panicked");
    let result_a = result_a.unwrap();
    let result_b = result_b.unwrap();
    assert!(result_a.converged && result_b.converged);
    assert_eq!(result_a.services_failed + result_b.services_failed, 0);

    let same_a = bridge.latest_successful_ownership("same-a");
    let same_b = bridge.latest_successful_ownership("same-b");
    assert_ne!(same_a.container_id, same_b.container_id);
    let same_a_guest =
        stack_guest_generation_evidence(&raw_runtime, "same-a", &same_a.container_id).await;
    let same_b_guest =
        stack_guest_generation_evidence(&raw_runtime, "same-b", &same_b.container_id).await;
    let same_lifecycle = raw_runtime.lifecycle_diagnostics().await.unwrap();
    evidence["concurrent_same_service"] = serde_json::json!({
        "service_name": "db",
        "barrier": {
            "kind": "create_before_reservation",
            "both_reached_before_release": true,
            "container_ids": barrier_ids,
        },
        "stacks": [
            {
                "stack_id": "same-a",
                "container_id": same_a.container_id,
                "ownership": ownership_json(&same_a),
                "guest": same_a_guest,
            },
            {
                "stack_id": "same-b",
                "container_id": same_b.container_id,
                "ownership": ownership_json(&same_b),
                "guest": same_b_guest,
            }
        ],
        "lifecycle": lifecycle_inventory(&same_lifecycle),
    });
    write_stack_container_ownership_evidence(&evidence);
    stop_stack_strict(&mut orchestrator_a, "same-a");
    stop_stack_strict(&mut orchestrator_b, "same-b");

    // Inject a lost acknowledgement after the real backend has published the
    // running generation. The returned ownership proof must survive the failed
    // observed state and authorize exactly one cleanup before replacement.
    bridge.inject_once_after_publication("owned");
    let spec_owned_a = stack_ownership_spec("owned", "worker", "owned-generation-a", None);
    let mut owned_orchestrator = stack_ownership_orchestrator(bridge.clone(), tmp.path(), "owned");
    let first_owned = owned_orchestrator.run(&spec_owned_a, None).unwrap();
    assert_eq!(first_owned.services_failed, 1);
    let failure_token = bridge.injected_ownership();
    let failed_state = owned_orchestrator
        .executor()
        .store()
        .load_observed_state("owned")
        .unwrap();
    let observed_token = failed_state
        .iter()
        .find(|state| state.replica.service_name == "worker")
        .and_then(|state| state.failed_create_ownership.clone())
        .expect("owned post-publication failure lost its cleanup proof");
    assert_eq!(observed_token, failure_token);
    let failed_guest =
        stack_guest_generation_evidence(&raw_runtime, "owned", &failure_token.container_id).await;
    bridge.set_failed_cgroup_path(
        failed_guest["cgroup_path"]
            .as_str()
            .expect("failed generation omitted cgroup path"),
    );
    let failed_lifecycle = raw_runtime.lifecycle_diagnostics().await.unwrap();

    let spec_owned_b = stack_ownership_spec("owned", "worker", "owned-generation-b", None);
    let replacement_result = owned_orchestrator.run(&spec_owned_b, None).unwrap();
    assert!(replacement_result.converged);
    assert_eq!(replacement_result.services_failed, 0);
    let replacement_token = bridge.latest_successful_ownership("owned");
    assert_eq!(replacement_token.container_id, failure_token.container_id);
    assert!(replacement_token.generation > failure_token.generation);
    let replacement_guest =
        stack_guest_generation_evidence(&raw_runtime, "owned", &replacement_token.container_id)
            .await;
    let replacement_lifecycle = raw_runtime.lifecycle_diagnostics().await.unwrap();
    evidence["owned_failure"] = serde_json::json!({
        "stack_id": "owned",
        "service_name": "worker",
        "injection_point": "after_runtime_publication_before_executor_finalize",
        "injected_error_code": "injected_post_publication",
        "failure_token": ownership_json(&failure_token),
        "observed_token": ownership_json(&observed_token),
        "failed_guest": failed_guest,
        "failed_lifecycle": lifecycle_inventory(&failed_lifecycle),
        "cleanup_operations": bridge.cleanup_operations(),
        "after_remove_before_recreate": bridge.cleanup_checkpoint(),
        "replacement_token": ownership_json(&replacement_token),
        "replacement_guest": replacement_guest,
        "replacement_lifecycle": lifecycle_inventory(&replacement_lifecycle),
    });
    write_stack_container_ownership_evidence(&evidence);
    stop_stack_strict(&mut owned_orchestrator, "owned");

    // A foreign explicit container_name collision carries no authority. The
    // contender must fail closed without invoking generation cleanup or
    // changing the foreign generation, route, or raw guest process identity.
    let owner_spec = stack_ownership_spec(
        "foreign-owner",
        "owner",
        "foreign-owner",
        Some("shared-explicit-id"),
    );
    let mut owner_orchestrator =
        stack_ownership_orchestrator(bridge.clone(), tmp.path(), "foreign-owner");
    let owner_result = owner_orchestrator.run(&owner_spec, None).unwrap();
    assert!(owner_result.converged);
    assert_eq!(owner_result.services_failed, 0);
    let owner_token = bridge.latest_successful_ownership("foreign-owner");
    let owner_before_guest =
        stack_guest_generation_evidence(&raw_runtime, "foreign-owner", &owner_token.container_id)
            .await;
    let before_collision = raw_runtime.lifecycle_diagnostics().await.unwrap();
    let cleanup_count_before_collision = bridge.cleanup_operations().len();

    let contender_spec = stack_ownership_spec(
        "foreign-contender",
        "contender",
        "foreign-contender",
        Some("shared-explicit-id"),
    );
    let mut contender_orchestrator =
        stack_ownership_orchestrator(bridge.clone(), tmp.path(), "foreign-contender");
    let contender_result = contender_orchestrator.run(&contender_spec, None).unwrap();
    assert_eq!(contender_result.services_failed, 1);
    let contender_state = contender_orchestrator
        .executor()
        .store()
        .load_observed_state("foreign-contender")
        .unwrap();
    let contender_cleanup = contender_state
        .iter()
        .find(|state| state.replica.service_name == "contender")
        .and_then(|state| state.failed_create_ownership.clone());
    assert!(contender_cleanup.is_none());
    assert_eq!(
        bridge.unowned_failure_code("foreign-contender"),
        MachineErrorCode::StateConflict
    );
    assert_eq!(
        bridge.cleanup_operations().len(),
        cleanup_count_before_collision,
        "foreign collision invoked generation cleanup"
    );
    let owner_after_guest =
        stack_guest_generation_evidence(&raw_runtime, "foreign-owner", &owner_token.container_id)
            .await;
    assert_eq!(owner_before_guest, owner_after_guest);
    let after_collision = raw_runtime.lifecycle_diagnostics().await.unwrap();
    evidence["foreign_collision"] = serde_json::json!({
        "owner_stack_id": "foreign-owner",
        "contender_stack_id": "foreign-contender",
        "container_id": "shared-explicit-id",
        "owner_token": ownership_json(&owner_token),
        "collision_error_code": MachineErrorCode::StateConflict.as_str(),
        "collision_cleanup": serde_json::Value::Null,
        "contender_observed_cleanup": serde_json::Value::Null,
        "cleanup_operations": [],
        "owner_before_guest": owner_before_guest,
        "owner_after_guest": owner_after_guest,
        "before_lifecycle": lifecycle_inventory(&before_collision),
        "after_lifecycle": lifecycle_inventory(&after_collision),
    });
    write_stack_container_ownership_evidence(&evidence);

    stop_stack_strict(&mut contender_orchestrator, "foreign-contender");
    stop_stack_strict(&mut owner_orchestrator, "foreign-owner");

    let final_diagnostics = raw_runtime.lifecycle_diagnostics().await.unwrap();
    let final_tracked = raw_runtime
        .list_containers()
        .unwrap()
        .into_iter()
        .map(|container| container.id)
        .collect::<Vec<_>>();
    let tested_container_ids = vec![
        same_a.container_id,
        same_b.container_id,
        replacement_token.container_id,
        owner_token.container_id,
    ];
    evidence["final"] = serde_json::json!({
        "tracked_container_ids": final_tracked,
        "tested_container_ids": tested_container_ids,
        "lifecycle": lifecycle_inventory(&final_diagnostics),
    });
    write_stack_container_ownership_evidence(&evidence);

    assert!(raw_runtime.list_containers().unwrap().is_empty());
    assert_eq!(final_diagnostics.vm_handles, 0);
    assert_eq!(final_diagnostics.stack_vms, 0);
    assert_eq!(final_diagnostics.container_routes, 0);
    assert_eq!(final_diagnostics.exec_bindings, 0);
    assert_eq!(final_diagnostics.active_lifecycles, 0);
    assert_eq!(final_diagnostics.exec_sessions, 0);
    assert_eq!(final_diagnostics.setup_restore_entries, 0);
    assert_eq!(final_diagnostics.overlay_cleanup_pending, 0);
    assert_eq!(final_diagnostics.rootfs_directories, 0);
    assert!(
        final_diagnostics
            .generations
            .iter()
            .all(|entry| !entry.reserved)
    );
}

#[test]
fn stack_error_machine_code_normalization() {
    assert_eq!(
        StackError::InvalidSpec("bad config".to_string()).machine_code(),
        MachineErrorCode::ValidationError
    );
    assert_eq!(
        StackError::Network("unsupported_operation: operation=exec".to_string()).machine_code(),
        MachineErrorCode::UnsupportedOperation
    );
    assert_eq!(
        StackError::Network("image not found".to_string()).machine_code(),
        MachineErrorCode::NotFound
    );
    assert_eq!(
        StackError::Network("request timed out".to_string()).machine_code(),
        MachineErrorCode::Timeout
    );
    assert_eq!(
        StackError::Network("bridge unavailable".to_string()).machine_code(),
        MachineErrorCode::BackendUnavailable
    );
    assert_eq!(
        StackError::Machine {
            code: MachineErrorCode::PolicyDenied,
            message: "denied".to_string(),
        }
        .machine_code(),
        MachineErrorCode::PolicyDenied
    );
}

#[test]
fn compose_unsupported_error_shape_is_stable() {
    let err = StackError::ComposeUnsupportedFeature {
        feature: "services.web.networks.frontend.aliases".to_string(),
        reason: "network attachment options are not supported".to_string(),
    };
    assert_eq!(err.machine_code(), MachineErrorCode::UnsupportedOperation);
    let message = err.to_string();
    assert!(message.starts_with("unsupported_operation:"));
    assert!(message.contains("surface=compose"));
}

#[test]
fn contract_terminal_state_and_lease_exec_gating_rules() {
    let mut sandbox = Sandbox {
        sandbox_id: "sbx-test".to_string(),
        backend: SandboxBackend::MacosVz,
        spec: SandboxSpec::default(),
        state: SandboxState::Ready,
        created_at: 1,
        updated_at: 1,
        labels: BTreeMap::new(),
    };
    sandbox.ensure_can_open_lease().unwrap();
    sandbox.transition_to(SandboxState::Draining).unwrap();
    sandbox.transition_to(SandboxState::Terminated).unwrap();
    assert!(matches!(
        sandbox.ensure_can_open_lease(),
        Err(ContractInvariantError::LeaseRequiresReadySandbox { .. })
    ));

    let mut lease = Lease {
        lease_id: "lease-test".to_string(),
        sandbox_id: "sbx-test".to_string(),
        ttl_secs: 60,
        last_heartbeat_at: 1,
        state: LeaseState::Active,
    };
    lease.ensure_can_submit_work("create_container").unwrap();
    lease.transition_to(LeaseState::Closed).unwrap();
    assert!(matches!(
        lease.ensure_can_submit_work("exec_container"),
        Err(ContractInvariantError::WorkRequiresActiveLease { .. })
    ));

    let mut container = Container {
        container_id: "ctr-test".to_string(),
        sandbox_id: "sbx-test".to_string(),
        image_digest: "sha256:abc".to_string(),
        container_spec: Default::default(),
        state: ContainerState::Running,
        created_at: 1,
        started_at: Some(1),
        ended_at: None,
    };
    container.ensure_can_exec().unwrap();
    container.transition_to(ContainerState::Exited).unwrap();
    assert!(matches!(
        container.ensure_can_exec(),
        Err(ContractInvariantError::ExecRequiresRunningContainer { .. })
    ));
}

/// Parse a 2-service compose YAML, reconcile, execute through real OCI runtime,
/// and verify containers reach Running state.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn full_pipeline_two_services() {
    if !require_virtualization_entitlement() {
        return;
    }
    let yaml = r#"
services:
  worker:
    image: alpine:latest
    command: ["sleep", "300"]

  web:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - worker
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    // Parse compose.
    let spec = parse_compose(yaml, "e2e-test").unwrap();
    assert_eq!(spec.services.len(), 2);

    // Execute through the real OCI runtime.
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, exec_store, tmp.path());

    // Dependency-aware reconciler behavior can require multiple rounds:
    // first create `worker`, then create dependent `web`.
    for round in 1..=3 {
        let health = HashMap::new();
        let result = apply(&spec, executor.store(), &health).unwrap();

        assert!(
            !result.actions.is_empty(),
            "expected at least one action in round {round}"
        );
        if round == 1 {
            assert!(
                matches!(&result.actions[0], Action::ServiceCreate { target } if target.service_name == "worker"),
                "first round should prioritize worker dependency, got: {:?}",
                result.actions[0]
            );
        }

        let exec_result = executor.execute(&spec, &result.actions).unwrap();
        assert_eq!(
            exec_result.failed, 0,
            "no actions should fail in round {round}: {:?}",
            exec_result.errors
        );

        let observed = executor.store().load_observed_state("e2e-test").unwrap();
        let ready = observed
            .iter()
            .filter(|service| service.container_id.is_some())
            .count();
        if ready >= 2 {
            break;
        }

        assert!(
            round < 3,
            "services did not converge after 3 reconcile rounds"
        );
    }

    // Verify observed state: both services running.
    let observed = executor.store().load_observed_state("e2e-test").unwrap();
    for name in &["worker", "web"] {
        let svc = observed
            .iter()
            .find(|o| o.replica.service_name == *name)
            .unwrap_or_else(|| panic!("service '{name}' should be in observed state"));
        assert!(
            svc.container_id.is_some(),
            "service '{name}' should have a container ID"
        );
    }

    // Verify events were emitted.
    let events = executor.store().load_events("e2e-test").unwrap();
    let creating_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceCreating { .. }))
        .count();
    let ready_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceReady { .. }))
        .count();
    assert!(
        creating_count >= 2,
        "should have at least 2 creating events"
    );
    assert!(ready_count >= 2, "should have at least 2 ready events");

    // Exec a command inside the worker container to prove it's alive.
    let worker_id = observed
        .iter()
        .find(|o| o.replica.service_name == "worker")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();
    let exit_code = executor
        .runtime()
        .exec(worker_id, &["echo".into(), "stack-e2e".into()])
        .unwrap();
    assert_eq!(exit_code, 0, "exec inside worker should succeed");

    // Teardown: stop and remove both containers.
    let down_actions: Vec<Action> = spec
        .services
        .iter()
        .map(|s| Action::ServiceRemove {
            target: vz_stack::ServiceReplicaKey::first(s.name.clone()).unwrap(),
        })
        .collect();
    let down_result = executor.execute(&spec, &down_actions).unwrap();
    assert_eq!(
        down_result.failed, 0,
        "teardown should succeed: {:?}",
        down_result.errors
    );
    assert!(down_result.all_succeeded());
    executor
        .runtime()
        .shutdown_sandbox("e2e-test")
        .expect("e2e-test shared VM shutdown should succeed");
}

/// Parse and reconcile, then execute a single service and verify exec works.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn single_service_exec() {
    if !require_virtualization_entitlement() {
        return;
    }
    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      MY_VAR: "hello-from-stack"
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec = parse_compose(yaml, "exec-test").unwrap();
    let store = StateStore::open(&db_path).unwrap();
    let result = apply(&spec, &store, &HashMap::new()).unwrap();

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert_eq!(exec_result.failed, 0);

    // Exec into the container.
    let observed = executor.store().load_observed_state("exec-test").unwrap();
    let app = observed
        .iter()
        .find(|o| o.replica.service_name == "app")
        .unwrap();
    let container_id = app.container_id.as_ref().unwrap();

    let exit_code = executor
        .runtime()
        .exec(container_id, &["echo".into(), "alive".into()])
        .unwrap();
    assert_eq!(exit_code, 0);

    // Cleanup.
    let down = vec![Action::ServiceRemove {
        target: vz_stack::ServiceReplicaKey::first("app").unwrap(),
    }];
    let down_result = executor.execute(&spec, &down).unwrap();
    assert_eq!(
        down_result.failed, 0,
        "teardown should succeed: {:?}",
        down_result.errors
    );
    assert!(down_result.all_succeeded());
    executor
        .runtime()
        .shutdown_sandbox("exec-test")
        .expect("exec-test shared VM shutdown should succeed");
}

/// Exercise the orchestration loop: deploy 2 services through the
/// StackOrchestrator and verify convergence with real OCI containers.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn orchestrator_converges_two_services() {
    if !require_virtualization_entitlement() {
        return;
    }
    let yaml = r#"
services:
  db:
    image: alpine:latest
    command: ["sleep", "300"]

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - db
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec = parse_compose(yaml, "orch-test").unwrap();

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    let result = orchestrator.run(&spec, None).unwrap();

    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 2, "both services should be ready");
    assert_eq!(result.services_failed, 0, "no services should fail");
    assert!(
        result.rounds >= 1,
        "orchestration rounds should be at least 1, got {}",
        result.rounds
    );

    // Verify observed state through the executor's store.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("orch-test")
        .unwrap();
    assert_eq!(observed.len(), 2);
    for name in &["db", "api"] {
        let svc = observed
            .iter()
            .find(|o| o.replica.service_name == *name)
            .unwrap_or_else(|| panic!("service '{name}' should be in observed state"));
        assert!(
            svc.container_id.is_some(),
            "service '{name}' should have a container ID"
        );
    }

    // Teardown through the orchestrator's executor.
    let down_spec = vz_stack::StackSpec {
        name: "orch-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged);
    assert_eq!(down_result.services_failed, 0);
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("orch-test")
        .expect("orch-test shared VM shutdown should succeed");
}

// ── Real service tests ──────────────────────────────────────────

/// Boot real Postgres and Redis services, wait for health checks to pass,
/// then verify functionality via exec.
///
/// This proves vz stack can run real services, not just alpine sleep containers.
/// Postgres is verified with `psql SELECT 1`, Redis with `redis-cli PING`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn real_services_postgres_and_redis() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "app"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 10s

  cache:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 5s
"#;

    // Use persistent data dir for image cache (avoid Docker Hub rate limits).
    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "real-svc").unwrap();
    assert_eq!(spec.services.len(), 2);

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 30,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();

    assert!(
        result.converged,
        "stack should converge: ready={}, failed={}, rounds={}",
        result.services_ready, result.services_failed, result.rounds
    );
    assert_eq!(result.services_ready, 2, "both services should be ready");

    // Get container IDs from observed state.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("real-svc")
        .unwrap();

    let db_container_id = observed
        .iter()
        .find(|o| o.replica.service_name == "db")
        .unwrap_or_else(|| panic!("db should be in observed state"))
        .container_id
        .as_ref()
        .unwrap();
    let cache_container_id = observed
        .iter()
        .find(|o| o.replica.service_name == "cache")
        .unwrap_or_else(|| panic!("cache should be in observed state"))
        .container_id
        .as_ref()
        .unwrap();

    let rt = orchestrator.executor().runtime();

    // Verify Postgres: run SQL query via psql.
    let (exit_code, stdout, _) = rt.exec_with_output(
        db_container_id,
        vec![
            "psql".into(),
            "-U".into(),
            "app".into(),
            "-d".into(),
            "app".into(),
            "-c".into(),
            "SELECT 1".into(),
        ],
    );
    assert_eq!(exit_code, 0, "psql SELECT 1 should succeed");
    assert!(
        stdout.contains('1'),
        "psql output should contain '1': {stdout}"
    );

    // Verify Redis: run PING via redis-cli.
    let (exit_code, stdout, _) =
        rt.exec_with_output(cache_container_id, vec!["redis-cli".into(), "PING".into()]);
    assert_eq!(exit_code, 0, "redis-cli PING should succeed");
    assert!(
        stdout.contains("PONG"),
        "redis-cli output should contain 'PONG': {stdout}"
    );

    // Teardown: remove containers then shut down the shared VM.
    let down_spec = vz_stack::StackSpec {
        name: "real-svc".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged, "teardown should converge");

    // Shut down the shared VM.
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("real-svc")
        .expect("real-svc shared VM shutdown should succeed");
}

/// End-to-end test for exec via Unix control socket.
///
/// Boots Redis, starts a control socket listener, connects a client
/// through the socket, runs `redis-cli PING`, and validates the response.
/// This tests the full `vz stack exec` pipe: socket → container lookup →
/// exec_with_output → response serialization.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn exec_via_control_socket() {
    if !require_virtualization_entitlement() {
        return;
    }
    use serde::{Deserialize, Serialize};
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{UnixListener, UnixStream};

    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  cache:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 5s
"#;

    // Use persistent data dir for image cache.
    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "exec-sock").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);
    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "Redis should converge");
    assert_eq!(result.services_ready, 1);

    // Set up control socket.
    let sock_path = tmp.path().join("control.sock");
    let listener = UnixListener::bind(&sock_path).unwrap();

    // JSON protocol structs (mirrors vz-cli's ControlRequest/ControlResponse).
    #[derive(Debug, Serialize, Deserialize)]
    struct Req {
        service: String,
        cmd: Vec<String>,
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct Resp {
        exit_code: i32,
        stdout: String,
        stderr: String,
        error: Option<String>,
    }

    // Spawn a client task that sends the exec request through the socket.
    let client_sock_path = sock_path.clone();
    let client = tokio::spawn(async move {
        // Small delay to let the server start accepting.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let stream = UnixStream::connect(&client_sock_path).await.unwrap();
        let (reader, mut writer) = stream.into_split();

        let req = Req {
            service: "cache".into(),
            cmd: vec!["redis-cli".into(), "PING".into()],
        };
        let mut json = serde_json::to_string(&req).unwrap();
        json.push('\n');
        writer.write_all(json.as_bytes()).await.unwrap();
        writer.flush().await.unwrap();

        let mut lines = BufReader::new(reader).lines();
        let line = lines.next_line().await.unwrap().unwrap();
        serde_json::from_str::<Resp>(&line).unwrap()
    });

    // Server: accept the connection on the main task (which owns the orchestrator).
    let (stream, _) = listener.accept().await.unwrap();
    let (reader, mut writer) = stream.into_split();
    let mut lines = BufReader::new(reader).lines();
    let line = lines.next_line().await.unwrap().unwrap();
    let req: Req = serde_json::from_str(&line).unwrap();

    // Look up container ID from state.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("exec-sock")
        .unwrap();
    let svc = observed
        .iter()
        .find(|o| o.replica.service_name == req.service)
        .unwrap();
    let container_id = svc.container_id.as_ref().unwrap();

    // Execute via the ORIGINAL runtime (which owns the VM handles).
    let (exit_code, stdout, stderr) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(container_id, req.cmd);

    let resp = Resp {
        exit_code,
        stdout,
        stderr,
        error: None,
    };
    let mut json = serde_json::to_string(&resp).unwrap();
    json.push('\n');
    writer.write_all(json.as_bytes()).await.unwrap();
    writer.flush().await.unwrap();

    // Wait for the client and validate the response it received.
    let client_resp = client.await.unwrap();
    assert_eq!(
        client_resp.exit_code, 0,
        "redis-cli PING via socket should succeed"
    );
    assert!(
        client_resp.stdout.contains("PONG"),
        "response stdout should contain 'PONG': {}",
        client_resp.stdout
    );
    assert!(client_resp.error.is_none(), "no error expected");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "exec-sock".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "exec-sock teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("exec-sock")
        .expect("exec-sock shared VM shutdown should succeed");
}

/// Boot a 2-service stack with port forwarding, then connect from the host
/// and verify TCP data round-trip through the per-service network namespace.
///
/// Service "echo" runs `nc -l -p 8080` mapped to a dynamically reserved
/// loopback port with
/// `target_host` pointing at its per-service netns IP. The host connects
/// and reads the response, proving the full port-forwarding path works:
/// host → vsock → guest agent → netns bridge → container.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_port_forwarding() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let before_listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("a dynamic loopback port must be available before stack startup");
    let host_port = before_listener
        .local_addr()
        .expect("dynamic loopback listener must have an address")
        .port();
    drop(before_listener);

    let yaml = format!(
        r#"
services:
  echo:
    image: alpine:latest
    command: ["sh", "-c", "echo pong | nc -l -p 8080"]
    ports:
      - "{host_port}:8080"

  sidecar:
    image: alpine:latest
    command: ["sleep", "300"]
"#
    );

    // Use persistent data dir for image cache.
    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(&yaml, "port-fwd").unwrap();
    assert_eq!(spec.services.len(), 2);

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    const STACK_ID: &str = "port-fwd";
    let before = orchestrator.executor().runtime().lifecycle_diagnostics();
    assert_eq!(before.stack_vms, 0);
    assert_eq!(before.stack_port_forwards, 0);
    assert!(
        orchestrator
            .executor()
            .runtime()
            .tracked_container_ids()
            .is_empty()
    );

    let mut teardown_evidence = serde_json::json!({
        "schema_version": 2,
        "scenario": "stack-port-forwarding-teardown",
        "stack_id": STACK_ID,
        "host_listener": {
            "address": "127.0.0.1",
            "port": host_port,
            "free_before_start": true,
            "owned_while_active": false,
            "owned_after_service_down": false,
            "rebound_after_vm_shutdown": false,
        },
        "operations": {
            "up": { "succeeded": false, "error": "not completed" },
            "down": { "succeeded": false, "error": "not completed" },
            "shutdown": { "succeeded": false, "error": "not completed" },
        },
        "container_ids": [],
        "before": {
            "tracked_container_ids": [],
            "lifecycle": lifecycle_inventory(&before),
        },
        "active": serde_json::Value::Null,
        "after_service_down": serde_json::Value::Null,
        "after_vm_shutdown": serde_json::Value::Null,
    });
    write_stack_teardown_evidence(&teardown_evidence);

    let result = match orchestrator.run(&spec, None) {
        Ok(result) => result,
        Err(error) => {
            teardown_evidence["operations"]["up"]["error"] = serde_json::json!(error.to_string());
            write_stack_teardown_evidence(&teardown_evidence);
            panic!("stack startup failed: {error}");
        }
    };
    teardown_evidence["operations"]["up"] = serde_json::json!({
        "succeeded": result.converged && result.services_failed == 0,
        "error": if result.converged && result.services_failed == 0 {
            serde_json::Value::Null
        } else {
            serde_json::json!(format!(
                "startup did not converge cleanly: ready={}, failed={}",
                result.services_ready, result.services_failed
            ))
        },
    });
    write_stack_teardown_evidence(&teardown_evidence);
    assert!(
        result.converged,
        "stack should converge: ready={}, failed={}",
        result.services_ready, result.services_failed
    );
    assert_eq!(result.services_ready, 2);

    let active_container_ids = orchestrator.executor().runtime().tracked_container_ids();
    assert_eq!(active_container_ids.len(), 2);
    let active = orchestrator.executor().runtime().lifecycle_diagnostics();
    assert_eq!(active.vm_handle_ids, active_container_ids);
    assert_eq!(active.stack_vm_ids, [STACK_ID]);
    assert_eq!(active.stack_port_forward_ids, [STACK_ID]);
    assert_eq!(active.container_routes, 2);
    assert!(
        active
            .container_route_pairs
            .iter()
            .all(
                |(container_id, stack_id)| active_container_ids.contains(container_id)
                    && stack_id == STACK_ID
            )
    );
    let active_bind_error = std::net::TcpListener::bind(("127.0.0.1", host_port))
        .expect_err("active host forwarding listener must own the exact loopback port");
    teardown_evidence["container_ids"] = serde_json::json!(active_container_ids);
    teardown_evidence["active"] = serde_json::json!({
        "tracked_container_ids": active_container_ids,
        "lifecycle": lifecycle_inventory(&active),
    });
    teardown_evidence["host_listener"]["owned_while_active"] =
        serde_json::json!(active_bind_error.kind() == ErrorKind::AddrInUse);
    write_stack_teardown_evidence(&teardown_evidence);
    assert_eq!(active_bind_error.kind(), ErrorKind::AddrInUse);

    // Give the nc listener a moment to start inside the container.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Connect exactly once: convergence publishes the active forwarding
    // listener, so a test retry would mask an admission/readiness defect.
    use tokio::io::AsyncReadExt;
    let mut conn = tokio::time::timeout(
        Duration::from_secs(10),
        tokio::net::TcpStream::connect(("127.0.0.1", host_port)),
    )
    .await
    .expect("port forwarding connect timed out")
    .expect("port forwarding connection failed");
    let mut buf = vec![0u8; 64];
    let n = tokio::time::timeout(Duration::from_secs(10), conn.read(&mut buf))
        .await
        .expect("port forward read timed out")
        .expect("port forward read failed");
    let response = String::from_utf8_lossy(&buf[..n]);
    assert!(
        response.contains("pong"),
        "expected 'pong' from port-forwarded nc, got: {response}"
    );

    // Drop connection before cleanup.
    drop(conn);

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "port-fwd".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = match orchestrator.run(&down_spec, None) {
        Ok(result) => result,
        Err(error) => {
            teardown_evidence["operations"]["down"]["error"] = serde_json::json!(error.to_string());
            write_stack_teardown_evidence(&teardown_evidence);
            panic!("service down failed: {error}");
        }
    };
    teardown_evidence["operations"]["down"] = serde_json::json!({
        "succeeded": down.converged && down.services_failed == 0,
        "error": if down.converged && down.services_failed == 0 {
            serde_json::Value::Null
        } else {
            serde_json::json!(format!(
                "service down did not converge cleanly: ready={}, failed={}",
                down.services_ready, down.services_failed
            ))
        },
    });
    write_stack_teardown_evidence(&teardown_evidence);
    assert!(down.converged);
    assert_eq!(down.services_ready, 0);
    assert_eq!(down.services_failed, 0);
    assert!(
        orchestrator
            .executor()
            .runtime()
            .tracked_container_ids()
            .is_empty()
    );
    let after_service_down = orchestrator.executor().runtime().lifecycle_diagnostics();
    assert_eq!(after_service_down.vm_handles, 0);
    assert_eq!(after_service_down.container_routes, 0);
    assert_eq!(after_service_down.exec_bindings, 0);
    assert_eq!(after_service_down.active_lifecycles, 0);
    assert_eq!(after_service_down.stack_vm_ids, [STACK_ID]);
    assert_eq!(after_service_down.stack_port_forward_ids, [STACK_ID]);
    let down_bind_error = std::net::TcpListener::bind(("127.0.0.1", host_port))
        .expect_err("stack listener must remain owned until VM shutdown");
    teardown_evidence["after_service_down"] = serde_json::json!({
        "tracked_container_ids": [],
        "lifecycle": lifecycle_inventory(&after_service_down),
    });
    teardown_evidence["host_listener"]["owned_after_service_down"] =
        serde_json::json!(down_bind_error.kind() == ErrorKind::AddrInUse);
    write_stack_teardown_evidence(&teardown_evidence);
    assert_eq!(down_bind_error.kind(), ErrorKind::AddrInUse);

    if let Err(error) = orchestrator.executor().runtime().shutdown_sandbox(STACK_ID) {
        teardown_evidence["operations"]["shutdown"]["error"] = serde_json::json!(error.to_string());
        write_stack_teardown_evidence(&teardown_evidence);
        panic!("shared VM shutdown failed: {error}");
    }
    teardown_evidence["operations"]["shutdown"] =
        serde_json::json!({ "succeeded": true, "error": serde_json::Value::Null });
    let after_vm_shutdown = orchestrator.executor().runtime().lifecycle_diagnostics();
    assert!(
        orchestrator
            .executor()
            .runtime()
            .tracked_container_ids()
            .is_empty()
    );
    assert_eq!(after_vm_shutdown.vm_handles, 0);
    assert!(after_vm_shutdown.vm_handle_ids.is_empty());
    assert_eq!(after_vm_shutdown.stack_vms, 0);
    assert!(after_vm_shutdown.stack_vm_ids.is_empty());
    assert_eq!(after_vm_shutdown.container_routes, 0);
    assert!(after_vm_shutdown.container_route_pairs.is_empty());
    assert_eq!(after_vm_shutdown.stack_port_forwards, 0);
    assert!(after_vm_shutdown.stack_port_forward_ids.is_empty());
    assert_eq!(after_vm_shutdown.exec_bindings, 0);
    assert_eq!(after_vm_shutdown.active_lifecycles, 0);
    assert_eq!(after_vm_shutdown.exec_sessions, 0);
    assert_eq!(after_vm_shutdown.setup_restore_entries, 0);
    assert_eq!(after_vm_shutdown.overlay_cleanup_pending, 0);
    assert_eq!(after_vm_shutdown.rootfs_directories, 0);
    assert!(after_vm_shutdown.generations.iter().all(|generation| {
        !active_container_ids.contains(&generation.container_id) || !generation.reserved
    }));
    let rebound_listener = std::net::TcpListener::bind(("127.0.0.1", host_port))
        .expect("exact loopback port must be reusable after shared VM shutdown");
    teardown_evidence["host_listener"]["rebound_after_vm_shutdown"] = serde_json::json!(true);
    teardown_evidence["after_vm_shutdown"] = serde_json::json!({
        "tracked_container_ids": [],
        "lifecycle": lifecycle_inventory(&after_vm_shutdown),
    });
    write_stack_teardown_evidence(&teardown_evidence);
    drop(rebound_listener);
}

fn snapshot_stack_service_ids(
    observed: &[vz_stack::ServiceObservedState],
) -> Result<BTreeMap<String, String>, String> {
    let mut ids = BTreeMap::new();
    let mut failures = Vec::new();
    for service_name in ["api", "cache", "db"] {
        match observed
            .iter()
            .find(|entry| entry.replica.service_name == service_name)
            .and_then(|entry| entry.container_id.as_ref())
        {
            Some(container_id) => {
                ids.insert(service_name.to_string(), container_id.clone());
            }
            None => failures.push(format!(
                "service '{service_name}' has no observed container ID"
            )),
        }
    }
    if failures.is_empty() {
        Ok(ids)
    } else {
        Err(failures.join("; "))
    }
}

fn probe_snapshot_stack_services_once(
    runtime: &OciContainerRuntime,
    service_ids: &BTreeMap<String, String>,
    phase: &str,
) -> Result<BTreeMap<String, serde_json::Value>, String> {
    let probes = [
        (
            "db",
            vec![
                "pg_isready".to_string(),
                "-U".to_string(),
                "app".to_string(),
            ],
        ),
        ("cache", vec!["redis-cli".to_string(), "ping".to_string()]),
        (
            "api",
            vec![
                "/bin/sh".to_string(),
                "-c".to_string(),
                "printf vz-api-snapshot-probe".to_string(),
            ],
        ),
    ];
    let mut outcomes = BTreeMap::new();
    let mut failures = Vec::new();

    // Deliberately collect every result before deciding whether the phase
    // passed. A broken service must not prevent the other two one-shot probes.
    for (service_name, command) in probes {
        let Some(container_id) = service_ids.get(service_name) else {
            failures.push(format!("{phase}/{service_name}: missing container ID"));
            continue;
        };
        match runtime.try_exec_with_output(container_id, command.clone()) {
            Ok((exit_code, stdout, stderr)) => {
                let semantic_output_ok = match service_name {
                    "db" => stdout.contains("accepting connections"),
                    "cache" => stdout.trim().eq_ignore_ascii_case("PONG"),
                    "api" => stdout == "vz-api-snapshot-probe",
                    _ => false,
                };
                outcomes.insert(
                    service_name.to_string(),
                    serde_json::json!({
                        "container_id": container_id,
                        "command": command,
                        "exit_code": exit_code,
                        "stdout": stdout,
                        "stderr": stderr,
                        "semantic_output_ok": semantic_output_ok,
                    }),
                );
                if exit_code != 0 || !semantic_output_ok {
                    failures.push(format!(
                        "{phase}/{service_name}: exit={exit_code}, stdout={stdout:?}, stderr={stderr:?}"
                    ));
                }
            }
            Err(error) => {
                outcomes.insert(
                    service_name.to_string(),
                    serde_json::json!({
                        "container_id": container_id,
                        "command": command,
                        "error": error,
                    }),
                );
                failures.push(format!("{phase}/{service_name}: {error}"));
            }
        }
    }

    if outcomes.len() != 3 {
        failures.push(format!(
            "{phase}: expected exactly three one-shot probes, recorded {}",
            outcomes.len()
        ));
    }
    if failures.is_empty() {
        Ok(outcomes)
    } else {
        Err(failures.join("; "))
    }
}

fn snapshot_stack_guest_identities_once(
    runtime: &OciContainerRuntime,
    service_ids: &BTreeMap<String, String>,
    phase: &str,
) -> Result<BTreeMap<String, serde_json::Value>, String> {
    let mut identities = BTreeMap::new();
    let mut failures = Vec::new();
    for (service_name, container_id) in service_ids {
        match runtime.try_stack_guest_generation_evidence("snapshot-stack", container_id) {
            Ok(identity) => {
                identities.insert(service_name.clone(), identity);
            }
            Err(error) => failures.push(format!("{phase}/{service_name}: {error}")),
        }
    }
    if identities.len() != 3 {
        failures.push(format!(
            "{phase}: expected three guest identities, recorded {}",
            identities.len()
        ));
    }
    if failures.is_empty() {
        Ok(identities)
    } else {
        Err(failures.join("; "))
    }
}

fn vm_full_unsupported_result(
    result: Result<(), MacosOciError>,
    expected_operation: &str,
) -> Result<String, String> {
    match result {
        Err(MacosOciError::UnsupportedOperation { operation, reason }) => {
            if operation != expected_operation {
                return Err(format!(
                    "expected unsupported operation '{expected_operation}', got '{operation}'"
                ));
            }
            if reason != EXPECTED_VM_FULL_UNSUPPORTED_REASON {
                return Err(format!(
                    "unsupported '{operation}' reason changed: expected {EXPECTED_VM_FULL_UNSUPPORTED_REASON:?}, got {reason:?}"
                ));
            }
            Ok(reason)
        }
        Err(error) => Err(format!(
            "expected typed UnsupportedOperation for '{expected_operation}', got {error:?}"
        )),
        Ok(()) => Err(format!(
            "unsupported VM-full operation '{expected_operation}' unexpectedly succeeded"
        )),
    }
}

fn cleanup_snapshot_stack(
    orchestrator: &mut StackOrchestrator<OciContainerRuntime>,
    baseline_tracked: &BTreeSet<String>,
    captured_owned: &BTreeSet<String>,
) -> Result<serde_json::Value, String> {
    const STACK_ID: &str = "snapshot-stack";
    let mut failures = Vec::new();
    let mut exact_owned = captured_owned.clone();

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator
            .executor()
            .store()
            .load_observed_state(STACK_ID)
    })) {
        Ok(Ok(observed)) => exact_owned.extend(
            observed
                .into_iter()
                .filter_map(|service| service.container_id),
        ),
        Ok(Err(error)) => failures.push(format!("cleanup could not load observed state: {error}")),
        Err(payload) => failures.push(format!(
            "cleanup observed-state inspection panicked: {}",
            panic_payload_description(payload.as_ref())
        )),
    }
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator
            .executor()
            .runtime()
            .try_tracked_container_ids()
    })) {
        Ok(Ok(tracked)) => exact_owned.extend(
            tracked
                .into_iter()
                .filter(|container_id| !baseline_tracked.contains(container_id)),
        ),
        Ok(Err(error)) => failures.push(format!(
            "cleanup could not inspect tracked containers: {error}"
        )),
        Err(payload) => failures.push(format!(
            "cleanup tracked-container inspection panicked: {}",
            panic_payload_description(payload.as_ref())
        )),
    }

    let down_spec = vz_stack::StackSpec {
        name: STACK_ID.to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator.run(&down_spec, None)
    })) {
        Ok(Ok(result)) if result.converged && result.services_failed == 0 => {}
        Ok(Ok(result)) => failures.push(format!(
            "stack down did not converge cleanly: converged={}, failed={}",
            result.converged, result.services_failed
        )),
        Ok(Err(error)) => failures.push(format!("stack down failed: {error}")),
        Err(payload) => failures.push(format!(
            "stack down panicked: {}",
            panic_payload_description(payload.as_ref())
        )),
    }

    // Always execute physical shutdown, even if logical stack-down failed.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator.executor().runtime().shutdown_sandbox(STACK_ID)
    })) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(format!("shared VM shutdown failed: {error}")),
        Err(payload) => failures.push(format!(
            "shared VM shutdown panicked: {}",
            panic_payload_description(payload.as_ref())
        )),
    }

    // Successful VM shutdown publishes every member as stopped. Remove only
    // the exact containers owned by this scenario if logical down left durable
    // metadata/rootfs behind.
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator
            .executor()
            .runtime()
            .try_tracked_container_ids()
    })) {
        Ok(Ok(tracked)) => {
            let tracked = tracked.into_iter().collect::<BTreeSet<_>>();
            for container_id in exact_owned.intersection(&tracked) {
                match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    orchestrator.executor().runtime().remove(container_id)
                })) {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => failures.push(format!(
                        "residual exact-owner removal failed for '{container_id}': {error}"
                    )),
                    Err(payload) => failures.push(format!(
                        "residual exact-owner removal panicked for '{container_id}': {}",
                        panic_payload_description(payload.as_ref())
                    )),
                }
            }
        }
        Ok(Err(error)) => failures.push(format!(
            "residual tracked-container inspection failed: {error}"
        )),
        Err(payload) => failures.push(format!(
            "residual tracked-container inspection panicked: {}",
            panic_payload_description(payload.as_ref())
        )),
    }

    let mut final_tracked_evidence = None;
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator
            .executor()
            .runtime()
            .try_tracked_container_ids()
    })) {
        Ok(Ok(final_tracked)) => {
            let final_tracked = final_tracked.into_iter().collect::<BTreeSet<_>>();
            final_tracked_evidence = Some(final_tracked.clone());
            if &final_tracked != baseline_tracked {
                failures.push(format!(
                    "tracked-container inventory did not return to baseline: baseline={baseline_tracked:?}, final={final_tracked:?}"
                ));
            }
            let leaked_owned = exact_owned
                .intersection(&final_tracked)
                .cloned()
                .collect::<Vec<_>>();
            if !leaked_owned.is_empty() {
                failures.push(format!("owned container metadata leaked: {leaked_owned:?}"));
            }
        }
        Ok(Err(error)) => failures.push(format!(
            "final tracked-container inspection failed: {error}"
        )),
        Err(payload) => failures.push(format!(
            "final tracked-container inspection panicked: {}",
            panic_payload_description(payload.as_ref())
        )),
    }

    let mut final_lifecycle_evidence = None;
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator
            .executor()
            .runtime()
            .try_lifecycle_diagnostics()
    })) {
        Ok(Ok(diagnostics)) => {
            let relevant_counts = [
                ("vm_handles", diagnostics.vm_handles),
                ("stack_vms", diagnostics.stack_vms),
                ("container_routes", diagnostics.container_routes),
                ("stack_port_forwards", diagnostics.stack_port_forwards),
                ("exec_bindings", diagnostics.exec_bindings),
                ("active_lifecycles", diagnostics.active_lifecycles),
                ("exec_sessions", diagnostics.exec_sessions),
                ("setup_restore_entries", diagnostics.setup_restore_entries),
                (
                    "overlay_cleanup_pending",
                    diagnostics.overlay_cleanup_pending,
                ),
                ("rootfs_directories", diagnostics.rootfs_directories),
            ];
            let nonzero = relevant_counts
                .into_iter()
                .filter(|(_, count)| *count != 0)
                .collect::<Vec<_>>();
            if !nonzero.is_empty() {
                failures.push(format!("nonzero final lifecycle diagnostics: {nonzero:?}"));
            }
            let reserved_owned = diagnostics
                .generations
                .iter()
                .filter(|generation| {
                    exact_owned.contains(&generation.container_id) && generation.reserved
                })
                .map(|generation| generation.container_id.clone())
                .collect::<Vec<_>>();
            if !reserved_owned.is_empty() {
                failures.push(format!(
                    "owned lifecycle generations remain reserved: {reserved_owned:?}"
                ));
            }
            final_lifecycle_evidence = Some(lifecycle_inventory(&diagnostics));
        }
        Ok(Err(error)) => failures.push(format!("final lifecycle diagnostics failed: {error}")),
        Err(payload) => failures.push(format!(
            "final lifecycle diagnostics panicked: {}",
            panic_payload_description(payload.as_ref())
        )),
    }
    let sandbox_active = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        orchestrator.executor().runtime().has_sandbox(STACK_ID)
    })) {
        Ok(active) => {
            if active {
                failures.push("shared VM remains active after cleanup".to_string());
            }
            Some(active)
        }
        Err(payload) => {
            failures.push(format!(
                "final sandbox inspection panicked: {}",
                panic_payload_description(payload.as_ref())
            ));
            None
        }
    };

    let evidence = serde_json::json!({
        "stack_id": STACK_ID,
        "exact_owned_container_ids": exact_owned,
        "baseline_tracked_container_ids": baseline_tracked,
        "final_tracked_container_ids": final_tracked_evidence,
        "final_lifecycle": final_lifecycle_evidence,
        "sandbox_active": sandbox_active,
        "zero_inventory": failures.is_empty(),
    });

    if failures.is_empty() {
        Ok(evidence)
    } else {
        Err(format!("{}; evidence={evidence}", failures.join("; ")))
    }
}

fn panic_payload_description(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| {
            payload
                .downcast_ref::<&str>()
                .map(|value| (*value).to_string())
        })
        .unwrap_or_else(|| "non-string panic payload".to_string())
}

/// VM-full snapshots are not supported for MacosVz shared-stack VMs because
/// their container root filesystems and volumes are external VirtioFS state.
/// Prove both raw entry points fail closed without touching a live stack.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
#[allow(clippy::print_stderr)]
async fn complex_stack_vm_full_snapshot_fails_closed_without_mutation() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "app"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 10s

  cache:
    image: redis:7-alpine
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 5s

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      db:
        condition: service_healthy
      cache:
        condition: service_healthy
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let snapshot_path = tmp.path().join("snapshot-stack.state");

    let spec = parse_compose(yaml, "snapshot-stack").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let baseline_tracked = bridge
        .try_tracked_container_ids()
        .expect("pre-start container inventory should be readable")
        .into_iter()
        .collect::<BTreeSet<_>>();
    assert!(
        baseline_tracked.is_empty(),
        "snapshot scenario requires an empty per-run OCI store: {baseline_tracked:?}"
    );
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge.clone(), exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 30,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);
    let _physical_cleanup = EnvironmentLifecyclePhysicalCleanup {
        runtime: bridge,
        backend_keys: vec!["snapshot-stack".to_string()],
        disk_paths: vec![],
    };
    let mut captured_owned = BTreeSet::new();
    let mut scenario_evidence = None;

    let scenario_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
        || -> Result<(), String> {
            let result = orchestrator
                .run(&spec, None)
                .map_err(|error| format!("stack startup failed: {error}"))?;
            if !result.converged || result.services_ready != 3 || result.services_failed != 0 {
                return Err(format!(
                    "stack did not converge cleanly: converged={}, ready={}, failed={}, rounds={}",
                    result.converged, result.services_ready, result.services_failed, result.rounds
                ));
            }

            let observed_before = orchestrator
                .executor()
                .store()
                .load_observed_state("snapshot-stack")
                .map_err(|error| format!("pre-call observed-state load failed: {error}"))?;
            let service_ids_before = snapshot_stack_service_ids(&observed_before)?;
            captured_owned.extend(service_ids_before.values().cloned());

            let mut failures = Vec::new();
            let probes_before = match probe_snapshot_stack_services_once(
                orchestrator.executor().runtime(),
                &service_ids_before,
                "before",
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            let guest_before = match snapshot_stack_guest_identities_once(
                orchestrator.executor().runtime(),
                &service_ids_before,
                "before",
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            let tracked_before = match orchestrator
                .executor()
                .runtime()
                .try_tracked_container_ids()
            {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(format!(
                        "pre-call tracked-container inspection failed: {error}"
                    ));
                    None
                }
            };
            let lifecycle_before = match orchestrator
                .executor()
                .runtime()
                .try_lifecycle_diagnostics()
            {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(format!("pre-call lifecycle diagnostics failed: {error}"));
                    None
                }
            };
            let absent_before = !snapshot_path.exists();
            if !absent_before {
                failures.push(format!(
                    "snapshot destination existed before unsupported calls: {}",
                    snapshot_path.display()
                ));
            }

            // Each raw VM-full operation is invoked exactly once. Validate both
            // only after both calls so one malformed result cannot hide the other.
            let save_result = vm_full_unsupported_result(
                orchestrator
                    .executor()
                    .runtime()
                    .save_shared_vm_snapshot("snapshot-stack", &snapshot_path),
                "create_checkpoint",
            );
            let absent_after_save = !snapshot_path.exists();
            let restore_result = vm_full_unsupported_result(
                orchestrator
                    .executor()
                    .runtime()
                    .restore_shared_vm_snapshot("snapshot-stack", &snapshot_path),
                "restore_checkpoint",
            );
            let absent_after_restore = !snapshot_path.exists();
            let save_reason = match save_result {
                Ok(reason) => Some(reason),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            let restore_reason = match restore_result {
                Ok(reason) => Some(reason),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            if !absent_after_save || !absent_after_restore {
                failures.push(format!(
                    "unsupported snapshot calls mutated destination existence: after_save={absent_after_save}, after_restore={absent_after_restore}"
                ));
            }

            let observed_after = orchestrator
                .executor()
                .store()
                .load_observed_state("snapshot-stack")
                .map_err(|error| format!("post-call observed-state load failed: {error}"))?;
            let service_ids_after = snapshot_stack_service_ids(&observed_after)?;
            captured_owned.extend(service_ids_after.values().cloned());
            if service_ids_after != service_ids_before {
                failures.push(format!(
                    "service/container identity changed across unsupported calls: before={service_ids_before:?}, after={service_ids_after:?}"
                ));
            }
            let probes_after = match probe_snapshot_stack_services_once(
                orchestrator.executor().runtime(),
                &service_ids_after,
                "after",
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            let guest_after = match snapshot_stack_guest_identities_once(
                orchestrator.executor().runtime(),
                &service_ids_after,
                "after",
            ) {
                Ok(value) => Some(value),
                Err(error) => {
                    failures.push(error);
                    None
                }
            };
            if let (Some(before), Some(after)) = (&guest_before, &guest_after)
                && before != after
            {
                failures.push(format!(
                    "guest boot/PID/start/cgroup/namespace/root identity changed: before={before:?}, after={after:?}"
                ));
            }

            let tracked_after = match orchestrator
                .executor()
                .runtime()
                .try_tracked_container_ids()
            {
                Ok(after)
                    if tracked_before
                        .as_ref()
                        .is_some_and(|before| before == &after) =>
                {
                    Some(after)
                }
                Ok(after) => {
                    failures.push(format!(
                        "tracked-container IDs changed: before={tracked_before:?}, after={after:?}"
                    ));
                    Some(after)
                }
                Err(error) => {
                    failures.push(format!(
                        "post-call tracked-container inspection failed: {error}"
                    ));
                    None
                }
            };
            let lifecycle_after = match orchestrator
                .executor()
                .runtime()
                .try_lifecycle_diagnostics()
            {
                Ok(after)
                    if lifecycle_before
                        .as_ref()
                        .is_some_and(|before| before == &after) =>
                {
                    Some(after)
                }
                Ok(after) => {
                    failures.push(format!(
                        "runtime lifecycle identity changed: before={lifecycle_before:?}, after={after:?}"
                    ));
                    Some(after)
                }
                Err(error) => {
                    failures.push(format!("post-call lifecycle diagnostics failed: {error}"));
                    None
                }
            };

            scenario_evidence = Some(serde_json::json!({
                "schema_version": 2,
                "scenario": "complex_stack_vm_full_snapshot_fails_closed_without_mutation",
                "stack_id": "snapshot-stack",
                "service_container_ids": {
                    "before": service_ids_before,
                    "after": service_ids_after,
                },
                "service_probes": {
                    "before": probes_before,
                    "after": probes_after,
                },
                "guest_generation_identities": {
                    "before": guest_before,
                    "after": guest_after,
                },
                "runtime": {
                    "tracked_container_ids_before": tracked_before,
                    "tracked_container_ids_after": tracked_after,
                    "lifecycle_before": lifecycle_before.as_ref().map(lifecycle_inventory),
                    "lifecycle_after": lifecycle_after.as_ref().map(lifecycle_inventory),
                },
                "vm_full_operations": {
                    "save": {
                        "invocations": 1,
                        "error_variant": "UnsupportedOperation",
                        "operation": "create_checkpoint",
                        "reason": save_reason,
                    },
                    "restore": {
                        "invocations": 1,
                        "error_variant": "UnsupportedOperation",
                        "operation": "restore_checkpoint",
                        "reason": restore_reason,
                    },
                },
                "snapshot_destination": {
                    "path": snapshot_path,
                    "absent_before": absent_before,
                    "absent_after_save": absent_after_save,
                    "absent_after_restore": absent_after_restore,
                },
            }));

            if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("; "))
            }
        },
    ));

    let cleanup_outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cleanup_snapshot_stack(&mut orchestrator, &baseline_tracked, &captured_owned)
    }));

    match (scenario_outcome, cleanup_outcome) {
        (Ok(Ok(())), Ok(Ok(cleanup_evidence))) => {
            let absent_after_cleanup = !snapshot_path.exists();
            if !absent_after_cleanup {
                panic!(
                    "snapshot destination appeared during cleanup: {}",
                    snapshot_path.display()
                );
            }
            let evidence = serde_json::json!({
                "scenario": scenario_evidence,
                "cleanup": cleanup_evidence,
                "snapshot_destination_absent_after_cleanup": absent_after_cleanup,
            });
            match serde_json::to_string(&evidence) {
                Ok(line) => eprintln!("VZ_STACK_VM_FULL_UNSUPPORTED_EVIDENCE:{line}"),
                Err(error) => panic!("could not serialize snapshot scenario evidence: {error}"),
            }
        }
        (Ok(Err(error)), Ok(Ok(_))) => panic!("snapshot scenario failed: {error}"),
        (Err(payload), Ok(Ok(_))) => std::panic::resume_unwind(payload),
        (Ok(Ok(())), Ok(Err(cleanup))) => panic!("snapshot cleanup failed: {cleanup}"),
        (Ok(Err(error)), Ok(Err(cleanup))) => {
            panic!("snapshot scenario failed: {error}; cleanup also failed: {cleanup}")
        }
        (Err(payload), Ok(Err(cleanup))) => panic!(
            "snapshot scenario panicked: {}; cleanup also failed: {cleanup}",
            panic_payload_description(payload.as_ref())
        ),
        (Ok(result), Err(payload)) => panic!(
            "snapshot outcome was {result:?}; cleanup panicked: {}",
            panic_payload_description(payload.as_ref())
        ),
        (Err(scenario), Err(cleanup)) => panic!(
            "snapshot scenario panicked: {}; cleanup panicked: {}",
            panic_payload_description(scenario.as_ref()),
            panic_payload_description(cleanup.as_ref())
        ),
    }
}

/// Verify sandbox lifecycle: create_sandbox → services up → shutdown_sandbox.
///
/// Tests the fundamental sandbox lifecycle:
/// 1. Orchestrator creates a sandbox via `create_sandbox()`
/// 2. Services are created inside the sandbox via `create_in_sandbox()`
/// 3. Network isolation is set up via `setup_sandbox_network()`
/// 4. Stack teardown removes containers and shuts down the sandbox
/// 5. `has_sandbox()` returns false after shutdown
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_lifecycle_create_and_teardown() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  worker:
    image: alpine:latest
    command: ["sleep", "300"]

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - worker
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "sbx-lifecycle").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    // 1. Deploy: sandbox + services created.
    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 2, "both services should be ready");

    // 2. Verify sandbox exists.
    assert!(
        orchestrator
            .executor()
            .runtime()
            .has_sandbox("sbx-lifecycle"),
        "sandbox should exist after deploy"
    );

    // 3. Verify services are alive via exec.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("sbx-lifecycle")
        .unwrap();
    for name in &["worker", "api"] {
        let svc = observed
            .iter()
            .find(|o| o.replica.service_name == *name)
            .unwrap_or_else(|| panic!("{name} should be in observed state"));
        let cid = svc.container_id.as_ref().unwrap();
        let (exit_code, stdout, _) = orchestrator
            .executor()
            .runtime()
            .exec_with_output(cid, vec!["echo".into(), "alive".into()]);
        assert_eq!(exit_code, 0, "{name} exec should succeed");
        assert!(
            stdout.contains("alive"),
            "{name} should output 'alive', got: {stdout}"
        );
    }

    // 4. Teardown: remove services.
    let down_spec = vz_stack::StackSpec {
        name: "sbx-lifecycle".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged, "teardown should converge");

    // 5. Shut down sandbox and verify it's gone.
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("sbx-lifecycle")
        .unwrap();

    assert!(
        !orchestrator
            .executor()
            .runtime()
            .has_sandbox("sbx-lifecycle"),
        "sandbox should not exist after shutdown"
    );
}

/// Verify that the state store accurately tracks service phases through
/// the full lifecycle: pending → creating → running → stopped.
///
/// Also verifies that events are emitted for each state transition and
/// that the observed state is consistent after teardown.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn state_store_tracks_service_phases() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  tracker:
    image: alpine:latest
    command: ["sleep", "300"]
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "phase-track").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    // Deploy.
    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);
    assert_eq!(result.services_ready, 1);

    // Check observed state.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("phase-track")
        .unwrap();
    let tracker = observed
        .iter()
        .find(|o| o.replica.service_name == "tracker")
        .unwrap();
    assert!(tracker.container_id.is_some());
    assert_eq!(tracker.phase, ServicePhase::Running);

    // Check events include creating + ready.
    let events = orchestrator
        .executor()
        .store()
        .load_events("phase-track")
        .unwrap();
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceCreating { service_name, .. } if service_name == "tracker")),
        "should have ServiceCreating event"
    );
    assert!(
        events
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceReady { service_name, .. } if service_name == "tracker")),
        "should have ServiceReady event"
    );

    // Teardown and verify stopped state.
    let down_spec = vz_stack::StackSpec {
        name: "phase-track".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = orchestrator.run(&down_spec, None).unwrap();
    assert!(down_result.converged);

    let events_after = orchestrator
        .executor()
        .store()
        .load_events("phase-track")
        .unwrap();
    assert!(
        events_after
            .iter()
            .any(|e| matches!(e, StackEvent::ServiceStopped { service_name, .. } if service_name == "tracker")),
        "should have ServiceStopped event after teardown"
    );

    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("phase-track")
        .expect("phase-track shared VM shutdown should succeed");
}

/// Verify orchestrator idempotency: running the same spec twice should
/// converge immediately on the second run (no-op).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn orchestrator_idempotent_rerun() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "idempotent").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    // First run: deploy.
    let r1 = orchestrator.run(&spec, None).unwrap();
    assert!(r1.converged);
    assert_eq!(r1.services_ready, 1);

    // Save container ID from first run.
    let observed1 = orchestrator
        .executor()
        .store()
        .load_observed_state("idempotent")
        .unwrap();
    let cid1 = observed1[0].container_id.clone().unwrap();

    // Second run: should be a no-op (same container stays running).
    let r2 = orchestrator.run(&spec, None).unwrap();
    assert!(r2.converged);
    assert_eq!(r2.services_ready, 1);

    // Container ID should be the same (no recreate).
    let observed2 = orchestrator
        .executor()
        .store()
        .load_observed_state("idempotent")
        .unwrap();
    let cid2 = observed2[0].container_id.clone().unwrap();
    assert_eq!(
        cid1, cid2,
        "container should not be recreated on idempotent rerun"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "idempotent".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "idempotent teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("idempotent")
        .expect("idempotent shared VM shutdown should succeed");
}

/// Verify that `depends_on` with `service_healthy` blocks dependent services
/// until the dependency passes its health check.
///
/// Uses Postgres with `pg_isready` health check, and an API service
/// that depends on db being healthy. The orchestrator should not create
/// the API container until after Postgres health check passes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn depends_on_service_healthy_blocks_until_ready() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: app
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: app
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "app"]
      interval: 2s
      timeout: 5s
      retries: 10
      start_period: 10s

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      db:
        condition: service_healthy
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "dep-healthy").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 30,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    // Use round callback to verify ordering.
    let mut db_ready_round: Option<usize> = None;
    let mut api_created_round: Option<usize> = None;

    let result = orchestrator
        .run(
            &spec,
            Some(&mut |report: &vz_stack::RoundReport| {
                if let Some(exec) = &report.exec_result {
                    if exec.succeeded > 0 {
                        // Check events to see what was created this round.
                        // We track by round number.
                        if db_ready_round.is_none() {
                            // First round with success creates db.
                            db_ready_round = Some(report.round);
                        }
                    }
                }
                if report.services_ready == 2 && api_created_round.is_none() {
                    api_created_round = Some(report.round);
                }
            }),
        )
        .unwrap();

    assert!(
        result.converged,
        "stack should converge: ready={}, failed={}, rounds={}",
        result.services_ready, result.services_failed, result.rounds
    );
    assert_eq!(result.services_ready, 2);

    // API should have been created in a LATER round than DB.
    if let (Some(db_round), Some(api_round)) = (db_ready_round, api_created_round) {
        assert!(
            api_round > db_round,
            "api should be created after db is ready: db_round={db_round}, api_round={api_round}"
        );
    }

    // Verify Postgres is healthy via psql.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("dep-healthy")
        .unwrap();
    let db_cid = observed
        .iter()
        .find(|o| o.replica.service_name == "db")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        db_cid,
        vec![
            "psql".into(),
            "-U".into(),
            "app".into(),
            "-d".into(),
            "app".into(),
            "-c".into(),
            "SELECT 1".into(),
        ],
    );
    assert_eq!(exit_code, 0, "psql should succeed");
    assert!(stdout.contains('1'), "psql should return 1");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "dep-healthy".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "dep-healthy teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("dep-healthy")
        .expect("dep-healthy shared VM shutdown should succeed");
}

/// Verify environment variables are correctly passed to containers in a stack.
///
/// Tests both inline `environment:` and multi-variable configurations.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_environment_variable_passthrough() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      APP_ENV: production
      APP_PORT: "8080"
      APP_NAME: "my-service"
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "env-test").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);
    assert_eq!(result.services_ready, 1);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("env-test")
        .unwrap();
    let cid = observed[0].container_id.as_ref().unwrap();

    // Verify environment variables were written to the OCI bundle config.
    // We read the host-side config.json directly because exec via nsenter
    // doesn't inherit the OCI process environment, and the VM kernel lacks
    // CONFIG_PID_NS so /proc/1/environ shows the VM init, not the container.
    let bundle_config = oci_data.join(format!("rootfs/{cid}/run/vz-oci/bundles/{cid}/config.json"));
    let config_bytes = std::fs::read(&bundle_config).unwrap_or_else(|e| {
        panic!(
            "failed to read OCI config at {}: {e}",
            bundle_config.display()
        )
    });
    let config: serde_json::Value = serde_json::from_slice(&config_bytes).unwrap();
    let env_arr = config["process"]["env"]
        .as_array()
        .expect("process.env should be an array");
    let env_strs: Vec<&str> = env_arr.iter().filter_map(|v| v.as_str()).collect();
    for (var, expected) in [
        ("APP_ENV", "production"),
        ("APP_PORT", "8080"),
        ("APP_NAME", "my-service"),
    ] {
        let needle = format!("{var}={expected}");
        assert!(
            env_strs.iter().any(|e| *e == needle),
            "OCI config env should contain '{needle}', got: {env_strs:?}"
        );
    }

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "env-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "env-test teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("env-test")
        .expect("env-test shared VM shutdown should succeed");
}

/// Verify inter-service DNS connectivity within a stack sandbox.
///
/// Two services in the same sandbox should be able to reach each other
/// by service name (via /etc/hosts injection). This tests the full
/// network isolation + connectivity path.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_inter_service_connectivity() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  server:
    image: alpine:latest
    command: ["sleep", "300"]

  client:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - server
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "net-conn").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);
    assert_eq!(result.services_ready, 2);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("net-conn")
        .unwrap();

    let client_cid = observed
        .iter()
        .find(|o| o.replica.service_name == "client")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // Ping server by hostname from client container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        client_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "5".into(),
            "server".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "ping server by hostname should succeed: stdout={stdout}, stderr={stderr}"
    );

    // Verify /etc/hosts contains the server entry.
    let (exit_code, stdout, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(client_cid, vec!["cat".into(), "/etc/hosts".into()]);
    assert_eq!(exit_code, 0, "reading /etc/hosts should succeed");
    assert!(
        stdout.contains("server"),
        "/etc/hosts should contain 'server' entry: {stdout}"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "net-conn".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "net-conn teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("net-conn")
        .expect("net-conn shared VM shutdown should succeed");
}

/// Verify stack handles service update (config change → recreate).
///
/// Deploy a service, then change its environment and redeploy.
/// The orchestrator should detect the config drift and recreate.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_service_config_change_triggers_recreate() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    // Any config change (env, image, command, mounts, etc.) triggers
    // ServiceRecreate in the reconciler via full config digest comparison.
    let yaml_v1 = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      VERSION: "1"
"#;

    let yaml_v2 = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    environment:
      VERSION: "2"
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    // Deploy v1.
    let spec_v1 = parse_compose(yaml_v1, "update-test").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let r1 = orchestrator.run(&spec_v1, None).unwrap();
    assert!(r1.converged);
    assert_eq!(r1.services_ready, 1);

    let observed1 = orchestrator
        .executor()
        .store()
        .load_observed_state("update-test")
        .unwrap();
    let cid1 = observed1[0].container_id.clone().unwrap();

    // Deploy v2 (same stack name, different env).
    // The reconciler detects the config change via full service config digest
    // and triggers ServiceRecreate (stop + remove + create).
    let spec_v2 = parse_compose(yaml_v2, "update-test").unwrap();
    let r2 = orchestrator.run(&spec_v2, None).unwrap();
    assert!(r2.converged);
    assert_eq!(r2.services_ready, 1);

    let observed2 = orchestrator
        .executor()
        .store()
        .load_observed_state("update-test")
        .unwrap();
    let cid2 = observed2[0].container_id.clone().unwrap();

    // Container should have been recreated. Verify via stop event.
    let _ = cid1; // used below for conceptual clarity
    let events = orchestrator
        .executor()
        .store()
        .load_events("update-test")
        .unwrap();
    let stop_count = events
        .iter()
        .filter(|e| matches!(e, StackEvent::ServiceStopped { service_name, .. } if service_name == "app"))
        .count();
    assert!(
        stop_count >= 1,
        "env change should trigger service recreate (stop+create), got {stop_count} stop events"
    );

    // Verify the recreated container's OCI bundle has VERSION=2.
    let config_path = oci_data.join(format!(
        "rootfs/{cid2}/run/vz-oci/bundles/{cid2}/config.json"
    ));
    let config_bytes = std::fs::read(&config_path)
        .unwrap_or_else(|e| panic!("read OCI config {}: {e}", config_path.display()));
    let config: serde_json::Value = serde_json::from_slice(&config_bytes).unwrap();
    let env_arr = config["process"]["env"]
        .as_array()
        .expect("process.env should be an array");
    let env_strs: Vec<&str> = env_arr.iter().filter_map(|v| v.as_str()).collect();
    assert!(
        env_strs.iter().any(|e| *e == "VERSION=2"),
        "recreated container should have VERSION=2, got: {env_strs:?}"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "update-test".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "update-test teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("update-test")
        .expect("update-test shared VM shutdown should succeed");
}

/// Verify three-service dependency chain: C depends on B, B depends on A.
///
/// Tests that services are created in correct topological order.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn three_service_dependency_chain() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  db:
    image: alpine:latest
    command: ["sleep", "300"]

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - db

  frontend:
    image: alpine:latest
    command: ["sleep", "300"]
    depends_on:
      - api
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "chain-3").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(
        result.converged,
        "3-service chain should converge: ready={}, failed={}, rounds={}",
        result.services_ready, result.services_failed, result.rounds
    );
    assert_eq!(result.services_ready, 3, "all 3 services should be ready");

    // Verify all services are alive.
    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("chain-3")
        .unwrap();
    assert_eq!(observed.len(), 3, "should have 3 observed services");
    for name in &["db", "api", "frontend"] {
        let svc = observed
            .iter()
            .find(|o| o.replica.service_name == *name)
            .unwrap_or_else(|| panic!("{name} should exist"));
        assert!(
            svc.container_id.is_some(),
            "{name} should have a container ID"
        );
    }

    // Verify events show correct ordering.
    let events = orchestrator
        .executor()
        .store()
        .load_events("chain-3")
        .unwrap();
    let creating_events: Vec<&str> = events
        .iter()
        .filter_map(|e| {
            if let StackEvent::ServiceCreating { service_name, .. } = e {
                Some(service_name.as_str())
            } else {
                None
            }
        })
        .collect();
    // db should appear before api, api before frontend.
    let db_pos = creating_events.iter().position(|n| *n == "db");
    let api_pos = creating_events.iter().position(|n| *n == "api");
    let fe_pos = creating_events.iter().position(|n| *n == "frontend");
    if let (Some(db), Some(api), Some(fe)) = (db_pos, api_pos, fe_pos) {
        assert!(
            db < api,
            "db should be created before api: db={db}, api={api}"
        );
        assert!(
            api < fe,
            "api should be created before frontend: api={api}, frontend={fe}"
        );
    }

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "chain-3".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "chain-3 teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("chain-3")
        .expect("chain-3 shared VM shutdown should succeed");
}

/// Verify exec_with_output works correctly through the stack runtime bridge.
///
/// Deploys a service and exercises exec_with_output with various commands
/// to verify stdout, stderr, and exit code capture.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn stack_exec_with_output_capture() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  runner:
    image: alpine:latest
    command: ["sleep", "300"]
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "exec-out").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let mut orchestrator =
        StackOrchestrator::new(executor, reconcile_store, OrchestrationConfig::default());

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("exec-out")
        .unwrap();
    let cid = observed[0].container_id.as_ref().unwrap();

    // Test 1: stdout capture.
    let (exit_code, stdout, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(cid, vec!["echo".into(), "hello-world".into()]);
    assert_eq!(exit_code, 0);
    assert_eq!(stdout.trim(), "hello-world");

    // Test 2: non-zero exit code.
    let (exit_code, _, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(cid, vec!["sh".into(), "-c".into(), "exit 42".into()]);
    assert_eq!(exit_code, 42, "should capture non-zero exit code");

    // Test 3: multi-line output.
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        cid,
        vec![
            "sh".into(),
            "-c".into(),
            "echo line1 && echo line2 && echo line3".into(),
        ],
    );
    assert_eq!(exit_code, 0);
    let lines: Vec<&str> = stdout.trim().lines().collect();
    assert_eq!(lines.len(), 3, "should have 3 lines: {stdout}");
    assert_eq!(lines[0], "line1");
    assert_eq!(lines[1], "line2");
    assert_eq!(lines[2], "line3");

    // Test 4: write file then read it back (filesystem persistence within container).
    let (exit_code, _, _) = orchestrator.executor().runtime().exec_with_output(
        cid,
        vec![
            "sh".into(),
            "-c".into(),
            "echo 'persistent-data' > /tmp/test-file".into(),
        ],
    );
    assert_eq!(exit_code, 0);
    let (exit_code, stdout, _) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(cid, vec!["cat".into(), "/tmp/test-file".into()]);
    assert_eq!(exit_code, 0);
    assert_eq!(stdout.trim(), "persistent-data");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "exec-out".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "exec-out teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("exec-out")
        .expect("exec-out shared VM shutdown should succeed");
}

/// Deploy a service with replicas=3 and verify 3 running containers with distinct IDs.
#[tokio::test(flavor = "multi_thread")]
async fn replicated_service_creates_multiple_containers() {
    if !require_virtualization_entitlement() {
        return;
    }

    let yaml = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    deploy:
      replicas: 3
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec = parse_compose(yaml, "replica-e2e").unwrap();
    assert_eq!(spec.services.len(), 1);
    assert_eq!(spec.services[0].resources.replicas, 3);

    let bridge = OciContainerRuntime::new(&oci_data);

    // Pre-pull image so parallel replica creation doesn't race on layer extraction.
    bridge.pull("alpine:latest").unwrap();

    let store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, store, tmp.path());

    // Reconcile and execute.
    let health = HashMap::new();
    let result = apply(&spec, executor.store(), &health).unwrap();
    assert_eq!(result.actions.len(), 1);
    assert!(matches!(
        &result.actions[0],
        Action::ServiceCreate { target } if target.service_name == "web"
    ));

    let exec_result = executor.execute(&spec, &result.actions).unwrap();
    assert_eq!(
        exec_result.failed, 0,
        "all replicas should succeed: {:?}",
        exec_result.errors
    );

    // Verify 3 running replicas in observed state.
    let observed = executor.store().load_observed_state("replica-e2e").unwrap();
    let running: Vec<&str> = observed
        .iter()
        .filter(|o| o.container_id.is_some() && matches!(o.phase, ServicePhase::Running))
        .map(|o| o.replica.service_name.as_str())
        .collect();
    assert_eq!(
        running.len(),
        3,
        "expected 3 running replicas, got: {running:?}"
    );

    // Each replica should have a distinct container_id.
    let cids: std::collections::HashSet<&str> = observed
        .iter()
        .filter_map(|o| o.container_id.as_deref())
        .collect();
    assert_eq!(cids.len(), 3, "expected 3 distinct container IDs");

    // Second reconcile should be a no-op (converged).
    let result2 = apply(&spec, executor.store(), &health).unwrap();
    assert!(
        result2.actions.is_empty(),
        "converged replicas should produce no actions, got: {:?}",
        result2.actions
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "replica-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = apply(&down_spec, executor.store(), &health).unwrap();
    let execution = executor
        .execute(&down_spec, &down_result.actions)
        .expect("replica teardown execution should return");
    assert!(execution.all_succeeded(), "replica teardown should succeed");
    executor
        .runtime()
        .shutdown_sandbox("replica-e2e")
        .expect("replica shared VM shutdown should succeed");
}

/// Deploy replicas=3, then redeploy with replicas=1 and verify scale-down.
#[tokio::test(flavor = "multi_thread")]
async fn replicated_service_scale_down() {
    if !require_virtualization_entitlement() {
        return;
    }

    let yaml_3 = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    deploy:
      replicas: 3
"#;

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");
    let oci_data = tmp.path().join("oci-data");
    std::fs::create_dir_all(&oci_data).unwrap();

    let spec3 = parse_compose(yaml_3, "scale-e2e").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);

    // Pre-pull image so parallel replica creation doesn't race on layer extraction.
    bridge.pull("alpine:latest").unwrap();

    let store = StateStore::open(&db_path).unwrap();
    let mut executor = StackExecutor::new(bridge, store, tmp.path());

    // Deploy with replicas=3.
    let health = HashMap::new();
    let r1 = apply(&spec3, executor.store(), &health).unwrap();
    let er1 = executor.execute(&spec3, &r1.actions).unwrap();
    assert_eq!(
        er1.failed, 0,
        "initial deploy should succeed: {:?}",
        er1.errors
    );

    let observed = executor.store().load_observed_state("scale-e2e").unwrap();
    let running_count = observed
        .iter()
        .filter(|o| matches!(o.phase, ServicePhase::Running))
        .count();
    assert_eq!(running_count, 3, "should have 3 running replicas");

    // Redeploy with replicas=1 → should scale down.
    let yaml_1 = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    deploy:
      replicas: 1
"#;
    let spec1 = parse_compose(yaml_1, "scale-e2e").unwrap();
    let r2 = apply(&spec1, executor.store(), &health).unwrap();

    // Should remove web-2 and web-3.
    let remove_names: Vec<&str> = r2
        .actions
        .iter()
        .filter_map(|a| match a {
            Action::ServiceRemove { target } => Some(target.service_name.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(
        remove_names.len(),
        2,
        "should remove 2 excess replicas, got: {remove_names:?}"
    );

    let er2 = executor.execute(&spec1, &r2.actions).unwrap();
    assert_eq!(er2.failed, 0, "scale-down should succeed: {:?}", er2.errors);

    // Verify only 1 running replica remains.
    let observed2 = executor.store().load_observed_state("scale-e2e").unwrap();
    let still_running: Vec<&str> = observed2
        .iter()
        .filter(|o| matches!(o.phase, ServicePhase::Running))
        .map(|o| o.replica.service_name.as_str())
        .collect();
    assert_eq!(
        still_running,
        vec!["web"],
        "only base replica should remain running"
    );

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "scale-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down_result = apply(&down_spec, executor.store(), &health).unwrap();
    let execution = executor
        .execute(&down_spec, &down_result.actions)
        .expect("scale teardown execution should return");
    assert!(execution.all_succeeded(), "scale teardown should succeed");
    executor
        .runtime()
        .shutdown_sandbox("scale-e2e")
        .expect("scale shared VM shutdown should succeed");
}

// ────────────────────────────────────────────────────────────────────────────
// Sandbox real-world scenario tests: volumes, secrets, env_file, multi-network
// ────────────────────────────────────────────────────────────────────────────

/// Verify bind mounts and named volumes work inside sandbox containers.
///
/// 1. Bind mount: a host file is visible inside the container.
/// 2. Named volume: data written by one container survives a recreate.
/// 3. Shared named volume: two services can read each other's writes.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_bind_mount_and_named_volume() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let tmp = tempfile::tempdir().unwrap();

    // Create a host file for bind mounting.
    let bind_dir = tmp.path().join("bind-src");
    std::fs::create_dir_all(&bind_dir).unwrap();
    std::fs::write(bind_dir.join("hello.txt"), "bind-mount-works").unwrap();

    let bind_dir_str = bind_dir.to_str().unwrap();

    let yaml = format!(
        r#"
services:
  writer:
    image: alpine:latest
    command: ["sleep", "300"]
    volumes:
      - {bind_dir}:/mnt/host:ro
      - shared:/mnt/shared

  reader:
    image: alpine:latest
    command: ["sleep", "300"]
    volumes:
      - shared:/mnt/shared
    depends_on:
      - writer

volumes:
  shared:
"#,
        bind_dir = bind_dir_str
    );

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let db_path = tmp.path().join("state.db");
    let spec = parse_compose(&yaml, "vol-e2e").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 2);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("vol-e2e")
        .unwrap();

    let writer_cid = observed
        .iter()
        .find(|o| o.replica.service_name == "writer")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();
    let reader_cid = observed
        .iter()
        .find(|o| o.replica.service_name == "reader")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // 1. Verify bind mount: host file visible inside writer container.
    let (exit_code, stdout, stderr) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(writer_cid, vec!["cat".into(), "/mnt/host/hello.txt".into()]);
    assert_eq!(
        exit_code, 0,
        "reading bind-mounted file should succeed: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "bind-mount-works");

    // 2. Write data to the shared named volume from writer.
    let (exit_code, _, stderr) = orchestrator.executor().runtime().exec_with_output(
        writer_cid,
        vec![
            "sh".into(),
            "-c".into(),
            "echo volume-data-from-writer > /mnt/shared/data.txt".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "writing to shared volume should succeed: stderr={stderr}"
    );

    // 3. Read the shared data from reader container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        reader_cid,
        vec!["cat".into(), "/mnt/shared/data.txt".into()],
    );
    assert_eq!(
        exit_code, 0,
        "reader should see writer's data: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "volume-data-from-writer");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "vol-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "vol-e2e teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("vol-e2e")
        .expect("vol-e2e shared VM shutdown should succeed");
}

/// Verify file-based secrets are injected at /run/secrets/<name> inside sandbox containers.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_secret_injection() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let tmp = tempfile::tempdir().unwrap();

    // Stage secret files on the host.
    let secrets_src = tmp.path().join("secret-files");
    std::fs::create_dir_all(&secrets_src).unwrap();
    std::fs::write(secrets_src.join("db_password"), "s3cret-p@ss!").unwrap();
    std::fs::write(secrets_src.join("api_key"), "ak-1234567890").unwrap();

    let db_pw_path = secrets_src.join("db_password");
    let api_key_path = secrets_src.join("api_key");

    let yaml = format!(
        r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    secrets:
      - db_password
      - api_key

secrets:
  db_password:
    file: {db_pw}
  api_key:
    file: {api_key}
"#,
        db_pw = db_pw_path.to_str().unwrap(),
        api_key = api_key_path.to_str().unwrap(),
    );

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let db_path = tmp.path().join("state.db");
    let spec = parse_compose(&yaml, "secret-e2e").unwrap();
    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 1);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("secret-e2e")
        .unwrap();

    let app_cid = observed
        .iter()
        .find(|o| o.replica.service_name == "app")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // Verify db_password secret is readable inside the container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["cat".into(), "/run/secrets/db_password".into()],
    );
    assert_eq!(
        exit_code, 0,
        "reading /run/secrets/db_password should succeed: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "s3cret-p@ss!");

    // Verify api_key secret.
    let (exit_code, stdout, stderr) = orchestrator
        .executor()
        .runtime()
        .exec_with_output(app_cid, vec!["cat".into(), "/run/secrets/api_key".into()]);
    assert_eq!(
        exit_code, 0,
        "reading /run/secrets/api_key should succeed: stderr={stderr}"
    );
    assert_eq!(stdout.trim(), "ak-1234567890");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "secret-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "secret-e2e teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("secret-e2e")
        .expect("secret-e2e shared VM shutdown should succeed");
}

/// Verify env_file variables are loaded and inline environment takes precedence.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_env_file_loading() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let tmp = tempfile::tempdir().unwrap();
    let compose_dir = tmp.path().join("compose");
    std::fs::create_dir_all(&compose_dir).unwrap();

    // Write env file with base values.
    std::fs::write(
        compose_dir.join("app.env"),
        "DB_HOST=db.internal\nDB_PORT=5432\nLOG_LEVEL=info\n",
    )
    .unwrap();

    let yaml = r#"
services:
  app:
    image: alpine:latest
    command: ["sleep", "300"]
    env_file:
      - app.env
    environment:
      LOG_LEVEL: debug
      CUSTOM_VAR: injected
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let db_path = tmp.path().join("state.db");
    // Use parse_compose_with_dir so env_file paths are resolved.
    let spec = parse_compose_with_dir(yaml, "envfile-e2e", &compose_dir).unwrap();

    // Verify the parsed spec merged env_file + inline environment.
    let app_spec = &spec.services[0];
    assert_eq!(
        app_spec.environment.get("DB_HOST").map(String::as_str),
        Some("db.internal"),
        "DB_HOST should come from env_file"
    );
    assert_eq!(
        app_spec.environment.get("LOG_LEVEL").map(String::as_str),
        Some("debug"),
        "inline environment should override env_file"
    );

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 1);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("envfile-e2e")
        .unwrap();

    let app_cid = observed
        .iter()
        .find(|o| o.replica.service_name == "app")
        .unwrap()
        .container_id
        .as_ref()
        .unwrap();

    // Verify env_file variable is present inside the running container.
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["sh".into(), "-c".into(), "echo $DB_HOST".into()],
    );
    assert_eq!(exit_code, 0, "echo DB_HOST should succeed: stderr={stderr}");
    assert_eq!(stdout.trim(), "db.internal");

    // Verify inline env overrides env_file.
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["sh".into(), "-c".into(), "echo $LOG_LEVEL".into()],
    );
    assert_eq!(exit_code, 0);
    assert_eq!(
        stdout.trim(),
        "debug",
        "inline environment should override env_file value"
    );

    // Verify purely-inline variable.
    let (exit_code, stdout, _) = orchestrator.executor().runtime().exec_with_output(
        app_cid,
        vec!["sh".into(), "-c".into(), "echo $CUSTOM_VAR".into()],
    );
    assert_eq!(exit_code, 0);
    assert_eq!(stdout.trim(), "injected");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "envfile-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "envfile-e2e teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("envfile-e2e")
        .expect("envfile-e2e shared VM shutdown should succeed");
}

/// Verify multi-network isolation: services on different networks cannot reach each other.
///
/// Topology:
///   - frontend network: service `web`
///   - backend network: service `api`, service `db`
///   - `api` is on both networks (bridge between frontend and backend)
///
/// Expected:
///   - `web` can ping `api` (both on frontend)
///   - `api` can ping `db` (both on backend)
///   - `web` CANNOT ping `db` (different networks, no shared membership)
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
async fn sandbox_multi_network_isolation() {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let yaml = r#"
services:
  web:
    image: alpine:latest
    command: ["sleep", "300"]
    networks:
      - frontend

  api:
    image: alpine:latest
    command: ["sleep", "300"]
    networks:
      - frontend
      - backend

  db:
    image: alpine:latest
    command: ["sleep", "300"]
    networks:
      - backend

networks:
  frontend:
  backend:
"#;

    let oci_data = stack_e2e_oci_data_dir();
    std::fs::create_dir_all(&oci_data).unwrap();

    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("state.db");

    let spec = parse_compose(yaml, "multinet-e2e").unwrap();
    assert_eq!(spec.networks.len(), 2, "should have 2 networks");

    let bridge = OciContainerRuntime::new(&oci_data);
    let exec_store = StateStore::open(&db_path).unwrap();
    let reconcile_store = StateStore::open(&db_path).unwrap();
    let executor = StackExecutor::new(bridge, exec_store, tmp.path());

    let orch_config = OrchestrationConfig {
        poll_interval: Some(2),
        max_rounds: 20,
        image_policy: ImagePolicy::AllowAll,
    };
    let mut orchestrator = StackOrchestrator::new(executor, reconcile_store, orch_config);

    let result = orchestrator.run(&spec, None).unwrap();
    assert!(result.converged, "stack should converge");
    assert_eq!(result.services_ready, 3);

    let observed = orchestrator
        .executor()
        .store()
        .load_observed_state("multinet-e2e")
        .unwrap();

    let cid_of = |name: &str| -> String {
        observed
            .iter()
            .find(|o| o.replica.service_name == name)
            .unwrap()
            .container_id
            .clone()
            .unwrap()
    };
    let web_cid = cid_of("web");
    let api_cid = cid_of("api");
    let db_cid = cid_of("db");

    let assert_all_services_alive = |phase: &str| {
        for (service, container_id) in [
            ("web", web_cid.as_str()),
            ("api", api_cid.as_str()),
            ("db", db_cid.as_str()),
        ] {
            let result = orchestrator.executor().runtime().try_exec_with_output(
                container_id,
                vec![
                    "/bin/busybox".into(),
                    "kill".into(),
                    "-0".into(),
                    "1".into(),
                ],
            );
            let (exit_code, stdout, stderr) = result.unwrap_or_else(|error| {
                panic!(
                    "{service} must remain alive during {phase}: container={container_id}, error={error}"
                )
            });
            assert_eq!(
                exit_code, 0,
                "{service} must remain alive during {phase}: container={container_id}, stdout={stdout}, stderr={stderr}"
            );
        }
    };

    assert_all_services_alive("immediate post-convergence validation");
    std::thread::sleep(Duration::from_millis(250));
    assert_all_services_alive("delayed post-convergence validation");

    // web → api should succeed (both on frontend network).
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        &web_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "5".into(),
            "api".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "web should reach api (same frontend network): stdout={stdout}, stderr={stderr}"
    );

    // api → db should succeed (both on backend network).
    let (exit_code, stdout, stderr) = orchestrator.executor().runtime().exec_with_output(
        &api_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "5".into(),
            "db".into(),
        ],
    );
    assert_eq!(
        exit_code, 0,
        "api should reach db (same backend network): stdout={stdout}, stderr={stderr}"
    );

    // web → db should FAIL (different networks, no shared membership).
    // ping with a short timeout; non-zero exit means no connectivity.
    let (exit_code, _, _) = orchestrator.executor().runtime().exec_with_output(
        &web_cid,
        vec![
            "/bin/busybox".into(),
            "ping".into(),
            "-c".into(),
            "1".into(),
            "-W".into(),
            "2".into(),
            "db".into(),
        ],
    );
    assert_ne!(exit_code, 0, "web should NOT reach db (different networks)");

    // Teardown.
    let down_spec = vz_stack::StackSpec {
        name: "multinet-e2e".to_string(),
        services: vec![],
        networks: vec![],
        volumes: vec![],
        secrets: vec![],
        disk_size_mb: None,
    };
    let down = orchestrator.run(&down_spec, None).unwrap();
    assert!(down.converged, "multinet-e2e teardown should converge");
    orchestrator
        .executor()
        .runtime()
        .shutdown_sandbox("multinet-e2e")
        .expect("multinet-e2e shared VM shutdown should succeed");
}

/// Prove a durable Environment journal fences real VM stop/up/delete work,
/// survives store reopen, preserves the selected Machine incarnation and disk,
/// and never mutates a sibling Environment in the same Project.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires Apple Silicon + Linux kernel artifacts"]
#[allow(clippy::expect_used)]
async fn environment_lifecycle_journal_linux_vm_stop_up_delete_recovers_without_cross_environment_damage()
 {
    if !require_virtualization_entitlement() {
        return;
    }
    let _ = tracing_subscriber::fmt()
        .with_env_filter("info,vz_oci_macos=debug,vz_linux=debug,vz_stack=debug")
        .with_test_writer()
        .try_init();

    let tmp = tempfile::tempdir().expect("Environment lifecycle tempdir should be created");
    let db_path = tmp.path().join("environment-lifecycle.db");
    let target_disk = tmp.path().join("target-machine.img");
    let sibling_disk = tmp.path().join("sibling-machine.img");
    for path in [&target_disk, &sibling_disk] {
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .unwrap_or_else(|error| panic!("create sparse disk {}: {error}", path.display()));
        file.set_len(512 * 1024 * 1024)
            .unwrap_or_else(|error| panic!("size sparse disk {}: {error}", path.display()));
    }

    let project_id = ProjectId::new("prj_lifecycle_mac_e2e").unwrap();
    let target_environment_id = EnvironmentId::new("env_lifecycle_target").unwrap();
    let sibling_environment_id = EnvironmentId::new("env_lifecycle_sibling").unwrap();
    let target_machine_id = MachineId::new("mch_lifecycle_target").unwrap();
    let sibling_machine_id = MachineId::new("mch_lifecycle_sibling").unwrap();
    let target_incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_lifecycle_target_g1").unwrap(),
        machine_id: target_machine_id.clone(),
        generation: 1,
        created_at: 100,
    };
    let sibling_incarnation = MachineIncarnation {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        incarnation_id: MachineIncarnationId::new("inc_lifecycle_sibling_g1").unwrap(),
        machine_id: sibling_machine_id.clone(),
        generation: 1,
        created_at: 101,
    };
    let target_owner = ResourceOwner {
        project_id: project_id.clone(),
        environment_id: target_environment_id.clone(),
        machine_id: Some(target_machine_id.clone()),
    };
    let sibling_owner = ResourceOwner {
        project_id: project_id.clone(),
        environment_id: sibling_environment_id.clone(),
        machine_id: Some(sibling_machine_id.clone()),
    };
    let target_backend_key = target_owner
        .bounded_resource_name(&OwnedResourceKind::Machine, "runtime-vm", 64)
        .unwrap();
    let sibling_backend_key = sibling_owner
        .bounded_resource_name(&OwnedResourceKind::Machine, "runtime-vm", 64)
        .unwrap();
    let target_disk_resource = target_owner
        .bounded_resource_name(&OwnedResourceKind::Disk, "root-disk", 64)
        .unwrap();
    let sibling_disk_resource = sibling_owner
        .bounded_resource_name(&OwnedResourceKind::Disk, "root-disk", 64)
        .unwrap();
    assert_ne!(target_backend_key, sibling_backend_key);
    assert_ne!(target_disk_resource, sibling_disk_resource);

    let target = TargetSpec {
        os: OperatingSystem::Linux,
        arch: Architecture::Aarch64,
        image: "vz-linux-developer-bundle".to_string(),
        version: Some("0.4.0-e2e".to_string()),
        channel: Some("local".to_string()),
        digest: Some("sha256:environment-lifecycle-e2e".to_string()),
    };
    let capabilities = CapabilitySet::new([
        MachineCapability::PosixExec,
        MachineCapability::PosixPty,
        MachineCapability::Signals,
        MachineCapability::Files,
        MachineCapability::DockerEngine,
        MachineCapability::Compose,
        MachineCapability::Buildx,
    ]);
    let definition = ProjectDefinition {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        project_id: project_id.clone(),
        name: "environment-lifecycle-e2e".to_string(),
        environment: EnvironmentSpec {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            machines: vec![MachineSpec {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target: target.clone(),
                resources: MachineResources {
                    cpus: Some(2),
                    memory_mb: Some(1024),
                    disk_bytes: Some(512 * 1024 * 1024),
                },
                requested_capabilities: capabilities.clone(),
                workspace: None,
            }],
            networks: vec![],
            endpoints: vec![],
        },
    };
    let definition_digest = definition.digest().unwrap();
    let make_environment = |environment_id: EnvironmentId,
                            machine_id: MachineId,
                            incarnation: MachineIncarnation,
                            name: &str,
                            disk_resource: String,
                            created_at: u64| {
        EnvironmentInstance {
            schema_version: TOPOLOGY_SCHEMA_VERSION,
            environment_id: environment_id.clone(),
            project_id: project_id.clone(),
            name: name.to_string(),
            definition_digest: definition_digest.clone(),
            state: EnvironmentState::Stopped,
            lifecycle_generation: 0,
            active_operation_id: None,
            bindings: vec![],
            machines: vec![MachineInstance {
                schema_version: TOPOLOGY_SCHEMA_VERSION,
                machine_id: machine_id.clone(),
                environment_id: environment_id.clone(),
                name: "linux".to_string(),
                profile: MachineProfile::Developer,
                target: target.clone(),
                resources: MachineResources {
                    cpus: Some(2),
                    memory_mb: Some(1024),
                    disk_bytes: Some(512 * 1024 * 1024),
                },
                requested_capabilities: capabilities.clone(),
                negotiated_capabilities: capabilities.clone(),
                backend: Some(MachineBackend::MacosVirtualizationLinux),
                incarnation: Some(incarnation.clone()),
                state: MachineState::Stopped,
                legacy_sandbox_id: None,
            }],
            networks: vec![],
            endpoints: vec![],
            ownership: vec![
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Machine,
                    resource_id: machine_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Incarnation,
                    resource_id: incarnation.incarnation_id.to_string(),
                    environment_id: environment_id.clone(),
                    machine_id: Some(machine_id.clone()),
                },
                OwnershipRecord {
                    schema_version: TOPOLOGY_SCHEMA_VERSION,
                    resource_kind: OwnedResourceKind::Disk,
                    resource_id: disk_resource,
                    environment_id,
                    machine_id: Some(machine_id),
                },
            ],
            legacy_migration: None,
            created_at,
            updated_at: created_at,
        }
    };
    let initial_state = ProjectState {
        schema_version: TOPOLOGY_SCHEMA_VERSION,
        definition,
        environments: vec![
            make_environment(
                target_environment_id.clone(),
                target_machine_id.clone(),
                target_incarnation.clone(),
                "target",
                target_disk_resource.clone(),
                200,
            ),
            make_environment(
                sibling_environment_id.clone(),
                sibling_machine_id.clone(),
                sibling_incarnation.clone(),
                "sibling",
                sibling_disk_resource.clone(),
                201,
            ),
        ],
    };
    initial_state
        .validate()
        .expect("E2E topology should validate");

    let oci_data = stack_e2e_oci_data_dir();
    let runtime = OciContainerRuntime::new(&oci_data);
    let _physical_cleanup = EnvironmentLifecyclePhysicalCleanup {
        runtime: runtime.clone(),
        backend_keys: vec![target_backend_key.clone(), sibling_backend_key.clone()],
        disk_paths: vec![target_disk.clone(), sibling_disk.clone()],
    };
    let store = StateStore::open(&db_path).expect("lifecycle StateStore should open");
    store
        .save_project_state(&initial_state)
        .expect("lifecycle Project should bootstrap");

    let mut clock = 1_000_u64;
    let mut boot_invocations = 0_u64;
    let mut shutdown_invocations = 0_u64;
    let mut disk_remove_attempts = 0_u64;
    let mut disk_removed = 0_u64;
    let mut disk_already_absent = 0_u64;
    let mut operations = Vec::new();

    let target_up = store
        .begin_environment_lifecycle(
            target_environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "request-target-up-initial",
            "idempotency-target-up-initial",
            &environment_lifecycle_sha256("request-target-up-initial"),
            clock,
        )
        .expect("target Up journal should begin");
    clock += 1;
    assert_eq!(target_up.status, EnvironmentLifecycleStatus::Running);
    assert_eq!(
        target_up.machine_steps[0].status,
        LifecycleStepStatus::Pending
    );
    boot_invocations += 1;
    runtime
        .create_sandbox(
            &target_backend_key,
            vec![],
            StackResourceHint {
                cpus: Some(2),
                memory_mb: Some(1024),
                disk_image_path: Some(target_disk.clone()),
                ..StackResourceHint::default()
            },
        )
        .expect("target VM should boot");
    let target_up = store
        .acknowledge_environment_machine_step(
            &environment_lifecycle_machine_ack(
                &target_up,
                target_up.machine_steps[0].expected_incarnation.clone(),
            ),
            clock,
        )
        .expect("target Up Machine step should persist");
    clock += 1;
    let target_up = store
        .finish_environment_lifecycle(target_up.operation_id.as_str(), target_up.generation, clock)
        .expect("target Up should finish");
    clock += 1;
    operations.push(environment_lifecycle_operation_evidence(
        "target_initial_up",
        &target_up,
    ));

    let sibling_up = store
        .begin_environment_lifecycle(
            sibling_environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "request-sibling-up-initial",
            "idempotency-sibling-up-initial",
            &environment_lifecycle_sha256("request-sibling-up-initial"),
            clock,
        )
        .expect("sibling Up journal should begin");
    clock += 1;
    boot_invocations += 1;
    runtime
        .create_sandbox(
            &sibling_backend_key,
            vec![],
            StackResourceHint {
                cpus: Some(2),
                memory_mb: Some(1024),
                disk_image_path: Some(sibling_disk.clone()),
                ..StackResourceHint::default()
            },
        )
        .expect("sibling VM should boot");
    let sibling_up = store
        .acknowledge_environment_machine_step(
            &environment_lifecycle_machine_ack(
                &sibling_up,
                sibling_up.machine_steps[0].expected_incarnation.clone(),
            ),
            clock,
        )
        .expect("sibling Up Machine step should persist");
    clock += 1;
    let sibling_up = store
        .finish_environment_lifecycle(
            sibling_up.operation_id.as_str(),
            sibling_up.generation,
            clock,
        )
        .expect("sibling Up should finish");
    clock += 1;
    operations.push(environment_lifecycle_operation_evidence(
        "sibling_initial_up",
        &sibling_up,
    ));

    let target_sentinel = "target-environment-lifecycle-sentinel-v1";
    let sibling_sentinel = "sibling-environment-lifecycle-sentinel-v1";
    let target_boot_initial = environment_lifecycle_guest_exec(
        &runtime,
        &target_backend_key,
        &format!(
            "printf '%s' '{target_sentinel}' > /run/vz-oci/volumes/lifecycle-sentinel && sync && cat /proc/sys/kernel/random/boot_id"
        ),
    );
    let sibling_boot_initial = environment_lifecycle_guest_exec(
        &runtime,
        &sibling_backend_key,
        &format!(
            "printf '%s' '{sibling_sentinel}' > /run/vz-oci/volumes/lifecycle-sentinel && sync && cat /proc/sys/kernel/random/boot_id"
        ),
    );
    assert!(!target_boot_initial.is_empty());
    assert!(!sibling_boot_initial.is_empty());

    let sibling_snapshot =
        environment_lifecycle_environment(&store, &project_id, &sibling_environment_id);
    let sibling_aggregate_bytes = serde_json::to_vec(&sibling_snapshot).unwrap();
    let sibling_ownership_bytes = serde_json::to_vec(&sibling_snapshot.ownership).unwrap();

    let target_stop = store
        .begin_environment_lifecycle(
            target_environment_id.as_str(),
            EnvironmentLifecycleKind::Stop,
            "request-target-stop",
            "idempotency-target-stop",
            &environment_lifecycle_sha256("request-target-stop"),
            clock,
        )
        .expect("target Stop journal should begin");
    clock += 1;
    shutdown_invocations += 1;
    runtime
        .shutdown_sandbox(&target_backend_key)
        .expect("target VM should stop");
    assert!(!runtime.has_sandbox(&target_backend_key));
    assert!(runtime.has_sandbox(&sibling_backend_key));
    let sibling_boot_during_target_stop = environment_lifecycle_guest_exec(
        &runtime,
        &sibling_backend_key,
        "cat /proc/sys/kernel/random/boot_id",
    );
    assert_eq!(sibling_boot_during_target_stop, sibling_boot_initial);
    let target_stop = store
        .acknowledge_environment_machine_step(
            &environment_lifecycle_machine_ack(&target_stop, None),
            clock,
        )
        .expect("target Stop Machine step should persist");
    clock += 1;
    let target_stop = store
        .finish_environment_lifecycle(
            target_stop.operation_id.as_str(),
            target_stop.generation,
            clock,
        )
        .expect("target Stop should finish");
    clock += 1;
    operations.push(environment_lifecycle_operation_evidence(
        "target_stop",
        &target_stop,
    ));
    let stopped_target =
        environment_lifecycle_environment(&store, &project_id, &target_environment_id);
    assert_eq!(stopped_target.state, EnvironmentState::Stopped);
    assert_eq!(stopped_target.machines[0].state, MachineState::Stopped);
    assert_eq!(
        stopped_target.machines[0].incarnation.as_ref(),
        Some(&target_incarnation)
    );
    assert!(target_disk.exists());
    let sibling_after_target_stop =
        environment_lifecycle_environment(&store, &project_id, &sibling_environment_id);
    let sibling_aggregate_bytes_equal_after_stop =
        serde_json::to_vec(&sibling_after_target_stop).unwrap() == sibling_aggregate_bytes;
    let sibling_ownership_bytes_equal_after_stop =
        serde_json::to_vec(&sibling_after_target_stop.ownership).unwrap()
            == sibling_ownership_bytes;
    assert!(sibling_aggregate_bytes_equal_after_stop);
    assert!(sibling_ownership_bytes_equal_after_stop);

    let counts_before_stop_replay = (boot_invocations, shutdown_invocations, disk_remove_attempts);
    let target_stop_replay = store
        .begin_environment_lifecycle(
            target_environment_id.as_str(),
            EnvironmentLifecycleKind::Stop,
            "request-target-stop",
            "idempotency-target-stop",
            &environment_lifecycle_sha256("request-target-stop"),
            clock,
        )
        .expect("exact Stop replay should return its terminal journal");
    clock += 1;
    assert_eq!(target_stop_replay, target_stop);
    assert_eq!(
        counts_before_stop_replay,
        (boot_invocations, shutdown_invocations, disk_remove_attempts)
    );
    assert!(
        target_stop_replay
            .machine_steps
            .iter()
            .all(|step| step.status == LifecycleStepStatus::Succeeded)
    );
    let target_stop_replay_pending_steps = target_stop_replay
        .machine_steps
        .iter()
        .filter(|step| {
            matches!(
                step.status,
                LifecycleStepStatus::Pending | LifecycleStepStatus::Running
            )
        })
        .count();
    assert_eq!(target_stop_replay_pending_steps, 0);
    assert!(
        store
            .load_current_environment_lifecycle(target_environment_id.as_str())
            .unwrap()
            .is_none()
    );

    drop(store);
    let store = StateStore::open(&db_path).expect("lifecycle StateStore should reopen");
    assert!(runtime.has_sandbox(&sibling_backend_key));
    assert!(!runtime.has_sandbox(&target_backend_key));
    let target_up_after_reopen = store
        .begin_environment_lifecycle(
            target_environment_id.as_str(),
            EnvironmentLifecycleKind::Up,
            "request-target-up-after-reopen",
            "idempotency-target-up-after-reopen",
            &environment_lifecycle_sha256("request-target-up-after-reopen"),
            clock,
        )
        .expect("target Up after StateStore reopen should begin");
    clock += 1;
    assert_eq!(
        target_up_after_reopen.machine_steps[0]
            .expected_incarnation
            .as_ref(),
        Some(&target_incarnation)
    );
    boot_invocations += 1;
    runtime
        .create_sandbox(
            &target_backend_key,
            vec![],
            StackResourceHint {
                cpus: Some(2),
                memory_mb: Some(1024),
                disk_image_path: Some(target_disk.clone()),
                ..StackResourceHint::default()
            },
        )
        .expect("target VM should boot from its existing disk");
    let target_boot_after_reopen = environment_lifecycle_guest_exec(
        &runtime,
        &target_backend_key,
        "cat /proc/sys/kernel/random/boot_id",
    );
    assert_ne!(target_boot_after_reopen, target_boot_initial);
    let target_sentinel_after_reopen = environment_lifecycle_guest_exec(
        &runtime,
        &target_backend_key,
        "cat /run/vz-oci/volumes/lifecycle-sentinel",
    );
    assert_eq!(target_sentinel_after_reopen, target_sentinel);
    let sibling_boot_after_target_restart = environment_lifecycle_guest_exec(
        &runtime,
        &sibling_backend_key,
        "cat /proc/sys/kernel/random/boot_id",
    );
    assert_eq!(sibling_boot_after_target_restart, sibling_boot_initial);
    let target_up_after_reopen = store
        .acknowledge_environment_machine_step(
            &environment_lifecycle_machine_ack(
                &target_up_after_reopen,
                target_up_after_reopen.machine_steps[0]
                    .expected_incarnation
                    .clone(),
            ),
            clock,
        )
        .expect("reopened target Up Machine step should persist");
    clock += 1;
    let target_up_after_reopen = store
        .finish_environment_lifecycle(
            target_up_after_reopen.operation_id.as_str(),
            target_up_after_reopen.generation,
            clock,
        )
        .expect("reopened target Up should finish");
    clock += 1;
    operations.push(environment_lifecycle_operation_evidence(
        "target_up_after_reopen",
        &target_up_after_reopen,
    ));
    let sibling_after_target_restart =
        environment_lifecycle_environment(&store, &project_id, &sibling_environment_id);
    let sibling_aggregate_bytes_equal_after_restart =
        serde_json::to_vec(&sibling_after_target_restart).unwrap() == sibling_aggregate_bytes;
    let sibling_ownership_bytes_equal_after_restart =
        serde_json::to_vec(&sibling_after_target_restart.ownership).unwrap()
            == sibling_ownership_bytes;
    assert!(sibling_aggregate_bytes_equal_after_restart);
    assert!(sibling_ownership_bytes_equal_after_restart);

    let target_delete = store
        .begin_environment_lifecycle(
            target_environment_id.as_str(),
            EnvironmentLifecycleKind::Delete,
            "request-target-delete",
            "idempotency-target-delete",
            &environment_lifecycle_sha256("request-target-delete"),
            clock,
        )
        .expect("target Delete journal should begin");
    let target_delete_planned_digest = environment_lifecycle_plan_digest(&target_delete);
    clock += 1;
    shutdown_invocations += 1;
    runtime
        .shutdown_sandbox(&target_backend_key)
        .expect("target VM should shut down for delete");
    let target_delete = store
        .acknowledge_environment_machine_step(
            &environment_lifecycle_machine_ack(&target_delete, None),
            clock,
        )
        .expect("target Delete Machine step should persist");
    assert_eq!(
        environment_lifecycle_plan_digest(&target_delete),
        target_delete_planned_digest
    );
    clock += 1;
    disk_remove_attempts += 1;
    match std::fs::remove_file(&target_disk) {
        Ok(()) => disk_removed += 1,
        Err(error) => panic!("target disk should be removed before acknowledgement: {error}"),
    }
    let target_delete_before_reopen = target_delete.clone();
    let target_delete_before_reopen_bytes =
        serde_json::to_vec(&target_delete_before_reopen).unwrap();
    let target_delete_plan_before_reopen =
        environment_lifecycle_plan_digest(&target_delete_before_reopen);
    drop(store);

    let store = StateStore::open(&db_path).expect("StateStore should reopen during Delete");
    let target_delete_after_reopen = store
        .load_resumable_environment_lifecycle(target_environment_id.as_str())
        .expect("resumable target Delete should load")
        .expect("target Delete should remain fenced after reopen");
    let target_delete_operation_byte_equal = serde_json::to_vec(&target_delete_after_reopen)
        .unwrap()
        == target_delete_before_reopen_bytes;
    let target_delete_plan_after_reopen =
        environment_lifecycle_plan_digest(&target_delete_after_reopen);
    let target_disk_step_pending = target_delete_after_reopen.cleanup_steps.iter().any(|step| {
        step.ownership.resource_kind == OwnedResourceKind::Disk
            && step.ownership.resource_id == target_disk_resource
            && step.status == LifecycleStepStatus::Pending
    });
    assert!(target_delete_operation_byte_equal);
    assert_eq!(
        target_delete_after_reopen.operation_id,
        target_delete_before_reopen.operation_id
    );
    assert_eq!(
        target_delete_after_reopen.generation,
        target_delete_before_reopen.generation
    );
    assert_eq!(
        target_delete_plan_after_reopen,
        target_delete_plan_before_reopen
    );
    assert_eq!(
        target_delete_plan_after_reopen,
        target_delete_planned_digest
    );
    assert!(target_disk_step_pending);
    disk_remove_attempts += 1;
    match std::fs::remove_file(&target_disk) {
        Ok(()) => panic!("target disk must already be absent after StateStore reopen"),
        Err(error) if error.kind() == ErrorKind::NotFound => disk_already_absent += 1,
        Err(error) => panic!("idempotent target disk removal failed: {error}"),
    }
    let mut target_delete = target_delete_after_reopen;
    for step in target_delete.cleanup_steps.clone() {
        target_delete = store
            .acknowledge_environment_cleanup_step(
                &OwnershipCleanupStepAcknowledgement {
                    operation_id: target_delete.operation_id.clone(),
                    generation: target_delete.generation,
                    ownership: step.ownership,
                    result: LifecycleStepResult::Succeeded,
                },
                clock,
            )
            .expect("target exact ownership cleanup should persist");
        clock += 1;
    }
    let (target_delete, target_tombstone) = store
        .finish_environment_delete(
            target_delete.operation_id.as_str(),
            target_delete.generation,
            clock,
        )
        .expect("target Delete should finish with a tombstone");
    clock += 1;
    assert_eq!(target_delete.status, EnvironmentLifecycleStatus::Succeeded);
    assert_eq!(
        environment_lifecycle_plan_digest(&target_delete),
        target_delete_planned_digest
    );
    assert_eq!(target_tombstone.environment_id, target_environment_id);
    operations.push(environment_lifecycle_operation_evidence(
        "target_delete",
        &target_delete,
    ));
    assert!(
        store
            .load_project_state(project_id.as_str())
            .unwrap()
            .unwrap()
            .environments
            .iter()
            .all(|environment| environment.environment_id != target_environment_id)
    );
    let sibling_after_target_delete =
        environment_lifecycle_environment(&store, &project_id, &sibling_environment_id);
    let sibling_aggregate_bytes_equal_after_delete =
        serde_json::to_vec(&sibling_after_target_delete).unwrap() == sibling_aggregate_bytes;
    let sibling_ownership_bytes_equal_after_delete =
        serde_json::to_vec(&sibling_after_target_delete.ownership).unwrap()
            == sibling_ownership_bytes;
    assert!(sibling_aggregate_bytes_equal_after_delete);
    assert!(sibling_ownership_bytes_equal_after_delete);
    assert!(runtime.has_sandbox(&sibling_backend_key));
    let sibling_boot_after_target_delete = environment_lifecycle_guest_exec(
        &runtime,
        &sibling_backend_key,
        "cat /proc/sys/kernel/random/boot_id",
    );
    let sibling_sentinel_after_target_delete = environment_lifecycle_guest_exec(
        &runtime,
        &sibling_backend_key,
        "cat /run/vz-oci/volumes/lifecycle-sentinel",
    );
    assert_eq!(sibling_boot_after_target_delete, sibling_boot_initial);
    assert_eq!(sibling_sentinel_after_target_delete, sibling_sentinel);

    let sibling_delete = store
        .begin_environment_lifecycle(
            sibling_environment_id.as_str(),
            EnvironmentLifecycleKind::Delete,
            "request-sibling-delete",
            "idempotency-sibling-delete",
            &environment_lifecycle_sha256("request-sibling-delete"),
            clock,
        )
        .expect("sibling Delete journal should begin");
    clock += 1;
    shutdown_invocations += 1;
    runtime
        .shutdown_sandbox(&sibling_backend_key)
        .expect("sibling VM should shut down for delete");
    let mut sibling_delete = store
        .acknowledge_environment_machine_step(
            &environment_lifecycle_machine_ack(&sibling_delete, None),
            clock,
        )
        .expect("sibling Delete Machine step should persist");
    clock += 1;
    disk_remove_attempts += 1;
    match std::fs::remove_file(&sibling_disk) {
        Ok(()) => disk_removed += 1,
        Err(error) => panic!("sibling disk should be removed: {error}"),
    }
    for step in sibling_delete.cleanup_steps.clone() {
        sibling_delete = store
            .acknowledge_environment_cleanup_step(
                &OwnershipCleanupStepAcknowledgement {
                    operation_id: sibling_delete.operation_id.clone(),
                    generation: sibling_delete.generation,
                    ownership: step.ownership,
                    result: LifecycleStepResult::Succeeded,
                },
                clock,
            )
            .expect("sibling exact ownership cleanup should persist");
        clock += 1;
    }
    let (sibling_delete, sibling_tombstone) = store
        .finish_environment_delete(
            sibling_delete.operation_id.as_str(),
            sibling_delete.generation,
            clock,
        )
        .expect("sibling Delete should finish with a tombstone");
    assert_eq!(sibling_tombstone.environment_id, sibling_environment_id);
    operations.push(environment_lifecycle_operation_evidence(
        "sibling_delete",
        &sibling_delete,
    ));

    drop(store);
    let store = StateStore::open(&db_path).expect("final lifecycle StateStore should reopen");
    let final_project = store
        .load_project_state(project_id.as_str())
        .expect("final Project should load")
        .expect("Project definition should remain after Environment deletion");
    let final_environment_rows = final_project.environments.len();
    let final_ownership_rows = final_project
        .environments
        .iter()
        .map(|environment| environment.ownership.len())
        .sum::<usize>();
    let tombstones = store
        .list_environment_tombstones(project_id.as_str())
        .expect("both lifecycle tombstones should list");
    let lifecycle_operation_ids = [
        target_up.operation_id.as_str(),
        sibling_up.operation_id.as_str(),
        target_stop.operation_id.as_str(),
        target_up_after_reopen.operation_id.as_str(),
        target_delete.operation_id.as_str(),
        sibling_delete.operation_id.as_str(),
    ];
    for operation_id in lifecycle_operation_ids {
        assert!(
            store
                .load_environment_lifecycle(operation_id)
                .expect("final lifecycle journal should load")
                .is_some(),
            "expected lifecycle journal `{operation_id}` should remain readable"
        );
    }
    drop(store);
    let final_connection =
        rusqlite::Connection::open(&db_path).expect("final lifecycle database should reopen");
    let (
        final_operation_rows,
        final_tombstone_rows,
        actual_environment_rows,
        actual_ownership_rows,
    ): (i64, i64, i64, i64) = final_connection
        .query_row(
            "SELECT
                (SELECT COUNT(*) FROM environment_lifecycle_operations),
                (SELECT COUNT(*) FROM environment_tombstones),
                (SELECT COUNT(*) FROM environment_instances),
                (SELECT COUNT(*) FROM topology_ownership)",
            [],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .expect("final lifecycle database counts should load");
    assert_eq!(tombstones.len(), 2);
    assert_eq!(final_tombstone_rows, 2);
    assert_eq!(final_operation_rows, 6);
    assert_eq!(final_environment_rows, 0);
    assert_eq!(final_ownership_rows, 0);
    assert_eq!(actual_environment_rows, 0);
    assert_eq!(actual_ownership_rows, 0);
    assert!(!target_disk.exists());
    assert!(!sibling_disk.exists());
    assert!(!runtime.has_sandbox(&target_backend_key));
    assert!(!runtime.has_sandbox(&sibling_backend_key));
    let final_diagnostics = runtime.lifecycle_diagnostics();
    assert_eq!(final_diagnostics.vm_handles, 0);
    assert_eq!(final_diagnostics.stack_vms, 0);
    assert_eq!(final_diagnostics.active_lifecycles, 0);
    assert_eq!(operations.len(), 6);
    assert!(
        operations
            .iter()
            .all(|operation| operation["status"] == "succeeded")
    );

    let target_sentinel_sha256 = format!("{:x}", Sha256::digest(target_sentinel.as_bytes()));
    let sibling_sentinel_sha256 = format!("{:x}", Sha256::digest(sibling_sentinel.as_bytes()));
    let evidence = serde_json::json!({
        "schema_version": 1,
        "scenario": "environment-lifecycle-journal-linux-vm",
        "host_target": {
            "host_os": "macos",
            "host_arch": "aarch64",
            "machine_os": "linux",
            "machine_arch": "aarch64",
            "profile": "developer",
            "backend": "macos_virtualization_linux",
        },
        "ids": {
            "project_id": project_id,
            "target": {
                "environment_id": target_environment_id,
                "machine_id": target_machine_id,
                "incarnation_id": target_incarnation.incarnation_id,
                "backend_key": target_backend_key,
                "disk_resource_id": target_disk_resource,
            },
            "sibling": {
                "environment_id": sibling_environment_id,
                "machine_id": sibling_machine_id,
                "incarnation_id": sibling_incarnation.incarnation_id,
                "backend_key": sibling_backend_key,
                "disk_resource_id": sibling_disk_resource,
            },
        },
        "operations": operations,
        "phases": [
            {"name": "initial_up", "passed": true},
            {"name": "persistent_sentinels", "passed": true},
            {"name": "target_stop", "passed": true},
            {"name": "exact_stop_replay", "passed": true},
            {"name": "store_only_reopen", "passed": true},
            {"name": "target_up_after_reopen", "passed": true},
            {"name": "target_delete_reopen", "passed": true},
            {"name": "target_delete", "passed": true},
            {"name": "sibling_delete", "passed": true},
            {"name": "final_cleanup", "passed": true}
        ],
        "backend_invocations": {
            "boot": boot_invocations,
            "shutdown": shutdown_invocations,
            "disk_remove_attempts": disk_remove_attempts,
            "disk_removed": disk_removed,
            "disk_already_absent": disk_already_absent,
            "stop_replay": 0,
        },
        "stop_replay": {
            "same_operation": target_stop_replay.operation_id == target_stop.operation_id,
            "same_generation": target_stop_replay.generation == target_stop.generation,
            "same_plan_digest": environment_lifecycle_plan_digest(&target_stop_replay) == environment_lifecycle_plan_digest(&target_stop),
            "pending_steps": target_stop_replay_pending_steps,
            "backend_invocations": 0,
        },
        "boot_ids": {
            "target_initial": target_boot_initial,
            "target_after_reopen": target_boot_after_reopen,
            "sibling_initial": sibling_boot_initial,
            "sibling_after_target_delete": sibling_boot_after_target_delete,
        },
        "sentinels": {
            "target_sha256": target_sentinel_sha256,
            "sibling_sha256": sibling_sentinel_sha256,
            "target_persisted": true,
            "sibling_persisted": true,
        },
        "reopen": {
            "store_only": true,
            "runtime_kept_alive": true,
            "runtime_reattachment_claimed": false,
            "delete_operation_byte_equal": target_delete_operation_byte_equal,
            "delete_plan_digest_equal": target_delete_plan_after_reopen == target_delete_plan_before_reopen,
            "disk_step_pending": target_disk_step_pending,
        },
        "sibling_isolation": {
            "aggregate_bytes_equal_after_stop": sibling_aggregate_bytes_equal_after_stop,
            "aggregate_bytes_equal_after_restart": sibling_aggregate_bytes_equal_after_restart,
            "aggregate_bytes_equal_after_delete": sibling_aggregate_bytes_equal_after_delete,
            "ownership_bytes_equal_after_stop": sibling_ownership_bytes_equal_after_stop,
            "ownership_bytes_equal_after_restart": sibling_ownership_bytes_equal_after_restart,
            "ownership_bytes_equal_after_delete": sibling_ownership_bytes_equal_after_delete,
            "live_during_target_stop": true,
            "live_after_target_restart": true,
            "live_after_target_delete": true,
        },
        "final": {
            "tombstone_count": final_tombstone_rows,
            "operation_count": final_operation_rows,
            "environment_rows": actual_environment_rows,
            "ownership_rows": actual_ownership_rows,
            "disk_count": usize::from(target_disk.exists()) + usize::from(sibling_disk.exists()),
            "runtime_vm_handles": final_diagnostics.vm_handles,
            "runtime_stack_vms": final_diagnostics.stack_vms,
            "runtime_active_lifecycles": final_diagnostics.active_lifecycles,
            "runtime_exec_sessions": final_diagnostics.exec_sessions,
        },
        "controls": {
            "invocations": 1,
            "retries": 0,
            "fallbacks": 0,
        },
    });
    write_environment_lifecycle_evidence(&evidence);
}
