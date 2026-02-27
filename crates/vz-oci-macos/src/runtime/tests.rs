#![allow(clippy::unwrap_used)]

use std::env;
use std::io;

use super::*;
use vz_linux::KernelVersion;

fn unique_temp_dir(name: &str) -> PathBuf {
    let mut base = env::temp_dir();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    base.push(format!(
        "vz-oci-runtime-test-{name}-{}-{}",
        process::id(),
        nanos.as_nanos(),
    ));
    fs::create_dir_all(&base).unwrap();
    base
}

#[test]
fn checkpoint_capabilities_disable_vm_full_by_default() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("checkpoint-caps"),
        ..RuntimeConfig::default()
    });
    let caps = runtime.checkpoint_capabilities();
    assert!(caps.fs_quick_checkpoint);
    assert!(caps.checkpoint_fork);
    assert!(!caps.vm_full_checkpoint);
    assert!(!caps.docker_compat);
    assert!(caps.compose_adapter);
    assert!(!caps.gpu_passthrough);
    assert!(!caps.live_resize);
    assert!(caps.shared_vm);
    assert!(caps.stack_networking);
    assert!(caps.container_logs);
    vz_runtime_contract::validate_backend_adapter_contract_surface().unwrap();
    vz_runtime_contract::validate_backend_adapter_parity(caps).unwrap();
}

#[test]
fn ensure_checkpoint_class_supported_rejects_vm_full_without_capability() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("checkpoint-vmfull-gate"),
        ..RuntimeConfig::default()
    });
    let err = runtime
        .ensure_checkpoint_class_supported(
            vz_runtime_contract::CheckpointClass::VmFull,
            vz_runtime_contract::RuntimeOperation::CreateCheckpoint,
        )
        .unwrap_err();
    let message = err.to_string();
    assert!(message.contains("vm_full_checkpoint"));
}

#[test]
fn runtime_list_containers_reads_from_store() {
    let data_dir = unique_temp_dir("list");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "container-2".to_string(),
            image: "alpine:3.22".to_string(),
            image_id: "sha256:img2".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "container-1".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:img1".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 200,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    let containers = runtime.list_containers().unwrap();

    assert_eq!(containers.len(), 2);
    assert_eq!(containers[0].id, "container-1");
    assert_eq!(containers[1].id, "container-2");
}

#[tokio::test]
async fn runtime_remove_container_removes_metadata_and_rootfs() {
    let data_dir = unique_temp_dir("remove");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });
    let rootfs_path = data_dir.join("rootfs");
    fs::create_dir_all(&rootfs_path).unwrap();

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "container-1".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:img1".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: Some(rootfs_path.clone()),
            host_pid: None,
        })
        .unwrap();

    runtime.remove_container("container-1").await.unwrap();

    assert!(!rootfs_path.exists());
    assert!(runtime.list_containers().unwrap().is_empty());

    let missing = runtime.remove_container("container-1").await;
    let err = missing.err().unwrap();
    assert!(matches!(err, OciError::Storage(_)));
    if let OciError::Storage(io_err) = err {
        assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
    }
}

#[tokio::test]
async fn runtime_remove_container_rejects_running_container() {
    let data_dir = unique_temp_dir("remove-running");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "container-run".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:img1".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        })
        .unwrap();

    let error = runtime.remove_container("container-run").await.unwrap_err();
    assert!(matches!(error, OciError::InvalidConfig(_)));
}

#[tokio::test]
async fn one_off_auto_remove_cleanup_path_removes_container_and_lifecycle() {
    let data_dir = unique_temp_dir("one-off-auto-remove");
    let rootfs_path = data_dir.join("rootfs").join("one-off");
    fs::create_dir_all(&rootfs_path).unwrap();

    let runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "one-off".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:img1".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: Some(102),
            rootfs_path: Some(rootfs_path.clone()),
            host_pid: None,
        })
        .unwrap();

    runtime.active_lifecycle.lock().await.insert(
        "one-off".to_string(),
        ActiveContainerLifecycle {
            class: ContainerLifecycleClass::Ephemeral,
            auto_remove: true,
        },
    );

    runtime.finalize_one_off_cleanup("one-off", true).await;

    assert!(runtime.list_containers().unwrap().is_empty());
    assert!(!rootfs_path.exists());
    assert!(
        runtime
            .active_lifecycle
            .lock()
            .await
            .get("one-off")
            .is_none()
    );
}

#[tokio::test]
async fn stop_via_oci_runtime_sends_sigterm_and_polls_state() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });

    let exit_code = stop_via_oci_runtime(&mock, "svc-web", false, Duration::from_secs(5), None)
        .await
        .unwrap();

    assert_eq!(exit_code, 143); // 128 + SIGTERM(15)
    let calls = mock.calls.lock().unwrap();
    assert!(calls.contains(&"kill:SIGTERM"));
    assert!(calls.contains(&"state"));
}

#[tokio::test]
async fn stop_via_oci_runtime_forced_sends_sigkill() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });

    let exit_code = stop_via_oci_runtime(&mock, "svc-web", true, Duration::from_secs(5), None)
        .await
        .unwrap();

    assert_eq!(exit_code, 137); // 128 + SIGKILL(9)
    let calls = mock.calls.lock().unwrap();
    assert!(calls.contains(&"kill:SIGKILL"));
    assert!(!calls.contains(&"kill:SIGTERM"));
}

#[tokio::test]
async fn stop_via_oci_runtime_escalates_after_grace_period() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    // Keep the container "running" so SIGTERM doesn't stop it.
    *mock.state_status.lock().unwrap() = "running".to_string();

    // Override kill to NOT change state (simulate unresponsive container).
    struct StubbornMock;
    impl OciLifecycleOps for StubbornMock {
        fn oci_create<'a>(
            &'a self,
            _id: String,
            _bundle_path: String,
        ) -> OciLifecycleFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
        fn oci_start<'a>(&'a self, _id: String) -> OciLifecycleFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
        fn oci_exec<'a>(
            &'a self,
            _id: String,
            _command: String,
            _args: Vec<String>,
            _options: OciExecOptions,
        ) -> OciLifecycleFuture<'a, ExecOutput> {
            Box::pin(async {
                Ok(ExecOutput {
                    exit_code: 0,
                    stdout: String::new(),
                    stderr: String::new(),
                })
            })
        }
        fn oci_kill<'a>(&'a self, _id: String, _signal: String) -> OciLifecycleFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
        fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState> {
            // Always report running — container never stops from SIGTERM.
            Box::pin(async move {
                Ok(OciContainerState {
                    id,
                    status: "running".to_string(),
                    pid: Some(42),
                    bundle_path: None,
                })
            })
        }
        fn oci_delete<'a>(&'a self, _id: String, _force: bool) -> OciLifecycleFuture<'a, ()> {
            Box::pin(async { Ok(()) })
        }
    }

    let exit_code = stop_via_oci_runtime(
        &StubbornMock,
        "svc-stuck",
        false,
        Duration::from_millis(200),
        None,
    )
    .await
    .unwrap();

    // Should escalate to SIGKILL after grace period.
    assert_eq!(exit_code, 137);
}

#[test]
fn runtime_new_preserves_referenced_rootfs() {
    let data_dir = unique_temp_dir("cleanup-preserve");
    let rootfs_root = data_dir.join("rootfs");
    fs::create_dir_all(&rootfs_root).unwrap();

    let referenced_rootfs = rootfs_root.join("container-keep");
    let orphan_rootfs = rootfs_root.join("container-remove");
    let non_rootfs_path = rootfs_root.join("keep.txt");

    fs::create_dir_all(&referenced_rootfs).unwrap();
    fs::create_dir_all(&orphan_rootfs).unwrap();
    fs::write(&non_rootfs_path, b"preserve").unwrap();

    let container_store = ContainerStore::new(data_dir.clone());
    container_store
        .upsert(ContainerInfo {
            id: "container-1".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:img1".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: Some(referenced_rootfs.clone()),
            host_pid: Some(std::process::id()),
        })
        .unwrap();

    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    assert!(referenced_rootfs.is_dir());
    assert!(!orphan_rootfs.exists());
    assert!(non_rootfs_path.is_file());
}

#[test]
fn runtime_new_removes_unreferenced_rootfs_directories() {
    let data_dir = unique_temp_dir("cleanup-orphan");
    let rootfs_root = data_dir.join("rootfs");
    fs::create_dir_all(&rootfs_root).unwrap();

    let orphan_one = rootfs_root.join("orphan-one");
    let orphan_two = rootfs_root.join("orphan-two");
    fs::create_dir_all(&orphan_one).unwrap();
    fs::create_dir_all(&orphan_two).unwrap();

    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    assert!(!orphan_one.exists());
    assert!(!orphan_two.exists());
}

#[test]
fn resolve_run_config_prefers_run_command_when_present() {
    let image_config = ImageConfigSummary {
        entrypoint: Some(vec!["/default-entrypoint".to_string()]),
        cmd: Some(vec!["default-arg".to_string()]),
        ..ImageConfigSummary::default()
    };

    let run = RunConfig {
        cmd: vec!["container".to_string(), "command".to_string()],
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
    assert_eq!(
        resolved.cmd,
        vec!["container".to_string(), "command".to_string()],
    );
}

#[test]
fn resolve_run_config_uses_image_entrypoint_and_cmd_when_run_command_empty() {
    let image_config = ImageConfigSummary {
        entrypoint: Some(vec!["/entrypoint".to_string()]),
        cmd: Some(vec!["arg".to_string()]),
        ..ImageConfigSummary::default()
    };

    let resolved = resolve_run_config(image_config, RunConfig::default(), "container-123").unwrap();
    assert_eq!(
        resolved.cmd,
        vec!["/entrypoint".to_string(), "arg".to_string()],
    );
}

#[test]
fn resolve_run_config_preserves_execution_mode() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };

    let run = RunConfig {
        execution_mode: ExecutionMode::OciRuntime,
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
    assert_eq!(resolved.execution_mode, ExecutionMode::OciRuntime);
}

#[test]
fn resolve_container_lifecycle_uses_expected_defaults() {
    let empty = Vec::new();

    let run_defaults =
        resolve_container_lifecycle(&empty, ContainerLifecycleClass::Ephemeral, true).unwrap();
    assert_eq!(run_defaults.class, ContainerLifecycleClass::Ephemeral);
    assert!(run_defaults.auto_remove);

    let workspace_defaults =
        resolve_container_lifecycle(&empty, ContainerLifecycleClass::Workspace, false).unwrap();
    assert_eq!(workspace_defaults.class, ContainerLifecycleClass::Workspace);
    assert!(!workspace_defaults.auto_remove);

    let service_defaults =
        resolve_container_lifecycle(&empty, ContainerLifecycleClass::Service, false).unwrap();
    assert_eq!(service_defaults.class, ContainerLifecycleClass::Service);
    assert!(!service_defaults.auto_remove);
}

#[test]
fn resolve_container_lifecycle_honors_annotation_overrides() {
    let annotations = vec![
        (
            OCI_ANNOTATION_CONTAINER_CLASS.to_string(),
            "service".to_string(),
        ),
        (OCI_ANNOTATION_AUTO_REMOVE.to_string(), "true".to_string()),
    ];

    let lifecycle =
        resolve_container_lifecycle(&annotations, ContainerLifecycleClass::Workspace, false)
            .unwrap();

    assert_eq!(lifecycle.class, ContainerLifecycleClass::Service);
    assert!(lifecycle.auto_remove);
}

#[test]
fn resolve_container_lifecycle_rejects_invalid_annotation_values() {
    let invalid_class = vec![(
        OCI_ANNOTATION_CONTAINER_CLASS.to_string(),
        "daemon".to_string(),
    )];
    let class_err =
        resolve_container_lifecycle(&invalid_class, ContainerLifecycleClass::Workspace, false)
            .unwrap_err();
    assert!(
        matches!(class_err, OciError::InvalidConfig(ref msg) if msg.contains(OCI_ANNOTATION_CONTAINER_CLASS))
    );

    let invalid_auto_remove = vec![(
        OCI_ANNOTATION_AUTO_REMOVE.to_string(),
        "sometimes".to_string(),
    )];
    let auto_remove_err = resolve_container_lifecycle(
        &invalid_auto_remove,
        ContainerLifecycleClass::Workspace,
        false,
    )
    .unwrap_err();
    assert!(
        matches!(auto_remove_err, OciError::InvalidConfig(ref msg) if msg.contains(OCI_ANNOTATION_AUTO_REMOVE))
    );
}

#[test]
fn parse_compose_log_rotation_accepts_json_file_max_size_and_max_file() {
    let annotations = vec![
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER.to_string(),
            "json-file".to_string(),
        ),
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS.to_string(),
            "max-size=10m\nmax-file=3".to_string(),
        ),
    ];

    let rotation = parse_compose_log_rotation(&annotations)
        .unwrap()
        .expect("rotation config should be present");
    assert_eq!(rotation.max_size_bytes, 10 * 1024 * 1024);
    assert_eq!(rotation.max_files, 3);
}

#[test]
fn parse_compose_log_rotation_defaults_max_file_to_one() {
    let annotations = vec![
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER.to_string(),
            "local".to_string(),
        ),
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS.to_string(),
            "max-size=1m".to_string(),
        ),
    ];

    let rotation = parse_compose_log_rotation(&annotations)
        .unwrap()
        .expect("rotation config should be present");
    assert_eq!(rotation.max_size_bytes, 1024 * 1024);
    assert_eq!(rotation.max_files, 1);
}

#[test]
fn parse_compose_log_rotation_skips_none_driver_or_missing_max_size() {
    let none_driver = vec![
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER.to_string(),
            "none".to_string(),
        ),
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS.to_string(),
            "max-size=10m".to_string(),
        ),
    ];
    assert!(parse_compose_log_rotation(&none_driver).unwrap().is_none());

    let no_max_size = vec![
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER.to_string(),
            "json-file".to_string(),
        ),
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS.to_string(),
            "max-file=3".to_string(),
        ),
    ];
    assert!(parse_compose_log_rotation(&no_max_size).unwrap().is_none());
}

#[test]
fn parse_compose_log_rotation_rejects_labels_and_tag_options() {
    let labels = vec![
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER.to_string(),
            "json-file".to_string(),
        ),
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS.to_string(),
            "max-size=10m\nlabels=com.example.team".to_string(),
        ),
    ];
    let labels_err = parse_compose_log_rotation(&labels).unwrap_err();
    assert!(matches!(
        labels_err,
        OciError::InvalidConfig(ref message) if message.contains("labels")
    ));

    let tag = vec![
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_DRIVER.to_string(),
            "json-file".to_string(),
        ),
        (
            OCI_ANNOTATION_COMPOSE_LOGGING_OPTIONS.to_string(),
            "max-size=10m\ntag=svc".to_string(),
        ),
    ];
    let tag_err = parse_compose_log_rotation(&tag).unwrap_err();
    assert!(matches!(
        tag_err,
        OciError::InvalidConfig(ref message) if message.contains("tag")
    ));
}

#[test]
fn build_log_rotation_script_uses_copy_truncate_for_archives() {
    let script = build_log_rotation_script(
        "container-123",
        ComposeLogRotation {
            max_size_bytes: 1024,
            max_files: 3,
        },
    );
    assert!(script.contains("/run/vz-oci/logs/container-123/output.log"));
    assert!(script.contains("rm -f \"$log.2\""));
    assert!(script.contains("cp \"$log\" \"$log.1\""));
    assert!(script.contains(": > \"$log\""));
}

#[test]
fn build_log_rotation_script_truncates_when_max_file_is_one() {
    let script = build_log_rotation_script(
        "container-456",
        ComposeLogRotation {
            max_size_bytes: 2048,
            max_files: 1,
        },
    );
    assert!(script.contains("if [ \"$size\" -ge 2048 ]"));
    assert!(!script.contains("cp \"$log\" \"$log.1\""));
    assert!(script.contains(": > \"$log\""));
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RecordedOciExec {
    id: String,
    command: String,
    args: Vec<String>,
    options: OciExecOptions,
}

struct MockOciLifecycleOps {
    calls: std::sync::Mutex<Vec<&'static str>>,
    exec_call: std::sync::Mutex<Option<RecordedOciExec>>,
    exec_output: ExecOutput,
    fail_start: bool,
    state_status: std::sync::Mutex<String>,
}

impl MockOciLifecycleOps {
    fn new(exec_output: ExecOutput) -> Self {
        Self {
            calls: std::sync::Mutex::new(Vec::new()),
            exec_call: std::sync::Mutex::new(None),
            exec_output,
            fail_start: false,
            state_status: std::sync::Mutex::new("running".to_string()),
        }
    }
}

impl OciLifecycleOps for MockOciLifecycleOps {
    fn oci_create<'a>(&'a self, _id: String, _bundle_path: String) -> OciLifecycleFuture<'a, ()> {
        self.calls.lock().unwrap().push("create");
        Box::pin(async { Ok(()) })
    }

    fn oci_start<'a>(&'a self, _id: String) -> OciLifecycleFuture<'a, ()> {
        self.calls.lock().unwrap().push("start");
        let fail_start = self.fail_start;
        Box::pin(async move {
            if fail_start {
                Err(OciError::InvalidConfig("mock start failure".to_string()))
            } else {
                Ok(())
            }
        })
    }

    fn oci_exec<'a>(
        &'a self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> OciLifecycleFuture<'a, ExecOutput> {
        self.calls.lock().unwrap().push("exec");
        *self.exec_call.lock().unwrap() = Some(RecordedOciExec {
            id,
            command,
            args,
            options,
        });
        let output = self.exec_output.clone();
        Box::pin(async move { Ok(output) })
    }

    fn oci_kill<'a>(&'a self, _id: String, signal: String) -> OciLifecycleFuture<'a, ()> {
        self.calls.lock().unwrap().push(if signal == "SIGKILL" {
            "kill:SIGKILL"
        } else {
            "kill:SIGTERM"
        });
        // Simulate: after kill, container becomes stopped.
        *self.state_status.lock().unwrap() = "stopped".to_string();
        Box::pin(async { Ok(()) })
    }

    fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState> {
        self.calls.lock().unwrap().push("state");
        let status = self.state_status.lock().unwrap().clone();
        Box::pin(async move {
            Ok(OciContainerState {
                id,
                status,
                pid: None,
                bundle_path: None,
            })
        })
    }

    fn oci_delete<'a>(&'a self, _id: String, _force: bool) -> OciLifecycleFuture<'a, ()> {
        self.calls.lock().unwrap().push("delete");
        Box::pin(async { Ok(()) })
    }
}

#[tokio::test]
async fn oci_runtime_lifecycle_uses_create_start_exec_delete_sequence() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 7,
        stdout: "ok".to_string(),
        stderr: String::new(),
    });

    let output = run_oci_lifecycle(
        &mock,
        "svc-web".to_string(),
        "/run/vz-oci/bundles/svc-web".to_string(),
        "/bin/echo".to_string(),
        vec!["hello".to_string()],
        OciExecOptions {
            env: vec![("GREETING".to_string(), "hello".to_string())],
            cwd: Some("/workspace".to_string()),
            user: Some("1000:1001".to_string()),
        },
    )
    .await
    .expect("OCI lifecycle should succeed");

    assert_eq!(
        output,
        ExecOutput {
            exit_code: 7,
            stdout: "ok".to_string(),
            stderr: String::new(),
        }
    );
    assert_eq!(
        *mock.calls.lock().unwrap(),
        vec!["create", "start", "exec", "delete"]
    );
    assert_eq!(
        *mock.exec_call.lock().unwrap(),
        Some(RecordedOciExec {
            id: "svc-web".to_string(),
            command: "/bin/echo".to_string(),
            args: vec!["hello".to_string()],
            options: OciExecOptions {
                env: vec![("GREETING".to_string(), "hello".to_string())],
                cwd: Some("/workspace".to_string()),
                user: Some("1000:1001".to_string()),
            },
        }),
    );
}

#[tokio::test]
async fn oci_runtime_lifecycle_attempts_delete_on_start_failure() {
    let mut mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    mock.fail_start = true;

    let error = run_oci_lifecycle(
        &mock,
        "svc-start-fail".to_string(),
        "/run/vz-oci/bundles/svc-start-fail".to_string(),
        "/bin/echo".to_string(),
        vec!["hello".to_string()],
        OciExecOptions::default(),
    )
    .await
    .expect_err("start failure should surface");
    assert!(matches!(error, OciError::InvalidConfig(ref msg) if msg == "mock start failure"));
    assert_eq!(
        *mock.calls.lock().unwrap(),
        vec!["create", "start", "delete"]
    );
}

#[test]
fn oci_bundle_host_dir_is_rootfs_scoped() {
    let rootfs_dir = PathBuf::from("/tmp/vz-oci-rootfs");
    let guest_root = oci_bundle_guest_root(None).unwrap();
    let guest_path = oci_bundle_guest_path(&guest_root, "svc-bundle");
    let host_bundle = oci_bundle_host_dir(&rootfs_dir, &guest_path);
    assert_eq!(
        host_bundle,
        PathBuf::from("/tmp/vz-oci-rootfs/run/vz-oci/bundles/svc-bundle")
    );
    assert_eq!(guest_path, "/run/vz-oci/bundles/svc-bundle".to_string());
}

#[test]
fn oci_bundle_guest_root_uses_custom_state_dir() {
    let guest_root = oci_bundle_guest_root(Some(Path::new("/var/lib/vz-oci"))).unwrap();
    assert_eq!(guest_root, "/var/lib/vz-oci/bundles".to_string());
}

#[test]
fn oci_bundle_guest_root_rejects_relative_state_dir() {
    let error = oci_bundle_guest_root(Some(Path::new("var/lib/vz-oci"))).unwrap_err();
    assert!(matches!(error, OciError::InvalidConfig(_)));
}

#[test]
fn write_hosts_file_generates_correct_content() {
    let tmp = unique_temp_dir("hosts-gen");
    let hosts = vec![
        ("db".to_string(), "127.0.0.1".to_string()),
        ("cache".to_string(), "10.0.0.5".to_string()),
    ];
    write_hosts_file(&tmp, &hosts).unwrap();
    let content = fs::read_to_string(tmp.join("etc/hosts")).unwrap();
    assert!(content.contains("127.0.0.1\tlocalhost"));
    assert!(content.contains("::1\tlocalhost"));
    assert!(content.contains("127.0.0.1\tdb"));
    assert!(content.contains("10.0.0.5\tcache"));
}

#[tokio::test]
async fn run_rootfs_with_oci_runtime_rejects_nonexistent_rootfs() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("oci-missing-rootfs"),
        ..RuntimeConfig::default()
    });

    let err = runtime
        .run_rootfs_with_oci_runtime(
            "/tmp/vz-oci-missing-rootfs",
            RunConfig {
                cmd: vec!["/bin/true".to_string()],
                execution_mode: ExecutionMode::OciRuntime,
                ..RunConfig::default()
            },
            "test-container",
        )
        .await
        .expect_err("missing rootfs should fail before VM wiring");

    assert!(matches!(err, OciError::InvalidRootfs { .. }));
}

#[test]
fn resolve_run_config_merges_env_with_run_precedence() {
    let image_config = ImageConfigSummary {
        env: Some(vec![
            "BASE=1".to_string(),
            "OVERRIDE=old".to_string(),
            "VZ_CONTAINER_ID=stale".to_string(),
        ]),
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };

    let run = RunConfig {
        env: vec![
            ("OVERRIDE".to_string(), "new".to_string()),
            ("NEW".to_string(), "value".to_string()),
            ("OVERRIDE".to_string(), "newer".to_string()),
        ],
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
    assert_eq!(
        resolved.env,
        vec![
            ("BASE".to_string(), "1".to_string()),
            ("OVERRIDE".to_string(), "newer".to_string()),
            ("NEW".to_string(), "value".to_string()),
            ("VZ_CONTAINER_ID".to_string(), "container-123".to_string()),
        ],
    );
}

#[test]
fn resolve_run_config_preserves_ports() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };

    let run = RunConfig {
        ports: vec![PortMapping {
            host: 8080,
            container: 80,
            protocol: PortProtocol::Tcp,
            target_host: None,
        }],
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
    assert_eq!(
        resolved.ports,
        vec![PortMapping {
            host: 8080,
            container: 80,
            protocol: PortProtocol::Tcp,
            target_host: None,
        }],
    );
}

#[test]
fn resolve_run_config_sets_container_id() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };

    let resolved = resolve_run_config(image_config, RunConfig::default(), "container-abc").unwrap();

    assert_eq!(resolved.container_id, Some("container-abc".to_string()));
}

fn make_kernel_paths_with_youki(path: PathBuf) -> KernelPaths {
    KernelPaths {
        kernel: PathBuf::from("/tmp/vmlinux"),
        initramfs: PathBuf::from("/tmp/initramfs.img"),
        youki: path,
        version: KernelVersion {
            kernel: "6.12.11".to_string(),
            busybox: "1.37.0".to_string(),
            agent: "0.1.0".to_string(),
            youki: "0.5.7".to_string(),
            built: Some("2026-02-18T00:00:00Z".to_string()),
            sha256_vmlinux: None,
            sha256_initramfs: None,
            sha256_youki: None,
        },
    }
}

#[test]
fn resolve_oci_runtime_binary_path_uses_kernel_artifact_by_default() {
    let temp = unique_temp_dir("runtime-bin-default");
    let youki = temp.join("youki");
    fs::write(&youki, b"youki").unwrap();
    let kernel = make_kernel_paths_with_youki(youki.clone());

    let resolved = resolve_oci_runtime_binary_path(OciRuntimeKind::Youki, None, &kernel).unwrap();

    assert_eq!(resolved, youki);
}

#[test]
fn resolve_oci_runtime_binary_path_prefers_configured_override() {
    let temp = unique_temp_dir("runtime-bin-override");
    let bundled_dir = temp.join("bundled");
    let override_dir = temp.join("override");
    fs::create_dir_all(&bundled_dir).unwrap();
    fs::create_dir_all(&override_dir).unwrap();
    let bundled_youki = bundled_dir.join("youki");
    let override_youki = override_dir.join("youki");
    fs::write(&bundled_youki, b"bundled").unwrap();
    fs::write(&override_youki, b"override").unwrap();
    let kernel = make_kernel_paths_with_youki(bundled_youki);

    let resolved =
        resolve_oci_runtime_binary_path(OciRuntimeKind::Youki, Some(&override_youki), &kernel)
            .unwrap();

    assert_eq!(resolved, override_youki);
}

#[test]
fn resolve_oci_runtime_binary_path_rejects_non_youki_name() {
    let temp = unique_temp_dir("runtime-bin-name");
    let bad_path = temp.join("runtime");
    fs::write(&bad_path, b"binary").unwrap();
    let kernel = make_kernel_paths_with_youki(temp.join("youki"));

    let err = resolve_oci_runtime_binary_path(OciRuntimeKind::Youki, Some(&bad_path), &kernel)
        .unwrap_err();
    assert!(matches!(err, OciError::InvalidConfig(_)));
}

#[test]
fn make_oci_runtime_share_uses_parent_dir_with_expected_tag() {
    let temp = unique_temp_dir("runtime-share");
    let youki = temp.join("youki");
    fs::write(&youki, b"runtime").unwrap();

    let share = make_oci_runtime_share(&youki).unwrap();

    assert_eq!(share.tag, OCI_RUNTIME_BIN_SHARE_TAG);
    assert_eq!(share.source, temp);
    assert!(share.read_only);
}

#[test]
fn expand_home_dir_resolves_tilde_prefix() {
    let Some(home) = std::env::var_os("HOME") else {
        return;
    };

    let resolved = expand_home_dir(Path::new("~/.vz/oci"));
    assert_eq!(resolved, PathBuf::from(home).join(".vz/oci"));
}

// B09 - RuntimeConfig and RunConfig OCI extension tests

#[test]
fn runtime_config_guest_oci_runtime_defaults_to_youki() {
    let cfg = RuntimeConfig::default();
    assert_eq!(cfg.guest_oci_runtime, OciRuntimeKind::Youki);
    assert_eq!(cfg.guest_oci_runtime.binary_name(), "youki");
}

#[test]
fn runtime_config_guest_state_dir_defaults_to_none() {
    let cfg = RuntimeConfig::default();
    assert!(cfg.guest_state_dir.is_none());
    // When None, bundle root uses the default /run/vz-oci.
    let root = oci_bundle_guest_root(cfg.guest_state_dir.as_deref()).unwrap();
    assert_eq!(root, "/run/vz-oci/bundles");
}

#[test]
fn runtime_config_custom_guest_state_dir_flows_to_bundle_root() {
    let cfg = RuntimeConfig {
        guest_state_dir: Some(PathBuf::from("/var/lib/custom")),
        ..RuntimeConfig::default()
    };
    let root = oci_bundle_guest_root(cfg.guest_state_dir.as_deref()).unwrap();
    assert_eq!(root, "/var/lib/custom/bundles");
}

#[test]
fn resolve_run_config_preserves_init_process() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };
    let run = RunConfig {
        init_process: Some(vec!["/sbin/init".to_string(), "--flag".to_string()]),
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-abc").unwrap();
    assert_eq!(
        resolved.init_process,
        Some(vec!["/sbin/init".to_string(), "--flag".to_string()])
    );
}

#[test]
fn resolve_run_config_rejects_empty_init_process() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };
    let run = RunConfig {
        init_process: Some(Vec::new()),
        ..RunConfig::default()
    };

    let err = resolve_run_config(image_config, run, "container-abc").unwrap_err();
    assert!(matches!(err, OciError::InvalidConfig(_)));
}

#[test]
fn mount_specs_to_bundle_mounts_converts_bind_mount() {
    let mounts = vec![MountSpec {
        source: Some(PathBuf::from("/host/data")),
        target: PathBuf::from("/container/data"),
        mount_type: MountType::Bind,
        access: MountAccess::ReadWrite,
        subpath: None,
    }];

    let bundle_mounts = mount_specs_to_bundle_mounts(&mounts, 0).unwrap();
    assert_eq!(bundle_mounts.len(), 1);
    assert_eq!(
        bundle_mounts[0].destination,
        PathBuf::from("/container/data")
    );
    // Guest source should use the VirtioFS mount tag path.
    assert_eq!(bundle_mounts[0].source, PathBuf::from("/mnt/vz-mount-0"));
    assert_eq!(bundle_mounts[0].typ, "bind");
    assert!(bundle_mounts[0].options.contains(&"rbind".to_string()));
    assert!(bundle_mounts[0].options.contains(&"rw".to_string()));
}

#[test]
fn mount_specs_to_bundle_mounts_converts_ro_bind_mount() {
    let mounts = vec![MountSpec {
        source: Some(PathBuf::from("/host/config")),
        target: PathBuf::from("/etc/app"),
        mount_type: MountType::Bind,
        access: MountAccess::ReadOnly,
        subpath: None,
    }];

    let bundle_mounts = mount_specs_to_bundle_mounts(&mounts, 0).unwrap();
    assert_eq!(bundle_mounts.len(), 1);
    assert!(bundle_mounts[0].options.contains(&"ro".to_string()));
}

#[test]
fn mount_specs_to_bundle_mounts_converts_tmpfs_mount() {
    let mounts = vec![MountSpec {
        source: None,
        target: PathBuf::from("/tmp"),
        mount_type: MountType::Tmpfs,
        access: MountAccess::ReadWrite,
        subpath: None,
    }];

    let bundle_mounts = mount_specs_to_bundle_mounts(&mounts, 0).unwrap();
    assert_eq!(bundle_mounts.len(), 1);
    assert_eq!(bundle_mounts[0].destination, PathBuf::from("/tmp"));
    assert_eq!(bundle_mounts[0].source, PathBuf::from("tmpfs"));
    assert_eq!(bundle_mounts[0].typ, "tmpfs");
}

#[test]
fn mount_specs_to_bundle_mounts_rejects_relative_target() {
    let mounts = vec![MountSpec {
        source: Some(PathBuf::from("/host")),
        target: PathBuf::from("relative/path"),
        mount_type: MountType::Bind,
        access: MountAccess::ReadWrite,
        subpath: None,
    }];

    let err = mount_specs_to_bundle_mounts(&mounts, 0).unwrap_err();
    assert!(matches!(err, OciError::InvalidConfig(_)));
}

#[test]
fn mount_specs_to_bundle_mounts_rejects_bind_without_source() {
    let mounts = vec![MountSpec {
        source: None,
        target: PathBuf::from("/container/path"),
        mount_type: MountType::Bind,
        access: MountAccess::ReadWrite,
        subpath: None,
    }];

    let err = mount_specs_to_bundle_mounts(&mounts, 0).unwrap_err();
    assert!(matches!(err, OciError::InvalidConfig(_)));
}

#[test]
fn mount_specs_to_shared_dirs_generates_virtio_shares_for_binds() {
    let mounts = vec![
        MountSpec {
            source: Some(PathBuf::from("/host/a")),
            target: PathBuf::from("/container/a"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadWrite,
            subpath: None,
        },
        MountSpec {
            source: None,
            target: PathBuf::from("/tmp"),
            mount_type: MountType::Tmpfs,
            access: MountAccess::ReadWrite,
            subpath: None,
        },
        MountSpec {
            source: Some(PathBuf::from("/host/b")),
            target: PathBuf::from("/container/b"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadOnly,
            subpath: None,
        },
    ];

    let shares = mount_specs_to_shared_dirs(&mounts, 0);
    // Tmpfs is skipped, so only 2 entries.
    assert_eq!(shares.len(), 2);
    assert_eq!(shares[0].tag, "vz-mount-0");
    assert_eq!(shares[0].source, PathBuf::from("/host/a"));
    assert!(!shares[0].read_only);
    assert_eq!(shares[1].tag, "vz-mount-2");
    assert_eq!(shares[1].source, PathBuf::from("/host/b"));
    assert!(shares[1].read_only);
}

#[test]
fn mount_specs_to_shared_dirs_shares_parent_for_file_mounts() {
    // Create a temporary file to simulate a secret file mount
    let temp_dir = std::env::temp_dir();
    let secrets_dir = temp_dir.join("vz-test-secrets");
    std::fs::create_dir_all(&secrets_dir).unwrap();
    let secret_file = secrets_dir.join("my_secret");
    std::fs::write(&secret_file, "secret content").unwrap();

    let mounts = vec![MountSpec {
        source: Some(secret_file.clone()),
        target: PathBuf::from("/run/secrets/my_secret"),
        mount_type: MountType::Bind,
        access: MountAccess::ReadOnly,
        subpath: Some("my_secret".to_string()),
    }];

    let shares = mount_specs_to_shared_dirs(&mounts, 0);

    // Should share the parent directory, not the file
    assert_eq!(shares.len(), 1);
    assert_eq!(shares[0].tag, "vz-mount-0");
    assert_eq!(shares[0].source, secrets_dir);
    assert!(shares[0].read_only);

    // Cleanup
    std::fs::remove_file(secret_file).ok();
    std::fs::remove_dir(secrets_dir).ok();
}

#[test]
fn resolve_run_config_preserves_mounts() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };

    let run = RunConfig {
        mounts: vec![MountSpec {
            source: Some(PathBuf::from("/host/data")),
            target: PathBuf::from("/data"),
            mount_type: MountType::Bind,
            access: MountAccess::ReadWrite,
            subpath: None,
        }],
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-abc").unwrap();
    assert_eq!(resolved.mounts.len(), 1);
    assert_eq!(resolved.mounts[0].target, PathBuf::from("/data"));
}

#[test]
fn resolve_run_config_preserves_oci_annotations() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };
    let annotations = vec![
        (
            "org.opencontainers.image.title".to_string(),
            "test".to_string(),
        ),
        ("custom.key".to_string(), "value".to_string()),
    ];
    let run = RunConfig {
        oci_annotations: annotations.clone(),
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-abc").unwrap();
    assert_eq!(resolved.oci_annotations, annotations);
}

#[test]
fn exec_config_default_is_empty() {
    let cfg = ExecConfig::default();
    assert!(cfg.execution_id.is_none());
    assert!(cfg.cmd.is_empty());
    assert!(cfg.working_dir.is_none());
    assert!(cfg.env.is_empty());
    assert!(cfg.user.is_none());
    assert!(!cfg.pty);
    assert!(cfg.term_rows.is_none());
    assert!(cfg.term_cols.is_none());
    assert!(cfg.timeout.is_none());
}

#[test]
fn parse_signal_number_supports_symbolic_and_numeric_inputs() {
    assert_eq!(parse_signal_number("SIGTERM"), Some(15));
    assert_eq!(parse_signal_number("term"), Some(15));
    assert_eq!(parse_signal_number("2"), Some(2));
    assert_eq!(parse_signal_number("SIGWINCH"), Some(28));
    assert_eq!(parse_signal_number(""), None);
    assert_eq!(parse_signal_number("SIGDOESNOTEXIST"), None);
}

#[tokio::test]
async fn exec_control_missing_session_returns_not_found() {
    let data_dir = unique_temp_dir("exec-control-missing");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    let write = runtime
        .write_exec_stdin("exec-missing", b"hello")
        .await
        .unwrap_err();
    let signal = runtime
        .signal_exec("exec-missing", "SIGTERM")
        .await
        .unwrap_err();
    let resize = runtime
        .resize_exec_pty("exec-missing", 120, 40)
        .await
        .unwrap_err();
    let cancel = runtime.cancel_exec("exec-missing").await.unwrap_err();

    assert!(matches!(
        write,
        OciError::ExecutionSessionNotFound { execution_id } if execution_id == "exec-missing"
    ));
    assert!(matches!(
        signal,
        OciError::ExecutionSessionNotFound { execution_id } if execution_id == "exec-missing"
    ));
    assert!(matches!(
        resize,
        OciError::ExecutionSessionNotFound { execution_id } if execution_id == "exec-missing"
    ));
    assert!(matches!(
        cancel,
        OciError::ExecutionSessionNotFound { execution_id } if execution_id == "exec-missing"
    ));
}

#[tokio::test]
async fn exec_container_rejects_missing_vm_handle() {
    let data_dir = unique_temp_dir("exec-missing");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    let err = runtime
        .exec_container(
            "nonexistent",
            ExecConfig {
                cmd: vec!["/bin/echo".to_string(), "hello".to_string()],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(err, OciError::InvalidConfig(_)));
}

#[tokio::test]
async fn exec_container_rejects_empty_command() {
    let data_dir = unique_temp_dir("exec-empty-cmd");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    // Manually register a mock VM handle to bypass the "no handle" error.
    // We can't actually create a LinuxVm in unit tests, but we can verify
    // the error path before it reaches the VM by testing with no handle.
    let err = runtime
        .exec_container(
            "no-such-container",
            ExecConfig {
                cmd: vec![],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_err();

    // Should fail with "no active VM handle" since there's no container.
    assert!(matches!(err, OciError::InvalidConfig(_)));
}

#[tokio::test]
async fn create_container_rejects_macos_backend() {
    let data_dir = unique_temp_dir("create-macos");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    let err = runtime
        .create_container("macos:sonoma", RunConfig::default())
        .await
        .unwrap_err();

    assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("macos")));
}

// ── B14: Crash recovery conformance ──

/// Simulates host crash by seeding container store with stale state, then
/// creating a new Runtime (which triggers reconciliation in `::new()`).
#[test]
fn crash_recovery_transitions_stale_running_to_stopped() {
    let data_dir = unique_temp_dir("crash-stale-running");
    let store = ContainerStore::new(data_dir.clone());

    // Seed: a "Running" container whose host_pid is long dead.
    store
        .upsert(ContainerInfo {
            id: "running-stale".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:aaa".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(999_999_999),
        })
        .unwrap();

    // "Restart" — construct a fresh Runtime from the same data_dir.
    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let containers = ContainerStore::new(data_dir).load_all().unwrap();
    let c = containers.iter().find(|c| c.id == "running-stale").unwrap();
    assert!(matches!(
        c.status,
        ContainerStatus::Stopped { exit_code: -1 }
    ));
    assert!(c.stopped_unix_secs.is_some());
    assert!(c.host_pid.is_none());
}

#[test]
fn crash_recovery_transitions_stale_created_to_stopped() {
    let data_dir = unique_temp_dir("crash-stale-created");
    let store = ContainerStore::new(data_dir.clone());

    store
        .upsert(ContainerInfo {
            id: "created-stale".to_string(),
            image: "alpine:3.22".to_string(),
            image_id: "sha256:bbb".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 200,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(999_999_999),
        })
        .unwrap();

    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let containers = ContainerStore::new(data_dir).load_all().unwrap();
    let c = containers.iter().find(|c| c.id == "created-stale").unwrap();
    assert!(matches!(
        c.status,
        ContainerStatus::Stopped { exit_code: -1 }
    ));
    assert!(c.host_pid.is_none());
}

#[test]
fn crash_recovery_preserves_alive_running_container() {
    let data_dir = unique_temp_dir("crash-alive");
    let store = ContainerStore::new(data_dir.clone());

    store
        .upsert(ContainerInfo {
            id: "alive".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:ccc".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 300,
            started_unix_secs: Some(301),
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        })
        .unwrap();

    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let containers = ContainerStore::new(data_dir).load_all().unwrap();
    let c = containers.iter().find(|c| c.id == "alive").unwrap();
    assert!(matches!(c.status, ContainerStatus::Running));
    assert_eq!(c.host_pid, Some(process::id()));
}

#[test]
fn crash_recovery_does_not_alter_stopped_containers() {
    let data_dir = unique_temp_dir("crash-stopped");
    let store = ContainerStore::new(data_dir.clone());

    store
        .upsert(ContainerInfo {
            id: "already-done".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:ddd".to_string(),
            status: ContainerStatus::Stopped { exit_code: 42 },
            created_unix_secs: 50,
            started_unix_secs: Some(51),
            stopped_unix_secs: Some(60),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let containers = ContainerStore::new(data_dir).load_all().unwrap();
    let c = containers.iter().find(|c| c.id == "already-done").unwrap();
    assert!(matches!(
        c.status,
        ContainerStatus::Stopped { exit_code: 42 }
    ));
    assert_eq!(c.stopped_unix_secs, Some(60));
}

#[test]
fn crash_recovery_mixed_state_reconciles_correctly() {
    let data_dir = unique_temp_dir("crash-mixed");
    let rootfs_root = data_dir.join("rootfs");
    let store = ContainerStore::new(data_dir.clone());

    // Stale running container with rootfs.
    let stale_rootfs = rootfs_root.join("stale-ctr");
    fs::create_dir_all(&stale_rootfs).unwrap();
    store
        .upsert(ContainerInfo {
            id: "stale-ctr".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:s1".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: None,
            rootfs_path: Some(stale_rootfs.clone()),
            host_pid: Some(999_999_999),
        })
        .unwrap();

    // Alive running container with rootfs.
    let alive_rootfs = rootfs_root.join("alive-ctr");
    fs::create_dir_all(&alive_rootfs).unwrap();
    store
        .upsert(ContainerInfo {
            id: "alive-ctr".to_string(),
            image: "alpine:3.22".to_string(),
            image_id: "sha256:a1".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 200,
            started_unix_secs: Some(201),
            stopped_unix_secs: None,
            rootfs_path: Some(alive_rootfs.clone()),
            host_pid: Some(process::id()),
        })
        .unwrap();

    // Already stopped container.
    store
        .upsert(ContainerInfo {
            id: "stopped-ctr".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:p1".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 50,
            started_unix_secs: Some(51),
            stopped_unix_secs: Some(60),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    // Orphaned rootfs with no container record.
    let orphan_rootfs = rootfs_root.join("orphan-dir");
    fs::create_dir_all(&orphan_rootfs).unwrap();

    // Simulate restart.
    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let containers = ContainerStore::new(data_dir).load_all().unwrap();
    assert_eq!(containers.len(), 3);

    // Stale container: reconciled to stopped, rootfs cleaned.
    let stale = containers.iter().find(|c| c.id == "stale-ctr").unwrap();
    assert!(matches!(
        stale.status,
        ContainerStatus::Stopped { exit_code: -1 }
    ));
    assert!(stale.rootfs_path.is_none());
    assert!(!stale_rootfs.exists());

    // Alive container: untouched, rootfs preserved.
    let alive = containers.iter().find(|c| c.id == "alive-ctr").unwrap();
    assert!(matches!(alive.status, ContainerStatus::Running));
    assert!(alive_rootfs.is_dir());

    // Stopped container: unchanged.
    let stopped = containers.iter().find(|c| c.id == "stopped-ctr").unwrap();
    assert!(matches!(
        stopped.status,
        ContainerStatus::Stopped { exit_code: 0 }
    ));

    // Orphaned rootfs: cleaned up.
    assert!(!orphan_rootfs.exists());
}

#[test]
fn crash_recovery_is_idempotent() {
    let data_dir = unique_temp_dir("crash-idempotent");
    let store = ContainerStore::new(data_dir.clone());

    store
        .upsert(ContainerInfo {
            id: "stale-idem".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:idem".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(999_999_999),
        })
        .unwrap();

    // First restart — reconciles the stale container.
    let _rt1 = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let after_first = ContainerStore::new(data_dir.clone()).load_all().unwrap();
    let c1 = after_first.iter().find(|c| c.id == "stale-idem").unwrap();
    assert!(matches!(
        c1.status,
        ContainerStatus::Stopped { exit_code: -1 }
    ));
    let stopped_ts = c1.stopped_unix_secs;

    // Second restart — should produce identical state.
    let _rt2 = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let after_second = ContainerStore::new(data_dir).load_all().unwrap();
    let c2 = after_second.iter().find(|c| c.id == "stale-idem").unwrap();
    assert!(matches!(
        c2.status,
        ContainerStatus::Stopped { exit_code: -1 }
    ));
    // Timestamp should not be overwritten on second restart since it's already Stopped.
    assert_eq!(c2.stopped_unix_secs, stopped_ts);
}

#[test]
fn crash_recovery_stale_container_with_no_pid_is_reconciled() {
    let data_dir = unique_temp_dir("crash-no-pid");
    let store = ContainerStore::new(data_dir.clone());

    // A Created container with no host_pid — the creating process crashed
    // before recording its PID.
    store
        .upsert(ContainerInfo {
            id: "no-pid".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:nopid".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    let _runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    let containers = ContainerStore::new(data_dir).load_all().unwrap();
    let c = containers.iter().find(|c| c.id == "no-pid").unwrap();
    // host_pid is None → is_some_and returns false → treated as stale.
    assert!(matches!(
        c.status,
        ContainerStatus::Stopped { exit_code: -1 }
    ));
}

#[test]
fn crash_recovery_metadata_persists_across_restarts() {
    let data_dir = unique_temp_dir("crash-persist");
    let store = ContainerStore::new(data_dir.clone());

    store
        .upsert(ContainerInfo {
            id: "persist-1".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:p1".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: Some(110),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    store
        .upsert(ContainerInfo {
            id: "persist-2".to_string(),
            image: "alpine:3.22".to_string(),
            image_id: "sha256:p2".to_string(),
            status: ContainerStatus::Stopped { exit_code: 1 },
            created_unix_secs: 200,
            started_unix_secs: Some(201),
            stopped_unix_secs: Some(210),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    // Restart #1
    let rt1 = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });
    let list1 = rt1.list_containers().unwrap();
    assert_eq!(list1.len(), 2);

    // Restart #2
    let rt2 = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });
    let list2 = rt2.list_containers().unwrap();
    assert_eq!(list2.len(), 2);

    // Original metadata is unchanged.
    let c1 = list2.iter().find(|c| c.id == "persist-1").unwrap();
    assert_eq!(c1.image, "ubuntu:24.04");
    assert_eq!(c1.started_unix_secs, Some(101));
    assert_eq!(c1.stopped_unix_secs, Some(110));

    let c2 = list2.iter().find(|c| c.id == "persist-2").unwrap();
    assert_eq!(c2.image, "alpine:3.22");
    assert!(matches!(
        c2.status,
        ContainerStatus::Stopped { exit_code: 1 }
    ));
}

#[tokio::test]
async fn crash_recovery_reconciled_container_can_be_removed() {
    let data_dir = unique_temp_dir("crash-remove");
    let store = ContainerStore::new(data_dir.clone());

    store
        .upsert(ContainerInfo {
            id: "remove-me".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:rm".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(999_999_999),
        })
        .unwrap();

    // Restart reconciles it to Stopped.
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });

    // Removing the reconciled (now Stopped) container should succeed.
    runtime.remove_container("remove-me").await.unwrap();

    let remaining = runtime.list_containers().unwrap();
    assert!(remaining.is_empty());
}

// ── B15: Lifecycle conformance harness ──

#[tokio::test]
async fn lifecycle_stop_nonrunning_container_is_noop() {
    let data_dir = unique_temp_dir("lc-stop-noop");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    // Seed a Stopped container.
    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "stopped-ctr".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:s1".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: Some(110),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    // Stopping a non-running container returns it unchanged.
    let result = runtime
        .stop_container("stopped-ctr", false, None, None)
        .await
        .unwrap();
    assert!(matches!(
        result.status,
        ContainerStatus::Stopped { exit_code: 0 }
    ));
    assert_eq!(result.stopped_unix_secs, Some(110));
}

#[tokio::test]
async fn lifecycle_stop_created_container_is_noop() {
    let data_dir = unique_temp_dir("lc-stop-created");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "created-ctr".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:c1".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        })
        .unwrap();

    let result = runtime
        .stop_container("created-ctr", false, None, None)
        .await
        .unwrap();
    assert!(matches!(result.status, ContainerStatus::Created));
}

#[tokio::test]
async fn lifecycle_stop_missing_container_returns_error() {
    let data_dir = unique_temp_dir("lc-stop-missing");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    let err = runtime
        .stop_container("nonexistent", false, None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, OciError::Storage(_)));
    if let OciError::Storage(io_err) = err {
        assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
    }
}

#[tokio::test]
async fn lifecycle_remove_missing_container_returns_error() {
    let data_dir = unique_temp_dir("lc-remove-missing");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    let err = runtime.remove_container("nonexistent").await.unwrap_err();
    assert!(matches!(err, OciError::Storage(_)));
    if let OciError::Storage(io_err) = err {
        assert_eq!(io_err.kind(), io::ErrorKind::NotFound);
    }
}

#[tokio::test]
async fn lifecycle_remove_created_container_succeeds() {
    let data_dir = unique_temp_dir("lc-remove-created");
    let rootfs = data_dir.join("rootfs").join("ctr-created");
    fs::create_dir_all(&rootfs).unwrap();

    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "ctr-created".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:c1".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: Some(rootfs.clone()),
            host_pid: Some(process::id()),
        })
        .unwrap();

    runtime.remove_container("ctr-created").await.unwrap();
    assert!(runtime.list_containers().unwrap().is_empty());
    assert!(!rootfs.exists());
}

#[tokio::test]
async fn lifecycle_remove_stopped_container_cleans_rootfs() {
    let data_dir = unique_temp_dir("lc-remove-stopped-rootfs");
    let rootfs = data_dir.join("rootfs").join("ctr-stopped");
    fs::create_dir_all(&rootfs).unwrap();

    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "ctr-stopped".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:s1".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: Some(110),
            rootfs_path: Some(rootfs.clone()),
            host_pid: None,
        })
        .unwrap();

    runtime.remove_container("ctr-stopped").await.unwrap();
    assert!(runtime.list_containers().unwrap().is_empty());
    assert!(!rootfs.exists());
}

#[tokio::test]
async fn lifecycle_exec_on_stopped_container_returns_error() {
    let data_dir = unique_temp_dir("lc-exec-stopped");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "stopped-exec".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:se".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: Some(110),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    let err = runtime
        .exec_container(
            "stopped-exec",
            ExecConfig {
                cmd: vec!["echo".to_string(), "hello".to_string()],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_err();

    // No VM handle exists for a stopped container.
    assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("not be running")));
}

#[tokio::test]
async fn lifecycle_exec_on_created_container_returns_error() {
    let data_dir = unique_temp_dir("lc-exec-created");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "created-exec".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:ce".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        })
        .unwrap();

    let err = runtime
        .exec_container(
            "created-exec",
            ExecConfig {
                cmd: vec!["echo".to_string()],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_err();

    // No VM handle for a Created container that hasn't started.
    assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("not be running")));
}

#[tokio::test]
async fn lifecycle_exec_on_missing_container_returns_error() {
    let data_dir = unique_temp_dir("lc-exec-missing");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    let err = runtime
        .exec_container(
            "ghost",
            ExecConfig {
                cmd: vec!["echo".to_string()],
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_err();

    assert!(matches!(err, OciError::InvalidConfig(ref msg) if msg.contains("not be running")));
}

#[test]
fn lifecycle_list_containers_returns_all_states() {
    let data_dir = unique_temp_dir("lc-list-all");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "created-1".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:a".to_string(),
            status: ContainerStatus::Created,
            created_unix_secs: 100,
            started_unix_secs: None,
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        })
        .unwrap();

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "running-1".to_string(),
            image: "alpine:3.22".to_string(),
            image_id: "sha256:b".to_string(),
            status: ContainerStatus::Running,
            created_unix_secs: 200,
            started_unix_secs: Some(201),
            stopped_unix_secs: None,
            rootfs_path: None,
            host_pid: Some(process::id()),
        })
        .unwrap();

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "stopped-1".to_string(),
            image: "debian:bookworm".to_string(),
            image_id: "sha256:c".to_string(),
            status: ContainerStatus::Stopped { exit_code: 42 },
            created_unix_secs: 50,
            started_unix_secs: Some(51),
            stopped_unix_secs: Some(60),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    let list = runtime.list_containers().unwrap();
    assert_eq!(list.len(), 3);

    // Sorted by ID.
    assert_eq!(list[0].id, "created-1");
    assert!(matches!(list[0].status, ContainerStatus::Created));
    assert_eq!(list[1].id, "running-1");
    assert!(matches!(list[1].status, ContainerStatus::Running));
    assert_eq!(list[2].id, "stopped-1");
    assert!(matches!(
        list[2].status,
        ContainerStatus::Stopped { exit_code: 42 }
    ));
}

#[tokio::test]
async fn lifecycle_double_remove_returns_not_found() {
    let data_dir = unique_temp_dir("lc-double-remove");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    runtime
        .container_store
        .upsert(ContainerInfo {
            id: "once".to_string(),
            image: "ubuntu:24.04".to_string(),
            image_id: "sha256:once".to_string(),
            status: ContainerStatus::Stopped { exit_code: 0 },
            created_unix_secs: 100,
            started_unix_secs: Some(101),
            stopped_unix_secs: Some(110),
            rootfs_path: None,
            host_pid: None,
        })
        .unwrap();

    runtime.remove_container("once").await.unwrap();

    // Second remove should fail with NotFound.
    let err = runtime.remove_container("once").await.unwrap_err();
    assert!(matches!(err, OciError::Storage(_)));
}

#[tokio::test]
async fn lifecycle_oci_sequence_create_start_exec_delete() {
    // Validates the mock OCI lifecycle sequence end-to-end.
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: "world".to_string(),
        stderr: String::new(),
    });

    let output = run_oci_lifecycle(
        &mock,
        "conformance-ctr".to_string(),
        "/run/vz-oci/bundles/conformance-ctr".to_string(),
        "/bin/echo".to_string(),
        vec!["hello".to_string()],
        OciExecOptions::default(),
    )
    .await
    .unwrap();

    assert_eq!(output.exit_code, 0);
    assert_eq!(output.stdout, "world");
    let calls = mock.calls.lock().unwrap();
    assert_eq!(calls.as_slice(), &["create", "start", "exec", "delete"]);
}

#[tokio::test]
async fn lifecycle_oci_kill_graceful_then_state() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });

    let exit_code = stop_via_oci_runtime(&mock, "kill-test", false, Duration::from_secs(5), None)
        .await
        .unwrap();

    // SIGTERM exit convention: 128 + 15 = 143.
    assert_eq!(exit_code, 143);
    let calls = mock.calls.lock().unwrap();
    assert!(calls.contains(&"kill:SIGTERM"));
    assert!(calls.contains(&"state"));
    assert!(!calls.contains(&"kill:SIGKILL"));
}

#[tokio::test]
async fn lifecycle_oci_kill_forced_sends_sigkill() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });

    let exit_code = stop_via_oci_runtime(&mock, "force-kill", true, Duration::from_secs(5), None)
        .await
        .unwrap();

    // SIGKILL exit convention: 128 + 9 = 137.
    assert_eq!(exit_code, 137);
    let calls = mock.calls.lock().unwrap();
    assert!(calls.contains(&"kill:SIGKILL"));
    // Forced kill should not attempt SIGTERM first.
    assert!(!calls.contains(&"kill:SIGTERM"));
}

#[tokio::test]
async fn lifecycle_oci_delete_after_start_failure() {
    let mut mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    mock.fail_start = true;

    let err = run_oci_lifecycle(
        &mock,
        "fail-start".to_string(),
        "/run/vz-oci/bundles/fail-start".to_string(),
        "/bin/echo".to_string(),
        vec![],
        OciExecOptions::default(),
    )
    .await
    .unwrap_err();

    assert!(matches!(err, OciError::InvalidConfig(_)));
    let calls = mock.calls.lock().unwrap();
    // create → start (fails) → delete (cleanup).
    assert_eq!(calls.as_slice(), &["create", "start", "delete"]);
}

#[tokio::test]
async fn lifecycle_oci_exec_with_env_and_cwd() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: "ok".to_string(),
        stderr: String::new(),
    });

    let _ = run_oci_lifecycle(
        &mock,
        "env-cwd-ctr".to_string(),
        "/run/vz-oci/bundles/env-cwd-ctr".to_string(),
        "/usr/bin/env".to_string(),
        vec![],
        OciExecOptions {
            env: vec![("FOO".to_string(), "bar".to_string())],
            cwd: Some("/workspace".to_string()),
            user: Some("1000:1000".to_string()),
        },
    )
    .await
    .unwrap();

    let recorded = mock.exec_call.lock().unwrap();
    let exec = recorded.as_ref().unwrap();
    assert_eq!(exec.command, "/usr/bin/env");
    assert_eq!(
        exec.options.env,
        vec![("FOO".to_string(), "bar".to_string())]
    );
    assert_eq!(exec.options.cwd, Some("/workspace".to_string()));
    assert_eq!(exec.options.user, Some("1000:1000".to_string()));
}
