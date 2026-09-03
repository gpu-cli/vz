#![allow(clippy::unwrap_used)]

use std::env;

use super::stack_vm::{
    activation_error_with_rollback, clear_recovery_route_last, commit_stack_cleanup_batch,
    hosts_write_command, publish_recovery_route_first, require_running_pid,
    require_successful_hosts_write, shutdown_container_cleanup_transition,
};
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
fn container_id_validation_rejects_path_bearing_and_non_ascii_identifiers() {
    let oversized = "x".repeat(129);
    let invalid = [
        "",
        "/absolute/path",
        "../escape",
        ".hidden",
        "_hidden",
        "-option",
        "dir/name",
        "dir\\name",
        "développeur",
        "has space",
        "line\nbreak",
        "control\u{0001}byte",
        oversized.as_str(),
    ];

    for container_id in invalid {
        let error = validate_container_id(container_id).unwrap_err();
        assert!(
            matches!(error, OciError::InvalidConfig(_)),
            "invalid ID must fail closed: {container_id:?}"
        );
    }
}

#[test]
fn container_id_validation_accepts_exact_guest_grammar_and_length_bound() {
    let valid_128 = format!("a{}", "-".repeat(127));

    for container_id in ["a", "A0_name-with.dots-9", valid_128.as_str()] {
        validate_container_id(container_id).unwrap();
    }
    assert_eq!(valid_128.len(), MAX_CONTAINER_ID_BYTES);
}

#[test]
fn every_container_creation_path_validates_id_before_pull() {
    fn assert_validation_precedes_pull(source: &str, function: &str) {
        let body = source
            .split_once(function)
            .unwrap_or_else(|| panic!("missing function marker {function}"))
            .1;
        let validation = body
            .find("validate_container_id(&container_id)?")
            .unwrap_or_else(|| panic!("missing container ID validation in {function}"));
        let pull = body
            .find("self.pull(image).await?")
            .unwrap_or_else(|| panic!("missing image pull in {function}"));
        assert!(
            validation < pull,
            "{function} must reject unsafe IDs before image-store mutation"
        );
    }

    let runtime_source = include_str!("mod.rs");
    assert_validation_precedes_pull(runtime_source, "pub async fn run(");
    assert_validation_precedes_pull(runtime_source, "pub async fn create_container(");
    assert_validation_precedes_pull(
        include_str!("stack_vm.rs"),
        "pub async fn create_container_in_stack(",
    );
}

#[tokio::test]
async fn has_shared_vm_returns_false_when_stack_absent() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("shared-vm-for-none"),
        ..RuntimeConfig::default()
    });
    assert!(!runtime.has_shared_vm("never-booted").await);
}

#[test]
fn running_pid_validation_rejects_stopped_state_with_stale_pid() {
    let state = OciContainerState {
        id: "web".to_string(),
        status: "stopped".to_string(),
        pid: Some(221),
        bundle_path: None,
    };

    let error = require_running_pid("web", "exec", &state).unwrap_err();
    assert!(matches!(
        error,
        OciError::InvalidConfig(ref message)
            if message.contains("not running")
                && message.contains("stopped")
                && message.contains("221")
    ));
}

#[test]
fn hosts_write_validation_rejects_nonzero_exit() {
    let output = ExecOutput {
        exit_code: 1,
        stdout: String::new(),
        stderr: "nsenter: /proc/221/root is gone".to_string(),
    };

    let error = require_successful_hosts_write("web", &output).unwrap_err();
    assert!(matches!(
        error,
        OciError::InvalidConfig(ref message)
            if message.contains("/etc/hosts write failed")
                && message.contains("exit code 1")
                && message.contains("/proc/221/root")
    ));
}

#[test]
fn hosts_write_passes_user_data_as_an_opaque_positional_argument() {
    let content = "127.0.0.1\tlocal'host\n10.0.0.8\t$(touch /tmp/escaped)\n".to_string();
    let (command, args) = hosts_write_command(content.clone());

    assert_eq!(command, "/bin/sh");
    assert_eq!(args[0], "-c");
    assert_eq!(args[1], "set -eu; printf '%s' \"$1\" > /etc/hosts");
    assert_eq!(args[2], "vz-write-hosts");
    assert_eq!(args[3], content);
    assert!(!args[1].contains("local'host"));
    assert!(!args[1].contains("touch /tmp/escaped"));
}

#[test]
fn guest_overlay_teardown_is_verified_ordered_and_uses_opaque_id() {
    let container_id = "stable-name;touch /tmp/not-executed";
    let (command, args) = super::bundle::guest_overlay_teardown_command(container_id);

    assert_eq!(command, "/bin/busybox");
    assert_eq!(args[0], "sh");
    assert_eq!(args[1], "-c");
    assert_eq!(args[3], "vz-overlay-teardown");
    assert_eq!(args[4], container_id);
    assert!(!args[2].contains(container_id));
    let merged_unmount = args[2].find("umount \"$merged\"").unwrap();
    let base_unmount = args[2].find("umount \"$base\"").unwrap();
    let remove = args[2].find("rm -rf \"$base\"").unwrap();
    let verify = args[2].find("test ! -e \"$base\"").unwrap();
    assert!(merged_unmount < base_unmount);
    assert!(base_unmount < remove);
    assert!(remove < verify);
}

#[test]
fn partial_overlay_setup_failure_aggregates_cleanup_failure() {
    let setup = OciError::InvalidConfig("mount overlay failed".to_string());
    let cleanup = OciError::InvalidConfig("unmount tmpfs failed".to_string());
    let error = super::bundle::overlay_setup_error_with_cleanup(setup, Err(cleanup));
    let message = error.to_string();
    assert!(message.contains("mount overlay failed"));
    assert!(message.contains("partial-overlay cleanup also failed"));
    assert!(message.contains("unmount tmpfs failed"));
}

#[tokio::test]
async fn overlay_cleanup_pending_marker_is_generation_scoped() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("overlay-cleanup-pending"),
        ..RuntimeConfig::default()
    });
    let first = ContainerGeneration(7);
    let replacement = ContainerGeneration(8);

    runtime.mark_overlay_cleanup_pending("stable-name", first);
    assert!(runtime.overlay_cleanup_is_pending("stable-name", first));
    assert!(!runtime.overlay_cleanup_is_pending("stable-name", replacement));
    assert_eq!(
        runtime
            .lifecycle_diagnostics()
            .await
            .unwrap()
            .overlay_cleanup_pending,
        1
    );
    runtime.clear_overlay_cleanup_pending("stable-name");
    assert!(!runtime.overlay_cleanup_is_pending("stable-name", first));
}

#[tokio::test]
async fn route_first_publication_and_route_last_clear_survive_cancellation() {
    let routes = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let handles = Arc::new(Mutex::new(HashMap::<String, String>::new()));
    let (published, published_rx) = oneshot::channel();
    let task_routes = Arc::clone(&routes);
    let task_handles = Arc::clone(&handles);
    let publish = tokio::spawn(async move {
        publish_recovery_route_first(
            &task_routes,
            &task_handles,
            "stack",
            "container",
            &"vm".to_string(),
            async move {
                let _ = published.send(());
                std::future::pending::<()>().await;
            },
        )
        .await;
    });
    published_rx.await.unwrap();
    publish.abort();
    let _ = publish.await;
    assert_eq!(
        routes.lock().await.get("container").map(String::as_str),
        Some("stack")
    );
    assert!(!handles.lock().await.contains_key("container"));

    let (overlay_starting, overlay_starting_rx) = oneshot::channel();
    let task_routes = Arc::clone(&routes);
    let task_handles = Arc::clone(&handles);
    let overlay_setup = tokio::spawn(async move {
        publish_recovery_route_first(
            &task_routes,
            &task_handles,
            "stack",
            "container",
            &"vm".to_string(),
            std::future::ready(()),
        )
        .await;
        let _ = overlay_starting.send(());
        std::future::pending::<()>().await;
    });
    overlay_starting_rx.await.unwrap();
    overlay_setup.abort();
    let _ = overlay_setup.await;
    assert_eq!(
        handles.lock().await.get("container").map(String::as_str),
        Some("vm")
    );
    assert_eq!(
        routes.lock().await.get("container").map(String::as_str),
        Some("stack")
    );
    let (handle_cleared, handle_cleared_rx) = oneshot::channel();
    let task_routes = Arc::clone(&routes);
    let task_handles = Arc::clone(&handles);
    let clear = tokio::spawn(async move {
        clear_recovery_route_last(&task_routes, &task_handles, "container", async move {
            let _ = handle_cleared.send(());
            std::future::pending::<()>().await;
        })
        .await;
    });
    handle_cleared_rx.await.unwrap();
    clear.abort();
    let _ = clear.await;
    assert!(!handles.lock().await.contains_key("container"));
    assert!(routes.lock().await.contains_key("container"));

    clear_recovery_route_last(&routes, &handles, "container", std::future::ready(())).await;
    assert!(!routes.lock().await.contains_key("container"));
}

#[tokio::test]
async fn cleanup_failures_retain_recovery_state_and_successful_retry_clears_it() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("shutdown-overlay-transition"),
        ..RuntimeConfig::default()
    });
    let lease = runtime
        .container_store
        .try_acquire_container_write_lease("stable-name")
        .unwrap();
    let generation = runtime
        .container_store
        .reserve_generation_with_write_lease("stable-name", &lease)
        .unwrap();
    runtime
        .container_store
        .upsert_if_generation(
            ContainerInfo {
                id: "stable-name".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:first".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 1,
                started_unix_secs: Some(2),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            },
            generation,
        )
        .unwrap();

    let routes = Mutex::new(HashMap::<String, String>::new());
    let handles = Mutex::new(HashMap::<String, String>::new());
    publish_recovery_route_first(
        &routes,
        &handles,
        "stack",
        "stable-name",
        &"vm".to_string(),
        std::future::ready(()),
    )
    .await;

    let delete_count = std::sync::atomic::AtomicUsize::new(0);
    let overlay_count = std::sync::atomic::AtomicUsize::new(0);

    let delete_failure = shutdown_container_cleanup_transition(
        &runtime,
        "stable-name",
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(OciError::InvalidConfig(
                "injected delete failure".to_string(),
            ))
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap_err();
    assert!(delete_failure.to_string().contains("OCI delete failed"));
    assert_eq!(delete_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(overlay_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert!(routes.lock().await.contains_key("stable-name"));
    assert!(handles.lock().await.contains_key("stable-name"));
    assert!(!runtime.overlay_cleanup_is_pending("stable-name", generation));
    assert!(matches!(
        runtime
            .container_store
            .find("stable-name")
            .unwrap()
            .unwrap()
            .status,
        ContainerStatus::Running
    ));

    let overlay_failure = shutdown_container_cleanup_transition(
        &runtime,
        "stable-name",
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(OciError::InvalidConfig(
                "injected overlay failure".to_string(),
            ))
        },
    )
    .await
    .unwrap_err();
    assert!(
        overlay_failure
            .to_string()
            .contains("overlay teardown failed")
    );
    assert_eq!(delete_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(overlay_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    let persisted = runtime
        .container_store
        .find("stable-name")
        .unwrap()
        .unwrap();
    assert!(matches!(
        persisted.status,
        ContainerStatus::Stopped { exit_code: 0 }
    ));
    assert_eq!(persisted.host_pid, None);
    assert!(runtime.overlay_cleanup_is_pending("stable-name", generation));
    assert!(routes.lock().await.contains_key("stable-name"));
    assert!(handles.lock().await.contains_key("stable-name"));

    shutdown_container_cleanup_transition(
        &runtime,
        "stable-name",
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(
        delete_count.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "retry must skip OCI delete after the durable pending marker"
    );
    assert_eq!(overlay_count.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert!(runtime.overlay_cleanup_is_pending("stable-name", generation));
    assert!(routes.lock().await.contains_key("stable-name"));
    assert!(handles.lock().await.contains_key("stable-name"));
    commit_stack_cleanup_batch(&runtime, &routes, &handles, &["stable-name".to_string()]).await;
    assert!(!runtime.overlay_cleanup_is_pending("stable-name", generation));
    assert!(!routes.lock().await.contains_key("stable-name"));
    assert!(!handles.lock().await.contains_key("stable-name"));
}

#[tokio::test]
async fn pending_delete_marker_skips_signal_and_delete_after_publication_failure() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("shutdown-publication-retry"),
        ..RuntimeConfig::default()
    });
    let lease = runtime
        .container_store
        .try_acquire_container_write_lease("publication-retry")
        .unwrap();
    let generation = runtime
        .container_store
        .reserve_generation_with_write_lease("publication-retry", &lease)
        .unwrap();
    let delete_count = std::sync::atomic::AtomicUsize::new(0);

    let publication_error = shutdown_container_cleanup_transition(
        &runtime,
        "publication-retry",
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async { Ok(()) },
    )
    .await
    .unwrap_err();
    assert!(
        publication_error
            .to_string()
            .contains("stopped-state publication failed")
    );
    assert!(runtime.overlay_cleanup_is_pending("publication-retry", generation));

    runtime
        .container_store
        .upsert_if_generation(
            ContainerInfo {
                id: "publication-retry".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:publication-retry".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 1,
                started_unix_secs: Some(2),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            },
            generation,
        )
        .unwrap();
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    let exit_code = stop_or_reuse_exit_code(
        &mock,
        "publication-retry",
        &ContainerStatus::Running,
        true,
        false,
        Duration::from_secs(5),
        None,
    )
    .await
    .unwrap();
    assert_eq!(exit_code, 0);
    assert!(mock.calls.lock().unwrap().is_empty());

    shutdown_container_cleanup_transition(
        &runtime,
        "publication-retry",
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async { Ok(()) },
    )
    .await
    .unwrap();
    assert_eq!(
        delete_count.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "generation-scoped pending marker must prevent a second OCI delete"
    );
    assert!(matches!(
        runtime
            .container_store
            .find("publication-retry")
            .unwrap()
            .unwrap()
            .status,
        ContainerStatus::Stopped { exit_code: 0 }
    ));
}

#[tokio::test]
async fn stopped_publication_failure_retains_completed_guest_cleanup_for_retry() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("individual-stop-publication-failure"),
        ..RuntimeConfig::default()
    });
    let lease = runtime
        .container_store
        .try_acquire_container_write_lease("publication-failure")
        .unwrap();
    let generation = runtime
        .container_store
        .reserve_generation_with_write_lease("publication-failure", &lease)
        .unwrap();
    let stopped = ContainerInfo {
        id: "publication-failure".to_string(),
        image: "alpine:latest".to_string(),
        image_id: "sha256:publication-failure".to_string(),
        status: ContainerStatus::Stopped { exit_code: 0 },
        created_unix_secs: 1,
        started_unix_secs: Some(2),
        stopped_unix_secs: Some(3),
        rootfs_path: None,
        host_pid: None,
    };
    runtime
        .container_store
        .upsert_if_generation(stopped.clone(), generation)
        .unwrap();
    runtime
        .container_stack
        .lock()
        .await
        .insert(stopped.id.clone(), "stack".to_string());
    runtime.mark_container_vm_stop_complete(&stopped.id, generation);

    let delete_count = std::sync::atomic::AtomicUsize::new(0);
    let overlay_count = std::sync::atomic::AtomicUsize::new(0);
    shutdown_container_cleanup_transition(
        &runtime,
        &stopped.id,
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();

    let error = runtime
        .persist_generation_and_commit_cleanup(
            stopped.clone(),
            ContainerGeneration(generation.0 + 1),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OciError::ContainerAlreadyExists { ref id } if id == &stopped.id
    ));
    assert!(runtime.overlay_cleanup_is_pending(&stopped.id, generation));
    assert!(runtime.stack_guest_cleanup_is_complete(&stopped.id, generation));
    assert!(runtime.container_vm_stop_is_complete(&stopped.id, generation));
    assert!(
        runtime
            .container_stack
            .lock()
            .await
            .contains_key(&stopped.id)
    );

    shutdown_container_cleanup_transition(
        &runtime,
        &stopped.id,
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(delete_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(overlay_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    runtime
        .persist_generation_and_commit_cleanup(stopped.clone(), generation)
        .await
        .unwrap();
    assert!(!runtime.overlay_cleanup_is_pending(&stopped.id, generation));
    assert!(!runtime.stack_guest_cleanup_is_complete(&stopped.id, generation));
    assert!(!runtime.container_vm_stop_is_complete(&stopped.id, generation));
    assert!(
        !runtime
            .container_stack
            .lock()
            .await
            .contains_key(&stopped.id)
    );
}

#[tokio::test]
async fn cancelled_individual_cleanup_commit_retains_all_retry_ownership() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("individual-stop-commit-cancel"),
        ..RuntimeConfig::default()
    });
    let lease = runtime
        .container_store
        .try_acquire_container_write_lease("commit-cancel")
        .unwrap();
    let generation = runtime
        .container_store
        .reserve_generation_with_write_lease("commit-cancel", &lease)
        .unwrap();
    let stopped = ContainerInfo {
        id: "commit-cancel".to_string(),
        image: "alpine:latest".to_string(),
        image_id: "sha256:commit-cancel".to_string(),
        status: ContainerStatus::Stopped { exit_code: 0 },
        created_unix_secs: 1,
        started_unix_secs: Some(2),
        stopped_unix_secs: Some(3),
        rootfs_path: None,
        host_pid: None,
    };
    runtime
        .container_store
        .upsert_if_generation(stopped.clone(), generation)
        .unwrap();
    runtime
        .container_stack
        .lock()
        .await
        .insert(stopped.id.clone(), "stack".to_string());
    runtime.mark_container_vm_stop_complete(&stopped.id, generation);

    let delete_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let overlay_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let deletes = Arc::clone(&delete_count);
    let overlays = Arc::clone(&overlay_count);
    shutdown_container_cleanup_transition(
        &runtime,
        &stopped.id,
        generation,
        || async move {
            deletes.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async move {
            overlays.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();

    let blocked_commit_guard = runtime.setup_restored_containers.lock().await;
    let task_runtime = runtime.clone();
    let task_container = stopped.clone();
    let commit = tokio::spawn(async move {
        task_runtime
            .persist_generation_and_commit_cleanup(task_container, generation)
            .await
    });
    for _ in 0..100 {
        if runtime.active_lifecycle.try_lock().is_err() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        runtime.active_lifecycle.try_lock().is_err(),
        "cleanup commit must reach the blocked map-lock window"
    );
    assert!(runtime.overlay_cleanup_is_pending(&stopped.id, generation));
    assert!(runtime.stack_guest_cleanup_is_complete(&stopped.id, generation));
    assert!(runtime.container_vm_stop_is_complete(&stopped.id, generation));
    commit.abort();
    assert!(commit.await.unwrap_err().is_cancelled());
    drop(blocked_commit_guard);
    assert!(
        runtime
            .container_stack
            .lock()
            .await
            .contains_key(&stopped.id)
    );

    shutdown_container_cleanup_transition(
        &runtime,
        &stopped.id,
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(delete_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(overlay_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    runtime
        .persist_generation_and_commit_cleanup(stopped.clone(), generation)
        .await
        .unwrap();
    assert!(
        !runtime
            .container_stack
            .lock()
            .await
            .contains_key(&stopped.id)
    );
    assert!(!runtime.overlay_cleanup_is_pending(&stopped.id, generation));
    assert!(!runtime.stack_guest_cleanup_is_complete(&stopped.id, generation));
    assert!(!runtime.container_vm_stop_is_complete(&stopped.id, generation));
}

#[tokio::test]
async fn stack_cleanup_batch_retains_earlier_success_until_later_failure_is_retried() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("shutdown-overlay-multi-container"),
        ..RuntimeConfig::default()
    });
    let mut leases = Vec::new();
    let mut generations = HashMap::new();
    for (index, container_id) in ["container-a", "container-b"].into_iter().enumerate() {
        let lease = runtime
            .container_store
            .try_acquire_container_write_lease(container_id)
            .unwrap();
        let generation = runtime
            .container_store
            .reserve_generation_with_write_lease(container_id, &lease)
            .unwrap();
        runtime
            .container_store
            .upsert_if_generation(
                ContainerInfo {
                    id: container_id.to_string(),
                    image: "alpine:latest".to_string(),
                    image_id: format!("sha256:{index}"),
                    status: ContainerStatus::Running,
                    created_unix_secs: 1,
                    started_unix_secs: Some(2),
                    stopped_unix_secs: None,
                    rootfs_path: None,
                    host_pid: Some(process::id()),
                },
                generation,
            )
            .unwrap();
        runtime.active_lifecycle.lock().await.insert(
            container_id.to_string(),
            ActiveContainerLifecycle {
                class: ContainerLifecycleClass::Service,
                auto_remove: false,
            },
        );
        runtime.setup_restored_containers.lock().await.insert(
            container_id.to_string(),
            SetupRestoreIdentity {
                generation,
                commit_ref: format!("commit-{index}"),
            },
        );
        generations.insert(container_id, generation);
        leases.push(lease);
    }

    let routes = Mutex::new(HashMap::<String, String>::new());
    let handles = Mutex::new(HashMap::<String, String>::new());
    for container_id in ["container-a", "container-b"] {
        publish_recovery_route_first(
            &routes,
            &handles,
            "stack",
            container_id,
            &format!("vm-{container_id}"),
            std::future::ready(()),
        )
        .await;
    }

    let delete_a = std::sync::atomic::AtomicUsize::new(0);
    let delete_b = std::sync::atomic::AtomicUsize::new(0);
    let overlay_a = std::sync::atomic::AtomicUsize::new(0);
    let overlay_b = std::sync::atomic::AtomicUsize::new(0);
    let generation_a = generations["container-a"];
    let generation_b = generations["container-b"];

    shutdown_container_cleanup_transition(
        &runtime,
        "container-a",
        generation_a,
        || async {
            delete_a.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_a.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    let failure = shutdown_container_cleanup_transition(
        &runtime,
        "container-b",
        generation_b,
        || async {
            delete_b.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_b.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(OciError::InvalidConfig(
                "injected container-b overlay failure".to_string(),
            ))
        },
    )
    .await
    .unwrap_err();
    assert!(failure.to_string().contains("overlay teardown failed"));

    for container_id in ["container-a", "container-b"] {
        assert!(routes.lock().await.contains_key(container_id));
        assert!(handles.lock().await.contains_key(container_id));
        assert!(runtime.overlay_cleanup_is_pending(container_id, generations[container_id]));
    }
    let retained = runtime.lifecycle_diagnostics().await.unwrap();
    assert_eq!(retained.active_lifecycles, 2);
    assert_eq!(retained.setup_restore_entries, 2);
    assert_eq!(retained.overlay_cleanup_pending, 2);

    shutdown_container_cleanup_transition(
        &runtime,
        "container-a",
        generation_a,
        || async {
            delete_a.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_a.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    shutdown_container_cleanup_transition(
        &runtime,
        "container-b",
        generation_b,
        || async {
            delete_b.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_b.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert_eq!(delete_a.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(delete_b.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(
        overlay_a.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "completed guest cleanup must not rerun while another member is retried"
    );
    assert_eq!(overlay_b.load(std::sync::atomic::Ordering::SeqCst), 2);

    commit_stack_cleanup_batch(
        &runtime,
        &routes,
        &handles,
        &["container-a".to_string(), "container-b".to_string()],
    )
    .await;
    assert!(routes.lock().await.is_empty());
    assert!(handles.lock().await.is_empty());
    let cleared = runtime.lifecycle_diagnostics().await.unwrap();
    assert_eq!(cleared.vm_handles, 0);
    assert_eq!(cleared.container_routes, 0);
    assert_eq!(cleared.exec_bindings, 0);
    assert_eq!(cleared.active_lifecycles, 0);
    assert_eq!(cleared.exec_sessions, 0);
    assert_eq!(cleared.setup_restore_entries, 0);
    assert_eq!(cleared.overlay_cleanup_pending, 0);
    for container_id in ["container-a", "container-b"] {
        assert!(matches!(
            runtime
                .container_store
                .find(container_id)
                .unwrap()
                .unwrap()
                .status,
            ContainerStatus::Stopped { exit_code: 0 }
        ));
    }
    drop(leases);
}

#[tokio::test]
async fn shared_infra_retry_skips_completed_guest_cleanup_and_vm_stop() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("shared-infra-retry"),
        ..RuntimeConfig::default()
    });
    let lease = runtime
        .container_store
        .try_acquire_container_write_lease("shared-member")
        .unwrap();
    let generation = runtime
        .container_store
        .reserve_generation_with_write_lease("shared-member", &lease)
        .unwrap();
    runtime
        .container_store
        .upsert_if_generation(
            ContainerInfo {
                id: "shared-member".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:shared-member".to_string(),
                status: ContainerStatus::Running,
                created_unix_secs: 1,
                started_unix_secs: Some(2),
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: Some(process::id()),
            },
            generation,
        )
        .unwrap();

    runtime
        .container_stack
        .lock()
        .await
        .insert("shared-member".to_string(), "shared-stack".to_string());
    let handles = Mutex::new(HashMap::from([(
        "shared-member".to_string(),
        "shared-vm".to_string(),
    )]));
    let stack_vms = Mutex::new(HashMap::from([(
        "shared-stack".to_string(),
        "shared-vm".to_string(),
    )]));
    let delete_count = std::sync::atomic::AtomicUsize::new(0);
    let overlay_count = std::sync::atomic::AtomicUsize::new(0);
    shutdown_container_cleanup_transition(
        &runtime,
        "shared-member",
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();

    let failing_listener = tokio::spawn(async move {
        panic!("injected first-attempt forwarding failure");
        #[allow(unreachable_code)]
        Ok::<(), String>(())
    });
    runtime.stack_port_forwards.lock().await.insert(
        "shared-stack".to_string(),
        test_port_forwarding(failing_listener),
    );
    shutdown_port_forwarding_registry_entry(&runtime.stack_port_forwards, "shared-stack")
        .await
        .unwrap_err();
    runtime.mark_stack_vm_stop_complete("shared-stack");

    assert!(
        runtime
            .container_stack
            .lock()
            .await
            .contains_key("shared-member")
    );
    assert!(handles.lock().await.contains_key("shared-member"));
    assert!(stack_vms.lock().await.contains_key("shared-stack"));
    assert!(
        runtime
            .stack_port_forwards
            .lock()
            .await
            .contains_key("shared-stack")
    );
    let snapshot_path = unique_temp_dir("shared-infra-retry-snapshot").join("state.vz");
    let restore_error = runtime
        .restore_shared_vm_snapshot("shared-stack", &snapshot_path)
        .await
        .unwrap_err();
    assert!(
        restore_error
            .to_string()
            .contains("teardown cleanup is pending")
    );
    let save_error = runtime
        .save_shared_vm_snapshot("shared-stack", &snapshot_path)
        .await
        .unwrap_err();
    assert!(
        save_error
            .to_string()
            .contains("teardown cleanup is pending")
    );
    runtime.clear_stack_vm_stop_complete("shared-stack");
    assert!(
        runtime
            .restore_shared_vm_snapshot("shared-stack", &snapshot_path)
            .await
            .is_err_and(|error| error.to_string().contains("teardown cleanup is pending")),
        "completed generation-scoped guest cleanup independently blocks restore"
    );
    runtime.mark_stack_vm_stop_complete("shared-stack");

    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    let persisted = runtime
        .container_store
        .find("shared-member")
        .unwrap()
        .unwrap();
    stop_or_reuse_exit_code(
        &mock,
        "shared-member",
        &persisted.status,
        runtime.overlay_cleanup_is_pending("shared-member", generation),
        false,
        Duration::from_secs(5),
        None,
    )
    .await
    .unwrap();
    shutdown_container_cleanup_transition(
        &runtime,
        "shared-member",
        generation,
        || async {
            delete_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
        || async {
            overlay_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(mock.calls.lock().unwrap().is_empty());
    assert_eq!(delete_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(overlay_count.load(std::sync::atomic::Ordering::SeqCst), 1);

    shutdown_port_forwarding_registry_entry(&runtime.stack_port_forwards, "shared-stack")
        .await
        .unwrap();
    let vm_stop_count = std::sync::atomic::AtomicUsize::new(1);
    if !runtime.stack_vm_stop_is_complete("shared-stack") {
        vm_stop_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    assert_eq!(vm_stop_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    stack_vms.lock().await.remove("shared-stack");
    runtime.clear_stack_vm_stop_complete("shared-stack");
    commit_stack_cleanup_batch(
        &runtime,
        &runtime.container_stack,
        &handles,
        &["shared-member".to_string()],
    )
    .await;

    assert!(runtime.container_stack.lock().await.is_empty());
    assert!(handles.lock().await.is_empty());
    assert!(stack_vms.lock().await.is_empty());
    assert!(runtime.stack_port_forwards.lock().await.is_empty());
    let post_cleanup_restore = runtime
        .restore_shared_vm_snapshot("shared-stack", &snapshot_path)
        .await
        .unwrap_err();
    assert!(
        post_cleanup_restore
            .to_string()
            .contains("no shared VM running")
    );
    assert!(
        !post_cleanup_restore
            .to_string()
            .contains("teardown cleanup is pending")
    );
}

#[tokio::test]
async fn snapshot_restore_waits_for_stack_lifecycle_writer() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("snapshot-stack-lifecycle-lock"),
        ..RuntimeConfig::default()
    });
    let stack_lock = runtime.stack_lifecycle_lock("locked-stack").await;
    let guard = stack_lock.write_owned().await;
    let restore_runtime = runtime.clone();
    let restore = tokio::spawn(async move {
        restore_runtime
            .restore_shared_vm_snapshot("locked-stack", Path::new("unused-snapshot"))
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !restore.is_finished(),
        "restore must wait behind the active stack lifecycle writer"
    );

    drop(guard);
    let error = restore.await.unwrap().unwrap_err();
    assert!(error.to_string().contains("no shared VM running"));
}

#[test]
fn macos_runtime_container_exec_adapters_never_construct_namespace_argv() {
    let exec_source = include_str!("exec.rs");
    let lifecycle_source = include_str!("oci_lifecycle.rs");
    let stack_source = include_str!("stack_vm.rs");

    for (name, source) in [
        ("exec", exec_source),
        ("lifecycle", lifecycle_source),
        ("stack", stack_source),
    ] {
        assert!(
            !source.contains("nsenter"),
            "{name} adapter must send container identity and raw argv to the guest"
        );
    }
    assert!(exec_source.contains("exec_container_stream_ready_with_options"));
    assert!(exec_source.contains("exec_container_interactive_ready"));
    assert!(exec_source.contains("vm.oci_exec("));
    assert!(exec_source.matches("id.to_string(),").count() >= 3);
    assert!(exec_source.matches("command").count() >= 3);
    assert!(lifecycle_source.contains("exec_container_collect_with_options"));
    assert!(lifecycle_source.contains(
        "                    id,\n                    command,\n                    args,"
    ));
    assert!(stack_source.contains("exec_container_collect_with_options"));
    assert!(stack_source.contains("                    oci_container_id.clone(),\n                    hosts_command,\n                    hosts_args,"));
}

#[tokio::test]
async fn oci_unary_adapter_rejects_pty_before_resolving_a_vm() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("oci-unary-pty"),
        ..RuntimeConfig::default()
    });
    let error = runtime
        .exec_container_oci_unary(
            "missing",
            ExecConfig {
                cmd: vec!["/bin/true".to_string()],
                pty: true,
                ..ExecConfig::default()
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        OciError::ExecutionControlUnsupported { operation, reason }
            if operation == "exec_container_oci_unary" && reason.contains("does not support PTY")
    ));
}

#[test]
fn activation_failure_reports_rollback_failure_without_losing_primary_error() {
    let error = activation_error_with_rollback(
        OciError::InvalidConfig("post-start liveness failed".to_string()),
        Err(OciError::InvalidConfig(
            "OCI delete failed; tracking retained".to_string(),
        )),
    );

    let message = error.to_string();
    assert!(message.contains("post-start liveness failed"));
    assert!(message.contains("OCI delete failed; tracking retained"));
}

#[tokio::test]
async fn activation_locks_serialize_one_stack_but_not_distinct_stacks() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("activation-locks"),
        ..RuntimeConfig::default()
    });
    let same_a = runtime.stack_activation_lock("stack-a").await;
    let same_a_again = runtime.stack_activation_lock("stack-a").await;
    let stack_b = runtime.stack_activation_lock("stack-b").await;
    assert!(Arc::ptr_eq(&same_a, &same_a_again));
    assert!(!Arc::ptr_eq(&same_a, &stack_b));

    let first_guard = same_a.lock().await;
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let waiter = tokio::spawn(async move {
        let _guard = same_a_again.lock().await;
        let _ = entered_tx.send(());
    });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished(), "same-stack activation must wait");

    // A different stack lock remains independently acquirable while stack-a
    // is held.
    let stack_b_guard = stack_b.try_lock().expect("distinct stack must not wait");
    drop(stack_b_guard);
    drop(first_guard);
    entered_rx.await.unwrap();
    waiter.await.unwrap();
}

#[tokio::test]
async fn activation_guard_proves_the_same_stack_lock_is_held() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("activation-guard"),
        ..RuntimeConfig::default()
    });
    let same_stack = runtime.stack_activation_lock("stack-a").await;
    let distinct_stack = runtime.stack_activation_lock("stack-b").await;

    let activation_guard = runtime.acquire_stack_activation_guard("stack-a").await;
    assert!(
        same_stack.try_lock().is_err(),
        "activation permit must own the same-stack lock"
    );
    assert!(
        distinct_stack.try_lock().is_ok(),
        "activation permit must not serialize independent stacks"
    );
    drop(activation_guard);
    assert!(same_stack.try_lock().is_ok());
}

#[tokio::test]
async fn caller_selected_id_is_serialized_and_duplicate_is_rejected() {
    let data_dir = unique_temp_dir("container-generation-lock");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });
    let mut first_run = RunConfig {
        container_id: Some("fixed-id".to_string()),
        ..RunConfig::default()
    };
    let first = runtime
        .begin_container_create(&mut first_run, None)
        .await
        .unwrap();
    runtime
        .persist_owned(
            &first,
            ContainerInfo {
                id: "fixed-id".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:first".to_string(),
                status: ContainerStatus::Created,
                created_unix_secs: 1,
                started_unix_secs: None,
                stopped_unix_secs: None,
                rootfs_path: None,
                host_pid: None,
            },
        )
        .unwrap();

    let waiter_runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });
    let duplicate = tokio::time::timeout(std::time::Duration::from_millis(100), async move {
        let mut run = RunConfig {
            container_id: Some("fixed-id".to_string()),
            ..RunConfig::default()
        };
        waiter_runtime.begin_container_create(&mut run, None).await
    })
    .await
    .expect("duplicate create admission must fail without waiting for the owner");
    let error = match duplicate {
        Ok(_) => panic!("duplicate generation unexpectedly reserved"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        OciError::ContainerAlreadyExists { ref id } if id == "fixed-id"
    ));

    drop(first);
    let mut stopped_duplicate = RunConfig {
        container_id: Some("fixed-id".to_string()),
        ..RunConfig::default()
    };
    let error = match runtime
        .begin_container_create(&mut stopped_duplicate, None)
        .await
    {
        Ok(_) => panic!("duplicate generation unexpectedly reserved"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("already exists"));
}

#[tokio::test]
async fn second_runtime_stop_remove_waits_for_cross_process_id_writer() {
    let data_dir = unique_temp_dir("cross-runtime-waiting-writer");
    let owner = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });
    let contender = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });
    let mut run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let transaction = owner.begin_container_create(&mut run, None).await.unwrap();
    owner
        .persist_owned(
            &transaction,
            ContainerInfo {
                id: "stable-name".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:first".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 1,
                started_unix_secs: None,
                stopped_unix_secs: Some(2),
                rootfs_path: None,
                host_pid: None,
            },
        )
        .unwrap();

    let waiter = tokio::spawn(async move { contender.remove_container("stable-name").await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(transaction);
    waiter.await.unwrap().unwrap();
}

#[tokio::test]
async fn second_runtime_writer_waits_for_cross_process_exec_admission() {
    let data_dir = unique_temp_dir("cross-runtime-read-admission");
    let reader = Runtime::new(RuntimeConfig {
        data_dir: data_dir.clone(),
        ..RuntimeConfig::default()
    });
    let writer = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });
    let mut run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let transaction = reader.begin_container_create(&mut run, None).await.unwrap();
    reader
        .persist_owned(
            &transaction,
            ContainerInfo {
                id: "stable-name".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:first".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 1,
                started_unix_secs: None,
                stopped_unix_secs: Some(2),
                rootfs_path: None,
                host_pid: None,
            },
        )
        .unwrap();
    drop(transaction);
    let admission = reader
        .acquire_container_read_admission("stable-name")
        .await
        .unwrap();
    let waiter = tokio::spawn(async move { writer.begin_existing_container("stable-name").await });
    tokio::task::yield_now().await;
    assert!(!waiter.is_finished());
    drop(admission);
    assert!(waiter.await.unwrap().is_ok());
}

#[tokio::test]
async fn lifecycle_observer_reports_stop_request_before_writer_admission() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("lifecycle-observer-stop-order"),
        ..RuntimeConfig::default()
    });
    let mut run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let transaction = runtime
        .begin_container_create(&mut run, None)
        .await
        .unwrap();
    runtime
        .persist_owned(
            &transaction,
            ContainerInfo {
                id: "stable-name".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:first".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 1,
                started_unix_secs: None,
                stopped_unix_secs: Some(2),
                rootfs_path: None,
                host_pid: None,
            },
        )
        .unwrap();
    drop(transaction);

    let read_admission = runtime
        .acquire_container_read_admission("stable-name")
        .await
        .unwrap();
    let mut observer = runtime.install_lifecycle_observer();
    let stopping_runtime = runtime.clone();
    tokio::task::LocalSet::new()
        .run_until(async move {
            let stop = tokio::task::spawn_local(async move {
                stopping_runtime
                    .stop_container("stable-name", true, None, None)
                    .await
            });
            let requested = observer.recv().await.unwrap();
            assert_eq!(
                requested.kind(),
                RuntimeLifecycleAdmissionKind::StopWriterRequested
            );
            requested.resume();
            assert!(
                tokio::time::timeout(Duration::from_millis(25), observer.recv())
                    .await
                    .is_err(),
                "writer admission was reported while a read lease was held"
            );
            drop(read_admission);
            let acquired = observer.recv().await.unwrap();
            assert_eq!(
                acquired.kind(),
                RuntimeLifecycleAdmissionKind::StopWriterAcquired
            );
            acquired.resume();
            stop.await.unwrap().unwrap();
        })
        .await;
}

#[tokio::test]
async fn explicit_remove_allows_same_name_with_higher_generation() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("container-generation-recreate"),
        ..RuntimeConfig::default()
    });
    let mut first_run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let first = runtime
        .begin_container_create(&mut first_run, None)
        .await
        .unwrap();
    let first_generation = first.generation();
    runtime
        .persist_owned(
            &first,
            ContainerInfo {
                id: "stable-name".to_string(),
                image: "alpine:latest".to_string(),
                image_id: "sha256:first".to_string(),
                status: ContainerStatus::Stopped { exit_code: 0 },
                created_unix_secs: 1,
                started_unix_secs: Some(2),
                stopped_unix_secs: Some(3),
                rootfs_path: None,
                host_pid: None,
            },
        )
        .unwrap();
    drop(first);

    runtime.remove_container("stable-name").await.unwrap();
    let mut second_run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let second = runtime
        .begin_container_create(&mut second_run, None)
        .await
        .unwrap();
    assert!(second.generation().0 > first_generation.0);
}

#[tokio::test]
async fn cancelled_rootfs_waiter_cleans_before_releasing_generation() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("rootfs-cancelled-waiter"),
        ..RuntimeConfig::default()
    });
    let mut run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let mut transaction = runtime
        .begin_container_create(&mut run, None)
        .await
        .unwrap();
    let generation = transaction.generation();
    let rootfs = runtime.data_dir().join("rootfs/stable-name");
    fs::create_dir_all(&rootfs).unwrap();
    fs::write(rootfs.join("assembled"), b"old generation").unwrap();

    let returned = RootfsAssemblyReturn {
        lease: Some(transaction.take_lease()),
        result: Some(Ok(rootfs.clone())),
        container_store: runtime.container_store.clone(),
        container_id: "stable-name".to_string(),
        generation,
    };
    drop(returned);
    assert!(!rootfs.exists());

    let mut retry = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let replacement = runtime
        .begin_container_create(&mut retry, None)
        .await
        .unwrap();
    assert!(replacement.generation().0 > generation.0);
}

#[test]
fn startup_orphan_cleanup_preserves_reserved_generation_writer() {
    let data_dir = unique_temp_dir("reserved-rootfs-startup-cleanup");
    let store = ContainerStore::new(data_dir.clone());
    let generation = store.reserve_generation("stable-name").unwrap();
    let rootfs = data_dir.join("rootfs/stable-name");
    fs::create_dir_all(&rootfs).unwrap();
    fs::write(rootfs.join("partial"), b"active generation").unwrap();

    let _runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });
    assert!(rootfs.exists());
    assert!(
        store
            .release_generation_if_absent("stable-name", generation)
            .unwrap()
    );
}

#[tokio::test]
async fn setup_restore_identity_is_generation_and_commit_scoped() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("setup-restore-generation"),
        ..RuntimeConfig::default()
    });
    let mut run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let transaction = runtime
        .begin_container_create(&mut run, None)
        .await
        .unwrap();
    runtime.setup_restored_containers.lock().await.insert(
        "stable-name".to_string(),
        SetupRestoreIdentity {
            generation: transaction.generation(),
            commit_ref: "commit-a".to_string(),
        },
    );
    assert!(runtime.was_setup_restored("stable-name", "commit-a").await);
    assert!(!runtime.was_setup_restored("stable-name", "commit-b").await);
    drop(transaction);

    let mut replacement = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let _replacement = runtime
        .begin_container_create(&mut replacement, None)
        .await
        .unwrap();
    assert!(!runtime.was_setup_restored("stable-name", "commit-a").await);
}

#[tokio::test]
async fn lifecycle_diagnostics_reports_reservations_and_map_counts() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("lifecycle-diagnostics"),
        ..RuntimeConfig::default()
    });
    let mut run = RunConfig {
        container_id: Some("stable-name".to_string()),
        ..RunConfig::default()
    };
    let transaction = runtime
        .begin_container_create(&mut run, None)
        .await
        .unwrap();
    let active = runtime.lifecycle_diagnostics().await.unwrap();
    assert_eq!(active.container_lock_slots, 1);
    assert_eq!(active.vm_handles, 0);
    assert!(active.vm_handle_ids.is_empty());
    assert_eq!(active.stack_vms, 0);
    assert!(active.stack_vm_ids.is_empty());
    assert_eq!(active.container_routes, 0);
    assert!(active.container_route_pairs.is_empty());
    assert_eq!(active.stack_port_forwards, 0);
    assert!(active.stack_port_forward_ids.is_empty());
    assert_eq!(active.exec_bindings, 0);
    assert_eq!(active.active_lifecycles, 0);
    assert!(
        active
            .generations
            .iter()
            .any(|generation| { generation.container_id == "stable-name" && generation.reserved })
    );
    drop(transaction);
    let released = runtime.lifecycle_diagnostics().await.unwrap();
    assert!(
        released
            .generations
            .iter()
            .any(|generation| { generation.container_id == "stable-name" && !generation.reserved })
    );
}

#[test]
fn readiness_event_pairs_host_generation_with_complete_guest_identity() {
    let identity = |device, inode| vz_linux::KernelObjectIdentity { device, inode };
    let ready = container_ready_generation(
        "stable-name",
        ContainerGeneration(17),
        vz_linux::ContainerGeneration {
            container_id: "stable-name".to_string(),
            init_pid: 42,
            init_start_time: 9001,
            cgroup_path: "/vz/stable-name".to_string(),
            cgroup: Some(identity(1, 2)),
            namespaces: Some(vz_linux::ContainerNamespaceIdentity {
                mount: Some(identity(3, 4)),
                network: Some(identity(5, 6)),
                pid: Some(identity(7, 8)),
                ipc: Some(identity(9, 10)),
                uts: Some(identity(11, 12)),
            }),
            root: Some(identity(13, 14)),
        },
    )
    .unwrap();

    assert_eq!(ready.lifecycle_generation, 17);
    assert_eq!(ready.container_id, "stable-name");
    assert_eq!(ready.init_pid, 42);
    assert_eq!(
        ready.cgroup,
        KernelObjectIdentity {
            device: 1,
            inode: 2
        }
    );
    assert_eq!(
        ready.namespaces.pid,
        KernelObjectIdentity {
            device: 7,
            inode: 8
        }
    );
    assert_eq!(
        ready.root,
        KernelObjectIdentity {
            device: 13,
            inode: 14
        }
    );
}

#[test]
fn readiness_rejects_guest_container_id_mismatch() {
    let error = container_ready_generation(
        "requested",
        ContainerGeneration(1),
        vz_linux::ContainerGeneration {
            container_id: "different".to_string(),
            ..vz_linux::ContainerGeneration::default()
        },
    )
    .unwrap_err();
    assert!(error.to_string().contains("identity mismatch"));
}

#[tokio::test]
async fn stack_lifecycle_gate_precedes_container_generation_gate() {
    let runtime = Runtime::new(RuntimeConfig {
        data_dir: unique_temp_dir("stack-generation-order"),
        ..RuntimeConfig::default()
    });
    let stack_write = runtime
        .stack_lifecycle_lock("stack-a")
        .await
        .write_owned()
        .await;
    let waiter_runtime = runtime.clone();
    let waiter = tokio::spawn(async move {
        let mut run = RunConfig {
            container_id: Some("service-a".to_string()),
            ..RunConfig::default()
        };
        waiter_runtime
            .begin_container_create(&mut run, Some("stack-a"))
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "create must wait behind stack shutdown/boot"
    );

    let mut other_run = RunConfig {
        container_id: Some("service-b".to_string()),
        ..RunConfig::default()
    };
    let other = runtime
        .begin_container_create(&mut other_run, Some("stack-b"))
        .await
        .unwrap();
    drop(other);
    drop(stack_write);
    assert!(waiter.await.unwrap().is_ok());
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
    assert!(matches!(
        err,
        OciError::ContainerNotFound { ref id } if id == "container-1"
    ));
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
    let transaction = runtime.begin_existing_container("one-off").await.unwrap();
    runtime
        .finalize_one_off_cleanup("one-off", true, &transaction)
        .await;

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
async fn stop_via_oci_runtime_already_stopped_is_idempotent_without_kill() {
    let mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    *mock.state_status.lock().unwrap() = "stopped".to_string();

    let exit_code =
        stop_via_oci_runtime(&mock, "svc-finished", false, Duration::from_secs(5), None)
            .await
            .unwrap();

    assert_eq!(exit_code, 0);
    assert_eq!(*mock.calls.lock().unwrap(), vec!["state"]);
}

#[tokio::test]
async fn stop_via_oci_runtime_accepts_natural_exit_racing_with_kill() {
    let mut mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    mock.fail_kill = true;
    mock.stop_on_failed_kill = true;

    let exit_code = stop_via_oci_runtime(&mock, "svc-raced", false, Duration::from_secs(5), None)
        .await
        .unwrap();

    assert_eq!(exit_code, 0);
    assert_eq!(
        *mock.calls.lock().unwrap(),
        vec!["state", "kill:SIGTERM", "state"]
    );
}

#[tokio::test]
async fn stopped_retry_reuses_exit_code_and_retries_oci_delete() {
    let mut mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    mock.fail_delete_calls = 1;

    let first_exit = stop_or_reuse_exit_code(
        &mock,
        "svc-delete-retry",
        &ContainerStatus::Running,
        false,
        false,
        Duration::from_secs(5),
        None,
    )
    .await
    .unwrap();
    let first_delete = mock
        .oci_delete("svc-delete-retry".to_string(), true)
        .await
        .unwrap_err();
    assert!(first_delete.to_string().contains("mock OCI delete failure"));

    let retry_exit = stop_or_reuse_exit_code(
        &mock,
        "svc-delete-retry",
        &ContainerStatus::Stopped {
            exit_code: first_exit,
        },
        false,
        false,
        Duration::from_secs(5),
        None,
    )
    .await
    .unwrap();
    mock.oci_delete("svc-delete-retry".to_string(), true)
        .await
        .unwrap();

    assert_eq!(retry_exit, first_exit);
    assert_eq!(
        *mock.calls.lock().unwrap(),
        vec!["state", "kill:SIGTERM", "state", "delete", "delete"],
        "retry must resume at OCI delete without re-signalling"
    );
}

#[tokio::test]
async fn stop_via_oci_runtime_preserves_kill_error_while_still_running() {
    let mut mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    mock.fail_kill = true;

    let error = stop_via_oci_runtime(&mock, "svc-running", false, Duration::from_secs(5), None)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("mock kill failure"));
    assert_eq!(
        *mock.calls.lock().unwrap(),
        vec!["state", "kill:SIGTERM", "state"]
    );
}

#[tokio::test]
async fn stop_via_oci_runtime_preserves_kill_error_when_race_recheck_transport_fails() {
    let mut mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    mock.fail_kill = true;
    mock.fail_state_on_call = Some(2);

    let error = stop_via_oci_runtime(
        &mock,
        "svc-race-unknown",
        false,
        Duration::from_secs(5),
        None,
    )
    .await
    .unwrap_err();

    assert!(error.to_string().contains("mock kill failure"));
    assert!(!error.to_string().contains("mock state transport failure"));
    assert_eq!(
        *mock.calls.lock().unwrap(),
        vec!["state", "kill:SIGTERM", "state"]
    );
}

#[tokio::test]
async fn stop_via_oci_runtime_preserves_authoritative_state_transport_error() {
    let mut mock = MockOciLifecycleOps::new(ExecOutput {
        exit_code: 0,
        stdout: String::new(),
        stderr: String::new(),
    });
    mock.fail_state = true;

    let error = stop_via_oci_runtime(&mock, "svc-unknown", false, Duration::from_secs(5), None)
        .await
        .unwrap_err();

    assert!(error.to_string().contains("mock state transport failure"));
    assert_eq!(*mock.calls.lock().unwrap(), vec!["state"]);
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
        fn exec_in_container<'a>(
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
fn resolve_run_config_preserves_cpu_bandwidth_limits() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        ..ImageConfigSummary::default()
    };

    let run = RunConfig {
        cpu_quota: Some(50_000),
        cpu_period: Some(100_000),
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "container-123").unwrap();
    assert_eq!(resolved.cpu_quota, Some(50_000));
    assert_eq!(resolved.cpu_period, Some(100_000));
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
    fail_kill: bool,
    stop_on_failed_kill: bool,
    fail_state: bool,
    fail_state_on_call: Option<usize>,
    fail_delete_calls: usize,
    state_calls: std::sync::atomic::AtomicUsize,
    delete_calls: std::sync::atomic::AtomicUsize,
    state_status: std::sync::Mutex<String>,
}

impl MockOciLifecycleOps {
    fn new(exec_output: ExecOutput) -> Self {
        Self {
            calls: std::sync::Mutex::new(Vec::new()),
            exec_call: std::sync::Mutex::new(None),
            exec_output,
            fail_start: false,
            fail_kill: false,
            stop_on_failed_kill: false,
            fail_state: false,
            fail_state_on_call: None,
            fail_delete_calls: 0,
            state_calls: std::sync::atomic::AtomicUsize::new(0),
            delete_calls: std::sync::atomic::AtomicUsize::new(0),
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

    fn exec_in_container<'a>(
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
        if self.fail_kill {
            if self.stop_on_failed_kill {
                *self.state_status.lock().unwrap() = "stopped".to_string();
            }
            return Box::pin(async {
                Err(OciError::InvalidConfig("mock kill failure".to_string()))
            });
        }
        // Simulate: after kill, container becomes stopped.
        *self.state_status.lock().unwrap() = "stopped".to_string();
        Box::pin(async { Ok(()) })
    }

    fn oci_state<'a>(&'a self, id: String) -> OciLifecycleFuture<'a, OciContainerState> {
        self.calls.lock().unwrap().push("state");
        let call = self
            .state_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        if self.fail_state || self.fail_state_on_call == Some(call) {
            return Box::pin(async {
                Err(OciError::InvalidConfig(
                    "mock state transport failure".to_string(),
                ))
            });
        }
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
        let call = self
            .delete_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let should_fail = call <= self.fail_delete_calls;
        Box::pin(async move {
            if should_fail {
                Err(OciError::InvalidConfig(
                    "mock OCI delete failure".to_string(),
                ))
            } else {
                Ok(())
            }
        })
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
fn resolved_exec_defaults_inherit_image_user_and_working_directory() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        env: Some(vec!["IMAGE_DEFAULT=image".to_string()]),
        working_dir: Some("/workspace/image".to_string()),
        user: Some("image-user:image-group".to_string()),
        ..ImageConfigSummary::default()
    };

    let resolved =
        resolve_run_config(image_config, RunConfig::default(), "image-container").unwrap();
    let defaults = ContainerExecDefaults::from(&resolved);

    assert_eq!(defaults.working_dir, Some("/workspace/image".to_string()));
    assert_eq!(defaults.user, Some("image-user:image-group".to_string()));
    assert_eq!(
        defaults.env,
        vec![
            ("IMAGE_DEFAULT".to_string(), "image".to_string()),
            ("VZ_CONTAINER_ID".to_string(), "image-container".to_string(),),
        ]
    );
}

#[test]
fn resolved_exec_defaults_capture_image_and_run_overrides() {
    let image_config = ImageConfigSummary {
        cmd: Some(vec!["default".to_string()]),
        env: Some(vec![
            "IMAGE_ONLY=image".to_string(),
            "OVERRIDE=image".to_string(),
        ]),
        working_dir: Some("/workspace/image".to_string()),
        user: Some("image-user:image-group".to_string()),
        ..ImageConfigSummary::default()
    };
    let run = RunConfig {
        env: vec![("OVERRIDE".to_string(), "run".to_string())],
        working_dir: Some("/workspace/run".to_string()),
        user: Some("run-user:run-group".to_string()),
        ..RunConfig::default()
    };

    let resolved = resolve_run_config(image_config, run, "defaults-container").unwrap();
    let defaults = ContainerExecDefaults::from(&resolved);

    assert_eq!(defaults.working_dir, Some("/workspace/run".to_string()));
    assert_eq!(defaults.user, Some("run-user:run-group".to_string()));
    assert_eq!(
        defaults.env,
        vec![
            ("IMAGE_ONLY".to_string(), "image".to_string()),
            ("OVERRIDE".to_string(), "run".to_string()),
            (
                "VZ_CONTAINER_ID".to_string(),
                "defaults-container".to_string(),
            ),
        ]
    );
}

#[test]
fn omitted_and_empty_exec_fields_inherit_resolved_defaults() {
    let defaults = ContainerExecDefaults {
        env: vec![("BASE".to_string(), "configured".to_string())],
        working_dir: Some("/workspace/default".to_string()),
        user: Some("developer:staff".to_string()),
    };

    for exec in [
        ExecConfig::default(),
        ExecConfig {
            working_dir: Some(String::new()),
            user: Some(String::new()),
            ..ExecConfig::default()
        },
    ] {
        let options = resolve_container_exec_options(&defaults, &exec);
        assert_eq!(options.working_dir, Some("/workspace/default".to_string()));
        assert_eq!(options.user, Some("developer:staff".to_string()));
        assert_eq!(
            options.env,
            vec![("BASE".to_string(), "configured".to_string())]
        );
    }
}

#[test]
fn explicit_exec_values_override_defaults_with_last_environment_value_winning() {
    let defaults = ContainerExecDefaults {
        env: vec![
            ("BASE".to_string(), "first".to_string()),
            ("DUPLICATE".to_string(), "old-a".to_string()),
            ("DUPLICATE".to_string(), "old-b".to_string()),
        ],
        working_dir: Some("/workspace/default".to_string()),
        user: Some("developer".to_string()),
    };
    let before = defaults.clone();
    let exec = ExecConfig {
        env: vec![
            ("DUPLICATE".to_string(), "exec-a".to_string()),
            ("EXEC_ONLY".to_string(), "present".to_string()),
            ("DUPLICATE".to_string(), "exec-b".to_string()),
        ],
        working_dir: Some("/workspace/exec".to_string()),
        user: Some("operator:wheel".to_string()),
        ..ExecConfig::default()
    };

    let options = resolve_container_exec_options(&defaults, &exec);

    assert_eq!(options.working_dir, Some("/workspace/exec".to_string()));
    assert_eq!(options.user, Some("operator:wheel".to_string()));
    assert_eq!(
        options.env,
        vec![
            ("BASE".to_string(), "first".to_string()),
            ("DUPLICATE".to_string(), "exec-b".to_string()),
            ("EXEC_ONLY".to_string(), "present".to_string()),
        ]
    );
    assert_eq!(
        defaults, before,
        "exec resolution must not mutate the cache"
    );
}

#[test]
fn exec_defaults_use_root_only_when_no_working_directory_is_configured() {
    let defaults = ContainerExecDefaults {
        env: Vec::new(),
        working_dir: None,
        user: None,
    };

    let options = resolve_container_exec_options(&defaults, &ExecConfig::default());

    assert_eq!(options.working_dir, Some("/".to_string()));
    assert!(options.user.is_none());
}

#[test]
fn absent_exec_binding_fails_closed_during_activation_or_after_shutdown() {
    let error =
        resolve_container_exec_binding::<String>("not-published", None, &ExecConfig::default())
            .unwrap_err();

    assert!(matches!(
        error,
        OciError::InvalidConfig(ref message)
            if message.contains("not-published")
                && message.contains("no active exec binding")
                && message.contains("may not be running")
                && message.contains("activating")
    ));
}

#[test]
fn exec_binding_atomically_couples_vm_generation_and_default_snapshot() {
    let first_vm = Arc::new("vm-generation-one".to_string());
    let second_vm = Arc::new("vm-generation-two".to_string());
    let mut bindings: HashMap<String, ContainerExecBinding<String>> = HashMap::new();
    bindings.insert(
        "container".to_string(),
        ContainerExecBinding {
            vm: Arc::clone(&first_vm),
            generation: ContainerGeneration(1),
            defaults: ContainerExecDefaults {
                env: vec![("GENERATION".to_string(), "one".to_string())],
                working_dir: Some("/generation-one".to_string()),
                user: Some("user-one".to_string()),
            },
        },
    );

    let (resolved_vm, resolved_options) = resolve_container_exec_binding(
        "container",
        bindings.get("container"),
        &ExecConfig::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&resolved_vm, &first_vm));
    assert_eq!(
        resolved_options.working_dir,
        Some("/generation-one".to_string())
    );
    assert_eq!(resolved_options.user, Some("user-one".to_string()));
    assert_eq!(
        resolved_options.env,
        vec![("GENERATION".to_string(), "one".to_string())]
    );

    bindings.insert(
        "container".to_string(),
        ContainerExecBinding {
            vm: Arc::clone(&second_vm),
            generation: ContainerGeneration(2),
            defaults: ContainerExecDefaults {
                env: vec![("GENERATION".to_string(), "two".to_string())],
                working_dir: Some("/generation-two".to_string()),
                user: Some("user-two".to_string()),
            },
        },
    );
    let (resolved_vm, resolved_options) = resolve_container_exec_binding(
        "container",
        bindings.get("container"),
        &ExecConfig::default(),
    )
    .unwrap();
    assert!(Arc::ptr_eq(&resolved_vm, &second_vm));
    assert_eq!(
        resolved_options.working_dir,
        Some("/generation-two".to_string())
    );
    assert_eq!(resolved_options.user, Some("user-two".to_string()));
    assert_eq!(
        resolved_options.env,
        vec![("GENERATION".to_string(), "two".to_string())]
    );
}

#[test]
fn concrete_lifecycle_adapter_preserves_options_and_applies_final_cwd_fallback() {
    let options = lifecycle_exec_options(OciExecOptions {
        env: vec![("LIFECYCLE".to_string(), "preserved".to_string())],
        cwd: Some("/workspace/lifecycle".to_string()),
        user: Some("1234:2345".to_string()),
    });
    assert_eq!(
        options.working_dir,
        Some("/workspace/lifecycle".to_string())
    );
    assert_eq!(
        options.env,
        vec![("LIFECYCLE".to_string(), "preserved".to_string())]
    );
    assert_eq!(options.user, Some("1234:2345".to_string()));

    let fallback = lifecycle_exec_options(OciExecOptions::default());
    assert_eq!(fallback.working_dir, Some("/".to_string()));
    assert!(fallback.env.is_empty());
    assert!(fallback.user.is_none());
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
            profile: None,
            security_profile: None,
            busybox: "1.37.0".to_string(),
            agent: "0.1.0".to_string(),
            agent_protocol_revision: Some(1),
            youki: "0.5.7".to_string(),
            built: Some("2026-02-18T00:00:00Z".to_string()),
            sha256_vmlinux: None,
            sha256_initramfs: None,
            sha256_youki: None,
            capabilities: None,
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
    assert!(matches!(
        err,
        OciError::ContainerNotFound { ref id } if id == "nonexistent"
    ));
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
    assert!(matches!(
        err,
        OciError::ContainerNotFound { ref id } if id == "nonexistent"
    ));
}

#[tokio::test]
async fn lifecycle_remove_missing_container_returns_error() {
    let data_dir = unique_temp_dir("lc-remove-missing");
    let runtime = Runtime::new(RuntimeConfig {
        data_dir,
        ..RuntimeConfig::default()
    });

    let err = runtime.remove_container("nonexistent").await.unwrap_err();
    assert!(matches!(
        err,
        OciError::ContainerNotFound { ref id } if id == "nonexistent"
    ));
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

    assert!(matches!(
        err,
        OciError::ContainerNotFound { ref id } if id == "ghost"
    ));
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
    assert!(matches!(
        err,
        OciError::ContainerNotFound { ref id } if id == "once"
    ));
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
