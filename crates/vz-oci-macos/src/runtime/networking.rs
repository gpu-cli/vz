use super::oci_lifecycle::OciLifecycleOps;
use super::*;

pub(super) struct PortForwarding {
    shutdown_tx: watch::Sender<bool>,
    listener_tasks: Vec<tokio::task::JoinHandle<Result<(), String>>>,
}

impl PortForwarding {
    pub(super) async fn shutdown(&mut self) -> Result<(), OciError> {
        let _ = self.shutdown_tx.send(true);
        let deadline = tokio::time::Instant::now() + port_forward_shutdown_timeout();
        let mut failures = Vec::new();
        for mut task in std::mem::take(&mut self.listener_tasks) {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match tokio::time::timeout(remaining, &mut task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => failures.push(error),
                Ok(Err(error)) => failures.push(error.to_string()),
                Err(_) => {
                    task.abort();
                    if tokio::time::timeout(port_forward_abort_timeout(), &mut task)
                        .await
                        .is_err()
                    {
                        failures.push("aborted listener task did not join promptly".to_string());
                    }
                    failures.push(format!(
                        "listener task did not stop within {:.3}s",
                        port_forward_shutdown_timeout().as_secs_f64()
                    ));
                }
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(OciError::InvalidConfig(format!(
                "VZ_STACK_TEARDOWN_VIOLATION:PORT_FORWARD_SHUTDOWN_FAILED port forwarding shutdown failed: {}",
                failures.join("; ")
            )))
        }
    }
}

#[cfg(not(test))]
fn port_forward_shutdown_timeout() -> Duration {
    Duration::from_secs(5)
}

fn port_forward_abort_timeout() -> Duration {
    Duration::from_secs(1)
}

#[cfg(test)]
fn port_forward_shutdown_timeout() -> Duration {
    Duration::from_millis(100)
}

pub(super) async fn start_port_forwarding(
    vm: Arc<Vm>,
    ports: &[PortMapping],
) -> Result<Option<PortForwarding>, OciError> {
    tracing::info!(
        target: "vz_post_stop",
        port_count = ports.len(),
        sample_ports = ?ports.iter().take(4).map(|p| (p.host, p.container)).collect::<Vec<_>>(),
        "[L5/networking] start_port_forwarding entry"
    );
    if ports.is_empty() {
        tracing::info!(
            target: "vz_post_stop",
            "[L5/networking] ports empty — returning Ok(None) (no listeners spawned) (BUG SUSPECT (a))"
        );
        return Ok(None);
    }

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let mut listener_tasks = Vec::with_capacity(ports.len());

    for mapping in ports {
        if mapping.protocol != PortProtocol::Tcp {
            let primary = format!(
                "unsupported port forward protocol: {}",
                mapping.protocol.as_str()
            );
            return Err(
                shutdown_started_listeners(shutdown_tx, &mut listener_tasks, primary).await,
            );
        }

        let listener = match TcpListener::bind(("127.0.0.1", mapping.host)).await {
            Ok(listener) => {
                tracing::info!(
                    target: "vz_post_stop",
                    host_port = mapping.host,
                    container_port = mapping.container,
                    local_addr = ?listener.local_addr().ok(),
                    "[L5/networking] TcpListener::bind succeeded"
                );
                listener
            }
            Err(error) => {
                tracing::error!(
                    target: "vz_post_stop",
                    host_port = mapping.host,
                    container_port = mapping.container,
                    error = %error,
                    "[L5/networking] TcpListener::bind FAILED (BUG SUSPECT (b))"
                );
                let primary = format!(
                    "failed to bind host port {} for forwarding to {}: {error}",
                    mapping.host, mapping.container
                );
                return Err(
                    shutdown_started_listeners(shutdown_tx, &mut listener_tasks, primary).await,
                );
            }
        };

        let mut listener_shutdown_rx = shutdown_rx.clone();
        let listener_vm = Arc::clone(&vm);
        let listener_mapping = mapping.clone();

        listener_tasks.push(tokio::spawn(async move {
            let mut connection_tasks = JoinSet::new();
            let mut failures = Vec::new();

            loop {
                tokio::select! {
                    changed = listener_shutdown_rx.changed() => {
                        if changed.is_err() || *listener_shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accept_result = listener.accept() => {
                        match accept_result {
                            Ok((host_stream, _peer)) => {
                                let connection_vm = Arc::clone(&listener_vm);
                                let connection_mapping = listener_mapping.clone();
                                connection_tasks.spawn(async move {
                                    relay_port_forward_connection(
                                        connection_vm,
                                        host_stream,
                                        connection_mapping,
                                    )
                                    .await
                                });
                            }
                            Err(error) => {
                                warn!(
                                    host_port = listener_mapping.host,
                                    container_port = listener_mapping.container,
                                    error = %error,
                                    "port forward listener accept failed"
                                );
                                failures.push(format!(
                                    "host port {} forwarding to {} accept failed: {error}",
                                    listener_mapping.host, listener_mapping.container
                                ));
                                break;
                            }
                        }
                    }
                    join_result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                        if let Some(join_result) = join_result {
                            record_connection_join_result(
                                join_result,
                                &listener_mapping,
                                &mut failures,
                            );
                        }
                    }
                }
            }

            // Active relays can otherwise keep shutdown waiting forever on an
            // idle TCP connection. Abort them, then inspect every completion;
            // cancellation caused here is expected, while panics remain errors.
            connection_tasks.abort_all();
            while let Some(join_result) = connection_tasks.join_next().await {
                record_connection_join_result(join_result, &listener_mapping, &mut failures);
            }

            if failures.is_empty() {
                Ok(())
            } else {
                Err(failures.join("; "))
            }
        }));
    }

    Ok(Some(PortForwarding {
        shutdown_tx,
        listener_tasks,
    }))
}

pub(super) async fn shutdown_port_forwarding_registry_entry(
    registry: &Mutex<HashMap<String, PortForwarding>>,
    key: &str,
) -> Result<bool, OciError> {
    let mut registry = registry.lock().await;
    let Some(forwarding) = registry.get_mut(key) else {
        return Ok(false);
    };
    forwarding.shutdown().await?;
    registry.remove(key);
    Ok(true)
}

#[cfg(test)]
pub(super) fn test_port_forwarding(
    listener_task: tokio::task::JoinHandle<Result<(), String>>,
) -> PortForwarding {
    let (shutdown_tx, _shutdown_rx) = watch::channel(false);
    PortForwarding {
        shutdown_tx,
        listener_tasks: vec![listener_task],
    }
}

async fn shutdown_started_listeners(
    shutdown_tx: watch::Sender<bool>,
    listener_tasks: &mut Vec<tokio::task::JoinHandle<Result<(), String>>>,
    primary: String,
) -> OciError {
    let mut forwarding = PortForwarding {
        shutdown_tx,
        listener_tasks: std::mem::take(listener_tasks),
    };
    match forwarding.shutdown().await {
        Ok(()) => OciError::InvalidConfig(primary),
        Err(cleanup_error) => OciError::InvalidConfig(format!(
            "{primary}; additionally failed to shut down already-started forwarding: {cleanup_error}"
        )),
    }
}

fn record_connection_join_result(
    join_result: Result<Result<(), LinuxError>, tokio::task::JoinError>,
    mapping: &PortMapping,
    failures: &mut Vec<String>,
) {
    match join_result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            debug!(
                host_port = mapping.host,
                container_port = mapping.container,
                error = %error,
                "port forward connection failed"
            );
        }
        Err(error) if error.is_cancelled() => {}
        Err(error) => {
            warn!(
                host_port = mapping.host,
                container_port = mapping.container,
                error = %error,
                "port forward relay task join failed"
            );
            failures.push(format!(
                "host port {} forwarding to {} relay task failed: {error}",
                mapping.host, mapping.container
            ));
        }
    }
}

async fn relay_port_forward_connection(
    vm: Arc<Vm>,
    mut host_stream: TcpStream,
    mapping: PortMapping,
) -> Result<(), LinuxError> {
    let mut client =
        vz_linux::grpc_client::GrpcAgentClient::connect(vm, vz::protocol::AGENT_PORT).await?;
    let mut guest_stream = client
        .port_forward(
            mapping.container,
            mapping.protocol.as_str(),
            mapping.target_host.as_deref(),
        )
        .await?;

    tokio::io::copy_bidirectional(&mut host_stream, &mut guest_stream)
        .await
        .map_err(|error| LinuxError::Protocol(format!("port forward relay failed: {error}")))?;

    Ok(())
}

pub(super) async fn ensure_interactive_exec_pty_prerequisites(
    vm: &LinuxVm,
    exec_timeout: Duration,
) {
    let prep_timeout = exec_timeout.min(INTERACTIVE_EXEC_PTY_PREP_TIMEOUT);
    if prep_timeout.is_zero() {
        return;
    }

    // Best-effort guest PTY repair for older agent artifacts that may start
    // without devpts mounted or /dev/ptmx linked.
    let prep_script = "set -eu; \
        /bin/busybox mkdir -p /dev/pts; \
        if ! /bin/busybox awk '$2==\"/dev/pts\" && $3==\"devpts\" {found=1} END {exit found?0:1}' /proc/mounts; then \
          /bin/busybox mount -t devpts devpts /dev/pts -o ptmxmode=0666,mode=0620 || true; \
        fi; \
        if [ ! -e /dev/ptmx ]; then \
          /bin/busybox ln -sf pts/ptmx /dev/ptmx || true; \
        fi";

    match vm
        .exec_collect(
            "/bin/busybox".to_string(),
            vec!["sh".to_string(), "-lc".to_string(), prep_script.to_string()],
            prep_timeout,
        )
        .await
    {
        Ok(output) if output.exit_code == 0 => {}
        Ok(output) => {
            warn!(
                exit_code = output.exit_code,
                "interactive exec PTY prerequisite command returned non-zero status"
            );
        }
        Err(error) => {
            warn!(
                error = %error,
                "interactive exec PTY prerequisite check failed"
            );
        }
    }
}

/// Stop a container through OCI runtime lifecycle: state → kill → poll → escalate.
///
/// Graceful (force=false): sends the configured stop signal (default SIGTERM),
/// polls state until stopped or grace period expires, then escalates to SIGKILL.
/// Forced (force=true): sends SIGKILL immediately.
///
/// `signal` overrides the default stop signal. When `None`, SIGTERM is used.
///
/// A container that is already authoritatively stopped succeeds without a
/// signal and returns `0`. Otherwise this returns the conventional exit code:
/// 143 for SIGTERM or 137 for SIGKILL.
pub(super) async fn stop_via_oci_runtime(
    vm: &impl OciLifecycleOps,
    container_id: &str,
    force: bool,
    grace_period: Duration,
    signal: Option<&str>,
) -> Result<i32, OciError> {
    let id = container_id.to_string();
    let stop_signal = signal.unwrap_or("SIGTERM");

    // The durable host record can still say Running after the guest init exits
    // naturally. Consult the authoritative OCI state before signalling so an
    // already-stopped generation remains an idempotent stop. State transport
    // failures are not evidence that the process stopped.
    if is_container_stopped(vm, &id).await? {
        return Ok(0);
    }

    if force {
        signal_or_accept_stopped(vm, &id, "SIGKILL").await?;
        return Ok(137); // 128 + 9
    }

    // Graceful: send configured stop signal first.
    if signal_or_accept_stopped(vm, &id, stop_signal).await? {
        return Ok(0);
    }

    // Poll state until stopped or grace period expires.
    let deadline = tokio::time::Instant::now() + grace_period;
    loop {
        if is_container_stopped(vm, &id).await? {
            return Ok(143); // graceful stop succeeded (conventional SIGTERM exit code)
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(STOP_POLL_INTERVAL).await;
    }

    // Escalate to SIGKILL after grace period.
    signal_or_accept_stopped(vm, &id, "SIGKILL").await?;
    Ok(137) // 128 + 9
}

/// Stop a running generation or resume the guest-cleanup phase of a stop that
/// already published `Stopped` after a fallible OCI delete. A retry must not
/// signal the process again, but it must continue to the caller's delete step.
pub(super) async fn stop_or_reuse_exit_code(
    vm: &impl OciLifecycleOps,
    container_id: &str,
    status: &ContainerStatus,
    cleanup_pending: bool,
    force: bool,
    grace_period: Duration,
    signal: Option<&str>,
) -> Result<i32, OciError> {
    if cleanup_pending {
        return Ok(match status {
            ContainerStatus::Stopped { exit_code } => *exit_code,
            _ => 0,
        });
    }
    match status {
        ContainerStatus::Running => {
            stop_via_oci_runtime(vm, container_id, force, grace_period, signal).await
        }
        ContainerStatus::Stopped { exit_code } => Ok(*exit_code),
        _ => Err(OciError::InvalidConfig(format!(
            "container '{container_id}' has retained guest cleanup while not running or stopped"
        ))),
    }
}

/// Send a signal, accepting a failed signal only when a second authoritative
/// state read proves that the same admitted container is now exactly stopped.
///
/// The recheck closes the natural-exit race between the caller's state read and
/// `kill`. `Ok(true)` means that race was observed and no signal was delivered.
async fn signal_or_accept_stopped(
    vm: &impl OciLifecycleOps,
    container_id: &str,
    signal: &str,
) -> Result<bool, OciError> {
    match vm
        .oci_kill(container_id.to_string(), signal.to_string())
        .await
    {
        Ok(()) => Ok(false),
        Err(kill_error) => match is_container_stopped(vm, container_id).await {
            Ok(true) => Ok(true),
            Ok(false) | Err(_) => Err(kill_error),
        },
    }
}

/// Check if the OCI runtime reports the container as stopped.
async fn is_container_stopped(
    vm: &impl OciLifecycleOps,
    container_id: &str,
) -> Result<bool, OciError> {
    vm.oci_state(container_id.to_string())
        .await
        .map(|state| state.status == "stopped")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn port_forwarding_shutdown_joins_clean_listener_task() {
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let listener_task = tokio::spawn(async move {
            shutdown_rx
                .changed()
                .await
                .unwrap_or_else(|error| panic!("test shutdown operation failed: {error:?}"));
            assert!(*shutdown_rx.borrow());
            Ok(())
        });
        let mut forwarding = PortForwarding {
            shutdown_tx,
            listener_tasks: vec![listener_task],
        };

        forwarding
            .shutdown()
            .await
            .unwrap_or_else(|error| panic!("test shutdown operation failed: {error:?}"));
    }

    #[tokio::test]
    async fn port_forwarding_shutdown_propagates_listener_task_panic() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let listener_task = tokio::spawn(async move {
            panic!("deterministic listener failure");
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        });
        let mut forwarding = PortForwarding {
            shutdown_tx,
            listener_tasks: vec![listener_task],
        };

        let error = forwarding.shutdown().await.map_or_else(
            |error| error,
            |value| panic!("expected cleanup failure, got success: {value:?}"),
        );
        assert!(
            error
                .to_string()
                .contains("port forwarding shutdown failed")
        );
        assert!(error.to_string().contains("deterministic listener failure"));

        forwarding
            .shutdown()
            .await
            .unwrap_or_else(|error| panic!("test shutdown operation failed: {error:?}"));
    }

    #[tokio::test]
    async fn port_forwarding_shutdown_bounds_unresponsive_listener_task() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let listener_task = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok(())
        });
        let mut forwarding = PortForwarding {
            shutdown_tx,
            listener_tasks: vec![listener_task],
        };

        let error = forwarding.shutdown().await.map_or_else(
            |error| error,
            |value| panic!("expected cleanup failure, got success: {value:?}"),
        );
        assert!(error.to_string().contains("did not stop within"));
        forwarding
            .shutdown()
            .await
            .unwrap_or_else(|error| panic!("test shutdown operation failed: {error:?}"));
    }

    #[tokio::test]
    async fn connection_task_panic_is_recorded_as_listener_failure() {
        let join_result = tokio::spawn(async move {
            panic!("deterministic connection failure");
            #[allow(unreachable_code)]
            Ok::<(), LinuxError>(())
        })
        .await;
        let mapping = PortMapping {
            host: 12345,
            container: 80,
            protocol: PortProtocol::Tcp,
            target_host: None,
        };
        let mut failures = Vec::new();

        record_connection_join_result(join_result, &mapping, &mut failures);

        assert_eq!(failures.len(), 1);
        assert!(failures[0].contains("deterministic connection failure"));
    }

    #[tokio::test]
    async fn registry_retains_failed_entry_then_retry_removes_it() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let listener_task = tokio::spawn(async move {
            panic!("deterministic registry failure");
            #[allow(unreachable_code)]
            Ok::<(), String>(())
        });
        let registry = Mutex::new(HashMap::from([(
            "stack".to_string(),
            PortForwarding {
                shutdown_tx,
                listener_tasks: vec![listener_task],
            },
        )]));

        shutdown_port_forwarding_registry_entry(&registry, "stack")
            .await
            .map_or_else(
                |error| error,
                |value| panic!("expected cleanup failure, got success: {value:?}"),
            );
        assert!(registry.lock().await.contains_key("stack"));

        assert!(
            shutdown_port_forwarding_registry_entry(&registry, "stack")
                .await
                .unwrap_or_else(|error| panic!("test shutdown operation failed: {error:?}"))
        );
        assert!(!registry.lock().await.contains_key("stack"));
    }

    #[tokio::test]
    async fn registry_timeout_retry_removes_completed_entry() {
        let (shutdown_tx, _shutdown_rx) = watch::channel(false);
        let listener_task = tokio::spawn(async move {
            std::future::pending::<()>().await;
            Ok(())
        });
        let registry = Mutex::new(HashMap::from([(
            "container".to_string(),
            PortForwarding {
                shutdown_tx,
                listener_tasks: vec![listener_task],
            },
        )]));

        shutdown_port_forwarding_registry_entry(&registry, "container")
            .await
            .map_or_else(
                |error| error,
                |value| panic!("expected cleanup failure, got success: {value:?}"),
            );
        assert!(registry.lock().await.contains_key("container"));

        assert!(
            shutdown_port_forwarding_registry_entry(&registry, "container")
                .await
                .unwrap_or_else(|error| panic!("test shutdown operation failed: {error:?}"))
        );
        assert!(!registry.lock().await.contains_key("container"));
    }
}
