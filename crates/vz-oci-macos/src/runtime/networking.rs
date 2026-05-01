use super::oci_lifecycle::OciLifecycleOps;
use super::*;

pub(super) struct PortForwarding {
    shutdown_tx: watch::Sender<bool>,
    listener_tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl PortForwarding {
    pub(super) async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        for task in self.listener_tasks {
            let _ = task.await;
        }
    }
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
            let _ = shutdown_tx.send(true);
            for task in listener_tasks.drain(..) {
                let _ = task.await;
            }
            return Err(OciError::InvalidConfig(format!(
                "unsupported port forward protocol: {}",
                mapping.protocol.as_str()
            )));
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
                let _ = shutdown_tx.send(true);
                for task in listener_tasks.drain(..) {
                    let _ = task.await;
                }

                return Err(OciError::InvalidConfig(format!(
                    "failed to bind host port {} for forwarding to {}: {error}",
                    mapping.host, mapping.container
                )));
            }
        };

        let mut listener_shutdown_rx = shutdown_rx.clone();
        let listener_vm = Arc::clone(&vm);
        let listener_mapping = mapping.clone();

        listener_tasks.push(tokio::spawn(async move {
            let mut connection_tasks = JoinSet::new();

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
                                    let host_port = connection_mapping.host;
                                    let container_port = connection_mapping.container;
                                    if let Err(error) = relay_port_forward_connection(
                                        connection_vm,
                                        host_stream,
                                        connection_mapping,
                                    )
                                    .await
                                    {
                                        debug!(
                                            host_port,
                                            container_port,
                                            error = %error,
                                            "port forward connection failed"
                                        );
                                    }
                                });
                            }
                            Err(error) => {
                                warn!(
                                    host_port = listener_mapping.host,
                                    container_port = listener_mapping.container,
                                    error = %error,
                                    "port forward listener accept failed"
                                );
                                break;
                            }
                        }
                    }
                    join_result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                        if let Some(Err(error)) = join_result {
                            warn!(
                                host_port = listener_mapping.host,
                                container_port = listener_mapping.container,
                                error = %error,
                                "port forward relay task join failed"
                            );
                        }
                    }
                }
            }

            while let Some(join_result) = connection_tasks.join_next().await {
                if let Err(error) = join_result {
                    warn!(
                        host_port = listener_mapping.host,
                        container_port = listener_mapping.container,
                        error = %error,
                        "port forward relay task join failed"
                    );
                }
            }
        }));
    }

    Ok(Some(PortForwarding {
        shutdown_tx,
        listener_tasks,
    }))
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

/// Stop a container through OCI runtime lifecycle: kill → poll state → escalate.
///
/// Graceful (force=false): sends the configured stop signal (default SIGTERM),
/// polls state until stopped or grace period expires, then escalates to SIGKILL.
/// Forced (force=true): sends SIGKILL immediately.
///
/// `signal` overrides the default stop signal. When `None`, SIGTERM is used.
///
/// Returns the conventional exit code: 128+signal (143 for SIGTERM, 137 for SIGKILL).
pub(super) async fn stop_via_oci_runtime(
    vm: &impl OciLifecycleOps,
    container_id: &str,
    force: bool,
    grace_period: Duration,
    signal: Option<&str>,
) -> Result<i32, OciError> {
    let id = container_id.to_string();
    let stop_signal = signal.unwrap_or("SIGTERM");

    if force {
        let _ = vm.oci_kill(id.clone(), "SIGKILL".to_string()).await;
        return Ok(137); // 128 + 9
    }

    // Graceful: send configured stop signal first.
    vm.oci_kill(id.clone(), stop_signal.to_string()).await?;

    // Poll state until stopped or grace period expires.
    let deadline = tokio::time::Instant::now() + grace_period;
    loop {
        if is_container_stopped(vm, &id).await {
            return Ok(143); // graceful stop succeeded (conventional SIGTERM exit code)
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(STOP_POLL_INTERVAL).await;
    }

    // Escalate to SIGKILL after grace period.
    let _ = vm.oci_kill(id.clone(), "SIGKILL".to_string()).await;
    Ok(137) // 128 + 9
}

/// Check if the OCI runtime reports the container as stopped.
async fn is_container_stopped(vm: &impl OciLifecycleOps, container_id: &str) -> bool {
    match vm.oci_state(container_id.to_string()).await {
        Ok(state) => state.status == "stopped",
        Err(_) => true, // If state query fails, assume stopped.
    }
}
