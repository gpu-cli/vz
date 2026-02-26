use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;
use vz::Vm;
use vz::protocol::{ExecEvent, ExecOutput, NetworkServiceConfig, OciContainerState, OciExecResult};

use crate::grpc_client::{GrpcAgentClient, GrpcPortForwardStream};
use crate::{ExecOptions, LinuxError, LinuxVmConfig, OciExecOptions};

const AGENT_POLL_INITIAL: Duration = Duration::from_millis(50);
const AGENT_POLL_MAX: Duration = Duration::from_secs(1);
const AGENT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);

/// Linux VM wrapper with guest-agent readiness helpers.
///
/// Internally holds a [`GrpcAgentClient`] for all guest communication.
pub struct LinuxVm {
    vm: Arc<Vm>,
    config: LinuxVmConfig,
    grpc: Mutex<Option<GrpcAgentClient>>,
}

impl std::fmt::Debug for LinuxVm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinuxVm")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl LinuxVm {
    /// Create a Linux VM from config.
    pub async fn create(config: LinuxVmConfig) -> Result<Self, LinuxError> {
        config.validate()?;
        let vm_config = config.to_vm_config()?;
        let vm = Arc::new(Vm::create(vm_config).await?);
        Ok(Self {
            vm,
            config,
            grpc: Mutex::new(None),
        })
    }

    /// Start the VM (cold boot).
    pub async fn start(&self) -> Result<(), LinuxError> {
        self.vm.start().await?;
        Ok(())
    }

    /// Stop the VM forcefully.
    pub async fn stop(&self) -> Result<(), LinuxError> {
        self.vm.stop().await?;
        Ok(())
    }

    /// Save an in-place VM state snapshot and resume guest execution.
    ///
    /// This pauses the VM, writes state to `path`, resumes the VM, and clears
    /// any cached gRPC client so subsequent operations reconnect cleanly.
    pub async fn save_state_snapshot(&self, path: &Path) -> Result<(), LinuxError> {
        self.vm.pause().await?;
        self.vm.save_state(path).await?;
        self.vm.resume().await?;
        let mut grpc = self.grpc.lock().await;
        *grpc = None;
        Ok(())
    }

    /// Restore VM state from `path`, resume guest execution, and wait for agent.
    ///
    /// This force-stops the current VM execution, restores state, resumes, and
    /// reestablishes guest-agent readiness before returning.
    pub async fn restore_state_snapshot(
        &self,
        path: &Path,
        agent_ready_timeout: Duration,
    ) -> Result<(), LinuxError> {
        self.vm.stop().await?;
        self.vm.restore_state(path).await?;
        self.vm.resume().await?;
        let mut grpc = self.grpc.lock().await;
        *grpc = None;
        drop(grpc);
        self.wait_for_agent(agent_ready_timeout).await
    }

    /// Start the VM and wait until guest agent is reachable.
    pub async fn start_and_wait_for_agent(
        &self,
        timeout: Duration,
    ) -> Result<Duration, LinuxError> {
        self.start_and_wait_for_agent_with_progress(timeout, |_attempts, _last_error| {})
            .await
    }

    /// Start the VM and wait for agent readiness, reporting retry progress.
    pub async fn start_and_wait_for_agent_with_progress<F>(
        &self,
        timeout: Duration,
        on_retry: F,
    ) -> Result<Duration, LinuxError>
    where
        F: FnMut(u32, &str),
    {
        let started = Instant::now();
        self.start().await?;
        self.wait_for_agent_with_progress(timeout, on_retry).await?;
        Ok(started.elapsed())
    }

    /// Wait for guest agent readiness via gRPC ping.
    pub async fn wait_for_agent(&self, timeout: Duration) -> Result<(), LinuxError> {
        self.wait_for_agent_with_progress(timeout, |_attempts, _last_error| {})
            .await
    }

    /// Wait for guest agent readiness and report retry progress.
    ///
    /// On success, stores the [`GrpcAgentClient`] for subsequent operations.
    pub async fn wait_for_agent_with_progress<F>(
        &self,
        timeout: Duration,
        mut on_retry: F,
    ) -> Result<(), LinuxError>
    where
        F: FnMut(u32, &str),
    {
        let started = Instant::now();
        let mut attempts = 0u32;
        let mut last_error = "no attempts made".to_string();

        while started.elapsed() < timeout {
            attempts = attempts.saturating_add(1);
            let elapsed = started.elapsed();
            let remaining = timeout.saturating_sub(elapsed);
            let attempt_timeout = std::cmp::min(AGENT_ATTEMPT_TIMEOUT, remaining);

            let connect_result = tokio::time::timeout(attempt_timeout, async {
                let mut client =
                    GrpcAgentClient::connect(Arc::clone(&self.vm), vz::protocol::AGENT_PORT)
                        .await?;
                client.ping().await?;

                // Verify guest OS via system_info.
                let info = client.system_info().await?;
                if !info.os_version.to_lowercase().contains("linux") {
                    return Err(LinuxError::UnexpectedGuestOs(info.os_version));
                }

                Ok(client)
            })
            .await;

            match connect_result {
                Ok(Ok(client)) => {
                    let mut grpc = self.grpc.lock().await;
                    *grpc = Some(client);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    last_error = e.to_string();
                    on_retry(attempts, &last_error);
                }
                Err(_) => {
                    last_error = format!(
                        "agent connect timed out after {:.3}s",
                        attempt_timeout.as_secs_f64()
                    );
                    on_retry(attempts, &last_error);
                }
            }

            let elapsed_after_attempt = started.elapsed();
            if elapsed_after_attempt >= timeout {
                break;
            }
            let remaining_after_attempt = timeout.saturating_sub(elapsed_after_attempt);
            // Exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms, capped at 1s.
            let backoff = std::cmp::min(
                AGENT_POLL_MAX,
                AGENT_POLL_INITIAL * 2u32.saturating_pow(attempts.saturating_sub(1)),
            );
            tokio::time::sleep(std::cmp::min(backoff, remaining_after_attempt)).await;
        }

        Err(LinuxError::AgentUnreachable {
            attempts,
            last_error,
        })
    }

    /// Ensure a gRPC client is connected, reconnecting if needed.
    async fn ensure_grpc(&self) -> Result<(), LinuxError> {
        let mut grpc = self.grpc.lock().await;
        if grpc.is_none() {
            let mut client =
                GrpcAgentClient::connect(Arc::clone(&self.vm), vz::protocol::AGENT_PORT).await?;
            client.ping().await?;
            *grpc = Some(client);
        }
        Ok(())
    }

    /// Run a command on the guest and capture buffered output.
    pub async fn exec_capture(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<ExecOutput, LinuxError> {
        self.exec_capture_with_options(command, args, timeout, ExecOptions::default())
            .await
    }

    /// Run a command on the guest with explicit execution options.
    pub async fn exec_capture_with_options(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
        options: ExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        tokio::time::timeout(timeout, client.exec(command, args, options))
            .await
            .map_err(|_| {
                LinuxError::Protocol(format!(
                    "exec timed out after {:.3}s",
                    timeout.as_secs_f64()
                ))
            })?
    }

    /// Run a command on the guest and stream output events while buffering final output.
    pub async fn exec_capture_streaming<F>(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
        on_event: F,
    ) -> Result<ExecOutput, LinuxError>
    where
        F: FnMut(&ExecEvent),
    {
        self.exec_capture_with_options_streaming(
            command,
            args,
            timeout,
            ExecOptions::default(),
            on_event,
        )
        .await
    }

    /// Run a command with explicit execution options and stream output events.
    pub async fn exec_capture_with_options_streaming<F>(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
        options: ExecOptions,
        mut on_event: F,
    ) -> Result<ExecOutput, LinuxError>
    where
        F: FnMut(&ExecEvent),
    {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;

        tokio::time::timeout(timeout, async move {
            let mut stream = client.exec_stream(command, args, options).await?;
            let mut stdout_bytes = Vec::new();
            let mut stderr_bytes = Vec::new();
            let mut saw_exit = false;
            let mut exit_code = -1;

            while let Some(event) = stream.next().await {
                on_event(&event);
                match event {
                    ExecEvent::Stdout(data) => stdout_bytes.extend_from_slice(&data),
                    ExecEvent::Stderr(data) => stderr_bytes.extend_from_slice(&data),
                    ExecEvent::Exit(code) => {
                        saw_exit = true;
                        exit_code = code;
                        break;
                    }
                }
            }

            if !saw_exit {
                return Err(LinuxError::Protocol(
                    "exec stream ended without exit code".to_string(),
                ));
            }

            Ok(ExecOutput {
                exit_code,
                stdout: String::from_utf8_lossy(&stdout_bytes).into_owned(),
                stderr: String::from_utf8_lossy(&stderr_bytes).into_owned(),
            })
        })
        .await
        .map_err(|_| {
            LinuxError::Protocol(format!(
                "exec timed out after {:.3}s",
                timeout.as_secs_f64()
            ))
        })?
    }

    /// Open a dedicated port-forward stream to a guest-local target port.
    pub async fn open_port_forward_stream(
        &self,
        target_port: u16,
        protocol_name: &str,
        target_host: Option<&str>,
    ) -> Result<GrpcPortForwardStream, LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client
            .port_forward(target_port, protocol_name, target_host)
            .await
    }

    /// Create a container in the guest OCI runtime.
    pub async fn oci_create(&self, id: String, bundle_path: String) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.oci_create(id, bundle_path).await
    }

    /// Start a created container in the guest OCI runtime.
    pub async fn oci_start(&self, id: String) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.oci_start(id).await
    }

    /// Query container state from the guest OCI runtime.
    pub async fn oci_state(&self, id: String) -> Result<OciContainerState, LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.oci_state(id).await
    }

    /// Execute a command in a running guest OCI container.
    pub async fn oci_exec(
        &self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> Result<OciExecResult, LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.oci_exec(id, command, args, options).await
    }

    /// Signal a running container in the guest OCI runtime.
    pub async fn oci_kill(&self, id: String, signal: String) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.oci_kill(id, signal).await
    }

    /// Delete container state from the guest OCI runtime.
    pub async fn oci_delete(&self, id: String, force: bool) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.oci_delete(id, force).await
    }

    /// Set up per-service network isolation inside the VM.
    pub async fn network_setup(
        &self,
        stack_id: String,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        let proto_services = services
            .into_iter()
            .map(|s| vz_agent_proto::NetworkServiceConfig {
                name: s.name,
                addr: s.addr,
                network_name: s.network_name,
            })
            .collect();
        client.network_setup(stack_id, proto_services).await
    }

    /// Tear down the network resources for a stack.
    pub async fn network_teardown(
        &self,
        stack_id: String,
        service_names: Vec<String>,
    ) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.network_teardown(stack_id, service_names).await
    }

    /// Execute a command interactively with PTY allocation.
    ///
    /// Returns a streaming handle and exec_id for stdin/resize operations.
    pub async fn exec_interactive(
        &self,
        command: &str,
        args: &[&str],
        rows: u32,
        cols: u32,
    ) -> Result<(crate::grpc_client::GrpcExecStream, u64), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        client
            .exec_stream_interactive(
                command.to_string(),
                args_owned,
                ExecOptions::default(),
                rows,
                cols,
            )
            .await
    }

    /// Write data to a running exec's stdin (or PTY master).
    pub async fn stdin_write(&self, exec_id: u64, data: &[u8]) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.stdin_write(exec_id, data).await
    }

    /// Close a running exec's stdin.
    pub async fn stdin_close(&self, exec_id: u64) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.stdin_close(exec_id).await
    }

    /// Send a signal to a running exec process.
    pub async fn signal(&self, exec_id: u64, signal: i32) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.signal(exec_id, signal).await
    }

    /// Resize the PTY window for a running interactive exec session.
    pub async fn resize_exec_pty(
        &self,
        exec_id: u64,
        rows: u32,
        cols: u32,
    ) -> Result<(), LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.resize_exec_pty(exec_id, rows, cols).await
    }

    /// Borrow the underlying base VM.
    pub fn inner(&self) -> &Vm {
        self.vm.as_ref()
    }

    /// Clone the underlying base VM handle.
    pub fn inner_shared(&self) -> Arc<Vm> {
        Arc::clone(&self.vm)
    }

    /// Borrow the Linux VM config.
    pub fn config(&self) -> &LinuxVmConfig {
        &self.config
    }
}
