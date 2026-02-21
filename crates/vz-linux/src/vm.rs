use std::sync::Arc;
use std::time::Duration;

use tokio::time::Instant;
use vz::Vm;
use vz::protocol::{
    ExecOutput, HandshakeAck, NetworkServiceConfig, OciContainerState, OciExecResult,
};

use crate::agent::{
    exec_capture, exec_capture_with_options, handshake_and_ping, network_setup, network_teardown,
    oci_create, oci_delete, oci_exec, oci_kill, oci_start, oci_state, open_port_forward_stream,
};
use crate::{ExecOptions, LinuxError, LinuxVmConfig, OciExecOptions};

const AGENT_POLL_INTERVAL: Duration = Duration::from_millis(50);
const AGENT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(1);

/// Linux VM wrapper with guest-agent readiness helpers.
#[derive(Debug)]
pub struct LinuxVm {
    vm: Arc<Vm>,
    config: LinuxVmConfig,
}

impl LinuxVm {
    /// Create a Linux VM from config.
    pub async fn create(config: LinuxVmConfig) -> Result<Self, LinuxError> {
        config.validate()?;
        let vm_config = config.to_vm_config()?;
        let vm = Arc::new(Vm::create(vm_config).await?);
        Ok(Self { vm, config })
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

    /// Wait for guest agent readiness (handshake + ping).
    pub async fn wait_for_agent(&self, timeout: Duration) -> Result<HandshakeAck, LinuxError> {
        self.wait_for_agent_with_progress(timeout, |_attempts, _last_error| {})
            .await
    }

    /// Wait for guest agent readiness and report retry progress.
    pub async fn wait_for_agent_with_progress<F>(
        &self,
        timeout: Duration,
        mut on_retry: F,
    ) -> Result<HandshakeAck, LinuxError>
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

            match tokio::time::timeout(attempt_timeout, handshake_and_ping(&self.vm)).await {
                Ok(Ok(ack)) => {
                    if ack.os != "linux" {
                        return Err(LinuxError::UnexpectedGuestOs(ack.os));
                    }
                    return Ok(ack);
                }
                Ok(Err(e)) => {
                    last_error = e.to_string();
                    on_retry(attempts, &last_error);
                }
                Err(_) => {
                    last_error = format!(
                        "handshake attempt timed out after {:.3}s",
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
            tokio::time::sleep(std::cmp::min(AGENT_POLL_INTERVAL, remaining_after_attempt)).await;
        }

        Err(LinuxError::AgentUnreachable {
            attempts,
            last_error,
        })
    }

    /// Run a command on the guest and capture buffered output.
    pub async fn exec_capture(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<ExecOutput, LinuxError> {
        exec_capture(self.vm.as_ref(), command, args, timeout).await
    }

    /// Run a command on the guest with explicit execution options.
    pub async fn exec_capture_with_options(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
        options: ExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        exec_capture_with_options(self.vm.as_ref(), command, args, timeout, options).await
    }

    /// Open a dedicated port-forward stream to a guest-local target port.
    pub async fn open_port_forward_stream(
        &self,
        target_port: u16,
        protocol_name: &str,
        target_host: Option<&str>,
    ) -> Result<vz::VsockStream, LinuxError> {
        open_port_forward_stream(self.vm.as_ref(), target_port, protocol_name, target_host).await
    }

    /// Create a container in the guest OCI runtime.
    pub async fn oci_create(&self, id: String, bundle_path: String) -> Result<(), LinuxError> {
        oci_create(self.vm.as_ref(), id, bundle_path).await
    }

    /// Start a created container in the guest OCI runtime.
    pub async fn oci_start(&self, id: String) -> Result<(), LinuxError> {
        oci_start(self.vm.as_ref(), id).await
    }

    /// Query container state from the guest OCI runtime.
    pub async fn oci_state(&self, id: String) -> Result<OciContainerState, LinuxError> {
        oci_state(self.vm.as_ref(), id).await
    }

    /// Execute a command in a running guest OCI container.
    pub async fn oci_exec(
        &self,
        id: String,
        command: String,
        args: Vec<String>,
        options: OciExecOptions,
    ) -> Result<OciExecResult, LinuxError> {
        oci_exec(self.vm.as_ref(), id, command, args, options).await
    }

    /// Signal a running container in the guest OCI runtime.
    pub async fn oci_kill(&self, id: String, signal: String) -> Result<(), LinuxError> {
        oci_kill(self.vm.as_ref(), id, signal).await
    }

    /// Delete container state from the guest OCI runtime.
    pub async fn oci_delete(&self, id: String, force: bool) -> Result<(), LinuxError> {
        oci_delete(self.vm.as_ref(), id, force).await
    }

    /// Set up per-service network isolation inside the VM.
    pub async fn network_setup(
        &self,
        stack_id: String,
        services: Vec<NetworkServiceConfig>,
    ) -> Result<(), LinuxError> {
        network_setup(self.vm.as_ref(), stack_id, services).await
    }

    /// Tear down the network resources for a stack.
    pub async fn network_teardown(
        &self,
        stack_id: String,
        service_names: Vec<String>,
    ) -> Result<(), LinuxError> {
        network_teardown(self.vm.as_ref(), stack_id, service_names).await
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
