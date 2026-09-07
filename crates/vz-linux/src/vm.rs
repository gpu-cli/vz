use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Mutex;
use tokio::time::Instant;
use tracing::{debug, warn};
use vz::protocol::{ExecEvent, ExecOutput, NetworkServiceConfig, OciContainerState};
use vz::{Vm, VmState};
use vz_agent_proto::SystemInfoResponse;
use vz_agent_proto::{DockerEnsureEvent, docker_ensure_event};

use crate::grpc_client::{
    ContainerExecDispatchGate, ContainerExecStartError, GrpcAgentClient, GrpcExecStream,
    GrpcPortForwardStream,
};
use crate::{ExecOptions, LinuxError, LinuxVmConfig, OciExecOptions};

const AGENT_POLL_INITIAL: Duration = Duration::from_millis(50);
const AGENT_POLL_MAX: Duration = Duration::from_secs(1);
const AGENT_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);
const CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);
const CONTAINER_EXEC_CLEANUP_RETRY_DELAY: Duration = Duration::from_secs(1);
const CONTAINER_EXEC_REQUEST_ALLOCATION_TIMEOUT: Duration = Duration::from_secs(10);

type OwnedContainerExecStart = Result<
    (
        GrpcAgentClient,
        GrpcExecStream,
        u64,
        vz_agent_proto::ContainerGeneration,
    ),
    ContainerExecStartError,
>;

struct StartingOwnedContainerExec {
    task: Option<tokio::task::JoinHandle<OwnedContainerExecStart>>,
    vm: Arc<Vm>,
    request_id: String,
    dispatch_gate: ContainerExecDispatchGate,
    armed: bool,
}

impl StartingOwnedContainerExec {
    fn new(
        task: tokio::task::JoinHandle<OwnedContainerExecStart>,
        vm: Arc<Vm>,
        request_id: String,
        dispatch_gate: ContainerExecDispatchGate,
    ) -> Self {
        Self {
            task: Some(task),
            vm,
            request_id,
            dispatch_gate,
            armed: true,
        }
    }

    fn task_mut(&mut self) -> &mut tokio::task::JoinHandle<OwnedContainerExecStart> {
        match self.task.as_mut() {
            Some(task) => task,
            None => unreachable!("armed container exec startup must retain its task"),
        }
    }

    fn promote(
        &mut self,
        client: GrpcAgentClient,
        stream: GrpcExecStream,
        exec_id: u64,
    ) -> ReadyOwnedContainerExec {
        self.armed = false;
        self.task.take();
        ReadyOwnedContainerExec::new(Arc::clone(&self.vm), client, stream, exec_id)
    }

    fn finish_without_ready(&mut self) {
        self.armed = false;
        self.task.take();
    }

    async fn cancel_after_start(&mut self) -> Result<(), LinuxError> {
        if self.dispatch_gate.cancel_before_dispatch() {
            if let Some(task) = self.task.take() {
                task.abort();
                let _ = task.await;
            }
            self.finish_without_ready();
            return Ok(());
        }
        match self.task_mut().await {
            Ok(Ok((client, stream, exec_id, _generation))) => {
                let mut ready = self.promote(client, stream, exec_id);
                ready.cancel_and_reap().await;
                Ok(())
            }
            Ok(Err(ContainerExecStartError::Definite(_startup_error))) => {
                self.finish_without_ready();
                Ok(())
            }
            Ok(Err(ContainerExecStartError::Ambiguous(error))) => {
                let reconciliation = tokio::time::timeout(
                    CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT,
                    self.reconcile_ambiguous_start_failure(
                        "container exec startup cleanup received an ambiguous start failure",
                    ),
                )
                .await;
                match reconciliation {
                    Ok(outcome) => Err(LinuxError::Protocol(format!(
                        "{error}; reconciliation={outcome}"
                    ))),
                    Err(_) => {
                        self.retain_cleanup(
                            "container exec ambiguous startup reconciliation remains pending",
                        );
                        Err(LinuxError::Protocol(format!(
                            "{error}; reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"
                        )))
                    }
                }
            }
            Err(join_error) => {
                let reconciliation = tokio::time::timeout(
                    CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT,
                    self.reconcile_ambiguous_start_failure(
                        "container exec startup cleanup task failed ambiguously",
                    ),
                )
                .await;
                match reconciliation {
                    Ok(outcome) => Err(LinuxError::Protocol(format!(
                        "container exec startup task failed: {join_error}; reconciliation={outcome}"
                    ))),
                    Err(_) => {
                        self.retain_cleanup(
                            "failed container exec startup reconciliation remains pending",
                        );
                        Err(LinuxError::Protocol(format!(
                            "container exec startup task failed: {join_error}; reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"
                        )))
                    }
                }
            }
        }
    }

    async fn reconcile_ambiguous_start_failure(&mut self, context: &'static str) -> &'static str {
        self.task.take();
        let outcome = reconcile_exec_request_until_proven(
            Arc::clone(&self.vm),
            self.request_id.clone(),
            context,
        )
        .await;
        self.armed = false;
        outcome
    }

    fn retain_cleanup(&mut self, context: &'static str) {
        if !self.armed {
            return;
        }
        let dispatch_prevented = self.dispatch_gate.cancel_before_dispatch();
        self.armed = false;
        let Some(task) = self.task.take() else {
            retain_request_reconciliation(Arc::clone(&self.vm), self.request_id.clone(), context);
            return;
        };
        let vm = Arc::clone(&self.vm);
        let request_id = self.request_id.clone();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(
                %context,
                "container exec startup cleanup lost its Tokio runtime; retaining authority"
            );
            // The task owns the in-flight start and retains its output after
            // completion. Keep an explicit VM owner as well: leaking both is
            // the only fail-closed option when no executor can drive cleanup.
            std::mem::forget(task);
            std::mem::forget(vm);
            return;
        };
        runtime.spawn(async move {
            if dispatch_prevented {
                task.abort();
                let _ = task.await;
                return;
            }
            match task.await {
                Ok(Ok((client, stream, exec_id, _generation))) => {
                    let mut ready = ReadyOwnedContainerExec::new(vm, client, stream, exec_id);
                    ready.cancel_and_reap().await;
                }
                Ok(Err(ContainerExecStartError::Definite(_))) => {}
                Ok(Err(ContainerExecStartError::Ambiguous(_))) | Err(_) => {
                    reconcile_exec_request_until_proven(vm, request_id, context).await;
                }
            }
        });
    }
}

async fn reconcile_exec_request_until_proven(
    vm: Arc<Vm>,
    request_id: String,
    context: &str,
) -> &'static str {
    use vz_agent_proto::reconcile_exec_response::Outcome;

    loop {
        if let Some(outcome) = terminal_vm_reconciliation_outcome(vm.state()) {
            debug!(%request_id, %context, outcome, "terminal VM state proves container exec absence");
            return outcome;
        }
        let result = async {
            let mut client = GrpcAgentClient::connect_default(Arc::clone(&vm)).await?;
            client.ping().await?;
            client.reconcile_exec_request(request_id.clone()).await
        }
        .await;
        match result {
            Ok(response)
                if response.outcome == Outcome::FencedNeverStarted as i32
                    || response.outcome == Outcome::TerminalReaped as i32 =>
            {
                debug!(%request_id, %context, outcome = response.outcome, "container exec request reconciled");
                return if response.outcome == Outcome::FencedNeverStarted as i32 {
                    "FENCED_NEVER_STARTED"
                } else {
                    "TERMINAL_REAPED"
                };
            }
            Ok(response) => {
                warn!(%request_id, %context, outcome = response.outcome, "container exec reconciliation remains unproven")
            }
            Err(error) => {
                warn!(%request_id, %context, %error, "container exec reconciliation failed; retrying")
            }
        }
        tokio::time::sleep(CONTAINER_EXEC_CLEANUP_RETRY_DELAY).await;
    }
}

fn terminal_vm_reconciliation_outcome(state: VmState) -> Option<&'static str> {
    match state {
        VmState::Stopped => Some("VM_TERMINAL_STOPPED"),
        VmState::Error(_) => Some("VM_TERMINAL_ERROR"),
        _ => None,
    }
}

fn retain_request_reconciliation(vm: Arc<Vm>, request_id: String, context: &'static str) {
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        warn!(%request_id, %context, "container exec reconciliation lost its Tokio runtime; retaining authority");
        std::mem::forget(vm);
        return;
    };
    runtime.spawn(async move {
        let _ = reconcile_exec_request_until_proven(vm, request_id, context).await;
    });
}

impl Drop for StartingOwnedContainerExec {
    fn drop(&mut self) {
        self.retain_cleanup("container exec owner dropped during startup");
    }
}

struct ReadyOwnedContainerExec {
    vm: Arc<Vm>,
    client: Option<GrpcAgentClient>,
    stream: Option<GrpcExecStream>,
    exec_id: u64,
    armed: bool,
}

impl ReadyOwnedContainerExec {
    fn new(vm: Arc<Vm>, client: GrpcAgentClient, stream: GrpcExecStream, exec_id: u64) -> Self {
        Self {
            vm,
            client: Some(client),
            stream: Some(stream),
            exec_id,
            armed: true,
        }
    }

    fn stream_mut(&mut self) -> &mut GrpcExecStream {
        match self.stream.as_mut() {
            Some(stream) => stream,
            None => unreachable!("armed container exec must retain its stream"),
        }
    }

    fn complete(&mut self) {
        self.armed = false;
        self.stream.take();
        self.client.take();
    }

    async fn cancel_and_reap(&mut self) {
        loop {
            let attempt = {
                let client = match self.client.as_mut() {
                    Some(client) => client,
                    None => unreachable!("armed container exec must retain its control client"),
                };
                tokio::time::timeout(
                    CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT,
                    client.cancel_exec(self.exec_id),
                )
                .await
            };
            match attempt {
                Ok(Ok(_receipt)) => {
                    self.complete();
                    return;
                }
                Ok(Err(error)) => warn!(
                    exec_id = self.exec_id,
                    %error,
                    "container exec cancellation failed; retaining authority and retrying"
                ),
                Err(_) => warn!(
                    exec_id = self.exec_id,
                    timeout_secs = CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT.as_secs_f64(),
                    "container exec cancellation timed out; retaining authority and retrying"
                ),
            }

            match GrpcAgentClient::connect_default(Arc::clone(&self.vm)).await {
                Ok(client) => self.client = Some(client),
                Err(error) => warn!(
                    exec_id = self.exec_id,
                    %error,
                    "container exec cleanup reconnect failed; retaining authority and retrying"
                ),
            }
            tokio::time::sleep(CONTAINER_EXEC_CLEANUP_RETRY_DELAY).await;
        }
    }

    fn retain_cleanup(&mut self, context: &'static str) {
        if !self.armed {
            return;
        }
        self.armed = false;
        let (Some(client), Some(stream)) = (self.client.take(), self.stream.take()) else {
            return;
        };
        let vm = Arc::clone(&self.vm);
        let exec_id = self.exec_id;
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(
                exec_id,
                %context,
                "container exec cleanup lost its Tokio runtime; retaining authority"
            );
            // Preserve the control channel, stream, and VM rather than
            // signalling successful cleanup by dropping their last owners.
            std::mem::forget(client);
            std::mem::forget(stream);
            std::mem::forget(vm);
            return;
        };
        runtime.spawn(async move {
            let mut ready = ReadyOwnedContainerExec::new(vm, client, stream, exec_id);
            ready.cancel_and_reap().await;
        });
    }
}

impl Drop for ReadyOwnedContainerExec {
    fn drop(&mut self) {
        self.retain_cleanup("container exec owner dropped before terminal proof");
    }
}

fn exec_control_debug_enabled() -> bool {
    std::env::var("VZ_LINUX_EXEC_CONTROL_DEBUG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn validate_guest_system_info(info: &SystemInfoResponse) -> Result<(), LinuxError> {
    if !info.os_version.to_lowercase().contains("linux") {
        return Err(LinuxError::UnexpectedGuestOs(info.os_version.clone()));
    }

    let expected_revision = vz_agent_proto::AGENT_PROTOCOL_REVISION;
    if info.agent_protocol_revision != expected_revision {
        return Err(LinuxError::GuestProtocolRevisionMismatch {
            expected: expected_revision,
            found: info.agent_protocol_revision,
        });
    }

    Ok(())
}

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
        self.wait_for_agent(agent_ready_timeout).await?;

        // VM save-state restores the guest's dentry/inode caches while
        // VirtioFS reconnects to a live host-side directory. Evict those
        // restored caches before container mount namespaces are reused so
        // overlay lowerdirs are resolved against the reconnected share.
        let cache_evict = self
            .exec_collect(
                "/bin/busybox".to_string(),
                vec![
                    "sh".to_string(),
                    "-c".to_string(),
                    "echo 2 > /proc/sys/vm/drop_caches".to_string(),
                ],
                Duration::from_secs(5),
            )
            .await?;
        if cache_evict.exit_code != 0 {
            return Err(LinuxError::Protocol(format!(
                "restored guest cache eviction failed with exit {}: {}{}",
                cache_evict.exit_code, cache_evict.stdout, cache_evict.stderr
            )));
        }
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

    /// Restore from a saved snapshot and wait until guest agent is reachable.
    ///
    /// Intended for sandbox re-attach flows where a previous VM session
    /// checkpoint was persisted on detach.
    pub async fn restore_and_wait_for_agent(
        &self,
        path: &Path,
        timeout: Duration,
    ) -> Result<Duration, LinuxError> {
        let started = Instant::now();
        self.vm.restore_state(path).await?;
        self.vm.resume().await?;
        let mut grpc = self.grpc.lock().await;
        *grpc = None;
        drop(grpc);
        self.wait_for_agent(timeout).await?;
        Ok(started.elapsed())
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
                validate_guest_system_info(&info)?;

                Ok::<GrpcAgentClient, LinuxError>(client)
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
            let info = client.system_info().await?;
            validate_guest_system_info(&info)?;
            *grpc = Some(client);
        }
        Ok(())
    }

    /// Run a command on the guest and return a streaming handle.
    ///
    /// The returned [`GrpcExecStream`] yields [`ExecEvent`](vz::protocol::ExecEvent)
    /// values as they arrive from the guest agent. Call `.collect()` on the
    /// stream when you only need the final `ExecOutput`.
    pub async fn exec_stream(
        &self,
        command: String,
        args: Vec<String>,
    ) -> Result<GrpcExecStream, LinuxError> {
        self.exec_stream_with_options(command, args, ExecOptions::default())
            .await
    }

    /// Ensure the downstream Docker facade is ready, forwarding streamed startup events.
    ///
    /// This method is an explicit lazy hook for the host Docker socket proxy;
    /// no native `vz run`, stack, or build path calls it.
    pub async fn ensure_docker_ready_with_progress<F>(
        &self,
        mut on_event: F,
    ) -> Result<String, LinuxError>
    where
        F: FnMut(&DockerEnsureEvent),
    {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        let mut stream = client.ensure_docker_stream().await?;
        drop(grpc);
        while let Some(event) = stream.message().await? {
            on_event(&event);
            if event.stage == docker_ensure_event::Stage::Ready as i32 {
                if event.socket_path.is_empty() {
                    return Err(LinuxError::Protocol(
                        "Docker ready event omitted guest socket path".to_string(),
                    ));
                }
                return Ok(event.socket_path);
            }
        }
        Err(LinuxError::Protocol(
            "Docker startup stream ended before readiness".to_string(),
        ))
    }

    /// Ensure the downstream Docker facade is ready without rendering progress.
    pub async fn ensure_docker_ready(&self) -> Result<String, LinuxError> {
        self.ensure_docker_ready_with_progress(|_| {}).await
    }

    /// Fence this exact guest's Docker owner, reap its daemons, and positively
    /// close its persistent filesystem. Stream loss is never shutdown proof.
    pub async fn shutdown_docker(
        &self,
        request_id: String,
    ) -> Result<vz_agent_proto::DockerShutdownComplete, LinuxError> {
        self.shutdown_docker_with_progress(request_id, |_| {}).await
    }

    /// Shut down Docker while reporting its streamed progress to the caller.
    pub async fn shutdown_docker_with_progress<F>(
        &self,
        request_id: String,
        mut on_event: F,
    ) -> Result<vz_agent_proto::DockerShutdownComplete, LinuxError>
    where
        F: FnMut(&vz_agent_proto::DockerShutdownEvent),
    {
        if request_id.is_empty() || request_id.len() > 256 {
            return Err(LinuxError::Protocol(
                "invalid Docker shutdown request ID".to_string(),
            ));
        }
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        let mut stream = client.shutdown_docker_stream(request_id.clone()).await?;
        drop(grpc);
        let mut complete = None;
        while let Some(event) = stream.message().await? {
            if event.request_id != request_id || complete.is_some() {
                return Err(LinuxError::Protocol(
                    "Docker shutdown stream ownership/order mismatch".to_string(),
                ));
            }
            on_event(&event);
            if let Some(receipt) = event.complete {
                let started = receipt.supervisor_started;
                let closure = receipt.filesystem_synced && receipt.filesystem_unmounted;
                if receipt.request_id != request_id
                    || receipt.data_device != "/dev/vda"
                    || receipt.data_mount != "/var/lib/docker"
                    || receipt.filesystem_synced != receipt.filesystem_unmounted
                    || (started
                        && (!receipt.dockerd_reaped
                            || !receipt.containerd_reaped
                            || !closure
                            || receipt.never_started_unmounted))
                    || (!started
                        && (receipt.dockerd_reaped
                            || receipt.containerd_reaped
                            || !(closure ^ receipt.never_started_unmounted)))
                    || receipt.filesystem_state != "clean"
                    || !shutdown_filesystem_identity_valid(&receipt)
                    || !["has_journal", "extent"].iter().all(|feature| {
                        receipt
                            .filesystem_features
                            .iter()
                            .any(|value| value == feature)
                    })
                {
                    return Err(LinuxError::Protocol(
                        "Docker shutdown omitted exact process/filesystem closure proof"
                            .to_string(),
                    ));
                }
                complete = Some(receipt);
            }
        }
        complete.ok_or_else(|| {
            LinuxError::Protocol("Docker shutdown stream ended without closure proof".to_string())
        })
    }

    /// Run a command on the guest with explicit execution options and return a streaming handle.
    pub async fn exec_stream_with_options(
        &self,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<GrpcExecStream, LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.exec_stream(command, args, options).await
    }

    /// Allocate the request identity that must remain paired with a classified
    /// container exec start until it is ready or reconciled.
    pub async fn prepare_container_exec_request(&self) -> Result<String, LinuxError> {
        tokio::time::timeout(CONTAINER_EXEC_REQUEST_ALLOCATION_TIMEOUT, async {
            self.ensure_grpc().await?;
            let mut grpc = self.grpc.lock().await;
            let client = grpc
                .as_mut()
                .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
            client.prepare_container_exec_request().await
        })
        .await
        .map_err(|_| {
            LinuxError::Protocol(format!(
                "exec request allocation timed out after {:.3}s before dispatch",
                CONTAINER_EXEC_REQUEST_ALLOCATION_TIMEOUT.as_secs_f64()
            ))
        })?
    }

    /// Allocate a guest-incarnation-bound ticket for ordinary Machine execution.
    pub async fn prepare_machine_exec_request(&self) -> Result<String, LinuxError> {
        self.prepare_container_exec_request().await
    }

    /// Start supervised execution in this exact Machine, with no OCI target.
    /// `pty` is `(rows, cols)` or `None` for pipes. Retain this VM and request
    /// ticket until ready/terminal proof or exact-ticket reconciliation; dropped
    /// futures and ambiguous errors do not release lifecycle ownership.
    #[allow(clippy::too_many_arguments)]
    pub async fn exec_machine_stream_ready_classified_for_request(
        &self,
        dispatch_gate: ContainerExecDispatchGate,
        request_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
        pty: Option<(u32, u32)>,
    ) -> Result<(GrpcExecStream, u64), ContainerExecStartError> {
        let mut client = GrpcAgentClient::connect_default(Arc::clone(&self.vm))
            .await
            .map_err(ContainerExecStartError::Definite)?;
        client
            .ping()
            .await
            .map_err(ContainerExecStartError::Definite)?;
        let info = client
            .system_info()
            .await
            .map_err(ContainerExecStartError::Definite)?;
        validate_guest_system_info(&info).map_err(ContainerExecStartError::Definite)?;
        client
            .exec_machine_stream_ready_for_request(
                dispatch_gate,
                request_id,
                command,
                args,
                options,
                pty,
            )
            .await
    }

    /// Start a pipe exec using a request identity retained by the caller.
    /// Ambiguous errors and dropped futures require reconciliation of this
    /// exact ID before releasing VM or lifecycle authority.
    pub async fn exec_container_stream_ready_classified_for_request(
        &self,
        dispatch_gate: ContainerExecDispatchGate,
        request_id: String,
        container_id: String,
        command: String,
        args: Vec<String>,
        options: ExecOptions,
    ) -> Result<(GrpcExecStream, u64, vz_agent_proto::ContainerGeneration), ContainerExecStartError>
    {
        let mut client = GrpcAgentClient::connect_default(Arc::clone(&self.vm))
            .await
            .map_err(ContainerExecStartError::Definite)?;
        client
            .ping()
            .await
            .map_err(ContainerExecStartError::Definite)?;
        let info = client
            .system_info()
            .await
            .map_err(ContainerExecStartError::Definite)?;
        validate_guest_system_info(&info).map_err(ContainerExecStartError::Definite)?;
        client
            .exec_container_stream_ready_for_request(
                dispatch_gate,
                request_id,
                container_id,
                command,
                args,
                options,
            )
            .await
    }

    /// Run and collect a raw command inside a running OCI container.
    pub async fn exec_container_collect_with_options(
        &self,
        container_id: String,
        command: String,
        args: Vec<String>,
        timeout: Duration,
        options: ExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        let deadline = Instant::now() + timeout;
        let dispatch_gate = ContainerExecDispatchGate::new(deadline);
        let request_id = tokio::time::timeout_at(deadline, self.prepare_container_exec_request())
            .await
            .map_err(|_| {
                LinuxError::Protocol(format!(
                    "container exec timed out after {:.3}s before request allocation completed",
                    timeout.as_secs_f64()
                ))
            })??;
        let start_request_id = request_id.clone();
        let start_vm = Arc::clone(&self.vm);
        let cleanup_vm = Arc::clone(&self.vm);
        let start_dispatch_gate = dispatch_gate.clone();
        let start_task = tokio::spawn(async move {
            let mut client = GrpcAgentClient::connect_default(start_vm)
                .await
                .map_err(ContainerExecStartError::Definite)?;
            client
                .ping()
                .await
                .map_err(ContainerExecStartError::Definite)?;
            let info = client
                .system_info()
                .await
                .map_err(ContainerExecStartError::Definite)?;
            validate_guest_system_info(&info).map_err(ContainerExecStartError::Definite)?;
            let (stream, exec_id, generation) = client
                .exec_container_stream_ready_for_request(
                    start_dispatch_gate,
                    start_request_id,
                    container_id,
                    command,
                    args,
                    options,
                )
                .await?;
            Ok((client, stream, exec_id, generation))
        });
        let mut starting =
            StartingOwnedContainerExec::new(start_task, cleanup_vm, request_id, dispatch_gate);

        let start_result = tokio::time::timeout_at(deadline, starting.task_mut()).await;
        let mut ready = match start_result {
            Ok(Ok(Ok((client, stream, exec_id, _generation)))) => {
                starting.promote(client, stream, exec_id)
            }
            Ok(Ok(Err(ContainerExecStartError::Definite(error)))) => {
                starting.finish_without_ready();
                return Err(error);
            }
            Ok(Ok(Err(ContainerExecStartError::Ambiguous(error)))) => {
                let reconciliation = tokio::time::timeout(
                    CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT,
                    starting.reconcile_ambiguous_start_failure(
                        "container exec startup returned an ambiguous failure",
                    ),
                )
                .await;
                return match reconciliation {
                    Ok(outcome) => Err(LinuxError::Protocol(format!(
                        "{error}; reconciliation={outcome}"
                    ))),
                    Err(_) => {
                        starting.retain_cleanup(
                            "container exec ambiguous startup reconciliation remains pending",
                        );
                        Err(LinuxError::Protocol(format!(
                            "{error}; reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"
                        )))
                    }
                };
            }
            Ok(Err(join_error)) => {
                let reconciliation = tokio::time::timeout(
                    CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT,
                    starting.reconcile_ambiguous_start_failure(
                        "container exec startup task failed ambiguously",
                    ),
                )
                .await;
                return match reconciliation {
                    Ok(outcome) => Err(LinuxError::Protocol(format!(
                        "container exec startup task failed: {join_error}; reconciliation={outcome}"
                    ))),
                    Err(_) => {
                        starting.retain_cleanup(
                            "failed container exec startup reconciliation remains pending",
                        );
                        Err(LinuxError::Protocol(format!(
                            "container exec startup task failed: {join_error}; reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"
                        )))
                    }
                };
            }
            Err(_) => {
                let cleanup = tokio::time::timeout(
                    CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT,
                    starting.cancel_after_start(),
                )
                .await;
                let context = format!(
                    "container exec timed out after {:.3}s during startup",
                    timeout.as_secs_f64()
                );
                return match cleanup {
                    Ok(Ok(())) => Err(LinuxError::Protocol(context)),
                    Ok(Err(error)) => Err(LinuxError::Protocol(format!(
                        "{context}; cleanup failed: {error}"
                    ))),
                    Err(_) => {
                        starting.retain_cleanup(
                            "container exec startup timeout cleanup remains pending",
                        );
                        Err(LinuxError::Protocol(format!(
                            "{context}; cleanup proof remains pending under retained authority"
                        )))
                    }
                };
            }
        };

        let collected = tokio::time::timeout_at(deadline, async {
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            loop {
                match ready.stream_mut().next_checked().await? {
                    Some(ExecEvent::Stdout(data)) => stdout.extend_from_slice(&data),
                    Some(ExecEvent::Stderr(data)) => stderr.extend_from_slice(&data),
                    Some(ExecEvent::Exit(exit_code)) => {
                        return Ok(ExecOutput {
                            exit_code,
                            stdout: String::from_utf8_lossy(&stdout).into_owned(),
                            stderr: String::from_utf8_lossy(&stderr).into_owned(),
                        });
                    }
                    None => {
                        return Err(LinuxError::Protocol(
                            "container exec stream ended without an exit receipt".to_string(),
                        ));
                    }
                }
            }
        })
        .await;

        match collected {
            Ok(Ok(output)) => {
                ready.complete();
                Ok(output)
            }
            Ok(Err(error)) => {
                ready.cancel_and_reap().await;
                Err(error)
            }
            Err(_) => {
                ready.cancel_and_reap().await;
                Err(LinuxError::Protocol(format!(
                    "container exec timed out after {:.3}s",
                    timeout.as_secs_f64()
                )))
            }
        }
    }

    /// Run a command on the guest, collect output via streaming, with a timeout.
    ///
    /// Convenience wrapper: opens a stream, collects all events, applies timeout.
    /// Uses checked collection: a spawn error, transport loss, or missing
    /// terminal event is an error, never a synthesized process exit status.
    pub async fn exec_collect(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
    ) -> Result<ExecOutput, LinuxError> {
        tokio::time::timeout(timeout, async {
            let stream: GrpcExecStream = self.exec_stream(command, args).await?;
            stream.collect_checked().await
        })
        .await
        .map_err(|_| {
            LinuxError::Protocol(format!(
                "exec timed out after {:.3}s",
                timeout.as_secs_f64()
            ))
        })?
    }

    /// Run a command on the guest with explicit execution options, collect output via streaming.
    pub async fn exec_collect_with_options(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
        options: ExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        tokio::time::timeout(timeout, async {
            let stream: GrpcExecStream = self
                .exec_stream_with_options(command, args, options)
                .await?;
            stream.collect_checked().await
        })
        .await
        .map_err(|_| {
            LinuxError::Protocol(format!(
                "exec timed out after {:.3}s",
                timeout.as_secs_f64()
            ))
        })?
    }

    /// Run a command on the guest, stream output events via callback, collect final output.
    pub async fn exec_streaming<F>(
        &self,
        command: String,
        args: Vec<String>,
        timeout: Duration,
        on_event: F,
    ) -> Result<ExecOutput, LinuxError>
    where
        F: FnMut(&ExecEvent),
    {
        self.exec_streaming_with_options(command, args, timeout, ExecOptions::default(), on_event)
            .await
    }

    /// Run a command with explicit execution options and stream output events via callback.
    pub async fn exec_streaming_with_options<F>(
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
        tokio::time::timeout(timeout, async {
            let mut stream: GrpcExecStream = self
                .exec_stream_with_options(command, args, options)
                .await?;
            let mut stdout_bytes: Vec<u8> = Vec::new();
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

    /// Open an opaque connection to this VM's provisioned private Docker Engine.
    pub async fn open_docker_stream(&self) -> Result<crate::GrpcDockerStream, LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.docker_forward().await
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
        let debug = exec_control_debug_enabled();
        if debug {
            debug!("[vz-linux exec-control] oci_state start container_id={id}");
        }
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        let state_result = client.oci_state(id.clone()).await;
        if debug {
            match &state_result {
                Ok(state) => debug!(
                    "[vz-linux exec-control] oci_state complete container_id={} status={} pid={:?}",
                    id, state.status, state.pid
                ),
                Err(error) => debug!(
                    "[vz-linux exec-control] oci_state failed container_id={} error={error}",
                    id
                ),
            }
        }
        state_result
    }

    /// Legacy OCI unary exec is retired; synchronous callers must collect the
    /// supervised container exec stream instead.
    #[deprecated(note = "use exec_container_stream_ready_with_options and collect the stream")]
    pub async fn oci_exec(
        &self,
        _id: String,
        _command: String,
        _args: Vec<String>,
        _options: OciExecOptions,
    ) -> Result<ExecOutput, LinuxError> {
        Err(LinuxError::Protocol(
            "legacy OciService.Exec is retired; use supervised AgentService.Exec stream collection"
                .to_string(),
        ))
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
        working_dir: Option<&str>,
        rows: u32,
        cols: u32,
    ) -> Result<(crate::grpc_client::GrpcExecStream, u64), LinuxError> {
        let debug = exec_control_debug_enabled();
        if debug {
            debug!(
                "[vz-linux exec-control] exec_interactive start command={:?} args={:?} rows={} cols={} cwd={:?}",
                command, args, rows, cols, working_dir
            );
        }
        let mut client =
            GrpcAgentClient::connect(Arc::clone(&self.vm), vz::protocol::AGENT_PORT).await?;
        client.ping().await?;
        let info = client.system_info().await?;
        validate_guest_system_info(&info)?;
        let args_owned: Vec<String> = args.iter().map(|s| s.to_string()).collect();
        let options = ExecOptions {
            working_dir: working_dir.map(|s| s.to_string()),
            ..ExecOptions::default()
        };
        let interactive_result = client
            .exec_stream_interactive(command.to_string(), args_owned, options, rows, cols)
            .await;
        if debug {
            match &interactive_result {
                Ok((_, exec_id)) => {
                    debug!("[vz-linux exec-control] exec_interactive complete exec_id={exec_id}")
                }
                Err(error) => {
                    debug!("[vz-linux exec-control] exec_interactive failed error={error}")
                }
            }
        }
        interactive_result
    }

    /// Start a PTY exec using a request identity retained by the caller.
    /// Ambiguous errors and dropped futures require reconciliation of this
    /// exact ID before releasing VM or lifecycle authority.
    #[allow(clippy::too_many_arguments)]
    pub async fn exec_container_interactive_ready_classified_for_request(
        &self,
        dispatch_gate: ContainerExecDispatchGate,
        request_id: String,
        container_id: String,
        command: &str,
        args: &[&str],
        options: ExecOptions,
        rows: u32,
        cols: u32,
    ) -> Result<
        (
            crate::grpc_client::GrpcExecStream,
            u64,
            vz_agent_proto::ContainerGeneration,
        ),
        ContainerExecStartError,
    > {
        let mut client = GrpcAgentClient::connect(Arc::clone(&self.vm), vz::protocol::AGENT_PORT)
            .await
            .map_err(ContainerExecStartError::Definite)?;
        client
            .ping()
            .await
            .map_err(ContainerExecStartError::Definite)?;
        let info = client
            .system_info()
            .await
            .map_err(ContainerExecStartError::Definite)?;
        validate_guest_system_info(&info).map_err(ContainerExecStartError::Definite)?;
        let args_owned = args.iter().map(|arg| (*arg).to_string()).collect();
        client
            .exec_container_stream_interactive_ready_for_request(
                dispatch_gate,
                request_id,
                container_id,
                command.to_string(),
                args_owned,
                options,
                rows,
                cols,
            )
            .await
    }

    /// Write data to a running exec's stdin (or PTY master).
    pub async fn stdin_write(&self, exec_id: u64, data: &[u8]) -> Result<(), LinuxError> {
        let debug = exec_control_debug_enabled();
        if debug {
            debug!(
                "[vz-linux exec-control] stdin_write start exec_id={exec_id} bytes={}",
                data.len()
            );
        }
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        let write_result = client.stdin_write(exec_id, data).await;
        if debug {
            match &write_result {
                Ok(()) => {
                    debug!("[vz-linux exec-control] stdin_write complete exec_id={exec_id}")
                }
                Err(error) => debug!(
                    "[vz-linux exec-control] stdin_write failed exec_id={exec_id} error={error}"
                ),
            }
        }
        write_result
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

    /// Cancel an exec and wait until the guest confirms it is terminal and reaped.
    pub async fn cancel_exec(
        &self,
        exec_id: u64,
    ) -> Result<vz_agent_proto::CancelExecResponse, LinuxError> {
        self.ensure_grpc().await?;
        let mut grpc = self.grpc.lock().await;
        let client = grpc
            .as_mut()
            .ok_or_else(|| LinuxError::Protocol("gRPC client not connected".to_string()))?;
        client.cancel_exec(exec_id).await
    }

    /// Reconnect and obtain exact guest proof for an ambiguously dispatched
    /// container exec request. Callers must retain lifecycle authority until
    /// this returns a terminal proof outcome (rather than `STALE_UNKNOWN`).
    pub async fn reconcile_exec_request(
        &self,
        request_id: String,
    ) -> Result<vz_agent_proto::ReconcileExecResponse, LinuxError> {
        let mut client = GrpcAgentClient::connect_default(Arc::clone(&self.vm)).await?;
        client.ping().await?;
        let info = client.system_info().await?;
        validate_guest_system_info(&info)?;
        client.reconcile_exec_request(request_id).await
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

fn shutdown_filesystem_identity_valid(receipt: &vz_agent_proto::DockerShutdownComplete) -> bool {
    uuid::Uuid::parse_str(&receipt.filesystem_uuid).is_ok_and(|value| !value.is_nil())
        && !receipt
            .filesystem_features
            .iter()
            .any(|feature| feature == "needs_recovery")
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    #[test]
    fn shutdown_filesystem_identity_rejects_nil_and_pending_journal_recovery() {
        let mut receipt = vz_agent_proto::DockerShutdownComplete {
            filesystem_uuid: "80145f4c-5bb6-4220-bb9a-a01c19c8e178".into(),
            filesystem_features: vec!["has_journal".into(), "extent".into()],
            ..Default::default()
        };
        assert!(shutdown_filesystem_identity_valid(&receipt));
        receipt.filesystem_features.push("needs_recovery".into());
        assert!(!shutdown_filesystem_identity_valid(&receipt));
        receipt.filesystem_features.pop();
        for invalid in ["", "not-a-uuid", "00000000-0000-0000-0000-000000000000"] {
            receipt.filesystem_uuid = invalid.into();
            assert!(!shutdown_filesystem_identity_valid(&receipt));
        }
    }

    fn sample_info() -> SystemInfoResponse {
        SystemInfoResponse {
            cpu_count: 4,
            memory_bytes: 8_589_934_592,
            disk_free_bytes: 50_000_000_000,
            os_version: "Linux 6.12".to_string(),
            agent_protocol_revision: vz_agent_proto::AGENT_PROTOCOL_REVISION,
        }
    }

    #[test]
    fn validate_guest_system_info_accepts_expected_revision_and_linux_os() {
        let info = sample_info();
        validate_guest_system_info(&info).expect("valid guest system info");
    }

    #[test]
    fn validate_guest_system_info_rejects_non_linux_os() {
        let mut info = sample_info();
        info.os_version = "Darwin 25.0".to_string();
        let error = validate_guest_system_info(&info).expect_err("must reject non-linux guest");
        assert!(matches!(error, LinuxError::UnexpectedGuestOs(_)));
    }

    #[test]
    fn validate_guest_system_info_rejects_protocol_revision_mismatch() {
        let mut info = sample_info();
        info.agent_protocol_revision = vz_agent_proto::AGENT_PROTOCOL_REVISION.saturating_add(1);
        let error = validate_guest_system_info(&info).expect_err("must reject revision mismatch");
        assert!(matches!(
            error,
            LinuxError::GuestProtocolRevisionMismatch { .. }
        ));
    }

    #[test]
    fn container_collector_uses_addressable_checked_exec_path() {
        let source = include_str!("vm.rs");
        let collector = source
            .split_once("pub async fn exec_container_collect_with_options")
            .unwrap()
            .1
            .split_once("/// Run a command on the guest")
            .unwrap()
            .0;

        assert!(collector.contains("exec_container_stream_ready"));
        assert!(collector.contains("next_checked"));
        assert!(collector.contains("cancel_and_reap"));
        assert!(collector.contains("CONTAINER_EXEC_CLEANUP_ATTEMPT_TIMEOUT"));
        assert!(collector.contains("starting.cancel_after_start()"));
        assert!(collector.contains("starting.retain_cleanup("));
        assert!(collector.contains("ContainerExecStartError::Definite"));
        assert!(collector.contains("ContainerExecStartError::Ambiguous"));
        assert!(collector.contains("reconcile_ambiguous_start_failure("));
        assert!(collector.contains("reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"));
        assert!(collector.contains("exec_container_stream_ready_for_request"));
        assert!(!collector.contains(".collect().await"));
        assert!(!collector.contains("ExecEvent::Exit(-1)"));
    }

    #[test]
    fn container_exec_drop_guards_fail_closed_without_a_runtime() {
        let source = include_str!("vm.rs");
        let ownership = source
            .split_once("fn exec_control_debug_enabled")
            .unwrap()
            .0;

        assert!(ownership.matches("Handle::try_current()").count() >= 3);
        assert!(ownership.matches("std::mem::forget").count() >= 5);
        assert!(ownership.contains("container exec startup cleanup lost its Tokio runtime"));
        assert!(ownership.contains("container exec cleanup lost its Tokio runtime"));
    }

    #[test]
    fn terminal_vm_state_is_exact_reconciliation_fallback_proof() {
        assert_eq!(
            terminal_vm_reconciliation_outcome(VmState::Stopped),
            Some("VM_TERMINAL_STOPPED")
        );
        assert_eq!(
            terminal_vm_reconciliation_outcome(VmState::Error("failed".to_string())),
            Some("VM_TERMINAL_ERROR")
        );
        for state in [
            VmState::Starting,
            VmState::Running,
            VmState::Pausing,
            VmState::Paused,
            VmState::Resuming,
            VmState::Stopping,
            VmState::Saving,
            VmState::Restoring,
        ] {
            assert_eq!(terminal_vm_reconciliation_outcome(state), None);
        }
    }

    #[test]
    fn classified_pipe_start_uses_dedicated_client_without_control_mutex() {
        let source = include_str!("vm.rs");
        let method = source
            .split_once("pub async fn exec_container_stream_ready_classified_for_request")
            .unwrap()
            .1
            .split_once("/// Run and collect a raw command")
            .unwrap()
            .0;
        assert!(method.contains("GrpcAgentClient::connect_default"));
        assert!(method.contains(".ping()"));
        assert!(!method.contains("self.grpc.lock()"));
    }

    #[test]
    fn public_container_exec_surface_preserves_request_identity_or_owns_cleanup() {
        let vm = include_str!("vm.rs").split_once("#[cfg(test)]").unwrap().0;
        let client = include_str!("grpc_client.rs")
            .split_once("#[cfg(test)]")
            .unwrap()
            .0;

        for removed in [
            "pub async fn exec_container_stream_with_options(",
            "pub async fn exec_container_stream_ready_with_options(",
            "pub async fn exec_container_stream_ready_classified_with_options(",
            "pub async fn exec_container_interactive(",
            "pub async fn exec_container_interactive_ready(",
            "pub async fn exec_container_interactive_ready_classified(",
        ] {
            assert!(!vm.contains(removed), "lossy VM API remains: {removed}");
        }
        for removed in [
            "pub async fn exec_container_stream(",
            "pub async fn exec_container_stream_ready(",
            "pub async fn exec_container_stream_interactive(",
            "pub async fn exec_container_stream_interactive_ready(",
        ] {
            assert!(
                !client.contains(removed),
                "lossy client API remains: {removed}"
            );
        }

        assert!(vm.contains("pub async fn exec_container_collect_with_options("));
        assert!(vm.contains("pub async fn exec_container_stream_ready_classified_for_request("));
        assert!(
            vm.contains("pub async fn exec_container_interactive_ready_classified_for_request(")
        );
        assert!(client.contains("pub async fn exec_container_stream_ready_for_request("));
        assert!(
            client.contains("pub async fn exec_container_stream_interactive_ready_for_request(")
        );
    }
}
