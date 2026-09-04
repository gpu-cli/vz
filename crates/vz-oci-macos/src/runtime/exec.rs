use super::networking::ensure_interactive_exec_pty_prerequisites;
use super::oci_lifecycle::parse_signal_number;
use super::*;
use tracing::{debug, warn};

const EXEC_TERMINATION_WAIT: Duration = Duration::from_secs(10);
pub(super) const MAX_PENDING_EXEC_CONTROLS: usize = 256;
pub(super) const MAX_PENDING_EXEC_STDIN_BYTES: usize = 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PendingExecQueueError {
    Cancelled,
    OperationLimit,
    StdinByteLimit,
}

impl PendingExecControls {
    fn reserve_operation(&self) -> Result<(), PendingExecQueueError> {
        if self.cancel_requested {
            return Err(PendingExecQueueError::Cancelled);
        }
        if self.operations.len() >= MAX_PENDING_EXEC_CONTROLS {
            return Err(PendingExecQueueError::OperationLimit);
        }
        Ok(())
    }

    pub(super) fn queue_signal(&mut self, signal: i32) -> Result<(), PendingExecQueueError> {
        self.reserve_operation()?;
        self.operations.push(PendingExecControl::Signal(signal));
        Ok(())
    }

    pub(super) fn queue_stdin(&mut self, data: &[u8]) -> Result<(), PendingExecQueueError> {
        self.reserve_operation()?;
        let stdin_bytes = self
            .stdin_bytes
            .checked_add(data.len())
            .ok_or(PendingExecQueueError::StdinByteLimit)?;
        if stdin_bytes > MAX_PENDING_EXEC_STDIN_BYTES {
            return Err(PendingExecQueueError::StdinByteLimit);
        }
        self.stdin_bytes = stdin_bytes;
        self.operations
            .push(PendingExecControl::Stdin(data.to_vec()));
        Ok(())
    }

    pub(super) fn queue_resize(
        &mut self,
        rows: u32,
        cols: u32,
    ) -> Result<(), PendingExecQueueError> {
        if self.cancel_requested {
            return Err(PendingExecQueueError::Cancelled);
        }
        if let Some(PendingExecControl::Resize {
            rows: pending_rows,
            cols: pending_cols,
        }) = self.operations.last_mut()
        {
            *pending_rows = rows;
            *pending_cols = cols;
        } else {
            self.reserve_operation()?;
            self.operations
                .push(PendingExecControl::Resize { rows, cols });
        }
        Ok(())
    }

    pub(super) fn request_cancel(&mut self) -> bool {
        if self.cancel_requested {
            return false;
        }
        self.cancel_requested = true;
        self.operations.clear();
        self.stdin_bytes = 0;
        self.operations.push(PendingExecControl::Cancel);
        true
    }
}

pub(super) fn pending_control_error(error: PendingExecQueueError, execution_id: &str) -> OciError {
    match error {
        PendingExecQueueError::Cancelled => OciError::ExecutionSessionNotFound {
            execution_id: execution_id.to_string(),
        },
        PendingExecQueueError::OperationLimit => OciError::InvalidConfig(format!(
            "execution session '{execution_id}' exceeded the pending control limit of {MAX_PENDING_EXEC_CONTROLS}"
        )),
        PendingExecQueueError::StdinByteLimit => OciError::InvalidConfig(format!(
            "execution session '{execution_id}' exceeded the pending stdin limit of {MAX_PENDING_EXEC_STDIN_BYTES} bytes"
        )),
    }
}

impl ContainerExecSession {
    fn new(vm: Arc<LinuxVm>, pty_enabled: bool) -> Self {
        Self {
            vm,
            pty_enabled,
            control: Arc::new(Mutex::new(())),
            state: Arc::new(Mutex::new(ContainerExecSessionState::Starting {
                pending: PendingExecControls::default(),
                dispatch_gate: None,
            })),
            start_cancel: Arc::new(tokio::sync::Notify::new()),
            terminal: Arc::new(tokio::sync::Notify::new()),
        }
    }

    async fn install_dispatch_gate(&self, dispatch_gate: ContainerExecDispatchGate) {
        let mut state = self.state.lock().await;
        match &mut *state {
            ContainerExecSessionState::Starting {
                pending,
                dispatch_gate: installed,
            } => {
                if pending.cancel_requested {
                    dispatch_gate.cancel_before_dispatch();
                }
                *installed = Some(dispatch_gate);
            }
            ContainerExecSessionState::Running { .. } => {
                unreachable!("dispatch gate must be installed before guest binding")
            }
            ContainerExecSessionState::Finished => {
                dispatch_gate.cancel_before_dispatch();
            }
        }
    }

    async fn bind_guest(&self, guest_exec_id: u64) -> Result<(), OciError> {
        if guest_exec_id == 0 {
            return Err(OciError::InvalidConfig(
                "container exec readiness omitted guest exec ID".to_string(),
            ));
        }

        let _control = self.control.lock().await;
        let pending = {
            let mut state = self.state.lock().await;
            match &mut *state {
                ContainerExecSessionState::Starting { pending, .. } => {
                    let pending = std::mem::take(pending);
                    *state = ContainerExecSessionState::Running {
                        guest_exec_id,
                        cancel_requested: false,
                    };
                    pending
                }
                ContainerExecSessionState::Running { .. } => {
                    return Err(OciError::InvalidConfig(
                        "container exec session was registered twice".to_string(),
                    ));
                }
                ContainerExecSessionState::Finished => {
                    return Err(OciError::InvalidConfig(
                        "container exec became terminal before guest registration".to_string(),
                    ));
                }
            }
        };

        for operation in pending.operations {
            if !matches!(operation, PendingExecControl::Cancel)
                && self.running_cancel_requested().await
            {
                break;
            }
            match operation {
                PendingExecControl::Signal(signal) => {
                    self.vm.signal(guest_exec_id, signal).await?;
                }
                PendingExecControl::Stdin(data) => {
                    self.vm.stdin_write(guest_exec_id, &data).await?;
                }
                PendingExecControl::Resize { rows, cols } => {
                    self.vm.resize_exec_pty(guest_exec_id, rows, cols).await?;
                }
                PendingExecControl::Cancel => {
                    self.cancel_running(guest_exec_id).await?;
                    break;
                }
            }
        }
        Ok(())
    }

    async fn signal(&self, signal: i32, execution_id: &str) -> Result<(), OciError> {
        let _control = self.control.lock().await;
        let guest_exec_id = {
            let mut state = self.state.lock().await;
            match &mut *state {
                ContainerExecSessionState::Starting { pending, .. } => {
                    return pending
                        .queue_signal(signal)
                        .map_err(|error| pending_control_error(error, execution_id));
                }
                ContainerExecSessionState::Running {
                    guest_exec_id,
                    cancel_requested: false,
                } => *guest_exec_id,
                ContainerExecSessionState::Running {
                    cancel_requested: true,
                    ..
                } => {
                    return Err(OciError::ExecutionSessionNotFound {
                        execution_id: execution_id.to_string(),
                    });
                }
                ContainerExecSessionState::Finished => {
                    return Err(OciError::ExecutionSessionNotFound {
                        execution_id: execution_id.to_string(),
                    });
                }
            }
        };
        self.vm.signal(guest_exec_id, signal).await?;
        Ok(())
    }

    async fn cancel(&self, execution_id: &str) -> Result<(), OciError> {
        let guest_exec_id = {
            let mut state = self.state.lock().await;
            match &mut *state {
                ContainerExecSessionState::Starting {
                    pending,
                    dispatch_gate,
                } => {
                    if let Some(dispatch_gate) = dispatch_gate {
                        dispatch_gate.cancel_before_dispatch();
                    }
                    if pending.request_cancel() {
                        self.start_cancel.notify_waiters();
                    }
                    None
                }
                ContainerExecSessionState::Running {
                    guest_exec_id,
                    cancel_requested,
                } => {
                    *cancel_requested = true;
                    Some(*guest_exec_id)
                }
                ContainerExecSessionState::Finished => {
                    return Err(OciError::ExecutionSessionNotFound {
                        execution_id: execution_id.to_string(),
                    });
                }
            }
        };

        if let Some(guest_exec_id) = guest_exec_id {
            self.cancel_running(guest_exec_id).await?;
            return Ok(());
        }

        tokio::time::timeout(EXEC_TERMINATION_WAIT, self.wait_finished())
            .await
            .map_err(|_| {
                OciError::InvalidConfig(format!(
                    "timed out waiting for starting exec '{execution_id}' to cancel"
                ))
            })?;
        Ok(())
    }

    async fn wait_start_cancel_requested(&self) {
        loop {
            let notified = self.start_cancel.notified();
            if self.start_cancel_requested().await {
                return;
            }
            notified.await;
        }
    }

    async fn start_cancel_requested(&self) -> bool {
        matches!(
            *self.state.lock().await,
            ContainerExecSessionState::Starting {
                pending: PendingExecControls {
                    cancel_requested: true,
                    ..
                },
                ..
            } | ContainerExecSessionState::Finished
        )
    }

    async fn write_stdin(&self, data: &[u8], execution_id: &str) -> Result<(), OciError> {
        let _control = self.control.lock().await;
        let guest_exec_id = {
            let mut state = self.state.lock().await;
            match &mut *state {
                ContainerExecSessionState::Starting { pending, .. } => {
                    return pending
                        .queue_stdin(data)
                        .map_err(|error| pending_control_error(error, execution_id));
                }
                ContainerExecSessionState::Running {
                    guest_exec_id,
                    cancel_requested: false,
                } => *guest_exec_id,
                ContainerExecSessionState::Running {
                    cancel_requested: true,
                    ..
                } => {
                    return Err(OciError::ExecutionSessionNotFound {
                        execution_id: execution_id.to_string(),
                    });
                }
                ContainerExecSessionState::Finished => {
                    return Err(OciError::ExecutionSessionNotFound {
                        execution_id: execution_id.to_string(),
                    });
                }
            }
        };
        self.vm.stdin_write(guest_exec_id, data).await?;
        Ok(())
    }

    async fn resize_pty(&self, rows: u32, cols: u32, execution_id: &str) -> Result<(), OciError> {
        let _control = self.control.lock().await;
        let guest_exec_id = {
            let mut state = self.state.lock().await;
            match &mut *state {
                ContainerExecSessionState::Starting { pending, .. } => {
                    return pending
                        .queue_resize(rows, cols)
                        .map_err(|error| pending_control_error(error, execution_id));
                }
                ContainerExecSessionState::Running {
                    guest_exec_id,
                    cancel_requested: false,
                } => *guest_exec_id,
                ContainerExecSessionState::Running {
                    cancel_requested: true,
                    ..
                } => {
                    return Err(OciError::ExecutionSessionNotFound {
                        execution_id: execution_id.to_string(),
                    });
                }
                ContainerExecSessionState::Finished => {
                    return Err(OciError::ExecutionSessionNotFound {
                        execution_id: execution_id.to_string(),
                    });
                }
            }
        };
        self.vm.resize_exec_pty(guest_exec_id, rows, cols).await?;
        Ok(())
    }

    async fn cancel_running(&self, guest_exec_id: u64) -> Result<i32, OciError> {
        match self.vm.cancel_exec(guest_exec_id).await {
            Ok(receipt) => {
                self.mark_finished().await;
                Ok(receipt.exit_code)
            }
            Err(error) => {
                if self.is_finished().await {
                    Ok(-1)
                } else {
                    Err(error.into())
                }
            }
        }
    }

    async fn guest_exec_id(&self) -> Option<u64> {
        match *self.state.lock().await {
            ContainerExecSessionState::Running { guest_exec_id, .. } => Some(guest_exec_id),
            ContainerExecSessionState::Starting { .. } | ContainerExecSessionState::Finished => {
                None
            }
        }
    }

    async fn running_cancel_requested(&self) -> bool {
        matches!(
            *self.state.lock().await,
            ContainerExecSessionState::Running {
                cancel_requested: true,
                ..
            } | ContainerExecSessionState::Finished
        )
    }

    async fn is_finished(&self) -> bool {
        matches!(
            *self.state.lock().await,
            ContainerExecSessionState::Finished
        )
    }

    async fn mark_finished(&self) {
        *self.state.lock().await = ContainerExecSessionState::Finished;
        self.terminal.notify_waiters();
    }

    async fn wait_finished(&self) {
        loop {
            let notified = self.terminal.notified();
            if self.is_finished().await {
                return;
            }
            notified.await;
        }
    }
}

struct ExecSessionRegistration {
    execution_id: String,
    session: ContainerExecSession,
    registry: Arc<Mutex<HashMap<String, ContainerExecSession>>>,
    armed: bool,
}

impl ExecSessionRegistration {
    async fn finish(&mut self) {
        self.session.mark_finished().await;
        let mut registry = self.registry.lock().await;
        if registry
            .get(&self.execution_id)
            .is_some_and(|current| Arc::ptr_eq(&current.state, &self.session.state))
        {
            registry.remove(&self.execution_id);
        }
        self.armed = false;
    }
}

impl Drop for ExecSessionRegistration {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let execution_id = self.execution_id.clone();
        let session = self.session.clone();
        let registry = Arc::clone(&self.registry);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Some(guest_exec_id) = session.guest_exec_id().await {
                    loop {
                        match tokio::time::timeout(
                            EXEC_TERMINATION_WAIT,
                            session.cancel_running(guest_exec_id),
                        )
                        .await
                        {
                            Ok(Ok(_)) => break,
                            Ok(Err(error)) => warn!(
                                execution_id,
                                guest_exec_id,
                                %error,
                                "exec registration drop cleanup failed; retaining authority"
                            ),
                            Err(_) => warn!(
                                execution_id,
                                guest_exec_id,
                                "exec registration drop cleanup timed out; retaining authority"
                            ),
                        }
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                } else {
                    // No guest identity was ever bound, so no post-readiness
                    // process exists for this registration to cancel.
                    session.mark_finished().await;
                }
                let mut registry = registry.lock().await;
                if registry
                    .get(&execution_id)
                    .is_some_and(|current| Arc::ptr_eq(&current.state, &session.state))
                {
                    registry.remove(&execution_id);
                }
            });
        }
    }
}

type GuestExecStart = Result<
    (vz_linux::GrpcExecStream, u64, vz_linux::ContainerGeneration),
    vz_linux::ContainerExecStartError,
>;

async fn reconcile_ambiguous_start_until_proven(
    vm: &LinuxVm,
    request_id: &str,
    context: &str,
) -> &'static str {
    use vz_linux::reconcile_exec_response::Outcome;

    loop {
        match vm.inner().state() {
            vz::VmState::Stopped => return "VM_TERMINAL_STOPPED",
            vz::VmState::Error(_) => return "VM_TERMINAL_ERROR",
            _ => {}
        }
        let attempt = tokio::time::timeout(
            EXEC_TERMINATION_WAIT,
            vm.reconcile_exec_request(request_id.to_string()),
        )
        .await;
        match attempt {
            Ok(Ok(response)) if response.outcome == Outcome::FencedNeverStarted as i32 => {
                return "FENCED_NEVER_STARTED";
            }
            Ok(Ok(response)) if response.outcome == Outcome::TerminalReaped as i32 => {
                return "TERMINAL_REAPED";
            }
            Ok(Ok(response)) => warn!(
                "[vz-oci-macos exec-control] {context}; request {request_id} reconciliation outcome {} remains unproven",
                response.outcome
            ),
            Ok(Err(error)) => warn!(
                "[vz-oci-macos exec-control] {context}; request {request_id} reconciliation failed: {error}"
            ),
            Err(_) => warn!(
                "[vz-oci-macos exec-control] {context}; request {request_id} reconciliation attempt timed out"
            ),
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn retain_ambiguous_start_authority(
    vm: Arc<LinuxVm>,
    request_id: String,
    registration: Option<ExecSessionRegistration>,
    admission_guard: Option<ContainerReadAdmission>,
    context: String,
) {
    let authority = (vm, registration, admission_guard);
    let Ok(runtime) = tokio::runtime::Handle::try_current() else {
        warn!(
            "[vz-oci-macos exec-control] {context}; no Tokio runtime remains; leaking ambiguous lifecycle authority"
        );
        std::mem::forget(authority);
        return;
    };
    runtime.spawn(async move {
        let (vm, mut registration, admission_guard) = authority;
        let outcome = reconcile_ambiguous_start_until_proven(&vm, &request_id, &context).await;
        if let Some(active) = registration.as_mut() {
            active.finish().await;
        }
        drop(admission_guard);
        warn!(
            "[vz-oci-macos exec-control] {context}; request {request_id} reconciliation={outcome}"
        );
    });
}

struct StartingExecLease {
    task: Option<tokio::task::JoinHandle<GuestExecStart>>,
    vm: Arc<LinuxVm>,
    request_id: String,
    dispatch_gate: ContainerExecDispatchGate,
    registration: Option<ExecSessionRegistration>,
    admission_guard: Option<ContainerReadAdmission>,
    armed: bool,
}

impl StartingExecLease {
    fn new(
        task: tokio::task::JoinHandle<GuestExecStart>,
        vm: Arc<LinuxVm>,
        request_id: String,
        dispatch_gate: ContainerExecDispatchGate,
        registration: Option<ExecSessionRegistration>,
        admission_guard: Option<ContainerReadAdmission>,
    ) -> Self {
        Self {
            task: Some(task),
            vm,
            request_id,
            dispatch_gate,
            registration,
            admission_guard,
            armed: true,
        }
    }

    fn task_mut(&mut self) -> &mut tokio::task::JoinHandle<GuestExecStart> {
        let Some(task) = self.task.as_mut() else {
            unreachable!("armed starting exec lease must retain its task");
        };
        task
    }

    fn cancel_session(&self) -> Option<ContainerExecSession> {
        self.registration
            .as_ref()
            .map(|registration| registration.session.clone())
    }

    fn promote(&mut self, stream: vz_linux::GrpcExecStream, guest_exec_id: u64) -> ReadyExecLease {
        self.armed = false;
        self.task.take();
        ReadyExecLease::new(
            Arc::clone(&self.vm),
            stream,
            guest_exec_id,
            self.registration.take(),
            self.admission_guard.take(),
        )
    }

    async fn finish_pre_ready(&mut self) {
        if let Some(registration) = self.registration.as_mut() {
            registration.finish().await;
        }
        self.armed = false;
        self.task.take();
        self.admission_guard.take();
    }

    async fn finish_if_dispatch_prevented(&mut self) -> bool {
        if !self.dispatch_gate.cancel_before_dispatch() {
            return false;
        }
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(registration) = self.registration.as_mut() {
            registration.finish().await;
        }
        self.admission_guard.take();
        self.armed = false;
        true
    }

    fn retain_unknown_start_failure(&mut self, context: String) {
        if !self.armed {
            return;
        }
        self.armed = false;
        self.task.take();
        retain_ambiguous_start_authority(
            Arc::clone(&self.vm),
            self.request_id.clone(),
            self.registration.take(),
            self.admission_guard.take(),
            context,
        );
    }

    async fn reconcile_unknown_start_failure(&mut self, context: String) -> &'static str {
        self.task.take();
        let outcome =
            reconcile_ambiguous_start_until_proven(&self.vm, &self.request_id, &context).await;
        if let Some(registration) = self.registration.as_mut() {
            registration.finish().await;
        }
        self.admission_guard.take();
        self.armed = false;
        outcome
    }

    async fn resolve_unknown_start_failure_bounded(
        &mut self,
        context: String,
    ) -> ExecStartFailureResolution {
        match tokio::time::timeout(
            EXEC_TERMINATION_WAIT,
            self.reconcile_unknown_start_failure(context.clone()),
        )
        .await
        {
            Ok(outcome) => ExecStartFailureResolution::Reconciled(outcome),
            Err(_) => {
                self.retain_unknown_start_failure(context);
                ExecStartFailureResolution::Pending
            }
        }
    }

    fn retain_startup_cleanup(&mut self, context: String) {
        if !self.armed {
            return;
        }
        let dispatch_prevented = self.dispatch_gate.cancel_before_dispatch();
        self.armed = false;
        let Some(task) = self.task.take() else {
            retain_ambiguous_start_authority(
                Arc::clone(&self.vm),
                self.request_id.clone(),
                self.registration.take(),
                self.admission_guard.take(),
                context,
            );
            return;
        };
        let vm = Arc::clone(&self.vm);
        let request_id = self.request_id.clone();
        let mut registration = self.registration.take();
        let admission_guard = self.admission_guard.take();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            warn!(
                "[vz-oci-macos exec-control] {context}; no Tokio runtime remains; leaking pending startup authority"
            );
            std::mem::forget(task);
            std::mem::forget(vm);
            std::mem::forget(registration);
            std::mem::forget(admission_guard);
            return;
        };
        runtime.spawn(async move {
            if dispatch_prevented {
                task.abort();
                let _ = task.await;
                if let Some(active) = registration.as_mut() {
                    active.finish().await;
                }
                drop(admission_guard);
                return;
            }
            match task.await {
                Ok(Ok((stream, guest_exec_id, _generation))) => {
                    let mut ready = ReadyExecLease::new(
                        vm,
                        stream,
                        guest_exec_id,
                        registration.take(),
                        admission_guard,
                    );
                    if let Some(active) = ready.registration()
                        && let Err(error) = active.session.bind_guest(guest_exec_id).await
                    {
                        warn!(
                            "[vz-oci-macos exec-control] {context}; guest binding failed during retained startup cleanup: {error}"
                        );
                    }
                    if let Err(error) = ready.cleanup_failure(context).await {
                        warn!(
                            "[vz-oci-macos exec-control] {error}; cleanup remains under retained authority"
                        );
                    }
                }
                Ok(Err(vz_linux::ContainerExecStartError::Definite(_guest_error))) => {
                    if let Some(active) = registration.as_mut() {
                        active.finish().await;
                    }
                    drop(admission_guard);
                }
                Ok(Err(vz_linux::ContainerExecStartError::Ambiguous(error))) => {
                    retain_ambiguous_start_authority(
                        vm,
                        request_id,
                        registration,
                        admission_guard,
                        format!("{context}; ambiguous guest start failure: {error}"),
                    );
                }
                Err(join_error) => {
                    retain_ambiguous_start_authority(
                        vm,
                        request_id,
                        registration,
                        admission_guard,
                        format!("{context}; startup cleanup task failed: {join_error}"),
                    );
                }
            }
        });
    }
}

async fn resolve_exec_start_failure(
    starting: &mut StartingExecLease,
    failure: vz_linux::ContainerExecStartError,
    context: String,
) -> (LinuxError, ExecStartFailureResolution) {
    match failure {
        vz_linux::ContainerExecStartError::Definite(error) => {
            starting.finish_pre_ready().await;
            (error, ExecStartFailureResolution::Definite)
        }
        vz_linux::ContainerExecStartError::Ambiguous(error) => {
            let reconciliation_context =
                format!("{context}; ambiguous guest start failure: {error}");
            let resolution = starting
                .resolve_unknown_start_failure_bounded(reconciliation_context)
                .await;
            (error, resolution)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecStartFailureResolution {
    Definite,
    Reconciled(&'static str),
    Pending,
}

impl Drop for StartingExecLease {
    fn drop(&mut self) {
        self.retain_startup_cleanup(
            "host exec owner dropped while guest startup was pending".to_string(),
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ExecStartInterruption {
    Cancelled,
    TimedOut,
}

pub(super) async fn await_exec_start<T, F>(
    task: &mut tokio::task::JoinHandle<T>,
    deadline: tokio::time::Instant,
    cancel: F,
    dispatch_gate: &ContainerExecDispatchGate,
) -> Result<Result<T, tokio::task::JoinError>, ExecStartInterruption>
where
    T: Send + 'static,
    F: Future<Output = ()>,
{
    tokio::pin!(cancel);
    tokio::select! {
        biased;
        () = &mut cancel => {
            dispatch_gate.cancel_before_dispatch();
            Err(ExecStartInterruption::Cancelled)
        },
        () = tokio::time::sleep_until(deadline) => {
            dispatch_gate.cancel_before_dispatch();
            Err(ExecStartInterruption::TimedOut)
        },
        result = task => Ok(result),
    }
}

async fn finish_pre_dispatch(
    registration: &mut Option<ExecSessionRegistration>,
    admission_guard: &mut Option<ContainerReadAdmission>,
) {
    if let Some(active) = registration.as_mut() {
        active.finish().await;
    }
    drop(admission_guard.take());
}

async fn prepare_exec_request_before_dispatch(
    vm: &LinuxVm,
    registration: &mut Option<ExecSessionRegistration>,
    admission_guard: &mut Option<ContainerReadAdmission>,
    execution_id: Option<&str>,
    deadline: tokio::time::Instant,
    timeout: Duration,
) -> Result<String, OciError> {
    let cancel_session = registration.as_ref().map(|active| active.session.clone());
    let allocation = vm.prepare_container_exec_request();
    tokio::pin!(allocation);
    let outcome = if let Some(session) = cancel_session.as_ref() {
        tokio::select! {
            biased;
            () = session.wait_start_cancel_requested() => Err(ExecStartInterruption::Cancelled),
            result = &mut allocation => Ok(result),
            () = tokio::time::sleep_until(deadline) => Err(ExecStartInterruption::TimedOut),
        }
    } else {
        tokio::select! {
            result = &mut allocation => Ok(result),
            () = tokio::time::sleep_until(deadline) => Err(ExecStartInterruption::TimedOut),
        }
    };

    let request_id = match outcome {
        Ok(Ok(request_id)) => request_id,
        Ok(Err(error)) => {
            finish_pre_dispatch(registration, admission_guard).await;
            return Err(error.into());
        }
        Err(ExecStartInterruption::Cancelled) => {
            finish_pre_dispatch(registration, admission_guard).await;
            return Err(OciError::InvalidConfig(format!(
                "exec '{}' was cancelled during startup",
                execution_id.unwrap_or("unaddressed")
            )));
        }
        Err(ExecStartInterruption::TimedOut) => {
            finish_pre_dispatch(registration, admission_guard).await;
            return Err(OciError::InvalidConfig(format!(
                "exec timed out after {:.3}s before request allocation completed",
                timeout.as_secs_f64()
            )));
        }
    };

    // A cancellation racing the allocation response still precedes dispatch.
    // The allocated ticket is single-use but holds no process state, so it is
    // safe to burn it and finish the session without contacting Exec.
    if let Some(session) = cancel_session
        && session.start_cancel_requested().await
    {
        finish_pre_dispatch(registration, admission_guard).await;
        return Err(OciError::InvalidConfig(format!(
            "exec '{}' was cancelled during startup",
            execution_id.unwrap_or("unaddressed")
        )));
    }

    Ok(request_id)
}

async fn await_guest_exec_cleanup(
    vm: &LinuxVm,
    stream: &mut vz_linux::GrpcExecStream,
    guest_exec_id: u64,
    context: &str,
) -> Result<(), OciError> {
    match tokio::time::timeout(EXEC_TERMINATION_WAIT, vm.cancel_exec(guest_exec_id)).await {
        Ok(Ok(_receipt)) => Ok(()),
        Ok(Err(cancel_error)) => {
            let terminal = tokio::time::timeout(EXEC_TERMINATION_WAIT, async {
                loop {
                    match stream.next_checked().await {
                        Ok(Some(ExecEvent::Exit(_))) => return true,
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => return false,
                    }
                }
            })
            .await
            .unwrap_or(false);
            if terminal {
                Ok(())
            } else {
                Err(OciError::InvalidConfig(format!(
                    "{context}; guest cleanup failed: {cancel_error}"
                )))
            }
        }
        Err(_) => Err(OciError::InvalidConfig(format!(
            "{context}; guest cleanup did not complete"
        ))),
    }
}

struct ReadyExecLease {
    vm: Arc<LinuxVm>,
    stream: Option<vz_linux::GrpcExecStream>,
    guest_exec_id: u64,
    registration: Option<ExecSessionRegistration>,
    admission_guard: Option<ContainerReadAdmission>,
    armed: bool,
}

impl ReadyExecLease {
    fn new(
        vm: Arc<LinuxVm>,
        stream: vz_linux::GrpcExecStream,
        guest_exec_id: u64,
        registration: Option<ExecSessionRegistration>,
        admission_guard: Option<ContainerReadAdmission>,
    ) -> Self {
        Self {
            vm,
            stream: Some(stream),
            guest_exec_id,
            registration,
            admission_guard,
            armed: true,
        }
    }

    fn registration(&self) -> Option<&ExecSessionRegistration> {
        self.registration.as_ref()
    }

    fn has_admission(&self) -> bool {
        self.admission_guard.is_some()
    }

    fn release_admission(&mut self) {
        drop(self.admission_guard.take());
    }

    fn stream_mut(&mut self) -> &mut vz_linux::GrpcExecStream {
        let Some(stream) = self.stream.as_mut() else {
            unreachable!("armed ready exec lease must retain its stream");
        };
        stream
    }

    async fn complete(&mut self) {
        if let Some(registration) = self.registration.as_mut() {
            registration.finish().await;
        }
        self.armed = false;
        self.stream.take();
        self.admission_guard.take();
    }

    async fn cleanup_failure(&mut self, context: String) -> Result<(), OciError> {
        let vm = Arc::clone(&self.vm);
        let guest_exec_id = self.guest_exec_id;
        let cleanup =
            await_guest_exec_cleanup(vm.as_ref(), self.stream_mut(), guest_exec_id, &context).await;
        match cleanup {
            Ok(()) => {
                self.complete().await;
                Ok(())
            }
            Err(error) => {
                self.retain_cleanup(context);
                Err(error)
            }
        }
    }

    fn retain_cleanup(&mut self, context: String) {
        if !self.armed {
            return;
        }
        self.armed = false;
        if let Some(stream) = self.stream.take() {
            retain_ready_exec_cleanup(
                Arc::clone(&self.vm),
                stream,
                self.guest_exec_id,
                self.registration.take(),
                self.admission_guard.take(),
                context,
            );
        }
    }
}

impl Drop for ReadyExecLease {
    fn drop(&mut self) {
        self.retain_cleanup("host exec owner dropped before terminal proof".to_string());
    }
}

fn retain_ready_exec_cleanup(
    vm: Arc<LinuxVm>,
    mut stream: vz_linux::GrpcExecStream,
    guest_exec_id: u64,
    mut registration: Option<ExecSessionRegistration>,
    admission_guard: Option<ContainerReadAdmission>,
    context: String,
) {
    tokio::spawn(async move {
        loop {
            match await_guest_exec_cleanup(vm.as_ref(), &mut stream, guest_exec_id, &context).await
            {
                Ok(()) => break,
                Err(error) => {
                    warn!("[vz-oci-macos exec-control] {error}; retaining cleanup authority");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
        if let Some(registration) = registration.as_mut() {
            registration.finish().await;
        }
        drop(admission_guard);
    });
}

async fn finish_interrupted_exec_start(
    starting: &mut StartingExecLease,
    context: String,
) -> Result<(), OciError> {
    if starting.finish_if_dispatch_prevented().await {
        return Ok(());
    }
    match tokio::time::timeout(EXEC_TERMINATION_WAIT, starting.task_mut()).await {
        Ok(Ok(Ok((stream, guest_exec_id, _generation)))) => {
            let mut lease = starting.promote(stream, guest_exec_id);
            if let Some(active) = lease.registration() {
                if let Err(error) = active.session.bind_guest(guest_exec_id).await {
                    let cleanup_context = format!("{context}; guest binding failed: {error}");
                    return lease.cleanup_failure(cleanup_context).await;
                }
            }
            let already_finished = if let Some(active) = lease.registration() {
                active.session.is_finished().await
            } else {
                false
            };
            if already_finished {
                lease.complete().await;
                Ok(())
            } else {
                lease.cleanup_failure(context).await
            }
        }
        Ok(Ok(Err(failure))) => {
            let (error, reconciliation) =
                resolve_exec_start_failure(starting, failure, context.clone()).await;
            match reconciliation {
                ExecStartFailureResolution::Definite => Ok(()),
                ExecStartFailureResolution::Reconciled(outcome) => {
                    Err(OciError::InvalidConfig(format!(
                        "{context}; ambiguous guest start failure: {error}; reconciliation={outcome}; lifecycle authority released after proof"
                    )))
                }
                ExecStartFailureResolution::Pending => Err(OciError::InvalidConfig(format!(
                    "{context}; ambiguous guest start failure: {error}; reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"
                ))),
            }
        }
        Ok(Err(join_error)) => {
            let resolution = starting
                .resolve_unknown_start_failure_bounded(format!(
                    "{context}; startup cleanup task failed: {join_error}"
                ))
                .await;
            let reconciliation = match resolution {
                ExecStartFailureResolution::Reconciled(outcome) => outcome,
                ExecStartFailureResolution::Pending => "PENDING_UNDER_RETAINED_AUTHORITY",
                ExecStartFailureResolution::Definite => unreachable!(),
            };
            Err(OciError::InvalidConfig(format!(
                "{context}; startup cleanup task failed: {join_error}; reconciliation={reconciliation}"
            )))
        }
        Err(_) => {
            starting.retain_startup_cleanup(context.clone());
            Err(OciError::InvalidConfig(format!(
                "{context}; cleanup proof remains pending under retained background authority"
            )))
        }
    }
}

fn non_empty(value: Option<&str>) -> Option<String> {
    value
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

fn overlay_environment(target: &mut Vec<(String, String)>, overlay: &[(String, String)]) {
    for (key, value) in overlay {
        if let Some((_, existing_value)) = target.iter_mut().find(|(item, _)| item == key) {
            *existing_value = value.clone();
        } else {
            target.push((key.clone(), value.clone()));
        }
    }
}

/// Resolve one exec request against an immutable activation-time snapshot.
///
/// This is deliberately transport-independent: unary, streaming pipe, and PTY
/// execution all consume the same resolved values. Empty cwd/user fields mean
/// "omitted" and inherit the container defaults; `/` is used only when neither
/// the request nor the resolved container configuration supplies a cwd.
pub(super) fn resolve_container_exec_options(
    defaults: &ContainerExecDefaults,
    exec: &ExecConfig,
) -> ExecOptions {
    let mut env = Vec::with_capacity(defaults.env.len() + exec.env.len());
    overlay_environment(&mut env, &defaults.env);
    overlay_environment(&mut env, &exec.env);

    let working_dir = non_empty(exec.working_dir.as_deref())
        .or_else(|| non_empty(defaults.working_dir.as_deref()))
        .unwrap_or_else(|| "/".to_string());
    let user = non_empty(exec.user.as_deref()).or_else(|| non_empty(defaults.user.as_deref()));

    ExecOptions {
        working_dir: Some(working_dir),
        env,
        user,
    }
}

pub(super) fn resolve_container_exec_binding<V>(
    container_id: &str,
    binding: Option<&ContainerExecBinding<V>>,
    exec: &ExecConfig,
) -> Result<(Arc<V>, ExecOptions), OciError> {
    let binding = binding.ok_or_else(|| {
        OciError::InvalidConfig(format!(
            "no active exec binding for container '{container_id}'; container may not be running, may still be activating, or its activation invariant was violated"
        ))
    })?;
    Ok((
        Arc::clone(&binding.vm),
        resolve_container_exec_options(&binding.defaults, exec),
    ))
}

fn exec_control_debug_enabled() -> bool {
    std::env::var("VZ_OCI_EXEC_CONTROL_DEBUG")
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn kernel_object_identity(identity: vz_linux::KernelObjectIdentity) -> KernelObjectIdentity {
    KernelObjectIdentity {
        device: identity.device,
        inode: identity.inode,
    }
}

pub(super) fn container_ready_generation(
    expected_container_id: &str,
    lifecycle_generation: ContainerGeneration,
    guest: vz_linux::ContainerGeneration,
) -> Result<ContainerReadyGeneration, OciError> {
    if guest.container_id != expected_container_id {
        return Err(OciError::InvalidConfig(format!(
            "container readiness identity mismatch: requested '{expected_container_id}', guest acknowledged '{}'",
            guest.container_id
        )));
    }
    let cgroup = guest.cgroup.ok_or_else(|| {
        OciError::InvalidConfig("container readiness omitted cgroup identity".to_string())
    })?;
    let namespaces = guest.namespaces.ok_or_else(|| {
        OciError::InvalidConfig("container readiness omitted namespace identities".to_string())
    })?;
    let required = |name: &str, identity: Option<vz_linux::KernelObjectIdentity>| {
        identity.map(kernel_object_identity).ok_or_else(|| {
            OciError::InvalidConfig(format!("container readiness omitted {name} identity"))
        })
    };
    Ok(ContainerReadyGeneration {
        lifecycle_generation: lifecycle_generation.0,
        container_id: guest.container_id,
        init_pid: guest.init_pid,
        init_start_time: guest.init_start_time,
        cgroup_path: guest.cgroup_path,
        cgroup: kernel_object_identity(cgroup),
        namespaces: ContainerNamespaceIdentity {
            mount: required("mount namespace", namespaces.mount)?,
            network: required("network namespace", namespaces.network)?,
            pid: required("PID namespace", namespaces.pid)?,
            ipc: required("IPC namespace", namespaces.ipc)?,
            uts: required("UTS namespace", namespaces.uts)?,
        },
        root: required("root", guest.root)?,
    })
}

impl Runtime {
    async fn register_exec_session(
        &self,
        execution_id: Option<&str>,
        vm: Arc<LinuxVm>,
        pty_enabled: bool,
    ) -> Result<Option<ExecSessionRegistration>, OciError> {
        let Some(execution_id) = execution_id else {
            return Ok(None);
        };
        if execution_id.trim().is_empty() {
            return Err(OciError::InvalidConfig(
                "execution_id must not be empty".to_string(),
            ));
        }

        let session = ContainerExecSession::new(vm, pty_enabled);
        let mut registry = self.exec_sessions.lock().await;
        if registry.contains_key(execution_id) {
            return Err(OciError::InvalidConfig(format!(
                "execution session '{execution_id}' is already active"
            )));
        }
        registry.insert(execution_id.to_string(), session.clone());
        drop(registry);
        Ok(Some(ExecSessionRegistration {
            execution_id: execution_id.to_string(),
            session,
            registry: Arc::clone(&self.exec_sessions),
            armed: true,
        }))
    }

    async fn resolve_exec_binding(
        &self,
        container_id: &str,
        exec: &ExecConfig,
    ) -> Result<(Arc<LinuxVm>, ExecOptions, ContainerGeneration), OciError> {
        let binding = self
            .container_exec_bindings
            .lock()
            .await
            .get(container_id)
            .cloned();
        let generation = match binding.as_ref() {
            Some(binding) => binding.generation,
            None => {
                return if self
                    .container_store
                    .find(container_id)
                    .map_err(|error| Self::map_container_store_error(container_id, error))?
                    .is_some()
                {
                    Err(OciError::InvalidConfig(format!(
                        "container '{container_id}' has no active exec binding and may not be running"
                    )))
                } else {
                    Err(OciError::ContainerNotFound {
                        id: container_id.to_string(),
                    })
                };
            }
        };
        let current_generation = self
            .container_store
            .current_generation(container_id)
            .map_err(|error| Self::map_container_store_error(container_id, error))?;
        if current_generation != Some(generation) {
            return Err(OciError::ContainerNotFound {
                id: container_id.to_string(),
            });
        }
        let (vm, options) = resolve_container_exec_binding(container_id, binding.as_ref(), exec)?;
        Ok((vm, options, generation))
    }

    pub async fn exec_container(&self, id: &str, exec: ExecConfig) -> Result<ExecOutput, OciError> {
        self.exec_container_streaming(id, exec, |_| {}).await
    }

    /// Execute only while `ownership` remains the exact published generation.
    ///
    /// The durable generation check and exec dispatch share one lifecycle read
    /// admission, so replacement or removal cannot cross the authorization
    /// boundary between validation and guest RPC dispatch.
    pub async fn exec_owned_container_generation(
        &self,
        ownership: &vz_runtime_contract::ContainerGenerationOwnership,
        exec: ExecConfig,
    ) -> Result<ExecOutput, OciError> {
        ownership.validate().map_err(OciError::InvalidConfig)?;
        let admission_guard = self
            .acquire_container_read_admission(&ownership.container_id)
            .await?;
        match self.inspect_scoped_container_generation(ownership)? {
            vz_runtime_contract::ContainerGenerationInspection::Published(found)
                if found == *ownership => {}
            other => {
                return Err(OciError::ContainerOwnershipMismatch {
                    id: ownership.container_id.clone(),
                    reason: format!(
                        "exact generation is not the current published owner: {other:?}"
                    ),
                });
            }
        }
        self.exec_container_streaming_admitted(
            &ownership.container_id,
            exec,
            |_| {},
            Some(admission_guard),
            Some(ContainerGeneration(ownership.generation)),
        )
        .await
    }

    /// Execute a command inside an already-running container and emit
    /// incremental output events when available.
    pub async fn exec_container_streaming<F>(
        &self,
        id: &str,
        exec: ExecConfig,
        on_event: F,
    ) -> Result<ExecOutput, OciError>
    where
        F: FnMut(InteractiveExecEvent),
    {
        let admission_guard = self.acquire_container_read_admission(id).await?;
        self.exec_container_streaming_admitted(id, exec, on_event, Some(admission_guard), None)
            .await
    }

    pub(crate) async fn exec_container_in_transaction(
        &self,
        id: &str,
        exec: ExecConfig,
        transaction: &ContainerLifecycleTransaction,
    ) -> Result<ExecOutput, OciError> {
        debug_assert_eq!(id, transaction.container_id());
        self.exec_container_streaming_admitted(
            id,
            exec,
            |_| {},
            None,
            Some(transaction.generation()),
        )
        .await
    }

    async fn exec_container_streaming_admitted<F>(
        &self,
        id: &str,
        exec: ExecConfig,
        mut on_event: F,
        mut admission_guard: Option<ContainerReadAdmission>,
        expected_generation: Option<ContainerGeneration>,
    ) -> Result<ExecOutput, OciError>
    where
        F: FnMut(InteractiveExecEvent),
    {
        let debug = exec_control_debug_enabled();
        let (command, args) = exec
            .cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("exec command must not be empty".to_string()))?;

        let timeout = exec.timeout.unwrap_or(self.config.exec_timeout);
        let deadline = tokio::time::Instant::now() + timeout;
        let execution_id = exec.execution_id.clone();
        let (vm, options, lifecycle_generation) = self.resolve_exec_binding(id, &exec).await?;
        if let Some(expected_generation) = expected_generation
            && expected_generation != lifecycle_generation
        {
            return Err(OciError::ContainerOwnershipMismatch {
                id: id.to_string(),
                reason: format!(
                    "exec binding generation {} does not match authorized generation {}",
                    lifecycle_generation.0, expected_generation.0,
                ),
            });
        }
        let mut registration = self
            .register_exec_session(execution_id.as_deref(), Arc::clone(&vm), exec.pty)
            .await?;
        let dispatch_gate = ContainerExecDispatchGate::new(deadline);
        if let Some(active) = registration.as_ref() {
            active
                .session
                .install_dispatch_gate(dispatch_gate.clone())
                .await;
        }
        if admission_guard.is_some() {
            self.observe_lifecycle_admission(
                super::RuntimeLifecycleAdmissionKind::ExecBeforeGuestRpc,
                id,
            )
            .await;
        }

        // Cancellation may be queued while the deterministic pre-RPC
        // admission point is paused. Consume it before allocating a request ID
        // or dispatching any guest work, so the cancel waiter receives exact
        // terminal proof without creating a process that then needs reaping.
        if let Some(active) = registration.as_mut()
            && active.session.start_cancel_requested().await
        {
            let context = format!(
                "exec '{}' was cancelled during startup",
                execution_id.as_deref().unwrap_or("unaddressed")
            );
            active.finish().await;
            drop(admission_guard.take());
            return Err(OciError::InvalidConfig(context));
        }

        if exec.pty {
            let execution_id = match execution_id {
                Some(execution_id) => execution_id,
                None => {
                    return Err(OciError::ExecutionControlUnsupported {
                        operation: "exec_container".to_string(),
                        reason: "interactive exec requires execution_id".to_string(),
                    });
                }
            };

            let term_rows = u32::from(exec.term_rows.unwrap_or(DEFAULT_INTERACTIVE_EXEC_ROWS));
            let term_cols = u32::from(exec.term_cols.unwrap_or(DEFAULT_INTERACTIVE_EXEC_COLS));
            let vm_key = Arc::as_ptr(&vm) as usize;
            let should_prepare_pty = {
                let mut prepared = self.interactive_pty_prep_vms.lock().await;
                prepared.insert(vm_key)
            };

            if should_prepare_pty {
                if debug {
                    debug!(
                        "[vz-oci-macos exec-control] interactive exec preparing pty prerequisites execution_id={execution_id} timeout_secs={:.3}",
                        timeout.as_secs_f64()
                    );
                }
                ensure_interactive_exec_pty_prerequisites(vm.as_ref(), timeout).await;
                if debug {
                    debug!(
                        "[vz-oci-macos exec-control] interactive exec prerequisite step complete execution_id={execution_id}"
                    );
                }
            } else if debug {
                debug!(
                    "[vz-oci-macos exec-control] interactive exec skipping pty prerequisite step execution_id={execution_id}"
                );
            }

            if debug {
                debug!(
                    "[vz-oci-macos exec-control] interactive exec invoking guest exec RPC execution_id={execution_id} command={:?} args={:?} rows={} cols={}",
                    command, args, term_rows, term_cols
                );
            }
            let start_vm = Arc::clone(&vm);
            let request_id = prepare_exec_request_before_dispatch(
                vm.as_ref(),
                &mut registration,
                &mut admission_guard,
                Some(&execution_id),
                deadline,
                timeout,
            )
            .await?;
            let start_dispatch_gate = dispatch_gate.clone();
            let start_request_id = request_id.clone();
            let start_container_id = id.to_string();
            let start_command = command.clone();
            let start_args = args.to_vec();
            let start_runtime = self.clone();
            let start_task = tokio::spawn(async move {
                let arg_refs: Vec<&str> = start_args.iter().map(String::as_str).collect();
                let result = start_vm
                    .exec_container_interactive_ready_classified_for_request(
                        start_dispatch_gate,
                        start_request_id,
                        start_container_id.clone(),
                        &start_command,
                        &arg_refs,
                        options,
                        term_rows,
                        term_cols,
                    )
                    .await;
                if result.is_ok() {
                    start_runtime
                        .observe_lifecycle_admission(
                            super::RuntimeLifecycleAdmissionKind::ExecGuestRpcReadyBeforeOwner,
                            &start_container_id,
                        )
                        .await;
                }
                result
            });
            let mut starting = StartingExecLease::new(
                start_task,
                Arc::clone(&vm),
                request_id,
                dispatch_gate.clone(),
                registration.take(),
                admission_guard.take(),
            );
            let cancel_session = starting.cancel_session();
            let start_result = if let Some(session) = cancel_session {
                await_exec_start(
                    starting.task_mut(),
                    deadline,
                    session.wait_start_cancel_requested(),
                    &dispatch_gate,
                )
                .await
            } else {
                await_exec_start(
                    starting.task_mut(),
                    deadline,
                    std::future::pending(),
                    &dispatch_gate,
                )
                .await
            };
            let (stream, guest_exec_id, guest_generation) = match start_result {
                Ok(Ok(Ok(ready))) => ready,
                Ok(Ok(Err(failure))) => {
                    let (error, reconciliation) = resolve_exec_start_failure(
                        &mut starting,
                        failure,
                        "interactive exec startup failed".to_string(),
                    )
                    .await;
                    match reconciliation {
                        ExecStartFailureResolution::Definite => return Err(error.into()),
                        ExecStartFailureResolution::Reconciled(outcome) => {
                            return Err(OciError::InvalidConfig(format!(
                                "interactive exec startup failed: {error}; reconciliation={outcome}; lifecycle authority released after proof"
                            )));
                        }
                        ExecStartFailureResolution::Pending => {
                            return Err(OciError::InvalidConfig(format!(
                                "interactive exec startup failed: {error}; reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"
                            )));
                        }
                    }
                }
                Ok(Err(join_error)) => {
                    let resolution = starting
                        .resolve_unknown_start_failure_bounded(format!(
                            "exec startup task failed: {join_error}"
                        ))
                        .await;
                    let reconciliation = match resolution {
                        ExecStartFailureResolution::Reconciled(outcome) => outcome,
                        ExecStartFailureResolution::Pending => "PENDING_UNDER_RETAINED_AUTHORITY",
                        ExecStartFailureResolution::Definite => unreachable!(),
                    };
                    return Err(OciError::InvalidConfig(format!(
                        "exec startup task failed: {join_error}; reconciliation={reconciliation}"
                    )));
                }
                Err(interruption) => {
                    let context = match interruption {
                        ExecStartInterruption::Cancelled => {
                            format!("exec '{execution_id}' was cancelled during startup")
                        }
                        ExecStartInterruption::TimedOut => {
                            format!(
                                "exec timed out after {:.3}s during startup",
                                timeout.as_secs_f64()
                            )
                        }
                    };
                    finish_interrupted_exec_start(&mut starting, context.clone()).await?;
                    return Err(OciError::InvalidConfig(context));
                }
            };
            let mut lease = starting.promote(stream, guest_exec_id);
            if let Some(active_registration) = lease.registration()
                && let Err(error) = active_registration.session.bind_guest(guest_exec_id).await
            {
                lease
                    .cleanup_failure(format!(
                        "container exec control registration failed: {error}"
                    ))
                    .await?;
                return Err(error);
            }
            let ready = match container_ready_generation(id, lifecycle_generation, guest_generation)
            {
                Ok(ready) => ready,
                Err(error) => {
                    lease
                        .cleanup_failure(format!(
                            "container exec readiness validation failed: {error}"
                        ))
                        .await?;
                    return Err(error);
                }
            };
            if lease.has_admission() {
                self.observe_lifecycle_admission(
                    super::RuntimeLifecycleAdmissionKind::ExecGuestReady,
                    id,
                )
                .await;
            }
            if debug {
                debug!(
                    "[vz-oci-macos exec-control] interactive exec guest exec RPC ready execution_id={execution_id} guest_exec_id={guest_exec_id}"
                );
            }

            // Publish readiness only after control-plane registration, and never
            // invoke caller code while holding host lifecycle admission.
            lease.release_admission();
            on_event(InteractiveExecEvent::ContainerReady(ready));

            let stream_result = tokio::time::timeout_at(deadline, async {
                let stream = lease.stream_mut();
                let mut stdout = Vec::new();
                let mut stderr = Vec::new();
                let mut saw_exit = false;
                let mut exit_code = -1;

                loop {
                    match stream.next_checked().await {
                        Ok(Some(event)) => match event {
                            ExecEvent::Stdout(data) => {
                                on_event(InteractiveExecEvent::Stdout(data.clone()));
                                stdout.extend_from_slice(&data);
                            }
                            ExecEvent::Stderr(data) => {
                                on_event(InteractiveExecEvent::Stderr(data.clone()));
                                stderr.extend_from_slice(&data);
                            }
                            ExecEvent::Exit(code) => {
                                on_event(InteractiveExecEvent::Exit(code));
                                saw_exit = true;
                                exit_code = code;
                                break;
                            }
                        },
                        Ok(None) => break,
                        Err(error) => return Err(error.into()),
                    }
                }

                if !saw_exit {
                    return Err(OciError::InvalidConfig(
                        "interactive exec stream ended without exit code".to_string(),
                    ));
                }

                Ok(ExecOutput {
                    exit_code,
                    stdout: String::from_utf8_lossy(&stdout).into_owned(),
                    stderr: String::from_utf8_lossy(&stderr).into_owned(),
                })
            })
            .await;

            let output = match stream_result {
                Ok(Ok(output)) => output,
                Ok(Err(error)) => {
                    let context = format!("interactive exec stream failed: {error}");
                    lease.cleanup_failure(context).await?;
                    return Err(error);
                }
                Err(_) => {
                    let context = format!("exec timed out after {:.3}s", timeout.as_secs_f64());
                    lease.cleanup_failure(context.clone()).await?;
                    return Err(OciError::InvalidConfig(context));
                }
            };

            lease.complete().await;
            return Ok(output);
        }

        let start_vm = Arc::clone(&vm);
        let request_id = prepare_exec_request_before_dispatch(
            vm.as_ref(),
            &mut registration,
            &mut admission_guard,
            execution_id.as_deref(),
            deadline,
            timeout,
        )
        .await?;
        let start_dispatch_gate = dispatch_gate.clone();
        let start_request_id = request_id.clone();
        let start_container_id = id.to_string();
        let start_command = command.clone();
        let start_args = args.to_vec();
        let start_runtime = self.clone();
        let start_task = tokio::spawn(async move {
            let result = start_vm
                .exec_container_stream_ready_classified_for_request(
                    start_dispatch_gate,
                    start_request_id,
                    start_container_id.clone(),
                    start_command,
                    start_args,
                    options,
                )
                .await;
            if result.is_ok() {
                start_runtime
                    .observe_lifecycle_admission(
                        super::RuntimeLifecycleAdmissionKind::ExecGuestRpcReadyBeforeOwner,
                        &start_container_id,
                    )
                    .await;
            }
            result
        });
        let mut starting = StartingExecLease::new(
            start_task,
            Arc::clone(&vm),
            request_id,
            dispatch_gate.clone(),
            registration.take(),
            admission_guard.take(),
        );
        let cancel_session = starting.cancel_session();
        let start_result = if let Some(session) = cancel_session {
            await_exec_start(
                starting.task_mut(),
                deadline,
                session.wait_start_cancel_requested(),
                &dispatch_gate,
            )
            .await
        } else {
            await_exec_start(
                starting.task_mut(),
                deadline,
                std::future::pending(),
                &dispatch_gate,
            )
            .await
        };
        let (stream, guest_exec_id, guest_generation) = match start_result {
            Ok(Ok(Ok(ready))) => ready,
            Ok(Ok(Err(failure))) => {
                let (error, reconciliation) = resolve_exec_start_failure(
                    &mut starting,
                    failure,
                    "container exec startup failed".to_string(),
                )
                .await;
                match reconciliation {
                    ExecStartFailureResolution::Definite => return Err(error.into()),
                    ExecStartFailureResolution::Reconciled(outcome) => {
                        return Err(OciError::InvalidConfig(format!(
                            "container exec startup failed: {error}; reconciliation={outcome}; lifecycle authority released after proof"
                        )));
                    }
                    ExecStartFailureResolution::Pending => {
                        return Err(OciError::InvalidConfig(format!(
                            "container exec startup failed: {error}; reconciliation=PENDING_UNDER_RETAINED_AUTHORITY"
                        )));
                    }
                }
            }
            Ok(Err(join_error)) => {
                let resolution = starting
                    .resolve_unknown_start_failure_bounded(format!(
                        "exec startup task failed: {join_error}"
                    ))
                    .await;
                let reconciliation = match resolution {
                    ExecStartFailureResolution::Reconciled(outcome) => outcome,
                    ExecStartFailureResolution::Pending => "PENDING_UNDER_RETAINED_AUTHORITY",
                    ExecStartFailureResolution::Definite => unreachable!(),
                };
                return Err(OciError::InvalidConfig(format!(
                    "exec startup task failed: {join_error}; reconciliation={reconciliation}"
                )));
            }
            Err(interruption) => {
                let context = match interruption {
                    ExecStartInterruption::Cancelled => format!(
                        "exec '{}' was cancelled during startup",
                        execution_id.as_deref().unwrap_or("unaddressed")
                    ),
                    ExecStartInterruption::TimedOut => format!(
                        "exec timed out after {:.3}s during startup",
                        timeout.as_secs_f64()
                    ),
                };
                finish_interrupted_exec_start(&mut starting, context.clone()).await?;
                return Err(OciError::InvalidConfig(context));
            }
        };
        let mut lease = starting.promote(stream, guest_exec_id);
        if let Some(active_registration) = lease.registration()
            && let Err(error) = active_registration.session.bind_guest(guest_exec_id).await
        {
            lease
                .cleanup_failure(format!(
                    "container exec control registration failed: {error}"
                ))
                .await?;
            return Err(error);
        }
        let ready = match container_ready_generation(id, lifecycle_generation, guest_generation) {
            Ok(ready) => ready,
            Err(error) => {
                lease
                    .cleanup_failure(format!(
                        "container exec readiness validation failed: {error}"
                    ))
                    .await?;
                return Err(error);
            }
        };
        if lease.has_admission() {
            self.observe_lifecycle_admission(
                super::RuntimeLifecycleAdmissionKind::ExecGuestReady,
                id,
            )
            .await;
        }
        lease.release_admission();
        on_event(InteractiveExecEvent::ContainerReady(ready));

        let stream_result = tokio::time::timeout_at(deadline, async {
            let stream = lease.stream_mut();
            let mut stdout = Vec::new();
            let mut stderr = Vec::new();
            let mut saw_exit = false;
            let mut exit_code = -1;
            loop {
                match stream.next_checked().await {
                    Ok(Some(event)) => match event {
                        ExecEvent::Stdout(data) => {
                            on_event(InteractiveExecEvent::Stdout(data.clone()));
                            stdout.extend_from_slice(&data);
                        }
                        ExecEvent::Stderr(data) => {
                            on_event(InteractiveExecEvent::Stderr(data.clone()));
                            stderr.extend_from_slice(&data);
                        }
                        ExecEvent::Exit(code) => {
                            on_event(InteractiveExecEvent::Exit(code));
                            saw_exit = true;
                            exit_code = code;
                            break;
                        }
                    },
                    Ok(None) => break,
                    Err(error) => return Err(error.into()),
                }
            }
            if !saw_exit {
                return Err(OciError::InvalidConfig(
                    "container exec stream ended without exit code".to_string(),
                ));
            }
            Ok(ExecOutput {
                exit_code,
                stdout: String::from_utf8_lossy(&stdout).into_owned(),
                stderr: String::from_utf8_lossy(&stderr).into_owned(),
            })
        })
        .await;

        let output = match stream_result {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                let context = format!("container exec stream failed: {error}");
                lease.cleanup_failure(context).await?;
                return Err(error);
            }
            Err(_) => {
                let context = format!("exec timed out after {:.3}s", timeout.as_secs_f64());
                lease.cleanup_failure(context.clone()).await?;
                return Err(OciError::InvalidConfig(context));
            }
        };
        lease.complete().await;
        Ok(output)
    }

    /// Execute through the bounded OCI unary compatibility RPC.
    #[doc(hidden)]
    pub async fn exec_container_oci_unary(
        &self,
        id: &str,
        exec: ExecConfig,
    ) -> Result<ExecOutput, OciError> {
        if exec.pty {
            return Err(OciError::ExecutionControlUnsupported {
                operation: "exec_container_oci_unary".to_string(),
                reason: "OCI unary exec does not support PTY allocation".to_string(),
            });
        }
        let admission_guard = self.acquire_container_read_admission(id).await?;
        self.exec_container_streaming_admitted(id, exec, |_| {}, Some(admission_guard), None)
            .await
    }

    /// Write stdin bytes into an active interactive execution session.
    pub async fn write_exec_stdin(&self, execution_id: &str, data: &[u8]) -> Result<(), OciError> {
        let debug = exec_control_debug_enabled();
        if debug {
            debug!(
                "[vz-oci-macos exec-control] write_exec_stdin start execution_id={execution_id} bytes={}",
                data.len()
            );
        }
        let session = self.require_exec_session(execution_id).await?;
        if !session.pty_enabled {
            return Err(OciError::ExecutionControlUnsupported {
                operation: "write_exec_stdin".to_string(),
                reason: "execution session is not interactive".to_string(),
            });
        }
        let write_result = session.write_stdin(data, execution_id).await;
        if debug {
            match &write_result {
                Ok(()) => debug!(
                    "[vz-oci-macos exec-control] write_exec_stdin complete execution_id={execution_id}"
                ),
                Err(error) => debug!(
                    "[vz-oci-macos exec-control] write_exec_stdin failed execution_id={execution_id} error={error}"
                ),
            }
        }
        write_result
    }

    /// Send a signal into an active interactive execution session.
    pub async fn signal_exec(&self, execution_id: &str, signal: &str) -> Result<(), OciError> {
        let session = self.require_exec_session(execution_id).await?;
        let Some(signal_num) = parse_signal_number(signal) else {
            return Err(OciError::InvalidConfig(format!(
                "unsupported signal '{signal}'"
            )));
        };
        session.signal(signal_num, execution_id).await
    }

    /// Resize PTY dimensions for an active interactive execution session.
    pub async fn resize_exec_pty(
        &self,
        execution_id: &str,
        cols: u16,
        rows: u16,
    ) -> Result<(), OciError> {
        let session = self.require_exec_session(execution_id).await?;
        if !session.pty_enabled {
            return Err(OciError::ExecutionControlUnsupported {
                operation: "resize_exec_pty".to_string(),
                reason: "execution session has no PTY".to_string(),
            });
        }
        session
            .resize_pty(u32::from(rows), u32::from(cols), execution_id)
            .await
    }

    /// Cancel an active execution session and await guest terminal/reap proof.
    pub async fn cancel_exec(&self, execution_id: &str) -> Result<(), OciError> {
        let session = self.require_exec_session(execution_id).await?;
        session.cancel(execution_id).await
    }

    async fn require_exec_session(
        &self,
        execution_id: &str,
    ) -> Result<ContainerExecSession, OciError> {
        self.exec_sessions
            .lock()
            .await
            .get(execution_id)
            .cloned()
            .ok_or_else(|| OciError::ExecutionSessionNotFound {
                execution_id: execution_id.to_string(),
            })
    }

    /// Execute a command at the VM level (not inside a container namespace).
    ///
    /// Uses the guest agent's direct exec path. This works even
    /// when the container's init process has exited, making it suitable for
    /// reading logs from the VM-level log directory.
    pub async fn exec_host(
        &self,
        container_id: &str,
        exec: ExecConfig,
    ) -> Result<ExecOutput, OciError> {
        let vm = self
            .vm_handles
            .lock()
            .await
            .get(container_id)
            .cloned()
            .ok_or_else(|| {
                OciError::InvalidConfig(format!(
                    "no active VM handle for container '{container_id}'"
                ))
            })?;

        let (command, args) = exec
            .cmd
            .split_first()
            .ok_or_else(|| OciError::InvalidConfig("exec command must not be empty".to_string()))?;

        let timeout = exec.timeout.unwrap_or(self.config.exec_timeout);

        let result = vm
            .exec_collect(command.clone(), args.to_vec(), timeout)
            .await
            .map_err(OciError::from)?;

        Ok(ExecOutput {
            exit_code: result.exit_code,
            stdout: result.stdout,
            stderr: result.stderr,
        })
    }
}
