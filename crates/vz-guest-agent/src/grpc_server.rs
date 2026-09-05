//! gRPC service implementations for the guest agent.
//!
//! Each service struct holds shared state (process table, etc.) and delegates
//! to the existing handler logic in the parent module. This bridges from
//! protobuf request/response types to the underlying handler functions.

// tonic::Status is the canonical error type for all gRPC service methods;
// its size is dictated by the tonic crate and cannot be reduced here.
#![allow(clippy::result_large_err)]

use std::collections::HashMap;
#[cfg(any(target_os = "linux", test))]
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

#[cfg(target_os = "linux")]
use tracing::error;

use vz_agent_proto::*;

#[cfg(target_os = "linux")]
use crate::process_table::SpawnedProcessIdentity;
use crate::process_table::{ExecCancellation, ExecTerminalReceipt, ProcessIdentity, ProcessTable};
use crate::process_table::{ExecRequestClaimError, ExecRequestPermit, ExecRequestReconcile};

const EXEC_CANCEL_GRACE: std::time::Duration = std::time::Duration::from_secs(2);
const EXEC_CANCEL_REAP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const EXEC_CANCEL_DRIVER_RETRY: std::time::Duration = std::time::Duration::from_millis(250);
const PTY_READER_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);
const MAX_STDIN_WRITE_BYTES: usize = 1024 * 1024;
const STDIN_WRITE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

#[cfg(target_os = "linux")]
const CONTAINER_READY_ROOT: &str = "/run/vz-agent-exec";
#[cfg(target_os = "linux")]
const CONTAINER_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[cfg(target_os = "linux")]
struct ContainerReadyListener {
    listener: tokio::net::UnixListener,
    path: PathBuf,
    challenge: String,
}

#[cfg(target_os = "linux")]
impl ContainerReadyListener {
    fn bind() -> Result<Self, Status> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        std::fs::create_dir_all(CONTAINER_READY_ROOT).map_err(|error| {
            Status::internal(format!("cannot create exec-ready directory: {error}"))
        })?;
        let metadata = std::fs::symlink_metadata(CONTAINER_READY_ROOT).map_err(|error| {
            Status::internal(format!("cannot inspect exec-ready directory: {error}"))
        })?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() || metadata.uid() != 0 {
            return Err(Status::permission_denied(
                "exec-ready directory is not a root-owned real directory",
            ));
        }
        std::fs::set_permissions(CONTAINER_READY_ROOT, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| {
                Status::internal(format!("cannot secure exec-ready directory: {error}"))
            })?;
        let challenge = random_ready_challenge()?;
        let path = Path::new(CONTAINER_READY_ROOT).join(format!("{challenge}.sock"));
        let listener = std::os::unix::net::UnixListener::bind(&path)
            .map_err(|error| Status::internal(format!("cannot bind exec-ready socket: {error}")))?;
        listener.set_nonblocking(true).map_err(|error| {
            Status::internal(format!("cannot configure exec-ready socket: {error}"))
        })?;
        let listener = tokio::net::UnixListener::from_std(listener).map_err(|error| {
            Status::internal(format!("cannot adopt exec-ready socket: {error}"))
        })?;
        Ok(Self {
            listener,
            path,
            challenge,
        })
    }

    fn challenge(&self) -> &str {
        &self.challenge
    }

    fn endpoint(&self) -> Result<(&str, &str), Status> {
        let path = self
            .path
            .to_str()
            .ok_or_else(|| Status::internal("exec-ready socket path is not UTF-8"))?;
        Ok((path, self.challenge()))
    }

    async fn wait(
        &self,
        expected_process: &SpawnedProcessIdentity,
        expected_container_id: &str,
    ) -> Result<ContainerGeneration, Status> {
        use tokio::io::AsyncReadExt as _;

        let deadline = tokio::time::Instant::now() + CONTAINER_READY_TIMEOUT;
        let operation = async {
            let (stream, _) =
                self.listener.accept().await.map_err(|error| {
                    Status::internal(format!("exec-ready accept failed: {error}"))
                })?;
            let credentials = stream.peer_cred().map_err(|error| {
                Status::internal(format!("exec-ready peer credentials failed: {error}"))
            })?;
            if credentials.pid() != Some(expected_process.pid() as i32) || credentials.uid() != 0 {
                return Err(Status::permission_denied(
                    "container exec readiness came from an unexpected process",
                ));
            }
            expected_process.ensure_same_generation().map_err(|error| {
                Status::failed_precondition(format!(
                    "container exec readiness sender changed: {error}"
                ))
            })?;
            let mut bytes = Vec::new();
            stream
                .take(4097)
                .read_to_end(&mut bytes)
                .await
                .map_err(|error| Status::internal(format!("exec-ready read failed: {error}")))?;
            let identity = crate::container_exec::decode_ready_identity(
                &bytes,
                &self.challenge,
                expected_process.start_time(),
            )
            .map_err(|error| Status::failed_precondition(error.to_string()))?;
            if identity.container_id != expected_container_id {
                return Err(Status::failed_precondition(
                    "container exec readiness named a different target",
                ));
            }
            Ok(container_generation(identity))
        };
        enforce_ready_deadline(deadline, operation).await
    }

    async fn wait_machine(&self, expected: &SpawnedProcessIdentity) -> Result<(), Status> {
        use tokio::io::AsyncReadExt as _;
        enforce_ready_deadline(
            tokio::time::Instant::now() + CONTAINER_READY_TIMEOUT,
            async {
                let (stream, _) =
                    self.listener.accept().await.map_err(|error| {
                        Status::internal(format!("Machine ready accept: {error}"))
                    })?;
                let credentials = stream.peer_cred().map_err(|error| {
                    Status::internal(format!("Machine ready credentials: {error}"))
                })?;
                if credentials.pid() != Some(expected.pid() as i32) || credentials.uid() != 0 {
                    return Err(Status::permission_denied(
                        "Machine readiness sender is not the pinned supervisor",
                    ));
                }
                expected.ensure_same_generation().map_err(|error| {
                    Status::failed_precondition(format!("Machine supervisor changed: {error}"))
                })?;
                let mut bytes = Vec::new();
                stream
                    .take(4097)
                    .read_to_end(&mut bytes)
                    .await
                    .map_err(|error| Status::internal(format!("Machine ready read: {error}")))?;
                crate::container_exec::machine::decode_ready(
                    &bytes,
                    &self.challenge,
                    expected.start_time(),
                )
                .map_err(|error| Status::failed_precondition(error.to_string()))
            },
        )
        .await
    }
}

#[cfg(any(target_os = "linux", test))]
async fn enforce_ready_deadline<T>(
    deadline: tokio::time::Instant,
    operation: impl std::future::Future<Output = Result<T, Status>>,
) -> Result<T, Status> {
    tokio::time::timeout_at(deadline, operation)
        .await
        .map_err(|_| Status::deadline_exceeded("timed out waiting for container exec readiness"))?
}

#[cfg(target_os = "linux")]
fn random_ready_challenge() -> Result<String, Status> {
    let mut bytes = [0_u8; 32];
    let mut used = 0;
    while used < bytes.len() {
        // SAFETY: the remaining byte slice is writable for the requested
        // length and getrandom does not retain its pointer.
        let result =
            unsafe { libc::getrandom(bytes[used..].as_mut_ptr().cast(), bytes.len() - used, 0) };
        if result > 0 {
            used += result as usize;
            continue;
        }
        if result == 0 {
            return Err(Status::internal(
                "cannot generate exec-ready challenge: getrandom returned zero bytes",
            ));
        }
        if result < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(Status::internal(format!(
            "cannot generate exec-ready challenge: {}",
            std::io::Error::last_os_error()
        )));
    }
    Ok(hex_ready_challenge(bytes))
}

#[cfg(any(target_os = "linux", test))]
fn hex_ready_challenge(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(64);
    for byte in bytes {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(target_os = "linux")]
impl Drop for ContainerReadyListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[cfg(target_os = "linux")]
fn kernel_object((device, inode): (u64, u64)) -> KernelObjectIdentity {
    KernelObjectIdentity { device, inode }
}

#[cfg(target_os = "linux")]
fn container_generation(
    identity: crate::container_exec::ContainerReadyIdentity,
) -> ContainerGeneration {
    ContainerGeneration {
        container_id: identity.container_id,
        init_pid: identity.pid,
        init_start_time: identity.start_time,
        cgroup_path: identity.cgroup_path,
        cgroup: Some(kernel_object(identity.cgroup)),
        namespaces: Some(ContainerNamespaceIdentity {
            mount: Some(kernel_object(identity.namespaces[0])),
            network: Some(kernel_object(identity.namespaces[1])),
            pid: Some(kernel_object(identity.namespaces[2])),
            ipc: Some(kernel_object(identity.namespaces[3])),
            uts: Some(kernel_object(identity.namespaces[4])),
        }),
        root: Some(kernel_object(identity.root)),
    }
}

// ── PTY handle tracking ─────────────────────────────────────────

/// Owns a newly-spawned pipe child until it is registered in ProcessTable.
///
/// Every ordinary error path transfers the child to durable cleanup before
/// awaiting bounded reap proof. Drop is the cancellation backstop: kill-on-drop
/// alone is not enough because it does not prove the trampoline was reaped.
trait PendingPipeProcess: Send + 'static {
    fn process_id(&self) -> Option<u32>;
    fn start_kill(&mut self) -> std::io::Result<()>;
    fn try_wait_reaped(&mut self) -> std::io::Result<bool>;
}

impl PendingPipeProcess for tokio::process::Child {
    fn process_id(&self) -> Option<u32> {
        self.id()
    }

    fn start_kill(&mut self) -> std::io::Result<()> {
        self.start_kill()
    }

    fn try_wait_reaped(&mut self) -> std::io::Result<bool> {
        self.try_wait().map(|status| status.is_some())
    }
}

struct PendingPipeChild<C: PendingPipeProcess = tokio::process::Child> {
    child: Option<C>,
    request_permit: RetainedExecRequestPermit,
}

impl<C: PendingPipeProcess> PendingPipeChild<C> {
    #[cfg(test)]
    fn new(child: C) -> Self {
        Self::with_request_permit(child, None)
    }

    fn with_request_permit(child: C, request_permit: Option<ExecRequestPermit>) -> Self {
        Self {
            child: Some(child),
            request_permit: RetainedExecRequestPermit {
                permit: request_permit,
                preserve_supervisor: false,
            },
        }
    }

    async fn terminate_and_reap_with_timeout(
        mut self,
        timeout: std::time::Duration,
        retry: std::time::Duration,
    ) -> PendingChildCleanupOutcome {
        let Some(child) = self.child.take() else {
            return PendingChildCleanupOutcome::Missing;
        };
        let (proof_tx, proof_rx) = tokio::sync::oneshot::channel();
        retain_pending_pipe_cleanup(
            child,
            std::mem::take(&mut self.request_permit),
            Some(proof_tx),
            retry,
        );
        match tokio::time::timeout(timeout, proof_rx).await {
            Ok(Ok(())) => PendingChildCleanupOutcome::Reaped,
            Ok(Err(_)) | Err(_) => PendingChildCleanupOutcome::Retained,
        }
    }
}

impl PendingPipeChild<tokio::process::Child> {
    fn child_mut(&mut self) -> Result<&mut tokio::process::Child, Status> {
        self.child
            .as_mut()
            .ok_or_else(|| Status::internal("pending pipe child is missing"))
    }

    fn take(mut self) -> Result<(tokio::process::Child, RetainedExecRequestPermit), Status> {
        let child = self
            .child
            .take()
            .ok_or_else(|| Status::internal("pending pipe child is missing"))?;
        Ok((child, std::mem::take(&mut self.request_permit)))
    }
}

impl<C: PendingPipeProcess> Drop for PendingPipeChild<C> {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return;
        };
        retain_pending_pipe_cleanup(
            child,
            std::mem::take(&mut self.request_permit),
            None,
            EXEC_CANCEL_DRIVER_RETRY,
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingChildCleanupOutcome {
    Reaped,
    Retained,
    Missing,
}

#[derive(Default)]
struct RetainedExecRequestPermit {
    permit: Option<ExecRequestPermit>,
    preserve_supervisor: bool,
}

impl RetainedExecRequestPermit {
    fn take_for_publish(&mut self) -> Option<ExecRequestPermit> {
        self.permit.take()
    }

    fn fence_after_reap(mut self) {
        drop(self.permit.take());
    }
}

impl Drop for RetainedExecRequestPermit {
    fn drop(&mut self) {
        // If cleanup itself is cancelled or panics, preserving Starting is the
        // only safe state: fencing would incorrectly assert that no child can
        // still exist. The permit is released explicitly only after reap proof
        // or transferred for publication.
        if let Some(permit) = self.permit.take() {
            std::mem::forget(permit);
        }
    }
}

/// Schedule blocking cleanup without ever making the public rejection path
/// perform an unbounded synchronous reap. This is only the fallback for OS
/// thread creation failure. If there is no Tokio runtime, deliberately retain
/// the work forever rather than dropping the sole child authority.
fn retain_cleanup_with_tokio_or_leak<T, F>(work: Arc<StdMutex<T>>, cleanup: F)
where
    T: Send + 'static,
    F: FnOnce(Arc<StdMutex<T>>) + Send + 'static,
{
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        runtime.spawn_blocking(move || cleanup(work));
    } else {
        std::mem::forget(work);
    }
}

fn drive_pending_pipe_cleanup<C: PendingPipeProcess>(
    mut child: C,
    request_permit: RetainedExecRequestPermit,
    proof: Option<tokio::sync::oneshot::Sender<()>>,
    retry: std::time::Duration,
) {
    let pid = child.process_id();
    #[cfg(target_os = "linux")]
    let force_deadline = std::time::Instant::now() + EXEC_CANCEL_GRACE;
    #[cfg(not(target_os = "linux"))]
    let force_deadline = std::time::Instant::now();
    loop {
        request_pending_child_force_cancel(pid);
        // Machine supervisors themselves are the descendant-reap authority.
        // Never SIGKILL that authority just because the bounded caller timed
        // out: keep signalling its owned group and retain uncertainty instead.
        if !request_permit.preserve_supervisor && std::time::Instant::now() >= force_deadline {
            if let Err(error) = child.start_kill() {
                warn!(?pid, %error, "grpc: pending pipe child kill failed; retaining and retrying");
            }
        }
        match child.try_wait_reaped() {
            Ok(true) => {
                request_permit.fence_after_reap();
                if let Some(proof) = proof {
                    let _ = proof.send(());
                }
                return;
            }
            Ok(false) => {}
            Err(error) => {
                warn!(?pid, %error, "grpc: pending pipe child terminal poll failed; retaining and retrying");
            }
        }
        std::thread::sleep(retry);
    }
}

struct PendingPipeCleanupWork<C: PendingPipeProcess> {
    child: Option<C>,
    request_permit: Option<RetainedExecRequestPermit>,
    proof: Option<tokio::sync::oneshot::Sender<()>>,
}

fn retain_pending_pipe_cleanup<C: PendingPipeProcess>(
    child: C,
    request_permit: RetainedExecRequestPermit,
    proof: Option<tokio::sync::oneshot::Sender<()>>,
    retry: std::time::Duration,
) {
    let work = Arc::new(StdMutex::new(PendingPipeCleanupWork {
        child: Some(child),
        request_permit: Some(request_permit),
        proof,
    }));
    let worker_work = Arc::clone(&work);
    let spawn = std::thread::Builder::new()
        .name("vz-pending-pipe-reap".to_string())
        .spawn(move || {
            let (child, request_permit, proof) = {
                let mut work = worker_work.lock().unwrap_or_else(|p| p.into_inner());
                (
                    work.child.take(),
                    work.request_permit.take(),
                    work.proof.take(),
                )
            };
            if let (Some(child), Some(request_permit)) = (child, request_permit) {
                drive_pending_pipe_cleanup(child, request_permit, proof, retry);
            }
        });
    if let Err(error) = spawn {
        warn!(%error, "grpc: failed to spawn pending pipe cleanup thread; retaining fallback authority");
        retain_cleanup_with_tokio_or_leak(work, move |work| {
            let (child, request_permit, proof) = {
                let mut work = work.lock().unwrap_or_else(|p| p.into_inner());
                (
                    work.child.take(),
                    work.request_permit.take(),
                    work.proof.take(),
                )
            };
            if let (Some(child), Some(request_permit)) = (child, request_permit) {
                drive_pending_pipe_cleanup(child, request_permit, proof, retry);
            }
        });
    }
}

fn request_pending_child_force_cancel(pid: Option<u32>) {
    #[cfg(target_os = "linux")]
    if let Some(pid) = pid {
        // The Child handle remains owned until wait completes, so this PID
        // cannot be recycled between lookup and delivery.
        // SAFETY: kill receives the exact positive PID returned by Child::id.
        let _ = unsafe { libc::kill(pid as libc::pid_t, libc::SIGRTMIN()) };
    }

    #[cfg(not(target_os = "linux"))]
    let _ = pid;
}

fn definite_exec_rejection(
    request_id: &str,
    detail: String,
) -> Response<ReceiverStream<Result<ExecEvent, Status>>> {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let _ = sender.try_send(Ok(ExecEvent {
        event: Some(exec_event::Event::Error(detail)),
        sequence: 1,
        request_id: request_id.to_string(),
        // This reserved value is the protocol proof that no logical exec was
        // published and lets the host distinguish a definite rejection from a
        // transport failure whose remote process state is unknowable.
        exec_id: 0,
    }));
    Response::new(ReceiverStream::new(receiver))
}

async fn reject_pending_pipe(
    child: PendingPipeChild,
    request_id: &str,
    rejection: Status,
) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
    reject_pending_pipe_with_timeout(
        child,
        request_id,
        rejection,
        EXEC_CANCEL_REAP_TIMEOUT,
        EXEC_CANCEL_DRIVER_RETRY,
    )
    .await
}

async fn reject_pending_pipe_with_timeout<C: PendingPipeProcess>(
    child: PendingPipeChild<C>,
    request_id: &str,
    rejection: Status,
    timeout: std::time::Duration,
    retry: std::time::Duration,
) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
    match child.terminate_and_reap_with_timeout(timeout, retry).await {
        PendingChildCleanupOutcome::Reaped => Ok(definite_exec_rejection(
            request_id,
            format!(
                "exec rejected before readiness; spawned process reaped: {}",
                rejection.message()
            ),
        )),
        PendingChildCleanupOutcome::Retained => Err(Status::unavailable(format!(
            "exec rejected before readiness; cleanup proof remains pending under retained authority: {}",
            rejection.message()
        ))),
        PendingChildCleanupOutcome::Missing => Err(Status::internal(format!(
            "exec rejected before readiness but its child owner was missing: {}",
            rejection.message()
        ))),
    }
}

/// Holds the writer and master PTY for a PTY session, supporting
/// stdin writes and terminal resizing.
struct PtyMasterHandle {
    writer: Box<dyn std::io::Write + Send>,
    master: Box<dyn portable_pty::MasterPty + Send>,
}

/// Owns a just-spawned PTY trampoline until its ready handshake completes.
/// Cancellation of the gRPC handler must not leave an untracked trampoline
/// resolving a mutable container ID in the background.
struct PendingPtyChild {
    child: Option<Box<dyn portable_pty::Child + Send>>,
    request_permit: RetainedExecRequestPermit,
}

fn drive_pending_pty_cleanup(
    mut child: Box<dyn portable_pty::Child + Send>,
    request_permit: RetainedExecRequestPermit,
    proof: Option<tokio::sync::oneshot::Sender<()>>,
    retry: std::time::Duration,
) {
    let pid = child.process_id();
    #[cfg(target_os = "linux")]
    let force_deadline = std::time::Instant::now() + EXEC_CANCEL_GRACE;
    #[cfg(not(target_os = "linux"))]
    let force_deadline = std::time::Instant::now();
    loop {
        request_pending_child_force_cancel(pid);
        if !request_permit.preserve_supervisor && std::time::Instant::now() >= force_deadline {
            if let Err(error) = child.kill() {
                warn!(?pid, %error, "grpc: pending PTY child kill failed; retaining and retrying");
            }
        }
        match child.try_wait() {
            Ok(Some(_)) => {
                request_permit.fence_after_reap();
                if let Some(proof) = proof {
                    let _ = proof.send(());
                }
                return;
            }
            Ok(None) => {}
            Err(error) => {
                warn!(?pid, %error, "grpc: pending PTY child terminal poll failed; retaining and retrying");
            }
        }
        std::thread::sleep(retry);
    }
}

struct PendingPtyCleanupWork {
    child: Option<Box<dyn portable_pty::Child + Send>>,
    request_permit: Option<RetainedExecRequestPermit>,
    proof: Option<tokio::sync::oneshot::Sender<()>>,
}

fn retain_pending_pty_cleanup(
    child: Box<dyn portable_pty::Child + Send>,
    request_permit: RetainedExecRequestPermit,
    proof: Option<tokio::sync::oneshot::Sender<()>>,
    retry: std::time::Duration,
) {
    let work = Arc::new(StdMutex::new(PendingPtyCleanupWork {
        child: Some(child),
        request_permit: Some(request_permit),
        proof,
    }));
    let worker_work = Arc::clone(&work);
    let spawn = std::thread::Builder::new()
        .name("vz-pending-pty-reap".to_string())
        .spawn(move || {
            let (child, request_permit, proof) = {
                let mut work = worker_work.lock().unwrap_or_else(|p| p.into_inner());
                (
                    work.child.take(),
                    work.request_permit.take(),
                    work.proof.take(),
                )
            };
            if let (Some(child), Some(request_permit)) = (child, request_permit) {
                drive_pending_pty_cleanup(child, request_permit, proof, retry);
            }
        });
    if let Err(error) = spawn {
        warn!(%error, "grpc: failed to spawn pending PTY cleanup thread; retaining fallback authority");
        retain_cleanup_with_tokio_or_leak(work, move |work| {
            let (child, request_permit, proof) = {
                let mut work = work.lock().unwrap_or_else(|p| p.into_inner());
                (
                    work.child.take(),
                    work.request_permit.take(),
                    work.proof.take(),
                )
            };
            if let (Some(child), Some(request_permit)) = (child, request_permit) {
                drive_pending_pty_cleanup(child, request_permit, proof, retry);
            }
        });
    }
}

impl PendingPtyChild {
    fn new(
        child: Box<dyn portable_pty::Child + Send>,
        request_permit: Option<ExecRequestPermit>,
    ) -> Self {
        Self {
            child: Some(child),
            request_permit: RetainedExecRequestPermit {
                permit: request_permit,
                preserve_supervisor: false,
            },
        }
    }

    fn child_mut(&mut self) -> Result<&mut Box<dyn portable_pty::Child + Send>, Status> {
        self.child
            .as_mut()
            .ok_or_else(|| Status::internal("pending PTY child is missing"))
    }

    fn take(
        mut self,
    ) -> Result<
        (
            Box<dyn portable_pty::Child + Send>,
            RetainedExecRequestPermit,
        ),
        Status,
    > {
        let child = self
            .child
            .take()
            .ok_or_else(|| Status::internal("pending PTY child is missing"))?;
        Ok((child, std::mem::take(&mut self.request_permit)))
    }

    async fn terminate_and_reap_with_timeout(
        mut self,
        timeout: std::time::Duration,
        retry: std::time::Duration,
    ) -> PendingChildCleanupOutcome {
        let Some(child) = self.child.take() else {
            return PendingChildCleanupOutcome::Missing;
        };
        let (proof_tx, proof_rx) = tokio::sync::oneshot::channel();
        // This durable worker becomes the sole child owner before the public
        // rejection path awaits anything. Dropping or timing out that await
        // cannot orphan the spawned trampoline.
        retain_pending_pty_cleanup(
            child,
            std::mem::take(&mut self.request_permit),
            Some(proof_tx),
            retry,
        );
        match tokio::time::timeout(timeout, proof_rx).await {
            Ok(Ok(())) => PendingChildCleanupOutcome::Reaped,
            Ok(Err(_)) | Err(_) => PendingChildCleanupOutcome::Retained,
        }
    }
}

impl Drop for PendingPtyChild {
    fn drop(&mut self) {
        let Some(child) = self.child.take() else {
            return;
        };
        retain_pending_pty_cleanup(
            child,
            std::mem::take(&mut self.request_permit),
            None,
            EXEC_CANCEL_DRIVER_RETRY,
        );
    }
}

async fn reject_pending_pty(
    child: PendingPtyChild,
    request_id: &str,
    rejection: Status,
) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
    reject_pending_pty_with_timeout(
        child,
        request_id,
        rejection,
        EXEC_CANCEL_REAP_TIMEOUT,
        EXEC_CANCEL_DRIVER_RETRY,
    )
    .await
}

async fn reject_pending_pty_with_timeout(
    child: PendingPtyChild,
    request_id: &str,
    rejection: Status,
    timeout: std::time::Duration,
    retry: std::time::Duration,
) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
    match child.terminate_and_reap_with_timeout(timeout, retry).await {
        PendingChildCleanupOutcome::Reaped => Ok(definite_exec_rejection(
            request_id,
            format!(
                "PTY exec rejected before readiness; spawned process reaped: {}",
                rejection.message()
            ),
        )),
        PendingChildCleanupOutcome::Retained => Err(Status::unavailable(format!(
            "PTY exec rejected before readiness; cleanup proof remains pending under retained authority: {}",
            rejection.message()
        ))),
        PendingChildCleanupOutcome::Missing => Err(Status::internal(format!(
            "PTY exec rejected before readiness but its child owner was missing: {}",
            rejection.message()
        ))),
    }
}

static PTY_HANDLES: OnceLock<StdMutex<HashMap<u64, Arc<StdMutex<PtyMasterHandle>>>>> =
    OnceLock::new();

fn pty_handles() -> &'static StdMutex<HashMap<u64, Arc<StdMutex<PtyMasterHandle>>>> {
    PTY_HANDLES.get_or_init(|| StdMutex::new(HashMap::new()))
}

#[cfg(target_os = "linux")]
fn devpts_is_mounted() -> bool {
    let Ok(mounts) = std::fs::read_to_string("/proc/mounts") else {
        return false;
    };
    mounts.lines().any(|line| {
        let mut fields = line.split_whitespace();
        let _source = fields.next();
        let target = fields.next();
        let fs_type = fields.next();
        target == Some("/dev/pts") && fs_type == Some("devpts")
    })
}

#[cfg(target_os = "linux")]
fn ensure_devpts_ready() -> Result<(), Status> {
    use std::ffi::CString;
    use std::os::unix::fs::symlink;
    use std::path::Path;

    std::fs::create_dir_all("/dev/pts")
        .map_err(|error| Status::internal(format!("failed to create /dev/pts: {error}")))?;

    if !devpts_is_mounted() {
        let source = CString::new("devpts")
            .map_err(|error| Status::internal(format!("invalid devpts source: {error}")))?;
        let target = CString::new("/dev/pts")
            .map_err(|error| Status::internal(format!("invalid devpts target: {error}")))?;
        let fs_type = CString::new("devpts")
            .map_err(|error| Status::internal(format!("invalid devpts fs type: {error}")))?;
        let data = CString::new("ptmxmode=0666,mode=0620")
            .map_err(|error| Status::internal(format!("invalid devpts mount options: {error}")))?;
        let mount_result = unsafe {
            libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                fs_type.as_ptr(),
                0,
                data.as_ptr().cast(),
            )
        };
        if mount_result != 0 {
            let mount_error = std::io::Error::last_os_error();
            if mount_error.raw_os_error() != Some(libc::EBUSY) {
                return Err(Status::internal(format!(
                    "failed to mount devpts at /dev/pts: {mount_error}"
                )));
            }
        }
    }

    let ptmx_path = Path::new("/dev/ptmx");
    if !ptmx_path.exists() {
        symlink("pts/ptmx", ptmx_path).map_err(|error| {
            Status::internal(format!("failed to create /dev/ptmx symlink: {error}"))
        })?;
    }

    Ok(())
}

// ── Shared state passed to all service impls ────────────────────────

/// Shared state accessible by all gRPC service implementations.
#[derive(Clone)]
pub struct SharedState {
    /// Process table for tracking spawned child processes.
    pub process_table: Arc<Mutex<ProcessTable>>,
    /// Docker facade supervisor. Construction is inert; `EnsureDocker` starts it.
    pub docker_supervisor: Arc<crate::docker::DockerSupervisor>,
}

#[derive(Clone)]
struct ExecOrderContext {
    sender: tokio::sync::mpsc::Sender<Result<ExecEvent, Status>>,
    event_gate: Arc<Mutex<()>>,
    control_gate: Arc<Mutex<()>>,
    sequence: Arc<AtomicU64>,
    request_id: String,
}

impl ExecOrderContext {
    #[cfg(test)]
    fn new(
        sender: tokio::sync::mpsc::Sender<Result<ExecEvent, Status>>,
        request_id: String,
    ) -> Self {
        Self::with_initial_sequence(sender, request_id, 0)
    }

    fn with_initial_sequence(
        sender: tokio::sync::mpsc::Sender<Result<ExecEvent, Status>>,
        request_id: String,
        initial_sequence: u64,
    ) -> Self {
        Self {
            sender,
            event_gate: Arc::new(Mutex::new(())),
            control_gate: Arc::new(Mutex::new(())),
            sequence: Arc::new(AtomicU64::new(initial_sequence)),
            request_id,
        }
    }
}

static EXEC_ORDER_CONTEXTS: OnceLock<StdMutex<HashMap<u64, ExecOrderContext>>> = OnceLock::new();

fn exec_order_contexts() -> &'static StdMutex<HashMap<u64, ExecOrderContext>> {
    EXEC_ORDER_CONTEXTS.get_or_init(|| StdMutex::new(HashMap::new()))
}

fn with_exec_order_contexts<R>(f: impl FnOnce(&mut HashMap<u64, ExecOrderContext>) -> R) -> R {
    let mut guard = exec_order_contexts()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    f(&mut guard)
}

fn register_exec_order_context(exec_id: u64, context: ExecOrderContext) {
    with_exec_order_contexts(|contexts| {
        contexts.insert(exec_id, context);
    });
}

fn lookup_exec_order_context(exec_id: u64) -> Option<ExecOrderContext> {
    with_exec_order_contexts(|contexts| contexts.get(&exec_id).cloned())
}

fn remove_exec_order_context(exec_id: u64) {
    with_exec_order_contexts(|contexts| {
        contexts.remove(&exec_id);
    });
}

fn generated_request_id(prefix: &str) -> String {
    static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
    let seq = NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{seq:016x}")
}

fn allocate_logical_exec_id_from(next: &AtomicU64) -> Option<u64> {
    next.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        current.checked_add(1)
    })
    .ok()
}

fn allocate_logical_exec_id() -> Result<u64, Status> {
    static NEXT_EXEC_ID: AtomicU64 = AtomicU64::new(1);
    allocate_logical_exec_id_from(&NEXT_EXEC_ID)
        .filter(|exec_id| *exec_id != 0)
        .ok_or_else(|| Status::resource_exhausted("logical exec ID space exhausted"))
}

#[cfg(target_os = "linux")]
fn capture_signal_identity(pid: u32) -> ProcessIdentity {
    ProcessIdentity::capture_pidfd(pid).unwrap_or_else(|error| {
        warn!(pid, %error, "grpc: pidfd capture failed; later signals will fail closed");
        ProcessIdentity::from_pid(pid)
    })
}

fn request_id_from_metadata(metadata: Option<&TransportMetadata>, prefix: &str) -> String {
    metadata
        .and_then(|metadata| {
            let trimmed = metadata.request_id.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .unwrap_or_else(|| generated_request_id(prefix))
}

fn validate_container_exec_request_id(request_id: &str) -> Result<(), Status> {
    let Some(encoded_ticket) = request_id.strip_prefix("exec_req_") else {
        return Err(Status::invalid_argument(
            "invalid container exec request ID prefix",
        ));
    };
    let Some((encoded_uuid, encoded_sequence)) = encoded_ticket.split_once('_') else {
        return Err(Status::invalid_argument(
            "container exec request ID must be one canonical 62-byte guest ticket",
        ));
    };
    let parsed = uuid::Uuid::parse_str(encoded_uuid).map_err(|_| {
        Status::invalid_argument(
            "container exec request ID must be one canonical 62-byte guest ticket",
        )
    })?;
    let sequence = u64::from_str_radix(encoded_sequence, 16).ok();
    if request_id.len() != 62
        || parsed.get_version_num() != 4
        || parsed.get_variant() != uuid::Variant::RFC4122
        || parsed.to_string() != encoded_uuid
        || encoded_sequence.len() != 16
        || !encoded_sequence
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || sequence == Some(0)
        || sequence.is_none()
    {
        return Err(Status::invalid_argument(
            "container exec request ID must be one canonical 62-byte guest ticket",
        ));
    }
    Ok(())
}

fn validate_allocate_exec_transport_metadata(
    metadata: Option<&TransportMetadata>,
) -> Result<(), Status> {
    let metadata = metadata.ok_or_else(|| {
        Status::invalid_argument("allocate_exec_request transport metadata is required")
    })?;
    let request_id = metadata.request_id.trim();
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(Status::invalid_argument(
            "allocate_exec_request metadata request_id must contain 1..=128 bytes",
        ));
    }
    if metadata.idempotency_key.len() > 256 {
        return Err(Status::invalid_argument(
            "allocate_exec_request metadata idempotency_key exceeds 256 bytes",
        ));
    }
    Ok(())
}

fn validate_reconcile_transport_metadata(
    metadata: Option<&TransportMetadata>,
) -> Result<(), Status> {
    let metadata = metadata
        .ok_or_else(|| Status::invalid_argument("reconcile_exec transport metadata is required"))?;
    let request_id = metadata.request_id.trim();
    if request_id.is_empty() || request_id.len() > 128 {
        return Err(Status::invalid_argument(
            "reconcile_exec metadata request_id must contain 1..=128 bytes",
        ));
    }
    if metadata.idempotency_key.len() > 256 {
        return Err(Status::invalid_argument(
            "reconcile_exec metadata idempotency_key exceeds 256 bytes",
        ));
    }
    Ok(())
}

fn validate_exec_signal(signal: i32) -> Result<(), Status> {
    if signal <= 0 || signal > 64 {
        return Err(Status::invalid_argument(format!(
            "unsupported signal {signal}"
        )));
    }
    #[cfg(target_os = "linux")]
    if !crate::container_exec::supports_forwarded_signal(signal) {
        return Err(Status::invalid_argument(
            "signal cannot be forwarded safely by the exec supervisor",
        ));
    }
    Ok(())
}

async fn send_ordered_exec_event(exec_id: u64, event: exec_event::Event) -> Result<u64, ()> {
    let Some(context) = lookup_exec_order_context(exec_id) else {
        return Err(());
    };
    let _guard = context.event_gate.lock().await;
    let sequence = context.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    context
        .sender
        .send(Ok(ExecEvent {
            event: Some(event),
            sequence,
            request_id: context.request_id.clone(),
            exec_id,
        }))
        .await
        .map_err(|_| ())?;
    Ok(sequence)
}

type OutputReaderResult = Result<(), String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputDrain {
    Drained,
    ReceiverClosed,
}

async fn await_pipe_output_drain(
    exec_id: u64,
    sender: tokio::sync::mpsc::Sender<Result<ExecEvent, Status>>,
    stdout: &mut tokio::task::JoinHandle<OutputReaderResult>,
    stderr: &mut tokio::task::JoinHandle<OutputReaderResult>,
) -> OutputDrain {
    tokio::select! {
        joined = async { tokio::join!(&mut *stdout, &mut *stderr) } => {
            match joined {
                (Ok(Ok(())), Ok(Ok(()))) => OutputDrain::Drained,
                (stdout_result, stderr_result) => {
                    warn!(
                        exec_id,
                        ?stdout_result,
                        ?stderr_result,
                        "grpc: output reader failed; retaining finish authority until receiver closes"
                    );
                    sender.closed().await;
                    OutputDrain::ReceiverClosed
                }
            }
        }
        () = sender.closed() => {
            stdout.abort();
            stderr.abort();
            let _ = stdout.await;
            let _ = stderr.await;
            OutputDrain::ReceiverClosed
        }
    }
}

async fn await_pty_output_drain(
    exec_id: u64,
    sender: tokio::sync::mpsc::Sender<Result<ExecEvent, Status>>,
    cancel_reader: Arc<std::sync::atomic::AtomicBool>,
    reader: &mut tokio::task::JoinHandle<OutputReaderResult>,
) -> OutputDrain {
    tokio::select! {
        result = &mut *reader => {
            match result {
                Ok(Ok(())) => OutputDrain::Drained,
                result => {
                    warn!(
                        exec_id,
                        ?result,
                        "grpc: PTY output reader failed; retaining finish authority until receiver closes"
                    );
                    sender.closed().await;
                    OutputDrain::ReceiverClosed
                }
            }
        }
        () = sender.closed() => {
            cancel_reader.store(true, Ordering::Release);
            let _ = reader.await;
            OutputDrain::ReceiverClosed
        }
    }
}

async fn finish_exec_stream(
    process_table: &Arc<Mutex<ProcessTable>>,
    exec_id: u64,
    exit_code: i32,
    output_drain: OutputDrain,
) {
    // The watcher calling this function is the sole finish authority and only
    // arrives here after child reap plus either complete output EOF or explicit
    // receiver closure and reader shutdown.
    process_table.lock().await.finish(exec_id, exit_code);

    if output_drain == OutputDrain::Drained {
        if let Ok(sequence) =
            send_ordered_exec_event(exec_id, exec_event::Event::ExitCode(exit_code)).await
        {
            debug!(exec_id, sequence, "grpc: exit event");
        }
    }
    remove_exec_order_context(exec_id);
}

struct OrderedControl {
    sequence: u64,
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

async fn begin_ordered_control(exec_id: u64, operation: &str) -> Option<OrderedControl> {
    let context = lookup_exec_order_context(exec_id)?;
    // Control operations serialize with each other, but never wait behind an
    // output send blocked by bounded mpsc backpressure. Their sequence still
    // shares the stream-wide atomic counter.
    let guard = context.control_gate.clone().lock_owned().await;
    let sequence = context.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    debug!(
        exec_id,
        sequence, operation, "grpc: exec control op ordered"
    );
    Some(OrderedControl {
        sequence,
        _guard: guard,
    })
}

fn mark_nonblocking_control(exec_id: u64, operation: &str) -> Option<u64> {
    let context = lookup_exec_order_context(exec_id)?;
    let sequence = context.sequence.fetch_add(1, Ordering::Relaxed) + 1;
    debug!(
        exec_id,
        sequence, operation, "grpc: nonblocking exec control op ordered"
    );
    Some(sequence)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CancelOutcome {
    exit_code: i32,
    forced: bool,
}

impl From<ExecTerminalReceipt> for CancelOutcome {
    fn from(receipt: ExecTerminalReceipt) -> Self {
        Self {
            exit_code: receipt.exit_code,
            forced: receipt.forced,
        }
    }
}

#[cfg(test)]
fn terminal_cancel_outcome(table: &ProcessTable, exec_id: u64) -> Result<CancelOutcome, Status> {
    table
        .terminal_receipt(exec_id)
        .map(CancelOutcome::from)
        .ok_or_else(|| Status::not_found(format!("process {exec_id} not found")))
}

async fn write_pipe_stdin(
    process_table: &Arc<Mutex<ProcessTable>>,
    exec_id: u64,
    data: &[u8],
) -> Result<(), Status> {
    use tokio::io::AsyncWriteExt as _;

    if data.len() > MAX_STDIN_WRITE_BYTES {
        return Err(Status::resource_exhausted(format!(
            "stdin write exceeds {MAX_STDIN_WRITE_BYTES} bytes"
        )));
    }
    let mut stdin = {
        let mut table = process_table.lock().await;
        table
            .get_mut(exec_id)
            .ok_or_else(|| Status::not_found(format!("process {exec_id} not found")))?
            .stdin
            .take()
            .ok_or_else(|| Status::failed_precondition("stdin already closed or busy"))?
    };

    let write_result = tokio::time::timeout(STDIN_WRITE_TIMEOUT, stdin.write_all(data)).await;
    {
        let mut table = process_table.lock().await;
        if let Some(entry) = table.get_mut(exec_id) {
            if entry.stdin.is_none() {
                entry.stdin = Some(stdin);
            }
        }
    }
    match write_result {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(Status::internal(format!("stdin write failed: {error}"))),
        Err(_) => Err(Status::deadline_exceeded(format!(
            "stdin write exceeded {} ms",
            STDIN_WRITE_TIMEOUT.as_millis()
        ))),
    }
}

async fn cancel_active_exec(
    process_table: &Arc<Mutex<ProcessTable>>,
    exec_id: u64,
) -> Result<CancelOutcome, Status> {
    cancel_active_exec_with_grace(process_table, exec_id, EXEC_CANCEL_GRACE).await
}

async fn cancel_active_exec_with_grace(
    process_table: &Arc<Mutex<ProcessTable>>,
    exec_id: u64,
    grace: std::time::Duration,
) -> Result<CancelOutcome, Status> {
    let cancellation = {
        let mut table = process_table.lock().await;
        table
            .begin_cancel(exec_id, tokio::time::Instant::now() + grace)
            .ok_or_else(|| Status::not_found(format!("process {exec_id} not found")))?
    };
    let (completion, force_deadline, start_driver, driver_identity, initial_term_failed) =
        match cancellation {
            ExecCancellation::Completed(receipt) => return Ok(receipt.into()),
            ExecCancellation::Active {
                completion,
                force_deadline,
                start_driver,
                driver_identity,
                initial_term_failed,
            } => (
                completion,
                force_deadline,
                start_driver,
                driver_identity,
                initial_term_failed,
            ),
        };

    if start_driver {
        spawn_exec_cancel_driver(
            Arc::clone(process_table),
            exec_id,
            completion.clone(),
            force_deadline,
            driver_identity,
            initial_term_failed,
        );
    }

    let receipt = tokio::time::timeout(EXEC_CANCEL_REAP_TIMEOUT, completion.wait())
        .await
        .map_err(|_| Status::deadline_exceeded(format!("timed out reaping exec {exec_id}")))?
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "exec {exec_id} retired without a terminal reap receipt"
            ))
        })?;
    Ok(receipt.into())
}

fn spawn_exec_cancel_driver(
    process_table: Arc<Mutex<ProcessTable>>,
    exec_id: u64,
    completion: crate::process_table::ExecCompletion,
    force_deadline: tokio::time::Instant,
    identity: Arc<ProcessIdentity>,
    initial_term_failed: bool,
) {
    tokio::spawn(async move {
        if initial_term_failed {
            loop {
                match tokio::time::timeout_at(
                    std::cmp::min(
                        force_deadline,
                        tokio::time::Instant::now() + EXEC_CANCEL_DRIVER_RETRY,
                    ),
                    completion.clone().wait(),
                )
                .await
                {
                    Ok(Some(_)) => return,
                    Ok(None) => {
                        let _ = identity.force_cancel();
                        return;
                    }
                    Err(_) if tokio::time::Instant::now() >= force_deadline => break,
                    Err(_) => match identity.signal(libc::SIGTERM) {
                        Ok(()) => break,
                        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => break,
                        Err(error) => {
                            warn!(exec_id, %error, "grpc: durable cancellation TERM retry failed");
                        }
                    },
                }
            }
        }
        match tokio::time::timeout_at(force_deadline, completion.clone().wait()).await {
            Ok(Some(_)) => return,
            Ok(None) => {
                let _ = identity.force_cancel();
                warn!(
                    exec_id,
                    "grpc: cancellation lost cleanup authority before reap"
                );
                return;
            }
            Err(_) => {}
        }

        loop {
            let signal_result = {
                let mut table = process_table.lock().await;
                if table.terminal_receipt(exec_id).is_some() {
                    return;
                }
                table.force_cancel(exec_id)
            };
            match signal_result {
                Some(Ok(())) => {}
                Some(Err(error)) if error.raw_os_error() == Some(libc::ESRCH) => {}
                Some(Err(error)) => {
                    warn!(exec_id, %error, "grpc: durable cancellation force signal failed; retrying");
                }
                None => {
                    let result = identity.force_cancel();
                    warn!(
                        exec_id,
                        ?result,
                        "grpc: table control disappeared; retained driver identity forced cleanup"
                    );
                }
            }

            match tokio::time::timeout(EXEC_CANCEL_DRIVER_RETRY, completion.clone().wait()).await {
                Ok(Some(_)) => return,
                Ok(None) => {
                    let _ = identity.force_cancel();
                    warn!(
                        exec_id,
                        "grpc: cancellation completion disappeared before reap"
                    );
                    return;
                }
                Err(_) => {}
            }
        }
    });
}

fn spawn_exec_cleanup(process_table: Arc<Mutex<ProcessTable>>, exec_id: u64) {
    tokio::spawn(async move {
        cancel_until_terminal_receipt(&process_table, exec_id).await;
    });
}

async fn cancel_until_terminal_receipt(process_table: &Arc<Mutex<ProcessTable>>, exec_id: u64) {
    loop {
        match cancel_active_exec(process_table, exec_id).await {
            Ok(_) => return,
            Err(error) if error.code() == tonic::Code::NotFound => return,
            Err(error) => {
                warn!(exec_id, %error, "grpc: cleanup subscriber retrying until reap receipt");
                tokio::time::sleep(EXEC_CANCEL_DRIVER_RETRY).await;
            }
        }
    }
}

fn classify_child_wait(
    result: std::io::Result<Option<std::process::ExitStatus>>,
) -> std::io::Result<Option<i32>> {
    result.map(|status| status.map(crate::process_table::normalized_exit_status))
}

fn monitor_exec_stream_loss(process_table: Arc<Mutex<ProcessTable>>, exec_id: u64) {
    tokio::spawn(async move {
        loop {
            let Some(context) = lookup_exec_order_context(exec_id) else {
                return;
            };
            if context.sender.is_closed() {
                warn!(
                    exec_id,
                    "grpc: exec stream lost; cancelling supervised process"
                );
                cancel_until_terminal_receipt(&process_table, exec_id).await;
                return;
            }
            drop(context);
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    });
}

// ── AgentService ────────────────────────────────────────────────────

/// gRPC implementation of the `AgentService` trait.
pub struct AgentServiceImpl {
    state: SharedState,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PreparedAgentExec {
    command: String,
    args: Vec<String>,
    spawn_working_dir: Option<String>,
    spawn_user: Option<String>,
    spawn_environment: Vec<(String, String)>,
    clear_environment: bool,
    container_targeted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ContainerExecProcessSpec {
    trampoline: crate::container_exec::TrampolineCommand,
    environment: Vec<(String, String)>,
}

const DEFAULT_CONTAINER_EXEC_PATH: &str =
    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

fn normalized_container_environment(
    environment: &HashMap<String, String>,
) -> Result<Vec<(String, String)>, Status> {
    let mut normalized = environment
        .iter()
        .map(|(key, value)| {
            if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
                return Err(Status::invalid_argument(
                    "container exec environment contains an invalid key or NUL byte",
                ));
            }
            Ok((key.clone(), value.clone()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if !environment.contains_key("PATH") {
        normalized.push(("PATH".to_string(), DEFAULT_CONTAINER_EXEC_PATH.to_string()));
    }
    normalized.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(normalized)
}

fn normalized_container_exec(
    container_id: &str,
    command: &str,
    args: &[String],
    working_dir: Option<&str>,
    user: Option<&str>,
    environment: &HashMap<String, String>,
    ready_handshake: Option<(&str, &str)>,
) -> Result<ContainerExecProcessSpec, Status> {
    let environment = normalized_container_environment(environment)?;
    let trampoline = crate::container_exec::prepare_trampoline_with_ready_socket(
        container_id,
        command,
        args,
        working_dir,
        user,
        environment.iter().any(|(key, _)| key == "SHELL"),
        ready_handshake,
    )
    .map_err(|error| Status::invalid_argument(error.to_string()))?;
    Ok(ContainerExecProcessSpec {
        trampoline,
        environment,
    })
}

fn prepare_agent_exec(
    req: &ExecRequest,
    ready_handshake: Option<(&str, &str)>,
) -> Result<PreparedAgentExec, Status> {
    if req.supervised_machine {
        if req.container_target.is_some() {
            return Err(Status::invalid_argument(
                "Machine and container execution targets are mutually exclusive",
            ));
        }
        #[cfg(any(target_os = "linux", test))]
        {
            if req.env.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            }) {
                return Err(Status::invalid_argument(
                    "Machine exec environment contains an invalid key or NUL byte",
                ));
            }
            let handshake = ready_handshake.ok_or_else(|| {
                Status::invalid_argument("Machine execution requires authenticated readiness")
            })?;
            let command = crate::container_exec::machine::prepare(
                &req.command,
                &req.args,
                &req.working_dir,
                &req.user,
                handshake,
            )
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
            return Ok(PreparedAgentExec {
                command: command.program,
                args: command.args,
                spawn_working_dir: Some("/".to_string()),
                spawn_user: None,
                spawn_environment: req
                    .env
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect(),
                clear_environment: false,
                container_targeted: false,
            });
        }
        #[cfg(not(any(target_os = "linux", test)))]
        return Err(Status::unimplemented(
            "supervised Machine execution requires a Linux guest",
        ));
    }
    if let Some(target) = &req.container_target {
        let spec = normalized_container_exec(
            &target.container_id,
            &req.command,
            &req.args,
            (!req.working_dir.is_empty()).then_some(req.working_dir.as_str()),
            (!req.user.is_empty()).then_some(req.user.as_str()),
            &req.env,
            ready_handshake,
        )?;
        return Ok(PreparedAgentExec {
            command: spec.trampoline.program,
            args: spec.trampoline.args,
            // Cwd and user are applied inside the pinned container root and
            // namespaces, never to the trampoline in the guest namespace.
            spawn_working_dir: None,
            spawn_user: None,
            spawn_environment: spec.environment,
            clear_environment: true,
            container_targeted: true,
        });
    }

    Ok(PreparedAgentExec {
        command: req.command.clone(),
        args: req.args.clone(),
        spawn_working_dir: (!req.working_dir.is_empty()).then(|| req.working_dir.clone()),
        spawn_user: (!req.user.is_empty()).then(|| req.user.clone()),
        spawn_environment: req
            .env
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        clear_environment: false,
        container_targeted: false,
    })
}

#[cfg(any(target_os = "linux", test))]
fn validate_claimed_container_exec(
    req: &ExecRequest,
    request_id: &str,
    transport: &str,
) -> Result<(), Response<ReceiverStream<Result<ExecEvent, Status>>>> {
    prepare_agent_exec(req, None).map(|_| ()).map_err(|error| {
        definite_exec_rejection(
            request_id,
            format!("container {transport} exec validation rejected before spawn: {error}"),
        )
    })
}

#[cfg(test)]
fn prepare_oci_exec(req: &OciExecRequest) -> Result<ContainerExecProcessSpec, Status> {
    normalized_container_exec(
        &req.container_id,
        &req.command,
        &req.args,
        (!req.working_dir.is_empty()).then_some(req.working_dir.as_str()),
        (!req.user.is_empty()).then_some(req.user.as_str()),
        &req.env,
        None,
    )
}

impl AgentServiceImpl {
    /// Create a new `AgentServiceImpl` with the given shared state.
    pub fn new(state: SharedState) -> Self {
        Self { state }
    }

    /// Pipe-based exec (the original non-PTY path). Spawns a child process with
    /// piped stdin/stdout/stderr and streams output events back to the client.
    async fn exec_pipe(
        &self,
        req: ExecRequest,
        request_id: String,
        mut request_permit: Option<ExecRequestPermit>,
    ) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
        use tokio::io::AsyncReadExt;

        // Request-ID claim/authentication already happened in Exec. Therefore
        // even malformed container launch input is a definite no-child result.
        #[cfg(target_os = "linux")]
        if req.container_target.is_some() {
            if let Err(rejection) = validate_claimed_container_exec(&req, &request_id, "pipe") {
                return Ok(rejection);
            }
        }
        #[cfg(target_os = "linux")]
        let server_admission = if let Some(target) = req.container_target.as_ref() {
            match acquire_shared_container_admission(&target.container_id).await {
                Ok(admission) => Some(admission),
                Err(error) => {
                    return Ok(definite_exec_rejection(
                        &request_id,
                        format!("container pipe exec admission rejected before spawn: {error}"),
                    ));
                }
            }
        } else {
            None
        };
        #[cfg(target_os = "linux")]
        let ready_listener = if req.container_target.is_some() || req.supervised_machine {
            match ContainerReadyListener::bind() {
                Ok(listener) => Some(listener),
                Err(error) => {
                    return Ok(definite_exec_rejection(
                        &request_id,
                        format!("container pipe exec readiness rejected before spawn: {error}"),
                    ));
                }
            }
        } else {
            None
        };
        #[cfg(target_os = "linux")]
        let ready_endpoint = match ready_listener
            .as_ref()
            .map(ContainerReadyListener::endpoint)
            .transpose()
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("container pipe exec endpoint rejected before spawn: {error}"),
                ));
            }
        };
        let launch = match prepare_agent_exec(
            &req,
            #[cfg(target_os = "linux")]
            ready_endpoint,
            #[cfg(not(target_os = "linux"))]
            None,
        ) {
            Ok(launch) => launch,
            Err(error) if req.container_target.is_some() || req.supervised_machine => {
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("container pipe exec rejected before spawn: {error}"),
                ));
            }
            Err(error) => return Err(error),
        };
        if let Some(permit) = request_permit.as_mut() {
            if permit.authorize_start().is_err() {
                return Ok(definite_exec_rejection(
                    &request_id,
                    "container pipe exec was fenced before spawn authorization".to_string(),
                ));
            }
        }
        let spawn_result = if let Some(ref username) = launch.spawn_user {
            crate::spawn_as_user(
                username,
                &launch.command,
                &launch.args,
                launch.spawn_working_dir.as_deref(),
                &launch.spawn_environment,
                launch.clear_environment,
            )
        } else {
            crate::spawn_direct(
                &launch.command,
                &launch.args,
                launch.spawn_working_dir.as_deref(),
                &launch.spawn_environment,
                launch.clear_environment,
            )
        };

        let child = match spawn_result {
            Ok(child) => child,
            Err(e) => {
                warn!(request_id = %request_id, command = %launch.command, error = %e, "grpc: exec spawn failed");
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("exec rejected before spawn: {e}"),
                ));
            }
        };
        let mut pending_child = PendingPipeChild::with_request_permit(child, request_permit.take());
        pending_child.request_permit.preserve_supervisor = req.supervised_machine;

        info!(request_id = %request_id, command = %launch.command, arg_count = launch.args.len(), container_targeted = launch.container_targeted, "grpc: process spawned");

        let spawned_pid = match pending_child.child_mut() {
            Ok(child) => child.id().unwrap_or(0),
            Err(rejection) => {
                return reject_pending_pipe(pending_child, &request_id, rejection).await;
            }
        };
        if spawned_pid == 0 {
            let rejection = Status::internal("spawned exec has no process ID");
            return reject_pending_pipe(pending_child, &request_id, rejection).await;
        }
        let exec_id = match allocate_logical_exec_id() {
            Ok(exec_id) => exec_id,
            Err(rejection) => {
                return reject_pending_pipe(pending_child, &request_id, rejection).await;
            }
        };

        #[cfg(target_os = "linux")]
        let (ready_generation, process_identity) = if req.supervised_machine {
            let Some(listener) = ready_listener else {
                return reject_pending_pipe(
                    pending_child,
                    &request_id,
                    Status::internal("Machine execution lost its readiness listener"),
                )
                .await;
            };
            let identity = match SpawnedProcessIdentity::capture(spawned_pid) {
                Ok(identity) => identity,
                Err(error) => {
                    return reject_pending_pipe(
                        pending_child,
                        &request_id,
                        Status::failed_precondition(format!(
                            "Machine supervisor identity unavailable: {error}"
                        )),
                    )
                    .await;
                }
            };
            if let Err(error) = listener.wait_machine(&identity).await {
                return reject_pending_pipe(pending_child, &request_id, error).await;
            }
            (None, identity.into_process_identity())
        } else if let Some(listener) = ready_listener {
            let Some(target) = req.container_target.as_ref() else {
                let rejection = Status::internal("ready listener lost container target");
                return reject_pending_pipe(pending_child, &request_id, rejection).await;
            };
            let spawned_process = match SpawnedProcessIdentity::capture(spawned_pid) {
                Ok(identity) => identity,
                Err(error) => {
                    let rejection = Status::failed_precondition(format!(
                        "cannot capture spawned container exec identity: {error}"
                    ));
                    return reject_pending_pipe(pending_child, &request_id, rejection).await;
                }
            };
            let container_id = target.container_id.clone();
            let wait = tokio::spawn(async move {
                let result = listener.wait(&spawned_process, &container_id).await;
                drop(server_admission);
                (result, spawned_process)
            });
            let (result, spawned_process) = match wait.await {
                Ok(result) => result,
                Err(error) => {
                    let rejection = Status::internal(format!("exec-ready task failed: {error}"));
                    return reject_pending_pipe(pending_child, &request_id, rejection).await;
                }
            };
            match result {
                Ok(generation) => (Some(generation), spawned_process.into_process_identity()),
                Err(error) => {
                    return reject_pending_pipe(pending_child, &request_id, error).await;
                }
            }
        } else {
            (None, capture_signal_identity(spawned_pid))
        };
        #[cfg(not(target_os = "linux"))]
        let process_identity = ProcessIdentity::from_pid(spawned_pid);

        let (stdout, stderr, stdin) = match pending_child.child_mut() {
            Ok(child) => (child.stdout.take(), child.stderr.take(), child.stdin.take()),
            Err(rejection) => {
                return reject_pending_pipe(pending_child, &request_id, rejection).await;
            }
        };

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ExecEvent, Status>>(64);
        #[cfg(target_os = "linux")]
        let initial_sequence = u64::from(ready_generation.is_some() || req.supervised_machine);
        #[cfg(not(target_os = "linux"))]
        let initial_sequence = 0;

        let process_table = self.state.process_table.clone();
        {
            let mut table = self.state.process_table.lock().await;
            let (child, mut retained_permit) = match pending_child.take() {
                Ok(owned) => owned,
                Err(rejection) => {
                    return Err(Status::internal(format!(
                        "exec rejected before registration with missing cleanup authority: {rejection}"
                    )));
                }
            };
            let _ = table.insert(
                exec_id,
                child,
                stdin,
                process_identity,
                launch.container_targeted || req.supervised_machine,
            );
            if let Some(permit) = retained_permit.take_for_publish() {
                permit.publish(exec_id);
            }
        }
        #[cfg(target_os = "linux")]
        if let Some(generation) = ready_generation {
            let _ = tx
                .send(Ok(ExecEvent {
                    event: Some(exec_event::Event::ContainerReady(ContainerExecReady {
                        generation: Some(generation),
                    })),
                    sequence: 1,
                    request_id: request_id.clone(),
                    exec_id,
                }))
                .await;
        } else if req.supervised_machine {
            let _ = tx
                .send(Ok(ExecEvent {
                    event: Some(exec_event::Event::MachineReady(MachineExecReady {})),
                    sequence: 1,
                    request_id: request_id.clone(),
                    exec_id,
                }))
                .await;
        }
        register_exec_order_context(
            exec_id,
            ExecOrderContext::with_initial_sequence(tx.clone(), request_id, initial_sequence),
        );
        monitor_exec_stream_loss(self.state.process_table.clone(), exec_id);
        let finish_sender = tx.clone();

        let mut stdout_handle = tokio::spawn(async move {
            if let Some(mut stdout) = stdout {
                let mut buf = vec![0u8; 65536];
                loop {
                    match stdout.read(&mut buf).await {
                        Ok(0) => return Ok(()),
                        Ok(n) => {
                            match send_ordered_exec_event(
                                exec_id,
                                exec_event::Event::Stdout(buf[..n].to_vec()),
                            )
                            .await
                            {
                                Ok(sequence) => {
                                    debug!(exec_id, sequence, bytes = n, "grpc: stdout chunk");
                                }
                                Err(_) => {
                                    return Err("exec stream closed while sending stdout".into());
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exec_id, error = %e, "grpc: stdout read error");
                            return Err(format!("stdout read failed: {e}"));
                        }
                    }
                }
            }
            Ok(())
        });

        let mut stderr_handle = tokio::spawn(async move {
            if let Some(mut stderr) = stderr {
                let mut buf = vec![0u8; 65536];
                loop {
                    match stderr.read(&mut buf).await {
                        Ok(0) => return Ok(()),
                        Ok(n) => {
                            match send_ordered_exec_event(
                                exec_id,
                                exec_event::Event::Stderr(buf[..n].to_vec()),
                            )
                            .await
                            {
                                Ok(sequence) => {
                                    debug!(exec_id, sequence, bytes = n, "grpc: stderr chunk");
                                }
                                Err(_) => {
                                    return Err("exec stream closed while sending stderr".into());
                                }
                            }
                        }
                        Err(e) => {
                            warn!(exec_id, error = %e, "grpc: stderr read error");
                            return Err(format!("stderr read failed: {e}"));
                        }
                    }
                }
            }
            Ok(())
        });

        let exit_table = process_table;
        tokio::spawn(async move {
            // Never hold the global process table lock while waiting for process exit.
            // Otherwise a slow/hung non-PTY command can block unrelated PTY exec setup.
            let mut cleanup_started = false;
            let exit_code = loop {
                let poll = {
                    let mut table = exit_table.lock().await;
                    if let Some(entry) = table.get_mut(exec_id) {
                        classify_child_wait(entry.child.try_wait())
                    } else if table.terminal_receipt(exec_id).is_some() {
                        remove_exec_order_context(exec_id);
                        return;
                    } else {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "exec wait handle is missing",
                        ))
                    }
                };
                match poll {
                    Ok(Some(code)) => break code,
                    Ok(None) => {}
                    Err(error) => {
                        warn!(exec_id, %error, "grpc: wait failed; retaining cleanup authority");
                        if !cleanup_started {
                            cleanup_started = true;
                            spawn_exec_cleanup(exit_table.clone(), exec_id);
                        }
                    }
                }
                tokio::time::sleep(std::time::Duration::from_millis(25)).await;
            };

            let output_drain = await_pipe_output_drain(
                exec_id,
                finish_sender,
                &mut stdout_handle,
                &mut stderr_handle,
            )
            .await;

            info!(exec_id, exit_code, "grpc: process exited");
            finish_exec_stream(&exit_table, exec_id, exit_code, output_drain).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    /// PTY-based exec. Allocates a pseudo-terminal via `portable-pty` and spawns
    /// the child process with the slave side as its controlling terminal. Output
    /// is read from the master and streamed as stdout events.
    async fn exec_pty(
        &self,
        req: ExecRequest,
        request_id: String,
        mut request_permit: Option<ExecRequestPermit>,
    ) -> Result<Response<ReceiverStream<Result<ExecEvent, Status>>>, Status> {
        use portable_pty::{CommandBuilder, PtySize, native_pty_system};
        use std::io::Read;

        info!(
            request_id = %request_id,
            command = %req.command,
            args = ?req.args,
            "grpc: pty exec request received"
        );

        #[cfg(target_os = "linux")]
        if req.container_target.is_some() {
            if let Err(rejection) = validate_claimed_container_exec(&req, &request_id, "PTY") {
                return Ok(rejection);
            }
        }
        #[cfg(target_os = "linux")]
        let server_admission = if let Some(target) = req.container_target.as_ref() {
            match acquire_shared_container_admission(&target.container_id).await {
                Ok(admission) => Some(admission),
                Err(error) => {
                    return Ok(definite_exec_rejection(
                        &request_id,
                        format!("container PTY exec admission rejected before spawn: {error}"),
                    ));
                }
            }
        } else {
            None
        };
        #[cfg(target_os = "linux")]
        let ready_listener = if req.container_target.is_some() || req.supervised_machine {
            match ContainerReadyListener::bind() {
                Ok(listener) => Some(listener),
                Err(error) => {
                    return Ok(definite_exec_rejection(
                        &request_id,
                        format!("container PTY exec readiness rejected before spawn: {error}"),
                    ));
                }
            }
        } else {
            None
        };
        #[cfg(target_os = "linux")]
        let ready_endpoint = match ready_listener
            .as_ref()
            .map(ContainerReadyListener::endpoint)
            .transpose()
        {
            Ok(endpoint) => endpoint,
            Err(error) => {
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("container PTY exec endpoint rejected before spawn: {error}"),
                ));
            }
        };
        let launch = match prepare_agent_exec(
            &req,
            #[cfg(target_os = "linux")]
            ready_endpoint,
            #[cfg(not(target_os = "linux"))]
            None,
        ) {
            Ok(launch) => launch,
            Err(error) if req.container_target.is_some() || req.supervised_machine => {
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("container PTY exec rejected before spawn: {error}"),
                ));
            }
            Err(error) => return Err(error),
        };
        let rows = if req.term_rows == 0 {
            24
        } else {
            req.term_rows
        };
        let cols = if req.term_cols == 0 {
            80
        } else {
            req.term_cols
        };

        #[cfg(target_os = "linux")]
        if let Err(error) = ensure_devpts_ready() {
            if req.container_target.is_some() || req.supervised_machine {
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("container PTY exec devpts rejected before spawn: {error}"),
                ));
            }
            return Err(error);
        }

        let pty_system = native_pty_system();
        info!(
            request_id = %request_id,
            rows,
            cols,
            "grpc: opening PTY pair"
        );
        let pair = match pty_system.openpty(PtySize {
            rows: rows as u16,
            cols: cols as u16,
            pixel_width: 0,
            pixel_height: 0,
        }) {
            Ok(pair) => pair,
            Err(error) if req.container_target.is_some() || req.supervised_machine => {
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("container PTY exec openpty rejected before spawn: {error}"),
                ));
            }
            Err(error) => return Err(Status::internal(format!("openpty failed: {error}"))),
        };
        info!(request_id = %request_id, "grpc: PTY pair opened");

        let mut cmd = CommandBuilder::new(&launch.command);
        cmd.args(&launch.args);

        if let Some(working_dir) = &launch.spawn_working_dir {
            cmd.cwd(working_dir);
        } else if launch.container_targeted {
            // portable-pty otherwise falls back to the guest user's passwd
            // home (typically /root) and unconditionally uses it as the
            // trampoline's cwd. Minimal guests need not provide that
            // directory. The requested container cwd is applied later by the
            // verified trampoline, after namespace entry and chroot.
            cmd.cwd("/");
        }

        if launch.clear_environment {
            cmd.env_clear();
        } else {
            cmd.env("TERM", "xterm-256color");
        }
        for (key, value) in &launch.spawn_environment {
            cmd.env(key, value);
        }

        info!(
            request_id = %request_id,
            command = %launch.command,
            "grpc: spawning PTY process"
        );
        if let Some(permit) = request_permit.as_mut() {
            if permit.authorize_start().is_err() {
                return Ok(definite_exec_rejection(
                    &request_id,
                    "container PTY exec was fenced before spawn authorization".to_string(),
                ));
            }
        }
        let child = match pair.slave.spawn_command(cmd) {
            Ok(child) => child,
            Err(error) => {
                warn!(command = %launch.command, %error, "grpc: pty exec spawn failed");
                return Ok(definite_exec_rejection(
                    &request_id,
                    format!("PTY exec rejected before spawn: {error}"),
                ));
            }
        };
        let mut pending_child = PendingPtyChild::new(child, request_permit.take());
        pending_child.request_permit.preserve_supervisor = req.supervised_machine;
        info!(request_id = %request_id, "grpc: PTY process spawned");

        // Drop slave — only the child uses it.
        drop(pair.slave);

        let spawned_pid = match pending_child.child_mut() {
            Ok(child) => child.process_id().unwrap_or(0),
            Err(rejection) => {
                return reject_pending_pty(pending_child, &request_id, rejection).await;
            }
        };
        if spawned_pid == 0 {
            return reject_pending_pty(
                pending_child,
                &request_id,
                Status::internal("spawned PTY exec has no process ID"),
            )
            .await;
        }
        let exec_id = match allocate_logical_exec_id() {
            Ok(exec_id) => exec_id,
            Err(rejection) => {
                return reject_pending_pty(pending_child, &request_id, rejection).await;
            }
        };
        info!(
            request_id = %request_id, exec_id, spawned_pid, command = %launch.command,
            arg_count = launch.args.len(), rows, cols, container_targeted = launch.container_targeted,
            "grpc: pty process spawned"
        );

        #[cfg(target_os = "linux")]
        let (ready_generation, process_identity) = if req.supervised_machine {
            let Some(listener) = ready_listener else {
                return reject_pending_pty(
                    pending_child,
                    &request_id,
                    Status::internal("Machine execution lost its readiness listener"),
                )
                .await;
            };
            let identity = match SpawnedProcessIdentity::capture(spawned_pid) {
                Ok(identity) => identity,
                Err(error) => {
                    return reject_pending_pty(
                        pending_child,
                        &request_id,
                        Status::failed_precondition(format!(
                            "Machine supervisor identity unavailable: {error}"
                        )),
                    )
                    .await;
                }
            };
            if let Err(error) = listener.wait_machine(&identity).await {
                return reject_pending_pty(pending_child, &request_id, error).await;
            }
            (None, identity.into_process_identity())
        } else if let Some(listener) = ready_listener {
            let Some(target) = req.container_target.as_ref() else {
                return reject_pending_pty(
                    pending_child,
                    &request_id,
                    Status::internal("ready listener lost container target"),
                )
                .await;
            };
            let spawned_process = match SpawnedProcessIdentity::capture(spawned_pid) {
                Ok(identity) => identity,
                Err(error) => {
                    let rejection = Status::failed_precondition(format!(
                        "cannot capture spawned PTY container exec identity: {error}"
                    ));
                    return reject_pending_pty(pending_child, &request_id, rejection).await;
                }
            };
            let container_id = target.container_id.clone();
            let wait = tokio::spawn(async move {
                let result = listener.wait(&spawned_process, &container_id).await;
                drop(server_admission);
                (result, spawned_process)
            });
            let (result, spawned_process) = match wait.await {
                Ok(result) => result,
                Err(error) => {
                    let rejection = Status::internal(format!("exec-ready task failed: {error}"));
                    return reject_pending_pty(pending_child, &request_id, rejection).await;
                }
            };
            match result {
                Ok(generation) => (Some(generation), spawned_process.into_process_identity()),
                Err(error) => {
                    return reject_pending_pty(pending_child, &request_id, error).await;
                }
            }
        } else {
            (None, capture_signal_identity(spawned_pid))
        };
        #[cfg(not(target_os = "linux"))]
        let process_identity = ProcessIdentity::from_pid(spawned_pid);

        let (tx, rx) = tokio::sync::mpsc::channel::<Result<ExecEvent, Status>>(64);
        // Container-targeted sessions use a typed generation proof as their
        // first event. Ordinary guest PTY sessions retain the empty correlation
        // frame for backwards compatibility.
        #[cfg(target_os = "linux")]
        let first_event = ready_generation.map_or_else(
            || {
                if req.supervised_machine {
                    exec_event::Event::MachineReady(MachineExecReady {})
                } else {
                    exec_event::Event::Stdout(Vec::new())
                }
            },
            |generation| {
                exec_event::Event::ContainerReady(ContainerExecReady {
                    generation: Some(generation),
                })
            },
        );
        #[cfg(not(target_os = "linux"))]
        let first_event = exec_event::Event::Stdout(Vec::new());

        // Get reader (cloned handle) and writer from the master.
        let mut reader = match pair.master.try_clone_reader() {
            Ok(reader) => reader,
            Err(error) => {
                let rejection = Status::internal(format!("failed to clone PTY reader: {error}"));
                return reject_pending_pty(pending_child, &request_id, rejection).await;
            }
        };
        let writer = match pair.master.take_writer() {
            Ok(writer) => writer,
            Err(error) => {
                let rejection = Status::internal(format!("failed to take PTY writer: {error}"));
                return reject_pending_pty(pending_child, &request_id, rejection).await;
            }
        };
        #[cfg(unix)]
        let reader_poll_fd = match pair.master.as_raw_fd() {
            Some(fd) => Some(fd),
            None => {
                return reject_pending_pty(
                    pending_child,
                    &request_id,
                    Status::failed_precondition(
                        "PTY master has no pollable descriptor for cancellable output",
                    ),
                )
                .await;
            }
        };
        #[cfg(not(unix))]
        let reader_poll_fd = None;
        let master_handle = Arc::new(StdMutex::new(PtyMasterHandle {
            writer,
            master: pair.master,
        }));

        // Acquire the async table lock while PendingPtyChild still guarantees
        // cancellation cleanup. After insertion, all remaining registration
        // is synchronous until the watcher has been spawned.
        {
            let mut table = self.state.process_table.lock().await;
            let (child, mut retained_permit) = match pending_child.take() {
                Ok(owned) => owned,
                Err(rejection) => {
                    return Err(Status::internal(format!(
                        "PTY exec rejected before registration with missing cleanup authority: {rejection}"
                    )));
                }
            };
            // portable-pty Child isn't tokio-compatible, so we wrap it in the
            // process table as a waitable entry below instead.
            let _ = table.insert_pty(
                exec_id,
                child,
                process_identity,
                launch.container_targeted || req.supervised_machine,
            );
            if let Some(permit) = retained_permit.take_for_publish() {
                permit.publish(exec_id);
            }
        }
        {
            let mut handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            handles.insert(exec_id, master_handle);
        }
        register_exec_order_context(
            exec_id,
            ExecOrderContext::with_initial_sequence(tx.clone(), request_id.clone(), 1),
        );
        monitor_exec_stream_loss(self.state.process_table.clone(), exec_id);
        info!(exec_id, "grpc: queueing initial PTY exec event");
        let _ = tx
            .send(Ok(ExecEvent {
                event: Some(first_event),
                sequence: 1,
                request_id: request_id.clone(),
                exec_id,
            }))
            .await;
        let finish_sender = tx.clone();

        // Spawn blocking reader task. portable-pty gives us a synchronous Read,
        // so we read in a blocking thread and forward chunks as exec events.
        let reader_exec_id = exec_id;
        let reader_cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let reader_cancel_task = reader_cancel.clone();
        let (reader_done_tx, reader_done_rx) = tokio::sync::oneshot::channel::<()>();
        let mut pty_reader_handle = tokio::task::spawn_blocking(move || -> OutputReaderResult {
            let mut buf = vec![0u8; 65536];
            let result = loop {
                if reader_cancel_task.load(Ordering::Acquire) {
                    break Err("PTY reader cancelled after receiver close".into());
                }
                if let Some(fd) = reader_poll_fd {
                    let mut descriptor = libc::pollfd {
                        fd,
                        events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
                        revents: 0,
                    };
                    // SAFETY: descriptor points to one initialized pollfd and
                    // remains valid for the duration of this call.
                    let poll_result = unsafe {
                        libc::poll(
                            &raw mut descriptor,
                            1,
                            PTY_READER_POLL_INTERVAL.as_millis() as libc::c_int,
                        )
                    };
                    if poll_result == 0 {
                        continue;
                    }
                    if poll_result < 0 {
                        let error = std::io::Error::last_os_error();
                        if error.kind() == std::io::ErrorKind::Interrupted {
                            continue;
                        }
                        break Err(format!("PTY poll failed: {error}"));
                    }
                    if descriptor.revents & libc::POLLNVAL != 0 {
                        break Err("PTY poll reported an invalid descriptor".into());
                    }
                }
                match reader.read(&mut buf) {
                    Ok(0) => break Ok(()),
                    Ok(n) => {
                        let data = buf[..n].to_vec();
                        let rt = tokio::runtime::Handle::current();
                        match rt.block_on(send_ordered_exec_event(
                            reader_exec_id,
                            exec_event::Event::Stdout(data),
                        )) {
                            Ok(sequence) => {
                                debug!(
                                    exec_id = reader_exec_id,
                                    sequence,
                                    bytes = n,
                                    "grpc: pty stdout chunk"
                                );
                            }
                            Err(_) => {
                                break Err("exec stream closed while sending PTY output".into());
                            }
                        }
                    }
                    Err(e) => {
                        // EIO is expected when the slave side closes (child exited).
                        if e.raw_os_error() == Some(libc::EIO) {
                            break Ok(());
                        }
                        warn!(exec_id = reader_exec_id, error = %e, "grpc: pty read error");
                        break Err(format!("PTY read failed: {e}"));
                    }
                }
            };
            let _ = reader_done_tx.send(());
            result
        });

        let reader_cleanup_table = self.state.process_table.clone();
        tokio::spawn(async move {
            if reader_done_rx.await.is_ok() {
                spawn_exec_cleanup(reader_cleanup_table, exec_id);
            }
        });

        // Spawn exit watcher for the PTY session.
        let exit_table = self.state.process_table.clone();
        tokio::spawn(async move {
            let Some(mut child) = ({
                let mut table = exit_table.lock().await;
                table.take_pty(exec_id)
            }) else {
                warn!(
                    exec_id,
                    "grpc: PTY wait handle missing; retaining cleanup authority"
                );
                spawn_exec_cleanup(exit_table.clone(), exec_id);
                return;
            };

            let mut cleanup_started = false;
            let exit_code = loop {
                let attempt = tokio::task::spawn_blocking(move || match child.wait() {
                    Ok(status) => Ok(status.exit_code() as i32),
                    Err(error) => Err((child, error.to_string())),
                })
                .await;
                match attempt {
                    Ok(Ok(exit_code)) => break exit_code,
                    Ok(Err((returned_child, error))) => {
                        child = returned_child;
                        warn!(exec_id, %error, "grpc: PTY wait failed; killing and retrying");
                        if !cleanup_started {
                            cleanup_started = true;
                            spawn_exec_cleanup(exit_table.clone(), exec_id);
                        }
                        tokio::time::sleep(EXEC_CANCEL_DRIVER_RETRY).await;
                    }
                    Err(error) => {
                        // A panicked blocking waiter has lost the only portable
                        // wait handle. Never manufacture a reaped receipt.
                        warn!(exec_id, %error, "grpc: PTY wait task failed without a reap receipt");
                        spawn_exec_cleanup(exit_table.clone(), exec_id);
                        return;
                    }
                }
            };

            let output_drain = await_pty_output_drain(
                exec_id,
                finish_sender,
                reader_cancel,
                &mut pty_reader_handle,
            )
            .await;

            info!(exec_id, exit_code, "grpc: pty process exited");

            // Clean up: remove from process table, PTY handles, and order context.
            {
                let mut handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
                handles.remove(&exec_id);
            }
            finish_exec_stream(&exit_table, exec_id, exit_code, output_drain).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl agent_service_server::AgentService for AgentServiceImpl {
    async fn ping(&self, _request: Request<PingRequest>) -> Result<Response<PingResponse>, Status> {
        info!("grpc: ping");
        Ok(Response::new(PingResponse {}))
    }

    async fn system_info(
        &self,
        _request: Request<SystemInfoRequest>,
    ) -> Result<Response<SystemInfoResponse>, Status> {
        let (cpu_count, memory_bytes, disk_free_bytes, os_version) = crate::collect_system_info()
            .map_err(|e| {
            warn!(error = %e, "grpc: system_info failed");
            Status::internal(format!("system info failed: {e}"))
        })?;

        Ok(Response::new(SystemInfoResponse {
            cpu_count,
            memory_bytes,
            disk_free_bytes,
            os_version,
            agent_protocol_revision: vz_agent_proto::AGENT_PROTOCOL_REVISION,
        }))
    }

    async fn resource_stats(
        &self,
        _request: Request<ResourceStatsRequest>,
    ) -> Result<Response<ResourceStatsResponse>, Status> {
        let stats = crate::collect_resource_stats().map_err(|e| {
            warn!(error = %e, "grpc: resource_stats failed");
            Status::internal(format!("resource stats failed: {e}"))
        })?;

        Ok(Response::new(ResourceStatsResponse {
            cpu_usage_percent: stats.cpu_usage_percent,
            memory_used_bytes: stats.memory_used_bytes,
            memory_total_bytes: stats.memory_total_bytes,
            disk_used_bytes: stats.disk_used_bytes,
            disk_total_bytes: stats.disk_total_bytes,
            process_count: stats.process_count,
            load_average: stats.load_average.to_vec(),
        }))
    }

    async fn allocate_exec_request(
        &self,
        request: Request<AllocateExecRequestRequest>,
    ) -> Result<Response<AllocateExecRequestResponse>, Status> {
        let request = request.into_inner();
        validate_allocate_exec_transport_metadata(request.metadata.as_ref())?;
        let registry = self.state.process_table.lock().await.request_registry();
        let exec_request_id = registry.allocate_request_id().map_err(|_| {
            Status::resource_exhausted("container exec request ticket space exhausted")
        })?;
        Ok(Response::new(AllocateExecRequestResponse {
            exec_request_id,
        }))
    }

    type ExecStream = ReceiverStream<Result<ExecEvent, Status>>;

    type EnsureDockerStream = ReceiverStream<Result<DockerEnsureEvent, Status>>;

    async fn exec(
        &self,
        request: Request<ExecRequest>,
    ) -> Result<Response<Self::ExecStream>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "exec");
        info!(
            request_id = %request_id,
            allocate_pty = req.allocate_pty,
            command = %req.command,
            "grpc: exec request routing"
        );

        let request_permit = if req.container_target.is_some() || req.supervised_machine {
            validate_container_exec_request_id(&request_id)?;
            let registry = self.state.process_table.lock().await.request_registry();
            match registry.claim(&request_id) {
                Ok(permit) => Some(permit),
                Err(ExecRequestClaimError::Active) => {
                    return Err(Status::already_exists(format!(
                        "container exec request `{request_id}` is already active"
                    )));
                }
                Err(ExecRequestClaimError::DefiniteRejection) => {
                    return Ok(definite_exec_rejection(
                        &request_id,
                        "container exec request ticket is invalid, stale, retired, or already completed"
                            .to_string(),
                    ));
                }
            }
        } else {
            None
        };

        if req.supervised_machine && req.container_target.is_some() {
            return Ok(definite_exec_rejection(
                &request_id,
                "Machine and container execution targets are mutually exclusive".to_string(),
            ));
        }
        #[cfg(not(target_os = "linux"))]
        if req.supervised_machine {
            return Ok(definite_exec_rejection(
                &request_id,
                "supervised Machine execution requires a Linux guest".to_string(),
            ));
        }

        if req.allocate_pty {
            self.exec_pty(req, request_id, request_permit).await
        } else {
            self.exec_pipe(req, request_id, request_permit).await
        }
    }

    async fn ensure_docker(
        &self,
        request: Request<DockerEnsureRequest>,
    ) -> Result<Response<Self::EnsureDockerStream>, Status> {
        let request = request.into_inner();
        let request_id = request_id_from_metadata(request.metadata.as_ref(), "ensure-docker");
        let supervisor = Arc::clone(&self.state.docker_supervisor);
        let (sender, receiver) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            use docker_ensure_event::Stage;

            if sender
                .send(Ok(DockerEnsureEvent {
                    stage: Stage::Validating as i32,
                    message: "Validating persistent Docker facade artifacts and mounts".to_string(),
                    socket_path: String::new(),
                }))
                .await
                .is_err()
            {
                return;
            }

            if let Err(error) = supervisor.ensure_started().await {
                warn!(request_id = %request_id, %error, "Docker facade startup validation failed");
                let _ = sender
                    .send(Err(Status::failed_precondition(format!(
                        "Docker facade startup failed: {error}"
                    ))))
                    .await;
                return;
            }

            for (stage, message) in [
                (Stage::Starting, "Guest-agent Docker supervision started"),
                (
                    Stage::Waiting,
                    "Waiting for the guest Docker Engine API socket",
                ),
            ] {
                if sender
                    .send(Ok(DockerEnsureEvent {
                        stage: stage as i32,
                        message: message.to_string(),
                        socket_path: String::new(),
                    }))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            match supervisor.wait_ready().await {
                Ok(socket_path) => {
                    info!(request_id = %request_id, socket_path, "Docker facade ready");
                    let _ = sender
                        .send(Ok(DockerEnsureEvent {
                            stage: Stage::Ready as i32,
                            message: "Docker facade ready".to_string(),
                            socket_path: socket_path.to_string(),
                        }))
                        .await;
                }
                Err(error) => {
                    warn!(request_id = %request_id, %error, "Docker facade startup failed");
                    let _ = sender
                        .send(Err(Status::failed_precondition(format!(
                            "Docker facade startup failed: {error}"
                        ))))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn stdin_write(
        &self,
        request: Request<StdinWriteRequest>,
    ) -> Result<Response<StdinWriteResponse>, Status> {
        let req = request.into_inner();
        if req.data.len() > MAX_STDIN_WRITE_BYTES {
            return Err(Status::resource_exhausted(format!(
                "stdin write exceeds {MAX_STDIN_WRITE_BYTES} bytes"
            )));
        }
        let control = begin_ordered_control(req.exec_id, "stdin_write")
            .await
            .ok_or_else(|| Status::not_found(format!("process {} not found", req.exec_id)))?;
        debug!(
            exec_id = req.exec_id,
            sequence = control.sequence,
            bytes = req.data.len(),
            "grpc: stdin write ordered"
        );

        // For PTY sessions, write to the master PTY writer.
        let pty_handle = {
            let handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            handles.get(&req.exec_id).cloned()
        };
        if let Some(handle) = pty_handle {
            let data = req.data.clone();
            let write = tokio::task::spawn_blocking(move || -> Result<(), Status> {
                use std::io::Write;
                let mut guard = handle.lock().unwrap_or_else(|p| p.into_inner());
                guard
                    .writer
                    .write_all(&data)
                    .map_err(|e| Status::internal(format!("pty write failed: {e}")))
            });
            match tokio::time::timeout(STDIN_WRITE_TIMEOUT, write).await {
                Ok(Ok(result)) => result?,
                Ok(Err(error)) => {
                    return Err(Status::internal(format!("pty write task failed: {error}")));
                }
                Err(_) => {
                    // The blocking writer may still own its Arc after this RPC
                    // returns. Retire the public handle and start durable
                    // cancellation so no additional writer tasks can queue.
                    pty_handles()
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&req.exec_id);
                    spawn_exec_cleanup(self.state.process_table.clone(), req.exec_id);
                    return Err(Status::deadline_exceeded(format!(
                        "PTY stdin write exceeded {} ms",
                        STDIN_WRITE_TIMEOUT.as_millis()
                    )));
                }
            }
            return Ok(Response::new(StdinWriteResponse {}));
        }

        write_pipe_stdin(&self.state.process_table, req.exec_id, &req.data).await?;

        Ok(Response::new(StdinWriteResponse {}))
    }

    async fn stdin_close(
        &self,
        request: Request<StdinCloseRequest>,
    ) -> Result<Response<StdinCloseResponse>, Status> {
        let req = request.into_inner();
        let control = begin_ordered_control(req.exec_id, "stdin_close")
            .await
            .ok_or_else(|| Status::not_found(format!("process {} not found", req.exec_id)))?;
        debug!(
            exec_id = req.exec_id,
            sequence = control.sequence,
            "grpc: stdin close ordered"
        );
        let mut table = self.state.process_table.lock().await;

        if let Some(entry) = table.get_mut(req.exec_id) {
            entry.stdin = None;
            info!(exec_id = req.exec_id, "grpc: stdin closed");
        } else {
            return Err(Status::not_found(format!(
                "process {} not found",
                req.exec_id
            )));
        }

        Ok(Response::new(StdinCloseResponse {}))
    }

    async fn signal(
        &self,
        request: Request<SignalRequest>,
    ) -> Result<Response<SignalResponse>, Status> {
        let req = request.into_inner();
        validate_exec_signal(req.signal)?;
        let control = begin_ordered_control(req.exec_id, "signal")
            .await
            .ok_or_else(|| Status::not_found(format!("process {} not found", req.exec_id)))?;
        debug!(
            exec_id = req.exec_id,
            sequence = control.sequence,
            signal = req.signal,
            "grpc: signal ordered"
        );
        let table = self.state.process_table.lock().await;

        match table.signal(req.exec_id, req.signal) {
            Some(Ok(())) => {
                info!(
                    exec_id = req.exec_id,
                    signal = req.signal,
                    "grpc: signal delivered to spawned process identity"
                );
            }
            Some(Err(error)) => {
                return Err(Status::failed_precondition(format!(
                    "cannot signal exec {}: {error}",
                    req.exec_id
                )));
            }
            None => {
                return Err(Status::not_found(format!(
                    "process {} not found",
                    req.exec_id
                )));
            }
        }

        Ok(Response::new(SignalResponse {}))
    }

    async fn cancel_exec(
        &self,
        request: Request<CancelExecRequest>,
    ) -> Result<Response<CancelExecResponse>, Status> {
        let req = request.into_inner();
        // Cancellation never waits behind stdin or another control future.
        // ProcessTable atomically starts or joins the one durable driver.
        let sequence = mark_nonblocking_control(req.exec_id, "cancel_exec");
        debug!(
            exec_id = req.exec_id,
            sequence, "grpc: cancellation ordered"
        );
        let outcome = cancel_active_exec(&self.state.process_table, req.exec_id).await?;
        Ok(Response::new(CancelExecResponse {
            exit_code: outcome.exit_code,
            forced: outcome.forced,
        }))
    }

    async fn reconcile_exec(
        &self,
        request: Request<ReconcileExecRequest>,
    ) -> Result<Response<ReconcileExecResponse>, Status> {
        use reconcile_exec_response::Outcome;

        let request = request.into_inner();
        if request.exec_request_id.is_empty() {
            return Err(Status::invalid_argument("exec_request_id is required"));
        }
        validate_container_exec_request_id(&request.exec_request_id)?;
        validate_reconcile_transport_metadata(request.metadata.as_ref())?;
        let registry = self.state.process_table.lock().await.request_registry();
        loop {
            match registry.reconcile(&request.exec_request_id) {
                ExecRequestReconcile::FencedNeverStarted => {
                    return Ok(Response::new(ReconcileExecResponse {
                        outcome: Outcome::FencedNeverStarted as i32,
                        exec_request_id: request.exec_request_id,
                        exec_id: 0,
                        exit_code: 0,
                        forced: false,
                    }));
                }
                ExecRequestReconcile::Starting => {
                    tokio::time::sleep(EXEC_CANCEL_DRIVER_RETRY).await;
                }
                ExecRequestReconcile::Published(exec_id) => {
                    let outcome = cancel_active_exec(&self.state.process_table, exec_id).await?;
                    return Ok(Response::new(ReconcileExecResponse {
                        outcome: Outcome::TerminalReaped as i32,
                        exec_request_id: request.exec_request_id,
                        exec_id,
                        exit_code: outcome.exit_code,
                        forced: outcome.forced,
                    }));
                }
                ExecRequestReconcile::Terminal(exec_id, receipt) => {
                    return Ok(Response::new(ReconcileExecResponse {
                        outcome: Outcome::TerminalReaped as i32,
                        exec_request_id: request.exec_request_id,
                        exec_id,
                        exit_code: receipt.exit_code,
                        forced: receipt.forced,
                    }));
                }
                ExecRequestReconcile::StaleUnknown => {
                    return Ok(Response::new(ReconcileExecResponse {
                        outcome: Outcome::StaleUnknown as i32,
                        exec_request_id: request.exec_request_id,
                        exec_id: 0,
                        exit_code: 0,
                        forced: false,
                    }));
                }
            }
        }
    }

    type DockerForwardStream = crate::docker_forward::DockerForwardStream;

    async fn docker_forward(
        &self,
        request: Request<tonic::Streaming<DockerForwardFrame>>,
    ) -> Result<Response<Self::DockerForwardStream>, Status> {
        let mut inbound = request.into_inner();
        let first = tokio::time::timeout(std::time::Duration::from_secs(5), inbound.message())
            .await
            .map_err(|_| Status::deadline_exceeded("Docker open timed out"))??;
        match first.and_then(|frame| frame.frame) {
            Some(docker_forward_frame::Frame::Open(open))
                if open
                    .metadata
                    .as_ref()
                    .is_some_and(|metadata| !metadata.request_id.is_empty()) => {}
            _ => {
                return Err(Status::invalid_argument(
                    "first Docker frame must be Open with request metadata",
                ));
            }
        }
        let target = self
            .state
            .docker_supervisor
            .connect_forward()
            .await
            .map_err(|error| {
                Status::failed_precondition(format!("Docker forwarding unavailable: {error}"))
            })?;
        Ok(Response::new(crate::docker_forward::start(inbound, target)))
    }

    type PortForwardStream = ReceiverStream<Result<PortForwardFrame, Status>>;

    async fn port_forward(
        &self,
        request: Request<tonic::Streaming<PortForwardFrame>>,
    ) -> Result<Response<Self::PortForwardStream>, Status> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut inbound = request.into_inner();

        // First frame must be PortForwardOpen.
        let first_frame = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("failed to read first frame: {e}")))?
            .ok_or_else(|| Status::invalid_argument("empty port forward stream"))?;

        let open = match first_frame.frame {
            Some(port_forward_frame::Frame::Open(open)) => open,
            _ => {
                return Err(Status::invalid_argument(
                    "first frame must be PortForwardOpen",
                ));
            }
        };

        if open.protocol != "tcp" {
            return Err(Status::invalid_argument(format!(
                "unsupported protocol: {}",
                open.protocol
            )));
        }

        let host = if open.target_host.is_empty() {
            "127.0.0.1"
        } else {
            &open.target_host
        };
        let port = open.target_port as u16;

        let target = crate::connect_port_forward_target(host, port)
            .await
            .map_err(|e| Status::unavailable(format!("failed to connect to {host}:{port}: {e}")))?;

        let (mut target_reader, mut target_writer) = target.into_split();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<PortForwardFrame, Status>>(64);

        // Task: read from target TCP socket, send as gRPC data frames.
        let reader_tx = tx.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 65536];
            loop {
                match target_reader.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if reader_tx
                            .send(Ok(PortForwardFrame {
                                frame: Some(port_forward_frame::Frame::Data(buf[..n].to_vec())),
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "grpc: port forward target read error");
                        break;
                    }
                }
            }
        });

        // Task: read gRPC data frames from client, write to target TCP socket.
        tokio::spawn(async move {
            while let Ok(Some(frame)) = inbound.message().await {
                if let Some(port_forward_frame::Frame::Data(data)) = frame.frame {
                    if let Err(e) = target_writer.write_all(&data).await {
                        warn!(error = %e, "grpc: port forward target write error");
                        break;
                    }
                }
            }
            // Client stream ended; shut down the target write side.
            let _ = target_writer.shutdown().await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn resize_exec_pty(
        &self,
        request: Request<ResizeExecPtyRequest>,
    ) -> Result<Response<ResizeExecPtyResponse>, Status> {
        use portable_pty::PtySize;

        let req = request.into_inner();
        let control = begin_ordered_control(req.exec_id, "resize_exec_pty")
            .await
            .ok_or_else(|| Status::not_found(format!("process {} not found", req.exec_id)))?;
        if req.rows == 0
            || req.cols == 0
            || req.rows > u32::from(u16::MAX)
            || req.cols > u32::from(u16::MAX)
        {
            return Err(Status::invalid_argument(
                "PTY rows and columns must be in 1..=65535",
            ));
        }
        let handle = {
            let handles = pty_handles().lock().unwrap_or_else(|p| p.into_inner());
            handles.get(&req.exec_id).cloned()
        }
        .ok_or_else(|| Status::not_found(format!("no PTY for exec {}", req.exec_id)))?;

        let rows = req.rows as u16;
        let cols = req.cols as u16;
        tokio::task::spawn_blocking(move || -> Result<(), Status> {
            let guard = handle.lock().unwrap_or_else(|p| p.into_inner());
            guard
                .master
                .resize(PtySize {
                    rows,
                    cols,
                    pixel_width: 0,
                    pixel_height: 0,
                })
                .map_err(|e| Status::internal(format!("pty resize failed: {e}")))
        })
        .await
        .map_err(|error| Status::internal(format!("pty resize task failed: {error}")))??;

        info!(
            exec_id = req.exec_id,
            sequence = control.sequence,
            rows = req.rows,
            cols = req.cols,
            "grpc: pty resized"
        );
        Ok(Response::new(ResizeExecPtyResponse {}))
    }
}

// ── OciService ──────────────────────────────────────────────────────

/// Path to the youki OCI runtime binary (delivered via VirtioFS).
#[cfg(target_os = "linux")]
const YOUKI_BIN: &str = crate::container_exec::YOUKI_BIN;

/// Root directory for youki container state.
#[cfg(target_os = "linux")]
const YOUKI_ROOT: &str = crate::container_exec::YOUKI_ROOT;

/// gRPC implementation of the `OciService` trait.
///
/// On Linux guests, delegates to the youki OCI runtime for container
/// lifecycle management. On other platforms, returns `UNIMPLEMENTED`.
pub struct OciServiceImpl;

#[cfg(target_os = "linux")]
async fn acquire_exclusive_container_admission(
    container_id: &str,
) -> Result<crate::container_exec::ContainerAdmissionGuard, Status> {
    let container_id = container_id.to_string();
    tokio::task::spawn_blocking(move || {
        crate::container_exec::ContainerAdmissionGuard::exclusive(&container_id)
    })
    .await
    .map_err(|error| Status::internal(format!("container admission task failed: {error}")))?
    .map_err(|error| Status::failed_precondition(error.to_string()))
}

#[cfg(target_os = "linux")]
async fn acquire_shared_container_admission(
    container_id: &str,
) -> Result<crate::container_exec::ContainerAdmissionGuard, Status> {
    let container_id = container_id.to_string();
    tokio::task::spawn_blocking(move || {
        crate::container_exec::ContainerAdmissionGuard::shared(&container_id)
    })
    .await
    .map_err(|error| Status::internal(format!("container admission task failed: {error}")))?
    .map_err(|error| Status::failed_precondition(error.to_string()))
}

#[cfg(target_os = "linux")]
#[tonic::async_trait]
impl oci_service_server::OciService for OciServiceImpl {
    async fn create(
        &self,
        request: Request<OciCreateRequest>,
    ) -> Result<Response<OciCreateResponse>, Status> {
        let req = request.into_inner();
        let _admission = acquire_exclusive_container_admission(&req.container_id).await?;
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-create");
        info!(
            request_id = %request_id,
            container_id = %req.container_id,
            bundle_path = %req.bundle_path,
            "oci: create"
        );

        // Patch the OCI config to work in the minimal guest VM kernel.
        let config_path = format!("{}/config.json", req.bundle_path);
        match patch_oci_config(&config_path).await {
            Ok(()) => info!(container_id = %req.container_id, "oci: config patched for guest VM"),
            Err(e) => {
                error!(container_id = %req.container_id, error = %e, "oci: failed to patch config")
            }
        }

        // Log bundle config for diagnostics.
        match tokio::fs::read_to_string(&config_path).await {
            Ok(config) => {
                info!(container_id = %req.container_id, config = %config, "oci: bundle config");

                let mountinfo = tokio::fs::read_to_string("/proc/self/mountinfo")
                    .await
                    .unwrap_or_else(|error| format!("<unavailable: {error}>"));
                let diagnostic = inspect_oci_rootfs(&req.bundle_path, &config, &mountinfo)
                    .map_err(|diagnostic| {
                        error!(container_id = %req.container_id, %diagnostic, "oci: rootfs preflight failed");
                        Status::failed_precondition(diagnostic)
                    })?;
                info!(
                    container_id = %req.container_id,
                    configured_rootfs = %diagnostic.configured.display(),
                    resolved_rootfs = %diagnostic.resolved.display(),
                    canonical_rootfs = %diagnostic.canonical.display(),
                    mountinfo = %diagnostic.mountinfo,
                    "oci: rootfs preflight passed"
                );

                ensure_youki_user_namespace_procfs(Path::new("/proc/self/uid_map")).map_err(
                    |kernel_diagnostic| {
                        let diagnostic = format!(
                            "{kernel_diagnostic}; config.root.path={} canonical={} mountinfo={}",
                            diagnostic.configured.display(),
                            diagnostic.canonical.display(),
                            diagnostic.mountinfo
                        );
                        error!(container_id = %req.container_id, %diagnostic, "oci: youki kernel preflight failed");
                        Status::failed_precondition(diagnostic)
                    },
                )?;
            }
            Err(e) => {
                error!(container_id = %req.container_id, error = %e, "oci: failed to read bundle config");
                return Err(Status::failed_precondition(format!(
                    "OCI bundle preflight failed: cannot read {config_path}: {e}"
                )));
            }
        }

        run_youki(&["create", "--bundle", &req.bundle_path, &req.container_id]).await?;
        Ok(Response::new(OciCreateResponse {}))
    }

    async fn start(
        &self,
        request: Request<OciStartRequest>,
    ) -> Result<Response<OciStartResponse>, Status> {
        let req = request.into_inner();
        let _admission = acquire_exclusive_container_admission(&req.container_id).await?;
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-start");
        info!(request_id = %request_id, container_id = %req.container_id, "oci: start");

        run_youki(&["start", &req.container_id]).await?;
        Ok(Response::new(OciStartResponse {}))
    }

    async fn state(
        &self,
        request: Request<OciStateRequest>,
    ) -> Result<Response<OciStateResponse>, Status> {
        let req = request.into_inner();
        let _admission = acquire_shared_container_admission(&req.container_id).await?;
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-state");
        debug!(request_id = %request_id, container_id = %req.container_id, "oci: state");

        let output =
            run_youki_output(&["state", &req.container_id], YOUKI_LIFECYCLE_TIMEOUT).await?;
        let state: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| Status::internal(format!("failed to parse youki state: {e}")))?;

        Ok(Response::new(OciStateResponse {
            container_id: state["id"].as_str().unwrap_or("").to_string(),
            status: state["status"].as_str().unwrap_or("unknown").to_string(),
            pid: state["pid"].as_u64().unwrap_or(0) as u32,
            bundle_path: state["bundle"].as_str().unwrap_or("").to_string(),
        }))
    }

    async fn exec(
        &self,
        _request: Request<OciExecRequest>,
    ) -> Result<Response<OciExecResponse>, Status> {
        Err(Status::unimplemented(
            "OciService.Exec is retired; collect the supervised AgentService.Exec stream",
        ))
    }

    async fn kill(
        &self,
        request: Request<OciKillRequest>,
    ) -> Result<Response<OciKillResponse>, Status> {
        let req = request.into_inner();
        let _admission = acquire_exclusive_container_admission(&req.container_id).await?;
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-kill");
        info!(
            request_id = %request_id,
            container_id = %req.container_id,
            signal = %req.signal,
            "oci: kill"
        );

        run_youki(&["kill", &req.container_id, &req.signal]).await?;
        Ok(Response::new(OciKillResponse {}))
    }

    async fn delete(
        &self,
        request: Request<OciDeleteRequest>,
    ) -> Result<Response<OciDeleteResponse>, Status> {
        let req = request.into_inner();
        let _admission = acquire_exclusive_container_admission(&req.container_id).await?;
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "oci-delete");
        info!(
            request_id = %request_id,
            container_id = %req.container_id,
            force = req.force,
            "oci: delete"
        );

        let mut id_components = Path::new(&req.container_id).components();
        let state_name = match (id_components.next(), id_components.next()) {
            (Some(std::path::Component::Normal(name)), None) => name,
            _ => return Err(Status::invalid_argument("invalid OCI container ID")),
        };
        let state_path = Path::new(YOUKI_ROOT).join(state_name);
        if req.force && !state_path.exists() {
            debug!(
                request_id = %request_id,
                container_id = %req.container_id,
                "oci: forced delete is already complete"
            );
            return Ok(Response::new(OciDeleteResponse {}));
        }

        if req.force {
            let delete = run_youki(&["delete", "--force", &req.container_id]).await;
            if delete.is_err() && !state_path.exists() {
                // youki may report failure after another cleanup wins the race.
                // Exact absence from its configured state root makes forced
                // delete idempotently complete; transport and genuine cleanup
                // failures retain state and continue to surface.
                debug!(
                    request_id = %request_id,
                    container_id = %req.container_id,
                    "oci: forced delete completed concurrently"
                );
            } else {
                delete?;
            }
        } else {
            run_youki(&["delete", &req.container_id]).await?;
        }
        Ok(Response::new(OciDeleteResponse {}))
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, PartialEq, Eq)]
struct OciRootfsDiagnostic {
    configured: PathBuf,
    resolved: PathBuf,
    canonical: PathBuf,
    mountinfo: String,
}

/// Resolve the OCI rootfs exactly as youki does and retain the governing
/// mountinfo entry. This turns otherwise context-free ENOENT failures into an
/// actionable guest diagnostic before youki forks its init process.
#[cfg(any(target_os = "linux", test))]
fn inspect_oci_rootfs(
    bundle_path: &str,
    config_json: &str,
    mountinfo: &str,
) -> Result<OciRootfsDiagnostic, String> {
    let config: serde_json::Value = serde_json::from_str(config_json)
        .map_err(|error| format!("OCI rootfs preflight failed: invalid config.json: {error}"))?;
    let configured = config
        .pointer("/root/path")
        .and_then(serde_json::Value::as_str)
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| "OCI rootfs preflight failed: config.root.path is missing".to_string())?;
    let resolved = if configured.is_absolute() {
        configured.clone()
    } else {
        Path::new(bundle_path).join(&configured)
    };
    let governing_mount = governing_mountinfo_entry(mountinfo, &resolved)
        .unwrap_or_else(|| "<no matching mountinfo entry>".to_string());
    let canonical = std::fs::canonicalize(&resolved).map_err(|error| {
        format!(
            "OCI rootfs preflight failed: config.root.path={} resolved={} cannot be canonicalized: {error}; mountinfo={governing_mount}",
            configured.display(),
            resolved.display()
        )
    })?;
    if !canonical.is_dir() {
        return Err(format!(
            "OCI rootfs preflight failed: config.root.path={} canonical={} is not a directory; mountinfo={governing_mount}",
            configured.display(),
            canonical.display()
        ));
    }

    Ok(OciRootfsDiagnostic {
        configured,
        resolved,
        canonical,
        mountinfo: governing_mount,
    })
}

#[cfg(any(target_os = "linux", test))]
fn governing_mountinfo_entry(mountinfo: &str, path: &Path) -> Option<String> {
    mountinfo
        .lines()
        .filter_map(|line| {
            let mount_point = line.split_whitespace().nth(4)?;
            let mount_point = Path::new(mount_point);
            path.starts_with(mount_point)
                .then(|| (mount_point.components().count(), line.to_string()))
        })
        .max_by_key(|(depth, _)| *depth)
        .map(|(_, line)| line)
}

/// Youki 0.5.7 unconditionally probes this procfs file from its init process.
/// It is absent when CONFIG_USER_NS is disabled, which youki reports only as
/// `io error: ENOENT` and can easily be mistaken for a missing OCI rootfs.
#[cfg(any(target_os = "linux", test))]
fn ensure_youki_user_namespace_procfs(uid_map_path: &Path) -> Result<(), String> {
    std::fs::read_to_string(uid_map_path)
        .map(|_| ())
        .map_err(|error| {
            format!(
                "youki kernel preflight failed: {} is unavailable: {error}; the guest kernel must enable CONFIG_USER_NS=y",
                uid_map_path.display()
            )
        })
}

/// Timeout for youki lifecycle commands (create, start, kill, delete).
#[cfg(target_os = "linux")]
const YOUKI_LIFECYCLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Ensure the youki state directory exists.
#[cfg(target_os = "linux")]
fn ensure_youki_state_dir() {
    let _ = std::fs::create_dir_all(YOUKI_ROOT);
}

/// Run a youki lifecycle command (create, start, kill, delete) and check for
/// success. Uses null stdio to avoid blocking on long-lived child processes
/// that inherit pipe FDs.
#[cfg(target_os = "linux")]
async fn run_youki(args: &[&str]) -> Result<(), Status> {
    ensure_youki_state_dir();
    let _ = std::fs::create_dir_all(YOUKI_LOG_DIR);

    let subcmd = args.first().unwrap_or(&"unknown");
    let container_id = args.last().unwrap_or(&"unknown");
    let log_file = format!("{YOUKI_LOG_DIR}/{container_id}-{subcmd}.log");

    let mut cmd = tokio::process::Command::new(YOUKI_BIN);
    cmd.arg("--root").arg(YOUKI_ROOT);
    cmd.arg("--log").arg(&log_file);
    if *subcmd == "create" {
        cmd.arg("--log-level").arg("debug");
    }
    cmd.kill_on_drop(true);
    // Lifecycle commands (create, start) fork child processes that inherit
    // pipe FDs. Using null stdio ensures wait() returns as soon as the
    // youki parent exits, without blocking on the init process.
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    for arg in args {
        cmd.arg(arg);
    }

    let cmd_desc = format!("youki {}", args.join(" "));
    info!(cmd = %cmd_desc, log_file = %log_file, "executing youki command");

    let mut child = cmd.spawn().map_err(|e| {
        error!(cmd = %cmd_desc, error = %e, "failed to spawn youki");
        Status::internal(format!("failed to execute youki: {e}"))
    })?;

    let status = match tokio::time::timeout(YOUKI_LIFECYCLE_TIMEOUT, child.wait()).await {
        Ok(Ok(status)) => status,
        Ok(Err(e)) => {
            error!(cmd = %cmd_desc, error = %e, "failed to wait for youki");
            dump_youki_log(&log_file).await;
            return Err(Status::internal(format!("youki {subcmd} failed: {e}")));
        }
        Err(_) => {
            error!(cmd = %cmd_desc, timeout_secs = YOUKI_LIFECYCLE_TIMEOUT.as_secs(), "youki command timed out");
            dump_youki_log(&log_file).await;
            return Err(Status::internal(format!(
                "{cmd_desc} timed out after {}s",
                YOUKI_LIFECYCLE_TIMEOUT.as_secs()
            )));
        }
    };

    if !status.success() {
        let youki_log = tokio::fs::read_to_string(&log_file)
            .await
            .unwrap_or_default();
        error!(command = %subcmd, log = %youki_log, "youki command failed");
        // Include the last few lines of the youki log in the error response
        // so the host can surface them without needing VM access.
        let log_tail: String = youki_log
            .lines()
            .rev()
            .take(10)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect::<Vec<_>>()
            .join("\n");
        let exit_code = status.code().unwrap_or(-1);
        return Err(Status::internal(if log_tail.is_empty() {
            format!("youki {subcmd} failed (exit {exit_code}): no log output")
        } else {
            format!("youki {subcmd} failed (exit {exit_code}): {log_tail}")
        }));
    }

    Ok(())
}

/// Directory for youki log files.
#[cfg(target_os = "linux")]
const YOUKI_LOG_DIR: &str = "/run/vz-oci/logs";

/// Run a youki command and return the raw output (success or failure).
#[cfg(target_os = "linux")]
async fn run_youki_output(
    args: &[&str],
    timeout: std::time::Duration,
) -> Result<std::process::Output, Status> {
    ensure_youki_state_dir();
    let _ = std::fs::create_dir_all(YOUKI_LOG_DIR);

    // Generate a unique log file for this invocation.
    let subcmd = args.first().unwrap_or(&"unknown");
    let container_id = args.last().unwrap_or(&"unknown");
    let log_file = format!("{YOUKI_LOG_DIR}/{container_id}-{subcmd}.log");

    let mut cmd = tokio::process::Command::new(YOUKI_BIN);
    cmd.arg("--root").arg(YOUKI_ROOT);
    cmd.arg("--log").arg(&log_file);
    cmd.kill_on_drop(true);
    for arg in args {
        cmd.arg(arg);
    }

    let cmd_desc = format!("youki {}", args.join(" "));
    info!(cmd = %cmd_desc, log_file = %log_file, "executing youki command");

    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(output)) => Ok(output),
        Ok(Err(e)) => {
            error!(cmd = %cmd_desc, error = %e, "failed to execute youki");
            dump_youki_log(&log_file).await;
            Err(Status::internal(format!("failed to execute youki: {e}")))
        }
        Err(_) => {
            error!(cmd = %cmd_desc, timeout_secs = timeout.as_secs(), "youki command timed out");
            dump_youki_log(&log_file).await;
            Err(Status::internal(format!(
                "{cmd_desc} timed out after {}s",
                timeout.as_secs()
            )))
        }
    }
}

/// Patch OCI config.json to be compatible with the minimal guest VM kernel.
///
/// The guest VM runs a stripped kernel that may lack certain filesystem types
/// (e.g. mqueue and cgroup v1). This function removes or adjusts mounts that
/// would cause youki to fail or hang while preserving the cgroup v2 surface
/// required for OCI resource enforcement.
#[cfg(target_os = "linux")]
async fn patch_oci_config(config_path: &str) -> Result<(), Status> {
    let content = tokio::fs::read_to_string(config_path)
        .await
        .map_err(|e| Status::internal(format!("read config.json: {e}")))?;

    let mut config: serde_json::Value = serde_json::from_str(&content)
        .map_err(|e| Status::internal(format!("parse config.json: {e}")))?;

    normalize_oci_config(&mut config);

    let patched = serde_json::to_string_pretty(&config)
        .map_err(|e| Status::internal(format!("serialize config.json: {e}")))?;

    tokio::fs::write(config_path, patched)
        .await
        .map_err(|e| Status::internal(format!("write config.json: {e}")))?;

    Ok(())
}

/// Normalize an OCI config for the guest kernel without discarding the
/// read-only cgroup v2 view used to inspect and enforce container resources.
#[cfg(any(target_os = "linux", test))]
fn normalize_oci_config(config: &mut serde_json::Value) {
    // Remove mounts with filesystem types not available in the minimal kernel.
    // cgroup2 is explicitly supported and must retain the read-only options
    // supplied by the host bundle. Types that still hang or fail include
    // mqueue (CONFIG_POSIX_MQUEUE), devpts, sysfs, and cgroup v1.
    if let Some(mounts) = config.pointer_mut("/mounts").and_then(|v| v.as_array_mut()) {
        let supported_types = ["proc", "tmpfs", "bind", "cgroup2"];
        mounts.retain(|m| {
            let typ = m.get("type").and_then(|t| t.as_str()).unwrap_or("");
            if !supported_types.contains(&typ) {
                tracing::info!(
                    mount_type = typ,
                    "stripping unsupported mount type from OCI config"
                );
                false
            } else {
                true
            }
        });
    }

    // Strip maskedPaths, readonlyPaths, and unsupported namespaces — the
    // minimal VM kernel doesn't support all namespace types youki tries to
    // unshare, and masked/readonly paths reference /proc and /sys paths that
    // may not exist, causing youki to hang.
    if let Some(linux) = config.pointer_mut("/linux") {
        if let Some(obj) = linux.as_object_mut() {
            if obj.remove("maskedPaths").is_some() {
                tracing::info!("stripped maskedPaths from OCI config");
            }
            if obj.remove("readonlyPaths").is_some() {
                tracing::info!("stripped readonlyPaths from OCI config");
            }
            // Strip unsupported namespaces but preserve mount, network, and
            // cgroup. The cgroup namespace makes the container's delegated
            // cgroup appear at the root of its read-only cgroup2 mount.
            // Network namespaces MUST be preserved — multi-service stacks
            // use per-service netns (e.g. /var/run/netns/svc-web) for
            // container network isolation and service discovery.
            if let Some(namespaces) = obj.get_mut("namespaces").and_then(|v| v.as_array_mut()) {
                let before = namespaces.len();
                namespaces.retain(|ns| {
                    let typ = ns.get("type").and_then(|t| t.as_str()).unwrap_or("");
                    matches!(typ, "mount" | "network" | "cgroup")
                });
                let stripped = before - namespaces.len();
                if stripped > 0 {
                    tracing::info!(stripped, "stripped unsupported namespaces from OCI config");
                }
            }
        }
    }
}

/// Read and log the contents of a youki log file for diagnostics.
#[cfg(target_os = "linux")]
async fn dump_youki_log(path: &str) {
    match tokio::fs::read_to_string(path).await {
        Ok(contents) if !contents.is_empty() => {
            error!(log_file = %path, contents = %contents, "youki log file contents");
        }
        Ok(_) => {
            warn!(log_file = %path, "youki log file is empty");
        }
        Err(e) => {
            warn!(log_file = %path, error = %e, "could not read youki log file");
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[tonic::async_trait]
impl oci_service_server::OciService for OciServiceImpl {
    async fn create(
        &self,
        _request: Request<OciCreateRequest>,
    ) -> Result<Response<OciCreateResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn start(
        &self,
        _request: Request<OciStartRequest>,
    ) -> Result<Response<OciStartResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn state(
        &self,
        _request: Request<OciStateRequest>,
    ) -> Result<Response<OciStateResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn exec(
        &self,
        _request: Request<OciExecRequest>,
    ) -> Result<Response<OciExecResponse>, Status> {
        Err(Status::unimplemented(
            "OciService.Exec is retired; collect the supervised AgentService.Exec stream",
        ))
    }

    async fn kill(
        &self,
        _request: Request<OciKillRequest>,
    ) -> Result<Response<OciKillResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }

    async fn delete(
        &self,
        _request: Request<OciDeleteRequest>,
    ) -> Result<Response<OciDeleteResponse>, Status> {
        Err(Status::unimplemented("OCI lifecycle requires Linux guest"))
    }
}

// ── NetworkService ──────────────────────────────────────────────────

/// gRPC implementation of the `NetworkService` trait.
pub struct NetworkServiceImpl;

#[tonic::async_trait]
impl network_service_server::NetworkService for NetworkServiceImpl {
    async fn setup(
        &self,
        request: Request<NetworkSetupRequest>,
    ) -> Result<Response<NetworkSetupResponse>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "network-setup");
        debug!(
            request_id = %request_id,
            stack_id = %req.stack_id,
            services = req.services.len(),
            "grpc: network setup request"
        );
        do_network_setup(&req.stack_id, &req.services)
    }

    async fn teardown(
        &self,
        request: Request<NetworkTeardownRequest>,
    ) -> Result<Response<NetworkTeardownResponse>, Status> {
        let req = request.into_inner();
        let request_id = request_id_from_metadata(req.metadata.as_ref(), "network-teardown");
        debug!(
            request_id = %request_id,
            stack_id = %req.stack_id,
            services = req.service_names.len(),
            "grpc: network teardown request"
        );
        do_network_teardown(&req.stack_id, &req.service_names).await
    }
}

#[cfg(target_os = "linux")]
fn do_network_setup(
    stack_id: &str,
    services: &[vz_agent_proto::NetworkServiceConfig],
) -> Result<Response<NetworkSetupResponse>, Status> {
    // Convert proto NetworkServiceConfig to vz protocol NetworkServiceConfig.
    let vz_services: Vec<::vz::protocol::NetworkServiceConfig> = services
        .iter()
        .map(|s| ::vz::protocol::NetworkServiceConfig {
            name: s.name.clone(),
            addr: s.addr.clone(),
            network_name: s.network_name.clone(),
        })
        .collect();

    match crate::network::setup_stack_network(stack_id, &vz_services) {
        Ok(()) => {
            info!(stack_id = %stack_id, services = services.len(), "grpc: network setup complete");
            Ok(Response::new(NetworkSetupResponse {}))
        }
        Err(e) => {
            error!(stack_id = %stack_id, error = %e, "grpc: network setup failed");
            Err(Status::internal(format!("network setup failed: {e}")))
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn do_network_setup(
    _stack_id: &str,
    _services: &[vz_agent_proto::NetworkServiceConfig],
) -> Result<Response<NetworkSetupResponse>, Status> {
    Err(Status::unimplemented("network setup requires Linux"))
}

#[cfg(target_os = "linux")]
async fn do_network_teardown(
    stack_id: &str,
    service_names: &[String],
) -> Result<Response<NetworkTeardownResponse>, Status> {
    let stack_id_owned = stack_id.to_string();
    let service_names_owned = service_names.to_vec();

    let result = tokio::task::spawn_blocking(move || {
        crate::network::teardown_stack_network(&stack_id_owned, &service_names_owned)
    })
    .await;

    match result {
        Ok(Ok(())) => {
            info!(stack_id = %stack_id, "grpc: network teardown complete");
            Ok(Response::new(NetworkTeardownResponse {}))
        }
        Ok(Err(e)) => {
            error!(stack_id = %stack_id, error = %e, "grpc: network teardown failed");
            Err(Status::internal(format!("network teardown failed: {e}")))
        }
        Err(e) => {
            error!(stack_id = %stack_id, error = %e, "grpc: network teardown task panicked");
            Err(Status::internal(format!("task panicked: {e}")))
        }
    }
}

#[cfg(not(target_os = "linux"))]
async fn do_network_teardown(
    _stack_id: &str,
    _service_names: &[String],
) -> Result<Response<NetworkTeardownResponse>, Status> {
    Err(Status::unimplemented("network teardown requires Linux"))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::ffi::OsString;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;

    fn test_exec_id() -> u64 {
        static NEXT_EXEC_ID: AtomicU64 = AtomicU64::new(10_000);
        NEXT_EXEC_ID.fetch_add(1, Ordering::Relaxed)
    }

    #[derive(Debug)]
    struct PendingPipeTestState {
        polls: std::sync::atomic::AtomicUsize,
        kills: std::sync::atomic::AtomicUsize,
        errors_remaining: std::sync::atomic::AtomicUsize,
        allow_reap: std::sync::atomic::AtomicBool,
        reaped: std::sync::atomic::AtomicBool,
    }

    #[derive(Debug)]
    struct PendingPipeTestChild {
        state: Arc<PendingPipeTestState>,
    }

    impl PendingPipeProcess for PendingPipeTestChild {
        fn process_id(&self) -> Option<u32> {
            Some(42_423)
        }

        fn start_kill(&mut self) -> std::io::Result<()> {
            self.state
                .kills
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            Ok(())
        }

        fn try_wait_reaped(&mut self) -> std::io::Result<bool> {
            self.state
                .polls
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
            if self
                .state
                .errors_remaining
                .fetch_update(
                    std::sync::atomic::Ordering::AcqRel,
                    std::sync::atomic::Ordering::Acquire,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err(std::io::Error::other("injected pipe wait failure"));
            }
            if !self
                .state
                .allow_reap
                .load(std::sync::atomic::Ordering::Acquire)
            {
                return Err(std::io::Error::other("persistent pipe wait failure"));
            }
            self.state
                .reaped
                .store(true, std::sync::atomic::Ordering::Release);
            Ok(true)
        }
    }

    fn pending_pipe_test_child(
        errors: usize,
        allow_reap: bool,
    ) -> (
        PendingPipeChild<PendingPipeTestChild>,
        Arc<PendingPipeTestState>,
    ) {
        let state = Arc::new(PendingPipeTestState {
            polls: std::sync::atomic::AtomicUsize::new(0),
            kills: std::sync::atomic::AtomicUsize::new(0),
            errors_remaining: std::sync::atomic::AtomicUsize::new(errors),
            allow_reap: std::sync::atomic::AtomicBool::new(allow_reap),
            reaped: std::sync::atomic::AtomicBool::new(false),
        });
        (
            PendingPipeChild::new(PendingPipeTestChild {
                state: Arc::clone(&state),
            }),
            state,
        )
    }

    async fn await_pending_pipe_test_reap(state: &PendingPipeTestState) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !state.reaped.load(std::sync::atomic::Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("retained pipe cleanup did not reach terminal reap proof");
    }

    #[tokio::test]
    async fn pending_pipe_rejection_retries_wait_errors_before_definite_error() {
        use tokio_stream::StreamExt as _;

        let (child, state) = pending_pipe_test_child(2, true);
        let response = reject_pending_pipe_with_timeout(
            child,
            "pipe-definite-reject",
            Status::failed_precondition("injected post-spawn rejection"),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(1),
        )
        .await
        .expect("eventual exact reap proof must permit a definite rejection");
        assert!(state.reaped.load(std::sync::atomic::Ordering::Acquire));
        assert!(state.polls.load(std::sync::atomic::Ordering::Acquire) >= 3);
        let event = response
            .into_inner()
            .next()
            .await
            .expect("definite rejection stream omitted its event")
            .expect("definite rejection stream returned a transport status");
        assert_eq!(event.exec_id, 0);
        assert!(matches!(
            event.event,
            Some(exec_event::Event::Error(detail))
                if detail.contains("spawned process reaped")
        ));
    }

    #[tokio::test]
    async fn pending_pipe_persistent_wait_error_is_bounded_and_ambiguous() {
        let (child, state) = pending_pipe_test_child(0, false);
        let error = reject_pending_pipe_with_timeout(
            child,
            "pipe-retained-reject",
            Status::failed_precondition("injected post-spawn rejection"),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(5),
        )
        .await
        .expect_err("unproven pipe cleanup must not emit a definite Error frame");
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("retained authority"));
        assert!(!state.reaped.load(std::sync::atomic::Ordering::Acquire));
        state
            .allow_reap
            .store(true, std::sync::atomic::Ordering::Release);
        await_pending_pipe_test_reap(&state).await;
    }

    #[tokio::test]
    async fn pending_pipe_cleanup_keeps_request_starting_until_reap() {
        let registry = ProcessTable::new().request_registry();
        let request_id = registry.allocate_request_id().unwrap();
        let mut permit = registry.claim(&request_id).unwrap();
        permit.authorize_start().unwrap();
        let (mut child, state) = pending_pipe_test_child(0, false);
        child.request_permit = RetainedExecRequestPermit {
            permit: Some(permit),
            preserve_supervisor: false,
        };
        let error = reject_pending_pipe_with_timeout(
            child,
            &request_id,
            Status::failed_precondition("injected post-spawn rejection"),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(2),
        )
        .await
        .expect_err("unreaped child must remain ambiguous");
        assert_eq!(error.code(), tonic::Code::Unavailable);
        for _ in 0..5 {
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::Starting
            );
            assert!(!state.reaped.load(std::sync::atomic::Ordering::Acquire));
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        state
            .allow_reap
            .store(true, std::sync::atomic::Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match registry.reconcile(&request_id) {
                    ExecRequestReconcile::FencedNeverStarted => {
                        assert!(state.reaped.load(std::sync::atomic::Ordering::Acquire));
                        break;
                    }
                    ExecRequestReconcile::Starting => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    state => panic!("unexpected cleanup reconcile state: {state:?}"),
                }
            }
        })
        .await
        .expect("pipe cleanup did not fence after reap");
    }

    #[tokio::test]
    async fn dropped_pending_pipe_child_retains_cleanup_through_wait_errors() {
        let (child, state) = pending_pipe_test_child(3, true);
        drop(child);
        await_pending_pipe_test_reap(&state).await;
        assert!(state.polls.load(std::sync::atomic::Ordering::Acquire) >= 4);
    }

    #[derive(Debug)]
    struct PendingPtyTestState {
        polls: std::sync::atomic::AtomicUsize,
        kills: std::sync::atomic::AtomicUsize,
        poll_errors: usize,
        reap_poll: usize,
        kill_errors: usize,
        allow_reap: std::sync::atomic::AtomicBool,
        reaped: std::sync::atomic::AtomicBool,
    }

    #[derive(Debug)]
    struct PendingPtyTestChild {
        state: Arc<PendingPtyTestState>,
    }

    #[derive(Debug)]
    struct PendingPtyTestKiller {
        state: Arc<PendingPtyTestState>,
    }

    fn pending_pty_test_kill(state: &PendingPtyTestState) -> std::io::Result<()> {
        let attempt = state
            .kills
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;
        if attempt <= state.kill_errors {
            Err(std::io::Error::other("injected PTY kill failure"))
        } else {
            Ok(())
        }
    }

    impl portable_pty::ChildKiller for PendingPtyTestKiller {
        fn kill(&mut self) -> std::io::Result<()> {
            pending_pty_test_kill(&self.state)
        }

        fn clone_killer(&self) -> Box<dyn portable_pty::ChildKiller + Send + Sync> {
            Box::new(Self {
                state: Arc::clone(&self.state),
            })
        }
    }

    impl portable_pty::ChildKiller for PendingPtyTestChild {
        fn kill(&mut self) -> std::io::Result<()> {
            pending_pty_test_kill(&self.state)
        }

        fn clone_killer(&self) -> Box<dyn portable_pty::ChildKiller + Send + Sync> {
            Box::new(PendingPtyTestKiller {
                state: Arc::clone(&self.state),
            })
        }
    }

    impl portable_pty::Child for PendingPtyTestChild {
        fn try_wait(&mut self) -> std::io::Result<Option<portable_pty::ExitStatus>> {
            let poll = self
                .state
                .polls
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
                + 1;
            if poll <= self.state.poll_errors {
                return Err(std::io::Error::other("injected PTY wait failure"));
            }
            if poll >= self.state.reap_poll
                && self
                    .state
                    .allow_reap
                    .load(std::sync::atomic::Ordering::Acquire)
            {
                self.state
                    .reaped
                    .store(true, std::sync::atomic::Ordering::Release);
                return Ok(Some(portable_pty::ExitStatus::with_exit_code(137)));
            }
            Ok(None)
        }

        fn wait(&mut self) -> std::io::Result<portable_pty::ExitStatus> {
            loop {
                if let Some(status) = self.try_wait()? {
                    return Ok(status);
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }

        fn process_id(&self) -> Option<u32> {
            Some(42_424)
        }
    }

    fn pending_pty_test_child(
        poll_errors: usize,
        reap_poll: usize,
        kill_errors: usize,
    ) -> (PendingPtyChild, Arc<PendingPtyTestState>) {
        let state = Arc::new(PendingPtyTestState {
            polls: std::sync::atomic::AtomicUsize::new(0),
            kills: std::sync::atomic::AtomicUsize::new(0),
            poll_errors,
            reap_poll,
            kill_errors,
            allow_reap: std::sync::atomic::AtomicBool::new(true),
            reaped: std::sync::atomic::AtomicBool::new(false),
        });
        (
            PendingPtyChild::new(
                Box::new(PendingPtyTestChild {
                    state: Arc::clone(&state),
                }),
                None,
            ),
            state,
        )
    }

    async fn await_pending_pty_test_reap(state: &PendingPtyTestState) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !state.reaped.load(std::sync::atomic::Ordering::Acquire) {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("retained PTY cleanup did not reach terminal reap proof");
    }

    #[tokio::test]
    async fn pending_pty_rejection_emits_definite_error_only_after_reap_proof() {
        use tokio_stream::StreamExt as _;

        let (child, state) = pending_pty_test_child(2, 4, 2);
        let response = reject_pending_pty_with_timeout(
            child,
            "pty-definite-reject",
            Status::failed_precondition("injected post-spawn rejection"),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_millis(1),
        )
        .await
        .expect("eventual exact reap proof must permit a definite rejection");
        assert!(state.reaped.load(std::sync::atomic::Ordering::Acquire));
        let event = response
            .into_inner()
            .next()
            .await
            .expect("definite rejection stream omitted its event")
            .expect("definite rejection stream returned a transport status");
        assert_eq!(event.exec_id, 0);
        assert!(matches!(
            event.event,
            Some(exec_event::Event::Error(detail))
                if detail.contains("spawned process reaped")
        ));
    }

    #[tokio::test]
    async fn pending_pty_rejection_timeout_is_ambiguous_while_cleanup_retains_owner() {
        let (child, state) = pending_pty_test_child(2, 6, usize::MAX);
        let error = reject_pending_pty_with_timeout(
            child,
            "pty-retained-reject",
            Status::failed_precondition("injected post-spawn rejection"),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(5),
        )
        .await
        .expect_err("unproven PTY cleanup must not emit a definite Error frame");
        assert_eq!(error.code(), tonic::Code::Unavailable);
        assert!(error.message().contains("retained authority"));
        assert!(!state.reaped.load(std::sync::atomic::Ordering::Acquire));
        await_pending_pty_test_reap(&state).await;
    }

    #[tokio::test]
    async fn pending_pty_cleanup_keeps_request_starting_until_reap() {
        let registry = ProcessTable::new().request_registry();
        let request_id = registry.allocate_request_id().unwrap();
        let mut permit = registry.claim(&request_id).unwrap();
        permit.authorize_start().unwrap();
        let (mut child, state) = pending_pty_test_child(0, 1, usize::MAX);
        state
            .allow_reap
            .store(false, std::sync::atomic::Ordering::Release);
        child.request_permit = RetainedExecRequestPermit {
            permit: Some(permit),
            preserve_supervisor: false,
        };
        let error = reject_pending_pty_with_timeout(
            child,
            &request_id,
            Status::failed_precondition("injected post-spawn rejection"),
            std::time::Duration::from_millis(1),
            std::time::Duration::from_millis(2),
        )
        .await
        .expect_err("unreaped PTY child must remain ambiguous");
        assert_eq!(error.code(), tonic::Code::Unavailable);
        for _ in 0..5 {
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::Starting
            );
            assert!(!state.reaped.load(std::sync::atomic::Ordering::Acquire));
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        state
            .allow_reap
            .store(true, std::sync::atomic::Ordering::Release);
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                match registry.reconcile(&request_id) {
                    ExecRequestReconcile::FencedNeverStarted => {
                        assert!(state.reaped.load(std::sync::atomic::Ordering::Acquire));
                        break;
                    }
                    ExecRequestReconcile::Starting => {
                        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    }
                    state => panic!("unexpected PTY cleanup reconcile state: {state:?}"),
                }
            }
        })
        .await
        .expect("PTY cleanup did not fence after reap");
    }

    #[tokio::test]
    async fn dropped_pending_pty_child_retains_cleanup_until_reaped() {
        let (child, state) = pending_pty_test_child(1, 4, 1);
        drop(child);
        await_pending_pty_test_reap(&state).await;
        assert!(state.polls.load(std::sync::atomic::Ordering::Acquire) >= 4);
    }

    #[test]
    fn logical_exec_ids_are_monotonic_and_never_wrap() {
        let next = AtomicU64::new(41);
        assert_eq!(allocate_logical_exec_id_from(&next), Some(41));
        assert_eq!(allocate_logical_exec_id_from(&next), Some(42));

        let exhausted = AtomicU64::new(u64::MAX);
        assert_eq!(allocate_logical_exec_id_from(&exhausted), None);
        assert_eq!(exhausted.load(Ordering::Relaxed), u64::MAX);
    }

    #[test]
    fn public_signal_validation_rejects_invalid_boundaries() {
        assert!(validate_exec_signal(libc::SIGTERM).is_ok());
        assert!(validate_exec_signal(libc::SIGINT).is_ok());
        assert!(validate_exec_signal(libc::SIGKILL).is_ok());
        assert!(validate_exec_signal(0).is_err());
        assert!(validate_exec_signal(65).is_err());
        #[cfg(target_os = "linux")]
        assert!(validate_exec_signal(libc::SIGRTMIN()).is_err());
    }

    #[test]
    fn ready_challenge_hex_encoding_is_fixed_width() {
        let mut bytes = [0_u8; 32];
        bytes[0] = 0x01;
        bytes[31] = 0xfe;
        let encoded = hex_ready_challenge(bytes);
        assert_eq!(encoded.len(), 64);
        assert!(encoded.starts_with("01"));
        assert!(encoded.ends_with("fe"));
        assert!(encoded.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }

    #[tokio::test]
    async fn one_ready_deadline_bounds_a_connected_stalled_reader() {
        use tokio::io::AsyncReadExt as _;

        let (mut reader, _stalled_writer) = tokio::io::duplex(8);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(25);
        let result = enforce_ready_deadline(deadline, async move {
            let mut bytes = Vec::new();
            reader
                .read_to_end(&mut bytes)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            Ok(bytes)
        })
        .await;
        assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    }

    fn exec_request(container_id: Option<&str>, allocate_pty: bool) -> ExecRequest {
        ExecRequest {
            command: "/bin/printf".to_string(),
            args: vec!["%s".to_string(), "$HOME;not-a-shell".to_string()],
            working_dir: "/workspace".to_string(),
            env: [("MODE".to_string(), "test".to_string())]
                .into_iter()
                .collect(),
            user: String::new(),
            metadata: None,
            allocate_pty,
            term_rows: 24,
            term_cols: 80,
            container_target: container_id.map(|container_id| ContainerExecTarget {
                container_id: container_id.to_string(),
            }),
            supervised_machine: false,
        }
    }

    #[test]
    fn ordinary_guest_exec_remains_direct_and_does_not_select_trampoline() {
        let prepared = prepare_agent_exec(&exec_request(None, false), None).unwrap();
        assert!(!prepared.container_targeted);
        assert_eq!(prepared.command, "/bin/printf");
        assert_eq!(prepared.args, ["%s", "$HOME;not-a-shell"]);
        assert_eq!(prepared.spawn_working_dir.as_deref(), Some("/workspace"));
        assert!(!crate::container_exec::is_trampoline_request(
            &prepared
                .args
                .iter()
                .cloned()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ));
    }

    #[test]
    fn explicit_machine_mode_selects_supervision_without_container_identity() {
        for pty in [false, true] {
            let mut req = exec_request(None, pty);
            req.supervised_machine = true;
            let challenge = "a".repeat(64);
            let launch =
                prepare_agent_exec(&req, Some(("/run/vz-agent-exec/test.sock", &challenge)))
                    .unwrap();
            assert!(!launch.container_targeted);
            assert!(launch.spawn_user.is_none());
            let args = launch.args.iter().map(OsString::from).collect::<Vec<_>>();
            assert!(crate::container_exec::machine::is_request(&args));
            assert!(!crate::container_exec::is_trampoline_request(&args));
            assert!(prepare_agent_exec(&req, None).is_err());
            req.container_target = Some(ContainerExecTarget {
                container_id: "web".into(),
            });
            assert!(
                prepare_agent_exec(&req, Some(("/run/vz-agent-exec/test.sock", &challenge)))
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn machine_request_claims_and_fences_the_same_exact_guest_ticket_journal() {
        use vz_agent_proto::agent_service_server::AgentService as _;
        let process_table = Arc::new(Mutex::new(ProcessTable::new()));
        let service = AgentServiceImpl::new(SharedState {
            process_table: Arc::clone(&process_table),
            docker_supervisor: Arc::new(crate::docker::DockerSupervisor::new()),
        });
        let registry = process_table.lock().await.request_registry();
        for pty in [false, true] {
            let ticket = registry.allocate_request_id().unwrap();
            let permit = registry.claim(&ticket).unwrap();
            let request = || {
                Request::new(ExecRequest {
                    supervised_machine: true,
                    allocate_pty: pty,
                    metadata: Some(TransportMetadata {
                        request_id: ticket.clone(),
                        idempotency_key: String::new(),
                    }),
                    ..ExecRequest::default()
                })
            };
            assert_eq!(
                service.exec(request()).await.unwrap_err().code(),
                tonic::Code::AlreadyExists
            );
            drop(permit);
            let mut rx = service
                .exec(request())
                .await
                .unwrap()
                .into_inner()
                .into_inner();
            let event = rx.recv().await.unwrap().unwrap();
            assert_eq!(event.request_id, ticket);
            assert_eq!(event.exec_id, 0);
            assert!(matches!(event.event, Some(exec_event::Event::Error(_))));
            assert_eq!(
                registry.reconcile(&ticket),
                ExecRequestReconcile::FencedNeverStarted
            );
        }
    }

    #[tokio::test]
    async fn machine_pending_pipe_keeps_supervisor_and_ticket_after_timeout() {
        let registry = ProcessTable::new().request_registry();
        let ticket = registry.allocate_request_id().unwrap();
        let mut permit = registry.claim(&ticket).unwrap();
        permit.authorize_start().unwrap();
        let (mut child, state) = pending_pipe_test_child(0, false);
        child.request_permit = RetainedExecRequestPermit {
            permit: Some(permit),
            preserve_supervisor: true,
        };
        let result = child
            .terminate_and_reap_with_timeout(
                std::time::Duration::from_millis(10),
                std::time::Duration::from_millis(1),
            )
            .await;
        assert_eq!(result, PendingChildCleanupOutcome::Retained);
        assert_eq!(registry.reconcile(&ticket), ExecRequestReconcile::Starting);
        assert_eq!(state.kills.load(Ordering::Acquire), 0);
        state.allow_reap.store(true, Ordering::Release);
        await_pending_pipe_test_reap(&state).await;
        assert_eq!(state.kills.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn machine_pending_pty_keeps_supervisor_and_ticket_after_timeout() {
        let registry = ProcessTable::new().request_registry();
        let ticket = registry.allocate_request_id().unwrap();
        let mut permit = registry.claim(&ticket).unwrap();
        permit.authorize_start().unwrap();
        let (mut child, state) = pending_pty_test_child(0, 1, 0);
        state.allow_reap.store(false, Ordering::Release);
        child.request_permit = RetainedExecRequestPermit {
            permit: Some(permit),
            preserve_supervisor: true,
        };
        let result = child
            .terminate_and_reap_with_timeout(
                std::time::Duration::from_millis(10),
                std::time::Duration::from_millis(1),
            )
            .await;
        assert_eq!(result, PendingChildCleanupOutcome::Retained);
        assert_eq!(registry.reconcile(&ticket), ExecRequestReconcile::Starting);
        assert_eq!(state.kills.load(Ordering::Acquire), 0);
        state.allow_reap.store(true, Ordering::Release);
        await_pending_pty_test_reap(&state).await;
        assert_eq!(state.kills.load(Ordering::Acquire), 0);
    }

    #[test]
    fn pipe_and_pty_container_requests_route_through_one_trampoline() {
        let ready_handshake = Some((
            "/run/vz-agent-exec/test.sock",
            "abababababababababababababababababababababababababababababababab",
        ));
        let pipe = prepare_agent_exec(&exec_request(Some("web"), false), ready_handshake).unwrap();
        let pty = prepare_agent_exec(&exec_request(Some("web"), true), ready_handshake).unwrap();
        assert!(pipe.container_targeted);
        assert_eq!(pipe, pty);
        assert_eq!(pipe.command, "/proc/self/exe");
        assert!(crate::container_exec::is_trampoline_request(
            &pipe
                .args
                .iter()
                .cloned()
                .map(OsString::from)
                .collect::<Vec<_>>()
        ));
        assert!(pipe.spawn_working_dir.is_none());
        assert!(pipe.spawn_user.is_none());
        assert!(pipe.clear_environment);
        assert_eq!(
            pipe.spawn_environment,
            [
                ("MODE".to_string(), "test".to_string()),
                ("PATH".to_string(), DEFAULT_CONTAINER_EXEC_PATH.to_string())
            ]
        );
    }

    #[test]
    fn unary_oci_exec_uses_the_same_trampoline_and_preserves_argv() {
        let agent = prepare_agent_exec(&exec_request(Some("web"), false), None).unwrap();
        let unary = prepare_oci_exec(&OciExecRequest {
            container_id: "web".to_string(),
            command: "/bin/printf".to_string(),
            args: vec!["%s".to_string(), "$HOME;not-a-shell".to_string()],
            env: [("MODE".to_string(), "test".to_string())]
                .into_iter()
                .collect(),
            working_dir: "/workspace".to_string(),
            user: String::new(),
            metadata: None,
        })
        .unwrap();

        assert_eq!(agent.command, unary.trampoline.program);
        assert_eq!(agent.args, unary.trampoline.args);
        assert_eq!(agent.spawn_environment, unary.environment);
    }

    #[tokio::test]
    async fn legacy_oci_exec_is_retired_fail_fast() {
        use vz_agent_proto::oci_service_server::OciService as _;

        let result = OciServiceImpl
            .exec(Request::new(OciExecRequest::default()))
            .await;
        let error = match result {
            Ok(_) => panic!("legacy unary OCI exec must not run"),
            Err(error) => error,
        };
        assert_eq!(error.code(), tonic::Code::Unimplemented);
        assert!(error.message().contains("AgentService.Exec"));
    }

    #[test]
    fn every_container_exec_adapter_has_one_exact_environment_and_user_spec() {
        let mut request = exec_request(Some("web"), false);
        request.user = "dev:builders".to_string();
        request
            .env
            .insert("PATH".to_string(), "/custom/bin".to_string());
        request.env.insert("Z_LAST".to_string(), "last".to_string());

        let pipe = prepare_agent_exec(&request, None).unwrap();
        request.allocate_pty = true;
        let pty = prepare_agent_exec(&request, None).unwrap();
        let unary = prepare_oci_exec(&OciExecRequest {
            container_id: "web".to_string(),
            command: request.command,
            args: request.args,
            env: request.env,
            working_dir: request.working_dir,
            user: request.user,
            metadata: None,
        })
        .unwrap();

        assert_eq!(pipe, pty);
        assert_eq!(pipe.args, unary.trampoline.args);
        assert_eq!(pipe.spawn_environment, unary.environment);
        assert_eq!(
            pipe.spawn_environment,
            [
                ("MODE".to_string(), "test".to_string()),
                ("PATH".to_string(), "/custom/bin".to_string()),
                ("Z_LAST".to_string(), "last".to_string()),
            ]
        );
    }

    #[test]
    fn container_exec_environment_rejects_invalid_entries() {
        for key in ["", "BAD=KEY", "BAD\0KEY"] {
            let environment = [(key.to_string(), "value".to_string())]
                .into_iter()
                .collect();
            assert!(normalized_container_environment(&environment).is_err());
        }
        let environment = [("KEY".to_string(), "bad\0value".to_string())]
            .into_iter()
            .collect();
        assert!(normalized_container_environment(&environment).is_err());
    }

    #[test]
    fn container_exec_request_ids_require_exact_guest_ticket_shape() {
        let valid = "exec_req_550e8400-e29b-41d4-a716-446655440000_0000000000000001".to_string();
        assert_eq!(valid.len(), 62);
        validate_container_exec_request_id(&valid).unwrap();

        for invalid in [
            "",
            "exec_req_00000000-0000-0000-0000-000000000000",
            "exec_req_00000000-0000-4000-0000-000000000000",
            "exec_req_550E8400-E29B-41D4-A716-446655440000",
            "exec_req_550e8400e29b41d4a716446655440000",
            "wrong_550e8400-e29b-41d4-a716-446655440000",
            "exec_req_550e8400-e29b-41d4-a716-446655440000_0000000000000000",
            "exec_req_550e8400-e29b-41d4-a716-446655440000_000000000000000A",
            "exec_req_550e8400-e29b-41d4-a716-446655440000_000000000000001",
        ] {
            assert!(
                validate_container_exec_request_id(invalid).is_err(),
                "{invalid}"
            );
        }
        let oversized = format!("{valid}x");
        assert!(validate_container_exec_request_id(&oversized).is_err());
    }

    #[test]
    fn allocate_exec_metadata_is_required_and_bounded() {
        assert!(validate_allocate_exec_transport_metadata(None).is_err());
        assert!(
            validate_allocate_exec_transport_metadata(Some(&TransportMetadata {
                request_id: String::new(),
                idempotency_key: String::new(),
            }))
            .is_err()
        );
        assert!(
            validate_allocate_exec_transport_metadata(Some(&TransportMetadata {
                request_id: "r".repeat(129),
                idempotency_key: String::new(),
            }))
            .is_err()
        );
        assert!(
            validate_allocate_exec_transport_metadata(Some(&TransportMetadata {
                request_id: "allocate-control".to_string(),
                idempotency_key: "k".repeat(257),
            }))
            .is_err()
        );
        validate_allocate_exec_transport_metadata(Some(&TransportMetadata {
            request_id: "allocate-control".to_string(),
            idempotency_key: "k".repeat(256),
        }))
        .unwrap();
    }

    #[tokio::test]
    async fn allocate_exec_request_issues_distinct_exact_guest_tickets() {
        use vz_agent_proto::agent_service_server::AgentService as _;

        let service = AgentServiceImpl::new(SharedState {
            process_table: Arc::new(Mutex::new(ProcessTable::new())),
            docker_supervisor: Arc::new(crate::docker::DockerSupervisor::new()),
        });
        let request = || {
            Request::new(AllocateExecRequestRequest {
                metadata: Some(TransportMetadata {
                    request_id: "allocate-test".to_string(),
                    idempotency_key: String::new(),
                }),
            })
        };
        let first = service
            .allocate_exec_request(request())
            .await
            .expect("first allocation")
            .into_inner()
            .exec_request_id;
        let second = service
            .allocate_exec_request(request())
            .await
            .expect("second allocation")
            .into_inner()
            .exec_request_id;
        assert_ne!(first, second);
        assert_eq!(first.len(), 62);
        assert_eq!(second.len(), 62);
        validate_container_exec_request_id(&first).unwrap();
        validate_container_exec_request_id(&second).unwrap();
    }

    #[tokio::test]
    async fn exec_claim_maps_active_to_status_and_fenced_to_definite_rejection() {
        use tokio_stream::StreamExt as _;
        use vz_agent_proto::agent_service_server::AgentService as _;

        let process_table = Arc::new(Mutex::new(ProcessTable::new()));
        let service = AgentServiceImpl::new(SharedState {
            process_table: Arc::clone(&process_table),
            docker_supervisor: Arc::new(crate::docker::DockerSupervisor::new()),
        });
        let registry = process_table.lock().await.request_registry();
        let active_ticket = registry.allocate_request_id().unwrap();
        let _active_permit = registry.claim(&active_ticket).unwrap();
        let exec_request = |request_id: &str| {
            Request::new(ExecRequest {
                metadata: Some(TransportMetadata {
                    request_id: request_id.to_string(),
                    idempotency_key: String::new(),
                }),
                container_target: Some(ContainerExecTarget {
                    container_id: "claim-test".to_string(),
                }),
                ..ExecRequest::default()
            })
        };

        let active = service
            .exec(exec_request(&active_ticket))
            .await
            .expect_err("an active ticket must remain transport-ambiguous");
        assert_eq!(active.code(), tonic::Code::AlreadyExists);

        let fenced_ticket = registry.allocate_request_id().unwrap();
        drop(registry.claim(&fenced_ticket).unwrap());
        let response = service
            .exec(exec_request(&fenced_ticket))
            .await
            .expect("a fenced ticket has a definite no-child response");
        let event = response
            .into_inner()
            .next()
            .await
            .expect("definite rejection stream omitted its event")
            .expect("definite rejection stream returned a transport status");
        assert_eq!(event.sequence, 1);
        assert_eq!(event.request_id, fenced_ticket);
        assert_eq!(event.exec_id, 0);
        assert!(matches!(event.event, Some(exec_event::Event::Error(_))));
    }

    #[test]
    fn reconcile_metadata_is_required_and_bounded() {
        assert!(validate_reconcile_transport_metadata(None).is_err());
        assert!(
            validate_reconcile_transport_metadata(Some(&TransportMetadata {
                request_id: String::new(),
                idempotency_key: String::new(),
            }))
            .is_err()
        );
        assert!(
            validate_reconcile_transport_metadata(Some(&TransportMetadata {
                request_id: "r".repeat(129),
                idempotency_key: String::new(),
            }))
            .is_err()
        );
        assert!(
            validate_reconcile_transport_metadata(Some(&TransportMetadata {
                request_id: "reconcile-control".to_string(),
                idempotency_key: "k".repeat(257),
            }))
            .is_err()
        );
        validate_reconcile_transport_metadata(Some(&TransportMetadata {
            request_id: "reconcile-control".to_string(),
            idempotency_key: "k".repeat(256),
        }))
        .unwrap();
    }

    #[tokio::test]
    async fn claimed_invalid_container_environment_is_exact_definite_rejection() {
        let mut request = exec_request(Some("web"), false);
        request
            .env
            .insert("BAD=KEY".to_string(), "value".to_string());
        let response = match validate_claimed_container_exec(&request, "claimed-request", "pipe") {
            Ok(()) => panic!("invalid claimed request must be rejected"),
            Err(response) => response,
        };
        let mut receiver = response.into_inner().into_inner();
        let event = receiver.recv().await.unwrap().unwrap();
        assert_eq!(event.exec_id, 0);
        assert_eq!(event.sequence, 1);
        assert_eq!(event.request_id, "claimed-request");
        assert!(matches!(
            event.event,
            Some(exec_event::Event::Error(detail))
                if detail.contains("validation rejected before spawn")
        ));
        assert!(receiver.recv().await.is_none());
    }

    #[tokio::test]
    async fn exec_order_sequence_is_monotonic_across_control_ops() {
        let exec_id = test_exec_id();
        let (tx, mut rx) = tokio::sync::mpsc::channel::<Result<ExecEvent, Status>>(8);
        register_exec_order_context(exec_id, ExecOrderContext::new(tx, "req-test".to_string()));

        let first_event = match send_ordered_exec_event(
            exec_id,
            exec_event::Event::Stdout(b"a".to_vec()),
        )
        .await
        {
            Ok(sequence) => sequence,
            Err(()) => panic!("first event should send"),
        };
        let control = match begin_ordered_control(exec_id, "stdin_close").await {
            Some(control) => control,
            None => panic!("control op should be ordered"),
        };
        let control_sequence = control.sequence;
        drop(control);
        let second_event = match send_ordered_exec_event(
            exec_id,
            exec_event::Event::Stderr(b"b".to_vec()),
        )
        .await
        {
            Ok(sequence) => sequence,
            Err(()) => panic!("second event should send"),
        };

        assert_eq!(first_event, 1);
        assert_eq!(control_sequence, 2);
        assert_eq!(second_event, 3);

        let first = rx.recv().await;
        assert!(matches!(
            first,
            Some(Ok(ExecEvent {
                sequence: 1,
                request_id,
                exec_id: observed_exec_id,
                event: Some(exec_event::Event::Stdout(_)),
                ..
            })) if request_id == "req-test" && observed_exec_id == exec_id
        ));
        let second = rx.recv().await;
        assert!(matches!(
            second,
            Some(Ok(ExecEvent {
                sequence: 3,
                request_id,
                exec_id: observed_exec_id,
                event: Some(exec_event::Event::Stderr(_)),
                ..
            })) if request_id == "req-test" && observed_exec_id == exec_id
        ));

        remove_exec_order_context(exec_id);
    }

    #[tokio::test]
    async fn ordered_send_and_control_require_registered_exec_context() {
        let exec_id = test_exec_id();
        remove_exec_order_context(exec_id);

        let sent = send_ordered_exec_event(exec_id, exec_event::Event::ExitCode(0)).await;
        assert!(sent.is_err());
        let control = begin_ordered_control(exec_id, "signal").await;
        assert!(control.is_none());
    }

    #[tokio::test]
    async fn output_backpressure_never_blocks_exec_control() {
        let exec_id = test_exec_id();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        register_exec_order_context(
            exec_id,
            ExecOrderContext::new(sender, "backpressure".to_string()),
        );
        assert_eq!(
            send_ordered_exec_event(exec_id, exec_event::Event::Stdout(vec![1])).await,
            Ok(1)
        );
        let blocked_send = tokio::spawn(async move {
            send_ordered_exec_event(exec_id, exec_event::Event::Stdout(vec![2])).await
        });
        tokio::task::yield_now().await;

        let control = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            begin_ordered_control(exec_id, "signal"),
        )
        .await
        .expect("control must not wait behind full output channel")
        .expect("registered control context");
        assert_eq!(control.sequence, 3);
        drop(control);

        assert_eq!(receiver.recv().await.unwrap().unwrap().sequence, 1);
        assert_eq!(blocked_send.await.unwrap(), Ok(2));
        assert_eq!(receiver.recv().await.unwrap().unwrap().sequence, 2);
        remove_exec_order_context(exec_id);
    }

    #[tokio::test]
    async fn large_backpressured_output_is_fully_queued_before_exit() {
        const CHUNKS_PER_READER: usize = 64;
        const CHUNK_BYTES: usize = 32 * 1024;

        let exec_id = test_exec_id();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let finish_sender = sender.clone();
        register_exec_order_context(
            exec_id,
            ExecOrderContext::new(sender, "large-output".to_string()),
        );

        let mut stdout = tokio::spawn(async move {
            for _ in 0..CHUNKS_PER_READER {
                send_ordered_exec_event(
                    exec_id,
                    exec_event::Event::Stdout(vec![b'o'; CHUNK_BYTES]),
                )
                .await
                .map_err(|_| "stdout stream closed".to_string())?;
            }
            Ok(())
        });
        let mut stderr = tokio::spawn(async move {
            for _ in 0..CHUNKS_PER_READER {
                send_ordered_exec_event(
                    exec_id,
                    exec_event::Event::Stderr(vec![b'e'; CHUNK_BYTES]),
                )
                .await
                .map_err(|_| "stderr stream closed".to_string())?;
            }
            Ok(())
        });
        let finalizer = tokio::spawn(async move {
            assert_eq!(
                await_pipe_output_drain(exec_id, finish_sender, &mut stdout, &mut stderr).await,
                OutputDrain::Drained
            );
            send_ordered_exec_event(exec_id, exec_event::Event::ExitCode(0))
                .await
                .expect("exit send")
        });

        tokio::task::yield_now().await;
        assert!(
            !finalizer.is_finished(),
            "a full stream channel must backpressure the output barrier"
        );

        let mut output_chunks = 0;
        let mut last_sequence = 0;
        loop {
            let event = receiver.recv().await.unwrap().unwrap();
            assert_eq!(event.sequence, last_sequence + 1);
            last_sequence = event.sequence;
            match event.event.unwrap() {
                exec_event::Event::Stdout(bytes) | exec_event::Event::Stderr(bytes) => {
                    assert_eq!(bytes.len(), CHUNK_BYTES);
                    output_chunks += 1;
                }
                exec_event::Event::ExitCode(0) => {
                    assert_eq!(output_chunks, CHUNKS_PER_READER * 2);
                    break;
                }
                other => panic!("unexpected event before terminal exit: {other:?}"),
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        assert_eq!(finalizer.await.unwrap(), (CHUNKS_PER_READER * 2 + 1) as u64);
        remove_exec_order_context(exec_id);
    }

    #[tokio::test]
    async fn slow_live_consumer_beyond_old_deadline_retains_pipe_finisher() {
        let exec_id = test_exec_id();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let finish_sender = sender.clone();
        register_exec_order_context(
            exec_id,
            ExecOrderContext::new(sender, "slow-live".to_string()),
        );
        let mut stdout = tokio::spawn(async move {
            send_ordered_exec_event(exec_id, exec_event::Event::Stdout(vec![1]))
                .await
                .map_err(|_| "first output rejected".to_string())?;
            send_ordered_exec_event(exec_id, exec_event::Event::Stdout(vec![2]))
                .await
                .map_err(|_| "second output rejected".to_string())?;
            Ok(())
        });
        let mut stderr = tokio::spawn(async { Ok(()) });
        let finisher = tokio::spawn(async move {
            await_pipe_output_drain(exec_id, finish_sender, &mut stdout, &mut stderr).await
        });

        tokio::time::sleep(std::time::Duration::from_millis(5_100)).await;
        assert!(
            !finisher.is_finished(),
            "a live slow consumer must retain finish authority beyond five seconds"
        );
        assert!(matches!(
            receiver.recv().await.unwrap().unwrap().event,
            Some(exec_event::Event::Stdout(bytes)) if bytes == [1]
        ));
        assert!(matches!(
            receiver.recv().await.unwrap().unwrap().event,
            Some(exec_event::Event::Stdout(bytes)) if bytes == [2]
        ));
        assert_eq!(finisher.await.unwrap(), OutputDrain::Drained);
        remove_exec_order_context(exec_id);
    }

    #[tokio::test]
    async fn closed_receiver_cancels_pipe_readers_and_releases_finisher() {
        let exec_id = test_exec_id();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let mut stdout = tokio::spawn(std::future::pending::<OutputReaderResult>());
        let mut stderr = tokio::spawn(std::future::pending::<OutputReaderResult>());
        let finisher = tokio::spawn(async move {
            await_pipe_output_drain(exec_id, sender, &mut stdout, &mut stderr).await
        });

        drop(receiver);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), finisher)
                .await
                .expect("closed receiver must release pipe finisher")
                .unwrap(),
            OutputDrain::ReceiverClosed
        );
    }

    #[tokio::test]
    async fn closed_receiver_still_publishes_reaped_terminal_receipt_without_exit() {
        use std::process::Stdio;

        let exec_id = test_exec_id();
        let mut command = tokio::process::Command::new("/bin/sleep");
        command
            .arg("0")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = command.spawn().expect("spawn terminal receipt child");
        let pid = child.id().expect("terminal receipt child PID");
        #[cfg(target_os = "linux")]
        let identity = capture_signal_identity(pid);
        #[cfg(not(target_os = "linux"))]
        let identity = ProcessIdentity::from_pid(pid);
        let table = Arc::new(Mutex::new(ProcessTable::new()));
        table
            .lock()
            .await
            .insert(exec_id, child, None, identity, false);
        let exit_code = {
            let mut table = table.lock().await;
            let status = table
                .get_mut(exec_id)
                .unwrap()
                .child
                .wait()
                .await
                .expect("reap terminal receipt child");
            crate::process_table::normalized_exit_status(status)
        };

        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        register_exec_order_context(
            exec_id,
            ExecOrderContext::new(sender, "closed-terminal".to_string()),
        );
        drop(receiver);
        finish_exec_stream(&table, exec_id, exit_code, OutputDrain::ReceiverClosed).await;

        assert_eq!(
            table.lock().await.terminal_receipt(exec_id),
            Some(ExecTerminalReceipt {
                exit_code: 0,
                forced: false,
            })
        );
        assert!(lookup_exec_order_context(exec_id).is_none());
    }

    #[tokio::test]
    async fn closed_receiver_cancels_pty_reader_and_releases_finisher() {
        let exec_id = test_exec_id();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let cancel_reader = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let task_cancel = cancel_reader.clone();
        let mut reader = tokio::task::spawn_blocking(move || -> OutputReaderResult {
            while !task_cancel.load(Ordering::Acquire) {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            Err("receiver closed".to_string())
        });
        let finisher = tokio::spawn(async move {
            await_pty_output_drain(exec_id, sender, cancel_reader, &mut reader).await
        });

        drop(receiver);
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), finisher)
                .await
                .expect("closed receiver must release PTY finisher")
                .unwrap(),
            OutputDrain::ReceiverClosed
        );
    }

    #[tokio::test]
    async fn pty_output_bytes_are_enqueued_before_exit() {
        const CHUNKS: usize = 32;
        const BYTES: usize = 4096;

        let exec_id = test_exec_id();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        let finish_sender = sender.clone();
        register_exec_order_context(
            exec_id,
            ExecOrderContext::new(sender, "pty-order".to_string()),
        );
        let cancel_reader = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut reader = tokio::spawn(async move {
            for _ in 0..CHUNKS {
                send_ordered_exec_event(exec_id, exec_event::Event::Stdout(vec![b'p'; BYTES]))
                    .await
                    .map_err(|_| "PTY output rejected".to_string())?;
            }
            Ok(())
        });
        let finalizer = tokio::spawn(async move {
            assert_eq!(
                await_pty_output_drain(exec_id, finish_sender, cancel_reader, &mut reader).await,
                OutputDrain::Drained
            );
            send_ordered_exec_event(exec_id, exec_event::Event::ExitCode(0))
                .await
                .expect("PTY exit send")
        });

        let mut bytes = 0;
        loop {
            let event = receiver.recv().await.unwrap().unwrap();
            match event.event.unwrap() {
                exec_event::Event::Stdout(chunk) => bytes += chunk.len(),
                exec_event::Event::ExitCode(0) => break,
                other => panic!("unexpected PTY event: {other:?}"),
            }
        }
        assert_eq!(bytes, CHUNKS * BYTES);
        assert_eq!(finalizer.await.unwrap(), (CHUNKS + 1) as u64);
        remove_exec_order_context(exec_id);
    }

    fn assert_process_absent(pid: u32) {
        // SAFETY: signal zero only queries whether the exact PID exists.
        assert_eq!(unsafe { libc::kill(pid as i32, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH),
            "child must be killed and reaped before ownership is released"
        );
    }

    #[tokio::test]
    async fn pre_ready_pipe_rejection_reaps_before_definite_error_frame() {
        use std::process::Stdio;

        let mut command = tokio::process::Command::new("/bin/sleep");
        command
            .arg("30")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let child = command.spawn().expect("spawn pending child");
        let pid = child.id().expect("pending child PID");

        let response = reject_pending_pipe(
            PendingPipeChild::new(child),
            "pre-ready-test",
            Status::failed_precondition("injected readiness failure"),
        )
        .await
        .expect("terminated and reaped child must permit a definite rejection");
        let mut receiver = response.into_inner().into_inner();
        let event = receiver.recv().await.unwrap().unwrap();
        assert_eq!(event.exec_id, 0);
        assert_eq!(event.sequence, 1);
        assert_eq!(event.request_id, "pre-ready-test");
        let Some(exec_event::Event::Error(detail)) = event.event else {
            panic!("pre-ready rejection must be an Error event");
        };
        assert!(detail.contains("rejected before readiness"));
        assert!(detail.contains("spawned process reaped"));
        assert_process_absent(pid);
    }

    #[tokio::test]
    async fn pending_pipe_drop_reaps_when_handler_is_cancelled() {
        use std::process::Stdio;

        let mut command = tokio::process::Command::new("/bin/sleep");
        command
            .arg("30")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        let child = command.spawn().expect("spawn pending child");
        let pid = child.id().expect("pending child PID");

        drop(PendingPipeChild::new(child));
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                // SAFETY: signal zero only queries whether the exact PID exists.
                if unsafe { libc::kill(pid as i32, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("drop cleanup did not reap pending pipe child");
        assert_process_absent(pid);
    }

    #[tokio::test]
    async fn cancellation_ordering_never_waits_behind_stdin_control() {
        let exec_id = test_exec_id();
        let (sender, _receiver) = tokio::sync::mpsc::channel(1);
        register_exec_order_context(
            exec_id,
            ExecOrderContext::new(sender, "stdin-cancel".to_string()),
        );
        let stdin_control = begin_ordered_control(exec_id, "stdin_write")
            .await
            .expect("stdin control");
        assert_eq!(stdin_control.sequence, 1);
        assert_eq!(mark_nonblocking_control(exec_id, "cancel_exec"), Some(2));
        drop(stdin_control);
        remove_exec_order_context(exec_id);
    }

    async fn register_test_process(
        exec_id: u64,
        ignore_term: bool,
        piped_stdin: bool,
    ) -> (
        Arc<Mutex<ProcessTable>>,
        crate::process_table::ExecCompletion,
    ) {
        use std::process::Stdio;

        let mut command = tokio::process::Command::new("/bin/sleep");
        command
            .arg("30")
            .stdin(if piped_stdin {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        if ignore_term {
            use std::os::unix::process::CommandExt as _;

            // SAFETY: pre_exec runs in the child and invokes only signal(2).
            unsafe {
                command.as_std_mut().pre_exec(|| {
                    if libc::signal(libc::SIGTERM, libc::SIG_IGN) == libc::SIG_ERR {
                        return Err(std::io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        let mut child = command.spawn().expect("spawn test process");
        let pid = child.id().expect("test process pid");
        let stdin = child.stdin.take();
        #[cfg(target_os = "linux")]
        let identity = capture_signal_identity(pid);
        #[cfg(not(target_os = "linux"))]
        let identity = ProcessIdentity::from_pid(pid);

        let table = Arc::new(Mutex::new(ProcessTable::new()));
        let completion = table
            .lock()
            .await
            .insert(exec_id, child, stdin, identity, false);
        let watcher_table = table.clone();
        tokio::spawn(async move {
            let exit_code = loop {
                let observed = {
                    let mut table = watcher_table.lock().await;
                    let entry = table.get_mut(exec_id).expect("registered test process");
                    entry
                        .child
                        .try_wait()
                        .expect("poll test process")
                        .map(crate::process_table::normalized_exit_status)
                };
                if let Some(exit_code) = observed {
                    break exit_code;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            };
            watcher_table.lock().await.finish(exec_id, exit_code);
        });
        (table, completion)
    }

    async fn wait_for_cancel_start(table: &Arc<Mutex<ProcessTable>>, exec_id: u64) {
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                if table.lock().await.cancellation_deadline(exec_id).is_some() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancellation driver must start");
    }

    #[tokio::test]
    async fn cancellation_waits_for_reap_and_returns_normalized_status() {
        let exec_id = test_exec_id();
        let (table, _completion) = register_test_process(exec_id, false, false).await;

        let outcome = cancel_active_exec(&table, exec_id)
            .await
            .expect("cancel active process");
        assert_eq!(outcome.exit_code, 128 + libc::SIGTERM);
        assert!(!outcome.forced);
        {
            let table = table.lock().await;
            assert!(table.completion(exec_id).is_none());
            assert_eq!(
                table.terminal_receipt(exec_id),
                Some(ExecTerminalReceipt {
                    exit_code: 128 + libc::SIGTERM,
                    forced: false,
                })
            );
        }
        assert_eq!(
            cancel_active_exec(&table, exec_id)
                .await
                .expect("completed cancellation must be idempotent"),
            outcome
        );
    }

    #[tokio::test]
    async fn cancellation_escalates_to_kill_for_term_ignoring_process() {
        let exec_id = test_exec_id();
        let (table, _completion) = register_test_process(exec_id, true, false).await;

        let outcome = cancel_active_exec(&table, exec_id)
            .await
            .expect("force-cancel active process");
        assert_eq!(outcome.exit_code, 128 + libc::SIGKILL);
        assert!(outcome.forced);
        {
            let table = table.lock().await;
            assert!(table.completion(exec_id).is_none());
            assert_eq!(
                table.terminal_receipt(exec_id),
                Some(ExecTerminalReceipt {
                    exit_code: 128 + libc::SIGKILL,
                    forced: true,
                })
            );
        }
        assert_eq!(
            cancel_active_exec(&table, exec_id)
                .await
                .expect("forced cancellation receipt must be replayable"),
            outcome
        );
    }

    #[tokio::test]
    async fn aborted_cancellation_rpc_cannot_abandon_escalation() {
        let exec_id = test_exec_id();
        let (table, completion) = register_test_process(exec_id, true, false).await;
        let caller_table = table.clone();
        let caller = tokio::spawn(async move {
            cancel_active_exec_with_grace(
                &caller_table,
                exec_id,
                std::time::Duration::from_millis(150),
            )
            .await
        });
        wait_for_cancel_start(&table, exec_id).await;
        caller.abort();

        let receipt = tokio::time::timeout(std::time::Duration::from_secs(2), completion.wait())
            .await
            .expect("durable driver must complete after caller abort")
            .expect("durable driver must publish an exact receipt");
        assert_eq!(receipt.exit_code, 128 + libc::SIGKILL);
        assert!(receipt.forced);
    }

    #[tokio::test]
    async fn concurrent_cancellation_retries_share_one_fixed_deadline_and_receipt() {
        let exec_id = test_exec_id();
        let (table, _completion) = register_test_process(exec_id, true, false).await;
        let first_table = table.clone();
        let first = tokio::spawn(async move {
            cancel_active_exec_with_grace(
                &first_table,
                exec_id,
                std::time::Duration::from_millis(200),
            )
            .await
        });
        wait_for_cancel_start(&table, exec_id).await;
        let original_deadline = table
            .lock()
            .await
            .cancellation_deadline(exec_id)
            .expect("fixed deadline");

        let mut retries = Vec::new();
        for _ in 0..4 {
            let retry_table = table.clone();
            retries.push(tokio::spawn(async move {
                cancel_active_exec_with_grace(
                    &retry_table,
                    exec_id,
                    std::time::Duration::from_secs(30),
                )
                .await
            }));
        }
        assert_eq!(
            table.lock().await.cancellation_deadline(exec_id),
            Some(original_deadline)
        );

        let expected = first.await.unwrap().expect("first cancellation receipt");
        assert_eq!(expected.exit_code, 128 + libc::SIGKILL);
        assert!(expected.forced);
        for retry in retries {
            assert_eq!(retry.await.unwrap().expect("retry receipt"), expected);
        }
    }

    #[tokio::test]
    async fn full_nonreading_stdin_cannot_block_bounded_cancellation() {
        let exec_id = test_exec_id();
        let (table, _completion) = register_test_process(exec_id, true, true).await;
        let writer_table = table.clone();
        let writer = tokio::spawn(async move {
            write_pipe_stdin(&writer_table, exec_id, &vec![b'x'; MAX_STDIN_WRITE_BYTES]).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(
            !writer.is_finished(),
            "non-reading child should fill stdin pipe"
        );

        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            cancel_active_exec_with_grace(&table, exec_id, std::time::Duration::from_millis(150)),
        )
        .await
        .expect("cancellation must not wait behind blocked stdin")
        .expect("cancellation receipt");
        assert_eq!(outcome.exit_code, 128 + libc::SIGKILL);
        assert!(outcome.forced);
        let _ = tokio::time::timeout(std::time::Duration::from_secs(1), writer)
            .await
            .expect("stdin work must be bounded");
    }

    #[tokio::test]
    async fn stdin_payload_is_bounded_before_process_lookup() {
        let table = Arc::new(Mutex::new(ProcessTable::new()));
        let error = write_pipe_stdin(&table, test_exec_id(), &vec![0; MAX_STDIN_WRITE_BYTES + 1])
            .await
            .expect_err("oversized stdin must fail");
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
    }

    #[test]
    fn wait_errors_are_retryable_and_never_normalized_as_terminal_receipts() {
        let error = classify_child_wait(Err(std::io::Error::other("injected wait failure")))
            .expect_err("wait error must remain an error");
        assert_eq!(error.kind(), std::io::ErrorKind::Other);
        assert_eq!(classify_child_wait(Ok(None)).unwrap(), None);
    }

    #[tokio::test]
    async fn cancellation_rejects_unknown_or_stale_exec_id() {
        let table = Arc::new(Mutex::new(ProcessTable::new()));
        let error = cancel_active_exec(&table, test_exec_id())
            .await
            .expect_err("unknown cancellation must fail");
        assert_eq!(error.code(), tonic::Code::NotFound);
        let table = table.lock().await;
        let error = terminal_cancel_outcome(&table, test_exec_id())
            .expect_err("unknown terminal receipt must fail");
        assert_eq!(error.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn dropped_stream_cancels_and_reaps_registered_process() {
        let exec_id = test_exec_id();
        let (table, completion) = register_test_process(exec_id, false, false).await;
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        register_exec_order_context(
            exec_id,
            ExecOrderContext::new(sender, "drop-test".to_string()),
        );
        monitor_exec_stream_loss(table.clone(), exec_id);
        drop(receiver);

        let receipt = tokio::time::timeout(std::time::Duration::from_secs(2), completion.wait())
            .await
            .expect("stream-loss cancellation must be bounded")
            .expect("stream-loss cancellation must publish a reap receipt");
        assert_eq!(receipt.exit_code, 128 + libc::SIGTERM);
        assert!(!receipt.forced);
        assert_eq!(
            table.lock().await.terminal_receipt(exec_id),
            Some(ExecTerminalReceipt {
                exit_code: 128 + libc::SIGTERM,
                forced: false,
            })
        );
        remove_exec_order_context(exec_id);
    }

    #[tokio::test]
    async fn stale_order_cleanup_cannot_remove_a_new_logical_exec_context() {
        let old_exec_id = test_exec_id();
        let new_exec_id = test_exec_id();
        let (old_tx, _old_rx) = tokio::sync::mpsc::channel(1);
        let (new_tx, mut new_rx) = tokio::sync::mpsc::channel(1);
        register_exec_order_context(
            old_exec_id,
            ExecOrderContext::new(old_tx, "old".to_string()),
        );
        register_exec_order_context(
            new_exec_id,
            ExecOrderContext::new(new_tx, "new".to_string()),
        );

        remove_exec_order_context(old_exec_id);
        assert!(
            send_ordered_exec_event(new_exec_id, exec_event::Event::ExitCode(0))
                .await
                .is_ok()
        );
        let event = new_rx.recv().await.unwrap().unwrap();
        assert_eq!(event.request_id, "new");
        remove_exec_order_context(new_exec_id);
    }

    #[test]
    fn oci_rootfs_preflight_reports_canonical_path_and_governing_mount() {
        let bundle = tempfile::tempdir().expect("create bundle");
        let rootfs = bundle.path().join("rootfs");
        std::fs::create_dir(&rootfs).expect("create rootfs");
        let config = r#"{"root":{"path":"rootfs"}}"#;
        let mountinfo = format!(
            "20 1 0:1 / / rw - rootfs rootfs rw\n21 20 0:2 / {} rw - tmpfs tmpfs rw",
            bundle.path().display()
        );

        let diagnostic = inspect_oci_rootfs(
            bundle.path().to_str().expect("UTF-8 bundle path"),
            config,
            &mountinfo,
        )
        .expect("valid rootfs");

        assert_eq!(diagnostic.configured, PathBuf::from("rootfs"));
        assert_eq!(diagnostic.resolved, rootfs);
        assert_eq!(diagnostic.canonical, rootfs.canonicalize().unwrap());
        assert!(diagnostic.mountinfo.contains(" 0:2 "));
    }

    #[test]
    fn oci_rootfs_preflight_preserves_path_and_mountinfo_on_enoent() {
        let bundle = tempfile::tempdir().expect("create bundle");
        let config = r#"{"root":{"path":"missing"}}"#;
        let mountinfo = "20 1 0:1 / / rw - rootfs rootfs rw";

        let error = inspect_oci_rootfs(
            bundle.path().to_str().expect("UTF-8 bundle path"),
            config,
            mountinfo,
        )
        .expect_err("missing rootfs must fail");

        assert!(error.contains("config.root.path=missing"));
        assert!(error.contains("cannot be canonicalized"));
        assert!(error.contains("mountinfo=20 1 0:1 / / rw"));
    }

    #[test]
    fn youki_kernel_preflight_names_user_namespace_requirement() {
        let temp = tempfile::tempdir().expect("create tempdir");
        let error = ensure_youki_user_namespace_procfs(&temp.path().join("uid_map"))
            .expect_err("missing uid_map must fail");

        assert!(error.contains("CONFIG_USER_NS=y"));
        assert!(error.contains("uid_map"));
    }

    #[test]
    fn oci_normalization_preserves_read_only_cgroup2_mount_and_namespace() {
        let mut config = serde_json::json!({
            "mounts": [
                { "destination": "/proc", "type": "proc", "source": "proc" },
                {
                    "destination": "/sys/fs/cgroup",
                    "type": "cgroup2",
                    "source": "cgroup2",
                    "options": ["nosuid", "noexec", "nodev", "relatime", "ro"]
                },
                { "destination": "/sys", "type": "sysfs", "source": "sysfs" }
            ],
            "linux": {
                "maskedPaths": ["/proc/kcore"],
                "readonlyPaths": ["/proc/sys"],
                "resources": {
                    "cpu": {
                        "quota": 50000,
                        "period": 100000
                    }
                },
                "namespaces": [
                    { "type": "mount" },
                    { "type": "network" },
                    { "type": "cgroup" },
                    { "type": "pid" }
                ]
            }
        });
        let resources_before = config["linux"]["resources"].clone();

        normalize_oci_config(&mut config);

        let mounts = config["mounts"].as_array().expect("mounts");
        assert_eq!(mounts.len(), 2);
        let cgroup2 = mounts
            .iter()
            .find(|mount| mount["type"] == "cgroup2")
            .expect("cgroup2 mount must survive normalization");
        assert_eq!(cgroup2["destination"], "/sys/fs/cgroup");
        assert_eq!(
            cgroup2["options"],
            serde_json::json!(["nosuid", "noexec", "nodev", "relatime", "ro"])
        );

        let namespace_types: Vec<&str> = config["linux"]["namespaces"]
            .as_array()
            .expect("namespaces")
            .iter()
            .map(|namespace| namespace["type"].as_str().expect("namespace type"))
            .collect();
        assert_eq!(namespace_types, ["mount", "network", "cgroup"]);
        assert_eq!(config["linux"]["resources"], resources_before);
        assert!(config["linux"].get("maskedPaths").is_none());
        assert!(config["linux"].get("readonlyPaths").is_none());
    }
}
