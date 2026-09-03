//! Process table for tracking spawned child processes.
//!
//! Maps exec_id to process entries, providing lookup, insertion,
//! removal, and iteration for the connection handler.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex as StdMutex};

#[cfg(any(target_os = "linux", test))]
use anyhow::Context as _;
#[cfg(any(target_os = "linux", test))]
use anyhow::bail;

use tokio::process::{Child, ChildStdin};
use tokio::sync::watch;
use uuid::Uuid;

/// Maximum number of completed logical exec generations retained for
/// idempotent cancellation receipts. Eviction is deterministic FIFO order.
const TERMINAL_RECEIPT_CAPACITY: usize = 1024;
const REQUEST_RECEIPT_CAPACITY: usize = 1024;
const ACTIVE_REQUEST_CAPACITY: usize = 1024;
const EXEC_REQUEST_ID_PREFIX: &str = "exec_req_";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecRequestState {
    Claimed,
    Starting,
    Published(u64),
    Terminal(u64, ExecTerminalReceipt),
    Fenced,
}

struct ExecRequestRegistryInner {
    boot_id: Uuid,
    issued_through: u64,
    retired_through: u64,
    states: HashMap<u64, ExecRequestState>,
    completed: VecDeque<u64>,
}

/// Bounded request-ID journal used to fence ambiguous container exec starts.
#[derive(Clone)]
pub struct ExecRequestRegistry(Arc<StdMutex<ExecRequestRegistryInner>>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecRequestReconcile {
    FencedNeverStarted,
    Starting,
    Published(u64),
    Terminal(u64, ExecTerminalReceipt),
    StaleUnknown,
}

/// Why a guest-issued exec request ticket could not be claimed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecRequestClaimError {
    /// The ticket is already claimed, starting, or associated with a live exec.
    Active,
    /// No live work exists: the ticket is invalid, retired, fenced, terminal,
    /// unissued, or capacity was unavailable before a claim was recorded.
    DefiniteRejection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecRequestAllocationExhausted;

pub struct ExecRequestPermit {
    registry: ExecRequestRegistry,
    sequence: u64,
    armed: bool,
}

impl ExecRequestRegistry {
    fn new() -> Self {
        Self(Arc::new(StdMutex::new(ExecRequestRegistryInner {
            boot_id: Uuid::new_v4(),
            issued_through: 0,
            retired_through: 0,
            states: HashMap::new(),
            completed: VecDeque::new(),
        })))
    }

    /// Allocate one single-use ticket without reserving journal capacity.
    ///
    /// A lost response only burns a sequence number. It cannot leave a claim,
    /// fence, or other state behind.
    pub fn allocate_request_id(&self) -> Result<String, ExecRequestAllocationExhausted> {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        inner.issued_through = inner
            .issued_through
            .checked_add(1)
            .ok_or(ExecRequestAllocationExhausted)?;
        Ok(format_request_id(inner.boot_id, inner.issued_through))
    }

    pub fn claim(&self, request_id: &str) -> Result<ExecRequestPermit, ExecRequestClaimError> {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let sequence = validated_sequence(&inner, request_id)
            .ok_or(ExecRequestClaimError::DefiniteRejection)?;
        if sequence <= inner.retired_through && !inner.states.contains_key(&sequence) {
            return Err(ExecRequestClaimError::DefiniteRejection);
        }
        match inner.states.get(&sequence) {
            None => {
                let active = inner.states.len().saturating_sub(inner.completed.len());
                if active >= ACTIVE_REQUEST_CAPACITY {
                    remember_completed(&mut inner, sequence, ExecRequestState::Fenced);
                    return Err(ExecRequestClaimError::DefiniteRejection);
                }
                inner.states.insert(sequence, ExecRequestState::Claimed);
            }
            Some(ExecRequestState::Fenced | ExecRequestState::Terminal(_, _)) => {
                return Err(ExecRequestClaimError::DefiniteRejection);
            }
            Some(_) => return Err(ExecRequestClaimError::Active),
        }
        Ok(ExecRequestPermit {
            registry: self.clone(),
            sequence,
            armed: true,
        })
    }

    pub fn reconcile(&self, request_id: &str) -> ExecRequestReconcile {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let Some(sequence) = validated_sequence(&inner, request_id) else {
            return ExecRequestReconcile::StaleUnknown;
        };
        match inner.states.get(&sequence).copied() {
            Some(ExecRequestState::Claimed) => {
                remember_completed(&mut inner, sequence, ExecRequestState::Fenced);
                ExecRequestReconcile::FencedNeverStarted
            }
            None => {
                if sequence <= inner.retired_through {
                    ExecRequestReconcile::StaleUnknown
                } else {
                    remember_completed(&mut inner, sequence, ExecRequestState::Fenced);
                    ExecRequestReconcile::FencedNeverStarted
                }
            }
            Some(ExecRequestState::Starting) => ExecRequestReconcile::Starting,
            Some(ExecRequestState::Published(exec_id)) => ExecRequestReconcile::Published(exec_id),
            Some(ExecRequestState::Terminal(exec_id, receipt)) => {
                ExecRequestReconcile::Terminal(exec_id, receipt)
            }
            Some(ExecRequestState::Fenced) => ExecRequestReconcile::FencedNeverStarted,
        }
    }

    pub fn finish_exec(&self, exec_id: u64, receipt: ExecTerminalReceipt) {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        let sequence = inner.states.iter().find_map(|(sequence, state)| {
            matches!(state, ExecRequestState::Published(id) if *id == exec_id).then_some(*sequence)
        });
        if let Some(sequence) = sequence {
            remember_completed(
                &mut inner,
                sequence,
                ExecRequestState::Terminal(exec_id, receipt),
            );
        }
    }

    fn clear(&self) {
        let mut inner = self.0.lock().unwrap_or_else(|error| error.into_inner());
        inner.retired_through = inner.issued_through;
        inner.states.clear();
        inner.completed.clear();
    }
}

impl ExecRequestPermit {
    pub fn authorize_start(&mut self) -> Result<(), ()> {
        let mut inner = self
            .registry
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match inner.states.get_mut(&self.sequence) {
            Some(state @ ExecRequestState::Claimed) => {
                *state = ExecRequestState::Starting;
                Ok(())
            }
            Some(ExecRequestState::Fenced) => Err(()),
            _ => Err(()),
        }
    }

    pub fn publish(mut self, exec_id: u64) {
        let mut inner = self
            .registry
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(state) = inner.states.get_mut(&self.sequence) else {
            unreachable!("live request permit must retain its registry entry");
        };
        assert!(matches!(state, ExecRequestState::Starting));
        *state = ExecRequestState::Published(exec_id);
        self.armed = false;
    }
}

impl Drop for ExecRequestPermit {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut inner = self
            .registry
            .0
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if matches!(
            inner.states.get(&self.sequence),
            Some(ExecRequestState::Claimed | ExecRequestState::Starting | ExecRequestState::Fenced)
        ) {
            remember_completed(&mut inner, self.sequence, ExecRequestState::Fenced);
        }
    }
}

fn remember_completed(
    inner: &mut ExecRequestRegistryInner,
    sequence: u64,
    state: ExecRequestState,
) {
    inner.states.insert(sequence, state);
    inner.completed.retain(|existing| *existing != sequence);
    inner.completed.push_back(sequence);
    while inner.completed.len() > REQUEST_RECEIPT_CAPACITY {
        if let Some(evicted) = inner.completed.pop_front() {
            inner.retired_through = inner.retired_through.max(evicted);
            inner.states.remove(&evicted);
        }
    }
}

fn format_request_id(boot_id: Uuid, sequence: u64) -> String {
    format!("{EXEC_REQUEST_ID_PREFIX}{boot_id}_{sequence:016x}")
}

fn parse_request_id(request_id: &str) -> Option<(Uuid, u64)> {
    let (encoded_boot_id, sequence) = request_id
        .strip_prefix(EXEC_REQUEST_ID_PREFIX)?
        .split_once('_')?;
    if encoded_boot_id.len() != 36
        || sequence.len() != 16
        || !sequence
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return None;
    }
    let boot_id = Uuid::parse_str(encoded_boot_id).ok()?;
    if boot_id.get_version_num() != 4
        || boot_id.get_variant() != uuid::Variant::RFC4122
        || boot_id.to_string() != encoded_boot_id
    {
        return None;
    }
    let sequence = u64::from_str_radix(sequence, 16).ok()?;
    (sequence != 0).then_some((boot_id, sequence))
}

fn validated_sequence(inner: &ExecRequestRegistryInner, request_id: &str) -> Option<u64> {
    let (boot_id, sequence) = parse_request_id(request_id)?;
    (boot_id == inner.boot_id && sequence <= inner.issued_through).then_some(sequence)
}

/// Exact terminal receipt retained after the process has been reaped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecTerminalReceipt {
    pub exit_code: i32,
    pub forced: bool,
}

/// Shell-compatible terminal status published exactly once by an exec watcher.
#[derive(Clone)]
pub struct ExecCompletion {
    receiver: watch::Receiver<Option<ExecTerminalReceipt>>,
}

impl ExecCompletion {
    /// Wait until the watcher has observed and reaped the supervised process.
    /// None means the table was cleared without a terminal reap receipt.
    pub async fn wait(mut self) -> Option<ExecTerminalReceipt> {
        loop {
            if let Some(receipt) = *self.receiver.borrow() {
                return Some(receipt);
            }
            if self.receiver.changed().await.is_err() {
                return None;
            }
        }
    }
}

struct ProcessControl {
    identity: std::sync::Arc<ProcessIdentity>,
    supervised_group: bool,
    force_cancel_deadline: Option<tokio::time::Instant>,
    force_cancel_requested: bool,
    completion: watch::Sender<Option<ExecTerminalReceipt>>,
}

/// Result of atomically joining or starting one exec's durable cancellation.
pub enum ExecCancellation {
    Completed(ExecTerminalReceipt),
    Active {
        completion: ExecCompletion,
        force_deadline: tokio::time::Instant,
        start_driver: bool,
        driver_identity: std::sync::Arc<ProcessIdentity>,
        initial_term_failed: bool,
    },
}

/// An entry in the process table for a spawned child process.
pub struct ProcessEntry {
    /// The tokio child process handle.
    pub child: Child,
    /// The child's stdin pipe (if still open).
    pub stdin: Option<ChildStdin>,
}

/// Kernel process identity retained separately from the logical exec ID.
///
/// Linux container handshakes populate a pidfd so later signal delivery cannot
/// target an unrelated process after numeric PID reuse.
pub struct ProcessIdentity {
    pid: i32,
    #[cfg(target_os = "linux")]
    pid_fd: Option<std::os::fd::OwnedFd>,
}

impl ProcessIdentity {
    /// Construct an identity when only the spawned numeric PID is available.
    pub fn from_pid(pid: u32) -> Self {
        Self {
            pid: pid as i32,
            #[cfg(target_os = "linux")]
            pid_fd: None,
        }
    }

    #[cfg(target_os = "linux")]
    fn from_pidfd(pid: u32, pid_fd: std::os::fd::OwnedFd) -> Self {
        Self {
            pid: pid as i32,
            pid_fd: Some(pid_fd),
        }
    }

    /// Capture a pidfd for safe Linux signal delivery.
    #[cfg(target_os = "linux")]
    pub fn capture_pidfd(pid: u32) -> anyhow::Result<Self> {
        use std::os::fd::FromRawFd as _;

        // SAFETY: pidfd_open has no pointer arguments and returns a new fd.
        let raw_fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) as libc::c_int };
        if raw_fd < 0 {
            return Err(std::io::Error::last_os_error()).context("spawned pidfd_open failed");
        }
        // SAFETY: pidfd_open returned a new owned descriptor.
        let pid_fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(raw_fd) };
        Ok(Self::from_pidfd(pid, pid_fd))
    }

    /// Deliver a signal to the retained kernel process identity.
    pub fn signal(&self, signal: i32) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        if let Some(pid_fd) = self.pid_fd.as_ref() {
            use std::os::fd::AsRawFd as _;

            // SAFETY: pidfd_send_signal uses a live pidfd, a scalar signal,
            // and null siginfo for ordinary process-directed delivery.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_pidfd_send_signal,
                    pid_fd.as_raw_fd(),
                    signal,
                    std::ptr::null::<libc::siginfo_t>(),
                    0,
                )
            };
            if result == 0 {
                return Ok(());
            }
            return Err(std::io::Error::last_os_error());
        }

        #[cfg(target_os = "linux")]
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "spawned process has no retained pidfd",
        ));

        #[cfg(not(target_os = "linux"))]
        // SAFETY: kill is a standard POSIX function and `pid` is the captured
        // child PID, never the externally visible logical exec ID.
        if unsafe { libc::kill(self.pid, signal) } == 0 {
            Ok(())
        } else {
            Err(std::io::Error::last_os_error())
        }
    }

    /// Ask the Linux trampoline to atomically kill the supervised process
    /// group. The reserved real-time signal is never exposed as public exec
    /// control and is delivered through the retained pidfd.
    pub fn force_cancel(&self) -> std::io::Result<()> {
        #[cfg(target_os = "linux")]
        return self.signal(libc::SIGRTMIN());

        #[cfg(not(target_os = "linux"))]
        return self.signal(libc::SIGKILL);
    }
}

/// Strong identity captured immediately after spawning a container trampoline.
#[cfg(target_os = "linux")]
pub struct SpawnedProcessIdentity {
    process: ProcessIdentity,
    start_time: u64,
}

#[cfg(target_os = "linux")]
impl SpawnedProcessIdentity {
    /// Bind a numeric child PID to a pidfd and its `/proc` generation.
    pub fn capture(pid: u32) -> anyhow::Result<Self> {
        let process = ProcessIdentity::capture_pidfd(pid)?;
        let start_time = read_proc_start_time(pid)?;
        let identity = Self {
            process,
            start_time,
        };
        identity.ensure_same_generation()?;
        Ok(identity)
    }

    /// Numeric PID used only for SO_PEERCRED comparison and diagnostics.
    pub fn pid(&self) -> u32 {
        self.process.pid as u32
    }

    /// Captured Linux process generation used in the authenticated record.
    pub fn start_time(&self) -> u64 {
        self.start_time
    }

    /// Reject a disappeared or numerically reused sender deterministically.
    pub fn ensure_same_generation(&self) -> anyhow::Result<()> {
        let path = format!("/proc/{}/stat", self.pid());
        match std::fs::read_to_string(&path) {
            Ok(stat) => {
                if stat.len() > 64 * 1024 {
                    bail!("spawned process stat exceeds 65536 bytes");
                }
                validate_observed_generation(
                    self.start_time,
                    Some(parse_proc_start_time(&stat)?),
                    false,
                )?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if let Err(generation_error) =
                    validate_observed_generation(self.start_time, None, self.has_exited()?)
                {
                    return Err(error).with_context(|| {
                        format!(
                            "spawned process identity disappeared at {path}: {generation_error}"
                        )
                    });
                }
                // A queued Unix connection remains bound to its original peer
                // after that peer exits. The record still has to carry this
                // captured start time and the per-request random challenge.
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to read spawned process identity at {path}"));
            }
        }
        Ok(())
    }

    fn has_exited(&self) -> anyhow::Result<bool> {
        use std::os::fd::AsRawFd as _;

        let Some(pid_fd) = self.process.pid_fd.as_ref() else {
            bail!("spawned process identity omitted pidfd");
        };
        let mut poll_fd = libc::pollfd {
            fd: pid_fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: poll_fd is one initialized entry for a nonblocking poll.
        let result = unsafe { libc::poll(&raw mut poll_fd, 1, 0) };
        if result < 0 {
            return Err(std::io::Error::last_os_error()).context("spawned pidfd poll failed");
        }
        Ok(result != 0)
    }

    /// Retain the pidfd for safe signal delivery after readiness.
    pub fn into_process_identity(self) -> ProcessIdentity {
        self.process
    }
}

#[cfg(any(target_os = "linux", test))]
fn validate_observed_generation(
    expected_start_time: u64,
    observed_start_time: Option<u64>,
    captured_pidfd_exited: bool,
) -> anyhow::Result<()> {
    match observed_start_time {
        Some(observed) if observed == expected_start_time => Ok(()),
        Some(_) => bail!("spawned process PID was reused before readiness"),
        None if captured_pidfd_exited => Ok(()),
        None => bail!("live spawned process generation is unavailable"),
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_proc_start_time(stat: &str) -> anyhow::Result<u64> {
    let comm_end = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("spawned process stat omitted command terminator"))?;
    let remaining = stat
        .get(comm_end + 1..)
        .ok_or_else(|| anyhow::anyhow!("spawned process stat is truncated"))?;
    remaining
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| anyhow::anyhow!("spawned process stat omitted starttime"))?
        .parse::<u64>()
        .context("spawned process starttime is invalid")
}

#[cfg(target_os = "linux")]
fn read_proc_start_time(pid: u32) -> anyhow::Result<u64> {
    let path = format!("/proc/{pid}/stat");
    let stat = std::fs::read_to_string(&path)
        .with_context(|| format!("failed to read spawned process identity at {path}"))?;
    if stat.len() > 64 * 1024 {
        bail!("spawned process stat exceeds 65536 bytes");
    }
    parse_proc_start_time(&stat)
}

/// Table of active child processes, keyed by exec_id.
pub struct ProcessTable {
    entries: HashMap<u64, ProcessEntry>,
    /// PTY children from portable-pty (separate from tokio children).
    pty_children: HashMap<u64, Box<dyn portable_pty::Child + Send>>,
    controls: HashMap<u64, ProcessControl>,
    terminal_receipts: VecDeque<(u64, ExecTerminalReceipt)>,
    request_registry: ExecRequestRegistry,
}

#[allow(dead_code)]
impl ProcessTable {
    /// Create an empty process table.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            pty_children: HashMap::new(),
            controls: HashMap::new(),
            terminal_receipts: VecDeque::new(),
            request_registry: ExecRequestRegistry::new(),
        }
    }

    pub fn request_registry(&self) -> ExecRequestRegistry {
        self.request_registry.clone()
    }

    /// Insert a new process entry.
    pub fn insert(
        &mut self,
        exec_id: u64,
        child: Child,
        stdin: Option<ChildStdin>,
        identity: ProcessIdentity,
        supervised_group: bool,
    ) -> ExecCompletion {
        self.forget_terminal_receipt(exec_id);
        let (completion, receiver) = watch::channel(None);
        self.entries.insert(exec_id, ProcessEntry { child, stdin });
        self.controls.insert(
            exec_id,
            ProcessControl {
                identity: std::sync::Arc::new(identity),
                supervised_group,
                force_cancel_deadline: None,
                force_cancel_requested: false,
                completion,
            },
        );
        ExecCompletion { receiver }
    }

    /// Look up a process by exec_id.
    pub fn get(&self, exec_id: u64) -> Option<&ProcessEntry> {
        self.entries.get(&exec_id)
    }

    /// Look up a process mutably by exec_id.
    pub fn get_mut(&mut self, exec_id: u64) -> Option<&mut ProcessEntry> {
        self.entries.get_mut(&exec_id)
    }

    /// Remove a process from the table.
    pub fn remove(&mut self, exec_id: u64) -> Option<ProcessEntry> {
        self.controls.remove(&exec_id);
        self.entries.remove(&exec_id)
    }

    /// Check if the table is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Iterate over all entries.
    pub fn iter(&self) -> impl Iterator<Item = (&u64, &ProcessEntry)> {
        self.entries.iter()
    }

    /// Iterate mutably over all entries.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&u64, &mut ProcessEntry)> {
        self.entries.iter_mut()
    }

    /// Remove all entries from the table.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.pty_children.clear();
        self.controls.clear();
        self.terminal_receipts.clear();
        self.request_registry.clear();
    }

    /// Insert a PTY child process (from portable-pty).
    pub fn insert_pty(
        &mut self,
        exec_id: u64,
        child: Box<dyn portable_pty::Child + Send>,
        identity: ProcessIdentity,
        supervised_group: bool,
    ) -> ExecCompletion {
        self.forget_terminal_receipt(exec_id);
        let (completion, receiver) = watch::channel(None);
        self.pty_children.insert(exec_id, child);
        self.controls.insert(
            exec_id,
            ProcessControl {
                identity: std::sync::Arc::new(identity),
                supervised_group,
                force_cancel_deadline: None,
                force_cancel_requested: false,
                completion,
            },
        );
        ExecCompletion { receiver }
    }

    /// Remove and return a PTY child process handle.
    pub fn take_pty(&mut self, exec_id: u64) -> Option<Box<dyn portable_pty::Child + Send>> {
        self.pty_children.remove(&exec_id)
    }

    /// Signal the actual spawned process bound to this logical exec ID.
    pub fn signal(&self, exec_id: u64, signal: i32) -> Option<std::io::Result<()>> {
        self.controls.get(&exec_id).map(|control| {
            #[cfg(target_os = "linux")]
            let signal = routed_signal(signal, control.supervised_group, libc::SIGRTMIN());
            control.identity.signal(signal)
        })
    }

    /// Force the exact active exec's supervisor to kill its process group.
    pub fn force_cancel(&mut self, exec_id: u64) -> Option<std::io::Result<()>> {
        self.controls.get_mut(&exec_id).map(|control| {
            let result = if control.supervised_group {
                control.identity.force_cancel()
            } else {
                control.identity.signal(libc::SIGKILL)
            };
            if matches!(&result, Ok(()))
                || matches!(&result, Err(error) if error.raw_os_error() == Some(libc::ESRCH))
            {
                control.force_cancel_requested = true;
            }
            result
        })
    }

    /// Subscribe to the terminal status for an active exec.
    pub fn completion(&self, exec_id: u64) -> Option<ExecCompletion> {
        self.controls.get(&exec_id).map(|control| ExecCompletion {
            receiver: control.completion.subscribe(),
        })
    }

    /// Start cancellation exactly once, or subscribe to the already-running
    /// cancellation using its original TERM-to-KILL deadline.
    pub fn begin_cancel(
        &mut self,
        exec_id: u64,
        force_deadline: tokio::time::Instant,
    ) -> Option<ExecCancellation> {
        if let Some(receipt) = self.terminal_receipt(exec_id) {
            return Some(ExecCancellation::Completed(receipt));
        }
        let control = self.controls.get_mut(&exec_id)?;
        let completion = ExecCompletion {
            receiver: control.completion.subscribe(),
        };
        if let Some(existing_deadline) = control.force_cancel_deadline {
            return Some(ExecCancellation::Active {
                completion,
                force_deadline: existing_deadline,
                start_driver: false,
                driver_identity: control.identity.clone(),
                initial_term_failed: false,
            });
        }

        let term_result = control.identity.signal(libc::SIGTERM);
        let initial_term_failed =
            matches!(&term_result, Err(error) if error.raw_os_error() != Some(libc::ESRCH));
        control.force_cancel_deadline = Some(force_deadline);
        Some(ExecCancellation::Active {
            completion,
            force_deadline,
            start_driver: true,
            driver_identity: control.identity.clone(),
            initial_term_failed,
        })
    }

    #[cfg(test)]
    pub fn cancellation_deadline(&self, exec_id: u64) -> Option<tokio::time::Instant> {
        self.controls.get(&exec_id)?.force_cancel_deadline
    }

    /// Return the retained terminal receipt for an exact completed logical
    /// exec generation. Unknown and deterministically evicted IDs return None.
    pub fn terminal_receipt(&self, exec_id: u64) -> Option<ExecTerminalReceipt> {
        self.terminal_receipts
            .iter()
            .rev()
            .find_map(|(id, receipt)| (*id == exec_id).then_some(*receipt))
    }

    /// Publish terminal status and retire the exact logical exec generation.
    pub fn finish(&mut self, exec_id: u64, exit_code: i32) {
        if let Some(control) = self.controls.remove(&exec_id) {
            let receipt = ExecTerminalReceipt {
                exit_code,
                forced: control.force_cancel_requested,
            };
            self.remember_terminal_receipt(exec_id, receipt);
            self.request_registry.finish_exec(exec_id, receipt);
            let _ = control.completion.send(Some(receipt));
        }
        self.entries.remove(&exec_id);
        self.pty_children.remove(&exec_id);
    }

    fn remember_terminal_receipt(&mut self, exec_id: u64, receipt: ExecTerminalReceipt) {
        self.forget_terminal_receipt(exec_id);
        self.terminal_receipts.push_back((exec_id, receipt));
        while self.terminal_receipts.len() > TERMINAL_RECEIPT_CAPACITY {
            self.terminal_receipts.pop_front();
        }
    }

    fn forget_terminal_receipt(&mut self, exec_id: u64) {
        self.terminal_receipts.retain(|(id, _)| *id != exec_id);
    }
}

#[cfg(any(target_os = "linux", test))]
fn routed_signal(requested: i32, supervised_group: bool, force_cancel_signal: i32) -> i32 {
    if supervised_group && requested == libc::SIGKILL {
        force_cancel_signal
    } else {
        requested
    }
}

/// Convert a native process status into the public shell-compatible form.
#[cfg(unix)]
pub fn normalized_exit_status(status: std::process::ExitStatus) -> i32 {
    use std::os::unix::process::ExitStatusExt as _;

    status
        .code()
        .or_else(|| status.signal().map(|signal| 128 + signal))
        .unwrap_or(-1)
}

#[cfg(not(unix))]
pub fn normalized_exit_status(status: std::process::ExitStatus) -> i32 {
    status.code().unwrap_or(-1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_table() {
        let table = ProcessTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert!(table.get(1).is_none());
    }

    #[test]
    fn proc_start_time_parser_handles_parenthesized_commands() {
        let mut fields = vec!["S"; 20];
        fields[19] = "987654";
        let stat = format!("42 (worker ) name) {}", fields.join(" "));
        assert_eq!(parse_proc_start_time(&stat).unwrap(), 987_654);
        assert!(parse_proc_start_time("42 malformed").is_err());
    }

    #[test]
    fn spawned_generation_validation_rejects_pid_reuse_and_live_disappearance() {
        assert!(validate_observed_generation(10, Some(10), false).is_ok());
        assert!(validate_observed_generation(10, Some(11), true).is_err());
        assert!(validate_observed_generation(10, None, false).is_err());
        assert!(validate_observed_generation(10, None, true).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn signaled_status_is_shell_compatible() {
        use std::os::unix::process::ExitStatusExt as _;

        assert_eq!(
            normalized_exit_status(std::process::ExitStatus::from_raw(15)),
            143
        );
    }

    #[test]
    fn supervised_sigkill_routes_through_group_cancel_control() {
        let private_cancel = 63;
        assert_eq!(
            routed_signal(libc::SIGKILL, true, private_cancel),
            private_cancel
        );
        assert_eq!(
            routed_signal(libc::SIGKILL, false, private_cancel),
            libc::SIGKILL
        );
        assert_eq!(
            routed_signal(libc::SIGTERM, true, private_cancel),
            libc::SIGTERM
        );
    }

    #[test]
    fn terminal_receipts_are_exact_and_fifo_bounded() {
        let mut table = ProcessTable::new();
        for exec_id in 1..=(TERMINAL_RECEIPT_CAPACITY as u64 + 2) {
            table.remember_terminal_receipt(
                exec_id,
                ExecTerminalReceipt {
                    exit_code: 128 + (exec_id % 32) as i32,
                    forced: exec_id % 2 == 0,
                },
            );
        }

        assert_eq!(table.terminal_receipts.len(), TERMINAL_RECEIPT_CAPACITY);
        assert_eq!(table.terminal_receipt(1), None);
        assert_eq!(table.terminal_receipt(2), None);
        assert_eq!(
            table.terminal_receipt(3),
            Some(ExecTerminalReceipt {
                exit_code: 131,
                forced: false,
            })
        );
        let newest = TERMINAL_RECEIPT_CAPACITY as u64 + 2;
        assert_eq!(
            table.terminal_receipt(newest),
            Some(ExecTerminalReceipt {
                exit_code: 128 + (newest % 32) as i32,
                forced: newest % 2 == 0,
            })
        );
    }

    #[test]
    fn replacing_a_logical_exec_id_forgets_its_stale_receipt() {
        let mut table = ProcessTable::new();
        table.remember_terminal_receipt(
            42,
            ExecTerminalReceipt {
                exit_code: 143,
                forced: false,
            },
        );
        assert!(table.terminal_receipt(42).is_some());

        table.forget_terminal_receipt(42);
        assert_eq!(table.terminal_receipt(42), None);
    }

    #[test]
    fn missing_wait_authority_cannot_publish_a_terminal_receipt() {
        let mut table = ProcessTable::new();
        table.finish(42, -1);
        assert_eq!(table.terminal_receipt(42), None);
    }

    #[test]
    fn request_recovery_before_handler_atomically_fences_late_start() {
        let registry = ExecRequestRegistry::new();
        let request_id = registry.allocate_request_id().unwrap();

        assert_eq!(
            registry.reconcile(&request_id),
            ExecRequestReconcile::FencedNeverStarted
        );
        assert_eq!(
            registry.claim(&request_id).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );
        assert_eq!(
            registry.reconcile(&request_id),
            ExecRequestReconcile::FencedNeverStarted
        );
    }

    #[test]
    fn request_handler_before_recovery_publishes_exact_exec_and_receipt() {
        let registry = ExecRequestRegistry::new();
        let request_id = registry.allocate_request_id().unwrap();
        let mut permit = registry.claim(&request_id).unwrap();
        permit.authorize_start().unwrap();
        assert_eq!(
            registry.reconcile(&request_id),
            ExecRequestReconcile::Starting
        );
        permit.publish(42);
        assert_eq!(
            registry.reconcile(&request_id),
            ExecRequestReconcile::Published(42)
        );

        let receipt = ExecTerminalReceipt {
            exit_code: 137,
            forced: true,
        };
        registry.finish_exec(42, receipt);
        assert_eq!(
            registry.reconcile(&request_id),
            ExecRequestReconcile::Terminal(42, receipt)
        );
        assert_eq!(
            registry.claim(&request_id).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );
    }

    #[test]
    fn dropped_start_permit_is_a_replay_safe_fence() {
        let registry = ExecRequestRegistry::new();
        let request_id = registry.allocate_request_id().unwrap();
        let mut permit = registry.claim(&request_id).unwrap();
        permit.authorize_start().unwrap();
        drop(permit);

        for _ in 0..3 {
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::FencedNeverStarted
            );
            assert_eq!(
                registry.claim(&request_id).err(),
                Some(ExecRequestClaimError::DefiniteRejection)
            );
        }
    }

    #[test]
    fn recovery_racing_claim_is_consumed_when_handler_quiesces() {
        let registry = ExecRequestRegistry::new();
        let request_id = registry.allocate_request_id().unwrap();
        let (_, sequence) = parse_request_id(&request_id).unwrap();
        let permit = registry.claim(&request_id).unwrap();
        assert_eq!(
            registry.reconcile(&request_id),
            ExecRequestReconcile::FencedNeverStarted
        );
        drop(permit);

        let inner = registry.0.lock().unwrap();
        assert!(inner.completed.iter().any(|entry| *entry == sequence));
        assert_eq!(inner.states.get(&sequence), Some(&ExecRequestState::Fenced));
    }

    #[test]
    fn fence_receipts_are_bounded_and_eviction_permanently_rejects_replay() {
        let registry = ExecRequestRegistry::new();
        let mut request_ids = Vec::new();
        for _ in 0..(REQUEST_RECEIPT_CAPACITY + 2) {
            let request_id = registry.allocate_request_id().unwrap();
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::FencedNeverStarted
            );
            request_ids.push(request_id);
        }
        let oldest = &request_ids[0];
        assert_eq!(
            registry.reconcile(oldest),
            ExecRequestReconcile::StaleUnknown
        );
        assert_eq!(
            registry.claim(oldest).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );

        let replacement = registry.allocate_request_id().unwrap();
        let permit = registry.claim(&replacement).unwrap();
        drop(permit);
    }

    #[test]
    fn completed_request_receipts_are_fifo_bounded() {
        let registry = ExecRequestRegistry::new();
        let mut request_ids = Vec::new();
        for _ in 0..(REQUEST_RECEIPT_CAPACITY + 2) {
            let request_id = registry.allocate_request_id().unwrap();
            let permit = registry.claim(&request_id).unwrap();
            drop(permit);
            request_ids.push(request_id);
        }
        let inner = registry.0.lock().unwrap();
        assert_eq!(inner.completed.len(), REQUEST_RECEIPT_CAPACITY);
        assert_eq!(inner.states.len(), REQUEST_RECEIPT_CAPACITY);
        assert_eq!(inner.retired_through, 2);
        assert!(!inner.states.contains_key(&1));
        assert!(!inner.states.contains_key(&2));
        assert!(inner.states.contains_key(&3));
        drop(inner);

        for request_id in &request_ids[..2] {
            assert_eq!(
                registry.reconcile(request_id),
                ExecRequestReconcile::StaleUnknown
            );
            assert_eq!(
                registry.claim(request_id).err(),
                Some(ExecRequestClaimError::DefiniteRejection)
            );
        }
    }

    #[test]
    fn allocator_burns_tickets_without_retaining_registry_state() {
        let registry = ExecRequestRegistry::new();
        let first = registry.allocate_request_id().unwrap();
        let second = registry.allocate_request_id().unwrap();
        let third = registry.allocate_request_id().unwrap();

        let (boot_id, first_sequence) = parse_request_id(&first).unwrap();
        assert_eq!(first_sequence, 1);
        assert_eq!(parse_request_id(&second), Some((boot_id, 2)));
        assert_eq!(parse_request_id(&third), Some((boot_id, 3)));
        let inner = registry.0.lock().unwrap();
        assert!(inner.states.is_empty());
        assert!(inner.completed.is_empty());
    }

    #[test]
    fn allocator_exhaustion_fails_without_wrapping_or_mutating_state() {
        let registry = ExecRequestRegistry::new();
        {
            let mut inner = registry.0.lock().unwrap();
            inner.issued_through = u64::MAX;
        }
        assert_eq!(
            registry.allocate_request_id(),
            Err(ExecRequestAllocationExhausted)
        );
        let inner = registry.0.lock().unwrap();
        assert_eq!(inner.issued_through, u64::MAX);
        assert!(inner.states.is_empty());
        assert!(inner.completed.is_empty());
    }

    #[test]
    fn wrong_incarnation_unissued_future_and_malformed_tickets_fail_closed() {
        let registry = ExecRequestRegistry::new();
        let valid = registry.allocate_request_id().unwrap();
        let (boot_id, sequence) = parse_request_id(&valid).unwrap();
        let future = format_request_id(boot_id, sequence + 1);
        let wrong_incarnation = ExecRequestRegistry::new().allocate_request_id().unwrap();

        for request_id in [
            future.as_str(),
            wrong_incarnation.as_str(),
            "exec_req_not-a-uuid_0000000000000001",
            "exec_req_00000000-0000-4000-8000-000000000000_0000000000000000",
            "exec_req_00000000-0000-4000-8000-000000000000_000000000000000A",
        ] {
            assert_eq!(
                registry.claim(request_id).err(),
                Some(ExecRequestClaimError::DefiniteRejection)
            );
            assert_eq!(
                registry.reconcile(request_id),
                ExecRequestReconcile::StaleUnknown
            );
        }

        assert!(registry.claim(&valid).is_ok());
    }

    #[test]
    fn duplicate_live_claim_is_distinct_from_definite_rejection() {
        let registry = ExecRequestRegistry::new();
        let request_id = registry.allocate_request_id().unwrap();
        let permit = registry.claim(&request_id).unwrap();
        assert_eq!(
            registry.claim(&request_id).err(),
            Some(ExecRequestClaimError::Active)
        );
        drop(permit);
        assert_eq!(
            registry.claim(&request_id).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );
    }

    #[test]
    fn active_capacity_rejection_consumes_ticket_and_does_not_block_progress() {
        let registry = ExecRequestRegistry::new();
        let mut permits = Vec::new();
        for _ in 0..ACTIVE_REQUEST_CAPACITY {
            let request_id = registry.allocate_request_id().unwrap();
            permits.push(registry.claim(&request_id).unwrap());
        }

        let rejected = registry.allocate_request_id().unwrap();
        assert_eq!(
            registry.claim(&rejected).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );
        assert_eq!(
            registry.reconcile(&rejected),
            ExecRequestReconcile::FencedNeverStarted
        );

        drop(permits.pop());
        assert_eq!(
            registry.claim(&rejected).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );
        let replacement = registry.allocate_request_id().unwrap();
        assert!(registry.claim(&replacement).is_ok());
    }

    #[test]
    fn active_entry_remains_authoritative_below_retirement_watermark() {
        let registry = ExecRequestRegistry::new();
        let old_active = registry.allocate_request_id().unwrap();
        let mut permit = registry.claim(&old_active).unwrap();
        permit.authorize_start().unwrap();
        permit.publish(42);

        for _ in 0..(REQUEST_RECEIPT_CAPACITY + 2) {
            let request_id = registry.allocate_request_id().unwrap();
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::FencedNeverStarted
            );
        }
        assert_eq!(
            registry.reconcile(&old_active),
            ExecRequestReconcile::Published(42)
        );

        let receipt = ExecTerminalReceipt {
            exit_code: 143,
            forced: false,
        };
        registry.finish_exec(42, receipt);
        assert_eq!(
            registry.reconcile(&old_active),
            ExecRequestReconcile::Terminal(42, receipt)
        );

        for _ in 0..REQUEST_RECEIPT_CAPACITY {
            let request_id = registry.allocate_request_id().unwrap();
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::FencedNeverStarted
            );
        }
        assert_eq!(
            registry.reconcile(&old_active),
            ExecRequestReconcile::StaleUnknown
        );
        assert_eq!(
            registry.claim(&old_active).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );
    }

    #[test]
    fn delayed_lower_ticket_is_rejected_after_higher_fences_retire_it() {
        let registry = ExecRequestRegistry::new();
        let delayed = registry.allocate_request_id().unwrap();
        let mut later = Vec::new();
        for _ in 0..(REQUEST_RECEIPT_CAPACITY + 1) {
            later.push(registry.allocate_request_id().unwrap());
        }
        for request_id in later {
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::FencedNeverStarted
            );
        }

        assert_eq!(
            registry.reconcile(&delayed),
            ExecRequestReconcile::StaleUnknown
        );
        assert_eq!(
            registry.claim(&delayed).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );
    }

    #[test]
    fn concurrent_claim_and_reconcile_always_end_in_a_replay_safe_state() {
        for _ in 0..64 {
            let registry = ExecRequestRegistry::new();
            let request_id = registry.allocate_request_id().unwrap();
            let barrier = Arc::new(std::sync::Barrier::new(2));

            let claim_registry = registry.clone();
            let claim_request_id = request_id.clone();
            let claim_barrier = Arc::clone(&barrier);
            let claim = std::thread::spawn(move || {
                claim_barrier.wait();
                match claim_registry.claim(&claim_request_id) {
                    Ok(mut permit) => permit.authorize_start().is_ok(),
                    Err(ExecRequestClaimError::DefiniteRejection) => false,
                    Err(ExecRequestClaimError::Active) => {
                        panic!("first handler cannot race another active handler")
                    }
                }
            });

            let reconcile_registry = registry.clone();
            let reconcile_request_id = request_id.clone();
            let reconcile_barrier = Arc::clone(&barrier);
            let reconcile = std::thread::spawn(move || {
                reconcile_barrier.wait();
                reconcile_registry.reconcile(&reconcile_request_id)
            });

            let authorized = claim.join().unwrap();
            let observed = reconcile.join().unwrap();
            assert!(matches!(
                observed,
                ExecRequestReconcile::FencedNeverStarted | ExecRequestReconcile::Starting
            ));
            if !authorized {
                assert_eq!(observed, ExecRequestReconcile::FencedNeverStarted);
            }
            assert_eq!(
                registry.reconcile(&request_id),
                ExecRequestReconcile::FencedNeverStarted
            );
            assert_eq!(
                registry.claim(&request_id).err(),
                Some(ExecRequestClaimError::DefiniteRejection)
            );
        }
    }

    #[test]
    fn clearing_registry_retires_every_pre_clear_ticket() {
        let registry = ExecRequestRegistry::new();
        let allocated = registry.allocate_request_id().unwrap();
        registry.clear();
        assert_eq!(
            registry.reconcile(&allocated),
            ExecRequestReconcile::StaleUnknown
        );
        assert_eq!(
            registry.claim(&allocated).err(),
            Some(ExecRequestClaimError::DefiniteRejection)
        );

        let after_clear = registry.allocate_request_id().unwrap();
        assert!(registry.claim(&after_clear).is_ok());
    }
}
