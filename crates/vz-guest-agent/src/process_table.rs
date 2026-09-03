//! Process table for tracking spawned child processes.
//!
//! Maps exec_id to process entries, providing lookup, insertion,
//! removal, and iteration for the connection handler.

use std::collections::HashMap;

#[cfg(any(target_os = "linux", test))]
use anyhow::Context as _;
#[cfg(any(target_os = "linux", test))]
use anyhow::bail;

use tokio::process::{Child, ChildStdin};

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
    identities: HashMap<u64, ProcessIdentity>,
}

#[allow(dead_code)]
impl ProcessTable {
    /// Create an empty process table.
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            pty_children: HashMap::new(),
            identities: HashMap::new(),
        }
    }

    /// Insert a new process entry.
    pub fn insert(
        &mut self,
        exec_id: u64,
        child: Child,
        stdin: Option<ChildStdin>,
        identity: ProcessIdentity,
    ) {
        self.entries.insert(exec_id, ProcessEntry { child, stdin });
        self.identities.insert(exec_id, identity);
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
        self.identities.remove(&exec_id);
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
        self.identities.clear();
    }

    /// Insert a PTY child process (from portable-pty).
    pub fn insert_pty(
        &mut self,
        exec_id: u64,
        child: Box<dyn portable_pty::Child + Send>,
        identity: ProcessIdentity,
    ) {
        self.pty_children.insert(exec_id, child);
        self.identities.insert(exec_id, identity);
    }

    /// Remove and return a PTY child process handle.
    pub fn take_pty(&mut self, exec_id: u64) -> Option<Box<dyn portable_pty::Child + Send>> {
        self.pty_children.remove(&exec_id)
    }

    /// Signal the actual spawned process bound to this logical exec ID.
    pub fn signal(&self, exec_id: u64, signal: i32) -> Option<std::io::Result<()>> {
        self.identities
            .get(&exec_id)
            .map(|identity| identity.signal(signal))
    }

    /// Wait for a PTY child to exit, returning its exit code.
    ///
    /// Must be called from an async context — internally uses `spawn_blocking`
    /// since portable-pty's `Child::wait` is synchronous.
    pub async fn wait_pty(&mut self, exec_id: u64) -> i32 {
        let Some(mut child) = self.take_pty(exec_id) else {
            return -1;
        };
        self.identities.remove(&exec_id);
        // portable-pty Child::wait() is blocking, run on a thread.
        tokio::task::spawn_blocking(move || match child.wait() {
            Ok(status) => status.exit_code() as i32,
            Err(_) => -1,
        })
        .await
        .unwrap_or(-1)
    }
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
}
