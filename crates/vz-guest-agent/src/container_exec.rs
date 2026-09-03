//! Fail-closed Linux OCI container execution trampoline.
//!
//! The long-running agent never moves itself into a container cgroup. It
//! starts a fresh copy of this binary in a hidden mode; that single-threaded
//! child pins and validates the target, joins its cgroup, verifies membership,
//! and only then enters the target namespaces and directly executes the
//! requested command.

#[cfg(any(target_os = "linux", test))]
use std::ffi::{CString, OsString};
#[cfg(any(target_os = "linux", test))]
use std::os::unix::ffi::OsStrExt as _;
#[cfg(target_os = "linux")]
use std::path::Path;

#[cfg(any(target_os = "linux", test))]
use anyhow::Context;
use anyhow::bail;

pub(crate) const TRAMPOLINE_MARKER: &str = "__vz_container_exec_v1";
const SELF_EXE: &str = "/proc/self/exe";
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_FILE_BYTES: usize = 64 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_PATH_BYTES: usize = 4096;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_COMPONENT_BYTES: usize = 255;
const MAX_CONTAINER_ID_BYTES: usize = 128;

/// A command which starts the hidden, same-binary trampoline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TrampolineCommand {
    pub(crate) program: String,
    pub(crate) args: Vec<String>,
}

/// Original container command carried into the trampoline without shell parsing.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct TrampolineInvocation {
    container_id: String,
    command: String,
    args: Vec<String>,
    working_dir: Option<String>,
}

/// Immutable identity of a running container target.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct TargetIdentity {
    container_id: String,
    pid: u32,
    start_time: u64,
    cgroup_path: String,
    namespaces: NamespaceIdentity,
    root: ObjectIdentity,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ObjectIdentity {
    device: u64,
    inode: u64,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct NamespaceIdentity {
    mount: ObjectIdentity,
    network: ObjectIdentity,
    pid: ObjectIdentity,
    ipc: ObjectIdentity,
    uts: ObjectIdentity,
}

/// Fully allocated `execve(2)` inputs, prepared before namespace entry and
/// `fork(2)` so the child can use only direct syscalls.
#[cfg(any(target_os = "linux", test))]
struct ExecPayload {
    command_candidates: Vec<CString>,
    argv: Vec<CString>,
    argv_ptrs: Vec<*const libc::c_char>,
    environment: Vec<CString>,
    environment_ptrs: Vec<*const libc::c_char>,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum ChildSetupStage {
    ParentRace = 1,
    CgroupAttach = 2,
    CgroupVerify = 3,
    IpcNamespace = 4,
    UtsNamespace = 5,
    NetworkNamespace = 6,
    MountNamespace = 7,
    RootDirectory = 8,
    Chroot = 9,
    WorkingDirectory = 10,
    CloseDescriptors = 11,
    SignalState = 12,
    TargetRace = 13,
}

#[cfg(any(target_os = "linux", test))]
impl ChildSetupStage {
    fn description(self) -> &'static str {
        match self {
            Self::ParentRace => "parent liveness check",
            Self::CgroupAttach => "cgroup attachment",
            Self::CgroupVerify => "cgroup verification",
            Self::IpcNamespace => "IPC namespace entry",
            Self::UtsNamespace => "UTS namespace entry",
            Self::NetworkNamespace => "network namespace entry",
            Self::MountNamespace => "mount namespace entry",
            Self::RootDirectory => "root directory selection",
            Self::Chroot => "chroot",
            Self::WorkingDirectory => "working directory selection",
            Self::CloseDescriptors => "descriptor closure",
            Self::SignalState => "signal state reset",
            Self::TargetRace => "target init liveness check",
        }
    }

    fn from_code(code: u8) -> Option<Self> {
        Some(match code {
            1 => Self::ParentRace,
            2 => Self::CgroupAttach,
            3 => Self::CgroupVerify,
            4 => Self::IpcNamespace,
            5 => Self::UtsNamespace,
            6 => Self::NetworkNamespace,
            7 => Self::MountNamespace,
            8 => Self::RootDirectory,
            9 => Self::Chroot,
            10 => Self::WorkingDirectory,
            11 => Self::CloseDescriptors,
            12 => Self::SignalState,
            13 => Self::TargetRace,
            _ => return None,
        })
    }
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ChildSetupError {
    stage: ChildSetupStage,
    errno: libc::c_int,
}

#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
struct ChildLaunchFds {
    setup_error: libc::c_int,
    launcher_pidfd: libc::c_int,
    target_pidfd: libc::c_int,
    cgroup_procs: libc::c_int,
    ipc_ns: libc::c_int,
    uts_ns: libc::c_int,
    network_ns: libc::c_int,
    mount_ns: libc::c_int,
    root: libc::c_int,
    working_dir: libc::c_int,
}

/// Bounded operations used by the ordered launcher and deterministic tests.
#[cfg(any(target_os = "linux", test))]
trait LauncherOps {
    type Pinned;

    fn resolve(&mut self, container_id: &str) -> anyhow::Result<TargetIdentity>;
    fn pin(&mut self, target: &TargetIdentity) -> anyhow::Result<Self::Pinned>;
    fn revalidate(&mut self, target: &TargetIdentity, pinned: &Self::Pinned) -> anyhow::Result<()>;
    fn launch_command(
        &mut self,
        target: &TargetIdentity,
        pinned: &Self::Pinned,
        invocation: &TrampolineInvocation,
    ) -> anyhow::Result<()>;
}

/// Build a hidden child invocation for both pipe and PTY execution.
pub(crate) fn prepare_trampoline(
    container_id: &str,
    command: &str,
    args: &[String],
    working_dir: Option<&str>,
) -> anyhow::Result<TrampolineCommand> {
    validate_container_id(container_id)?;
    if command.is_empty() {
        bail!("container exec command cannot be empty");
    }

    let encoded_cwd = match working_dir {
        Some(value) => format!("s{value}"),
        None => "n".to_string(),
    };
    let mut trampoline_args = Vec::with_capacity(args.len() + 4);
    trampoline_args.push(TRAMPOLINE_MARKER.to_string());
    trampoline_args.push(container_id.to_string());
    trampoline_args.push(encoded_cwd);
    trampoline_args.push(command.to_string());
    trampoline_args.extend(args.iter().cloned());

    Ok(TrampolineCommand {
        program: SELF_EXE.to_string(),
        args: trampoline_args,
    })
}

/// Whether process arguments select the internal trampoline mode.
#[cfg(any(target_os = "linux", test))]
pub(crate) fn is_trampoline_request(args: &[OsString]) -> bool {
    args.first().is_some_and(|arg| arg == TRAMPOLINE_MARKER)
}

/// Run the Linux trampoline before Tokio, tracing, or clap initialize.
#[cfg(target_os = "linux")]
pub(crate) fn run_trampoline(args: Vec<OsString>) -> anyhow::Result<()> {
    let invocation = parse_trampoline_args(&args)?;
    execute_ordered(&invocation, &mut RealLauncherOps)
}

#[cfg(any(target_os = "linux", test))]
fn parse_trampoline_args(args: &[OsString]) -> anyhow::Result<TrampolineInvocation> {
    if !is_trampoline_request(args) {
        bail!("container exec trampoline marker is missing");
    }
    let container_id = required_utf8_arg(args, 1, "container id")?;
    validate_container_id(&container_id)?;
    let encoded_cwd = required_utf8_arg(args, 2, "working directory")?;
    let working_dir = if encoded_cwd == "n" {
        None
    } else if let Some(value) = encoded_cwd.strip_prefix('s') {
        Some(value.to_string())
    } else {
        bail!("container exec trampoline working directory encoding is invalid");
    };
    let command = required_utf8_arg(args, 3, "command")?;
    if command.is_empty() {
        bail!("container exec command cannot be empty");
    }
    let command_args = args[4..]
        .iter()
        .map(|arg| {
            arg.to_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow::anyhow!("container exec argument is not valid UTF-8"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(TrampolineInvocation {
        container_id,
        command,
        args: command_args,
        working_dir,
    })
}

#[cfg(any(target_os = "linux", test))]
fn required_utf8_arg(args: &[OsString], index: usize, name: &str) -> anyhow::Result<String> {
    args.get(index)
        .and_then(|arg| arg.to_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("container exec trampoline {name} is missing or not UTF-8"))
}

fn validate_container_id(container_id: &str) -> anyhow::Result<()> {
    if container_id.is_empty() || container_id.len() > MAX_CONTAINER_ID_BYTES {
        bail!("container id must contain 1..={MAX_CONTAINER_ID_BYTES} bytes");
    }
    if !container_id
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        || !container_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        bail!("container id contains unsupported characters");
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn execute_ordered<O: LauncherOps>(
    invocation: &TrampolineInvocation,
    ops: &mut O,
) -> anyhow::Result<()> {
    let target = ops.resolve(&invocation.container_id)?;
    let pinned = ops.pin(&target)?;
    ops.revalidate(&target, &pinned)?;
    ops.launch_command(&target, &pinned, invocation)
}

#[cfg(any(target_os = "linux", test))]
fn parse_unified_cgroup(content: &str) -> anyhow::Result<String> {
    if content.len() > MAX_CGROUP_FILE_BYTES {
        bail!("cgroup membership file exceeds {MAX_CGROUP_FILE_BYTES} bytes");
    }

    let mut unified = None;
    let mut saw_legacy = false;
    for line in content.lines() {
        if line.is_empty() {
            bail!("cgroup membership contains an empty record");
        }
        let mut fields = line.splitn(3, ':');
        let hierarchy = fields.next().unwrap_or_default();
        let controllers = fields.next();
        let path = fields.next();
        let Some(controllers) = controllers else {
            bail!("malformed cgroup membership record");
        };
        let Some(path) = path else {
            bail!("malformed cgroup membership record");
        };

        if hierarchy == "0" && controllers.is_empty() {
            if unified.replace(validate_cgroup_path(path)?).is_some() {
                bail!("cgroup membership contains multiple unified entries");
            }
        } else {
            saw_legacy = true;
        }
    }

    if saw_legacy {
        bail!("legacy or hybrid cgroup membership is not accepted");
    }
    unified.ok_or_else(|| anyhow::anyhow!("unified cgroup-v2 membership is missing"))
}

#[cfg(any(target_os = "linux", test))]
fn validate_cgroup_path(path: &str) -> anyhow::Result<String> {
    if path.len() > MAX_CGROUP_PATH_BYTES {
        bail!("cgroup path exceeds {MAX_CGROUP_PATH_BYTES} bytes");
    }
    if path == "/" {
        bail!("refusing to attach container exec to the cgroup root");
    }
    if !path.starts_with('/') {
        bail!("cgroup path must be absolute");
    }
    if path.chars().any(char::is_control) {
        bail!("cgroup path contains control characters");
    }

    for component in path[1..].split('/') {
        if component.is_empty() || matches!(component, "." | "..") {
            bail!("cgroup path contains an empty or traversal component");
        }
        if component.len() > MAX_CGROUP_COMPONENT_BYTES {
            bail!("cgroup path component exceeds {MAX_CGROUP_COMPONENT_BYTES} bytes");
        }
    }
    Ok(path.to_string())
}

#[cfg(target_os = "linux")]
pub(crate) const YOUKI_BIN: &str = "/run/vz-oci/bin/youki";
#[cfg(target_os = "linux")]
pub(crate) const YOUKI_ROOT: &str = "/run/vz-oci/state";
#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
#[cfg(target_os = "linux")]
const CGROUP2_SUPER_MAGIC: u64 = 0x6367_7270;

#[cfg(target_os = "linux")]
struct RealLauncherOps;

#[cfg(target_os = "linux")]
struct PinnedTarget {
    pid_fd: std::os::fd::OwnedFd,
    cgroup_dir: std::os::fd::OwnedFd,
    cgroup_procs: std::os::fd::OwnedFd,
    cgroup_identity: ObjectIdentity,
    mount_ns: std::os::fd::OwnedFd,
    network_ns: std::os::fd::OwnedFd,
    pid_ns: std::os::fd::OwnedFd,
    ipc_ns: std::os::fd::OwnedFd,
    uts_ns: std::os::fd::OwnedFd,
    root: std::os::fd::OwnedFd,
}

#[cfg(target_os = "linux")]
impl LauncherOps for RealLauncherOps {
    type Pinned = PinnedTarget;

    fn resolve(&mut self, container_id: &str) -> anyhow::Result<TargetIdentity> {
        resolve_target(container_id)
    }

    fn pin(&mut self, target: &TargetIdentity) -> anyhow::Result<Self::Pinned> {
        pin_target(target)
    }

    fn revalidate(&mut self, target: &TargetIdentity, pinned: &Self::Pinned) -> anyhow::Result<()> {
        ensure_pid_alive(&pinned.pid_fd)?;
        // Re-resolve through youki so a deleted/recreated container ID cannot
        // stay bound to a still-live stale PID whose proc identity is intact.
        let current = resolve_target(&target.container_id)?;
        if &current != target {
            bail!("container target identity changed before exec");
        }
        validate_pinned_identity(target, pinned)?;
        let current_cgroup = open_cgroup_dir(&target.cgroup_path)?;
        if fd_identity(&current_cgroup)? != pinned.cgroup_identity {
            bail!("container cgroup directory was replaced before exec");
        }
        ensure_cgroup2(&current_cgroup)
    }

    fn launch_command(
        &mut self,
        target: &TargetIdentity,
        pinned: &Self::Pinned,
        invocation: &TrampolineInvocation,
    ) -> anyhow::Result<()> {
        use std::os::fd::AsRawFd as _;

        // Resolve cwd while the pinned container root is still directly
        // addressable. `RESOLVE_IN_ROOT` prevents either `..` or symlinks from
        // escaping the target root.
        let working_dir = open_container_working_dir(
            &pinned.root,
            invocation.working_dir.as_deref().unwrap_or("/"),
        )?;
        let payload = prepare_exec_payload(invocation)?;
        let expected_cgroup = format!("0::{}", target.cgroup_path).into_bytes();

        // A pidfd remains meaningful across PID namespaces and closes the
        // PR_SET_PDEATHSIG race even though the child cannot name its parent
        // after joining a descendant PID namespace.
        let launcher_pidfd = open_pidfd(std::process::id())?;
        let (setup_reader, setup_writer) = setup_error_pipe()?;

        // This is the final path/identity check after every allocation and
        // immediately before PID namespace entry and fork.
        self.revalidate(target, pinned)?;

        let child_fds = ChildLaunchFds {
            setup_error: setup_writer.as_raw_fd(),
            launcher_pidfd: launcher_pidfd.as_raw_fd(),
            target_pidfd: pinned.pid_fd.as_raw_fd(),
            cgroup_procs: pinned.cgroup_procs.as_raw_fd(),
            ipc_ns: pinned.ipc_ns.as_raw_fd(),
            uts_ns: pinned.uts_ns.as_raw_fd(),
            network_ns: pinned.network_ns.as_raw_fd(),
            mount_ns: pinned.mount_ns.as_raw_fd(),
            root: pinned.root.as_raw_fd(),
            working_dir: working_dir.as_raw_fd(),
        };

        // PID setns affects only subsequently created children. Enter it in
        // the supervisor, then perform every other state-changing operation in
        // the child so the supervisor never consumes a target cgroup slot.
        enter_namespace(&pinned.pid_ns, libc::CLONE_NEWPID, "PID")?;

        // SAFETY: the trampoline is deliberately dispatched before Tokio and
        // remains single-threaded. All child inputs and descriptors were
        // prepared before this fork.
        let child = unsafe { libc::fork() };
        if child < 0 {
            return Err(std::io::Error::last_os_error()).context("container exec fork failed");
        }
        if child == 0 {
            // SAFETY: the child does not use the reader and never returns to
            // run its inherited Rust destructors.
            unsafe { libc::close(setup_reader.as_raw_fd()) };
            child_exec(child_fds, &expected_cgroup, &payload);
        }

        drop(setup_writer);
        let setup_error = read_setup_error(&setup_reader);
        let status = wait_for_child(child)?;
        let setup_error = setup_error?;
        if let Some(error) = setup_error {
            bail!(
                "container exec child setup failed at {}: {}",
                error.stage.description(),
                std::io::Error::from_raw_os_error(error.errno)
            );
        }
        mirror_wait_status(status)
    }
}

#[cfg(target_os = "linux")]
fn resolve_target(container_id: &str) -> anyhow::Result<TargetIdentity> {
    validate_container_id(container_id)?;
    let output = std::process::Command::new(YOUKI_BIN)
        .args(["--root", YOUKI_ROOT, "state", container_id])
        // Request environment belongs to the eventual container command and
        // must not influence the trusted state-resolution helper.
        .env_clear()
        .output()
        .with_context(|| format!("failed to execute fixed youki binary for `{container_id}`"))?;
    if !output.status.success() {
        bail!(
            "youki state failed for `{container_id}` with exit {}: {}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    if output.stdout.len() > 1024 * 1024 {
        bail!("youki state output exceeds 1048576 bytes");
    }
    let state: serde_json::Value =
        serde_json::from_slice(&output.stdout).context("failed to parse youki state")?;
    let status = state.get("status").and_then(serde_json::Value::as_str);
    if status != Some("running") {
        bail!(
            "container `{container_id}` is not running (status `{}`)",
            status.unwrap_or("unknown")
        );
    }
    let pid_u64 = state
        .get("pid")
        .and_then(serde_json::Value::as_u64)
        .filter(|pid| *pid > 0)
        .ok_or_else(|| anyhow::anyhow!("youki state omitted a positive init PID"))?;
    let pid = u32::try_from(pid_u64).context("youki init PID exceeds u32")?;
    inspect_target_identity(container_id, pid)
}

#[cfg(target_os = "linux")]
fn inspect_target_identity(container_id: &str, pid: u32) -> anyhow::Result<TargetIdentity> {
    use std::os::unix::fs::MetadataExt as _;

    let proc_root = format!("/proc/{pid}");

    let stat = read_bounded_utf8(
        Path::new(&format!("{proc_root}/stat")),
        64 * 1024,
        "target process stat",
    )?;
    let start_time = parse_proc_start_time(&stat)?;
    let cgroup = read_bounded_utf8(
        Path::new(&format!("{proc_root}/cgroup")),
        MAX_CGROUP_FILE_BYTES,
        "target cgroup membership",
    )?;
    let cgroup_path = parse_unified_cgroup(&cgroup)?;

    let identity = |suffix: &str| -> anyhow::Result<ObjectIdentity> {
        let metadata = std::fs::metadata(format!("{proc_root}/{suffix}"))
            .with_context(|| format!("failed to stat target {suffix}"))?;
        Ok(ObjectIdentity {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    };

    Ok(TargetIdentity {
        container_id: container_id.to_string(),
        pid,
        start_time,
        cgroup_path,
        namespaces: NamespaceIdentity {
            mount: identity("ns/mnt")?,
            network: identity("ns/net")?,
            pid: identity("ns/pid")?,
            ipc: identity("ns/ipc")?,
            uts: identity("ns/uts")?,
        },
        root: identity("root")?,
    })
}

#[cfg(any(target_os = "linux", test))]
fn parse_proc_start_time(stat: &str) -> anyhow::Result<u64> {
    let comm_end = stat
        .rfind(')')
        .ok_or_else(|| anyhow::anyhow!("target process stat omitted command terminator"))?;
    let remaining = stat
        .get(comm_end + 1..)
        .ok_or_else(|| anyhow::anyhow!("target process stat is truncated"))?;
    // The remaining fields start at field 3 (`state`); starttime is field 22.
    remaining
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| anyhow::anyhow!("target process stat omitted starttime"))?
        .parse::<u64>()
        .context("target process starttime is invalid")
}

#[cfg(target_os = "linux")]
fn pin_target(target: &TargetIdentity) -> anyhow::Result<PinnedTarget> {
    let pid_fd = open_pidfd(target.pid)?;
    ensure_pid_alive(&pid_fd)?;
    let proc_root = format!("/proc/{}", target.pid);
    let mount_ns = open_readonly(&format!("{proc_root}/ns/mnt"))?;
    let network_ns = open_readonly(&format!("{proc_root}/ns/net"))?;
    let pid_ns = open_readonly(&format!("{proc_root}/ns/pid"))?;
    let ipc_ns = open_readonly(&format!("{proc_root}/ns/ipc"))?;
    let uts_ns = open_readonly(&format!("{proc_root}/ns/uts"))?;
    let root = open_path_directory(&format!("{proc_root}/root"))?;

    let pinned_namespaces = NamespaceIdentity {
        mount: fd_identity(&mount_ns)?,
        network: fd_identity(&network_ns)?,
        pid: fd_identity(&pid_ns)?,
        ipc: fd_identity(&ipc_ns)?,
        uts: fd_identity(&uts_ns)?,
    };
    if pinned_namespaces != target.namespaces || fd_identity(&root)? != target.root {
        bail!("container namespace or root identity changed while pinning");
    }

    let cgroup_dir = open_cgroup_dir(&target.cgroup_path)?;
    ensure_cgroup2(&cgroup_dir)?;
    let cgroup_identity = fd_identity(&cgroup_dir)?;
    let cgroup_procs = open_at_file(&cgroup_dir, "cgroup.procs", libc::O_WRONLY)?;

    Ok(PinnedTarget {
        pid_fd,
        cgroup_dir,
        cgroup_procs,
        cgroup_identity,
        mount_ns,
        network_ns,
        pid_ns,
        ipc_ns,
        uts_ns,
        root,
    })
}

#[cfg(target_os = "linux")]
fn validate_pinned_identity(target: &TargetIdentity, pinned: &PinnedTarget) -> anyhow::Result<()> {
    let pinned_namespaces = NamespaceIdentity {
        mount: fd_identity(&pinned.mount_ns)?,
        network: fd_identity(&pinned.network_ns)?,
        pid: fd_identity(&pinned.pid_ns)?,
        ipc: fd_identity(&pinned.ipc_ns)?,
        uts: fd_identity(&pinned.uts_ns)?,
    };
    if pinned_namespaces != target.namespaces || fd_identity(&pinned.root)? != target.root {
        bail!("pinned container namespace or root identity changed");
    }
    if fd_identity(&pinned.cgroup_dir)? != pinned.cgroup_identity {
        bail!("pinned container cgroup identity changed");
    }
    ensure_cgroup2(&pinned.cgroup_dir)
}

#[cfg(target_os = "linux")]
fn read_bounded_utf8(path: &Path, max: usize, description: &str) -> anyhow::Result<String> {
    use std::io::Read as _;

    let file = std::fs::File::open(path)
        .with_context(|| format!("failed to open {description} at {}", path.display()))?;
    let mut bytes = Vec::new();
    file.take((max + 1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read {description}"))?;
    if bytes.len() > max {
        bail!("{description} exceeds {max} bytes");
    }
    String::from_utf8(bytes).with_context(|| format!("{description} is not valid UTF-8"))
}

#[cfg(target_os = "linux")]
fn open_cgroup_dir(path: &str) -> anyhow::Result<std::os::fd::OwnedFd> {
    use std::os::fd::AsRawFd as _;

    let validated = validate_cgroup_path(path)?;
    let mut current = open_directory(CGROUP_ROOT)?;
    for component in validated[1..].split('/') {
        current = open_at_file(&current, component, libc::O_RDONLY | libc::O_DIRECTORY)
            .with_context(|| format!("failed to pin cgroup component `{component}`"))?;
    }
    // Exercise the raw fd before returning so an invalid descriptor cannot be
    // confused with a successfully pinned directory.
    if current.as_raw_fd() < 0 {
        bail!("pinned cgroup directory has an invalid descriptor");
    }
    Ok(current)
}

#[cfg(target_os = "linux")]
fn open_directory(path: &str) -> anyhow::Result<std::os::fd::OwnedFd> {
    open_raw(path, libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW)
}

#[cfg(target_os = "linux")]
fn open_readonly(path: &str) -> anyhow::Result<std::os::fd::OwnedFd> {
    open_raw(path, libc::O_RDONLY)
}

#[cfg(target_os = "linux")]
fn open_path_directory(path: &str) -> anyhow::Result<std::os::fd::OwnedFd> {
    open_raw(path, libc::O_PATH | libc::O_DIRECTORY)
}

#[cfg(target_os = "linux")]
fn open_container_working_dir(
    root: &std::os::fd::OwnedFd,
    path: &str,
) -> anyhow::Result<std::os::fd::OwnedFd> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};

    if path.is_empty() || !path.starts_with('/') {
        bail!("container exec working directory must be an absolute path");
    }
    if path.len() > MAX_CGROUP_PATH_BYTES || path.chars().any(char::is_control) {
        bail!("container exec working directory is invalid or too long");
    }

    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }

    // linux/openat2.h: resolve all components, including absolute symlinks,
    // as though the pinned container root were `/`, and reject magic links.
    const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
    const RESOLVE_IN_ROOT: u64 = 0x10;

    let path = std::ffi::CString::new(path).context("working directory contains NUL")?;
    let how = OpenHow {
        flags: (libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: RESOLVE_IN_ROOT | RESOLVE_NO_MAGICLINKS,
    };
    // SAFETY: `root`, `path`, and `how` remain valid for the syscall, which
    // returns a new owned descriptor on success.
    let fd = unsafe {
        libc::syscall(
            libc::SYS_openat2,
            root.as_raw_fd(),
            path.as_ptr(),
            &raw const how,
            std::mem::size_of::<OpenHow>(),
        ) as libc::c_int
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "failed to pin container working directory `{}`",
                path.to_string_lossy()
            )
        });
    }
    // SAFETY: openat2 returned a fresh descriptor owned by this process.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}

#[cfg(any(target_os = "linux", test))]
fn prepare_exec_payload(invocation: &TrampolineInvocation) -> anyhow::Result<ExecPayload> {
    let mut argv = Vec::with_capacity(invocation.args.len() + 1);
    argv.push(
        CString::new(invocation.command.as_bytes())
            .context("container exec argv[0] contains NUL")?,
    );
    for argument in &invocation.args {
        argv.push(
            CString::new(argument.as_bytes()).context("container exec argument contains NUL")?,
        );
    }
    let argv_ptrs = argv
        .iter()
        .map(|argument| argument.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect();

    let mut environment = Vec::new();
    let mut search_path = None;
    for (key, value) in std::env::vars_os() {
        let key = key.as_os_str().as_bytes();
        let value = value.as_os_str().as_bytes();
        if key == b"PATH" {
            search_path = Some(value.to_vec());
        }
        let mut entry = Vec::with_capacity(key.len() + value.len() + 1);
        entry.extend_from_slice(key);
        entry.push(b'=');
        entry.extend_from_slice(value);
        environment.push(CString::new(entry).context("container exec environment contains NUL")?);
    }
    let environment_ptrs = environment
        .iter()
        .map(|entry| entry.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect();
    let command_candidates = prepare_command_candidates(
        invocation.command.as_bytes(),
        search_path.as_deref().unwrap_or(b"/bin:/usr/bin"),
    )?;

    Ok(ExecPayload {
        command_candidates,
        argv,
        argv_ptrs,
        environment,
        environment_ptrs,
    })
}

#[cfg(any(target_os = "linux", test))]
fn prepare_command_candidates(command: &[u8], search_path: &[u8]) -> anyhow::Result<Vec<CString>> {
    if command.is_empty() {
        bail!("container exec command cannot be empty");
    }
    if command.contains(&b'/') {
        return Ok(vec![
            CString::new(command).context("container exec command contains NUL")?,
        ]);
    }

    search_path
        .split(|byte| *byte == b':')
        .map(|directory| {
            let mut candidate = Vec::with_capacity(directory.len() + command.len() + 1);
            if !directory.is_empty() {
                candidate.extend_from_slice(directory);
                if !directory.ends_with(b"/") {
                    candidate.push(b'/');
                }
            }
            candidate.extend_from_slice(command);
            CString::new(candidate).context("container exec PATH candidate contains NUL")
        })
        .collect()
}

#[cfg(target_os = "linux")]
fn enter_namespace(
    namespace: &std::os::fd::OwnedFd,
    namespace_type: libc::c_int,
    description: &str,
) -> anyhow::Result<()> {
    use std::os::fd::AsRawFd as _;

    // SAFETY: `namespace` is a live namespace fd and `namespace_type`
    // identifies the expected namespace kind.
    if unsafe { libc::setns(namespace.as_raw_fd(), namespace_type) } != 0 {
        return Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to enter target {description} namespace"));
    }
    Ok(())
}

/// Child half of the direct launcher. This function never returns and avoids
/// allocation after `fork(2)`.
#[cfg(target_os = "linux")]
fn child_exec(fds: ChildLaunchFds, expected_cgroup: &[u8], payload: &ExecPayload) -> ! {
    // Keep the CString storage visibly live for the raw pointer arrays.
    let _storage = (&payload.argv, &payload.environment);

    // SAFETY: prctl has no pointer arguments for PR_SET_PDEATHSIG.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        child_setup_fail(
            fds.setup_error,
            ChildSetupStage::ParentRace,
            current_errno(),
        );
    }
    if !pidfd_is_alive_raw(fds.launcher_pidfd) {
        child_setup_fail(fds.setup_error, ChildSetupStage::ParentRace, libc::ESRCH);
    }

    // Writing "0" moves only this child into the exact pinned target cgroup.
    // The supervisor remains outside, avoiding an unnecessary pids.max slot.
    if let Err(errno) = write_all_raw(fds.cgroup_procs, b"0") {
        child_setup_fail(fds.setup_error, ChildSetupStage::CgroupAttach, errno);
    }
    match child_cgroup_matches(expected_cgroup) {
        Ok(true) => {}
        Ok(false) => {
            child_setup_fail(fds.setup_error, ChildSetupStage::CgroupVerify, libc::EPERM);
        }
        Err(errno) => {
            child_setup_fail(fds.setup_error, ChildSetupStage::CgroupVerify, errno);
        }
    }

    child_setns(
        fds.setup_error,
        fds.ipc_ns,
        libc::CLONE_NEWIPC,
        ChildSetupStage::IpcNamespace,
    );
    child_setns(
        fds.setup_error,
        fds.uts_ns,
        libc::CLONE_NEWUTS,
        ChildSetupStage::UtsNamespace,
    );
    child_setns(
        fds.setup_error,
        fds.network_ns,
        libc::CLONE_NEWNET,
        ChildSetupStage::NetworkNamespace,
    );
    child_setns(
        fds.setup_error,
        fds.mount_ns,
        libc::CLONE_NEWNS,
        ChildSetupStage::MountNamespace,
    );

    // The cwd fd was opened beneath this exact pinned root. Root first, then
    // chroot("."), then cwd prevents either path from being re-resolved.
    // SAFETY: `root_fd` is a live directory fd inherited from the parent.
    if unsafe { libc::fchdir(fds.root) } != 0 {
        child_setup_fail(
            fds.setup_error,
            ChildSetupStage::RootDirectory,
            current_errno(),
        );
    }

    // SAFETY: the literal is a valid NUL-terminated C string.
    if unsafe { libc::chroot(c".".as_ptr()) } != 0 {
        child_setup_fail(fds.setup_error, ChildSetupStage::Chroot, current_errno());
    }
    // SAFETY: `working_dir_fd` is a live directory fd beneath `root_fd`.
    if unsafe { libc::fchdir(fds.working_dir) } != 0 {
        child_setup_fail(
            fds.setup_error,
            ChildSetupStage::WorkingDirectory,
            current_errno(),
        );
    }

    // Do not leak the agent's signal policy into the requested program.
    // SAFETY: the signal set is initialized before use and affects only this
    // post-fork child.
    unsafe {
        if libc::signal(libc::SIGPIPE, libc::SIG_DFL) == libc::SIG_ERR {
            child_setup_fail(
                fds.setup_error,
                ChildSetupStage::SignalState,
                current_errno(),
            );
        }
        let mut empty: libc::sigset_t = std::mem::zeroed();
        if libc::sigemptyset(&raw mut empty) != 0
            || libc::sigprocmask(libc::SIG_SETMASK, &raw const empty, std::ptr::null_mut()) != 0
        {
            child_setup_fail(
                fds.setup_error,
                ChildSetupStage::SignalState,
                current_errno(),
            );
        }
    }

    // Recheck the pinned container init immediately before descriptor closure
    // and exec. A dead init invalidates the namespace/root target even though
    // the pinned descriptors themselves remain usable.
    if !pidfd_is_alive_raw(fds.target_pidfd) {
        child_setup_fail(fds.setup_error, ChildSetupStage::TargetRace, libc::ESRCH);
    }

    // Preserve only the CLOEXEC setup-error pipe until execve. A successful
    // exec closes it and tells the supervisor setup completed; all preparation
    // descriptors are closed immediately.
    if !close_fds_except(fds.setup_error) {
        child_setup_fail(
            fds.setup_error,
            ChildSetupStage::CloseDescriptors,
            current_errno(),
        );
    }

    let mut saw_access_denied = false;
    for candidate in &payload.command_candidates {
        // SAFETY: all strings and pointer arrays are NUL-terminated and remain
        // live. `execve` returns only on failure.
        unsafe {
            libc::execve(
                candidate.as_ptr(),
                payload.argv_ptrs.as_ptr(),
                payload.environment_ptrs.as_ptr(),
            );
        }
        let errno = current_errno();
        if errno == libc::EACCES {
            saw_access_denied = true;
        } else if errno != libc::ENOENT && errno != libc::ENOTDIR {
            child_exit(126);
        }
    }
    child_exit(exec_failure_exit_code(if saw_access_denied {
        libc::EACCES
    } else {
        libc::ENOENT
    }));
}

#[cfg(target_os = "linux")]
fn child_setns(
    setup_error_fd: libc::c_int,
    namespace_fd: libc::c_int,
    namespace_type: libc::c_int,
    stage: ChildSetupStage,
) {
    // SAFETY: `namespace_fd` is pinned and `namespace_type` is its expected
    // namespace kind.
    if unsafe { libc::setns(namespace_fd, namespace_type) } != 0 {
        child_setup_fail(setup_error_fd, stage, current_errno());
    }
}

#[cfg(target_os = "linux")]
fn setup_error_pipe() -> anyhow::Result<(std::os::fd::OwnedFd, std::os::fd::OwnedFd)> {
    use std::os::fd::FromRawFd as _;

    let mut descriptors = [-1; 2];
    // SAFETY: `descriptors` has room for the two fds returned by pipe2.
    if unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("failed to create container exec setup-error pipe");
    }
    // SAFETY: pipe2 returned two distinct owned descriptors.
    Ok(unsafe {
        (
            std::os::fd::OwnedFd::from_raw_fd(descriptors[0]),
            std::os::fd::OwnedFd::from_raw_fd(descriptors[1]),
        )
    })
}

#[cfg(target_os = "linux")]
fn read_setup_error(reader: &std::os::fd::OwnedFd) -> anyhow::Result<Option<ChildSetupError>> {
    use std::os::fd::AsRawFd as _;

    let mut record = [0_u8; 5];
    let mut used = 0;
    loop {
        // SAFETY: the remaining record buffer is writable and `reader` is the
        // parent end of the setup-error pipe.
        let result = unsafe {
            libc::read(
                reader.as_raw_fd(),
                record[used..].as_mut_ptr().cast(),
                record.len() - used,
            )
        };
        if result == 0 {
            break;
        }
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error).context("failed to read container exec setup status");
        }
        used += result as usize;
        if used == record.len() {
            break;
        }
    }

    if used == 0 {
        return Ok(None);
    }
    if used != record.len() {
        bail!("container exec child returned a truncated setup error");
    }
    decode_setup_error(record)
        .map(Some)
        .ok_or_else(|| anyhow::anyhow!("container exec child returned an unknown setup stage"))
}

#[cfg(any(target_os = "linux", test))]
fn encode_setup_error(error: ChildSetupError) -> [u8; 5] {
    let mut record = [0_u8; 5];
    record[0] = error.stage as u8;
    record[1..].copy_from_slice(&error.errno.to_ne_bytes());
    record
}

#[cfg(any(target_os = "linux", test))]
fn decode_setup_error(record: [u8; 5]) -> Option<ChildSetupError> {
    let stage = ChildSetupStage::from_code(record[0])?;
    let errno = libc::c_int::from_ne_bytes([record[1], record[2], record[3], record[4]]);
    Some(ChildSetupError { stage, errno })
}

#[cfg(target_os = "linux")]
fn child_setup_fail(setup_error_fd: libc::c_int, stage: ChildSetupStage, errno: libc::c_int) -> ! {
    let errno = if errno == 0 { libc::EIO } else { errno };
    let record = encode_setup_error(ChildSetupError { stage, errno });
    let _ = write_all_raw(setup_error_fd, &record);
    child_exit(126)
}

#[cfg(target_os = "linux")]
fn write_all_raw(fd: libc::c_int, mut bytes: &[u8]) -> Result<(), libc::c_int> {
    while !bytes.is_empty() {
        // SAFETY: `bytes` is readable and `fd` is a live pipe or
        // cgroup.procs descriptor.
        let written = unsafe { libc::write(fd, bytes.as_ptr().cast(), bytes.len()) };
        if written < 0 {
            let errno = current_errno();
            if errno == libc::EINTR {
                continue;
            }
            return Err(errno);
        }
        if written == 0 {
            return Err(libc::EIO);
        }
        bytes = &bytes[written as usize..];
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn close_fds_except(preserved_fd: libc::c_int) -> bool {
    if preserved_fd < 3 {
        return false;
    }
    if preserved_fd > 3
        // SAFETY: close_range has no pointer arguments.
        && unsafe {
            libc::syscall(
                libc::SYS_close_range,
                3_u32,
                (preserved_fd - 1) as u32,
                0_u32,
            )
        } != 0
    {
        return false;
    }
    if preserved_fd < libc::c_int::MAX
        // SAFETY: close_range has no pointer arguments.
        && unsafe {
            libc::syscall(
                libc::SYS_close_range,
                (preserved_fd + 1) as u32,
                u32::MAX,
                0_u32,
            )
        } != 0
    {
        return false;
    }
    true
}

#[cfg(target_os = "linux")]
fn current_errno() -> libc::c_int {
    // SAFETY: Linux libc provides a thread-local errno pointer.
    unsafe { *libc::__errno_location() }
}

#[cfg(target_os = "linux")]
fn pidfd_is_alive_raw(pidfd: libc::c_int) -> bool {
    let mut poll_fd = libc::pollfd {
        fd: pidfd,
        events: libc::POLLIN,
        revents: 0,
    };
    loop {
        // SAFETY: `poll_fd` points to one initialized pollfd for this call.
        let result = unsafe { libc::poll(&raw mut poll_fd, 1, 0) };
        if result == 0 {
            return true;
        }
        if result > 0 {
            return false;
        }
        if current_errno() != libc::EINTR {
            return false;
        }
    }
}

#[cfg(target_os = "linux")]
fn child_cgroup_matches(expected: &[u8]) -> Result<bool, libc::c_int> {
    let mut contents = [0_u8; MAX_CGROUP_FILE_BYTES + 1];
    // SAFETY: the path literal is NUL-terminated and the returned fd is owned
    // by this child until explicitly closed below.
    let fd = unsafe {
        libc::open(
            c"/proc/self/cgroup".as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(current_errno());
    }

    let mut used = 0;
    let read_result = loop {
        // SAFETY: the remaining buffer is writable and `fd` is open.
        let result = unsafe {
            libc::read(
                fd,
                contents[used..].as_mut_ptr().cast(),
                contents.len() - used,
            )
        };
        if result == 0 {
            break Ok(used <= MAX_CGROUP_FILE_BYTES);
        }
        if result < 0 {
            if current_errno() == libc::EINTR {
                continue;
            }
            break Err(current_errno());
        }
        used += result as usize;
        if used > MAX_CGROUP_FILE_BYTES {
            break Ok(false);
        }
    };
    // SAFETY: `fd` was opened by this function and is no longer used.
    unsafe { libc::close(fd) };

    read_result.map(|read_ok| read_ok && exact_cgroup_record_matches(&contents[..used], expected))
}

#[cfg(any(target_os = "linux", test))]
fn exact_cgroup_record_matches(contents: &[u8], expected: &[u8]) -> bool {
    let record = contents.strip_suffix(b"\n").unwrap_or(contents);
    record == expected
}

#[cfg(any(target_os = "linux", test))]
fn exec_failure_exit_code(errno: libc::c_int) -> libc::c_int {
    if errno == libc::ENOENT { 127 } else { 126 }
}

#[cfg(target_os = "linux")]
fn child_exit(code: libc::c_int) -> ! {
    // SAFETY: `_exit` terminates only the post-fork child without running
    // inherited Rust destructors.
    unsafe { libc::_exit(code) }
}

#[cfg(target_os = "linux")]
fn wait_for_child(child: libc::pid_t) -> anyhow::Result<libc::c_int> {
    let mut status = 0;
    loop {
        // SAFETY: `child` is the direct child returned by fork and `status` is
        // writable for this call.
        let result = unsafe { libc::waitpid(child, &raw mut status, 0) };
        if result == child {
            break;
        }
        if result < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(std::io::Error::last_os_error()).context("container exec waitpid failed");
    }
    Ok(status)
}

#[cfg(target_os = "linux")]
fn mirror_wait_status(status: libc::c_int) -> anyhow::Result<()> {
    if libc::WIFEXITED(status) {
        child_exit(libc::WEXITSTATUS(status));
    }
    if libc::WIFSIGNALED(status) {
        mirror_signal(libc::WTERMSIG(status));
    }
    bail!("container exec child returned an unrecognized wait status {status}")
}

#[cfg(target_os = "linux")]
fn mirror_signal(signo: libc::c_int) -> ! {
    // Restore and unblock the terminating signal before delivering it to this
    // trampoline, so its caller observes the same wait status as the child.
    // SAFETY: all operations use initialized values and the current PID.
    unsafe {
        libc::signal(signo, libc::SIG_DFL);
        let mut signals: libc::sigset_t = std::mem::zeroed();
        libc::sigemptyset(&raw mut signals);
        libc::sigaddset(&raw mut signals, signo);
        libc::sigprocmask(libc::SIG_UNBLOCK, &raw const signals, std::ptr::null_mut());
        libc::kill(libc::getpid(), signo);
        libc::_exit(128 + signo);
    }
}

#[cfg(target_os = "linux")]
fn open_raw(path: &str, flags: libc::c_int) -> anyhow::Result<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd as _;

    let path = std::ffi::CString::new(path).context("path contains NUL")?;
    // SAFETY: `path` is a valid C string and the returned descriptor is owned.
    let fd = unsafe { libc::open(path.as_ptr(), flags | libc::O_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open failed");
    }
    // SAFETY: `open` returned a new owned descriptor.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn open_at_file(
    directory: &std::os::fd::OwnedFd,
    name: &str,
    flags: libc::c_int,
) -> anyhow::Result<std::os::fd::OwnedFd> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};

    if name.is_empty() || name.contains('/') || matches!(name, "." | "..") {
        bail!("openat name is not a single safe component");
    }
    let name = std::ffi::CString::new(name).context("openat name contains NUL")?;
    // SAFETY: `directory` and `name` are valid; O_NOFOLLOW prevents symlink
    // substitution at every cgroup path component and at `cgroup.procs`.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            flags | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("openat failed");
    }
    // SAFETY: `openat` returned a new owned descriptor.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn open_pidfd(pid: u32) -> anyhow::Result<std::os::fd::OwnedFd> {
    use std::os::fd::FromRawFd as _;

    // SAFETY: pidfd_open has no pointer arguments and returns a new fd.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) as libc::c_int };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("pidfd_open failed");
    }
    // SAFETY: pidfd_open returned a new owned descriptor.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn ensure_pid_alive(pid_fd: &std::os::fd::OwnedFd) -> anyhow::Result<()> {
    use std::os::fd::AsRawFd as _;

    let mut poll_fd = libc::pollfd {
        fd: pid_fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: `poll_fd` points to one initialized pollfd for this call.
    let result = unsafe { libc::poll(&raw mut poll_fd, 1, 0) };
    if result < 0 {
        return Err(std::io::Error::last_os_error()).context("pidfd poll failed");
    }
    if result != 0 {
        bail!("container init process disappeared before exec");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn fd_identity(fd: &std::os::fd::OwnedFd) -> anyhow::Result<ObjectIdentity> {
    use std::os::fd::AsRawFd as _;

    // SAFETY: zero is a valid initial representation for `stat`; fstat fills it.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `fd` is live and `stat` is writable for the duration of the call.
    if unsafe { libc::fstat(fd.as_raw_fd(), &raw mut stat) } != 0 {
        return Err(std::io::Error::last_os_error()).context("fstat failed");
    }
    Ok(ObjectIdentity {
        device: stat.st_dev,
        inode: stat.st_ino,
    })
}

#[cfg(target_os = "linux")]
fn ensure_cgroup2(fd: &std::os::fd::OwnedFd) -> anyhow::Result<()> {
    use std::os::fd::AsRawFd as _;

    // SAFETY: zero is a valid initial representation; fstatfs fills the struct.
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    // SAFETY: `fd` is live and `stat` is writable for the duration of the call.
    if unsafe { libc::fstatfs(fd.as_raw_fd(), &raw mut stat) } != 0 {
        return Err(std::io::Error::last_os_error()).context("fstatfs failed for cgroup");
    }
    if stat.f_type as u64 != CGROUP2_SUPER_MAGIC {
        bail!("resolved container cgroup is not on a cgroup2 filesystem");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use std::collections::HashSet;

    use super::*;

    fn identity(container_id: &str) -> TargetIdentity {
        let object = ObjectIdentity {
            device: 8,
            inode: 42,
        };
        TargetIdentity {
            container_id: container_id.to_string(),
            pid: 4242,
            start_time: 123_456,
            cgroup_path: "/youki/web".to_string(),
            namespaces: NamespaceIdentity {
                mount: object,
                network: object,
                pid: object,
                ipc: object,
                uts: object,
            },
            root: object,
        }
    }

    #[test]
    fn unified_cgroup_parser_accepts_one_bounded_non_root_path() {
        assert_eq!(
            parse_unified_cgroup("0::/youki/workspace-web\n").unwrap(),
            "/youki/workspace-web"
        );
    }

    #[test]
    fn unified_cgroup_parser_rejects_malformed_and_unsafe_inputs() {
        let overlong_path = format!("0::/{}\n", "a".repeat(MAX_CGROUP_PATH_BYTES));
        let overlong_component = format!("0::/{}\n", "a".repeat(256));
        let cases = [
            "",
            "garbage\n",
            "5:cpu:/legacy\n",
            "0::/one\n0::/two\n",
            "0::/unified\n5:cpu:/legacy\n",
            "0::/\n",
            "0::relative\n",
            "0::/one/../two\n",
            "0::/one/./two\n",
            "0::/one//two\n",
            "0::/one/\n",
            "0::/one\u{7f}two\n",
            &overlong_path,
            &overlong_component,
        ];
        for case in cases {
            assert!(
                parse_unified_cgroup(case).is_err(),
                "expected rejection for {case:?}"
            );
        }
    }

    #[test]
    fn unified_cgroup_parser_bounds_the_membership_file() {
        let oversized = "x".repeat(MAX_CGROUP_FILE_BYTES + 1);
        assert!(parse_unified_cgroup(&oversized).is_err());
    }

    #[test]
    fn proc_start_time_parser_handles_spaces_and_parentheses_in_comm() {
        let fields_3_through_22 = [
            "S", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
            "16", "17", "18", "987654",
        ];
        let stat = format!(
            "4242 (worker ) with spaces) {}",
            fields_3_through_22.join(" ")
        );
        assert_eq!(parse_proc_start_time(&stat).unwrap(), 987_654);
        assert!(parse_proc_start_time("4242 malformed").is_err());
    }

    #[test]
    fn trampoline_round_trip_preserves_direct_argv_and_cwd() {
        let original_args = vec![
            "a b".to_string(),
            "$HOME;touch /tmp/no".to_string(),
            "--".to_string(),
            "'quoted'".to_string(),
        ];
        let command = prepare_trampoline(
            "workspace-web",
            "/bin/printf",
            &original_args,
            Some("/work dir"),
        )
        .unwrap();
        assert_eq!(command.program, SELF_EXE);
        let parsed = parse_trampoline_args(
            &command
                .args
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(parsed.container_id, "workspace-web");
        assert_eq!(parsed.command, "/bin/printf");
        assert_eq!(parsed.args, original_args);
        assert_eq!(parsed.working_dir.as_deref(), Some("/work dir"));
    }

    #[test]
    fn direct_exec_payload_preserves_raw_argv() {
        let invocation = TrampolineInvocation {
            container_id: "web".to_string(),
            command: "/bin/printf".to_string(),
            args: vec!["%s".to_string(), "$HOME;literal".to_string()],
            working_dir: Some("/workspace".to_string()),
        };
        let payload = prepare_exec_payload(&invocation).unwrap();
        let argv = payload
            .argv
            .iter()
            .map(|argument| argument.to_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            payload.command_candidates[0].to_str().unwrap(),
            "/bin/printf"
        );
        assert_eq!(argv, ["/bin/printf", "%s", "$HOME;literal"]);
        assert!(!payload.environment.is_empty());
        assert!(payload.argv_ptrs.last().unwrap().is_null());
        assert!(payload.environment_ptrs.last().unwrap().is_null());
    }

    #[test]
    fn unqualified_commands_expand_into_preallocated_path_candidates() {
        let candidates = prepare_command_candidates(b"redis-cli", b"/usr/local/bin:/usr/bin:")
            .unwrap()
            .into_iter()
            .map(|candidate| candidate.to_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            candidates,
            [
                "/usr/local/bin/redis-cli",
                "/usr/bin/redis-cli",
                "redis-cli"
            ]
        );
        assert_eq!(
            prepare_command_candidates(b"./tool", b"/ignored").unwrap()[0]
                .to_str()
                .unwrap(),
            "./tool"
        );
    }

    #[test]
    fn child_cgroup_check_requires_one_exact_host_relative_record() {
        assert!(exact_cgroup_record_matches(
            b"0::/youki/web\n",
            b"0::/youki/web"
        ));
        assert!(exact_cgroup_record_matches(
            b"0::/youki/web",
            b"0::/youki/web"
        ));
        assert!(!exact_cgroup_record_matches(b"0::/\n", b"0::/youki/web"));
        assert!(!exact_cgroup_record_matches(
            b"0::/youki/web\n5:cpu:/legacy\n",
            b"0::/youki/web"
        ));
    }

    #[test]
    fn direct_exec_failure_codes_match_shell_convention() {
        assert_eq!(exec_failure_exit_code(libc::ENOENT), 127);
        assert_eq!(exec_failure_exit_code(libc::EACCES), 126);
        assert_eq!(exec_failure_exit_code(libc::ENOEXEC), 126);
    }

    #[test]
    fn setup_error_record_round_trips_stage_and_errno() {
        let error = ChildSetupError {
            stage: ChildSetupStage::CgroupAttach,
            errno: libc::EBUSY,
        };
        assert_eq!(error.stage.description(), "cgroup attachment");
        assert_eq!(decode_setup_error(encode_setup_error(error)), Some(error));
        assert!(decode_setup_error([u8::MAX, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn trampoline_rejects_ambiguous_container_identifiers() {
        for id in ["", "../web", "web/service", "web\nother", "--root"] {
            assert!(prepare_trampoline(id, "/bin/true", &[], None).is_err());
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    enum Stage {
        Resolve,
        Pin,
        Revalidate,
        Exec,
    }

    struct FakeOps {
        calls: Vec<Stage>,
        fail: Option<Stage>,
    }

    impl FakeOps {
        fn record(&mut self, stage: Stage) -> anyhow::Result<()> {
            self.calls.push(stage);
            if self.fail == Some(stage) {
                bail!("injected {stage:?} failure");
            }
            Ok(())
        }
    }

    impl LauncherOps for FakeOps {
        type Pinned = ();

        fn resolve(&mut self, container_id: &str) -> anyhow::Result<TargetIdentity> {
            self.record(Stage::Resolve)?;
            Ok(identity(container_id))
        }

        fn pin(&mut self, _target: &TargetIdentity) -> anyhow::Result<Self::Pinned> {
            self.record(Stage::Pin)
        }

        fn revalidate(
            &mut self,
            _target: &TargetIdentity,
            _pinned: &Self::Pinned,
        ) -> anyhow::Result<()> {
            self.record(Stage::Revalidate)
        }

        fn launch_command(
            &mut self,
            _target: &TargetIdentity,
            _pinned: &Self::Pinned,
            _invocation: &TrampolineInvocation,
        ) -> anyhow::Result<()> {
            self.record(Stage::Exec)
        }
    }

    fn invocation() -> TrampolineInvocation {
        TrampolineInvocation {
            container_id: "web".to_string(),
            command: "/bin/true".to_string(),
            args: Vec::new(),
            working_dir: Some("/".to_string()),
        }
    }

    #[test]
    fn launcher_orders_resolution_pin_revalidation_and_launch() {
        let mut ops = FakeOps {
            calls: Vec::new(),
            fail: None,
        };
        execute_ordered(&invocation(), &mut ops).unwrap();
        assert_eq!(
            ops.calls,
            [Stage::Resolve, Stage::Pin, Stage::Revalidate, Stage::Exec]
        );
    }

    #[test]
    fn launcher_never_execs_after_any_pre_exec_failure() {
        let failure_stages = HashSet::from([Stage::Resolve, Stage::Pin, Stage::Revalidate]);
        for fail in failure_stages {
            let mut ops = FakeOps {
                calls: Vec::new(),
                fail: Some(fail),
            };
            let error = execute_ordered(&invocation(), &mut ops)
                .expect_err("injected launcher failure must propagate");
            assert!(error.to_string().contains("injected"));
            assert!(
                !ops.calls.contains(&Stage::Exec),
                "exec reached after {fail:?}"
            );
        }
    }

    #[test]
    fn revalidation_failure_prevents_launch() {
        let mut ops = FakeOps {
            calls: Vec::new(),
            fail: Some(Stage::Revalidate),
        };
        execute_ordered(&invocation(), &mut ops).unwrap_err();
        assert!(ops.calls.contains(&Stage::Revalidate));
        assert!(!ops.calls.contains(&Stage::Exec));
    }
}
