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
#[cfg(target_os = "linux")]
use std::sync::atomic::{AtomicI32, Ordering};

#[cfg(any(target_os = "linux", test))]
use anyhow::Context;
use anyhow::bail;

// Ordinary Machine commands share only supervision primitives, never OCI
// resolution, target identities, cgroup attachment, or namespace entry.
#[cfg(any(target_os = "linux", test))]
pub(crate) mod machine;

pub(crate) const TRAMPOLINE_MARKER: &str = "__vz_container_exec_v4";
const SELF_EXE: &str = "/proc/self/exe";
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_FILE_BYTES: usize = 64 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_PATH_BYTES: usize = 4096;
#[cfg(any(target_os = "linux", test))]
const MAX_CGROUP_COMPONENT_BYTES: usize = 255;
const MAX_CONTAINER_ID_BYTES: usize = 128;
const READY_CHALLENGE_BYTES: usize = 32;
const READY_CHALLENGE_HEX_BYTES: usize = READY_CHALLENGE_BYTES * 2;
#[cfg(any(target_os = "linux", test))]
const MAX_IDENTITY_FILE_BYTES: usize = 1024 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_IDENTITY_RECORD_BYTES: usize = 16 * 1024;
#[cfg(any(target_os = "linux", test))]
const MAX_IDENTITY_NAME_BYTES: usize = 256;
#[cfg(any(target_os = "linux", test))]
const MAX_SUPPLEMENTARY_GROUPS: usize = 1024;
#[cfg(target_os = "linux")]
const DESCENDANT_REAP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Child process group currently owned by this single-use trampoline.
#[cfg(target_os = "linux")]
static SUPERVISED_PROCESS_GROUP: AtomicI32 = AtomicI32::new(0);
/// Private control signal which means "kill the complete supervised group".
#[cfg(target_os = "linux")]
static FORCE_CANCEL_SIGNAL: AtomicI32 = AtomicI32::new(0);

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
    expected_parent_pid: u32,
    expected_parent_start_time: u64,
    container_id: String,
    command: String,
    args: Vec<String>,
    working_dir: Option<String>,
    user: Option<String>,
    retain_shell_environment: bool,
    ready_socket: Option<String>,
    ready_challenge: Option<String>,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
struct ResolvedIdentity {
    uid: libc::uid_t,
    gid: libc::gid_t,
    supplementary_groups: Vec<libc::gid_t>,
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

/// Immutable guest-observed identity reported only after successful execve.
#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContainerReadyIdentity {
    pub(crate) container_id: String,
    pub(crate) pid: u32,
    pub(crate) start_time: u64,
    pub(crate) cgroup_path: String,
    pub(crate) cgroup: (u64, u64),
    pub(crate) namespaces: [(u64, u64); 5],
    pub(crate) root: (u64, u64),
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
    identity: ResolvedIdentity,
    identity_verification_groups: Vec<libc::gid_t>,
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
    SupplementaryGroups = 14,
    GroupIdentity = 15,
    UserIdentity = 16,
    IdentityVerify = 17,
    Execve = 18,
    DeathSentinel = 19,
    SupervisorReady = 20,
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
            Self::SupplementaryGroups => "supplementary group selection",
            Self::GroupIdentity => "group identity selection",
            Self::UserIdentity => "user identity selection",
            Self::IdentityVerify => "execution identity verification",
            Self::Execve => "command execve",
            Self::DeathSentinel => "death sentinel launch",
            Self::SupervisorReady => "supervisor readiness gate",
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
            14 => Self::SupplementaryGroups,
            15 => Self::GroupIdentity,
            16 => Self::UserIdentity,
            17 => Self::IdentityVerify,
            18 => Self::Execve,
            19 => Self::DeathSentinel,
            20 => Self::SupervisorReady,
            _ => return None,
        })
    }
}

#[cfg(any(target_os = "linux", test))]
fn setup_failure_requires_group_termination(stage: ChildSetupStage) -> bool {
    stage == ChildSetupStage::DeathSentinel
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
    launch_gate_reader: libc::c_int,
    launch_gate_writer: libc::c_int,
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
#[cfg(test)]
pub(crate) fn prepare_trampoline(
    container_id: &str,
    command: &str,
    args: &[String],
    working_dir: Option<&str>,
    user: Option<&str>,
    retain_shell_environment: bool,
) -> anyhow::Result<TrampolineCommand> {
    prepare_trampoline_with_ready_socket(
        container_id,
        command,
        args,
        working_dir,
        user,
        retain_shell_environment,
        None,
    )
}

/// Build a trampoline invocation which reports an authenticated ready record
/// to a private agent-owned Unix socket.
pub(crate) fn prepare_trampoline_with_ready_socket(
    container_id: &str,
    command: &str,
    args: &[String],
    working_dir: Option<&str>,
    user: Option<&str>,
    retain_shell_environment: bool,
    ready_handshake: Option<(&str, &str)>,
) -> anyhow::Result<TrampolineCommand> {
    validate_container_id(container_id)?;
    if command.is_empty() {
        bail!("container exec command cannot be empty");
    }

    let encoded_cwd = match working_dir {
        Some(value) => format!("s{value}"),
        None => "n".to_string(),
    };
    let encoded_user = match user {
        Some(value) if !value.is_empty() => format!("s{value}"),
        _ => "n".to_string(),
    };
    let (encoded_ready_socket, encoded_ready_challenge) = match ready_handshake {
        Some((socket, challenge)) if !socket.is_empty() && !challenge.is_empty() => {
            validate_ready_challenge(challenge)?;
            (format!("s{socket}"), format!("s{challenge}"))
        }
        Some(_) => bail!("container exec ready socket and challenge cannot be empty"),
        None => ("n".to_string(), "n".to_string()),
    };
    let (expected_parent_pid, expected_parent_start_time) = expected_agent_parent_identity()?;
    let mut trampoline_args = Vec::with_capacity(args.len() + 10);
    trampoline_args.push(TRAMPOLINE_MARKER.to_string());
    trampoline_args.push(expected_parent_pid.to_string());
    trampoline_args.push(expected_parent_start_time.to_string());
    trampoline_args.push(container_id.to_string());
    trampoline_args.push(encoded_cwd);
    trampoline_args.push(encoded_user);
    trampoline_args.push(if retain_shell_environment { "s" } else { "n" }.to_string());
    trampoline_args.push(encoded_ready_socket);
    trampoline_args.push(encoded_ready_challenge);
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
    arm_outer_parent_death(
        invocation.expected_parent_pid,
        invocation.expected_parent_start_time,
    )?;
    let admission = ContainerAdmissionGuard::shared(&invocation.container_id)?;
    let mut ops = RealLauncherOps {
        admission: Some(admission),
        ready_socket: invocation.ready_socket.clone(),
        ready_challenge: invocation.ready_challenge.clone(),
        sender_start_time: read_process_start_time(std::process::id())?,
    };
    let result = execute_ordered(&invocation, &mut ops);
    if let Err(error) = &result {
        ops.notify_failure(error);
    }
    result
}

#[cfg(any(target_os = "linux", test))]
fn parse_trampoline_args(args: &[OsString]) -> anyhow::Result<TrampolineInvocation> {
    if !is_trampoline_request(args) {
        bail!("container exec trampoline marker is missing");
    }
    let expected_parent_pid = required_utf8_arg(args, 1, "expected parent PID")?
        .parse::<u32>()
        .context("container exec expected parent PID is invalid")?;
    let expected_parent_start_time = required_utf8_arg(args, 2, "expected parent start time")?
        .parse::<u64>()
        .context("container exec expected parent start time is invalid")?;
    validate_expected_parent_identity(expected_parent_pid, expected_parent_start_time)?;
    let container_id = required_utf8_arg(args, 3, "container id")?;
    validate_container_id(&container_id)?;
    let encoded_cwd = required_utf8_arg(args, 4, "working directory")?;
    let working_dir = if encoded_cwd == "n" {
        None
    } else if let Some(value) = encoded_cwd.strip_prefix('s') {
        Some(value.to_string())
    } else {
        bail!("container exec trampoline working directory encoding is invalid");
    };
    let encoded_user = required_utf8_arg(args, 5, "user")?;
    let user = if encoded_user == "n" {
        None
    } else if let Some(value) = encoded_user
        .strip_prefix('s')
        .filter(|value| !value.is_empty())
    {
        Some(value.to_string())
    } else {
        bail!("container exec trampoline user encoding is invalid");
    };
    let retain_shell_environment = match required_utf8_arg(args, 6, "SHELL policy")?.as_str() {
        "s" => true,
        "n" => false,
        _ => bail!("container exec trampoline SHELL policy encoding is invalid"),
    };
    let encoded_ready_socket = required_utf8_arg(args, 7, "ready socket")?;
    let ready_socket = if encoded_ready_socket == "n" {
        None
    } else if let Some(value) = encoded_ready_socket.strip_prefix('s') {
        validate_ready_socket(value)?;
        Some(value.to_string())
    } else {
        bail!("container exec trampoline ready socket encoding is invalid");
    };
    let encoded_ready_challenge = required_utf8_arg(args, 8, "ready challenge")?;
    let ready_challenge = if encoded_ready_challenge == "n" {
        None
    } else if let Some(value) = encoded_ready_challenge.strip_prefix('s') {
        validate_ready_challenge(value)?;
        Some(value.to_string())
    } else {
        bail!("container exec trampoline ready challenge encoding is invalid");
    };
    if ready_socket.is_some() != ready_challenge.is_some() {
        bail!("container exec ready socket and challenge must be supplied together");
    }
    let command = required_utf8_arg(args, 9, "command")?;
    if command.is_empty() {
        bail!("container exec command cannot be empty");
    }
    let command_args = args[10..]
        .iter()
        .map(|arg| {
            arg.to_str()
                .map(str::to_string)
                .ok_or_else(|| anyhow::anyhow!("container exec argument is not valid UTF-8"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(TrampolineInvocation {
        expected_parent_pid,
        expected_parent_start_time,
        container_id,
        command,
        args: command_args,
        working_dir,
        user,
        retain_shell_environment,
        ready_socket,
        ready_challenge,
    })
}

#[cfg(target_os = "linux")]
fn expected_agent_parent_identity() -> anyhow::Result<(u32, u64)> {
    let pid = std::process::id();
    Ok((pid, read_process_start_time(pid)?))
}

#[cfg(not(target_os = "linux"))]
fn expected_agent_parent_identity() -> anyhow::Result<(u32, u64)> {
    Ok((std::process::id(), 1))
}

#[cfg(any(target_os = "linux", test))]
fn validate_expected_parent_identity(pid: u32, start_time: u64) -> anyhow::Result<()> {
    if pid == 0 || start_time == 0 {
        bail!("container exec expected parent identity must be nonzero");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn arm_outer_parent_death(expected_pid: u32, expected_start_time: u64) -> anyhow::Result<()> {
    validate_expected_parent_identity(expected_pid, expected_start_time)?;
    let observed_parent = unsafe { libc::getppid() };
    if observed_parent <= 0 || observed_parent as u32 != expected_pid {
        bail!("container exec agent parent changed before trampoline entry");
    }
    let parent_pidfd = open_pidfd(expected_pid)?;
    if read_process_start_time(expected_pid)? != expected_start_time {
        bail!("container exec agent parent PID was reused before trampoline entry");
    }
    // SAFETY: prctl receives scalar arguments and arms this outer trampoline.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("cannot arm container exec trampoline parent death");
    }
    ensure_pid_alive(&parent_pidfd)?;
    if unsafe { libc::getppid() } as u32 != expected_pid
        || read_process_start_time(expected_pid)? != expected_start_time
    {
        bail!("container exec agent parent changed while arming parent death");
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn validate_ready_socket(value: &str) -> anyhow::Result<()> {
    let path = std::path::Path::new(value);
    if path.parent() != Some(std::path::Path::new("/run/vz-agent-exec"))
        || !path.file_name().is_some_and(|name| {
            let bytes = name.as_encoded_bytes();
            !bytes.is_empty()
                && bytes.len() <= 96
                && bytes
                    .iter()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        })
    {
        bail!("container exec ready socket path is invalid");
    }
    Ok(())
}

fn validate_ready_challenge(value: &str) -> anyhow::Result<()> {
    if value.len() != READY_CHALLENGE_HEX_BYTES
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        bail!(
            "container exec ready challenge must be exactly {READY_CHALLENGE_HEX_BYTES} hex bytes"
        );
    }
    Ok(())
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
const CONTAINER_ADMISSION_ROOT: &str = "/run/vz-oci/admission";
#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
#[cfg(target_os = "linux")]
const CGROUP2_SUPER_MAGIC: u64 = 0x6367_7270;

#[cfg(target_os = "linux")]
struct RealLauncherOps {
    admission: Option<ContainerAdmissionGuard>,
    ready_socket: Option<String>,
    ready_challenge: Option<String>,
    sender_start_time: u64,
}

#[cfg(target_os = "linux")]
impl RealLauncherOps {
    fn notify_ready(&mut self, target: &TargetIdentity, pinned: &PinnedTarget) {
        let Some(socket) = self.ready_socket.as_deref() else {
            // Unary compatibility execution has no early-ready protocol, so
            // retain shared admission until the command terminates.
            return;
        };
        let Some(challenge) = self.ready_challenge.as_deref() else {
            return;
        };
        let identity = ready_identity(target, pinned);
        let _ = send_ready_record(
            socket,
            &encode_ready_identity(challenge, self.sender_start_time, &identity),
        );
        // The immutable descriptors and successfully exec'd child now own the
        // old generation. Guest lifecycle mutation may safely continue.
        self.admission.take();
    }

    fn notify_failure(&self, error: &anyhow::Error) {
        let Some(socket) = self.ready_socket.as_deref() else {
            return;
        };
        let Some(challenge) = self.ready_challenge.as_deref() else {
            return;
        };
        let detail = error
            .to_string()
            .chars()
            .map(|character| {
                if character.is_control() {
                    ' '
                } else {
                    character
                }
            })
            .take(2048)
            .collect::<String>();
        let _ = send_ready_record(
            socket,
            format!("ERROR\t{challenge}\t{}\t{detail}", self.sender_start_time).as_bytes(),
        );
    }
}

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
        let identity = resolve_container_identity(&pinned.root, invocation.user.as_deref())?;
        let mut payload = prepare_exec_payload(invocation, identity)?;
        let expected_cgroup = format!("0::{}", target.cgroup_path).into_bytes();

        // A pidfd remains meaningful across PID namespaces and closes the
        // PR_SET_PDEATHSIG race even though the child cannot name its parent
        // after joining a descendant PID namespace.
        let launcher_pidfd = open_pidfd(std::process::id())?;
        let (setup_reader, setup_writer) = setup_error_pipe()?;
        let (launch_gate_reader, launch_gate_writer) = setup_error_pipe()?;
        enable_child_subreaper()?;
        install_supervisor_signal_handlers()?;

        // This is the final path/identity check after every allocation and
        // immediately before PID namespace entry and fork.
        self.revalidate(target, pinned)?;

        let child_fds = ChildLaunchFds {
            setup_error: setup_writer.as_raw_fd(),
            launch_gate_reader: launch_gate_reader.as_raw_fd(),
            launch_gate_writer: launch_gate_writer.as_raw_fd(),
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
            child_exec(child_fds, &expected_cgroup, &mut payload);
        }
        drop(launch_gate_reader);

        // Bind the child to its own process group while forwarded signals are
        // still blocked. This closes the signal-before-registration race: a
        // pending signal cannot be delivered until the group is addressable.
        if let Err(error) = bind_supervised_process_group(child) {
            // SAFETY: child is the exact direct child just returned by fork.
            unsafe { libc::kill(child, libc::SIGKILL) };
            let _ = wait_for_child(child);
            return Err(error);
        }
        if let Err(error) =
            launch_death_sentinel(child, setup_writer.as_raw_fd(), launcher_pidfd.as_raw_fd())
        {
            // SAFETY: child is the exact group leader established above.
            unsafe { libc::kill(-child, libc::SIGKILL) };
            let _ = wait_for_child(child);
            let _ = terminate_and_reap_descendants(child);
            return Err(error);
        }
        if let Err(error) = assign_foreground_process_group(child) {
            // SAFETY: child is the exact group leader established above.
            unsafe { libc::kill(-child, libc::SIGKILL) };
            let _ = wait_for_child(child);
            let _ = terminate_and_reap_descendants(child);
            return Err(error);
        }
        SUPERVISED_PROCESS_GROUP.store(child, Ordering::Release);
        write_all_raw(launch_gate_writer.as_raw_fd(), b"R")
            .map_err(std::io::Error::from_raw_os_error)
            .context("cannot release container exec supervisor gate")?;
        drop(launch_gate_writer);
        unblock_supervisor_signals()?;

        drop(setup_writer);
        let setup_error = read_setup_error(&setup_reader);
        let setup_error = setup_error?;
        if let Some(error) = setup_error {
            // The sentinel acknowledges only after its parent-death policy is
            // armed, but descriptor cleanup immediately after that ack can
            // still fail. The command gate is open by then, so fail closed by
            // terminating the registered group before waiting for its leader.
            if setup_failure_requires_group_termination(error.stage) {
                // SAFETY: child remains the exact process-group leader bound
                // above; ESRCH merely means the group already terminated.
                let result = unsafe { libc::kill(-child, libc::SIGKILL) };
                if result != 0 {
                    let kill_error = std::io::Error::last_os_error();
                    if kill_error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(kill_error).context(
                            "cannot terminate container exec after death sentinel failure",
                        );
                    }
                }
            }
            let status = wait_for_child(child)?;
            terminate_and_reap_descendants(child)?;
            if error.stage == ChildSetupStage::Execve {
                let failure = anyhow::anyhow!(
                    "container exec child failed at {}: {}",
                    error.stage.description(),
                    std::io::Error::from_raw_os_error(error.errno)
                );
                self.notify_failure(&failure);
                return mirror_wait_status(status);
            }
            bail!(
                "container exec child setup failed at {}: {}",
                error.stage.description(),
                std::io::Error::from_raw_os_error(error.errno)
            );
        }
        // CLOEXEC EOF proves execve only while the child is still live. If it
        // already exited normally, the absence of a failure record proves it
        // reached the requested image because every pre-exec exit writes one.
        // A signal before observation is ambiguous and therefore fails closed.
        let status = match try_wait_for_child(child)? {
            Some(status) if libc::WIFSIGNALED(status) => {
                let failure = anyhow::anyhow!(
                    "container exec child was signaled before successful execve could be proven"
                );
                self.notify_failure(&failure);
                return mirror_wait_status(status);
            }
            Some(status) => {
                self.notify_ready(target, pinned);
                status
            }
            None => {
                self.notify_ready(target, pinned);
                wait_for_child(child)?
            }
        };
        terminate_and_reap_descendants(child)?;
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
fn read_process_start_time(pid: u32) -> anyhow::Result<u64> {
    let path = format!("/proc/{pid}/stat");
    let stat = read_bounded_utf8(Path::new(&path), 64 * 1024, "process stat")?;
    parse_proc_start_time(&stat)
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
pub(crate) struct ContainerAdmissionGuard {
    file: std::fs::File,
}

#[cfg(target_os = "linux")]
impl ContainerAdmissionGuard {
    pub(crate) fn shared(container_id: &str) -> anyhow::Result<Self> {
        Self::acquire(container_id, libc::LOCK_SH)
    }

    pub(crate) fn exclusive(container_id: &str) -> anyhow::Result<Self> {
        Self::acquire(container_id, libc::LOCK_EX)
    }

    fn acquire(container_id: &str, operation: libc::c_int) -> anyhow::Result<Self> {
        use std::os::fd::AsRawFd as _;
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _};

        validate_container_id(container_id)?;
        std::fs::create_dir_all(CONTAINER_ADMISSION_ROOT)
            .context("failed to create container admission directory")?;
        let directory = std::fs::symlink_metadata(CONTAINER_ADMISSION_ROOT)
            .context("failed to inspect container admission directory")?;
        if !directory.is_dir() || directory.file_type().is_symlink() || directory.uid() != 0 {
            bail!("container admission directory is not a root-owned real directory");
        }
        std::fs::set_permissions(
            CONTAINER_ADMISSION_ROOT,
            std::fs::Permissions::from_mode(0o700),
        )
        .context("failed to secure container admission directory")?;

        let path = format!("{CONTAINER_ADMISSION_ROOT}/{container_id}.lock");
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)
            .with_context(|| format!("failed to open admission lock for `{container_id}`"))?;
        let metadata = file
            .metadata()
            .context("failed to inspect container admission lock")?;
        if !metadata.is_file() || metadata.uid() != 0 || metadata.nlink() != 1 {
            bail!("container admission lock is not a root-owned regular file");
        }
        // SAFETY: flock operates on the live descriptor owned by this guard.
        if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
            return Err(std::io::Error::last_os_error())
                .context("failed to acquire container admission lock");
        }
        Ok(Self { file })
    }
}

#[cfg(target_os = "linux")]
impl Drop for ContainerAdmissionGuard {
    fn drop(&mut self) {
        use std::os::fd::AsRawFd as _;
        // SAFETY: unlocking a live flock descriptor is safe; close-on-drop is
        // the final fallback if the explicit unlock is interrupted.
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

#[cfg(target_os = "linux")]
fn ready_identity(target: &TargetIdentity, pinned: &PinnedTarget) -> ContainerReadyIdentity {
    ContainerReadyIdentity {
        container_id: target.container_id.clone(),
        pid: target.pid,
        start_time: target.start_time,
        cgroup_path: target.cgroup_path.clone(),
        cgroup: (pinned.cgroup_identity.device, pinned.cgroup_identity.inode),
        namespaces: [
            (
                target.namespaces.mount.device,
                target.namespaces.mount.inode,
            ),
            (
                target.namespaces.network.device,
                target.namespaces.network.inode,
            ),
            (target.namespaces.pid.device, target.namespaces.pid.inode),
            (target.namespaces.ipc.device, target.namespaces.ipc.inode),
            (target.namespaces.uts.device, target.namespaces.uts.inode),
        ],
        root: (target.root.device, target.root.inode),
    }
}

#[cfg(any(target_os = "linux", test))]
pub(crate) fn encode_ready_identity(
    challenge: &str,
    sender_start_time: u64,
    identity: &ContainerReadyIdentity,
) -> Vec<u8> {
    debug_assert!(validate_ready_challenge(challenge).is_ok());
    let mut fields = vec![
        "READY".to_string(),
        challenge.to_string(),
        sender_start_time.to_string(),
        identity.container_id.clone(),
        identity.pid.to_string(),
        identity.start_time.to_string(),
        identity.cgroup_path.clone(),
        identity.cgroup.0.to_string(),
        identity.cgroup.1.to_string(),
    ];
    for (device, inode) in identity.namespaces {
        fields.push(device.to_string());
        fields.push(inode.to_string());
    }
    fields.push(identity.root.0.to_string());
    fields.push(identity.root.1.to_string());
    fields.join("\t").into_bytes()
}

#[cfg(any(target_os = "linux", test))]
pub(crate) fn decode_ready_identity(
    bytes: &[u8],
    expected_challenge: &str,
    expected_sender_start_time: u64,
) -> anyhow::Result<ContainerReadyIdentity> {
    if bytes.len() > 4096 {
        bail!("container ready record exceeds 4096 bytes");
    }
    validate_ready_challenge(expected_challenge)?;
    let record = std::str::from_utf8(bytes).context("container ready record is not UTF-8")?;
    let fields = record.split('\t').collect::<Vec<_>>();
    if fields.len() < 3
        || fields[1] != expected_challenge
        || fields[2].parse::<u64>().ok() != Some(expected_sender_start_time)
    {
        bail!("container ready record challenge or sender generation does not match");
    }
    if fields[0] == "ERROR" {
        if fields.len() != 4 {
            bail!("container failure record is malformed");
        }
        bail!(
            "container trampoline failed before readiness: {}",
            fields[3]
        );
    }
    if fields.len() != 21 || fields[0] != "READY" {
        bail!("container ready record is malformed");
    }
    validate_container_id(fields[3])?;
    let number = |index: usize| -> anyhow::Result<u64> {
        fields[index]
            .parse::<u64>()
            .with_context(|| format!("container ready field {index} is invalid"))
    };
    let mut namespaces = [(0, 0); 5];
    for (index, identity) in namespaces.iter_mut().enumerate() {
        *identity = (number(9 + index * 2)?, number(10 + index * 2)?);
    }
    Ok(ContainerReadyIdentity {
        container_id: fields[3].to_string(),
        pid: u32::try_from(number(4)?).context("container ready PID exceeds u32")?,
        start_time: number(5)?,
        cgroup_path: validate_cgroup_path(fields[6])?,
        cgroup: (number(7)?, number(8)?),
        namespaces,
        root: (number(19)?, number(20)?),
    })
}

#[cfg(target_os = "linux")]
fn send_ready_record(socket: &str, record: &[u8]) -> anyhow::Result<()> {
    use std::io::Write as _;

    let mut stream = std::os::unix::net::UnixStream::connect(socket)
        .context("failed to connect container ready socket")?;
    stream
        .write_all(record)
        .context("failed to write container ready record")
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

#[cfg(target_os = "linux")]
fn open_container_file(
    root: &std::os::fd::OwnedFd,
    path: &str,
) -> anyhow::Result<std::os::fd::OwnedFd> {
    use std::os::fd::{AsRawFd as _, FromRawFd as _};

    #[repr(C)]
    struct OpenHow {
        flags: u64,
        mode: u64,
        resolve: u64,
    }

    const RESOLVE_NO_MAGICLINKS: u64 = 0x02;
    const RESOLVE_IN_ROOT: u64 = 0x10;

    let path = CString::new(path).context("container identity path contains NUL")?;
    let how = OpenHow {
        flags: (libc::O_RDONLY | libc::O_CLOEXEC) as u64,
        mode: 0,
        resolve: RESOLVE_IN_ROOT | RESOLVE_NO_MAGICLINKS,
    };
    // SAFETY: `root`, `path`, and `how` are valid for the duration of the
    // syscall. Resolution is anchored beneath the already-pinned root.
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
                "failed to open `{}` beneath container root",
                path.to_string_lossy()
            )
        });
    }
    // SAFETY: openat2 returned a fresh owned descriptor.
    Ok(unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) })
}

#[cfg(target_os = "linux")]
fn read_optional_container_identity_file(
    root: &std::os::fd::OwnedFd,
    path: &str,
) -> anyhow::Result<String> {
    use std::io::Read as _;

    let descriptor = match open_container_file(root, path) {
        Ok(descriptor) => descriptor,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .and_then(std::io::Error::raw_os_error)
                == Some(libc::ENOENT) =>
        {
            return Ok(String::new());
        }
        Err(error) => return Err(error),
    };
    let file = std::fs::File::from(descriptor);
    let metadata = file
        .metadata()
        .with_context(|| format!("failed to inspect `{path}` beneath container root"))?;
    if !metadata.is_file() {
        bail!("container identity database `{path}` is not a regular file");
    }
    let mut bytes = Vec::new();
    file.take((MAX_IDENTITY_FILE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read `{path}` beneath container root"))?;
    if bytes.len() > MAX_IDENTITY_FILE_BYTES {
        bail!("container identity database `{path}` exceeds {MAX_IDENTITY_FILE_BYTES} bytes");
    }
    String::from_utf8(bytes)
        .with_context(|| format!("container identity database `{path}` is not valid UTF-8"))
}

#[cfg(target_os = "linux")]
fn resolve_container_identity(
    root: &std::os::fd::OwnedFd,
    user: Option<&str>,
) -> anyhow::Result<ResolvedIdentity> {
    let passwd = read_optional_container_identity_file(root, "/etc/passwd")?;
    let group = read_optional_container_identity_file(root, "/etc/group")?;
    resolve_identity(user, &passwd, &group)
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy)]
struct PasswdRecord<'a> {
    name: &'a str,
    uid: libc::uid_t,
    gid: libc::gid_t,
}

#[cfg(any(target_os = "linux", test))]
#[derive(Debug, Clone, Copy)]
struct GroupRecord<'a> {
    name: &'a str,
    gid: libc::gid_t,
    members: &'a str,
}

#[cfg(any(target_os = "linux", test))]
fn validate_identity_database(content: &str, description: &str) -> anyhow::Result<()> {
    if content.len() > MAX_IDENTITY_FILE_BYTES {
        bail!("{description} exceeds {MAX_IDENTITY_FILE_BYTES} bytes");
    }
    if content
        .split_terminator('\n')
        .any(|record| record.len() > MAX_IDENTITY_RECORD_BYTES)
    {
        bail!("{description} contains a record exceeding {MAX_IDENTITY_RECORD_BYTES} bytes");
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn parse_numeric_identity(value: &str, description: &str) -> anyhow::Result<libc::uid_t> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("{description} is not an unsigned decimal identifier");
    }
    let identifier = value
        .parse::<u32>()
        .with_context(|| format!("{description} exceeds u32"))?;
    if identifier > i32::MAX as u32 {
        bail!(
            "{description} exceeds the Docker-compatible maximum {0}",
            i32::MAX
        );
    }
    Ok(identifier)
}

#[cfg(any(target_os = "linux", test))]
fn validate_identity_name(name: &str, description: &str) -> anyhow::Result<()> {
    if name.is_empty()
        || name.len() > MAX_IDENTITY_NAME_BYTES
        || name.contains(':')
        || name.contains(',')
        || name
            .chars()
            .any(|character| character.is_control() || character.is_whitespace())
    {
        bail!("{description} is empty, invalid, or too long");
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn passwd_records(content: &str) -> impl Iterator<Item = anyhow::Result<PasswdRecord<'_>>> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields = line.split(':').collect::<Vec<_>>();
            if fields.len() != 7 {
                bail!("container passwd contains a malformed record");
            }
            validate_identity_name(fields[0], "container passwd user name")?;
            Ok(PasswdRecord {
                name: fields[0],
                uid: parse_numeric_identity(fields[2], "container passwd uid")?,
                gid: parse_numeric_identity(fields[3], "container passwd gid")?,
            })
        })
}

#[cfg(any(target_os = "linux", test))]
fn group_records(content: &str) -> impl Iterator<Item = anyhow::Result<GroupRecord<'_>>> {
    content
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields = line.split(':').collect::<Vec<_>>();
            if fields.len() != 4 {
                bail!("container group database contains a malformed record");
            }
            validate_identity_name(fields[0], "container group name")?;
            for member in fields[3].split(',').filter(|member| !member.is_empty()) {
                validate_identity_name(member, "container group member name")?;
            }
            Ok(GroupRecord {
                name: fields[0],
                gid: parse_numeric_identity(fields[2], "container group gid")?,
                members: fields[3],
            })
        })
}

#[cfg(any(target_os = "linux", test))]
fn resolve_identity(
    user_spec: Option<&str>,
    passwd: &str,
    group: &str,
) -> anyhow::Result<ResolvedIdentity> {
    validate_identity_database(passwd, "container passwd")?;
    validate_identity_database(group, "container group database")?;
    let passwd_records = passwd_records(passwd).collect::<anyhow::Result<Vec<_>>>()?;
    let group_records = group_records(group).collect::<anyhow::Result<Vec<_>>>()?;

    let Some(user_spec) = user_spec.filter(|value| !value.is_empty()) else {
        return Ok(ResolvedIdentity {
            uid: 0,
            gid: 0,
            supplementary_groups: vec![0],
        });
    };
    if user_spec.matches(':').count() > 1 {
        bail!("container exec user must be USER, UID, USER:GROUP, or UID:GID");
    }
    let (user_part, group_part) = user_spec
        .split_once(':')
        .map_or((user_spec, None), |(user, group)| (user, Some(group)));
    if user_part.is_empty() || group_part == Some("") {
        bail!("container exec user and group components cannot be empty");
    }

    let numeric_uid = user_part.bytes().all(|byte| byte.is_ascii_digit());
    let matching_user = if numeric_uid {
        let uid = parse_numeric_identity(user_part, "container exec uid")?;
        passwd_records
            .iter()
            .copied()
            .find(|record| record.uid == uid)
    } else {
        validate_identity_name(user_part, "container exec user name")?;
        passwd_records
            .iter()
            .copied()
            .find(|record| record.name == user_part)
    };
    if !numeric_uid && matching_user.is_none() {
        bail!("container exec user `{user_part}` does not exist");
    }
    let uid = if numeric_uid {
        parse_numeric_identity(user_part, "container exec uid")?
    } else if let Some(record) = matching_user {
        record.uid
    } else {
        bail!("container exec user `{user_part}` does not exist");
    };
    let mut gid = matching_user.map_or(0, |record| record.gid);

    if let Some(group_part) = group_part {
        let numeric_gid = group_part.bytes().all(|byte| byte.is_ascii_digit());
        gid = if numeric_gid {
            parse_numeric_identity(group_part, "container exec gid")?
        } else {
            validate_identity_name(group_part, "container exec group name")?;
            group_records
                .iter()
                .find(|record| record.name == group_part)
                .map(|record| record.gid)
                .ok_or_else(|| {
                    anyhow::anyhow!("container exec group `{group_part}` does not exist")
                })?
        };
    }

    // Docker-compatible named-user execution initializes memberships from
    // /etc/group only when no explicit group overrides the user's group set.
    // Moby carries the effective primary gid in AdditionalGids, followed by
    // named-user memberships. Keep the set sorted and deduplicated so every
    // adapter produces the same observable `getgroups(2)` result.
    let mut supplementary_groups = vec![gid];
    if group_part.is_none() {
        if let Some(user_name) = matching_user.map(|record| record.name) {
            for record in &group_records {
                if record.members.split(',').any(|member| member == user_name)
                    && !supplementary_groups.contains(&record.gid)
                {
                    if supplementary_groups.len() >= MAX_SUPPLEMENTARY_GROUPS {
                        bail!(
                            "container user belongs to more than {MAX_SUPPLEMENTARY_GROUPS} supplementary groups"
                        );
                    }
                    supplementary_groups.push(record.gid);
                }
            }
        }
    }
    supplementary_groups.sort_unstable();

    Ok(ResolvedIdentity {
        uid,
        gid,
        supplementary_groups,
    })
}

#[cfg(any(target_os = "linux", test))]
fn prepare_exec_payload(
    invocation: &TrampolineInvocation,
    identity: ResolvedIdentity,
) -> anyhow::Result<ExecPayload> {
    prepare_command_payload(
        &invocation.command,
        &invocation.args,
        invocation.retain_shell_environment,
        identity,
    )
}

#[cfg(any(target_os = "linux", test))]
fn prepare_command_payload(
    command: &str,
    args: &[String],
    retain_shell_environment: bool,
    identity: ResolvedIdentity,
) -> anyhow::Result<ExecPayload> {
    let mut argv = Vec::with_capacity(args.len() + 1);
    argv.push(CString::new(command.as_bytes()).context("container exec argv[0] contains NUL")?);
    for argument in args {
        argv.push(
            CString::new(argument.as_bytes()).context("container exec argument contains NUL")?,
        );
    }
    let argv_ptrs = argv
        .iter()
        .map(|argument| argument.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect();

    let mut raw_environment = std::env::vars_os()
        .map(|(key, value)| {
            (
                key.as_os_str().as_bytes().to_vec(),
                value.as_os_str().as_bytes().to_vec(),
            )
        })
        // portable-pty injects SHELL while constructing its child Command.
        // Strip that adapter-only value unless it was part of the normalized
        // request environment, keeping unary, pipe, and PTY exec identical.
        .filter(|(key, _)| retain_shell_environment || key != b"SHELL")
        .collect::<Vec<_>>();
    raw_environment.sort_by(|left, right| left.0.cmp(&right.0));
    let mut environment = Vec::with_capacity(raw_environment.len());
    let mut search_path = None;
    for (key, value) in raw_environment {
        if key == b"PATH" {
            search_path = Some(value.clone());
        }
        let mut entry = Vec::with_capacity(key.len() + value.len() + 1);
        entry.extend_from_slice(&key);
        entry.push(b'=');
        entry.extend_from_slice(&value);
        environment.push(CString::new(entry).context("container exec environment contains NUL")?);
    }
    let environment_ptrs = environment
        .iter()
        .map(|entry| entry.as_ptr())
        .chain(std::iter::once(std::ptr::null()))
        .collect();
    let command_candidates = prepare_command_candidates(
        command.as_bytes(),
        search_path.as_deref().unwrap_or(b"/bin:/usr/bin"),
    )?;

    let identity_verification_groups = vec![0; identity.supplementary_groups.len()];
    Ok(ExecPayload {
        command_candidates,
        argv,
        argv_ptrs,
        environment,
        environment_ptrs,
        identity,
        identity_verification_groups,
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
fn child_exec(fds: ChildLaunchFds, expected_cgroup: &[u8], payload: &mut ExecPayload) -> ! {
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

    // The requested command and all ordinary descendants begin in a process
    // group owned by the outer trampoline. The parent also performs this bind
    // before unblocking forwarded signals, making either scheduling order safe.
    if unsafe { libc::setpgid(0, 0) } != 0 {
        let errno = current_errno();
        if errno != libc::EACCES {
            child_setup_fail(fds.setup_error, ChildSetupStage::SignalState, errno);
        }
    }

    // Drop the inherited writer first so outer-supervisor death becomes EOF,
    // then wait for the one-byte release sent only after the death sentinel
    // and process-group identity are fully installed.
    unsafe { libc::close(fds.launch_gate_writer) };
    let mut release = 0_u8;
    loop {
        let read = unsafe {
            libc::read(
                fds.launch_gate_reader,
                (&raw mut release).cast::<libc::c_void>(),
                1,
            )
        };
        if read == 1 && release == b'R' {
            break;
        }
        if read < 0 && current_errno() == libc::EINTR {
            continue;
        }
        child_setup_fail(
            fds.setup_error,
            ChildSetupStage::SupervisorReady,
            if read == 0 {
                libc::EPIPE
            } else {
                current_errno()
            },
        );
    }
    unsafe { libc::close(fds.launch_gate_reader) };

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

    child_apply_identity(fds.setup_error, payload);

    finish_child_exec(fds.setup_error, Some(fds.target_pidfd), payload)
}

/// Common post-identity exec path. Ordinary Machine execution has no target
/// init pidfd to recheck because it never enters an OCI target.
#[cfg(target_os = "linux")]
fn finish_child_exec(
    setup_error: libc::c_int,
    target_pidfd: Option<libc::c_int>,
    payload: &ExecPayload,
) -> ! {
    // Do not leak the agent's signal policy into the requested program.
    // SAFETY: the signal set is initialized before use and affects only this
    // post-fork child.
    unsafe {
        for signal in supervised_signals()
            .into_iter()
            .chain(std::iter::once(libc::SIGRTMIN()))
        {
            if libc::signal(signal, libc::SIG_DFL) == libc::SIG_ERR {
                child_setup_fail(setup_error, ChildSetupStage::SignalState, current_errno());
            }
        }
        if libc::signal(libc::SIGPIPE, libc::SIG_DFL) == libc::SIG_ERR {
            child_setup_fail(setup_error, ChildSetupStage::SignalState, current_errno());
        }
        let mut empty: libc::sigset_t = std::mem::zeroed();
        if libc::sigemptyset(&raw mut empty) != 0
            || libc::sigprocmask(libc::SIG_SETMASK, &raw const empty, std::ptr::null_mut()) != 0
        {
            child_setup_fail(setup_error, ChildSetupStage::SignalState, current_errno());
        }
    }

    // Recheck the pinned container init immediately before descriptor closure
    // and exec. A dead init invalidates the namespace/root target even though
    // the pinned descriptors themselves remain usable.
    if target_pidfd.is_some_and(|fd| !pidfd_is_alive_raw(fd)) {
        child_setup_fail(setup_error, ChildSetupStage::TargetRace, libc::ESRCH);
    }

    // Preserve only the CLOEXEC setup-error pipe until execve. A successful
    // exec closes it and tells the supervisor setup completed; all preparation
    // descriptors are closed immediately.
    if !close_fds_except(setup_error) {
        child_setup_fail(
            setup_error,
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
            child_execve_fail(setup_error, errno);
        }
    }
    child_execve_fail(
        setup_error,
        if saw_access_denied {
            libc::EACCES
        } else {
            libc::ENOENT
        },
    );
}

/// Start a descriptor-free sibling sentinel in the exec process group. If the
/// outer trampoline itself is SIGKILLed, the sentinel's PDEATHSIG handler kills
/// the complete command group. Keeping it as the command's sibling means it is
/// invisible to the requested program's waitpid(2) and SIGCHLD behavior.
#[cfg(target_os = "linux")]
fn launch_death_sentinel(
    process_group: libc::pid_t,
    setup_error_fd: libc::c_int,
    launcher_pidfd: libc::c_int,
) -> anyhow::Result<()> {
    let (gate_reader, gate_writer) = setup_error_pipe()?;
    let (ready_reader, ready_writer) = setup_error_pipe()?;
    use std::os::fd::AsRawFd as _;

    // SAFETY: the trampoline is single-threaded and has prepared all state.
    let sentinel = unsafe { libc::fork() };
    if sentinel < 0 {
        return Err(std::io::Error::last_os_error()).context("cannot fork exec death sentinel");
    }
    if sentinel != 0 {
        drop(gate_reader);
        drop(ready_writer);
        // Close the same process-group registration race from the parent.
        // SAFETY: sentinel is the exact child returned above.
        if unsafe { libc::setpgid(sentinel, process_group) } != 0 {
            let error = std::io::Error::last_os_error();
            // SAFETY: this is a read-only check for the exact child.
            if unsafe { libc::getpgid(sentinel) } != process_group {
                // SAFETY: sentinel is our exact direct child.
                unsafe { libc::kill(sentinel, libc::SIGKILL) };
                let _ = wait_for_child(sentinel);
                return Err(error).context("cannot bind exec death sentinel process group");
            }
        }
        write_all_raw(gate_writer.as_raw_fd(), b"R")
            .map_err(std::io::Error::from_raw_os_error)
            .context("cannot release exec death sentinel gate")?;
        drop(gate_writer);
        if !read_one_byte_gate(ready_reader.as_raw_fd(), b'R') {
            // SAFETY: sentinel is the exact child created above.
            unsafe { libc::kill(sentinel, libc::SIGKILL) };
            let _ = wait_for_child(sentinel);
            bail!("exec death sentinel failed before readiness acknowledgement");
        }
        return Ok(());
    }

    unsafe { libc::close(ready_reader.as_raw_fd()) };
    // Only the parent names namespace-relative process-group IDs. The sentinel
    // waits until that parent has confirmed setpgid using its own PID view.
    unsafe { libc::close(gate_writer.as_raw_fd()) };
    if !read_one_byte_gate(gate_reader.as_raw_fd(), b'R') {
        child_setup_fail(setup_error_fd, ChildSetupStage::DeathSentinel, libc::EPIPE);
    }
    unsafe { libc::close(gate_reader.as_raw_fd()) };

    // The sentinel ignores public forwarded signals; the command receives
    // them directly because both remain in the exact same process group.
    for signal in supervised_signals() {
        // SAFETY: signal disposition changes affect only the sentinel.
        if unsafe { libc::signal(signal, libc::SIG_IGN) } == libc::SIG_ERR {
            child_setup_fail(
                setup_error_fd,
                ChildSetupStage::DeathSentinel,
                current_errno(),
            );
        }
    }

    // SAFETY: the action is initialized and the handler only calls kill(2).
    let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
    action.sa_sigaction = sentinel_parent_died as *const () as usize;
    action.sa_flags = libc::SA_RESTART;
    if unsafe { libc::sigemptyset(&raw mut action.sa_mask) } != 0
        || unsafe {
            libc::sigaction(
                libc::SIGRTMIN(),
                &raw const action,
                std::ptr::null_mut(),
            )
        } != 0
        // SAFETY: prctl receives scalar arguments only.
        || unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGRTMIN()) } != 0
    {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::DeathSentinel,
            current_errno(),
        );
    }

    // The outer trampoline blocks controls across both forks to make process
    // group registration race-free. The sentinel must explicitly unblock its
    // private PDEATHSIG after installing the handler.
    let mut empty = unsafe { std::mem::zeroed::<libc::sigset_t>() };
    if unsafe { libc::sigemptyset(&raw mut empty) } != 0
        || unsafe { libc::sigprocmask(libc::SIG_SETMASK, &raw const empty, std::ptr::null_mut()) }
            != 0
    {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::DeathSentinel,
            current_errno(),
        );
    }

    // Close the PR_SET_PDEATHSIG race using the namespace-invariant pidfd; the
    // sentinel never compares a PID originating in the outer namespace.
    if !pidfd_is_alive_raw(launcher_pidfd) {
        sentinel_parent_died(libc::SIGRTMIN());
    }

    // Acknowledge only after the parent-bound group, ignored public controls,
    // PDEATHSIG handler, unblocked mask, and exact parent pidfd are all valid.
    if write_all_raw(ready_writer.as_raw_fd(), b"R").is_err() {
        sentinel_parent_died(libc::SIGRTMIN());
    }
    unsafe { libc::close(ready_writer.as_raw_fd()) };

    // Close the standard streams as well as the setup writer and pidfd,
    // otherwise this sentinel would keep pipe/PTY transports and the ready
    // handshake open. Parent death after the check is covered by PDEATHSIG.
    // SAFETY: close_range has no pointer arguments.
    if unsafe { libc::syscall(libc::SYS_close_range, 0_u32, u32::MAX, 0_u32) } != 0 {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::DeathSentinel,
            current_errno(),
        );
    }
    loop {
        // SAFETY: pause blocks this descriptor-free sentinel until parent death.
        unsafe { libc::pause() };
    }
}

#[cfg(target_os = "linux")]
fn read_one_byte_gate(fd: libc::c_int, expected: u8) -> bool {
    let mut value = 0_u8;
    loop {
        // SAFETY: value is writable and fd names the one-byte synchronization pipe.
        let read = unsafe { libc::read(fd, (&raw mut value).cast::<libc::c_void>(), 1) };
        if read == 1 {
            return gate_byte_matches(read, value, expected);
        }
        if read < 0 && current_errno() == libc::EINTR {
            continue;
        }
        return false;
    }
}

#[cfg(any(target_os = "linux", test))]
fn gate_byte_matches(read: isize, observed: u8, expected: u8) -> bool {
    read == 1 && observed == expected
}

#[cfg(any(target_os = "linux", test))]
fn absent_controlling_tty_errno(errno: libc::c_int) -> bool {
    matches!(errno, libc::ENOTTY | libc::EBADF)
}

#[cfg(target_os = "linux")]
fn assign_foreground_process_group(process_group: libc::pid_t) -> anyhow::Result<Option<i32>> {
    for fd in [libc::STDIN_FILENO, libc::STDOUT_FILENO, libc::STDERR_FILENO] {
        // tcgetpgrp succeeds only for the controlling terminal of this session.
        // SAFETY: fd is merely queried and no pointer arguments are involved.
        let foreground = unsafe { libc::tcgetpgrp(fd) };
        if foreground >= 0 {
            // Signals are still blocked in the trampoline, so tcsetpgrp cannot
            // suspend it with SIGTTOU between group bind and command release.
            // SAFETY: process_group was established for the exact child above.
            if unsafe { libc::tcsetpgrp(fd, process_group) } != 0 {
                return Err(std::io::Error::last_os_error())
                    .context("cannot assign container exec foreground process group");
            }
            return Ok(Some(fd));
        }
        let error = std::io::Error::last_os_error();
        if !error
            .raw_os_error()
            .is_some_and(absent_controlling_tty_errno)
        {
            return Err(error).context("cannot inspect container exec controlling terminal");
        }
    }
    Ok(None)
}

#[cfg(target_os = "linux")]
extern "C" fn sentinel_parent_died(_signal: libc::c_int) {
    // SAFETY: zero targets this sentinel's exact exec-owned process group and
    // kill(2) is async-signal-safe. SIGKILL includes the sentinel itself.
    unsafe {
        libc::kill(0, libc::SIGKILL);
        libc::_exit(128 + libc::SIGKILL);
    }
}

#[cfg(target_os = "linux")]
fn supervised_signals() -> [libc::c_int; 28] {
    [
        libc::SIGHUP,
        libc::SIGINT,
        libc::SIGQUIT,
        libc::SIGILL,
        libc::SIGTRAP,
        libc::SIGABRT,
        libc::SIGBUS,
        libc::SIGFPE,
        libc::SIGSEGV,
        libc::SIGTERM,
        libc::SIGUSR1,
        libc::SIGUSR2,
        libc::SIGPIPE,
        libc::SIGALRM,
        libc::SIGCONT,
        libc::SIGTSTP,
        libc::SIGTTIN,
        libc::SIGTTOU,
        libc::SIGWINCH,
        libc::SIGURG,
        libc::SIGXCPU,
        libc::SIGXFSZ,
        libc::SIGVTALRM,
        libc::SIGPROF,
        libc::SIGIO,
        libc::SIGPWR,
        libc::SIGSYS,
        libc::SIGCHLD,
    ]
}

#[cfg(target_os = "linux")]
pub(crate) fn supports_forwarded_signal(signal: libc::c_int) -> bool {
    signal == libc::SIGKILL || supervised_signals().contains(&signal)
}

#[cfg(target_os = "linux")]
extern "C" fn forward_supervisor_signal(signal: libc::c_int) {
    let group = SUPERVISED_PROCESS_GROUP.load(Ordering::Acquire);
    if group <= 0 {
        return;
    }
    let forwarded = forwarded_signal(signal, FORCE_CANCEL_SIGNAL.load(Ordering::Relaxed));
    // SAFETY: negative `group` targets only the process group established by
    // this trampoline; kill(2) is async-signal-safe.
    unsafe {
        libc::kill(-group, forwarded);
    }
}

#[cfg(any(target_os = "linux", test))]
fn forwarded_signal(signal: libc::c_int, force_cancel_signal: libc::c_int) -> libc::c_int {
    if signal == force_cancel_signal {
        libc::SIGKILL
    } else {
        signal
    }
}

#[cfg(target_os = "linux")]
fn supervisor_signal_set() -> anyhow::Result<libc::sigset_t> {
    // SAFETY: the signal set is initialized before use.
    let mut set = unsafe { std::mem::zeroed::<libc::sigset_t>() };
    if unsafe { libc::sigemptyset(&raw mut set) } != 0 {
        return Err(std::io::Error::last_os_error()).context("cannot initialize signal set");
    }
    for signal in supervised_signals()
        .into_iter()
        .chain(std::iter::once(libc::SIGRTMIN()))
    {
        if unsafe { libc::sigaddset(&raw mut set, signal) } != 0 {
            return Err(std::io::Error::last_os_error()).context("cannot populate signal set");
        }
    }
    Ok(set)
}

#[cfg(target_os = "linux")]
fn install_supervisor_signal_handlers() -> anyhow::Result<()> {
    let set = supervisor_signal_set()?;
    // SAFETY: trampoline dispatch occurs before Tokio and is single-threaded.
    if unsafe { libc::sigprocmask(libc::SIG_BLOCK, &raw const set, std::ptr::null_mut()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("cannot block supervisor signals");
    }
    FORCE_CANCEL_SIGNAL.store(libc::SIGRTMIN(), Ordering::Relaxed);
    for signal in supervised_signals()
        .into_iter()
        .chain(std::iter::once(libc::SIGRTMIN()))
    {
        // SAFETY: sigaction is fully initialized; handler uses only atomics and
        // async-signal-safe kill(2).
        let mut action = unsafe { std::mem::zeroed::<libc::sigaction>() };
        action.sa_sigaction = forward_supervisor_signal as *const () as usize;
        action.sa_flags = libc::SA_RESTART;
        if unsafe { libc::sigemptyset(&raw mut action.sa_mask) } != 0
            || unsafe { libc::sigaction(signal, &raw const action, std::ptr::null_mut()) } != 0
        {
            return Err(std::io::Error::last_os_error())
                .with_context(|| format!("cannot install supervisor signal handler for {signal}"));
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn unblock_supervisor_signals() -> anyhow::Result<()> {
    let set = supervisor_signal_set()?;
    // SAFETY: set is initialized and this remains the single trampoline thread.
    if unsafe { libc::sigprocmask(libc::SIG_UNBLOCK, &raw const set, std::ptr::null_mut()) } != 0 {
        return Err(std::io::Error::last_os_error()).context("cannot unblock supervisor signals");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn bind_supervised_process_group(child: libc::pid_t) -> anyhow::Result<()> {
    // SAFETY: child is our exact direct child. The same call is made in the
    // child; success by either side establishes the identical group.
    if unsafe { libc::setpgid(child, child) } == 0 {
        return Ok(());
    }
    let error = std::io::Error::last_os_error();
    // SAFETY: getpgid is a read-only identity check for the direct child.
    if unsafe { libc::getpgid(child) } == child {
        return Ok(());
    }
    Err(error).context("cannot bind container exec process group")
}

#[cfg(target_os = "linux")]
fn enable_child_subreaper() -> anyhow::Result<()> {
    // SAFETY: prctl receives scalar arguments and affects only this dedicated
    // single-use trampoline process.
    if unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) } != 0 {
        return Err(std::io::Error::last_os_error())
            .context("cannot make container exec trampoline a child subreaper");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn terminate_and_reap_descendants(group: libc::pid_t) -> anyhow::Result<()> {
    // The requested leader has already been reaped. Kill every ordinary
    // descendant which retained the exec-owned process group, then drain all
    // descendants adopted because this trampoline is a subreaper.
    // SAFETY: negative group targets the exact group established after fork.
    let result = unsafe { libc::kill(-group, libc::SIGKILL) };
    if result != 0 {
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() != Some(libc::ESRCH) {
            return Err(error).context("cannot terminate container exec descendants");
        }
    }

    SUPERVISED_PROCESS_GROUP.store(0, Ordering::Release);
    reap_adopted_descendants()
}

#[cfg(target_os = "linux")]
fn reap_adopted_descendants() -> anyhow::Result<()> {
    let deadline = std::time::Instant::now() + DESCENDANT_REAP_TIMEOUT;
    loop {
        kill_adopted_descendants()?;
        let mut status = 0;
        // SAFETY: -1 selects any adopted child and status is writable.
        let reaped = unsafe { libc::waitpid(-1, &raw mut status, libc::WNOHANG) };
        if reaped > 0 {
            continue;
        }
        if reaped < 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() == Some(libc::ECHILD) {
                return Ok(());
            }
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error).context("cannot reap container exec descendants");
        }
        if std::time::Instant::now() >= deadline {
            bail!("timed out reaping container exec descendants");
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

#[cfg(target_os = "linux")]
fn kill_adopted_descendants() -> anyhow::Result<()> {
    use std::os::fd::AsRawFd as _;

    // `setns(CLONE_NEWPID)` changes the namespace used for subsequent
    // children without changing this trampoline's cached/user-space PID.
    // Resolve both the child list and supervisor identity through the current
    // procfs mount so all numeric IDs below belong to the same PID view.
    let supervisor = read_proc_view_supervisor_pid()?;
    for pid in read_proc_view_child_pids(supervisor)? {
        let pid_fd = match open_pidfd(pid) {
            Ok(pid_fd) => pid_fd,
            Err(error) if error_has_errno(&error, libc::ESRCH) => continue,
            Err(error) => return Err(error).context("cannot pin adopted exec descendant"),
        };
        let parent = match read_proc_parent_pid(pid) {
            Ok(parent) => parent,
            Err(error)
                if error_has_errno(&error, libc::ENOENT)
                    || error_has_errno(&error, libc::ESRCH) =>
            {
                continue;
            }
            Err(error) => return Err(error),
        };
        if parent != supervisor {
            bail!("adopted exec descendant changed parent before cancellation");
        }
        // SAFETY: pid_fd pins the child identity and ordinary signal delivery
        // uses null siginfo.
        let result = unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                pid_fd.as_raw_fd(),
                libc::SIGKILL,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            )
        };
        if result != 0 {
            let error = std::io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error).context("cannot kill adopted exec descendant");
            }
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn error_has_errno(error: &anyhow::Error, errno: libc::c_int) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.raw_os_error() == Some(errno))
    })
}

#[cfg(target_os = "linux")]
fn read_proc_view_supervisor_pid() -> anyhow::Result<u32> {
    // `/proc/thread-self` is optional on older or minimal kernels. Enumerating
    // `/proc/self/task` uses the procfs mount's PID view and is available on
    // every procfs implementation that exposes per-task children. The
    // trampoline is deliberately single-threaded, so ambiguity fails closed.
    let mut tasks = std::fs::read_dir("/proc/self/task")
        .context("cannot enumerate exec supervisor procfs tasks")?
        .map(|entry| entry.context("cannot enumerate exec supervisor task"))
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .filter_map(|entry| entry.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| *pid != 0)
        .collect::<Vec<_>>();
    tasks.sort_unstable();
    tasks.dedup();
    if tasks.len() != 1 {
        bail!(
            "exec supervisor procfs task identity is ambiguous ({} tasks)",
            tasks.len()
        );
    }
    Ok(tasks[0])
}

#[cfg(target_os = "linux")]
fn read_proc_view_child_pids(supervisor: u32) -> anyhow::Result<Vec<u32>> {
    let children_path = format!("/proc/self/task/{supervisor}/children");
    match std::fs::read_to_string(&children_path) {
        Ok(children) => parse_proc_children(&children),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            // Some kernels omit the per-task children file. A bounded procfs
            // status scan is slower but retains the same exact PPid identity.
            scan_proc_children_by_parent(supervisor)
        }
        Err(error) => Err(error)
            .with_context(|| format!("cannot inspect adopted exec descendants at {children_path}")),
    }
}

#[cfg(any(target_os = "linux", test))]
fn parse_proc_children(children: &str) -> anyhow::Result<Vec<u32>> {
    if children.len() > MAX_CGROUP_FILE_BYTES {
        bail!("adopted exec descendant list exceeds {MAX_CGROUP_FILE_BYTES} bytes");
    }
    let mut pids = Vec::new();
    for field in children.split_whitespace() {
        let pid = field
            .parse::<u32>()
            .context("adopted exec descendant PID is invalid")?;
        if pid == 0 {
            bail!("adopted exec descendant PID is zero");
        }
        pids.push(pid);
    }
    pids.sort_unstable();
    pids.dedup();
    Ok(pids)
}

#[cfg(target_os = "linux")]
fn scan_proc_children_by_parent(supervisor: u32) -> anyhow::Result<Vec<u32>> {
    let mut children = Vec::new();
    let mut inspected = 0_usize;
    for entry in std::fs::read_dir("/proc").context("cannot scan procfs for exec descendants")? {
        let entry = entry.context("cannot enumerate procfs entry for exec descendants")?;
        let Some(pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<u32>().ok())
            .filter(|pid| *pid != 0)
        else {
            continue;
        };
        inspected += 1;
        if inspected > MAX_CGROUP_FILE_BYTES {
            bail!("procfs descendant scan exceeded {MAX_CGROUP_FILE_BYTES} processes");
        }
        match read_proc_parent_pid(pid) {
            Ok(parent) if parent == supervisor => children.push(pid),
            Ok(_) => {}
            Err(error)
                if error_has_errno(&error, libc::ENOENT)
                    || error_has_errno(&error, libc::ESRCH) => {}
            Err(error) => return Err(error),
        }
    }
    children.sort_unstable();
    children.dedup();
    Ok(children)
}

#[cfg(test)]
fn parse_proc_status_id(status: &str, field: &str) -> anyhow::Result<u32> {
    let value = parse_proc_status_u32(status, field)?;
    if value == 0 {
        bail!("process status {field} is zero");
    }
    Ok(value)
}

#[cfg(any(target_os = "linux", test))]
fn parse_proc_status_u32(status: &str, field: &str) -> anyhow::Result<u32> {
    let prefix = format!("{field}:");
    let mut values = status.lines().filter_map(|line| line.strip_prefix(&prefix));
    let value = values
        .next()
        .ok_or_else(|| anyhow::anyhow!("process status omitted {field}"))?;
    if values.next().is_some() {
        bail!("process status repeated {field}");
    }
    let value = value
        .trim()
        .parse::<u32>()
        .with_context(|| format!("process status {field} is invalid"))?;
    Ok(value)
}

#[cfg(target_os = "linux")]
fn read_proc_parent_pid(pid: u32) -> anyhow::Result<u32> {
    let path = format!("/proc/{pid}/status");
    let status = std::fs::read_to_string(&path)
        .with_context(|| format!("cannot inspect adopted exec descendant at {path}"))?;
    if status.len() > MAX_CGROUP_FILE_BYTES {
        bail!("adopted exec descendant status exceeds {MAX_CGROUP_FILE_BYTES} bytes");
    }
    // PPid 0 is legitimate for procfs-visible namespace roots and kernel
    // processes. Callers compare the value with the nonzero supervisor PID,
    // so such unrelated entries are safely ignored while reparenting still
    // fails the later exact-parent check.
    parse_proc_status_u32(&status, "PPid")
        .context("adopted exec descendant status has invalid parent identity")
}

#[cfg(target_os = "linux")]
fn child_apply_identity(setup_error_fd: libc::c_int, payload: &mut ExecPayload) {
    let identity = &payload.identity;
    // SAFETY: the supplementary group slice was fully allocated before fork.
    if unsafe {
        libc::setgroups(
            identity.supplementary_groups.len(),
            identity.supplementary_groups.as_ptr(),
        )
    } != 0
    {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::SupplementaryGroups,
            current_errno(),
        );
    }
    // Drop group privileges before user privileges; all three real/effective/
    // saved IDs are set so the requested process cannot regain guest root.
    if unsafe { libc::setresgid(identity.gid, identity.gid, identity.gid) } != 0 {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::GroupIdentity,
            current_errno(),
        );
    }
    if unsafe { libc::setresuid(identity.uid, identity.uid, identity.uid) } != 0 {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::UserIdentity,
            current_errno(),
        );
    }

    let mut real_uid = 0;
    let mut effective_uid = 0;
    let mut saved_uid = 0;
    let mut real_gid = 0;
    let mut effective_gid = 0;
    let mut saved_gid = 0;
    // SAFETY: each pointer names initialized writable storage in this child.
    if unsafe {
        libc::getresuid(
            &raw mut real_uid,
            &raw mut effective_uid,
            &raw mut saved_uid,
        )
    } != 0
    {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::IdentityVerify,
            current_errno(),
        );
    }
    if unsafe {
        libc::getresgid(
            &raw mut real_gid,
            &raw mut effective_gid,
            &raw mut saved_gid,
        )
    } != 0
    {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::IdentityVerify,
            current_errno(),
        );
    }
    if real_uid != identity.uid
        || effective_uid != identity.uid
        || saved_uid != identity.uid
        || real_gid != identity.gid
        || effective_gid != identity.gid
        || saved_gid != identity.gid
    {
        child_setup_fail(setup_error_fd, ChildSetupStage::IdentityVerify, libc::EPERM);
    }

    let group_count = if payload.identity_verification_groups.is_empty() {
        // A null pointer is required when the expected group count is zero.
        unsafe { libc::getgroups(0, std::ptr::null_mut()) }
    } else {
        unsafe {
            libc::getgroups(
                payload.identity_verification_groups.len() as libc::c_int,
                payload.identity_verification_groups.as_mut_ptr(),
            )
        }
    };
    if group_count < 0 {
        child_setup_fail(
            setup_error_fd,
            ChildSetupStage::IdentityVerify,
            current_errno(),
        );
    }
    payload.identity_verification_groups.sort_unstable();
    if group_count as usize != identity.supplementary_groups.len()
        || payload.identity_verification_groups != identity.supplementary_groups
    {
        child_setup_fail(setup_error_fd, ChildSetupStage::IdentityVerify, libc::EPERM);
    }
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
fn child_execve_fail(setup_error_fd: libc::c_int, errno: libc::c_int) -> ! {
    let errno = if errno == 0 { libc::EIO } else { errno };
    let record = encode_setup_error(ChildSetupError {
        stage: ChildSetupStage::Execve,
        errno,
    });
    let _ = write_all_raw(setup_error_fd, &record);
    child_exit(exec_failure_exit_code(errno))
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
fn try_wait_for_child(child: libc::pid_t) -> anyhow::Result<Option<libc::c_int>> {
    let mut status = 0;
    loop {
        // SAFETY: `child` is the direct child returned by fork and `status` is
        // writable for this non-blocking observation.
        let result = unsafe { libc::waitpid(child, &raw mut status, libc::WNOHANG) };
        if result == child {
            return Ok(Some(status));
        }
        if result == 0 {
            return Ok(None);
        }
        if std::io::Error::last_os_error().kind() == std::io::ErrorKind::Interrupted {
            continue;
        }
        return Err(std::io::Error::last_os_error()).context("container exec waitpid failed");
    }
}

#[cfg(target_os = "linux")]
fn mirror_wait_status(status: libc::c_int) -> anyhow::Result<()> {
    if libc::WIFEXITED(status) {
        child_exit(libc::WEXITSTATUS(status));
    }
    if libc::WIFSIGNALED(status) {
        child_exit(128 + libc::WTERMSIG(status));
    }
    bail!("container exec child returned an unrecognized wait status {status}")
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
    fn proc_status_ids_use_exact_nonzero_view_fields() {
        let status = "Name:\ttrampoline\nPid:\t37\nPPid:\t12\nTracerPid:\t0\n";
        assert_eq!(parse_proc_status_id(status, "Pid").unwrap(), 37);
        assert_eq!(parse_proc_status_id(status, "PPid").unwrap(), 12);

        assert!(parse_proc_status_id("PPid:\t12\n", "Pid").is_err());
        assert!(parse_proc_status_id("Pid:\t0\n", "Pid").is_err());
        assert!(parse_proc_status_id("Pid:\tinvalid\n", "Pid").is_err());
        assert!(parse_proc_status_id("Pid:\t1\nPid:\t2\n", "Pid").is_err());
        assert_eq!(parse_proc_status_u32("PPid:\t0\n", "PPid").unwrap(), 0);
        assert!(parse_proc_status_u32("PPid:\tinvalid\n", "PPid").is_err());
    }

    #[test]
    fn proc_children_parser_is_bounded_nonzero_and_deduplicated() {
        assert_eq!(parse_proc_children("41 7 41\n").unwrap(), vec![7, 41]);
        assert!(parse_proc_children("0\n").is_err());
        assert!(parse_proc_children("not-a-pid\n").is_err());
        assert!(parse_proc_children(&"1 ".repeat(MAX_CGROUP_FILE_BYTES)).is_err());
    }

    #[test]
    fn trampoline_parent_identity_requires_complete_nonzero_generation() {
        assert!(validate_expected_parent_identity(42, 99).is_ok());
        assert!(validate_expected_parent_identity(0, 99).is_err());
        assert!(validate_expected_parent_identity(42, 0).is_err());
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
            Some("dev:builders"),
            true,
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
        assert_eq!(parsed.user.as_deref(), Some("dev:builders"));
        assert!(parsed.retain_shell_environment);
        assert!(parsed.ready_socket.is_none());

        let challenge = "a".repeat(READY_CHALLENGE_HEX_BYTES);
        let command = prepare_trampoline_with_ready_socket(
            "workspace-web",
            "/bin/true",
            &[],
            None,
            None,
            false,
            Some(("/run/vz-agent-exec/test.sock", &challenge)),
        )
        .unwrap();
        let mut malformed = command.args.clone();
        malformed[8] = "n".to_string();
        assert!(
            parse_trampoline_args(
                &malformed
                    .into_iter()
                    .map(OsString::from)
                    .collect::<Vec<_>>()
            )
            .is_err()
        );
        let parsed = parse_trampoline_args(
            &command
                .args
                .into_iter()
                .map(OsString::from)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        assert_eq!(
            parsed.ready_socket.as_deref(),
            Some("/run/vz-agent-exec/test.sock")
        );
        assert_eq!(parsed.ready_challenge.as_deref(), Some(challenge.as_str()));
    }

    #[test]
    fn direct_exec_payload_preserves_raw_argv() {
        let invocation = TrampolineInvocation {
            expected_parent_pid: 1,
            expected_parent_start_time: 1,
            container_id: "web".to_string(),
            command: "/bin/printf".to_string(),
            args: vec!["%s".to_string(), "$HOME;literal".to_string()],
            working_dir: Some("/workspace".to_string()),
            user: Some("1000:1001".to_string()),
            retain_shell_environment: false,
            ready_socket: None,
            ready_challenge: None,
        };
        let identity = ResolvedIdentity {
            uid: 1000,
            gid: 1001,
            supplementary_groups: vec![27, 1001],
        };
        let payload = prepare_exec_payload(&invocation, identity.clone()).unwrap();
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
        assert_eq!(payload.identity, identity);
        assert_eq!(payload.identity_verification_groups, [0, 0]);
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
    fn supervisor_preserves_public_signals_and_reserves_force_cancel() {
        let private_cancel = 63;
        for signal in [
            libc::SIGHUP,
            libc::SIGINT,
            libc::SIGQUIT,
            libc::SIGTERM,
            libc::SIGWINCH,
        ] {
            assert_eq!(forwarded_signal(signal, private_cancel), signal);
        }
        assert_eq!(
            forwarded_signal(private_cancel, private_cancel),
            libc::SIGKILL
        );
    }

    #[test]
    fn supervisor_gates_require_exact_release_and_only_skip_non_ttys() {
        assert!(gate_byte_matches(1, b'R', b'R'));
        assert!(!gate_byte_matches(0, b'R', b'R'));
        assert!(!gate_byte_matches(1, b'X', b'R'));
        assert!(absent_controlling_tty_errno(libc::ENOTTY));
        assert!(absent_controlling_tty_errno(libc::EBADF));
        assert!(!absent_controlling_tty_errno(libc::EPERM));
    }

    #[test]
    fn post_ack_sentinel_failure_terminates_group_before_wait() {
        assert!(setup_failure_requires_group_termination(
            ChildSetupStage::DeathSentinel
        ));
        assert!(!setup_failure_requires_group_termination(
            ChildSetupStage::Execve
        ));
        assert!(!setup_failure_requires_group_termination(
            ChildSetupStage::CgroupAttach
        ));
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
        let execve = ChildSetupError {
            stage: ChildSetupStage::Execve,
            errno: libc::ENOENT,
        };
        assert_eq!(decode_setup_error(encode_setup_error(execve)), Some(execve));
        for stage in [
            ChildSetupStage::DeathSentinel,
            ChildSetupStage::SupervisorReady,
        ] {
            let error = ChildSetupError {
                stage,
                errno: libc::EPIPE,
            };
            assert_eq!(decode_setup_error(encode_setup_error(error)), Some(error));
        }
    }

    #[test]
    fn ready_identity_round_trips_and_failure_records_fail_closed() {
        let challenge = "ab".repeat(READY_CHALLENGE_BYTES);
        let other_challenge = "cd".repeat(READY_CHALLENGE_BYTES);
        let sender_start_time = 77;
        let identity = ContainerReadyIdentity {
            container_id: "web".to_string(),
            pid: 4242,
            start_time: 123_456,
            cgroup_path: "/youki/web".to_string(),
            cgroup: (1, 2),
            namespaces: [(3, 4), (5, 6), (7, 8), (9, 10), (11, 12)],
            root: (13, 14),
        };
        assert_eq!(
            decode_ready_identity(
                &encode_ready_identity(&challenge, sender_start_time, &identity),
                &challenge,
                sender_start_time,
            )
            .unwrap(),
            identity
        );
        let encoded = encode_ready_identity(&challenge, sender_start_time, &identity);
        assert!(decode_ready_identity(&encoded, &other_challenge, sender_start_time).is_err());
        assert!(decode_ready_identity(&encoded, &challenge, sender_start_time + 1).is_err());
        let failure = format!("ERROR\t{challenge}\t{sender_start_time}\tcommand execve: not found");
        assert!(decode_ready_identity(failure.as_bytes(), &challenge, sender_start_time).is_err());
        assert!(decode_ready_identity(b"READY\tweb", &challenge, sender_start_time).is_err());
    }

    #[test]
    fn trampoline_rejects_ambiguous_container_identifiers() {
        for id in ["", "../web", "web/service", "web\nother", "--root"] {
            assert!(prepare_trampoline(id, "/bin/true", &[], None, None, false).is_err());
        }
    }

    #[test]
    fn identity_resolution_supports_docker_user_forms_and_memberships() {
        let passwd = "root:x:0:0:root:/root:/bin/sh\ndev:x:1000:1001:Dev:/home/dev:/bin/sh\n";
        let group = "root:x:0:\nstaff:x:1001:\nvideo:x:27:dev\ndocker:x:998:dev,other\n";

        assert_eq!(
            resolve_identity(Some("dev"), passwd, group).unwrap(),
            ResolvedIdentity {
                uid: 1000,
                gid: 1001,
                supplementary_groups: vec![27, 998, 1001],
            }
        );
        assert_eq!(
            resolve_identity(Some("dev:video"), passwd, group).unwrap(),
            ResolvedIdentity {
                uid: 1000,
                gid: 27,
                supplementary_groups: vec![27],
            }
        );
        assert_eq!(
            resolve_identity(Some("1000:42"), passwd, group).unwrap(),
            ResolvedIdentity {
                uid: 1000,
                gid: 42,
                supplementary_groups: vec![42],
            }
        );
        assert_eq!(
            resolve_identity(Some("1000:video"), passwd, group).unwrap(),
            ResolvedIdentity {
                uid: 1000,
                gid: 27,
                supplementary_groups: vec![27],
            }
        );
        assert_eq!(
            resolve_identity(Some("dev:42"), passwd, group).unwrap(),
            ResolvedIdentity {
                uid: 1000,
                gid: 42,
                supplementary_groups: vec![42],
            }
        );
        assert_eq!(
            resolve_identity(Some("4242"), passwd, group).unwrap(),
            ResolvedIdentity {
                uid: 4242,
                gid: 0,
                supplementary_groups: vec![0],
            }
        );
        assert_eq!(resolve_identity(None, passwd, group).unwrap().uid, 0);
        assert_eq!(
            resolve_identity(
                Some("dev"),
                &format!("# image metadata\n{passwd}"),
                &format!("  # image metadata\n{group}"),
            )
            .unwrap()
            .uid,
            1000
        );
    }

    #[test]
    fn identity_resolution_rejects_missing_malformed_and_unbounded_inputs() {
        let passwd = "dev:x:1000:1001:Dev:/home/dev:/bin/sh\n";
        let group = "staff:x:1001:\n";
        for invalid in [
            "missing",
            "dev:missing",
            ":1000",
            "dev:",
            "dev:a:b",
            "-1",
            "dev user",
            "2147483648",
        ] {
            assert!(
                resolve_identity(Some(invalid), passwd, group).is_err(),
                "expected identity rejection for {invalid:?}"
            );
        }
        assert!(resolve_identity(Some("dev"), "malformed\n", group).is_err());
        assert!(resolve_identity(Some("dev"), passwd, "malformed\n").is_err());
        assert!(resolve_identity(Some("dev"), &format!("{passwd}malformed\n"), group).is_err());
        assert!(
            resolve_identity(Some("dev:staff"), passwd, &format!("{group}malformed\n")).is_err()
        );
        assert!(
            resolve_identity(Some("dev"), &"x".repeat(MAX_IDENTITY_FILE_BYTES + 1), group).is_err()
        );
        let oversized_record = format!("{}\n", "x".repeat(MAX_IDENTITY_RECORD_BYTES + 1));
        assert!(resolve_identity(Some("dev"), passwd, &oversized_record).is_err());
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
            expected_parent_pid: 1,
            expected_parent_start_time: 1,
            container_id: "web".to_string(),
            command: "/bin/true".to_string(),
            args: Vec::new(),
            working_dir: Some("/".to_string()),
            user: None,
            retain_shell_environment: false,
            ready_socket: None,
            ready_challenge: None,
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
