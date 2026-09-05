//! Single-use ordinary Machine command supervisor. No OCI target is resolved.

use super::*;

const MARKER: &str = "__vz_machine_exec_v1";

#[derive(Debug)]
struct Invocation {
    parent_pid: u32,
    parent_start: u64,
    socket: String,
    challenge: String,
    user: String,
    cwd: String,
    command: String,
    args: Vec<String>,
}

pub(crate) fn is_request(args: &[OsString]) -> bool {
    args.first().is_some_and(|arg| arg == MARKER)
}

pub(crate) fn prepare(
    command: &str,
    args: &[String],
    cwd: &str,
    user: &str,
    handshake: (&str, &str),
) -> anyhow::Result<TrampolineCommand> {
    validate_ready_socket(handshake.0)?;
    validate_ready_challenge(handshake.1)?;
    if command.is_empty()
        || [command, cwd, user]
            .into_iter()
            .any(|value| value.contains('\0'))
        || args.iter().any(|value| value.contains('\0'))
    {
        bail!("Machine exec requires a command and NUL-free arguments");
    }
    let parent = std::process::id();
    #[cfg(target_os = "linux")]
    let parent_start = read_process_start_time(parent)?;
    #[cfg(not(target_os = "linux"))]
    let parent_start = 1;
    let mut encoded = vec![
        MARKER.to_string(),
        parent.to_string(),
        parent_start.to_string(),
        handshake.0.to_string(),
        handshake.1.to_string(),
        user.to_string(),
        cwd.to_string(),
        command.to_string(),
    ];
    encoded.extend(args.iter().cloned());
    Ok(TrampolineCommand {
        program: SELF_EXE.to_string(),
        args: encoded,
    })
}

fn parse(args: &[OsString]) -> anyhow::Result<Invocation> {
    if !is_request(args) {
        bail!("Machine exec marker missing");
    }
    let invocation = Invocation {
        parent_pid: required_utf8_arg(args, 1, "parent pid")?.parse()?,
        parent_start: required_utf8_arg(args, 2, "parent start")?.parse()?,
        socket: required_utf8_arg(args, 3, "ready socket")?,
        challenge: required_utf8_arg(args, 4, "ready challenge")?,
        user: required_utf8_arg(args, 5, "user")?,
        cwd: required_utf8_arg(args, 6, "cwd")?,
        command: required_utf8_arg(args, 7, "command")?,
        args: args
            .iter()
            .skip(8)
            .map(|value| {
                value
                    .clone()
                    .into_string()
                    .map_err(|_| anyhow::anyhow!("Machine exec argument is not UTF-8"))
            })
            .collect::<anyhow::Result<_>>()?,
    };
    validate_expected_parent_identity(invocation.parent_pid, invocation.parent_start)?;
    validate_ready_socket(&invocation.socket)?;
    validate_ready_challenge(&invocation.challenge)?;
    if invocation.command.is_empty() {
        bail!("Machine exec command missing");
    }
    Ok(invocation)
}

pub(crate) fn decode_ready(bytes: &[u8], challenge: &str, start_time: u64) -> anyhow::Result<()> {
    if bytes.len() > 4096 {
        bail!("Machine readiness exceeds bound");
    }
    let record = std::str::from_utf8(bytes)?;
    let expected = format!("MACHINE\t{challenge}\t{start_time}");
    if let Some(detail) = record.strip_prefix(&format!("ERROR\t{challenge}\t{start_time}\t")) {
        if !detail.is_empty() && detail.len() <= 2048 && !detail.chars().any(char::is_control) {
            bail!("Machine exec rejected before readiness: {detail}");
        }
    }
    if record != expected {
        bail!("Machine command did not report authenticated execve readiness");
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn run(args: Vec<OsString>) -> anyhow::Result<()> {
    let invocation = parse(&args)?;
    arm_outer_parent_death(invocation.parent_pid, invocation.parent_start)?;
    let start = read_process_start_time(std::process::id())?;
    let result = supervise(&invocation, start);
    if let Err(error) = &result {
        let detail = error
            .to_string()
            .chars()
            .filter(|c| !c.is_control())
            .take(2048)
            .collect::<String>();
        let _ = send_ready_record(
            &invocation.socket,
            format!("ERROR\t{}\t{start}\t{detail}", invocation.challenge).as_bytes(),
        );
    }
    result
}

#[cfg(target_os = "linux")]
fn supervise(invocation: &Invocation, start: u64) -> anyhow::Result<()> {
    use std::os::fd::AsRawFd as _;
    // These helpers read user records beneath the current Machine root. They
    // never resolve an OCI container, join a cgroup, or enter a namespace.
    let root = open_raw("/", libc::O_PATH | libc::O_DIRECTORY)?;
    let cwd = open_raw(
        if invocation.cwd.is_empty() {
            "/"
        } else {
            &invocation.cwd
        },
        libc::O_PATH | libc::O_DIRECTORY,
    )?;
    let user = (!invocation.user.is_empty()).then_some(invocation.user.as_str());
    let identity = resolve_container_identity(&root, user)?;
    let mut payload =
        prepare_command_payload(&invocation.command, &invocation.args, true, identity)?;
    let launcher = open_pidfd(std::process::id())?;
    let (setup_reader, setup_writer) = setup_error_pipe()?;
    let (gate_reader, gate_writer) = setup_error_pipe()?;
    enable_child_subreaper()?;
    install_supervisor_signal_handlers()?;
    // SAFETY: this same-binary mode runs before Tokio and is single-threaded.
    // Every child allocation and descriptor is prepared before fork.
    let child = unsafe { libc::fork() };
    if child < 0 {
        return Err(std::io::Error::last_os_error()).context("Machine exec fork failed");
    }
    if child == 0 {
        // SAFETY: child never returns or runs inherited Rust destructors.
        unsafe {
            libc::close(setup_reader.as_raw_fd());
        }
        child_run(
            setup_writer.as_raw_fd(),
            gate_reader.as_raw_fd(),
            gate_writer.as_raw_fd(),
            launcher.as_raw_fd(),
            cwd.as_raw_fd(),
            &mut payload,
        );
    }
    drop(gate_reader);
    // The guard retains exact child and descendant cleanup through every
    // fallible setup operation; no post-fork error returns a detached child.
    let mut guard = ChildGuard {
        child,
        reaped: false,
        group_bound: false,
    };
    bind_supervised_process_group(child)?;
    guard.group_bound = true;
    launch_death_sentinel(child, setup_writer.as_raw_fd(), launcher.as_raw_fd())?;
    assign_foreground_process_group(child)?;
    SUPERVISED_PROCESS_GROUP.store(child, Ordering::Release);
    write_all_raw(gate_writer.as_raw_fd(), b"R").map_err(std::io::Error::from_raw_os_error)?;
    drop(gate_writer);
    unblock_supervisor_signals()?;
    drop(setup_writer);
    if let Some(error) = read_setup_error(&setup_reader)? {
        bail!(
            "Machine command setup failed at {}: {}",
            error.stage.description(),
            std::io::Error::from_raw_os_error(error.errno)
        );
    }
    let early = try_wait_for_child(child)?;
    guard.reaped = early.is_some();
    if early.is_some_and(|status| libc::WIFSIGNALED(status)) {
        bail!("Machine command was signaled before execve could be proven");
    }
    send_ready_record(
        &invocation.socket,
        format!("MACHINE\t{}\t{start}", invocation.challenge).as_bytes(),
    )?;
    let status = match early {
        Some(status) => status,
        None => wait_for_child(child)?,
    };
    guard.reaped = true;
    // A failed descendant reap must never be presented as terminal success.
    // Keep the supervisor alive/retry so the host retains uncertain ownership.
    reap_until_proven(child);
    std::mem::forget(guard);
    mirror_wait_status(status)
}

#[cfg(target_os = "linux")]
// This mode deliberately precedes tracing/Tokio initialization; stderr is the
// retained command diagnostic channel while terminal proof remains unavailable.
#[allow(clippy::print_stderr)]
fn reap_until_proven(child: libc::pid_t) {
    let mut result = terminate_and_reap_descendants(child);
    while let Err(error) = result {
        eprintln!("Machine exec descendant cleanup remains unproven: {error}");
        std::thread::sleep(std::time::Duration::from_millis(100));
        // The first pass killed the pinned group's sentinel. Do not address
        // that numeric group again: once empty it could be reused. Subsequent
        // passes operate only on freshly pinned, parent-verified descendants.
        result = reap_adopted_descendants();
    }
}

#[cfg(target_os = "linux")]
struct ChildGuard {
    child: libc::pid_t,
    reaped: bool,
    group_bound: bool,
}

#[cfg(target_os = "linux")]
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if !self.reaped {
            // SAFETY: unreaped direct child cannot have its PID reused.
            unsafe {
                libc::kill(self.child, libc::SIGKILL);
            }
            while wait_for_child(self.child).is_err() {
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
        }
        if self.group_bound {
            reap_until_proven(self.child);
        }
    }
}

#[cfg(target_os = "linux")]
fn child_run(
    setup: libc::c_int,
    gate_reader: libc::c_int,
    gate_writer: libc::c_int,
    launcher: libc::c_int,
    cwd: libc::c_int,
    payload: &mut ExecPayload,
) -> ! {
    // SAFETY: scalar prctl/setpgid affect only this dedicated post-fork child.
    if unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL) } != 0
        || !pidfd_is_alive_raw(launcher)
    {
        child_setup_fail(setup, ChildSetupStage::ParentRace, libc::ESRCH);
    }
    if unsafe { libc::setpgid(0, 0) } != 0 {
        child_setup_fail(setup, ChildSetupStage::SignalState, current_errno());
    }
    // SAFETY: inherited gate fds are owned by this child, which never returns.
    unsafe {
        libc::close(gate_writer);
    }
    let mut release = 0u8;
    loop {
        let count = unsafe { libc::read(gate_reader, (&raw mut release).cast(), 1) };
        if count == 1 && release == b'R' {
            break;
        }
        if count < 0 && current_errno() == libc::EINTR {
            continue;
        }
        child_setup_fail(setup, ChildSetupStage::SupervisorReady, libc::EPIPE);
    }
    unsafe {
        libc::close(gate_reader);
    }
    if unsafe { libc::fchdir(cwd) } != 0 {
        child_setup_fail(setup, ChildSetupStage::WorkingDirectory, current_errno());
    }
    child_apply_identity(setup, payload);
    finish_child_exec(setup, None, payload)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn machine_mode_preserves_arguments_without_an_oci_target() -> anyhow::Result<()> {
        let command = prepare(
            "/bin/echo",
            &["a; b".into(), "".into()],
            "/work",
            "1000:1000",
            ("/run/vz-agent-exec/test.sock", &"a".repeat(64)),
        )?;
        let args = command
            .args
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        assert!(!super::super::is_trampoline_request(&args));
        let parsed = parse(&args)?;
        assert_eq!(parsed.command, "/bin/echo");
        assert_eq!(parsed.args, ["a; b", ""]);
        assert_eq!(parsed.cwd, "/work");
        assert_eq!(parsed.user, "1000:1000");
        Ok(())
    }

    #[test]
    fn machine_ready_rejects_wrong_identity_and_container_records() {
        assert!(decode_ready(b"MACHINE\tx\t12", "x", 12).is_ok());
        for record in [
            b"MACHINE\ty\t12".as_slice(),
            b"MACHINE\tx\t13",
            b"READY\tx\t12",
            b"MACHINE\tx\t12\textra",
            b"ERROR\tx\t12\tspawn",
        ] {
            assert!(decode_ready(record, "x", 12).is_err());
        }
    }
}
