//! Darwin Machine exec supervisor. The unreaped command leader pins its process
//! group through cancellation and cleanup. Detached sessions remain Machine-owned.
use anyhow::{Context, Result, bail, ensure};
use std::{
    ffi::OsString,
    io::{Read, Write},
    os::unix::process::CommandExt,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicI32, Ordering},
    time::Duration,
};

const MARKER: &str = "__vz_native_machine_exec_v1";
const READY_ROOT: &str = "/private/var/run/vz-machine-exec";
pub(crate) const CANCEL_SIGNAL: i32 = libc::SIGUSR2;
static GROUP: AtomicI32 = AtomicI32::new(0);
static PENDING_SIGNAL: AtomicI32 = AtomicI32::new(0);

pub(crate) fn is_request(args: &[OsString]) -> bool {
    args.first().is_some_and(|s| s == MARKER)
}

pub(crate) fn prepare(
    command: &str,
    args: &[String],
    cwd: &str,
    user: &str,
    endpoint: (&str, &str),
) -> Result<(String, Vec<String>)> {
    ensure!(
        !command.is_empty()
            && [command, cwd, user].iter().all(|v| !v.contains('\0'))
            && args.iter().all(|v| !v.contains('\0')),
        "invalid native command"
    );
    let mut encoded = vec![
        MARKER.into(),
        std::process::id().to_string(),
        endpoint.0.into(),
        endpoint.1.into(),
        user.into(),
        cwd.into(),
        command.into(),
    ];
    encoded.extend_from_slice(args);
    Ok((
        std::env::current_exe()?
            .to_str()
            .context("agent executable path is not UTF-8")?
            .into(),
        encoded,
    ))
}

extern "C" fn forward(signal: libc::c_int) {
    // Preserve cancellation which races spawn before the PGID is published.
    PENDING_SIGNAL.store(signal, Ordering::Release);
    let group = GROUP.load(Ordering::Acquire);
    if group > 0 {
        // SAFETY: the dedicated direct child is not reaped until group cleanup,
        // so this process group identity cannot be recycled during delivery.
        unsafe {
            libc::kill(
                -group,
                if signal == CANCEL_SIGNAL {
                    libc::SIGKILL
                } else {
                    signal
                },
            );
        }
    }
}

/// Runs before Tokio; all child identity configuration precedes execve readiness.
pub(crate) fn run(args: Vec<OsString>) -> Result<()> {
    let args = args
        .into_iter()
        .map(|s| {
            s.into_string()
                .map_err(|_| anyhow::anyhow!("non-UTF8 native invocation"))
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        args.len() >= 7 && args[0] == MARKER,
        "native invocation missing arguments"
    );
    let parent: u32 = args[1].parse()?;
    ensure!(
        parent > 1 && unsafe { libc::getppid() } == parent as i32,
        "native supervisor parent changed"
    );
    let socket = PathBuf::from(&args[2]);
    ensure!(
        socket.parent() == Some(std::path::Path::new(READY_ROOT))
            && args[3].len() == 64
            && args[3].bytes().all(|c| c.is_ascii_hexdigit()),
        "invalid private native readiness endpoint"
    );
    let identity = if args[4].is_empty() {
        None
    } else {
        Some(crate::get_user_info(&args[4])?)
    };
    // The supervisor retains root; only its payload assumes the requested user.
    let mut command = Command::new(&args[6]);
    command.args(&args[7..]).process_group(0);
    command.current_dir(if args[5].is_empty() { "/" } else { &args[5] });
    if let Some((uid, gid, home)) = identity {
        command
            .env("HOME", home)
            .env("USER", &args[4])
            .env("LOGNAME", &args[4]);
        // SAFETY: only async-signal-safe identity syscalls execute after fork.
        unsafe {
            command.pre_exec(move || {
                if libc::setgroups(0, std::ptr::null()) != 0
                    || libc::setgid(gid) != 0
                    || libc::setuid(uid) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    // Install forwarding before spawn. Signals cannot terminate the supervisor
    // while it owns a live command; forced cancellation targets the group.
    for signal in [
        libc::SIGINT,
        libc::SIGTERM,
        libc::SIGHUP,
        libc::SIGQUIT,
        libc::SIGWINCH,
        CANCEL_SIGNAL,
    ] {
        // SAFETY: handler performs only atomic loads and kill with scalar args.
        unsafe {
            libc::signal(signal, forward as *const () as libc::sighandler_t);
        }
    }
    unsafe {
        libc::signal(libc::SIGTTOU, libc::SIG_IGN);
    }
    let child = command.spawn().context("native command execve failed")?;
    let pid = child.id() as i32;
    GROUP.store(pid, Ordering::Release);
    // WNOWAIT below retains the unreaped leader; std Child Drop does not reap.
    let mut guard = GroupGuard { child, pid };
    let pending = PENDING_SIGNAL.swap(0, Ordering::AcqRel);
    if pending != 0 {
        forward(pending);
    }
    if unsafe { libc::isatty(0) } == 1 {
        ensure!(
            unsafe { libc::tcsetpgrp(0, pid) } == 0,
            "cannot assign native terminal foreground group"
        );
        unsafe {
            libc::kill(-pid, libc::SIGCONT);
        }
    }
    let mut ready = std::os::unix::net::UnixStream::connect(socket)?;
    writeln!(ready, "MACHINE\t{}\t{}", args[3], std::process::id())?;
    ready.set_read_timeout(Some(Duration::from_secs(10)))?;
    let mut acknowledgement = [0_u8; 1];
    ready.read_exact(&mut acknowledgement)?;
    ensure!(acknowledgement == [1], "native readiness rejected");
    drop(ready);
    loop {
        // SAFETY: initialized siginfo buffer, exact unreaped child identity.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let result = unsafe {
            libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &raw mut info,
                libc::WEXITED | libc::WNOHANG | libc::WNOWAIT,
            )
        };
        if result < 0 && std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted {
            bail!(
                "cannot inspect native child exit: {}",
                std::io::Error::last_os_error()
            );
        }
        if result == 0 && info.si_pid == pid {
            break;
        }
        if unsafe { libc::getppid() } != parent as i32 {
            forward(CANCEL_SIGNAL);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    guard.drain_group();
    let status = guard.child.wait()?;
    GROUP.store(0, Ordering::Release);
    use std::os::unix::process::ExitStatusExt;
    let code = status
        .code()
        .unwrap_or_else(|| 128 + status.signal().unwrap_or(1));
    // No child/group remains; avoid a second Drop cancellation after reap.
    std::mem::forget(guard);
    std::process::exit(code)
}

struct GroupGuard {
    child: std::process::Child,
    pid: i32,
}
impl GroupGuard {
    fn drain_group(&self) {
        loop {
            // The unreaped leader pins PGID throughout every enumeration/kill.
            unsafe {
                libc::kill(-self.pid, libc::SIGKILL);
            }
            let mut members = [0_i32; 16384];
            let count = unsafe {
                libc::proc_listpids(
                    2,
                    self.pid as u32,
                    members.as_mut_ptr().cast(),
                    std::mem::size_of_val(&members) as i32,
                )
            };
            if count >= 0
                && (count as usize) < std::mem::size_of_val(&members)
                && members[..count as usize / 4]
                    .iter()
                    .all(|pid| *pid == 0 || *pid == self.pid)
            {
                return;
            }
            // Query failure or remaining members is uncertainty, never success.
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}
impl Drop for GroupGuard {
    fn drop(&mut self) {
        self.drain_group();
        while self.child.wait().is_err() {
            std::thread::sleep(Duration::from_millis(20));
        }
        GROUP.store(0, Ordering::Release);
    }
}

pub(crate) struct ReadyListener {
    listener: tokio::net::UnixListener,
    path: PathBuf,
    challenge: String,
}
impl ReadyListener {
    pub fn bind() -> Result<Self> {
        use std::os::unix::fs::{DirBuilderExt, MetadataExt};
        match std::fs::DirBuilder::new().mode(0o700).create(READY_ROOT) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(e) => return Err(e.into()),
        }
        let m = std::fs::symlink_metadata(READY_ROOT)?;
        ensure!(
            m.is_dir() && m.uid() == 0 && m.mode() & 0o077 == 0,
            "native readiness directory must be private and root owned"
        );
        let mut random = [0_u8; 32];
        std::fs::File::open("/dev/urandom")?.read_exact(&mut random)?;
        let challenge = random
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        let path = PathBuf::from(READY_ROOT).join(format!("{challenge}.sock"));
        let listener = tokio::net::UnixListener::bind(&path)?;
        Ok(Self {
            listener,
            path,
            challenge,
        })
    }
    pub fn endpoint(&self) -> Result<(&str, &str)> {
        Ok((
            self.path
                .to_str()
                .context("native ready path is not UTF-8")?,
            &self.challenge,
        ))
    }
    pub async fn wait_machine(&self, pid: u32) -> Result<()> {
        use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
        tokio::time::timeout(Duration::from_secs(10), async {
            let (mut stream, _) = self.listener.accept().await?;
            let peer = stream.peer_cred()?;
            ensure!(
                peer.pid() == Some(pid as i32) && peer.uid() == 0,
                "native readiness peer is not the exact unreaped supervisor"
            );
            let mut bytes = Vec::new();
            BufReader::new((&mut stream).take(4097))
                .read_until(b'\n', &mut bytes)
                .await?;
            ensure!(
                bytes == format!("MACHINE\t{}\t{pid}\n", self.challenge).as_bytes(),
                "native execve readiness mismatch"
            );
            stream.write_all(&[1]).await?;
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        Ok(())
    }
}
impl Drop for ReadyListener {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
