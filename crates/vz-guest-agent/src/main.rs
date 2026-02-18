//! vz-guest-agent: runs inside macOS VM, listens on vsock, executes commands.
//!
//! This binary is baked into the golden VM image and managed by launchd.
//! It listens on vsock port 7424 (default), accepts concurrent connections,
//! and processes Request/Response messages using length-prefixed JSON framing.

// The guest agent legitimately uses unsafe for libc syscalls (vsock, sysctl, etc.)
#![allow(unsafe_code)]

mod listener;
mod process_table;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use tokio::time::{Duration, Instant};
use tracing::{error, info, warn};

use vz::protocol::{
    self as protocol, AGENT_PORT, ChannelError, Handshake, HandshakeAck, PROTOCOL_VERSION, Request,
    Response,
};

use crate::listener::VsockListener;
use crate::process_table::ProcessTable;

/// Parameters for an exec request, bundled to avoid too many function arguments.
struct ExecParams {
    id: u64,
    command: String,
    args: Vec<String>,
    working_dir: Option<String>,
    env: Vec<(String, String)>,
    user: Option<String>,
}

/// Resource usage statistics collected from the guest OS.
struct ResourceStats {
    cpu_usage_percent: f64,
    memory_used_bytes: u64,
    memory_total_bytes: u64,
    disk_used_bytes: u64,
    disk_total_bytes: u64,
    process_count: u32,
    load_average: [f64; 3],
}

const PORT_FORWARD_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const PORT_FORWARD_CONNECT_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// vz-guest-agent: VM sandbox command executor
#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Vsock port to listen on.
    #[arg(long, default_value_t = AGENT_PORT)]
    port: u32,

    /// Seconds to keep retrying vsock bind during early boot.
    #[arg(long, default_value_t = 30)]
    bind_timeout_secs: u64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    info!(
        port = args.port,
        bind_timeout_secs = args.bind_timeout_secs,
        "starting vz-guest-agent"
    );

    let listener =
        bind_vsock_listener(args.port, Duration::from_secs(args.bind_timeout_secs)).await?;

    info!(port = args.port, "listening on vsock");

    accept_loop(listener).await
}

async fn bind_vsock_listener(port: u32, timeout: Duration) -> anyhow::Result<VsockListener> {
    let started = Instant::now();
    let mut attempts = 0u32;

    loop {
        attempts = attempts.saturating_add(1);
        match VsockListener::bind(port) {
            Ok(listener) => {
                if attempts > 1 {
                    info!(attempts, elapsed = ?started.elapsed(), "vsock listener bind eventually succeeded");
                }
                return Ok(listener);
            }
            Err(err) => {
                let err_text = err.to_string();
                warn!(
                    attempts,
                    elapsed = ?started.elapsed(),
                    error = %err_text,
                    "vsock bind failed, retrying"
                );

                if started.elapsed() >= timeout {
                    return Err(anyhow::anyhow!(
                        "failed to bind vsock listener on port {port} after {attempts} attempts ({:?}): {err_text}",
                        timeout,
                    ));
                }

                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// Main accept loop.
///
/// Each accepted connection is handled in its own task so exec/control traffic
/// can run concurrently with dedicated port-forward streams.
async fn accept_loop(listener: VsockListener) -> anyhow::Result<()> {
    let mut connection_tasks = JoinSet::new();

    loop {
        info!("waiting for connection");
        tokio::select! {
            accept_result = listener.accept() => {
                let stream = accept_result.context("failed to accept vsock connection")?;
                info!("connection accepted");

                connection_tasks.spawn(async move {
                    let process_table = Arc::new(Mutex::new(ProcessTable::new()));

                    match handle_connection(stream, process_table.clone()).await {
                        Ok(()) => info!("connection closed cleanly"),
                        Err(e) => warn!(error = %e, "connection ended with error"),
                    }

                    info!("draining processes");
                    drain_processes(process_table).await;
                    info!("drain complete, ready for next connection");
                });
            }
            join_result = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                if let Some(Err(error)) = join_result {
                    warn!(error = %error, "connection task join failed");
                }
            }
        }
    }
}

/// Handle a single vsock connection: handshake, then request dispatch loop.
async fn handle_connection<S>(
    stream: S,
    process_table: Arc<Mutex<ProcessTable>>,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let mut stream = stream;

    // Wait for handshake from host
    let handshake: Handshake = protocol::read_frame(&mut stream)
        .await
        .context("failed to read handshake")?;

    info!(
        host_version = handshake.protocol_version,
        capabilities = ?handshake.capabilities,
        "received handshake"
    );

    let negotiated_version = handshake.protocol_version.min(PROTOCOL_VERSION);
    if negotiated_version == 0 {
        let err_msg = "protocol version 0 is not supported";
        warn!(err_msg);
        anyhow::bail!(err_msg);
    }

    let ack = HandshakeAck {
        protocol_version: negotiated_version,
        agent_version: env!("CARGO_PKG_VERSION").to_string(),
        os: std::env::consts::OS.to_string(),
        capabilities: vec!["user_exec".to_string(), "port_forward".to_string()],
    };

    protocol::write_frame(&mut stream, &ack)
        .await
        .context("failed to write handshake ack")?;

    info!(
        negotiated_version,
        agent_version = env!("CARGO_PKG_VERSION"),
        "handshake complete"
    );

    // First request may be a dedicated PortForward connection.
    let first_request: Request = match protocol::read_frame(&mut stream).await {
        Ok(req) => req,
        Err(ChannelError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
            info!("connection closed by host (EOF)");
            return Ok(());
        }
        Err(e) => {
            return Err(e).context("failed to read first request frame");
        }
    };

    if let Request::PortForward {
        id,
        target_port,
        protocol,
    } = first_request
    {
        handle_port_forward_connection(id, target_port, &protocol, &mut stream).await?;
        return Ok(());
    }

    let (mut reader, writer) = tokio::io::split(stream);
    let writer = Arc::new(Mutex::new(writer));
    dispatch_request(first_request, writer.clone(), process_table.clone()).await;

    // Request dispatch loop
    loop {
        let request: Request = match protocol::read_frame(&mut reader).await {
            Ok(req) => req,
            Err(ChannelError::Io(e)) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                info!("connection closed by host (EOF)");
                return Ok(());
            }
            Err(e) => {
                return Err(e).context("failed to read request frame");
            }
        };

        dispatch_request(request, writer.clone(), process_table.clone()).await;
    }
}

/// Handle a dedicated port-forward connection.
///
/// This path requires the first post-handshake request to be `PortForward`.
/// After `PortForwardReady`, the stream becomes a raw bidirectional pipe.
async fn handle_port_forward_connection<S>(
    id: u64,
    target_port: u16,
    protocol_name: &str,
    stream: &mut S,
) -> anyhow::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    if protocol_name != "tcp" {
        protocol::write_frame(
            stream,
            &Response::Error {
                id,
                error: format!("unsupported port forward protocol: {protocol_name}"),
            },
        )
        .await
        .context("failed to write PortForward protocol error")?;
        return Ok(());
    }

    let mut target = match connect_port_forward_target(target_port).await {
        Ok(stream) => stream,
        Err(e) => {
            protocol::write_frame(
                stream,
                &Response::Error {
                    id,
                    error: format!("failed to connect to localhost:{target_port}: {e}"),
                },
            )
            .await
            .context("failed to write PortForward connect error")?;
            return Ok(());
        }
    };

    protocol::write_frame(stream, &Response::PortForwardReady { id })
        .await
        .context("failed to write PortForwardReady")?;

    let (to_target, to_host) = tokio::io::copy_bidirectional(stream, &mut target)
        .await
        .context("port forward relay failed")?;

    info!(
        id,
        target_port,
        bytes_to_target = to_target,
        bytes_to_host = to_host,
        "port forward relay finished"
    );

    // Best-effort graceful shutdown of the guest-side TCP stream.
    if let Err(e) = target.shutdown().await {
        warn!(error = %e, "failed to shutdown port-forward target stream");
    }

    Ok(())
}

async fn connect_port_forward_target(target_port: u16) -> std::io::Result<tokio::net::TcpStream> {
    let started = Instant::now();

    loop {
        match tokio::net::TcpStream::connect(("127.0.0.1", target_port)).await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                if started.elapsed() >= PORT_FORWARD_CONNECT_TIMEOUT {
                    return Err(error);
                }

                tokio::time::sleep(PORT_FORWARD_CONNECT_RETRY_INTERVAL).await;
            }
        }
    }
}

/// Dispatch a single request to the appropriate handler.
async fn dispatch_request<W>(
    request: Request,
    writer: Arc<Mutex<W>>,
    process_table: Arc<Mutex<ProcessTable>>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    match request {
        Request::Ping { id } => {
            send_response(&writer, &Response::Pong { id }).await;
        }
        Request::Exec {
            id,
            command,
            args,
            working_dir,
            env,
            user,
        } => {
            let params = ExecParams {
                id,
                command,
                args,
                working_dir,
                env,
                user,
            };
            handle_exec(params, writer, process_table).await;
        }
        Request::Signal { exec_id, signal } => {
            handle_signal(exec_id, signal, &process_table).await;
        }
        Request::StdinWrite { id, exec_id, data } => {
            handle_stdin_write(id, exec_id, &data, &writer, &process_table).await;
        }
        Request::StdinClose { exec_id } => {
            handle_stdin_close(exec_id, &process_table).await;
        }
        Request::SystemInfo { id } => {
            handle_system_info(id, &writer).await;
        }
        Request::ResourceStats { id } => {
            handle_resource_stats(id, &writer).await;
        }
        Request::PortForward { id, .. } => {
            send_response(
                &writer,
                &Response::Error {
                    id,
                    error: "port forwarding requires a dedicated connection".to_string(),
                },
            )
            .await;
        }
    }
}

/// Handle an Exec request: spawn child process, stream stdout/stderr, report exit.
async fn handle_exec<W>(
    params: ExecParams,
    writer: Arc<Mutex<W>>,
    process_table: Arc<Mutex<ProcessTable>>,
) where
    W: AsyncWrite + Unpin + Send + 'static,
{
    use tokio::io::AsyncReadExt;

    let ExecParams {
        id,
        command,
        args,
        working_dir,
        env,
        user,
    } = params;

    let spawn_result = if let Some(ref username) = user {
        spawn_as_user(username, &command, &args, working_dir.as_deref(), &env)
    } else {
        spawn_direct(&command, &args, working_dir.as_deref(), &env)
    };

    let mut child = match spawn_result {
        Ok(child) => child,
        Err(e) => {
            warn!(id, command, error = %e, "exec spawn failed");
            send_response(
                &writer,
                &Response::ExecError {
                    id,
                    error: e.to_string(),
                },
            )
            .await;
            return;
        }
    };

    info!(id, command, ?args, "process spawned");

    // Take ownership of pipes
    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdin = child.stdin.take();

    // Store in process table
    {
        let mut table = process_table.lock().await;
        table.insert(id, child, stdin);
    }

    let exec_id = id;

    // Spawn stdout reader task
    let stdout_writer = writer.clone();
    let stdout_handle = tokio::spawn(async move {
        if let Some(mut stdout) = stdout {
            let mut buf = vec![0u8; 8192];
            loop {
                match stdout.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        send_response(
                            &stdout_writer,
                            &Response::Stdout {
                                exec_id,
                                data: buf[..n].to_vec(),
                            },
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!(exec_id, error = %e, "stdout read error");
                        break;
                    }
                }
            }
        }
    });

    // Spawn stderr reader task
    let stderr_writer = writer.clone();
    let stderr_handle = tokio::spawn(async move {
        if let Some(mut stderr) = stderr {
            let mut buf = vec![0u8; 8192];
            loop {
                match stderr.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        send_response(
                            &stderr_writer,
                            &Response::Stderr {
                                exec_id,
                                data: buf[..n].to_vec(),
                            },
                        )
                        .await;
                    }
                    Err(e) => {
                        warn!(exec_id, error = %e, "stderr read error");
                        break;
                    }
                }
            }
        }
    });

    // Spawn exit watcher task
    let exit_writer = writer.clone();
    let exit_table = process_table.clone();
    tokio::spawn(async move {
        // Wait for stdout/stderr to finish before reporting exit
        let _ = stdout_handle.await;
        let _ = stderr_handle.await;

        // Wait for the child to exit
        let exit_code = {
            let mut table = exit_table.lock().await;
            if let Some(entry) = table.get_mut(exec_id) {
                match entry.child.wait().await {
                    Ok(status) => status.code().unwrap_or(-1),
                    Err(e) => {
                        warn!(exec_id, error = %e, "wait error");
                        -1
                    }
                }
            } else {
                // Process was already removed (e.g., killed during drain)
                -1
            }
        };

        info!(exec_id, exit_code, "process exited");

        send_response(
            &exit_writer,
            &Response::ExitCode {
                exec_id,
                code: exit_code,
            },
        )
        .await;

        // Remove from process table
        {
            let mut table = exit_table.lock().await;
            table.remove(exec_id);
        }
    });
}

/// Spawn a process directly (as root / agent's user).
fn spawn_direct(
    command: &str,
    args: &[String],
    working_dir: Option<&str>,
    env: &[(String, String)],
) -> anyhow::Result<tokio::process::Child> {
    use std::process::Stdio;
    use tokio::process::Command;

    let mut cmd = Command::new(command);
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::piped());

    if let Some(dir) = working_dir {
        cmd.current_dir(dir);
    }

    for (key, value) in env {
        cmd.env(key, value);
    }

    cmd.spawn().context("failed to spawn process")
}

/// Spawn a process as a specific user by dropping privileges via setuid/setgid.
fn spawn_as_user(
    username: &str,
    command: &str,
    args: &[String],
    working_dir: Option<&str>,
    env: &[(String, String)],
) -> anyhow::Result<tokio::process::Child> {
    use std::process::Stdio;
    use tokio::process::Command;

    let (uid, gid, home) = get_user_info(username)?;

    let mut cmd = Command::new(command);
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .stdin(Stdio::piped());

    // Set the user's home directory and shell environment
    cmd.env("HOME", &home);
    cmd.env("USER", username);
    cmd.env("LOGNAME", username);

    if let Some(dir) = working_dir {
        cmd.current_dir(dir);
    } else {
        cmd.current_dir(&home);
    }

    for (key, value) in env {
        cmd.env(key, value);
    }

    // SAFETY: pre_exec runs between fork and exec. setgid/setuid are
    // standard POSIX calls. We set gid first (must happen before dropping
    // root via setuid).
    unsafe {
        cmd.pre_exec(move || {
            if libc::setgid(gid) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            if libc::setuid(uid) != 0 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }

    cmd.spawn()
        .with_context(|| format!("failed to spawn process as user {username}"))
}

/// Look up UID, GID, and home directory for a username.
fn get_user_info(username: &str) -> anyhow::Result<(u32, u32, String)> {
    use std::ffi::CString;

    let c_name = CString::new(username).context("username contains null byte")?;

    // SAFETY: getpwnam is a standard POSIX function. We pass a valid C string
    // and check the return value for null.
    let pw = unsafe { libc::getpwnam(c_name.as_ptr()) };
    if pw.is_null() {
        anyhow::bail!("user not found: {username}");
    }

    // SAFETY: pw is non-null, so dereferencing is valid.
    let uid = unsafe { (*pw).pw_uid };
    let gid = unsafe { (*pw).pw_gid };
    let home = unsafe {
        std::ffi::CStr::from_ptr((*pw).pw_dir)
            .to_string_lossy()
            .into_owned()
    };
    Ok((uid, gid, home))
}

/// Handle a Signal request.
async fn handle_signal(exec_id: u64, signal: i32, process_table: &Arc<Mutex<ProcessTable>>) {
    let table = process_table.lock().await;
    if let Some(entry) = table.get(exec_id) {
        let pid = entry.pid();
        if let Some(pid) = pid {
            info!(exec_id, pid, signal, "sending signal");
            // SAFETY: kill is a standard POSIX function. We pass a valid pid and signal.
            unsafe {
                libc::kill(pid, signal);
            }
        }
    } else {
        warn!(exec_id, "signal: process not found");
    }
}

/// Handle a StdinWrite request.
async fn handle_stdin_write<W>(
    id: u64,
    exec_id: u64,
    data: &[u8],
    writer: &Arc<Mutex<W>>,
    process_table: &Arc<Mutex<ProcessTable>>,
) where
    W: AsyncWrite + Unpin + Send,
{
    use tokio::io::AsyncWriteExt;

    let mut table = process_table.lock().await;
    if let Some(entry) = table.get_mut(exec_id) {
        if let Some(ref mut stdin) = entry.stdin {
            match stdin.write_all(data).await {
                Ok(()) => {
                    send_response(writer, &Response::Ok { id }).await;
                }
                Err(e) => {
                    warn!(exec_id, error = %e, "stdin write error");
                    send_response(
                        writer,
                        &Response::Error {
                            id,
                            error: format!("stdin write failed: {e}"),
                        },
                    )
                    .await;
                }
            }
        } else {
            send_response(
                writer,
                &Response::Error {
                    id,
                    error: "stdin already closed".to_string(),
                },
            )
            .await;
        }
    } else {
        send_response(
            writer,
            &Response::Error {
                id,
                error: format!("process {exec_id} not found"),
            },
        )
        .await;
    }
}

/// Handle a StdinClose request (fire-and-forget).
async fn handle_stdin_close(exec_id: u64, process_table: &Arc<Mutex<ProcessTable>>) {
    let mut table = process_table.lock().await;
    if let Some(entry) = table.get_mut(exec_id) {
        entry.stdin = None; // Drop the stdin handle, closing the pipe
        info!(exec_id, "stdin closed");
    } else {
        warn!(exec_id, "stdin close: process not found");
    }
}

/// Handle a SystemInfo request.
async fn handle_system_info<W>(id: u64, writer: &Arc<Mutex<W>>)
where
    W: AsyncWrite + Unpin + Send,
{
    match collect_system_info() {
        Ok((cpu_count, memory_bytes, disk_free_bytes, os_version)) => {
            send_response(
                writer,
                &Response::SystemInfoResult {
                    id,
                    cpu_count,
                    memory_bytes,
                    disk_free_bytes,
                    os_version,
                },
            )
            .await;
        }
        Err(e) => {
            warn!(error = %e, "failed to collect system info");
            send_response(
                writer,
                &Response::Error {
                    id,
                    error: format!("system info failed: {e}"),
                },
            )
            .await;
        }
    }
}

/// Collect system information using sysctl and statfs.
fn collect_system_info() -> anyhow::Result<(u32, u64, u64, String)> {
    let cpu_count = get_sysctl_u32("hw.ncpu")?;
    let memory_bytes = get_sysctl_u64("hw.memsize")?;
    let disk_free_bytes = get_statfs_free("/")?;
    let os_version = get_os_version();
    Ok((cpu_count, memory_bytes, disk_free_bytes, os_version))
}

/// Handle a ResourceStats request.
async fn handle_resource_stats<W>(id: u64, writer: &Arc<Mutex<W>>)
where
    W: AsyncWrite + Unpin + Send,
{
    match collect_resource_stats() {
        Ok(stats) => {
            send_response(
                writer,
                &Response::ResourceStatsResult {
                    id,
                    cpu_usage_percent: stats.cpu_usage_percent,
                    memory_used_bytes: stats.memory_used_bytes,
                    memory_total_bytes: stats.memory_total_bytes,
                    disk_used_bytes: stats.disk_used_bytes,
                    disk_total_bytes: stats.disk_total_bytes,
                    process_count: stats.process_count,
                    load_average: stats.load_average,
                },
            )
            .await;
        }
        Err(e) => {
            warn!(error = %e, "failed to collect resource stats");
            send_response(
                writer,
                &Response::Error {
                    id,
                    error: format!("resource stats failed: {e}"),
                },
            )
            .await;
        }
    }
}

/// Collect resource usage statistics.
fn collect_resource_stats() -> anyhow::Result<ResourceStats> {
    let memory_total = get_sysctl_u64("hw.memsize")?;

    // Get load averages
    let mut loadavg: [f64; 3] = [0.0; 3];
    // SAFETY: getloadavg is a standard POSIX function.
    let ret = unsafe { libc::getloadavg(loadavg.as_mut_ptr(), 3) };
    if ret < 0 {
        anyhow::bail!("getloadavg failed");
    }

    // Disk stats via statfs
    let (disk_used, disk_total) = get_disk_usage("/")?;
    let disk_free = get_statfs_free("/")?;

    // Approximate memory used (total - free is not accurate on macOS,
    // but sysctl vm.page_pageable_internal_count etc. are complex)
    // For now, use a simpler approximation
    let memory_used = memory_total.saturating_sub(disk_free.min(memory_total));

    // CPU usage: approximate from load average / cpu count
    let cpu_count = get_sysctl_u32("hw.ncpu")? as f64;
    let cpu_usage = if cpu_count > 0.0 {
        (loadavg[0] / cpu_count * 100.0).min(100.0)
    } else {
        0.0
    };

    // Process count
    let process_count = get_process_count();

    Ok(ResourceStats {
        cpu_usage_percent: cpu_usage,
        memory_used_bytes: memory_used,
        memory_total_bytes: memory_total,
        disk_used_bytes: disk_used,
        disk_total_bytes: disk_total,
        process_count,
        load_average: loadavg,
    })
}

/// Get a u32 sysctl value.
#[cfg(target_os = "macos")]
fn get_sysctl_u32(name: &str) -> anyhow::Result<u32> {
    use std::ffi::CString;

    let c_name = CString::new(name)?;
    let mut value: u32 = 0;
    let mut size = std::mem::size_of::<u32>();

    // SAFETY: sysctlbyname is a standard macOS function.
    let ret = unsafe {
        libc::sysctlbyname(
            c_name.as_ptr(),
            &mut value as *mut u32 as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };

    if ret != 0 {
        anyhow::bail!("sysctl {name} failed: {}", std::io::Error::last_os_error());
    }

    Ok(value)
}

/// Get a u32 system value on non-macOS targets.
#[cfg(not(target_os = "macos"))]
fn get_sysctl_u32(name: &str) -> anyhow::Result<u32> {
    match name {
        "hw.ncpu" => {
            let count = std::thread::available_parallelism()
                .map_err(|e| anyhow::anyhow!("failed to get cpu count: {e}"))?;
            Ok(count.get() as u32)
        }
        _ => anyhow::bail!("unsupported sysctl key on this platform: {name}"),
    }
}

/// Get a u64 sysctl value.
#[cfg(target_os = "macos")]
fn get_sysctl_u64(name: &str) -> anyhow::Result<u64> {
    use std::ffi::CString;

    let c_name = CString::new(name)?;
    let mut value: u64 = 0;
    let mut size = std::mem::size_of::<u64>();

    // SAFETY: sysctlbyname is a standard macOS function.
    let ret = unsafe {
        libc::sysctlbyname(
            c_name.as_ptr(),
            &mut value as *mut u64 as *mut libc::c_void,
            &mut size,
            std::ptr::null_mut(),
            0,
        )
    };

    if ret != 0 {
        anyhow::bail!("sysctl {name} failed: {}", std::io::Error::last_os_error());
    }

    Ok(value)
}

/// Get a u64 system value on non-macOS targets.
#[cfg(not(target_os = "macos"))]
fn get_sysctl_u64(name: &str) -> anyhow::Result<u64> {
    match name {
        "hw.memsize" => {
            let meminfo = std::fs::read_to_string("/proc/meminfo")
                .context("failed to read /proc/meminfo for total memory")?;
            let line = meminfo
                .lines()
                .find(|l| l.starts_with("MemTotal:"))
                .ok_or_else(|| anyhow::anyhow!("MemTotal not found in /proc/meminfo"))?;
            let kb = line
                .split_whitespace()
                .nth(1)
                .ok_or_else(|| anyhow::anyhow!("MemTotal value missing"))?
                .parse::<u64>()
                .context("failed to parse MemTotal from /proc/meminfo")?;
            Ok(kb * 1024)
        }
        _ => anyhow::bail!("unsupported sysctl key on this platform: {name}"),
    }
}

/// Get free bytes on a filesystem via statfs.
fn get_statfs_free(path: &str) -> anyhow::Result<u64> {
    use std::ffi::CString;

    let c_path = CString::new(path)?;
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };

    // SAFETY: statfs is a standard POSIX function.
    let ret = unsafe { libc::statfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        anyhow::bail!("statfs {path} failed: {}", std::io::Error::last_os_error());
    }

    Ok(stat.f_bavail as u64 * stat.f_bsize as u64)
}

/// Get disk used/total bytes via statfs.
fn get_disk_usage(path: &str) -> anyhow::Result<(u64, u64)> {
    use std::ffi::CString;

    let c_path = CString::new(path)?;
    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };

    // SAFETY: statfs is a standard POSIX function.
    let ret = unsafe { libc::statfs(c_path.as_ptr(), &mut stat) };
    if ret != 0 {
        anyhow::bail!("statfs {path} failed: {}", std::io::Error::last_os_error());
    }

    let total = stat.f_blocks as u64 * stat.f_bsize as u64;
    let free = stat.f_bavail as u64 * stat.f_bsize as u64;
    let used = total.saturating_sub(free);

    Ok((used, total))
}

/// Get the macOS version string.
#[cfg(target_os = "macos")]
fn get_os_version() -> String {
    // Try to read from /System/Library/CoreServices/SystemVersion.plist
    // Fallback to "macOS (unknown)"
    std::process::Command::new("sw_vers")
        .arg("-productVersion")
        .output()
        .ok()
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| format!("macOS {}", s.trim()))
        .unwrap_or_else(|| "macOS (unknown)".to_string())
}

/// Get a Linux version string.
#[cfg(not(target_os = "macos"))]
fn get_os_version() -> String {
    std::fs::read_to_string("/proc/sys/kernel/osrelease")
        .ok()
        .map(|s| format!("Linux {}", s.trim()))
        .unwrap_or_else(|| "Linux (unknown)".to_string())
}

/// Get the number of running processes.
fn get_process_count() -> u32 {
    // Use sysctl kern.proc.all to count processes
    // Simpler approach: count lines from ps
    std::process::Command::new("ps")
        .arg("-A")
        .arg("-o")
        .arg("pid=")
        .output()
        .ok()
        .map(|output| String::from_utf8_lossy(&output.stdout).lines().count() as u32)
        .unwrap_or(0)
}

/// Send a response frame, logging any errors.
async fn send_response<W>(writer: &Arc<Mutex<W>>, response: &Response)
where
    W: AsyncWrite + Unpin + Send,
{
    let mut w = writer.lock().await;
    if let Err(e) = protocol::write_frame(&mut *w, response).await {
        error!(error = %e, "failed to send response");
    }
}

/// Drain all child processes: SIGTERM, wait 5s, SIGKILL.
async fn drain_processes(process_table: Arc<Mutex<ProcessTable>>) {
    let mut table = process_table.lock().await;

    if table.is_empty() {
        return;
    }

    let count = table.len();
    info!(count, "draining child processes");

    // SIGTERM all children
    for (exec_id, entry) in table.iter() {
        if let Some(pid) = entry.pid() {
            info!(exec_id, pid, "sending SIGTERM");
            // SAFETY: kill is a standard POSIX function.
            unsafe {
                libc::kill(pid, libc::SIGTERM);
            }
        }
    }

    // Wait up to 5 seconds for graceful exit
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !table.is_empty() && tokio::time::Instant::now() < deadline {
        let mut exited = Vec::new();
        for (exec_id, entry) in table.iter_mut() {
            match entry.child.try_wait() {
                Ok(Some(_status)) => {
                    exited.push(*exec_id);
                }
                Ok(None) => {} // still running
                Err(e) => {
                    warn!(exec_id, error = %e, "try_wait error during drain");
                    exited.push(*exec_id);
                }
            }
        }
        for id in exited {
            info!(exec_id = id, "process exited during drain");
            table.remove(id);
        }
        if !table.is_empty() {
            // Release the lock briefly to allow other tasks to proceed
            drop(table);
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            table = process_table.lock().await;
        }
    }

    // SIGKILL any remaining
    if !table.is_empty() {
        let remaining = table.len();
        warn!(remaining, "SIGKILL remaining processes after timeout");
        for (exec_id, entry) in table.iter() {
            if let Some(pid) = entry.pid() {
                info!(exec_id, pid, "sending SIGKILL");
                // SAFETY: kill is a standard POSIX function.
                unsafe {
                    libc::kill(pid, libc::SIGKILL);
                }
            }
        }
    }
    table.clear();
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use vz::protocol::{Handshake, Request, Response};

    /// Helper: run a connection handler with an in-memory duplex stream.
    async fn run_test_connection<F, Fut>(test_fn: F)
    where
        F: FnOnce(tokio::io::DuplexStream, Arc<Mutex<ProcessTable>>) -> Fut,
        Fut: std::future::Future<Output = ()>,
    {
        let (client, server) = tokio::io::duplex(64 * 1024);
        let process_table = Arc::new(Mutex::new(ProcessTable::new()));

        let server_table = process_table.clone();
        let server_handle = tokio::spawn(async move {
            let _ = handle_connection(server, server_table).await;
        });

        test_fn(client, process_table).await;

        // Abort the server (it's blocked waiting for more frames)
        server_handle.abort();
    }

    #[tokio::test]
    async fn handshake_and_ping_pong() {
        run_test_connection(|stream, _table| async move {
            let (mut reader, mut writer) = tokio::io::split(stream);

            // Send handshake
            let handshake = Handshake {
                protocol_version: 1,
                capabilities: vec![],
            };
            protocol::write_frame(&mut writer, &handshake)
                .await
                .expect("write handshake");

            // Read handshake ack
            let ack: HandshakeAck = protocol::read_frame(&mut reader).await.expect("read ack");
            assert_eq!(ack.protocol_version, 1);
            assert!(!ack.agent_version.is_empty());
            assert!(!ack.os.is_empty());
            assert!(ack.capabilities.iter().any(|c| c == "port_forward"));

            // Send ping
            protocol::write_frame(&mut writer, &Request::Ping { id: 42 })
                .await
                .expect("write ping");

            // Read pong
            let resp: Response = protocol::read_frame(&mut reader).await.expect("read pong");
            assert_eq!(resp, Response::Pong { id: 42 });
        })
        .await;
    }

    #[tokio::test]
    async fn exec_echo_command() {
        run_test_connection(|stream, _table| async move {
            let (mut reader, mut writer) = tokio::io::split(stream);

            // Handshake
            protocol::write_frame(
                &mut writer,
                &Handshake {
                    protocol_version: 1,
                    capabilities: vec![],
                },
            )
            .await
            .expect("handshake");
            let _ack: HandshakeAck = protocol::read_frame(&mut reader).await.expect("ack");

            // Send exec
            protocol::write_frame(
                &mut writer,
                &Request::Exec {
                    id: 1,
                    command: "echo".to_string(),
                    args: vec!["hello world".to_string()],
                    working_dir: None,
                    env: vec![],
                    user: None,
                },
            )
            .await
            .expect("exec");

            // Collect responses until ExitCode
            let mut stdout_data = Vec::new();
            loop {
                let resp: Response = protocol::read_frame(&mut reader).await.expect("read resp");
                match resp {
                    Response::Stdout { data, .. } => {
                        stdout_data.extend_from_slice(&data);
                    }
                    Response::ExitCode { exec_id, code } => {
                        assert_eq!(exec_id, 1);
                        assert_eq!(code, 0);
                        break;
                    }
                    other => {
                        // May get stderr chunks too, just collect them
                        if matches!(other, Response::Stderr { .. }) {
                            continue;
                        }
                        panic!("unexpected response: {other:?}");
                    }
                }
            }

            let stdout = String::from_utf8_lossy(&stdout_data);
            assert_eq!(stdout.trim(), "hello world");
        })
        .await;
    }

    #[tokio::test]
    async fn exec_nonexistent_command() {
        run_test_connection(|stream, _table| async move {
            let (mut reader, mut writer) = tokio::io::split(stream);

            // Handshake
            protocol::write_frame(
                &mut writer,
                &Handshake {
                    protocol_version: 1,
                    capabilities: vec![],
                },
            )
            .await
            .expect("handshake");
            let _ack: HandshakeAck = protocol::read_frame(&mut reader).await.expect("ack");

            // Send exec for nonexistent command
            protocol::write_frame(
                &mut writer,
                &Request::Exec {
                    id: 1,
                    command: "/nonexistent/binary/foobar".to_string(),
                    args: vec![],
                    working_dir: None,
                    env: vec![],
                    user: None,
                },
            )
            .await
            .expect("exec");

            // Should get ExecError
            let resp: Response = protocol::read_frame(&mut reader).await.expect("read resp");
            match resp {
                Response::ExecError { id, error } => {
                    assert_eq!(id, 1);
                    assert!(!error.is_empty());
                }
                other => panic!("expected ExecError, got: {other:?}"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn handshake_version_negotiation() {
        run_test_connection(|stream, _table| async move {
            let (mut reader, mut writer) = tokio::io::split(stream);

            // Send handshake with higher version
            protocol::write_frame(
                &mut writer,
                &Handshake {
                    protocol_version: 99,
                    capabilities: vec![],
                },
            )
            .await
            .expect("handshake");

            // Guest should negotiate down to its max (PROTOCOL_VERSION = 1)
            let ack: HandshakeAck = protocol::read_frame(&mut reader).await.expect("ack");
            assert_eq!(ack.protocol_version, PROTOCOL_VERSION);
        })
        .await;
    }

    #[tokio::test]
    async fn port_forward_relay_echo() {
        let tcp_listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind tcp listener");
        let target_port = tcp_listener.local_addr().expect("local addr").port();

        let tcp_task = tokio::spawn(async move {
            let (mut socket, _) = tcp_listener.accept().await.expect("accept tcp");
            let mut buf = [0u8; 64];
            let n = socket.read(&mut buf).await.expect("read tcp payload");
            socket
                .write_all(&buf[..n])
                .await
                .expect("write tcp payload");
        });

        run_test_connection(|stream, _table| async move {
            let (mut reader, mut writer) = tokio::io::split(stream);

            protocol::write_frame(
                &mut writer,
                &Handshake {
                    protocol_version: 1,
                    capabilities: vec![],
                },
            )
            .await
            .expect("handshake");
            let _ack: HandshakeAck = protocol::read_frame(&mut reader).await.expect("ack");

            protocol::write_frame(
                &mut writer,
                &Request::PortForward {
                    id: 7,
                    target_port,
                    protocol: "tcp".to_string(),
                },
            )
            .await
            .expect("port forward request");

            let ready: Response = protocol::read_frame(&mut reader)
                .await
                .expect("port forward ready");
            assert_eq!(ready, Response::PortForwardReady { id: 7 });

            writer.write_all(b"ping").await.expect("write raw ping");

            let mut echoed = [0u8; 4];
            reader.read_exact(&mut echoed).await.expect("read raw echo");
            assert_eq!(&echoed, b"ping");

            writer.shutdown().await.expect("shutdown writer");
        })
        .await;

        tcp_task.await.expect("tcp task");
    }

    #[tokio::test]
    async fn port_forward_relay_waits_for_delayed_target() {
        let target_port = {
            let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind temp port");
            listener.local_addr().expect("local addr").port()
        };

        let tcp_task = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;

            let tcp_listener = tokio::net::TcpListener::bind(("127.0.0.1", target_port))
                .await
                .expect("bind delayed tcp listener");

            let (mut socket, _) = tcp_listener.accept().await.expect("accept delayed tcp");
            let mut buf = [0u8; 64];
            let n = socket
                .read(&mut buf)
                .await
                .expect("read delayed tcp payload");
            socket
                .write_all(&buf[..n])
                .await
                .expect("write delayed tcp payload");
        });

        run_test_connection(|stream, _table| async move {
            let (mut reader, mut writer) = tokio::io::split(stream);

            protocol::write_frame(
                &mut writer,
                &Handshake {
                    protocol_version: 1,
                    capabilities: vec![],
                },
            )
            .await
            .expect("handshake");
            let _ack: HandshakeAck = protocol::read_frame(&mut reader).await.expect("ack");

            protocol::write_frame(
                &mut writer,
                &Request::PortForward {
                    id: 8,
                    target_port,
                    protocol: "tcp".to_string(),
                },
            )
            .await
            .expect("port forward request");

            let ready: Response = protocol::read_frame(&mut reader)
                .await
                .expect("port forward ready");
            assert_eq!(ready, Response::PortForwardReady { id: 8 });

            writer.write_all(b"ping").await.expect("write raw ping");

            let mut echoed = [0u8; 4];
            reader.read_exact(&mut echoed).await.expect("read raw echo");
            assert_eq!(&echoed, b"ping");

            writer.shutdown().await.expect("shutdown writer");
        })
        .await;

        tcp_task.await.expect("tcp task");
    }

    #[test]
    fn process_table_insert_remove() {
        let table = ProcessTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);

        // We can't easily create a real tokio::process::Child in a test,
        // so just test the basic structure
        assert!(table.get(1).is_none());
    }
}
