//! vz-guest-agent: runs inside VM, listens on vsock, serves gRPC.
//!
//! This binary is baked into the golden VM image and managed by launchd.
//! It listens on vsock port 7424 (default) and serves the gRPC agent,
//! OCI, and network services.

// The guest agent legitimately uses unsafe for libc syscalls (vsock, sysctl, etc.)
#![allow(unsafe_code)]

mod grpc_server;
mod listener;
#[cfg(target_os = "linux")]
pub(crate) mod network;
mod process_table;

use std::sync::Arc;

use anyhow::Context;
use clap::Parser;
use tokio::sync::Mutex;
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

use vz::protocol::AGENT_PORT;

use crate::listener::VsockListener;
use crate::process_table::ProcessTable;

/// Resource usage statistics collected from the guest OS.
pub(crate) struct ResourceStats {
    pub(crate) cpu_usage_percent: f64,
    pub(crate) memory_used_bytes: u64,
    pub(crate) memory_total_bytes: u64,
    pub(crate) disk_used_bytes: u64,
    pub(crate) disk_total_bytes: u64,
    pub(crate) process_count: u32,
    pub(crate) load_average: [f64; 3],
}

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

    grpc_accept_loop(listener).await
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

/// Start the gRPC (tonic) server over vsock.
///
/// Registers all three gRPC services (Agent, OCI, Network) and serves them
/// using tonic's `serve_with_incoming` with a custom [`VsockIncoming`] stream.
async fn grpc_accept_loop(listener: VsockListener) -> anyhow::Result<()> {
    use std::sync::Arc as StdArc;
    use vz_agent_proto::{
        agent_service_server::AgentServiceServer, network_service_server::NetworkServiceServer,
        oci_service_server::OciServiceServer,
    };

    use crate::grpc_server::{AgentServiceImpl, NetworkServiceImpl, OciServiceImpl, SharedState};
    use crate::listener::VsockIncoming;

    let shared_state = SharedState {
        process_table: Arc::new(Mutex::new(ProcessTable::new())),
    };

    let agent_svc = AgentServiceServer::new(AgentServiceImpl::new(shared_state));
    let oci_svc = OciServiceServer::new(OciServiceImpl);
    let network_svc = NetworkServiceServer::new(NetworkServiceImpl);

    let incoming = VsockIncoming::new(StdArc::new(listener));

    info!("gRPC server starting");

    tonic::transport::Server::builder()
        .add_service(agent_svc)
        .add_service(oci_svc)
        .add_service(network_svc)
        .serve_with_incoming(incoming)
        .await
        .context("gRPC server failed")?;

    Ok(())
}

/// Spawn a process directly (as root / agent's user).
pub(crate) fn spawn_direct(
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
pub(crate) fn spawn_as_user(
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

/// Collect system information using sysctl and statfs.
pub(crate) fn collect_system_info() -> anyhow::Result<(u32, u64, u64, String)> {
    let cpu_count = get_sysctl_u32("hw.ncpu")?;
    let memory_bytes = get_sysctl_u64("hw.memsize")?;
    let disk_free_bytes = get_statfs_free("/")?;
    let os_version = get_os_version();
    Ok((cpu_count, memory_bytes, disk_free_bytes, os_version))
}

/// Collect resource usage statistics.
pub(crate) fn collect_resource_stats() -> anyhow::Result<ResourceStats> {
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
    std::process::Command::new("ps")
        .arg("-A")
        .arg("-o")
        .arg("pid=")
        .output()
        .ok()
        .map(|output| String::from_utf8_lossy(&output.stdout).lines().count() as u32)
        .unwrap_or(0)
}

pub(crate) async fn connect_port_forward_target(
    target_host: &str,
    target_port: u16,
) -> std::io::Result<tokio::net::TcpStream> {
    let started = Instant::now();
    let port_forward_connect_timeout = Duration::from_secs(5);
    let port_forward_connect_retry_interval = Duration::from_millis(100);

    loop {
        match tokio::net::TcpStream::connect((target_host, target_port)).await {
            Ok(stream) => return Ok(stream),
            Err(error) => {
                if started.elapsed() >= port_forward_connect_timeout {
                    return Err(error);
                }

                tokio::time::sleep(port_forward_connect_retry_interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_table_insert_remove() {
        let table = ProcessTable::new();
        assert!(table.is_empty());
        assert_eq!(table.len(), 0);
        assert!(table.get(1).is_none());
    }
}
