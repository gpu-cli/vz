//! Per-service network namespace isolation for stack VMs.
//!
//! Creates a bridge, per-service network namespaces, veth pairs, IP
//! addresses, and default routes using busybox commands.
//!
//! # Network topology
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │ VM (shared for stack)                                           │
//! │                                                                 │
//! │  br-<stack> (172.20.0.1/24)                                    │
//! │     │                                                           │
//! │     ├── veth-web ←──→ [netns: web] eth0 (172.20.0.2/24)       │
//! │     ├── veth-db  ←──→ [netns: db]  eth0 (172.20.0.3/24)       │
//! │     └── ...                                                     │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Implementation notes
//!
//! BusyBox `ip` does not support the `netns` subcommand. Instead, we:
//! 1. Create named network namespaces via `unshare(2)` + bind mount
//!    (simple syscalls — no netlink, no memory risk)
//! 2. Use `nsenter --net=<path>` to run `ip` commands inside namespaces
//! 3. Move veth endpoints via `ip link set <dev> netns <path>`

use std::ffi::CString;
use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::os::fd::AsRawFd;
use std::path::Path;
use std::process::Command;

use tracing::info;
use vz::protocol::NetworkServiceConfig;

/// Directory where named network namespaces are stored.
const NETNS_RUN_DIR: &str = "/var/run/netns";

/// Set up per-service network isolation for a stack.
///
/// 1. Creates bridge `br-<stack_id>` with gateway IP (first address in subnet)
/// 2. For each service: creates netns, veth pair, assigns IP, sets up routes
pub fn setup_stack_network(stack_id: &str, services: &[NetworkServiceConfig]) -> io::Result<()> {
    if services.is_empty() {
        return Ok(());
    }

    info!(stack_id = %stack_id, services = services.len(), "setup_stack_network: starting");

    // Derive bridge address from first service's subnet (use .1).
    let first_addr = parse_cidr(&services[0].addr)?;
    let bridge_ip = Ipv4Addr::new(
        first_addr.0.octets()[0],
        first_addr.0.octets()[1],
        first_addr.0.octets()[2],
        1,
    );
    let prefix_len = first_addr.1;

    let bridge_name = format!("br-{}", truncate_name(stack_id, 12));

    // 1. Create bridge.
    info!(bridge = %bridge_name, "creating bridge");
    ip_run(&["link", "add", "name", &bridge_name, "type", "bridge"])?;
    ip_run(&[
        "addr",
        "add",
        &format!("{bridge_ip}/{prefix_len}"),
        "dev",
        &bridge_name,
    ])?;
    ip_run(&["link", "set", &bridge_name, "up"])?;
    info!(bridge = %bridge_name, addr = %bridge_ip, "bridge created");

    // 2. Set up each service.
    fs::create_dir_all(NETNS_RUN_DIR)?;

    for svc in services {
        let (svc_ip, svc_prefix) = parse_cidr(&svc.addr)?;
        let veth_host = format!("veth-{}", truncate_name(&svc.name, 10));
        let ns_name = &svc.name;
        let ns_path = format!("{NETNS_RUN_DIR}/{ns_name}");

        // Create network namespace via unshare(2) + bind mount.
        info!(service = %svc.name, "creating netns");
        create_named_netns(ns_name)?;

        // Create veth pair inside the namespace, then move host end out.
        // This avoids needing `ip link set ... netns <path>` which BusyBox
        // doesn't support (it only accepts PIDs).
        //
        // BusyBox ip ignores `peer name X` — it auto-names the peer as vethN.
        // We create the pair, then rename the peer to eth0 after moving the
        // host end out.
        info!(service = %svc.name, host = %veth_host, "creating veth pair");
        nsenter_ip(&ns_path, &[
            "link", "add", &veth_host, "type", "veth", "peer", "name", "veth0",
        ])?;

        // Move host end from netns to default namespace (PID 1's netns).
        nsenter_ip(&ns_path, &["link", "set", &veth_host, "netns", "1"])?;

        // Attach host end to bridge and bring up (in default namespace).
        ip_run(&["link", "set", &veth_host, "master", &bridge_name])?;
        ip_run(&["link", "set", &veth_host, "up"])?;

        // Configure inside the namespace.
        info!(service = %svc.name, "configuring namespace networking");
        nsenter_ip(&ns_path, &["link", "set", "lo", "up"])?;

        // Rename peer end to eth0.
        nsenter_ip(&ns_path, &["link", "set", "veth0", "name", "eth0"])?;

        nsenter_ip(
            &ns_path,
            &[
                "addr",
                "add",
                &format!("{svc_ip}/{svc_prefix}"),
                "dev",
                "eth0",
            ],
        )?;
        nsenter_ip(&ns_path, &["link", "set", "eth0", "up"])?;
        nsenter_ip(
            &ns_path,
            &["route", "add", "default", "via", &bridge_ip.to_string()],
        )?;

        info!(service = %svc.name, addr = %svc_ip, ns = %ns_name, "service network configured");
    }

    info!(stack_id = %stack_id, "setup_stack_network: complete");
    Ok(())
}

/// Tear down network resources for a stack.
pub fn teardown_stack_network(stack_id: &str, service_names: &[String]) -> io::Result<()> {
    // Remove network namespaces (deletes veth pairs automatically).
    for name in service_names {
        let ns_path = Path::new(NETNS_RUN_DIR).join(name);
        if ns_path.exists() {
            // Unmount and remove.
            let path_c = CString::new(ns_path.to_string_lossy().as_bytes())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            unsafe {
                libc::umount2(path_c.as_ptr(), libc::MNT_DETACH);
            }
            let _ = fs::remove_file(&ns_path);
        }
    }

    // Delete the bridge (also removes attached veth host ends).
    let bridge_name = format!("br-{}", truncate_name(stack_id, 12));
    let _ = ip_run(&["link", "del", &bridge_name]);

    Ok(())
}

// ── Helpers ────────────────────────────────────────────────────────

fn truncate_name(name: &str, max_len: usize) -> &str {
    if name.len() > max_len {
        &name[..max_len]
    } else {
        name
    }
}

fn parse_cidr(addr: &str) -> io::Result<(Ipv4Addr, u8)> {
    let (ip_str, prefix_str) = addr.split_once('/').ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing prefix in '{addr}'"),
        )
    })?;
    let ip: Ipv4Addr = ip_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let prefix: u8 = prefix_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    Ok((ip, prefix))
}

// ── Network namespace creation via syscalls ────────────────────────
//
// BusyBox `ip` doesn't support `ip netns`. Instead, we create named
// network namespaces using unshare(2) + bind mount, matching the
// iproute2 convention at /var/run/netns/<name>.

/// Create a named network namespace at `/var/run/netns/<name>`.
///
/// Uses fork + unshare(CLONE_NEWNET) + bind mount to create a persistent
/// named netns without requiring `ip netns` support.
fn create_named_netns(name: &str) -> io::Result<()> {
    let ns_path = format!("{NETNS_RUN_DIR}/{name}");
    let ns_path_obj = Path::new(&ns_path);

    // Create the bind mount target file.
    if !ns_path_obj.exists() {
        fs::write(&ns_path, b"")?;
    }

    // Fork a child that will unshare into a new network namespace,
    // then bind-mount its /proc/self/ns/net onto our target path.
    let ns_path_c = CString::new(ns_path.as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let proc_ns_net = CString::new("/proc/self/ns/net")
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

    // Safety: unshare and mount are standard POSIX-like syscalls.
    // We fork to isolate the new namespace from the agent process.
    unsafe {
        let pid = libc::fork();
        if pid < 0 {
            return Err(io::Error::last_os_error());
        }

        if pid == 0 {
            // Child process: create new netns and bind-mount it.
            if libc::unshare(libc::CLONE_NEWNET) != 0 {
                libc::_exit(1);
            }
            if libc::mount(
                proc_ns_net.as_ptr(),
                ns_path_c.as_ptr(),
                std::ptr::null(),
                libc::MS_BIND,
                std::ptr::null(),
            ) != 0
            {
                libc::_exit(2);
            }
            libc::_exit(0);
        }

        // Parent: wait for child.
        let mut status: libc::c_int = 0;
        if libc::waitpid(pid, &mut status, 0) < 0 {
            return Err(io::Error::last_os_error());
        }

        if !libc::WIFEXITED(status) || libc::WEXITSTATUS(status) != 0 {
            let exit_code = if libc::WIFEXITED(status) {
                libc::WEXITSTATUS(status)
            } else {
                -1
            };
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "create_named_netns({name}) failed: child exited with code {exit_code}"
                ),
            ));
        }
    }

    Ok(())
}

// ── Command-based network operations ───────────────────────────────
//
// Uses busybox `ip` for link/addr/route operations. Namespace entry
// is done via `nsenter` since BusyBox `ip` lacks `netns` support.
//
// This delegates netlink operations to child processes, avoiding a
// kernel OOM deadlock that occurs when the guest agent triggers
// a page fault that the OOM killer can't resolve by killing PID 1.

/// Absolute path to BusyBox `ip` inside the chroot.
///
/// We use an absolute path because Rust's `Command::new("ip")` resolves
/// the binary using the *parent* process's PATH, not the child's `.env("PATH", ...)`.
/// The init script creates `/bin/ip` as a symlink to `/bin/busybox`.
const IP_BIN: &str = "/bin/ip";

/// Run `ip <args>` and check for success.
fn ip_run(args: &[&str]) -> io::Result<()> {
    let output = Command::new(IP_BIN)
        .args(args)
        .output()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("failed to exec `{IP_BIN} {}`: {}", args.join(" "), e),
            )
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!("{IP_BIN} {} failed: {}", args.join(" "), stderr.trim()),
        ));
    }
    Ok(())
}

/// Run `ip <args>` inside a network namespace.
///
/// Uses `pre_exec` with `setns(2)` to enter the namespace before execing ip.
/// BusyBox nsenter doesn't support `--net=<path>`, and BusyBox ip
/// doesn't support `ip netns exec`, so we do it via pre_exec hook.
fn nsenter_ip(ns_path: &str, args: &[&str]) -> io::Result<()> {
    use std::os::unix::process::CommandExt;

    // Open the namespace fd.
    let ns_file = fs::File::open(ns_path)?;
    let ns_fd = ns_file.as_raw_fd();

    let output = unsafe {
        Command::new(IP_BIN)
            .args(args)
            .pre_exec(move || {
                if libc::setns(ns_fd, libc::CLONE_NEWNET) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            })
            .output()
    }
    .map_err(|e| {
        io::Error::new(
            e.kind(),
            format!(
                "failed to exec `{IP_BIN} {}` in netns {}: {}",
                args.join(" "),
                ns_path,
                e
            ),
        )
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::new(
            io::ErrorKind::Other,
            format!(
                "{IP_BIN} {} in netns {} failed: {}",
                args.join(" "),
                ns_path,
                stderr.trim()
            ),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_cidr_valid() {
        let (ip, prefix) = parse_cidr("172.20.0.2/24").unwrap();
        assert_eq!(ip, Ipv4Addr::new(172, 20, 0, 2));
        assert_eq!(prefix, 24);
    }

    #[test]
    fn parse_cidr_missing_prefix() {
        assert!(parse_cidr("192.168.1.1").is_err());
    }

    #[test]
    fn truncate_name_short() {
        assert_eq!(truncate_name("web", 12), "web");
    }

    #[test]
    fn truncate_name_long() {
        assert_eq!(truncate_name("very-long-stack-name", 12), "very-long-st");
    }
}
