//! Per-service network namespace isolation for stack VMs.
//!
//! Creates one bridge per logical network, per-service network namespaces,
//! veth pairs, IP addresses, and default routes using busybox commands.
//!
//! # Network topology (multi-network)
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │ VM (shared for stack)                                           │
//! │                                                                 │
//! │  br-<stack>-frontend (172.20.0.1/24)                           │
//! │     ├── veth-web-0 ←──→ [netns: web] eth0 (172.20.0.2/24)    │
//! │     └── veth-api-0 ←──→ [netns: api] eth0 (172.20.0.3/24)    │
//! │                                                                 │
//! │  br-<stack>-backend  (172.20.1.1/24)                           │
//! │     ├── veth-api-1 ←──→ [netns: api] eth1 (172.20.1.2/24)    │
//! │     └── veth-db-0  ←──→ [netns: db]  eth0 (172.20.1.3/24)    │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! Services belonging to multiple networks get multiple interfaces
//! (eth0, eth1, ...). The default route goes through the first bridge.
//!
//! # Implementation notes
//!
//! BusyBox `ip` does not support the `netns` subcommand. Instead, we:
//! 1. Create named network namespaces via `unshare(2)` + bind mount
//!    (simple syscalls — no netlink, no memory risk)
//! 2. Use `nsenter --net=<path>` to run `ip` commands inside namespaces
//! 3. Move veth endpoints via `ip link set <dev> netns <path>`

use std::collections::{HashMap, HashSet};
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
/// 1. Groups services by `network_name`
/// 2. Creates one bridge per network: `br-<stack_id>-<network_name>`
/// 3. Creates netns per unique service (once, even for multi-network services)
/// 4. For each (network, service) pair: creates veth, assigns IP, sets up route
pub fn setup_stack_network(stack_id: &str, services: &[NetworkServiceConfig]) -> io::Result<()> {
    if services.is_empty() {
        return Ok(());
    }

    info!(stack_id = %stack_id, services = services.len(), "setup_stack_network: starting");

    // ── Group services by network ───────────────────────────────────
    // Preserve insertion order by collecting distinct network names in order.
    let mut network_order: Vec<String> = Vec::new();
    let mut networks: HashMap<String, Vec<&NetworkServiceConfig>> = HashMap::new();
    for svc in services {
        if !networks.contains_key(&svc.network_name) {
            network_order.push(svc.network_name.clone());
        }
        networks
            .entry(svc.network_name.clone())
            .or_default()
            .push(svc);
    }

    fs::create_dir_all(NETNS_RUN_DIR)?;

    // ── Create network namespaces (one per unique service) ──────────
    let mut created_ns: HashSet<String> = HashSet::new();
    for svc in services {
        if created_ns.insert(svc.name.clone()) {
            info!(service = %svc.name, "creating netns");
            create_named_netns(&svc.name)?;
        }
    }

    // Track how many interfaces each service has already been given.
    // This determines the ethN index inside each netns.
    let mut service_iface_count: HashMap<String, u32> = HashMap::new();

    // Track whether each service has a default route yet.
    let mut has_default_route: HashSet<String> = HashSet::new();

    // ── Per-network: create bridge + attach services ────────────────
    for net_name in &network_order {
        let net_services = &networks[net_name];
        if net_services.is_empty() {
            continue;
        }

        // Derive bridge address from first service's subnet (use .1).
        let first_addr = parse_cidr(&net_services[0].addr)?;
        let bridge_ip = Ipv4Addr::new(
            first_addr.0.octets()[0],
            first_addr.0.octets()[1],
            first_addr.0.octets()[2],
            1,
        );
        let prefix_len = first_addr.1;

        let bridge_name = format!(
            "br-{}-{}",
            truncate_name(stack_id, 8),
            truncate_name(net_name, 8)
        );

        // Create bridge.
        info!(bridge = %bridge_name, network = %net_name, "creating bridge");
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

        // Attach each service to this bridge.
        for svc in net_services {
            let (svc_ip, svc_prefix) = parse_cidr(&svc.addr)?;
            let iface_idx = service_iface_count.entry(svc.name.clone()).or_insert(0);
            let eth_name = format!("eth{iface_idx}");

            // Unique veth host-end name: veth-<svc>-<idx>.
            let veth_host = format!("veth-{}-{}", truncate_name(&svc.name, 8), iface_idx);
            let ns_path = format!("{NETNS_RUN_DIR}/{}", svc.name);

            // Create veth pair inside the namespace, then move host end out.
            info!(
                service = %svc.name,
                network = %net_name,
                host = %veth_host,
                iface = %eth_name,
                "creating veth pair"
            );
            nsenter_ip(
                &ns_path,
                &[
                    "link", "add", &veth_host, "type", "veth", "peer", "name", "veth_tmp",
                ],
            )?;

            // Move host end from netns to default namespace (PID 1's netns).
            nsenter_ip(&ns_path, &["link", "set", &veth_host, "netns", "1"])?;

            // Attach host end to bridge and bring up (in default namespace).
            ip_run(&["link", "set", &veth_host, "master", &bridge_name])?;
            ip_run(&["link", "set", &veth_host, "up"])?;

            // Configure inside the namespace.
            if *iface_idx == 0 {
                nsenter_ip(&ns_path, &["link", "set", "lo", "up"])?;
            }

            // Rename peer end to ethN.
            nsenter_ip(&ns_path, &["link", "set", "veth_tmp", "name", &eth_name])?;

            nsenter_ip(
                &ns_path,
                &[
                    "addr",
                    "add",
                    &format!("{svc_ip}/{svc_prefix}"),
                    "dev",
                    &eth_name,
                ],
            )?;
            nsenter_ip(&ns_path, &["link", "set", &eth_name, "up"])?;

            // Only add default route once (first network wins).
            if has_default_route.insert(svc.name.clone()) {
                nsenter_ip(
                    &ns_path,
                    &["route", "add", "default", "via", &bridge_ip.to_string()],
                )?;
            }

            *iface_idx += 1;

            info!(
                service = %svc.name,
                network = %net_name,
                addr = %svc_ip,
                iface = %eth_name,
                "service network configured"
            );
        }
    }

    info!(stack_id = %stack_id, "setup_stack_network: complete");
    Ok(())
}

/// Tear down network resources for a stack.
///
/// Removes per-service network namespaces and all bridges created for the
/// stack (one per network, named `br-<stack_id>-<network_name>`).
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

    // Delete all bridges matching the br-<stack_id>-* pattern.
    // We enumerate by listing interfaces; alternatively, just try the
    // well-known prefix. Since bridge names are truncated, list all
    // interfaces and delete those matching our prefix.
    let prefix = format!("br-{}-", truncate_name(stack_id, 8));
    if let Ok(output) = Command::new(IP_BIN)
        .args(["link", "show", "type", "bridge"])
        .output()
    {
        let stdout = String::from_utf8_lossy(&output.stdout);
        for line in stdout.lines() {
            // Lines look like: "5: br-mystack-default: <BROADCAST,..."
            if let Some(name_part) = line.split(':').nth(1) {
                let bridge = name_part.trim();
                if bridge.starts_with(&prefix) {
                    let _ = ip_run(&["link", "del", bridge]);
                }
            }
        }
    }

    // Fallback: also try the legacy single-bridge name for backwards compat.
    let legacy_bridge = format!("br-{}", truncate_name(stack_id, 12));
    let _ = ip_run(&["link", "del", &legacy_bridge]);

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
                format!("create_named_netns({name}) failed: child exited with code {exit_code}"),
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
    let output = Command::new(IP_BIN).args(args).output().map_err(|e| {
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
