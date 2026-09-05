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
use std::os::fd::{AsRawFd, FromRawFd};
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use tracing::info;
use vz::protocol::NetworkServiceConfig;

use crate::network_holder::{
    NamespaceHolderOps, complete_namespace_handoff, prepare_namespace_holder, retry_waitpid,
};

/// Directory where named network namespaces are stored.
const NETNS_RUN_DIR: &str = "/var/run/netns";
const SYS_CLASS_NET: &str = "/sys/class/net";

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

        // Linux IFNAMSIZ is 16 (15 usable chars). Format: "br-{stack}-{net}"
        // overhead = 4 chars ("br-" + "-"), leaving 11 for stack + net.
        let bridge_name = format!(
            "br-{}-{}",
            truncate_name(stack_id, 5),
            truncate_name(net_name, 5)
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
            // Linux IFNAMSIZ is 16 (15 usable chars). Format: "ve-{svc}-{idx}"
            // overhead = 4 chars ("ve-" + "-"), leaving 11 for svc + idx digit(s).
            let veth_host = format!("ve-{}-{}", truncate_name(&svc.name, 9), iface_idx);
            let ns_path = format!("{NETNS_RUN_DIR}/{}", svc.name);

            // Create veth pair in root namespace, then move the peer end
            // into the service namespace. BusyBox `ip` ignores `peer name`,
            // so we find the auto-generated peer by parsing `ip link show`.
            info!(
                service = %svc.name,
                network = %net_name,
                host = %veth_host,
                iface = %eth_name,
                "creating veth pair"
            );
            ip_run(&["link", "add", &veth_host, "type", "veth"])?;

            // Find the auto-generated peer name (shown as "peerN@veth_host").
            let veth_guest = find_veth_peer(&veth_host)?;
            info!(peer = %veth_guest, "found veth peer");

            // Move peer end into the service namespace.
            move_link_to_netns(&veth_guest, &ns_path)?;

            // Attach host end to bridge and bring up (in default namespace).
            ip_run(&["link", "set", &veth_host, "master", &bridge_name])?;
            ip_run(&["link", "set", &veth_host, "up"])?;

            // Configure inside the namespace.
            if *iface_idx == 0 {
                nsenter_ip(&ns_path, &["link", "set", "lo", "up"])?;
            }

            // Rename guest end to ethN inside the namespace.
            // This can race briefly with netns handoff; retry on transient
            // kernel/iproute2 "No such device" errors to reduce flakiness.
            rename_netns_interface_with_retry(&ns_path, &veth_guest, &eth_name)?;

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
/// Removes per-service network namespaces, their host veth ends, and all
/// bridges created for the stack (one per network, named
/// `br-<stack_id>-<network_name>`).
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

    // Do not rely on destruction of an unmounted namespace to synchronously
    // remove veth pairs. Delete only agent-generated host ends for the exact
    // supplied services (`ve-<truncated-service>-<numeric-index>`).
    let veths = discover_service_veths(Path::new(SYS_CLASS_NET), service_names)?;

    // BusyBox `ip link show type bridge` is not a reliable enumeration API.
    // sysfs provides both the complete interface set and an unambiguous
    // bridge marker at `<interface>/bridge`. Restrict deletion to this stack's
    // current prefix or exact legacy name, and verify the interface is really
    // a bridge before invoking `ip link del`.
    let bridges = discover_stack_bridges(Path::new(SYS_CLASS_NET), stack_id)?;
    let mut first_error = None;
    for veth in veths {
        info!(stack_id = %stack_id, veth = %veth, "deleting stack service veth");
        if let Err(error) = delete_discovered_link(&veth) {
            first_error.get_or_insert(error);
        }
    }
    for bridge in bridges {
        info!(stack_id = %stack_id, bridge = %bridge, "deleting stack bridge");
        if let Err(error) = delete_discovered_link(&bridge) {
            first_error.get_or_insert(error);
        }
    }
    if let Some(error) = first_error {
        return Err(error);
    }

    Ok(())
}

/// Delete a link discovered in sysfs, tolerating only a confirmed concurrent
/// disappearance (for example when namespace destruction removes a veth).
fn delete_discovered_link(name: &str) -> io::Result<()> {
    let delete_result = ip_run(&["link", "del", name]);
    let existence_result = Path::new(SYS_CLASS_NET).join(name).try_exists();
    normalize_link_delete_result(name, delete_result, existence_result)
}

fn normalize_link_delete_result(
    name: &str,
    delete_result: io::Result<()>,
    existence_result: io::Result<bool>,
) -> io::Result<()> {
    match (delete_result, existence_result) {
        (Ok(()), _) | (Err(_), Ok(false)) => Ok(()),
        (Err(delete_error), Ok(true)) => Err(delete_error),
        (Err(delete_error), Err(existence_error)) => Err(io::Error::new(
            delete_error.kind(),
            format!(
                "failed to delete network link `{name}`: {delete_error}; cannot confirm link absence: {existence_error}"
            ),
        )),
    }
}

// ── Helpers ────────────────────────────────────────────────────────

fn truncate_name(name: &str, max_len: usize) -> &str {
    if name.len() > max_len {
        &name[..max_len]
    } else {
        name
    }
}

/// Discover agent-generated host veth ends for the supplied services only.
fn discover_service_veths(
    net_class_root: &Path,
    service_names: &[String],
) -> io::Result<Vec<String>> {
    let prefixes = service_names
        .iter()
        .map(|name| format!("ve-{}-", truncate_name(name, 9)))
        .collect::<HashSet<_>>();
    let mut veths = Vec::new();

    for entry in fs::read_dir(net_class_root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if prefixes.iter().any(|prefix| {
            name.strip_prefix(prefix).is_some_and(|index| {
                !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit())
            })
        }) {
            veths.push(name);
        }
    }
    veths.sort_unstable();
    Ok(veths)
}

/// Discover only real bridge interfaces belonging to one stack.
fn discover_stack_bridges(net_class_root: &Path, stack_id: &str) -> io::Result<Vec<String>> {
    let prefix = format!("br-{}-", truncate_name(stack_id, 5));
    let legacy = format!("br-{}", truncate_name(stack_id, 12));
    let mut bridges = Vec::new();

    for entry in fs::read_dir(net_class_root)? {
        let entry = entry?;
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if (name.starts_with(&prefix) || name == legacy) && entry.path().join("bridge").is_dir() {
            bridges.push(name);
        }
    }
    bridges.sort_unstable();
    Ok(bridges)
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
            return Err(io::Error::other(format!(
                "create_named_netns({name}) failed: child exited with code {exit_code}"
            )));
        }
    }

    Ok(())
}

/// Find the peer end of a veth pair by parsing `ip link show`.
///
/// BusyBox displays veth pairs as `N: peername@hostname: ...`.
/// We look for any interface whose `@suffix` matches the given host name.
fn find_veth_peer(host_name: &str) -> io::Result<String> {
    let output = Command::new(IP_BIN)
        .args(["link", "show"])
        .output()
        .map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("failed to exec `{IP_BIN} link show`: {e}"),
            )
        })?;

    let stdout = String::from_utf8_lossy(&output.stdout);
    let suffix = format!("@{host_name}");

    for line in stdout.lines() {
        // Lines: "5: veth0@ve-postgres-0: <BROADCAST,..."
        if let Some(name_part) = line.split(':').nth(1) {
            let name = name_part.trim();
            if let Some(peer) = name.strip_suffix(&suffix) {
                return Ok(peer.to_string());
            }
        }
    }

    Err(io::Error::new(
        io::ErrorKind::NotFound,
        format!("no veth peer found for '{host_name}'"),
    ))
}

/// Move a network interface into a named network namespace.
///
/// Uses `ip link set <dev> netns <pid>` by forking a child process
/// that enters the target namespace and waits while the parent moves
/// the interface using the child's PID.
fn move_link_to_netns(dev: &str, ns_path: &str) -> io::Result<()> {
    let ns_file = fs::File::open(ns_path)?;
    let ns_fd = ns_file.as_raw_fd();
    let mut ready_pipe = [0; 2];
    if unsafe { libc::pipe2(ready_pipe.as_mut_ptr(), libc::O_CLOEXEC) } != 0 {
        return Err(io::Error::last_os_error());
    }

    // Fork a child that enters the target namespace and signals readiness.
    // Waiting for that byte is essential: without it, the parent can invoke BusyBox ip
    // before the child completes setns(), producing a successful no-op move
    // into the root namespace and an irrecoverable in-namespace rename failure.
    unsafe {
        let mut holder_ops = LinuxNamespaceHolderOps;
        let pid = libc::fork();
        if pid < 0 {
            let error = io::Error::last_os_error();
            libc::close(ready_pipe[0]);
            libc::close(ready_pipe[1]);
            return Err(error);
        }

        if pid == 0 {
            libc::close(ready_pipe[0]);
            if let Err(failure) = prepare_namespace_holder(&mut holder_ops, ns_fd, ready_pipe[1]) {
                libc::_exit(failure.exit_code());
            }
            libc::close(ready_pipe[1]);
            libc::alarm(30);
            loop {
                libc::pause();
            }
        }

        libc::close(ready_pipe[1]);
        let mut ready_reader = fs::File::from_raw_fd(ready_pipe[0]);
        complete_namespace_handoff(&mut holder_ops, &mut ready_reader, dev, ns_path, pid)
    }
}

struct LinuxNamespaceHolderOps;

impl NamespaceHolderOps for LinuxNamespaceHolderOps {
    fn setns(&mut self, ns_fd: std::os::fd::RawFd) -> bool {
        unsafe { libc::setns(ns_fd, libc::CLONE_NEWNET) == 0 }
    }

    fn signal_ready(&mut self, ready_fd: std::os::fd::RawFd) -> bool {
        let ready = [1_u8];
        loop {
            let written = unsafe { libc::write(ready_fd, ready.as_ptr().cast(), ready.len()) };
            if written == 1 {
                return true;
            }
            if written < 0 && unsafe { *libc::__errno_location() } == libc::EINTR {
                continue;
            }
            return false;
        }
    }

    fn move_link(&mut self, dev: &str, pid: libc::pid_t) -> io::Result<()> {
        let pid_string = pid.to_string();
        ip_run(&["link", "set", dev, "netns", &pid_string])
    }

    fn terminate(&mut self, pid: libc::pid_t) -> io::Result<()> {
        if unsafe { libc::kill(pid, libc::SIGKILL) } != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn reap(&mut self, pid: libc::pid_t) -> io::Result<()> {
        retry_waitpid(pid, |pid| {
            let mut status: libc::c_int = 0;
            let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
            if waited < 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(waited)
            }
        })
    }
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
        return Err(io::Error::other(format!(
            "{IP_BIN} {} failed: {}",
            args.join(" "),
            stderr.trim()
        )));
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
        return Err(io::Error::other(format!(
            "{IP_BIN} {} in netns {} failed: {}",
            args.join(" "),
            ns_path,
            stderr.trim()
        )));
    }
    Ok(())
}

fn rename_netns_interface_with_retry(ns_path: &str, from: &str, to: &str) -> io::Result<()> {
    const ATTEMPTS: u32 = 8;
    const RETRY_DELAY: Duration = Duration::from_millis(75);

    let mut last_error: Option<io::Error> = None;
    for attempt in 1..=ATTEMPTS {
        match nsenter_ip(ns_path, &["link", "set", from, "name", to]) {
            Ok(()) => return Ok(()),
            Err(error) => {
                let transient = is_transient_link_rename_error(&error);
                if !transient || attempt == ATTEMPTS {
                    return Err(io::Error::new(
                        error.kind(),
                        format!(
                            "failed to rename netns interface `{from}` -> `{to}` in {ns_path} after {attempt} attempt(s): {error}"
                        ),
                    ));
                }
                last_error = Some(error);
                std::thread::sleep(RETRY_DELAY);
            }
        }
    }

    if let Some(error) = last_error {
        Err(io::Error::new(
            error.kind(),
            format!("failed to rename netns interface `{from}` -> `{to}` in {ns_path}: {error}"),
        ))
    } else {
        Err(io::Error::other(format!(
            "failed to rename netns interface `{from}` -> `{to}` in {ns_path}"
        )))
    }
}

fn is_transient_link_rename_error(error: &io::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("siocsifname")
        || message.contains("no such device")
        || message.contains("cannot find device")
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

    #[test]
    fn bridge_discovery_uses_busybox_independent_sysfs_type_marker() {
        let net = tempfile::tempdir().unwrap();
        for name in [
            "br-id-ow-defau",
            "br-id-ow-backe",
            "br-other-defau",
            "br-id-owner-sta",
            "ve-web-0",
        ] {
            fs::create_dir(net.path().join(name)).unwrap();
        }
        for name in ["br-id-ow-defau", "br-id-ow-backe", "br-id-owner-sta"] {
            fs::create_dir(net.path().join(name).join("bridge")).unwrap();
        }

        assert_eq!(
            discover_stack_bridges(net.path(), "id-owner-stack").unwrap(),
            ["br-id-ow-backe", "br-id-ow-defau", "br-id-owner-sta"]
        );
    }

    #[test]
    fn bridge_discovery_never_selects_prefix_matching_non_bridge() {
        let net = tempfile::tempdir().unwrap();
        fs::create_dir(net.path().join("br-id-ow-fake")).unwrap();
        fs::create_dir_all(net.path().join("br-other-real").join("bridge")).unwrap();

        assert!(
            discover_stack_bridges(net.path(), "id-owner-stack")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn service_veth_discovery_selects_every_numeric_index_for_supplied_services() {
        let net = tempfile::tempdir().unwrap();
        for name in [
            "ve-owner-0",
            "ve-owner-12",
            "ve-owner-x",
            "ve-owner-",
            "ve-other-0",
            "ve-owner-ser-3",
        ] {
            fs::create_dir(net.path().join(name)).unwrap();
        }
        let services = vec!["owner".to_string(), "owner-service".to_string()];

        assert_eq!(
            discover_service_veths(net.path(), &services).unwrap(),
            ["ve-owner-0", "ve-owner-12", "ve-owner-ser-3"]
        );
    }

    #[test]
    fn service_veth_discovery_is_empty_without_exact_service_prefixes() {
        let net = tempfile::tempdir().unwrap();
        for name in ["ve-owner-0", "ve-api-old-1", "br-id-ow-defau"] {
            fs::create_dir(net.path().join(name)).unwrap();
        }

        assert!(
            discover_service_veths(net.path(), &["api".to_string()])
                .unwrap()
                .is_empty()
        );
        assert!(discover_service_veths(net.path(), &[]).unwrap().is_empty());
    }

    #[test]
    fn link_delete_error_is_idempotent_only_after_confirmed_absence() {
        assert!(
            normalize_link_delete_result(
                "ve-owner-0",
                Err(io::Error::other("cannot find device")),
                Ok(false),
            )
            .is_ok()
        );

        let still_present = normalize_link_delete_result(
            "ve-owner-0",
            Err(io::Error::other("delete failed")),
            Ok(true),
        )
        .unwrap_err();
        assert!(still_present.to_string().contains("delete failed"));

        let unconfirmed = normalize_link_delete_result(
            "ve-owner-0",
            Err(io::Error::other("delete failed")),
            Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "sysfs denied",
            )),
        )
        .unwrap_err();
        assert!(
            unconfirmed
                .to_string()
                .contains("cannot confirm link absence")
        );

        assert!(
            normalize_link_delete_result(
                "ve-owner-0",
                Ok(()),
                Err(io::Error::other("ignored after successful delete")),
            )
            .is_ok()
        );
    }

    #[test]
    fn transient_link_rename_error_detection_matches_kernel_message() {
        let err = io::Error::other("ip: SIOCSIFNAME: No such device");
        assert!(is_transient_link_rename_error(&err));
    }

    #[test]
    fn transient_link_rename_error_detection_rejects_unrelated_messages() {
        let err = io::Error::other("permission denied");
        assert!(!is_transient_link_rename_error(&err));
    }
}
