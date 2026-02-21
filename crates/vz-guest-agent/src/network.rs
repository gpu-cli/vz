//! Per-service network namespace isolation for stack VMs.
//!
//! Creates a bridge, per-service network namespaces, veth pairs, IP
//! addresses, and default routes using raw netlink sockets and libc
//! namespace syscalls. No external tools (`ip`, `brctl`) are required.
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

use std::ffi::CString;
use std::fs;
use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsRawFd, OwnedFd};
use std::path::Path;

use tracing::{debug, info};
use vz::protocol::NetworkServiceConfig;

/// Directory where named network namespaces are stored.
const NETNS_RUN_DIR: &str = "/var/run/netns";

/// Set up per-service network isolation for a stack.
///
/// 1. Creates bridge `br-<stack_id>` with gateway IP (first address in subnet)
/// 2. For each service: creates netns, veth pair, assigns IP, sets up routes
pub fn setup_stack_network(
    stack_id: &str,
    services: &[NetworkServiceConfig],
) -> io::Result<()> {
    if services.is_empty() {
        return Ok(());
    }

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
    create_bridge(&bridge_name)?;
    add_addr_to_iface(&bridge_name, bridge_ip, prefix_len)?;
    set_iface_up(&bridge_name)?;
    info!(bridge = %bridge_name, addr = %bridge_ip, "bridge created");

    // 2. Set up each service.
    fs::create_dir_all(NETNS_RUN_DIR)?;

    for svc in services {
        let (svc_ip, svc_prefix) = parse_cidr(&svc.addr)?;
        let veth_host = format!("veth-{}", truncate_name(&svc.name, 10));
        let veth_guest = "eth0".to_string();
        let ns_name = svc.name.clone();

        // Create network namespace.
        create_named_netns(&ns_name)?;

        // Create veth pair.
        create_veth_pair(&veth_host, &veth_guest)?;

        // Move guest end into the namespace.
        move_iface_to_netns(&veth_guest, &ns_name)?;

        // Attach host end to bridge and bring up.
        set_iface_master(&veth_host, &bridge_name)?;
        set_iface_up(&veth_host)?;

        // Configure inside the namespace.
        in_netns(&ns_name, || {
            set_iface_up("lo")?;
            add_addr_to_iface("eth0", svc_ip, svc_prefix)?;
            set_iface_up("eth0")?;
            add_default_route(bridge_ip)?;
            Ok(())
        })?;

        debug!(service = %svc.name, addr = %svc_ip, ns = %ns_name, "service network configured");
    }

    Ok(())
}

/// Tear down network resources for a stack.
pub fn teardown_stack_network(
    stack_id: &str,
    service_names: &[String],
) -> io::Result<()> {
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
    let _ = delete_iface(&bridge_name);

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
        io::Error::new(io::ErrorKind::InvalidInput, format!("missing prefix in '{addr}'"))
    })?;
    let ip: Ipv4Addr = ip_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let prefix: u8 = prefix_str
        .parse()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    Ok((ip, prefix))
}

// ── Netlink operations (raw libc) ──────────────────────────────────
//
// All netlink operations use AF_NETLINK / NETLINK_ROUTE sockets with
// hand-crafted messages. This avoids external crate dependencies.

/// Netlink message header + payload buffer.
struct NlMsg {
    buf: Vec<u8>,
}

// Netlink constants not in libc.
const RTM_NEWLINK: u16 = 16;
const RTM_DELLINK: u16 = 17;
const RTM_NEWADDR: u16 = 20;
const RTM_NEWROUTE: u16 = 24;
const NLM_F_REQUEST: u16 = 1;
const NLM_F_ACK: u16 = 4;
const NLM_F_CREATE: u16 = 0x400;
const NLM_F_EXCL: u16 = 0x200;
const IFLA_IFNAME: u16 = 3;
const IFLA_MASTER: u16 = 10;
const IFLA_LINKINFO: u16 = 18;
const IFLA_INFO_KIND: u16 = 1;
const IFLA_INFO_DATA: u16 = 2;
const VETH_INFO_PEER: u16 = 1;
const IFA_LOCAL: u16 = 2;
const IFA_ADDRESS: u16 = 1;
const RTA_GATEWAY: u16 = 5;
const RT_TABLE_MAIN: u8 = 254;
const RTPROT_BOOT: u8 = 3;
const RT_SCOPE_UNIVERSE: u8 = 0;
const RTN_UNICAST: u8 = 1;

impl NlMsg {
    fn new() -> Self {
        // Reserve space for nlmsghdr (16 bytes).
        Self {
            buf: vec![0u8; 16],
        }
    }

    fn set_type_flags(&mut self, msg_type: u16, flags: u16) {
        self.buf[4..6].copy_from_slice(&msg_type.to_ne_bytes());
        self.buf[6..8].copy_from_slice(&flags.to_ne_bytes());
    }

    /// Append an ifinfomsg (16 bytes).
    fn push_ifinfomsg(&mut self, family: u8, ifi_type: u16, ifi_index: i32, ifi_flags: u32, ifi_change: u32) {
        self.buf.push(family);
        self.buf.push(0); // padding
        self.buf.extend_from_slice(&ifi_type.to_ne_bytes());
        self.buf.extend_from_slice(&ifi_index.to_ne_bytes());
        self.buf.extend_from_slice(&ifi_flags.to_ne_bytes());
        self.buf.extend_from_slice(&ifi_change.to_ne_bytes());
    }

    /// Append an ifaddrmsg (8 bytes).
    fn push_ifaddrmsg(&mut self, family: u8, prefix_len: u8, flags: u8, scope: u8, index: u32) {
        self.buf.push(family);
        self.buf.push(prefix_len);
        self.buf.push(flags);
        self.buf.push(scope);
        self.buf.extend_from_slice(&index.to_ne_bytes());
    }

    /// Append an rtmsg (12 bytes).
    fn push_rtmsg(
        &mut self,
        family: u8,
        dst_len: u8,
        src_len: u8,
        tos: u8,
        table: u8,
        protocol: u8,
        scope: u8,
        rtype: u8,
        flags: u32,
    ) {
        self.buf.push(family);
        self.buf.push(dst_len);
        self.buf.push(src_len);
        self.buf.push(tos);
        self.buf.push(table);
        self.buf.push(protocol);
        self.buf.push(scope);
        self.buf.push(rtype);
        self.buf.extend_from_slice(&flags.to_ne_bytes());
    }

    /// Start a nested attribute.
    fn start_nested(&mut self, attr_type: u16) -> usize {
        let offset = self.buf.len();
        // Placeholder for nla_len (2 bytes) + nla_type (2 bytes).
        self.buf.extend_from_slice(&[0, 0]);
        self.buf.extend_from_slice(&attr_type.to_ne_bytes());
        offset
    }

    /// End a nested attribute, writing back the length.
    fn end_nested(&mut self, offset: usize) {
        let len = (self.buf.len() - offset) as u16;
        self.buf[offset..offset + 2].copy_from_slice(&len.to_ne_bytes());
    }

    /// Append a netlink attribute with string payload.
    fn push_attr_str(&mut self, attr_type: u16, value: &str) {
        let payload = value.as_bytes();
        let nla_len = 4 + payload.len() + 1; // +1 for NUL
        let padded = (nla_len + 3) & !3;
        self.buf.extend_from_slice(&(nla_len as u16).to_ne_bytes());
        self.buf.extend_from_slice(&attr_type.to_ne_bytes());
        self.buf.extend_from_slice(payload);
        self.buf.push(0); // NUL
        // Pad to 4-byte alignment.
        while self.buf.len() < self.buf.len() + (padded - nla_len) {
            self.buf.push(0);
        }
        let total_now = self.buf.len();
        let target = ((total_now + 3) / 4) * 4;
        self.buf.resize(target, 0);
    }

    /// Append a netlink attribute with raw bytes payload.
    fn push_attr_bytes(&mut self, attr_type: u16, value: &[u8]) {
        let nla_len = 4 + value.len();
        let padded = (nla_len + 3) & !3;
        self.buf.extend_from_slice(&(nla_len as u16).to_ne_bytes());
        self.buf.extend_from_slice(&attr_type.to_ne_bytes());
        self.buf.extend_from_slice(value);
        self.buf.resize(self.buf.len() + (padded - nla_len), 0);
    }

    /// Append a netlink attribute with a u32 payload.
    fn push_attr_u32(&mut self, attr_type: u16, value: u32) {
        self.push_attr_bytes(attr_type, &value.to_ne_bytes());
    }

    /// Finalize: write total length into nlmsghdr.
    fn finalize(&mut self) {
        let len = self.buf.len() as u32;
        self.buf[0..4].copy_from_slice(&len.to_ne_bytes());
    }
}

fn open_netlink_socket() -> io::Result<OwnedFd> {
    let fd = unsafe {
        libc::socket(
            libc::AF_NETLINK,
            libc::SOCK_RAW | libc::SOCK_CLOEXEC,
            libc::NETLINK_ROUTE,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }

    let mut addr: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
    addr.nl_family = libc::AF_NETLINK as u16;
    let ret = unsafe {
        libc::bind(
            fd,
            &addr as *const libc::sockaddr_nl as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_nl>() as u32,
        )
    };
    if ret < 0 {
        let err = io::Error::last_os_error();
        unsafe { libc::close(fd) };
        return Err(err);
    }

    Ok(unsafe { OwnedFd::from_raw_fd(fd) })
}

fn nl_send_and_ack(fd: &OwnedFd, msg: &NlMsg) -> io::Result<()> {
    let sent = unsafe {
        libc::send(
            fd.as_raw_fd(),
            msg.buf.as_ptr() as *const libc::c_void,
            msg.buf.len(),
            0,
        )
    };
    if sent < 0 {
        return Err(io::Error::last_os_error());
    }

    // Read ack.
    let mut buf = [0u8; 4096];
    let n = unsafe {
        libc::recv(fd.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0)
    };
    if n < 0 {
        return Err(io::Error::last_os_error());
    }

    // Check for NLMSG_ERROR.
    if n >= 16 {
        let msg_type = u16::from_ne_bytes([buf[4], buf[5]]);
        if msg_type == libc::NLMSG_ERROR as u16 && n >= 20 {
            let errno = i32::from_ne_bytes([buf[16], buf[17], buf[18], buf[19]]);
            if errno < 0 {
                return Err(io::Error::from_raw_os_error(-errno));
            }
        }
    }

    Ok(())
}

fn if_nametoindex(name: &str) -> io::Result<u32> {
    let c_name = CString::new(name).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let idx = unsafe { libc::if_nametoindex(c_name.as_ptr()) };
    if idx == 0 {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("interface '{name}' not found"),
        ))
    } else {
        Ok(idx)
    }
}

fn create_bridge(name: &str) -> io::Result<()> {
    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL);
    msg.push_ifinfomsg(0, 0, 0, 0, 0);
    msg.push_attr_str(IFLA_IFNAME, name);

    let nested = msg.start_nested(IFLA_LINKINFO);
    msg.push_attr_str(IFLA_INFO_KIND, "bridge");
    msg.end_nested(nested);

    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

fn create_veth_pair(name1: &str, name2: &str) -> io::Result<()> {
    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL);
    msg.push_ifinfomsg(0, 0, 0, 0, 0);
    msg.push_attr_str(IFLA_IFNAME, name1);

    let linkinfo = msg.start_nested(IFLA_LINKINFO);
    msg.push_attr_str(IFLA_INFO_KIND, "veth");

    let infodata = msg.start_nested(IFLA_INFO_DATA);
    let peer = msg.start_nested(VETH_INFO_PEER);
    // Peer ifinfomsg (16 bytes of zeros for defaults).
    msg.push_ifinfomsg(0, 0, 0, 0, 0);
    msg.push_attr_str(IFLA_IFNAME, name2);
    msg.end_nested(peer);
    msg.end_nested(infodata);

    msg.end_nested(linkinfo);
    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

fn set_iface_up(name: &str) -> io::Result<()> {
    let idx = if_nametoindex(name)?;
    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK);
    msg.push_ifinfomsg(
        0,
        0,
        idx as i32,
        libc::IFF_UP as u32,
        libc::IFF_UP as u32,
    );
    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

fn set_iface_master(iface: &str, bridge: &str) -> io::Result<()> {
    let iface_idx = if_nametoindex(iface)?;
    let bridge_idx = if_nametoindex(bridge)?;
    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK);
    msg.push_ifinfomsg(0, 0, iface_idx as i32, 0, 0);
    msg.push_attr_u32(IFLA_MASTER, bridge_idx);
    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

fn add_addr_to_iface(name: &str, addr: Ipv4Addr, prefix_len: u8) -> io::Result<()> {
    let idx = if_nametoindex(name)?;
    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_NEWADDR, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL);
    msg.push_ifaddrmsg(libc::AF_INET as u8, prefix_len, 0, 0, idx);
    msg.push_attr_bytes(IFA_LOCAL, &addr.octets());
    msg.push_attr_bytes(IFA_ADDRESS, &addr.octets());
    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

fn add_default_route(gateway: Ipv4Addr) -> io::Result<()> {
    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_NEWROUTE, NLM_F_REQUEST | NLM_F_ACK | NLM_F_CREATE | NLM_F_EXCL);
    msg.push_rtmsg(
        libc::AF_INET as u8,
        0,   // dst_len = 0 means default route
        0,
        0,
        RT_TABLE_MAIN,
        RTPROT_BOOT,
        RT_SCOPE_UNIVERSE,
        RTN_UNICAST,
        0,
    );
    msg.push_attr_bytes(RTA_GATEWAY, &gateway.octets());
    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

fn delete_iface(name: &str) -> io::Result<()> {
    let idx = match if_nametoindex(name) {
        Ok(idx) => idx,
        Err(_) => return Ok(()), // Already gone.
    };
    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_DELLINK, NLM_F_REQUEST | NLM_F_ACK);
    msg.push_ifinfomsg(0, 0, idx as i32, 0, 0);
    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

fn move_iface_to_netns(iface: &str, ns_name: &str) -> io::Result<()> {
    let iface_idx = if_nametoindex(iface)?;
    let ns_path = Path::new(NETNS_RUN_DIR).join(ns_name);
    let ns_fd = fs::File::open(&ns_path)?;

    let fd = open_netlink_socket()?;
    let mut msg = NlMsg::new();
    msg.set_type_flags(RTM_NEWLINK, NLM_F_REQUEST | NLM_F_ACK);
    msg.push_ifinfomsg(0, 0, iface_idx as i32, 0, 0);

    // IFLA_NET_NS_FD = 28
    const IFLA_NET_NS_FD: u16 = 28;
    msg.push_attr_u32(IFLA_NET_NS_FD, ns_fd.as_raw_fd() as u32);

    msg.finalize();
    nl_send_and_ack(&fd, &msg)
}

// ── Namespace operations ───────────────────────────────────────────

fn create_named_netns(name: &str) -> io::Result<()> {
    let ns_path = Path::new(NETNS_RUN_DIR).join(name);

    // Create the mount point file.
    fs::write(&ns_path, b"")?;

    // Create a new network namespace using clone/unshare.
    let ret = unsafe { libc::unshare(libc::CLONE_NEWNET) };
    if ret < 0 {
        let err = io::Error::last_os_error();
        let _ = fs::remove_file(&ns_path);
        return Err(err);
    }

    // Bind-mount the current netns to the named path so it persists.
    let source = CString::new("/proc/self/ns/net")
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let target = CString::new(ns_path.to_string_lossy().as_bytes())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    let ret = unsafe {
        libc::mount(
            source.as_ptr(),
            target.as_ptr(),
            std::ptr::null(),
            libc::MS_BIND,
            std::ptr::null(),
        )
    };
    if ret < 0 {
        let err = io::Error::last_os_error();
        let _ = fs::remove_file(&ns_path);
        return Err(err);
    }

    // Switch back to the original (default) network namespace.
    // We do this by opening /proc/1/ns/net (init's netns is always the default).
    let init_ns = fs::File::open("/proc/1/ns/net")?;
    let ret = unsafe { libc::setns(init_ns.as_raw_fd(), libc::CLONE_NEWNET) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

/// Execute a closure inside a named network namespace, then return to the
/// original namespace.
fn in_netns<F>(name: &str, f: F) -> io::Result<()>
where
    F: FnOnce() -> io::Result<()>,
{
    let ns_path = Path::new(NETNS_RUN_DIR).join(name);
    let target_ns = fs::File::open(&ns_path)?;
    let orig_ns = fs::File::open("/proc/self/ns/net")?;

    // Enter target namespace.
    let ret = unsafe { libc::setns(target_ns.as_raw_fd(), libc::CLONE_NEWNET) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    let result = f();

    // Return to original namespace.
    let ret = unsafe { libc::setns(orig_ns.as_raw_fd(), libc::CLONE_NEWNET) };
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }

    result
}

use std::os::fd::FromRawFd;

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
    fn nlmsg_finalize_sets_length() {
        let mut msg = NlMsg::new();
        msg.set_type_flags(RTM_NEWLINK, NLM_F_REQUEST);
        msg.push_ifinfomsg(0, 0, 0, 0, 0);
        msg.finalize();
        let len = u32::from_ne_bytes([msg.buf[0], msg.buf[1], msg.buf[2], msg.buf[3]]);
        assert_eq!(len as usize, msg.buf.len());
    }

    #[test]
    fn nlmsg_attr_str_alignment() {
        let mut msg = NlMsg::new();
        msg.set_type_flags(RTM_NEWLINK, NLM_F_REQUEST);
        msg.push_ifinfomsg(0, 0, 0, 0, 0);
        msg.push_attr_str(IFLA_IFNAME, "br0");
        msg.finalize();
        // Total length should be 4-byte aligned.
        assert_eq!(msg.buf.len() % 4, 0);
    }
}
