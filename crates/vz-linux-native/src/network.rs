//! Host networking primitives for Linux-native container backend.
//!
//! Sets up bridge interfaces, veth pairs, and IP addressing for
//! inter-container connectivity in multi-container stacks.

use tokio::process::Command;
use tracing::{debug, info, warn};

use crate::error::LinuxNativeError;

/// Default bridge interface name for vz container networking.
pub const DEFAULT_BRIDGE_NAME: &str = "vz0";

/// Default bridge subnet.
pub const DEFAULT_BRIDGE_SUBNET: &str = "172.20.0.1/16";

/// Create a Linux bridge interface.
///
/// Runs:
/// ```text
/// ip link add <name> type bridge
/// ip addr add <subnet> dev <name>
/// ip link set <name> up
/// ```
pub async fn create_bridge(name: &str, subnet: &str) -> Result<(), LinuxNativeError> {
    info!(name, subnet, "creating bridge interface");

    // Create bridge.
    run_ip(&["link", "add", name, "type", "bridge"]).await?;

    // Assign IP.
    run_ip(&["addr", "add", subnet, "dev", name]).await?;

    // Bring up.
    run_ip(&["link", "set", name, "up"]).await?;

    debug!(name, "bridge interface created");
    Ok(())
}

/// Delete a Linux bridge interface.
pub async fn delete_bridge(name: &str) -> Result<(), LinuxNativeError> {
    info!(name, "deleting bridge interface");
    run_ip(&["link", "del", name]).await?;
    Ok(())
}

/// Create a veth pair and wire one end into a network namespace.
///
/// Creates `<veth_host>` on the host bridge and `<veth_container>` inside
/// the network namespace `<netns>` with the given IP address.
///
/// Runs:
/// ```text
/// ip link add <veth_host> type veth peer name <veth_container>
/// ip link set <veth_host> master <bridge>
/// ip link set <veth_host> up
/// ip link set <veth_container> netns <netns>
/// ip netns exec <netns> ip addr add <addr>/16 dev <veth_container>
/// ip netns exec <netns> ip link set <veth_container> up
/// ip netns exec <netns> ip link set lo up
/// ip netns exec <netns> ip route add default via <gateway>
/// ```
pub async fn wire_veth_to_netns(
    bridge: &str,
    netns: &str,
    veth_host: &str,
    veth_container: &str,
    addr: &str,
    gateway: &str,
) -> Result<(), LinuxNativeError> {
    info!(
        bridge,
        netns, veth_host, veth_container, addr, "wiring veth pair"
    );

    // Create veth pair.
    run_ip(&[
        "link",
        "add",
        veth_host,
        "type",
        "veth",
        "peer",
        "name",
        veth_container,
    ])
    .await?;

    // Attach host end to bridge.
    run_ip(&["link", "set", veth_host, "master", bridge]).await?;
    run_ip(&["link", "set", veth_host, "up"]).await?;

    // Move container end into netns.
    run_ip(&["link", "set", veth_container, "netns", netns]).await?;

    // Configure container end inside netns.
    // `addr` already includes the CIDR suffix (e.g., "172.20.0.2/24").
    run_ip_in_netns(netns, &["addr", "add", addr, "dev", veth_container]).await?;
    run_ip_in_netns(netns, &["link", "set", veth_container, "up"]).await?;
    run_ip_in_netns(netns, &["link", "set", "lo", "up"]).await?;
    run_ip_in_netns(netns, &["route", "add", "default", "via", gateway]).await?;

    debug!(veth_host, veth_container, "veth pair wired");
    Ok(())
}

/// Remove a veth pair (deleting one end removes both).
pub async fn delete_veth(veth_host: &str) -> Result<(), LinuxNativeError> {
    debug!(veth_host, "deleting veth pair");
    // Deleting the host end automatically removes the peer.
    let _ = run_ip(&["link", "del", veth_host]).await;
    Ok(())
}

/// Run `ip <args>` and check for success.
async fn run_ip(args: &[&str]) -> Result<(), LinuxNativeError> {
    debug!(?args, "running ip command");
    let output = Command::new("ip").args(args).output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Some failures are idempotent (e.g., "File exists" when
        // bridge/veth already created). Treat as warnings.
        if stderr.contains("File exists") || stderr.contains("RTNETLINK answers: File exists") {
            debug!(?args, "ip command: already exists (idempotent)");
            return Ok(());
        }
        return Err(LinuxNativeError::InvalidConfig(format!(
            "ip {args:?} failed: {stderr}"
        )));
    }
    Ok(())
}

/// Run `ip netns exec <netns> ip <args>`.
async fn run_ip_in_netns(netns: &str, args: &[&str]) -> Result<(), LinuxNativeError> {
    let mut full_args = vec!["netns", "exec", netns, "ip"];
    full_args.extend(args);
    run_ip(&full_args).await
}

/// Set up iptables NAT masquerade for a bridge subnet.
///
/// This allows containers to reach the internet via the host.
/// Runs: `iptables -t nat -A POSTROUTING -s <subnet> ! -o <bridge> -j MASQUERADE`
pub async fn setup_nat_masquerade(bridge: &str, subnet: &str) -> Result<(), LinuxNativeError> {
    info!(bridge, subnet, "setting up NAT masquerade");

    let output = Command::new("iptables")
        .args([
            "-t",
            "nat",
            "-A",
            "POSTROUTING",
            "-s",
            subnet,
            "!",
            "-o",
            bridge,
            "-j",
            "MASQUERADE",
        ])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        warn!(bridge, subnet, stderr = %stderr.trim(), "iptables masquerade setup warning");
    }

    Ok(())
}

/// Tear down iptables NAT masquerade for a bridge subnet.
pub async fn teardown_nat_masquerade(bridge: &str, subnet: &str) -> Result<(), LinuxNativeError> {
    info!(bridge, subnet, "tearing down NAT masquerade");

    let output = Command::new("iptables")
        .args([
            "-t",
            "nat",
            "-D",
            "POSTROUTING",
            "-s",
            subnet,
            "!",
            "-o",
            bridge,
            "-j",
            "MASQUERADE",
        ])
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        debug!(bridge, subnet, stderr = %stderr.trim(), "masquerade teardown (may already be removed)");
    }

    Ok(())
}

/// Set up iptables DNAT port forwarding from host port to a container IP:port.
///
/// Runs:
/// ```text
/// iptables -t nat -A PREROUTING -p <proto> --dport <host_port> -j DNAT --to-destination <dest_ip>:<container_port>
/// iptables -t nat -A OUTPUT -p <proto> --dport <host_port> -j DNAT --to-destination <dest_ip>:<container_port>
/// iptables -A FORWARD -p <proto> -d <dest_ip> --dport <container_port> -j ACCEPT
/// ```
pub async fn setup_port_forward(
    host_port: u16,
    dest_ip: &str,
    container_port: u16,
    protocol: &str,
) -> Result<(), LinuxNativeError> {
    let dest = format!("{dest_ip}:{container_port}");
    let host_port_s = host_port.to_string();
    let container_port_s = container_port.to_string();

    info!(
        host_port,
        dest_ip, container_port, protocol, "setting up port forward"
    );

    // DNAT for external traffic (PREROUTING).
    run_iptables(&[
        "-t",
        "nat",
        "-A",
        "PREROUTING",
        "-p",
        protocol,
        "--dport",
        &host_port_s,
        "-j",
        "DNAT",
        "--to-destination",
        &dest,
    ])
    .await?;

    // DNAT for local traffic (OUTPUT) — so host can reach services too.
    run_iptables(&[
        "-t",
        "nat",
        "-A",
        "OUTPUT",
        "-p",
        protocol,
        "--dport",
        &host_port_s,
        "-j",
        "DNAT",
        "--to-destination",
        &dest,
    ])
    .await?;

    // Allow forwarded traffic.
    run_iptables(&[
        "-A",
        "FORWARD",
        "-p",
        protocol,
        "-d",
        dest_ip,
        "--dport",
        &container_port_s,
        "-j",
        "ACCEPT",
    ])
    .await?;

    Ok(())
}

/// Tear down iptables DNAT port forwarding.
pub async fn teardown_port_forward(
    host_port: u16,
    dest_ip: &str,
    container_port: u16,
    protocol: &str,
) -> Result<(), LinuxNativeError> {
    let dest = format!("{dest_ip}:{container_port}");
    let host_port_s = host_port.to_string();
    let container_port_s = container_port.to_string();

    debug!(
        host_port,
        dest_ip, container_port, protocol, "tearing down port forward"
    );

    let _ = run_iptables(&[
        "-t",
        "nat",
        "-D",
        "PREROUTING",
        "-p",
        protocol,
        "--dport",
        &host_port_s,
        "-j",
        "DNAT",
        "--to-destination",
        &dest,
    ])
    .await;

    let _ = run_iptables(&[
        "-t",
        "nat",
        "-D",
        "OUTPUT",
        "-p",
        protocol,
        "--dport",
        &host_port_s,
        "-j",
        "DNAT",
        "--to-destination",
        &dest,
    ])
    .await;

    let _ = run_iptables(&[
        "-D",
        "FORWARD",
        "-p",
        protocol,
        "-d",
        dest_ip,
        "--dport",
        &container_port_s,
        "-j",
        "ACCEPT",
    ])
    .await;

    Ok(())
}

/// Run `iptables <args>` and check for success.
async fn run_iptables(args: &[&str]) -> Result<(), LinuxNativeError> {
    debug!(?args, "running iptables command");
    let output = Command::new("iptables").args(args).output().await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        // Some failures are idempotent.
        if stderr.contains("already exists") || stderr.contains("No chain/target/match") {
            debug!(?args, "iptables: idempotent (already exists or no match)");
            return Ok(());
        }
        return Err(LinuxNativeError::InvalidConfig(format!(
            "iptables {args:?} failed: {stderr}"
        )));
    }
    Ok(())
}
