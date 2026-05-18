//! Port allocation and conflict detection for stack services.
//!
//! # The actual networking model
//!
//! This module does **not** implement guest networking. The real model is:
//!
//! - **Guest → Internet / macOS host**: Apple's user-mode NAT via
//!   [`VZNATNetworkDeviceAttachment`] (`crates/vz/src/bridge.rs:409-441`,
//!   also `install.rs:505-512`). The guest receives an IPv4 via DHCP (BusyBox
//!   `udhcpc` in `linux/initramfs/init`) from Apple's `bootpd`. The router
//!   handed out by Apple's NAT is `192.168.64.1`, which is reachable from
//!   the guest as the macOS host (see `vz::protocol::HOST_INTERNAL_GATEWAY_IPV4`).
//!
//! - **Container → container DNS (sibling service discovery)**: per-container
//!   `/etc/hosts` populated from `RunConfig::extra_hosts`. The bundle writer
//!   lives in `crates/vz-oci-macos/src/runtime/bundle.rs:172-191` and the
//!   sibling-service auto-injection lives in
//!   `crates/vz-stack/src/executor/create.rs` (sees only services that share
//!   a network with the caller). A post-start `nsenter` rewrite at
//!   `crates/vz-oci-macos/src/runtime/stack_vm.rs:668-716` ensures the
//!   container sees the same `/etc/hosts` after `pivot_root`.
//!
//! - **Per-stack network isolation**: each service runs in its own Linux
//!   network namespace inside the shared guest VM; bridges/veths are wired
//!   up by `vz-linux-native` on the guest side.
//!
//! - **Inbound host-port publishing**: this module allocates and tracks host
//!   ports ([`resolve_ports`] + [`PortTracker`] in `executor/mod.rs`); actual
//!   forwarding is a userspace vsock relay in
//!   `crates/vz-oci-macos/src/runtime/networking.rs::start_port_forwarding`.
//!   For each `PortMapping`, the host spawns a TCP listener on
//!   `127.0.0.1:<host_port>`. Accepted connections are tunneled over a gRPC
//!   `port_forward` stream (vsock port 7424) to the guest agent, which then
//!   opens a TCP socket to `target_host:container_port` inside the guest.
//!   Bytes flow via `tokio::io::copy_bidirectional`. Limitations: TCP only;
//!   host listener bound to loopback only (no LAN exposure).
//!
//! There is no gvproxy. A reserved tracking issue exists for reviving
//! gvproxy as a production network backend (it would solve both
//! `host.vz.internal` properly and give portable host-port forwarding);
//! see the network-related beads for the live tracker.
//!
//! [`VZNATNetworkDeviceAttachment`]: https://developer.apple.com/documentation/virtualization/vznatnetworkdeviceattachment
//! [`PortTracker`]: crate::executor::PortTracker

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;

use serde::{Deserialize, Serialize};

use crate::error::StackError;
use crate::spec::{PortSpec, ServiceSpec};

/// A fully resolved port publication where host_port is always assigned.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishedPort {
    /// Transport protocol (tcp/udp).
    pub protocol: String,
    /// Port the container listens on.
    pub container_port: u16,
    /// Resolved host port (always present).
    pub host_port: u16,
}

/// A detected port conflict between two services.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortConflict {
    /// The conflicting host port.
    pub host_port: u16,
    /// Transport protocol.
    pub protocol: String,
    /// First service that claimed the port.
    pub service_a: String,
    /// Second service that also wants the port.
    pub service_b: String,
}

/// Resolve port specifications into fully assigned published ports.
///
/// For ports with an explicit host_port, verifies no conflict with `in_use`.
/// For ports without a host_port, finds an available port on the host.
pub fn resolve_ports(
    ports: &[PortSpec],
    in_use: &HashSet<u16>,
) -> Result<Vec<PublishedPort>, StackError> {
    let mut resolved = Vec::new();
    let mut newly_assigned: HashSet<u16> = HashSet::new();

    for port in ports {
        let host_port = match port.host_port {
            Some(hp) => {
                if in_use.contains(&hp) || newly_assigned.contains(&hp) {
                    return Err(StackError::Network(format!(
                        "port conflict: host port {hp} is already in use. \
                         Another service or stack may be bound to this port. \
                         Try 'vz stack ls' to check running stacks, or use a different host port"
                    )));
                }
                hp
            }
            None => find_available_port(in_use, &newly_assigned)?,
        };

        newly_assigned.insert(host_port);
        resolved.push(PublishedPort {
            protocol: port.protocol.clone(),
            container_port: port.container_port,
            host_port,
        });
    }

    Ok(resolved)
}

/// Find an available host port not in any exclusion set.
fn find_available_port(
    in_use: &HashSet<u16>,
    newly_assigned: &HashSet<u16>,
) -> Result<u16, StackError> {
    for _ in 0..100 {
        let listener = TcpListener::bind("127.0.0.1:0")
            .map_err(|e| StackError::Network(format!("failed to bind ephemeral port: {e}")))?;
        let port = listener
            .local_addr()
            .map_err(|e| StackError::Network(format!("failed to get local address: {e}")))?
            .port();

        if !in_use.contains(&port) && !newly_assigned.contains(&port) {
            return Ok(port);
        }
    }

    Err(StackError::Network(
        "unable to find available port after 100 attempts".to_string(),
    ))
}

/// Detect cross-service port conflicts within a stack.
///
/// Scans all services' port specs for duplicate host port bindings
/// on the same protocol and returns any conflicts found.
pub fn detect_port_conflicts(services: &[ServiceSpec]) -> Vec<PortConflict> {
    let mut seen: HashMap<(u16, &str), &str> = HashMap::new();
    let mut conflicts = Vec::new();

    for svc in services {
        for port in &svc.ports {
            if let Some(hp) = port.host_port {
                let key = (hp, port.protocol.as_str());
                if let Some(&other_svc) = seen.get(&key) {
                    conflicts.push(PortConflict {
                        host_port: hp,
                        protocol: port.protocol.clone(),
                        service_a: other_svc.to_string(),
                        service_b: svc.name.clone(),
                    });
                } else {
                    seen.insert(key, svc.name.as_str());
                }
            }
        }
    }

    conflicts
}

/// Detect whether port configurations have changed between two service specs.
///
/// Returns `true` if ports differ, which should trigger a service recreate.
pub fn ports_changed(old: &[PortSpec], new: &[PortSpec]) -> bool {
    old != new
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::collections::HashMap;

    fn svc(name: &str, ports: Vec<PortSpec>) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
            kind: crate::spec::ServiceKind::Service,
            image: "img:latest".to_string(),
            command: None,
            entrypoint: None,
            environment: HashMap::new(),
            working_dir: None,
            user: None,
            mounts: vec![],
            ports,
            depends_on: vec![],
            healthcheck: None,
            restart_policy: None,
            resources: Default::default(),
            extra_hosts: vec![],
            secrets: vec![],
            networks: vec![],
            cap_add: vec![],
            cap_drop: vec![],
            privileged: false,
            read_only: false,
            sysctls: HashMap::new(),
            ulimits: vec![],
            container_name: None,
            hostname: None,
            domainname: None,
            labels: HashMap::new(),
            stop_signal: None,
            stop_grace_period_secs: None,
            expose: vec![],
            stdin_open: false,
            tty: false,
            logging: None,
        }
    }

    fn tcp_port(container: u16, host: Option<u16>) -> PortSpec {
        PortSpec {
            protocol: "tcp".to_string(),
            container_port: container,
            host_port: host,
        }
    }

    // ── PublishedPort serialization ──

    #[test]
    fn published_port_round_trip() {
        let port = PublishedPort {
            protocol: "tcp".to_string(),
            container_port: 80,
            host_port: 8080,
        };
        let json = serde_json::to_string(&port).unwrap();
        let deserialized: PublishedPort = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, port);
    }

    // ── Port resolution ──

    #[test]
    fn resolve_ports_explicit_no_conflict() {
        let ports = vec![tcp_port(80, Some(8080)), tcp_port(443, Some(8443))];
        let in_use = HashSet::new();

        let resolved = resolve_ports(&ports, &in_use).unwrap();
        assert_eq!(resolved.len(), 2);
        assert_eq!(resolved[0].host_port, 8080);
        assert_eq!(resolved[1].host_port, 8443);
    }

    #[test]
    fn resolve_ports_rejects_in_use_conflict() {
        let ports = vec![tcp_port(80, Some(8080))];
        let in_use: HashSet<u16> = [8080].into();

        let err = resolve_ports(&ports, &in_use).unwrap_err();
        assert!(matches!(err, StackError::Network(_)));
    }

    #[test]
    fn resolve_ports_rejects_self_conflict() {
        let ports = vec![tcp_port(80, Some(8080)), tcp_port(443, Some(8080))];
        let in_use = HashSet::new();

        let err = resolve_ports(&ports, &in_use).unwrap_err();
        assert!(matches!(err, StackError::Network(_)));
    }

    #[test]
    fn resolve_ports_random_assignment() {
        let ports = vec![tcp_port(80, None), tcp_port(443, None)];
        let in_use = HashSet::new();

        let resolved = resolve_ports(&ports, &in_use).unwrap();
        assert_eq!(resolved.len(), 2);
        assert!(resolved[0].host_port > 0);
        assert!(resolved[1].host_port > 0);
        // Two random ports must be different.
        assert_ne!(resolved[0].host_port, resolved[1].host_port);
    }

    #[test]
    fn resolve_ports_mixed_explicit_and_random() {
        let ports = vec![tcp_port(80, Some(8080)), tcp_port(443, None)];
        let in_use = HashSet::new();

        let resolved = resolve_ports(&ports, &in_use).unwrap();
        assert_eq!(resolved[0].host_port, 8080);
        assert!(resolved[1].host_port > 0);
        assert_ne!(resolved[1].host_port, 8080);
    }

    // ── Port conflict detection ──

    #[test]
    fn detect_no_conflicts() {
        let services = vec![
            svc("web", vec![tcp_port(80, Some(8080))]),
            svc("api", vec![tcp_port(3000, Some(3000))]),
        ];
        let conflicts = detect_port_conflicts(&services);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn detect_same_port_conflict() {
        let services = vec![
            svc("web", vec![tcp_port(80, Some(8080))]),
            svc("api", vec![tcp_port(3000, Some(8080))]),
        ];
        let conflicts = detect_port_conflicts(&services);
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].host_port, 8080);
        assert_eq!(conflicts[0].service_a, "web");
        assert_eq!(conflicts[0].service_b, "api");
    }

    #[test]
    fn detect_conflict_same_port_different_protocol_is_ok() {
        let services = vec![
            svc(
                "web",
                vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
            ),
            svc(
                "dns",
                vec![PortSpec {
                    protocol: "udp".to_string(),
                    container_port: 53,
                    host_port: Some(8080),
                }],
            ),
        ];
        let conflicts = detect_port_conflicts(&services);
        assert!(conflicts.is_empty());
    }

    #[test]
    fn detect_conflict_skips_random_ports() {
        let services = vec![
            svc("web", vec![tcp_port(80, None)]),
            svc("api", vec![tcp_port(3000, None)]),
        ];
        let conflicts = detect_port_conflicts(&services);
        assert!(conflicts.is_empty());
    }

    // ── Port change detection ──

    #[test]
    fn ports_changed_detects_addition() {
        let old = vec![];
        let new = vec![tcp_port(80, Some(8080))];
        assert!(ports_changed(&old, &new));
    }

    #[test]
    fn ports_changed_detects_removal() {
        let old = vec![tcp_port(80, Some(8080))];
        let new = vec![];
        assert!(ports_changed(&old, &new));
    }

    #[test]
    fn ports_changed_detects_host_port_change() {
        let old = vec![tcp_port(80, Some(8080))];
        let new = vec![tcp_port(80, Some(9090))];
        assert!(ports_changed(&old, &new));
    }

    #[test]
    fn ports_unchanged_returns_false() {
        let ports = vec![tcp_port(80, Some(8080))];
        assert!(!ports_changed(&ports, &ports));
    }
}
