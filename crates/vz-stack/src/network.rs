//! Network backend abstraction and port allocation.
//!
//! Defines the [`NetworkBackend`] trait for swappable network
//! implementations and provides port conflict detection and
//! allocation logic. The [`GvproxyBackend`] is the first
//! shipping implementation using `gvproxy` from the
//! containers/gvisor-tap-vsock project.

use std::collections::{HashMap, HashSet};
use std::net::TcpListener;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::StackError;
use crate::spec::{PortSpec, ServiceSpec};

/// Handle representing an active per-stack network.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NetworkHandle {
    /// Stack name this network belongs to.
    pub stack_name: String,
    /// Network name from the spec.
    pub network_name: String,
    /// Subnet assigned to this stack network (CIDR notation).
    pub subnet: String,
    /// Gateway address for the network.
    pub gateway: String,
}

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

/// Network backend for per-stack isolation and port management.
///
/// Implementations manage the lifecycle of stack networks including
/// subnet allocation, port forwarding, and teardown.
pub trait NetworkBackend: std::fmt::Debug {
    /// Create a network for a stack.
    fn create_network(
        &mut self,
        stack_name: &str,
        network_name: &str,
    ) -> Result<NetworkHandle, StackError>;

    /// Destroy a stack's network and release all resources.
    fn destroy_network(&mut self, handle: &NetworkHandle) -> Result<(), StackError>;

    /// Publish ports for a service within a stack network.
    fn publish_ports(
        &mut self,
        handle: &NetworkHandle,
        service_name: &str,
        ports: &[PortSpec],
    ) -> Result<Vec<PublishedPort>, StackError>;

    /// Release published ports for a service.
    fn release_ports(
        &mut self,
        handle: &NetworkHandle,
        service_name: &str,
    ) -> Result<(), StackError>;

    /// Return the backend name for logging/diagnostics.
    fn backend_name(&self) -> &str;
}

/// Configuration for the gvproxy network backend.
#[derive(Debug, Clone)]
pub struct GvproxyConfig {
    /// Explicit path to the gvproxy binary.
    /// If `None`, searches standard locations and PATH.
    pub binary_path: Option<PathBuf>,
    /// Base subnet prefix for stack networks.
    /// Each stack gets a /24 carved from this range.
    /// Defaults to `"10.88"`.
    pub base_subnet_prefix: String,
}

impl Default for GvproxyConfig {
    fn default() -> Self {
        Self {
            binary_path: None,
            base_subnet_prefix: "10.88".to_string(),
        }
    }
}

/// The gvproxy network backend.
///
/// Uses `gvproxy` (from containers/gvisor-tap-vsock) for user-space
/// networking with per-stack isolation and host port forwarding.
#[derive(Debug)]
pub struct GvproxyBackend {
    /// Validated path to the gvproxy binary.
    binary_path: PathBuf,
    /// Base subnet prefix for allocation.
    base_subnet_prefix: String,
    /// Next subnet octet to allocate.
    next_subnet_octet: u8,
    /// Active networks keyed by `stack_name/network_name`.
    active_networks: HashMap<String, NetworkHandle>,
    /// Published ports keyed by `stack_name/service_name`.
    published_ports: HashMap<String, Vec<PublishedPort>>,
}

impl GvproxyBackend {
    /// Create a new gvproxy backend with the given configuration.
    ///
    /// Validates that the gvproxy binary exists and is accessible.
    pub fn new(config: GvproxyConfig) -> Result<Self, StackError> {
        let binary_path = locate_gvproxy(&config)?;
        Ok(Self::with_validated_binary(
            binary_path,
            config.base_subnet_prefix,
        ))
    }

    /// Create a backend with an already-validated binary path.
    ///
    /// Skips binary existence checks. Useful when the caller has
    /// already verified or provisioned the binary.
    pub fn with_validated_binary(binary_path: PathBuf, base_subnet_prefix: String) -> Self {
        Self {
            binary_path,
            base_subnet_prefix,
            next_subnet_octet: 1,
            active_networks: HashMap::new(),
            published_ports: HashMap::new(),
        }
    }

    /// Return the validated gvproxy binary path.
    pub fn binary_path(&self) -> &Path {
        &self.binary_path
    }
}

impl NetworkBackend for GvproxyBackend {
    fn create_network(
        &mut self,
        stack_name: &str,
        network_name: &str,
    ) -> Result<NetworkHandle, StackError> {
        let key = format!("{stack_name}/{network_name}");
        if self.active_networks.contains_key(&key) {
            return Err(StackError::Network(format!(
                "network '{key}' already exists"
            )));
        }

        let octet = self.next_subnet_octet;
        if octet == 0 {
            return Err(StackError::Network("subnet pool exhausted".to_string()));
        }
        self.next_subnet_octet = octet.checked_add(1).unwrap_or(0);

        let prefix = &self.base_subnet_prefix;
        let handle = NetworkHandle {
            stack_name: stack_name.to_string(),
            network_name: network_name.to_string(),
            subnet: format!("{prefix}.{octet}.0/24"),
            gateway: format!("{prefix}.{octet}.1"),
        };

        self.active_networks.insert(key, handle.clone());
        Ok(handle)
    }

    fn destroy_network(&mut self, handle: &NetworkHandle) -> Result<(), StackError> {
        let key = format!("{}/{}", handle.stack_name, handle.network_name);
        self.active_networks.remove(&key);
        // Release any ports associated with services in this stack.
        let prefix = format!("{}/", handle.stack_name);
        self.published_ports.retain(|k, _| !k.starts_with(&prefix));
        Ok(())
    }

    fn publish_ports(
        &mut self,
        handle: &NetworkHandle,
        service_name: &str,
        ports: &[PortSpec],
    ) -> Result<Vec<PublishedPort>, StackError> {
        let key = format!("{}/{service_name}", handle.stack_name);

        // Collect all currently published host ports across the backend.
        let in_use: HashSet<u16> = self
            .published_ports
            .values()
            .flat_map(|ps| ps.iter().map(|p| p.host_port))
            .collect();

        let resolved = resolve_ports(ports, &in_use)?;
        self.published_ports.insert(key, resolved.clone());
        Ok(resolved)
    }

    fn release_ports(
        &mut self,
        handle: &NetworkHandle,
        service_name: &str,
    ) -> Result<(), StackError> {
        let key = format!("{}/{service_name}", handle.stack_name);
        self.published_ports.remove(&key);
        Ok(())
    }

    fn backend_name(&self) -> &str {
        "gvproxy"
    }
}

/// Locate the gvproxy binary, searching explicit path then standard locations.
pub fn locate_gvproxy(config: &GvproxyConfig) -> Result<PathBuf, StackError> {
    if let Some(ref path) = config.binary_path {
        if path.exists() {
            return Ok(path.clone());
        }
        return Err(StackError::Network(format!(
            "gvproxy binary not found at '{}'",
            path.display()
        )));
    }

    // Search standard locations.
    let candidates = [
        "/usr/local/bin/gvproxy",
        "/usr/bin/gvproxy",
        "/opt/homebrew/bin/gvproxy",
    ];

    for candidate in &candidates {
        let path = PathBuf::from(candidate);
        if path.exists() {
            return Ok(path);
        }
    }

    // Try PATH via `which`.
    if let Ok(output) = std::process::Command::new("which").arg("gvproxy").output() {
        if output.status.success() {
            let path_str = String::from_utf8_lossy(&output.stdout);
            let path = PathBuf::from(path_str.trim());
            if path.exists() {
                return Ok(path);
            }
        }
    }

    Err(StackError::Network(
        "gvproxy binary not found; install it or set binary_path in GvproxyConfig".to_string(),
    ))
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
                        "host port {hp} is already in use"
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
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    fn dummy_backend() -> GvproxyBackend {
        GvproxyBackend::with_validated_binary(PathBuf::from("/usr/bin/true"), "10.88".to_string())
    }

    fn svc(name: &str, ports: Vec<PortSpec>) -> ServiceSpec {
        ServiceSpec {
            name: name.to_string(),
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
            hostname: None,
            domainname: None,
            labels: HashMap::new(),
        }
    }

    fn tcp_port(container: u16, host: Option<u16>) -> PortSpec {
        PortSpec {
            protocol: "tcp".to_string(),
            container_port: container,
            host_port: host,
        }
    }

    // ── NetworkHandle serialization ──

    #[test]
    fn network_handle_round_trip() {
        let handle = NetworkHandle {
            stack_name: "myapp".to_string(),
            network_name: "default".to_string(),
            subnet: "10.88.1.0/24".to_string(),
            gateway: "10.88.1.1".to_string(),
        };
        let json = serde_json::to_string(&handle).unwrap();
        let deserialized: NetworkHandle = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, handle);
    }

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

    // ── GvproxyBackend lifecycle ──

    #[test]
    fn backend_create_network_assigns_unique_subnets() {
        let mut backend = dummy_backend();
        let h1 = backend.create_network("app1", "default").unwrap();
        let h2 = backend.create_network("app2", "default").unwrap();

        assert_eq!(h1.subnet, "10.88.1.0/24");
        assert_eq!(h1.gateway, "10.88.1.1");
        assert_eq!(h2.subnet, "10.88.2.0/24");
        assert_eq!(h2.gateway, "10.88.2.1");
    }

    #[test]
    fn backend_create_duplicate_network_fails() {
        let mut backend = dummy_backend();
        backend.create_network("app", "net").unwrap();
        let err = backend.create_network("app", "net").unwrap_err();
        assert!(matches!(err, StackError::Network(_)));
    }

    #[test]
    fn backend_destroy_network_removes_handle() {
        let mut backend = dummy_backend();
        let handle = backend.create_network("app", "net").unwrap();
        backend.destroy_network(&handle).unwrap();

        // Should be able to recreate with the same name.
        let h2 = backend.create_network("app", "net").unwrap();
        assert_eq!(h2.stack_name, "app");
    }

    #[test]
    fn backend_destroy_releases_published_ports() {
        let mut backend = dummy_backend();
        let handle = backend.create_network("app", "net").unwrap();

        let ports = vec![tcp_port(80, Some(8080))];
        backend.publish_ports(&handle, "web", &ports).unwrap();

        backend.destroy_network(&handle).unwrap();

        // Recreate and publish the same port — should succeed.
        let h2 = backend.create_network("app", "net").unwrap();
        let result = backend.publish_ports(&h2, "web", &ports).unwrap();
        assert_eq!(result[0].host_port, 8080);
    }

    #[test]
    fn backend_publish_explicit_ports() {
        let mut backend = dummy_backend();
        let handle = backend.create_network("app", "net").unwrap();

        let ports = vec![tcp_port(80, Some(8080)), tcp_port(443, Some(8443))];
        let published = backend.publish_ports(&handle, "web", &ports).unwrap();

        assert_eq!(published.len(), 2);
        assert_eq!(published[0].host_port, 8080);
        assert_eq!(published[0].container_port, 80);
        assert_eq!(published[1].host_port, 8443);
        assert_eq!(published[1].container_port, 443);
    }

    #[test]
    fn backend_publish_random_port_assigns_nonzero() {
        let mut backend = dummy_backend();
        let handle = backend.create_network("app", "net").unwrap();

        let ports = vec![tcp_port(80, None)];
        let published = backend.publish_ports(&handle, "web", &ports).unwrap();

        assert_eq!(published.len(), 1);
        assert!(published[0].host_port > 0);
        assert_eq!(published[0].container_port, 80);
    }

    #[test]
    fn backend_publish_detects_cross_service_conflict() {
        let mut backend = dummy_backend();
        let handle = backend.create_network("app", "net").unwrap();

        let ports_a = vec![tcp_port(80, Some(8080))];
        backend.publish_ports(&handle, "web", &ports_a).unwrap();

        let ports_b = vec![tcp_port(3000, Some(8080))];
        let err = backend.publish_ports(&handle, "api", &ports_b).unwrap_err();
        assert!(matches!(err, StackError::Network(_)));
    }

    #[test]
    fn backend_release_allows_reuse() {
        let mut backend = dummy_backend();
        let handle = backend.create_network("app", "net").unwrap();

        let ports = vec![tcp_port(80, Some(9090))];
        backend.publish_ports(&handle, "web", &ports).unwrap();

        backend.release_ports(&handle, "web").unwrap();

        // Another service can now use the same host port.
        let published = backend.publish_ports(&handle, "api", &ports).unwrap();
        assert_eq!(published[0].host_port, 9090);
    }

    #[test]
    fn backend_name_is_gvproxy() {
        let backend = dummy_backend();
        assert_eq!(backend.backend_name(), "gvproxy");
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

    // ── Binary provisioning ──

    #[test]
    fn locate_gvproxy_with_explicit_valid_path() {
        let mut tmp = NamedTempFile::new().unwrap();
        writeln!(tmp, "#!/bin/sh").unwrap();
        let path = tmp.path().to_path_buf();

        let config = GvproxyConfig {
            binary_path: Some(path.clone()),
            ..Default::default()
        };
        let result = locate_gvproxy(&config).unwrap();
        assert_eq!(result, path);
    }

    #[test]
    fn locate_gvproxy_with_explicit_missing_path() {
        let config = GvproxyConfig {
            binary_path: Some(PathBuf::from("/nonexistent/gvproxy")),
            ..Default::default()
        };
        let err = locate_gvproxy(&config).unwrap_err();
        assert!(matches!(err, StackError::Network(_)));
    }

    // ── Trait object usage ──

    #[test]
    fn backend_usable_as_trait_object() {
        let mut backend: Box<dyn NetworkBackend> = Box::new(dummy_backend());
        let handle = backend.create_network("app", "default").unwrap();
        assert_eq!(handle.stack_name, "app");
        assert_eq!(backend.backend_name(), "gvproxy");
    }

    // ── Subnet exhaustion ──

    #[test]
    fn backend_subnet_exhaustion() {
        let mut backend = dummy_backend();

        // Allocate 255 networks (octets 1..=255), then the next should fail.
        for i in 1..=255u16 {
            backend.create_network(&format!("app{i}"), "net").unwrap();
        }

        let err = backend.create_network("overflow", "net").unwrap_err();
        assert!(matches!(err, StackError::Network(_)));
    }
}
