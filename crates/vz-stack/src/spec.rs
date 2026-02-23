//! Typed stack specification model.
//!
//! [`StackSpec`] is the source of truth for a multi-service workload.
//! Compose YAML is an input adapter that produces a `StackSpec`; the
//! reconciler operates exclusively on this typed model.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Condition for a service dependency.
///
/// Determines when a dependency is considered satisfied, following
/// Docker Compose v3 semantics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum DependencyCondition {
    /// Dependency is satisfied as soon as the service is running,
    /// regardless of health check status. This is the default.
    #[default]
    ServiceStarted,
    /// Dependency is satisfied only when the service is running AND
    /// its health check has passed at least once.
    ServiceHealthy,
    /// Dependency is satisfied when the service has completed (exited
    /// with code 0). Useful for init containers.
    ServiceCompletedSuccessfully,
}

/// A dependency on another service with an optional condition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceDependency {
    /// Name of the service this depends on.
    pub service: String,
    /// Condition that must be met before the dependency is satisfied.
    #[serde(default)]
    pub condition: DependencyCondition,
}

impl ServiceDependency {
    /// Create a dependency with the default condition (`service_started`).
    pub fn started(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            condition: DependencyCondition::ServiceStarted,
        }
    }

    /// Create a dependency requiring the service to be healthy.
    pub fn healthy(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            condition: DependencyCondition::ServiceHealthy,
        }
    }
}

/// Root specification for a multi-service stack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StackSpec {
    /// Stack name used as a namespace for services, networks, and volumes.
    pub name: String,
    /// Service definitions.
    #[serde(default)]
    pub services: Vec<ServiceSpec>,
    /// Network definitions.
    #[serde(default)]
    pub networks: Vec<NetworkSpec>,
    /// Volume definitions.
    #[serde(default)]
    pub volumes: Vec<VolumeSpec>,
    /// Secret definitions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<SecretDef>,
    /// Persistent volume disk image size in megabytes.
    ///
    /// Configurable via `x-vz.disk_size` in Docker Compose files (accepts
    /// human-readable sizes like `"20g"`, `"512m"`). Defaults to 10 GiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_size_mb: Option<u64>,
}

/// Specification for a single service within a stack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceSpec {
    /// Service name, unique within the stack.
    pub name: String,
    /// OCI image reference.
    pub image: String,
    /// Override command (replaces image CMD).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command: Option<Vec<String>>,
    /// Override entrypoint (replaces image ENTRYPOINT).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entrypoint: Option<Vec<String>>,
    /// Environment variables.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub environment: HashMap<String, String>,
    /// Working directory inside the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_dir: Option<String>,
    /// User to run as inside the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// Volume and bind mounts.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<MountSpec>,
    /// Port mappings from host to container.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortSpec>,
    /// Service dependencies with optional conditions for startup ordering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<ServiceDependency>,
    /// Health check configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<HealthCheckSpec>,
    /// Restart policy for the service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<RestartPolicy>,
    /// Resource constraints.
    #[serde(default)]
    pub resources: ResourcesSpec,
    /// Extra `/etc/hosts` entries as `(hostname, ip)` pairs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extra_hosts: Vec<(String, String)>,
    /// Secret references for this service.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<ServiceSecretRef>,
    /// Network names this service belongs to.
    ///
    /// When empty, the service joins the implicit `"default"` network.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub networks: Vec<String>,
    // ── Security fields ──────────────────────────────────────────
    /// Additional Linux capabilities to add to the container.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cap_add: Vec<String>,
    /// Linux capabilities to drop from the container defaults.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cap_drop: Vec<String>,
    /// Run the container in privileged mode (all capabilities).
    #[serde(default)]
    pub privileged: bool,
    /// Mount the container root filesystem as read-only.
    #[serde(default)]
    pub read_only: bool,
    /// Kernel parameters to set inside the container.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub sysctls: HashMap<String, String>,
    // ── Resource extensions ──────────────────────────────────────
    /// Per-process resource limits (ulimits).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ulimits: Vec<UlimitSpec>,
    // ── Container identity ───────────────────────────────────────
    /// Explicit container name override.
    ///
    /// When set, the container runtime uses this as the container identifier
    /// instead of the service name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub container_name: Option<String>,
    /// Container hostname override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// Container domain name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub domainname: Option<String>,
    /// Container labels (key-value metadata).
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub labels: HashMap<String, String>,
    // ── Stop lifecycle ──────────────────────────────────────────────
    /// Signal to send for graceful stop (e.g., "SIGQUIT"). Default: SIGTERM.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_signal: Option<String>,
    /// Seconds to wait after stop signal before SIGKILL. Default: 10.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stop_grace_period_secs: Option<u64>,
}

/// Mount specification for container volumes.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum MountSpec {
    /// Bind mount from host path to container path.
    #[serde(rename = "bind")]
    Bind {
        /// Host source path.
        source: String,
        /// Container destination path.
        target: String,
        /// Whether the mount is read-only.
        #[serde(default)]
        read_only: bool,
    },
    /// Named volume mount.
    #[serde(rename = "named")]
    Named {
        /// Volume name (must match a VolumeSpec entry).
        source: String,
        /// Container destination path.
        target: String,
        /// Whether the mount is read-only.
        #[serde(default)]
        read_only: bool,
    },
    /// Ephemeral tmpfs mount.
    #[serde(rename = "ephemeral")]
    Ephemeral {
        /// Container destination path.
        target: String,
    },
}

/// Host-to-container port mapping.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PortSpec {
    /// Transport protocol.
    #[serde(default = "default_protocol")]
    pub protocol: String,
    /// Container port to expose.
    pub container_port: u16,
    /// Host port to bind. If absent, a random port is assigned.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_port: Option<u16>,
}

fn default_protocol() -> String {
    "tcp".to_string()
}

/// Container health check configuration.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HealthCheckSpec {
    /// Command to execute for the health check.
    pub test: Vec<String>,
    /// Interval between checks in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
    /// Timeout for a single check in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Number of consecutive failures before marking unhealthy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
    /// Grace period before health checks start in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_period_secs: Option<u64>,
}

/// Restart policy for a service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "policy")]
pub enum RestartPolicy {
    /// Never restart.
    #[serde(rename = "no")]
    No,
    /// Always restart regardless of exit code.
    #[serde(rename = "always")]
    Always,
    /// Restart only on non-zero exit.
    #[serde(rename = "on-failure")]
    OnFailure {
        /// Maximum retry count before giving up.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_retries: Option<u32>,
    },
    /// Restart unless explicitly stopped.
    #[serde(rename = "unless-stopped")]
    UnlessStopped,
}

/// Resource constraints for a service.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct ResourcesSpec {
    /// CPU limit as fractional cores (e.g., 0.5 for half a core, 2.0 for two cores).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpus: Option<f64>,
    /// Memory limit in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    /// CPU reservation as fractional cores (informational, for scheduler).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation_cpus: Option<f64>,
    /// Memory reservation in bytes (informational, for scheduler).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reservation_memory_bytes: Option<u64>,
    /// Maximum number of PIDs in the container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pids_limit: Option<i64>,
    /// Number of replica instances to run.
    #[serde(default = "default_replicas")]
    pub replicas: u32,
}

fn default_replicas() -> u32 {
    1
}

/// Per-process resource limit (ulimit) specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UlimitSpec {
    /// Limit name (e.g., `nofile`, `nproc`, `memlock`).
    pub name: String,
    /// Soft limit value.
    pub soft: u64,
    /// Hard limit value.
    pub hard: u64,
}

/// Network definition for the stack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkSpec {
    /// Network name.
    pub name: String,
    /// Network driver.
    #[serde(default = "default_network_driver")]
    pub driver: String,
    /// Optional explicit subnet in CIDR notation (e.g., `"172.20.1.0/24"`).
    ///
    /// When `None`, the executor auto-assigns a subnet from the pool.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subnet: Option<String>,
}

fn default_network_driver() -> String {
    "bridge".to_string()
}

/// Volume definition for the stack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VolumeSpec {
    /// Volume name.
    pub name: String,
    /// Volume driver.
    #[serde(default = "default_volume_driver")]
    pub driver: String,
    /// Driver-specific options.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub driver_opts: Option<HashMap<String, String>>,
}

fn default_volume_driver() -> String {
    "local".to_string()
}

/// A top-level secret definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SecretDef {
    /// Secret name (referenced by services).
    pub name: String,
    /// Host file path containing the secret value.
    pub file: String,
}

/// A service-level reference to a defined secret.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ServiceSecretRef {
    /// Name of the top-level secret to mount.
    pub source: String,
    /// Mount target filename inside `/run/secrets/`. Defaults to source name.
    pub target: String,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn stack_spec_round_trip() {
        let spec = StackSpec {
            name: "myapp".to_string(),
            services: vec![ServiceSpec {
                name: "web".to_string(),
                image: "nginx:latest".to_string(),
                command: Some(vec!["nginx".to_string(), "-g".to_string()]),
                entrypoint: None,
                environment: HashMap::from([("PORT".to_string(), "8080".to_string())]),
                working_dir: Some("/app".to_string()),
                user: Some("1000:1000".to_string()),
                mounts: vec![MountSpec::Bind {
                    source: "/host/data".to_string(),
                    target: "/data".to_string(),
                    read_only: true,
                }],
                ports: vec![PortSpec {
                    protocol: "tcp".to_string(),
                    container_port: 80,
                    host_port: Some(8080),
                }],
                depends_on: vec![ServiceDependency::started("db")],
                healthcheck: Some(HealthCheckSpec {
                    test: vec![
                        "CMD".to_string(),
                        "curl".to_string(),
                        "localhost".to_string(),
                    ],
                    interval_secs: Some(30),
                    timeout_secs: Some(5),
                    retries: Some(3),
                    start_period_secs: Some(10),
                }),
                restart_policy: Some(RestartPolicy::OnFailure {
                    max_retries: Some(5),
                }),
                resources: ResourcesSpec {
                    cpus: Some(2.0),
                    memory_bytes: Some(512 * 1024 * 1024),
                    reservation_cpus: None,
                    reservation_memory_bytes: None,
                    pids_limit: None,
                    replicas: 1,
                },
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
            }],
            networks: vec![NetworkSpec {
                name: "frontend".to_string(),
                driver: "bridge".to_string(),
                subnet: None,
            }],
            volumes: vec![VolumeSpec {
                name: "dbdata".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            }],
            secrets: vec![],
            disk_size_mb: None,
        };

        let json = serde_json::to_string_pretty(&spec).unwrap();
        let deserialized: StackSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, spec);
    }

    #[test]
    fn stack_spec_minimal() {
        let spec = StackSpec {
            name: "minimal".to_string(),
            services: vec![],
            networks: vec![],
            volumes: vec![],
            secrets: vec![],
            disk_size_mb: None,
        };

        let json = serde_json::to_string(&spec).unwrap();
        let deserialized: StackSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, spec);
    }

    #[test]
    fn mount_spec_variants_serialize_with_tag() {
        let bind = MountSpec::Bind {
            source: "/src".to_string(),
            target: "/dst".to_string(),
            read_only: false,
        };
        let json = serde_json::to_string(&bind).unwrap();
        assert!(json.contains("\"type\":\"bind\""));

        let named = MountSpec::Named {
            source: "vol".to_string(),
            target: "/data".to_string(),
            read_only: true,
        };
        let json = serde_json::to_string(&named).unwrap();
        assert!(json.contains("\"type\":\"named\""));

        let ephemeral = MountSpec::Ephemeral {
            target: "/tmp".to_string(),
        };
        let json = serde_json::to_string(&ephemeral).unwrap();
        assert!(json.contains("\"type\":\"ephemeral\""));
    }

    #[test]
    fn restart_policy_variants_round_trip() {
        let policies = vec![
            RestartPolicy::No,
            RestartPolicy::Always,
            RestartPolicy::OnFailure {
                max_retries: Some(3),
            },
            RestartPolicy::UnlessStopped,
        ];
        for policy in policies {
            let json = serde_json::to_string(&policy).unwrap();
            let deserialized: RestartPolicy = serde_json::from_str(&json).unwrap();
            assert_eq!(deserialized, policy);
        }
    }

    #[test]
    fn network_spec_defaults_to_bridge_driver() {
        let json = r#"{"name":"net1"}"#;
        let spec: NetworkSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.driver, "bridge");
    }

    #[test]
    fn volume_spec_defaults_to_local_driver() {
        let json = r#"{"name":"vol1"}"#;
        let spec: VolumeSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.driver, "local");
    }

    #[test]
    fn port_spec_defaults_to_tcp_protocol() {
        let json = r#"{"container_port":80}"#;
        let spec: PortSpec = serde_json::from_str(json).unwrap();
        assert_eq!(spec.protocol, "tcp");
        assert!(spec.host_port.is_none());
    }
}
