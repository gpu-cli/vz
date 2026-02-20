//! Typed stack specification model.
//!
//! [`StackSpec`] is the source of truth for a multi-service workload.
//! Compose YAML is an input adapter that produces a `StackSpec`; the
//! reconciler operates exclusively on this typed model.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

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
    /// Service names this service depends on for startup ordering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// Health check configuration.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck: Option<HealthCheckSpec>,
    /// Restart policy for the service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_policy: Option<RestartPolicy>,
    /// Resource constraints.
    #[serde(default)]
    pub resources: ResourcesSpec,
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
    /// CPU limit (e.g., "2" for 2 cores).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<String>,
    /// Memory limit (e.g., "512m", "1g").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<String>,
}

/// Network definition for the stack.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct NetworkSpec {
    /// Network name.
    pub name: String,
    /// Network driver.
    #[serde(default = "default_network_driver")]
    pub driver: String,
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
                depends_on: vec!["db".to_string()],
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
                    cpu: Some("2".to_string()),
                    memory: Some("512m".to_string()),
                },
            }],
            networks: vec![NetworkSpec {
                name: "frontend".to_string(),
                driver: "bridge".to_string(),
            }],
            volumes: vec![VolumeSpec {
                name: "dbdata".to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            }],
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
