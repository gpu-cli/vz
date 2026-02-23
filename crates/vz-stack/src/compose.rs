//! Compose YAML subset importer.
//!
//! Parses a Docker Compose YAML file into a typed [`StackSpec`],
//! accepting only the feature subset defined in the v1 compliance
//! contract. Unsupported keys are rejected with stable error codes
//! before any reconciliation starts.

use std::collections::HashMap;
use std::path::Path;

use serde_yml::Value;

use crate::error::StackError;
use crate::spec::{
    DependencyCondition, HealthCheckSpec, MountSpec, NetworkSpec, PortSpec, ResourcesSpec,
    RestartPolicy, SecretDef, ServiceDependency, ServiceSecretRef, ServiceSpec, StackSpec,
    UlimitSpec, VolumeSpec,
};

// ── Accepted key sets ──────────────────────────────────────────────

/// Top-level keys allowed in the Compose file.
const ACCEPTED_TOP_LEVEL: &[&str] = &[
    "version", "name", "services", "volumes", "secrets", "networks", "x-vz",
];

/// Service-level keys allowed inside `services.<name>`.
const ACCEPTED_SERVICE: &[&str] = &[
    "image",
    "command",
    "entrypoint",
    "environment",
    "env_file",
    "working_dir",
    "user",
    "ports",
    "volumes",
    "depends_on",
    "healthcheck",
    "restart",
    "extra_hosts",
    "deploy",
    "secrets",
    "networks",
    // Security fields
    "cap_add",
    "cap_drop",
    "privileged",
    "read_only",
    "sysctls",
    // Resource extensions
    "ulimits",
    "pids_limit",
    // Container identity
    "container_name",
    "hostname",
    "domainname",
    "labels",
    // Filesystem
    "tmpfs",
    // Stop lifecycle
    "stop_signal",
    "stop_grace_period",
];

/// Volume-level keys allowed inside `volumes.<name>`.
const ACCEPTED_VOLUME: &[&str] = &["driver", "driver_opts"];

// ── Rejected keys with stable error messages ───────────────────────

/// Top-level keys explicitly rejected with stable error codes.
const REJECTED_TOP_LEVEL: &[(&str, &str)] = &[(
    "configs",
    "Compose configs are not supported; use environment variables or bind mounts instead",
)];

/// Service-level keys explicitly rejected with stable error codes.
const REJECTED_SERVICE: &[(&str, &str)] = &[
    (
        "build",
        "image building is not supported; use pre-built OCI images",
    ),
    (
        "profiles",
        "conditional profiles are not supported; define separate compose files instead",
    ),
    (
        "extends",
        "service inheritance is not supported; duplicate the configuration instead",
    ),
    (
        "devices",
        "device pass-through is not supported in this runtime",
    ),
    (
        "ipc",
        "IPC mode configuration is not supported; each service runs in its own namespace",
    ),
    (
        "pid",
        "PID namespace configuration is not supported; each service runs in its own namespace",
    ),
    (
        "cgroup",
        "cgroup mode configuration is not supported in this runtime",
    ),
    (
        "runtime",
        "container runtime selection is not supported; youki is used for all services",
    ),
];

// ── Public API ─────────────────────────────────────────────────────

/// Parse a Compose YAML string into a [`StackSpec`].
///
/// The `stack_name` is used as the stack namespace when the Compose
/// file does not contain a top-level `name` key.
///
/// This variant performs no filesystem access. `env_file` directives
/// are accepted but ignored and `${VAR}` references are not expanded.
/// Use [`parse_compose_with_dir`] for full env_file and variable
/// expansion support.
///
/// Returns an error if:
/// - The YAML is malformed.
/// - Any unsupported key is present (with a stable error code).
/// - Required fields are missing or have invalid types.
pub fn parse_compose(yaml: &str, stack_name: &str) -> Result<StackSpec, StackError> {
    parse_compose_inner(yaml, stack_name, None)
}

/// Parse a Compose YAML string with env_file and variable expansion.
///
/// Before parsing, this function:
/// 1. Loads a `.env` file from `compose_dir` (if present) as expansion
///    context.
/// 2. Expands `${VAR}`, `${VAR:-default}`, and `$VAR` references in
///    the YAML string using the loaded variables.
/// 3. Resolves `env_file` paths relative to `compose_dir` and merges
///    their contents into each service's environment. Service-level
///    `environment` entries take precedence over `env_file` entries.
pub fn parse_compose_with_dir(
    yaml: &str,
    stack_name: &str,
    compose_dir: &Path,
) -> Result<StackSpec, StackError> {
    // Load .env from compose directory if it exists.
    let dot_env_path = compose_dir.join(".env");
    let dot_env = if dot_env_path.is_file() {
        let content = std::fs::read_to_string(&dot_env_path).map_err(|e| {
            StackError::ComposeParse(format!("failed to read {}: {e}", dot_env_path.display()))
        })?;
        parse_env_file_content(&content)
    } else {
        HashMap::new()
    };

    // Expand variables in the YAML string.
    let expanded = expand_variables(yaml, &dot_env);

    parse_compose_inner(&expanded, stack_name, Some(compose_dir))
}

fn parse_compose_inner(
    yaml: &str,
    stack_name: &str,
    compose_dir: Option<&Path>,
) -> Result<StackSpec, StackError> {
    let root: Value =
        serde_yml::from_str(yaml).map_err(|e| StackError::ComposeParse(e.to_string()))?;

    let root_map = root
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("compose file must be a YAML mapping".into()))?;

    // ── Validate top-level keys ────────────────────────────────────
    validate_top_level_keys(root_map)?;

    // ── Resolve stack name ─────────────────────────────────────────
    let name = root_map
        .get(val("name"))
        .and_then(|v| v.as_str())
        .map(String::from)
        .unwrap_or_else(|| stack_name.to_string());

    // ── Parse services ─────────────────────────────────────────────
    let services_map = root_map
        .get(val("services"))
        .and_then(|v| v.as_mapping())
        .ok_or_else(|| StackError::ComposeParse("`services` must be a YAML mapping".into()))?;

    // Collect volume names from top-level for mount validation.
    let volume_names: Vec<String> = root_map
        .get(val("volumes"))
        .and_then(|v| v.as_mapping())
        .map(|m| {
            m.keys()
                .filter_map(|k| k.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    // ── Parse top-level secrets ───────────────────────────────────
    let secrets = parse_secrets_top_level(root_map)?;
    let secret_names: Vec<&str> = secrets.iter().map(|s| s.name.as_str()).collect();

    let mut services = Vec::new();
    for (key, svc_value) in services_map {
        let svc_name = key
            .as_str()
            .ok_or_else(|| StackError::ComposeParse("service name must be a string".into()))?;
        let svc = parse_service(
            svc_name,
            svc_value,
            &volume_names,
            &secret_names,
            compose_dir,
        )?;
        services.push(svc);
    }

    // Sort services by name for deterministic output.
    services.sort_by(|a, b| a.name.cmp(&b.name));

    // ── Parse volumes ──────────────────────────────────────────────
    let volumes = parse_volumes(root_map)?;

    // ── Parse networks ──────────────────────────────────────────────
    let parsed_networks = parse_networks(root_map)?;

    // ── Apply default network membership ────────────────────────────
    let (networks, mut services) = if parsed_networks.is_empty() {
        // No custom networks: create implicit default, all services join it.
        let default_net = NetworkSpec {
            name: "default".to_string(),
            driver: "bridge".to_string(),
            subnet: None,
        };
        let services = services
            .into_iter()
            .map(|mut s| {
                if s.networks.is_empty() {
                    s.networks = vec!["default".to_string()];
                }
                s
            })
            .collect();
        (vec![default_net], services)
    } else {
        // Custom networks: services with no explicit networks join "default".
        let mut nets = parsed_networks;
        let has_default = nets.iter().any(|n| n.name == "default");
        let mut need_default = false;

        let services: Vec<ServiceSpec> = services
            .into_iter()
            .map(|mut s| {
                if s.networks.is_empty() {
                    s.networks = vec!["default".to_string()];
                    need_default = true;
                }
                s
            })
            .collect();

        if need_default && !has_default {
            nets.push(NetworkSpec {
                name: "default".to_string(),
                driver: "bridge".to_string(),
                subnet: None,
            });
        }

        (nets, services)
    };

    // Re-sort after network assignment for deterministic output.
    services.sort_by(|a, b| a.name.cmp(&b.name));

    // ── Validate dependency references ─────────────────────────────
    let service_names: Vec<&str> = services.iter().map(|s| s.name.as_str()).collect();
    for svc in &services {
        for dep in &svc.depends_on {
            if !service_names.contains(&dep.service.as_str()) {
                return Err(StackError::ComposeValidation(format!(
                    "service `{}` depends on `{}` which is not defined",
                    svc.name, dep.service,
                )));
            }
        }
    }

    // ── Validate named volume references ───────────────────────────
    for svc in &services {
        for mount in &svc.mounts {
            if let MountSpec::Named { source, .. } = mount {
                if !volume_names.contains(source) {
                    return Err(StackError::ComposeValidation(format!(
                        "service `{}` references volume `{}` which is not defined in top-level `volumes`",
                        svc.name, source,
                    )));
                }
            }
        }
    }

    // ── Validate network references ────────────────────────────────
    let final_network_names: Vec<&str> = networks.iter().map(|n| n.name.as_str()).collect();
    for svc in &services {
        for net_name in &svc.networks {
            if !final_network_names.contains(&net_name.as_str()) {
                return Err(StackError::ComposeValidation(format!(
                    "service `{}` references network `{}` which is not defined",
                    svc.name, net_name,
                )));
            }
        }
    }

    // ── Parse x-vz extensions ────────────────────────────────────────
    let disk_size_mb = parse_xvz_disk_size(root_map)?;

    Ok(StackSpec {
        name,
        services,
        networks,
        volumes,
        secrets,
        disk_size_mb,
    })
}

// ── Validation ─────────────────────────────────────────────────────

fn validate_top_level_keys(root: &serde_yml::Mapping) -> Result<(), StackError> {
    for key in root.keys() {
        let key_str = key.as_str().unwrap_or("");

        // Check rejected keys first (stable error codes).
        for &(rejected, reason) in REJECTED_TOP_LEVEL {
            if key_str == rejected {
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: rejected.to_string(),
                    reason: reason.to_string(),
                });
            }
        }

        // Check accepted keys.
        if !ACCEPTED_TOP_LEVEL.contains(&key_str) {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: key_str.to_string(),
                reason: format!(
                    "unknown top-level key; accepted keys are: {}",
                    ACCEPTED_TOP_LEVEL.join(", ")
                ),
            });
        }
    }
    Ok(())
}

fn validate_service_keys(svc_name: &str, svc_map: &serde_yml::Mapping) -> Result<(), StackError> {
    for key in svc_map.keys() {
        let key_str = key.as_str().unwrap_or("");

        // Check rejected keys first (stable error codes).
        for &(rejected, reason) in REJECTED_SERVICE {
            if key_str == rejected {
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: format!("services.{svc_name}.{rejected}"),
                    reason: reason.to_string(),
                });
            }
        }

        // Check accepted keys.
        if !ACCEPTED_SERVICE.contains(&key_str) {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.{key_str}"),
                reason: format!(
                    "unknown service key; accepted keys are: {}",
                    ACCEPTED_SERVICE.join(", ")
                ),
            });
        }
    }
    Ok(())
}

fn validate_volume_keys(vol_name: &str, vol_map: &serde_yml::Mapping) -> Result<(), StackError> {
    for key in vol_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if !ACCEPTED_VOLUME.contains(&key_str) {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("volumes.{vol_name}.{key_str}"),
                reason: format!(
                    "unknown volume key; accepted keys are: {}",
                    ACCEPTED_VOLUME.join(", ")
                ),
            });
        }
    }
    Ok(())
}

// ── Service parsing ────────────────────────────────────────────────

fn parse_service(
    name: &str,
    value: &Value,
    defined_volumes: &[String],
    defined_secrets: &[&str],
    compose_dir: Option<&Path>,
) -> Result<ServiceSpec, StackError> {
    let svc_map = value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{name}` must be a YAML mapping"))
    })?;

    validate_service_keys(name, svc_map)?;

    let image = svc_map
        .get(val("image"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            StackError::ComposeValidation(format!("service `{name}` is missing required `image`"))
        })?
        .to_string();

    let command = parse_string_or_list(svc_map, "command")?;
    let entrypoint = parse_string_or_list(svc_map, "entrypoint")?;

    // Load env_file entries first, then overlay with explicit environment.
    let mut environment = load_env_file_entries(name, svc_map, compose_dir)?;
    let explicit_env = parse_environment(name, svc_map)?;
    environment.extend(explicit_env);

    let working_dir = svc_map
        .get(val("working_dir"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let user = svc_map
        .get(val("user"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let ports = parse_ports(name, svc_map)?;
    let mut mounts = parse_mounts(name, svc_map, defined_volumes)?;
    let tmpfs_mounts = parse_tmpfs(name, svc_map)?;
    mounts.extend(tmpfs_mounts);
    let depends_on = parse_depends_on(name, svc_map)?;
    let healthcheck = parse_healthcheck(name, svc_map)?;
    let restart_policy = parse_restart(name, svc_map)?;
    let extra_hosts = parse_extra_hosts(name, svc_map)?;
    let resources = parse_deploy(name, svc_map)?;
    let secrets = parse_service_secrets(name, svc_map, defined_secrets)?;
    let networks = parse_service_networks(name, svc_map)?;

    // Security fields
    let cap_add = parse_string_list(name, svc_map, "cap_add")?;
    let cap_drop = parse_string_list(name, svc_map, "cap_drop")?;
    let privileged = svc_map
        .get(val("privileged"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let read_only = svc_map
        .get(val("read_only"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let sysctls = parse_string_map(name, svc_map, "sysctls")?;

    // Resource extensions
    let ulimits = parse_ulimits(name, svc_map)?;
    let pids_limit = svc_map
        .get(val("pids_limit"))
        .map(|v| {
            v.as_i64().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{name}`: `pids_limit` must be an integer"
                ))
            })
        })
        .transpose()?;

    // Container identity
    let container_name = svc_map
        .get(val("container_name"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let hostname = svc_map
        .get(val("hostname"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let domainname = svc_map
        .get(val("domainname"))
        .and_then(|v| v.as_str())
        .map(String::from);
    let labels = parse_labels(name, svc_map)?;

    // Stop lifecycle
    let stop_signal = svc_map
        .get(val("stop_signal"))
        .map(|v| {
            v.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{name}`: `stop_signal` must be a string"
                ))
            })
        })
        .transpose()?
        .map(String::from);

    let stop_grace_period_secs = svc_map
        .get(val("stop_grace_period"))
        .map(|v| {
            let s = v.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{name}`: `stop_grace_period` must be a duration string"
                ))
            })?;
            parse_duration_string(s).ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{name}`: invalid duration `{s}` for `stop_grace_period`"
                ))
            })
        })
        .transpose()?;

    // Merge pids_limit: service-level overrides deploy-level when present.
    let resources = ResourcesSpec {
        pids_limit: pids_limit.or(resources.pids_limit),
        ..resources
    };

    Ok(ServiceSpec {
        name: name.to_string(),
        image,
        command,
        entrypoint,
        environment,
        working_dir,
        user,
        mounts,
        ports,
        depends_on,
        healthcheck,
        restart_policy,
        resources,
        extra_hosts,
        secrets,
        networks,
        cap_add,
        cap_drop,
        privileged,
        read_only,
        sysctls,
        ulimits,
        container_name,
        hostname,
        domainname,
        labels,
        stop_signal,
        stop_grace_period_secs,
    })
}

// ── Field parsers ──────────────────────────────────────────────────

/// Parse a field that can be either a single string or a list of strings.
///
/// Compose allows both `command: "foo bar"` (shell form, split on spaces)
/// and `command: ["foo", "bar"]` (exec form).
fn parse_string_or_list(
    map: &serde_yml::Mapping,
    key: &str,
) -> Result<Option<Vec<String>>, StackError> {
    let Some(value) = map.get(val(key)) else {
        return Ok(None);
    };

    if let Some(s) = value.as_str() {
        // Shell form: split on whitespace.
        Ok(Some(s.split_whitespace().map(String::from).collect()))
    } else if let Some(seq) = value.as_sequence() {
        let items: Result<Vec<String>, _> = seq
            .iter()
            .map(|v| {
                v.as_str().map(String::from).ok_or_else(|| {
                    StackError::ComposeParse(format!("`{key}` list items must be strings"))
                })
            })
            .collect();
        Ok(Some(items?))
    } else {
        Err(StackError::ComposeParse(format!(
            "`{key}` must be a string or list of strings"
        )))
    }
}

/// Parse environment variables from Compose format.
///
/// Supports both object and list forms:
/// ```yaml
/// environment:
///   KEY: value
/// ```
/// or:
/// ```yaml
/// environment:
///   - KEY=value
/// ```
fn parse_environment(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<HashMap<String, String>, StackError> {
    let Some(value) = map.get(val("environment")) else {
        return Ok(HashMap::new());
    };

    if let Some(obj) = value.as_mapping() {
        let mut env = HashMap::new();
        for (k, v) in obj {
            let key = k.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: environment key must be a string"
                ))
            })?;
            // Values can be strings, numbers, or booleans in YAML.
            let val_str = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => String::new(),
                _ => {
                    return Err(StackError::ComposeParse(format!(
                        "service `{svc_name}`: environment value for `{key}` must be a scalar"
                    )));
                }
            };
            env.insert(key.to_string(), val_str);
        }
        Ok(env)
    } else if let Some(seq) = value.as_sequence() {
        let mut env = HashMap::new();
        for item in seq {
            let s = item.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: environment list items must be strings"
                ))
            })?;
            if let Some((key, val)) = s.split_once('=') {
                env.insert(key.to_string(), val.to_string());
            } else {
                // Bare key without value.
                env.insert(s.to_string(), String::new());
            }
        }
        Ok(env)
    } else {
        Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `environment` must be a mapping or list"
        )))
    }
}

/// Parse port mappings from Compose format.
///
/// Supports:
/// - Short syntax: `"8080:80"`, `"8080:80/udp"`, `"80"`
/// - Long syntax: `{ target: 80, published: 8080, protocol: tcp }`
fn parse_ports(svc_name: &str, map: &serde_yml::Mapping) -> Result<Vec<PortSpec>, StackError> {
    let Some(value) = map.get(val("ports")) else {
        return Ok(vec![]);
    };

    let seq = value.as_sequence().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `ports` must be a list"))
    })?;

    let mut ports = Vec::new();
    for item in seq {
        if let Some(s) = item.as_str() {
            ports.push(parse_port_short(svc_name, s)?);
        } else if item.as_u64().is_some() {
            // Bare number like `ports: [80]`.
            let n = item.as_u64().unwrap_or(0);
            let port = u16::try_from(n).map_err(|_| {
                StackError::ComposeParse(format!("service `{svc_name}`: port {n} is out of range"))
            })?;
            ports.push(PortSpec {
                protocol: "tcp".to_string(),
                container_port: port,
                host_port: None,
            });
        } else if let Some(obj) = item.as_mapping() {
            ports.push(parse_port_long(svc_name, obj)?);
        } else {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: port entry must be a string, number, or mapping"
            )));
        }
    }
    Ok(ports)
}

/// Parse short-form port syntax: `"[host:]container[/protocol]"`.
fn parse_port_short(svc_name: &str, s: &str) -> Result<PortSpec, StackError> {
    // Split off protocol suffix.
    let (port_part, protocol) = if let Some((p, proto)) = s.rsplit_once('/') {
        (p, proto.to_string())
    } else {
        (s, "tcp".to_string())
    };

    // Split host:container.
    let (host_port, container_port) = if let Some((host, container)) = port_part.rsplit_once(':') {
        // Handle optional bind address: "127.0.0.1:8080:80" → ignore IP, take last two.
        let host_str = if host.contains(':') {
            // Has bind address — take the part after the last colon.
            host.rsplit_once(':').map(|(_, h)| h).unwrap_or(host)
        } else {
            host
        };
        let h: u16 = host_str.parse().map_err(|_| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: invalid host port `{host_str}` in `{s}`"
            ))
        })?;
        let c: u16 = container.parse().map_err(|_| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: invalid container port `{container}` in `{s}`"
            ))
        })?;
        (Some(h), c)
    } else {
        let c: u16 = port_part.parse().map_err(|_| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: invalid port `{port_part}` in `{s}`"
            ))
        })?;
        (None, c)
    };

    Ok(PortSpec {
        protocol,
        container_port,
        host_port,
    })
}

/// Parse long-form port mapping: `{ target, published, protocol }`.
fn parse_port_long(svc_name: &str, obj: &serde_yml::Mapping) -> Result<PortSpec, StackError> {
    let target = obj
        .get(val("target"))
        .and_then(|v| v.as_u64())
        .ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: port long-form requires `target`"
            ))
        })?;
    let container_port = u16::try_from(target).map_err(|_| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: port target {target} is out of range"
        ))
    })?;

    let host_port = obj
        .get(val("published"))
        .and_then(|v| v.as_u64())
        .map(|n| {
            u16::try_from(n).map_err(|_| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: port published {n} is out of range"
                ))
            })
        })
        .transpose()?;

    let protocol = obj
        .get(val("protocol"))
        .and_then(|v| v.as_str())
        .unwrap_or("tcp")
        .to_string();

    Ok(PortSpec {
        protocol,
        container_port,
        host_port,
    })
}

/// Parse volume/mount entries for a service.
///
/// Supports:
/// - Short syntax: `"source:target[:ro]"`, `"/path"` (ephemeral)
/// - Long syntax: `{ type, source, target, read_only }` (not yet needed)
fn parse_mounts(
    svc_name: &str,
    map: &serde_yml::Mapping,
    defined_volumes: &[String],
) -> Result<Vec<MountSpec>, StackError> {
    let Some(value) = map.get(val("volumes")) else {
        return Ok(vec![]);
    };

    let seq = value.as_sequence().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `volumes` must be a list"))
    })?;

    let mut mounts = Vec::new();
    for item in seq {
        if let Some(s) = item.as_str() {
            mounts.push(parse_mount_short(svc_name, s, defined_volumes)?);
        } else if let Some(obj) = item.as_mapping() {
            mounts.push(parse_mount_long(svc_name, obj)?);
        } else {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: volume entry must be a string or mapping"
            )));
        }
    }
    Ok(mounts)
}

/// Parse the `tmpfs` service key into ephemeral mounts.
///
/// Docker Compose allows `tmpfs` as a single path string or a list of paths:
/// ```yaml
/// tmpfs: /run
/// # or
/// tmpfs:
///   - /run
///   - /tmp
/// ```
fn parse_tmpfs(svc_name: &str, map: &serde_yml::Mapping) -> Result<Vec<MountSpec>, StackError> {
    let Some(value) = map.get(val("tmpfs")) else {
        return Ok(vec![]);
    };

    let paths: Vec<&str> = if let Some(s) = value.as_str() {
        vec![s]
    } else if let Some(seq) = value.as_sequence() {
        seq.iter()
            .map(|v| {
                v.as_str().ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: `tmpfs` entries must be strings"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        return Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `tmpfs` must be a string or list of strings"
        )));
    };

    paths
        .into_iter()
        .map(|path| {
            if !path.starts_with('/') {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: tmpfs path `{path}` must be an absolute path"
                )));
            }
            Ok(MountSpec::Ephemeral {
                target: path.to_string(),
            })
        })
        .collect()
}

/// Parse short-form mount syntax: `"source:target[:ro]"` or `"/target"`.
fn parse_mount_short(
    svc_name: &str,
    s: &str,
    defined_volumes: &[String],
) -> Result<MountSpec, StackError> {
    let parts: Vec<&str> = s.split(':').collect();

    match parts.len() {
        1 => {
            // Single path: ephemeral mount if it starts with /, otherwise error.
            let path = parts[0];
            if path.starts_with('/') {
                Ok(MountSpec::Ephemeral {
                    target: path.to_string(),
                })
            } else {
                Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: volume `{s}` must be an absolute path or `source:target`"
                )))
            }
        }
        2 | 3 => {
            let source = parts[0];
            let target = parts[1];
            let read_only = parts.get(2).is_some_and(|&opt| opt == "ro");

            if !target.starts_with('/') {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: mount target `{target}` must be an absolute path"
                )));
            }

            // Determine mount type by source format.
            if source.starts_with('/') || source.starts_with('.') {
                // Absolute or relative host path → bind mount.
                Ok(MountSpec::Bind {
                    source: source.to_string(),
                    target: target.to_string(),
                    read_only,
                })
            } else if defined_volumes.contains(&source.to_string()) {
                // Named volume.
                Ok(MountSpec::Named {
                    source: source.to_string(),
                    target: target.to_string(),
                    read_only,
                })
            } else {
                // Source doesn't look like a path and isn't a defined volume.
                // Treat as a named volume reference — validation later will
                // catch undefined volumes.
                Ok(MountSpec::Named {
                    source: source.to_string(),
                    target: target.to_string(),
                    read_only,
                })
            }
        }
        _ => Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: invalid mount syntax `{s}`"
        ))),
    }
}

/// Parse long-form mount: `{ type, source, target, read_only }`.
fn parse_mount_long(svc_name: &str, obj: &serde_yml::Mapping) -> Result<MountSpec, StackError> {
    let mount_type = obj
        .get(val("type"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: long-form volume requires `type`"
            ))
        })?;
    let target = obj
        .get(val("target"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: long-form volume requires `target`"
            ))
        })?
        .to_string();
    let read_only = obj
        .get(val("read_only"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    match mount_type {
        "bind" => {
            let source = obj
                .get(val("source"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: bind mount requires `source`"
                    ))
                })?
                .to_string();
            Ok(MountSpec::Bind {
                source,
                target,
                read_only,
            })
        }
        "volume" => {
            let source = obj
                .get(val("source"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: volume mount requires `source`"
                    ))
                })?
                .to_string();
            Ok(MountSpec::Named {
                source,
                target,
                read_only,
            })
        }
        "tmpfs" => Ok(MountSpec::Ephemeral { target }),
        _ => Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: unsupported mount type `{mount_type}`"
        ))),
    }
}

/// Parse `depends_on` which can be a list of strings or a mapping with conditions.
fn parse_depends_on(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Vec<ServiceDependency>, StackError> {
    let Some(value) = map.get(val("depends_on")) else {
        return Ok(vec![]);
    };

    if let Some(seq) = value.as_sequence() {
        // Simple list: depends_on: [db, cache]
        seq.iter()
            .map(|v| {
                v.as_str().map(ServiceDependency::started).ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: `depends_on` items must be strings"
                    ))
                })
            })
            .collect()
    } else if let Some(obj) = value.as_mapping() {
        // Conditional form: depends_on: { db: { condition: service_healthy } }
        obj.iter()
            .map(|(k, v)| {
                let dep_name = k.as_str().ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: `depends_on` keys must be strings"
                    ))
                })?;
                let condition = parse_dependency_condition(svc_name, dep_name, v)?;
                Ok(ServiceDependency {
                    service: dep_name.to_string(),
                    condition,
                })
            })
            .collect()
    } else {
        Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `depends_on` must be a list or mapping"
        )))
    }
}

/// Extract the condition from a depends_on mapping value.
///
/// The value can be a mapping like `{ condition: service_healthy }` or
/// omitted/empty (defaults to `service_started`).
fn parse_dependency_condition(
    svc_name: &str,
    dep_name: &str,
    value: &serde_yml::Value,
) -> Result<DependencyCondition, StackError> {
    let Some(obj) = value.as_mapping() else {
        // No condition specified → default.
        return Ok(DependencyCondition::ServiceStarted);
    };

    let Some(cond_val) = obj.get(val("condition")) else {
        return Ok(DependencyCondition::ServiceStarted);
    };

    let cond_str = cond_val.as_str().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: depends_on `{dep_name}` condition must be a string"
        ))
    })?;

    match cond_str {
        "service_started" => Ok(DependencyCondition::ServiceStarted),
        "service_healthy" => Ok(DependencyCondition::ServiceHealthy),
        "service_completed_successfully" => Ok(DependencyCondition::ServiceCompletedSuccessfully),
        other => Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: depends_on `{dep_name}` unknown condition `{other}`"
        ))),
    }
}

/// Parse healthcheck configuration.
fn parse_healthcheck(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Option<HealthCheckSpec>, StackError> {
    let Some(value) = map.get(val("healthcheck")) else {
        return Ok(None);
    };

    let obj = value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: `healthcheck` must be a mapping"
        ))
    })?;

    // `disable: true` means no health check.
    if obj
        .get(val("disable"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false)
    {
        return Ok(None);
    }

    let test = obj
        .get(val("test"))
        .ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: `healthcheck` requires `test`"
            ))
        })
        .and_then(|v| {
            if let Some(seq) = v.as_sequence() {
                seq.iter()
                    .map(|item| {
                        item.as_str().map(String::from).ok_or_else(|| {
                            StackError::ComposeParse(format!(
                                "service `{svc_name}`: healthcheck test items must be strings"
                            ))
                        })
                    })
                    .collect()
            } else if let Some(s) = v.as_str() {
                // Shell form: `test: curl localhost`
                Ok(vec!["CMD-SHELL".to_string(), s.to_string()])
            } else {
                Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: healthcheck `test` must be a string or list"
                )))
            }
        })?;

    let interval_secs = parse_duration_field(svc_name, obj, "interval")?;
    let timeout_secs = parse_duration_field(svc_name, obj, "timeout")?;
    let start_period_secs = parse_duration_field(svc_name, obj, "start_period")?;
    let retries = obj
        .get(val("retries"))
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);

    Ok(Some(HealthCheckSpec {
        test,
        interval_secs,
        timeout_secs,
        retries,
        start_period_secs,
    }))
}

/// Parse a Compose duration field (e.g., `interval: 30s`, `timeout: 5s`).
///
/// Supports:
/// - Bare seconds as integer: `30`
/// - Duration string: `"30s"`, `"1m"`, `"1m30s"`
fn parse_duration_field(
    svc_name: &str,
    obj: &serde_yml::Mapping,
    key: &str,
) -> Result<Option<u64>, StackError> {
    let Some(value) = obj.get(val(key)) else {
        return Ok(None);
    };

    if let Some(n) = value.as_u64() {
        return Ok(Some(n));
    }

    if let Some(s) = value.as_str() {
        return parse_duration_string(s).map(Some).ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: invalid duration `{s}` for `{key}`"
            ))
        });
    }

    Err(StackError::ComposeParse(format!(
        "service `{svc_name}`: `{key}` must be a number or duration string"
    )))
}

/// Parse a Compose-style duration string into seconds.
///
/// Supports: `"30s"`, `"5m"`, `"1h"`, `"1m30s"`.
fn parse_duration_string(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    let mut total: u64 = 0;
    let mut current = String::new();

    for ch in s.chars() {
        if ch.is_ascii_digit() {
            current.push(ch);
        } else {
            let n: u64 = current.parse().ok()?;
            current.clear();
            match ch {
                'h' => total += n * 3600,
                'm' => total += n * 60,
                's' => total += n,
                _ => return None,
            }
        }
    }

    // If trailing digits without a unit, treat as seconds.
    if !current.is_empty() {
        let n: u64 = current.parse().ok()?;
        total += n;
    }

    if total == 0 && s != "0s" && s != "0" {
        return None;
    }

    Some(total)
}

/// Parse restart policy string.
fn parse_restart(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Option<RestartPolicy>, StackError> {
    let Some(value) = map.get(val("restart")) else {
        return Ok(None);
    };

    let s = value.as_str().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `restart` must be a string"))
    })?;

    match s {
        "no" => Ok(Some(RestartPolicy::No)),
        "always" => Ok(Some(RestartPolicy::Always)),
        "unless-stopped" => Ok(Some(RestartPolicy::UnlessStopped)),
        _ if s.starts_with("on-failure") => {
            let max_retries = if let Some(count_str) = s.strip_prefix("on-failure:") {
                let count: u32 = count_str.parse().map_err(|_| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: invalid retry count in `{s}`"
                    ))
                })?;
                Some(count)
            } else {
                None
            };
            Ok(Some(RestartPolicy::OnFailure { max_retries }))
        }
        _ => Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: unknown restart policy `{s}`; \
             accepted values: no, always, on-failure, on-failure:N, unless-stopped"
        ))),
    }
}

/// Parse `extra_hosts` entries in `"hostname:ip"` format.
fn parse_extra_hosts(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Vec<(String, String)>, StackError> {
    let Some(value) = map.get(val("extra_hosts")) else {
        return Ok(vec![]);
    };

    let seq = value.as_sequence().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: `extra_hosts` must be a list"
        ))
    })?;

    let mut hosts = Vec::with_capacity(seq.len());
    for entry in seq {
        let s = entry.as_str().ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: `extra_hosts` entries must be strings"
            ))
        })?;

        let (hostname, ip) = s.split_once(':').ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: `extra_hosts` entry must be \"hostname:ip\", got \"{s}\""
            ))
        })?;

        hosts.push((hostname.to_string(), ip.to_string()));
    }

    Ok(hosts)
}

/// Parse `deploy.resources` into [`ResourcesSpec`].
///
/// Accepts both `deploy.resources.limits` and `deploy.resources.reservations`
/// sub-keys. Each may contain `cpus` and `memory`.
fn parse_deploy(svc_name: &str, map: &serde_yml::Mapping) -> Result<ResourcesSpec, StackError> {
    let Some(deploy_value) = map.get(val("deploy")) else {
        return Ok(ResourcesSpec::default());
    };

    if deploy_value.is_null() {
        return Ok(ResourcesSpec::default());
    }

    let deploy_map = deploy_value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `deploy` must be a mapping"))
    })?;

    // Accept `resources` and `replicas` under deploy.
    let mut replicas: u32 = 1;
    for key in deploy_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "resources" && key_str != "replicas" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.deploy.{key_str}"),
                reason: "only `deploy.resources` and `deploy.replicas` are supported".to_string(),
            });
        }
    }

    // Parse replicas if present.
    if let Some(replicas_value) = deploy_map.get(val("replicas")) {
        if !replicas_value.is_null() {
            replicas = replicas_value.as_i64().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `deploy.replicas` must be a number"
                ))
            })? as u32;
            if replicas == 0 {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `deploy.replicas` must be at least 1"
                )));
            }
        }
    }

    let Some(resources_value) = deploy_map.get(val("resources")) else {
        // No resources section, but we might have replicas - return default with replicas
        let mut spec = ResourcesSpec::default();
        spec.replicas = replicas;
        return Ok(spec);
    };

    if resources_value.is_null() {
        // resources is null, but we might have replicas
        let mut spec = ResourcesSpec::default();
        spec.replicas = replicas;
        return Ok(spec);
    }

    let resources_map = resources_value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: `deploy.resources` must be a mapping"
        ))
    })?;

    // `limits` and `reservations` are accepted under resources.
    for key in resources_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "limits" && key_str != "reservations" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.deploy.resources.{key_str}"),
                reason: "only `deploy.resources.limits` and `deploy.resources.reservations` are supported".to_string(),
            });
        }
    }

    let (cpus, memory_bytes, pids_limit) = parse_limits_sub_section(svc_name, resources_map)?;
    let (reservation_cpus, reservation_memory_bytes) =
        parse_resource_sub_section(svc_name, resources_map, "reservations")?;

    Ok(ResourcesSpec {
        cpus,
        memory_bytes,
        reservation_cpus,
        reservation_memory_bytes,
        pids_limit,
        replicas,
    })
}

/// Parse the `limits` sub-section under `deploy.resources`, including `pids`.
///
/// Returns `(cpus, memory_bytes, pids_limit)`.
#[allow(clippy::type_complexity)]
fn parse_limits_sub_section(
    svc_name: &str,
    resources_map: &serde_yml::Mapping,
) -> Result<(Option<f64>, Option<u64>, Option<i64>), StackError> {
    let section = "limits";
    let Some(section_value) = resources_map.get(val(section)) else {
        return Ok((None, None, None));
    };

    if section_value.is_null() {
        return Ok((None, None, None));
    }

    let section_map = section_value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: `deploy.resources.{section}` must be a mapping"
        ))
    })?;

    for key in section_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "cpus" && key_str != "memory" && key_str != "pids" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.deploy.resources.{section}.{key_str}"),
                reason: format!(
                    "only `cpus`, `memory`, and `pids` are supported under `{section}`"
                ),
            });
        }
    }

    let cpus = section_map
        .get(val("cpus"))
        .map(|v| {
            if let Some(s) = v.as_str() {
                s.parse::<f64>().map_err(|_| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: invalid `deploy.resources.{section}.cpus` value `{s}`"
                    ))
                })
            } else if let Some(f) = v.as_f64() {
                Ok(f)
            } else {
                Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `deploy.resources.{section}.cpus` must be a number or string"
                )))
            }
        })
        .transpose()?;

    let memory_bytes = section_map
        .get(val("memory"))
        .map(|v| {
            let s = v.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `deploy.resources.{section}.memory` must be a string (e.g., \"512m\", \"1g\")"
                ))
            })?;
            parse_memory_string(svc_name, s)
        })
        .transpose()?;

    let pids_limit = section_map
        .get(val("pids"))
        .map(|v| {
            v.as_i64().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `deploy.resources.{section}.pids` must be an integer"
                ))
            })
        })
        .transpose()?;

    Ok((cpus, memory_bytes, pids_limit))
}

/// Parse a `limits` or `reservations` sub-section under `deploy.resources`.
fn parse_resource_sub_section(
    svc_name: &str,
    resources_map: &serde_yml::Mapping,
    section: &str,
) -> Result<(Option<f64>, Option<u64>), StackError> {
    let Some(section_value) = resources_map.get(val(section)) else {
        return Ok((None, None));
    };

    if section_value.is_null() {
        return Ok((None, None));
    }

    let section_map = section_value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: `deploy.resources.{section}` must be a mapping"
        ))
    })?;

    for key in section_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "cpus" && key_str != "memory" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.deploy.resources.{section}.{key_str}"),
                reason: format!("only `cpus` and `memory` are supported under `{section}`"),
            });
        }
    }

    let cpus = section_map
        .get(val("cpus"))
        .map(|v| {
            if let Some(s) = v.as_str() {
                s.parse::<f64>().map_err(|_| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: invalid `deploy.resources.{section}.cpus` value `{s}`"
                    ))
                })
            } else if let Some(f) = v.as_f64() {
                Ok(f)
            } else {
                Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `deploy.resources.{section}.cpus` must be a number or string"
                )))
            }
        })
        .transpose()?;

    let memory_bytes = section_map
        .get(val("memory"))
        .map(|v| {
            let s = v.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `deploy.resources.{section}.memory` must be a string (e.g., \"512m\", \"1g\")"
                ))
            })?;
            parse_memory_string(svc_name, s)
        })
        .transpose()?;

    Ok((cpus, memory_bytes))
}

/// Parse a Docker Compose memory string into bytes.
///
/// Supports:
/// - `"512m"` or `"512M"` → 512 * 1024 * 1024
/// - `"1g"` or `"1G"` → 1 * 1024 * 1024 * 1024
/// - `"256k"` or `"256K"` → 256 * 1024
/// - `"1024b"` or `"1024B"` or `"1024"` → 1024
fn parse_memory_string(svc_name: &str, s: &str) -> Result<u64, StackError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: empty memory value"
        )));
    }

    let lower = s.to_lowercase();
    let (num_str, multiplier) = if let Some(n) = lower.strip_suffix('g') {
        (n, 1024u64 * 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix('m') {
        (n, 1024u64 * 1024)
    } else if let Some(n) = lower.strip_suffix('k') {
        (n, 1024u64)
    } else if let Some(n) = lower.strip_suffix('b') {
        (n, 1u64)
    } else {
        (lower.as_str(), 1u64)
    };

    let val: u64 = num_str.trim().parse().map_err(|_| {
        StackError::ComposeParse(format!("service `{svc_name}`: invalid memory value `{s}`"))
    })?;

    Ok(val * multiplier)
}

// ── env_file and variable expansion ─────────────────────────────────

/// Parse an `.env` file's content into key-value pairs.
///
/// Supports:
/// - `KEY=VALUE` lines
/// - Lines starting with `#` are comments
/// - Blank lines are ignored
/// - Optional quoting: `KEY="value"` or `KEY='value'` (quotes stripped)
/// - Lines prefixed with `export ` are accepted
pub fn parse_env_file_content(content: &str) -> HashMap<String, String> {
    let mut env = HashMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        // Strip optional `export ` prefix.
        let line = line.strip_prefix("export ").unwrap_or(line);
        let Some((key, val)) = line.split_once('=') else {
            continue;
        };
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let val = val.trim();
        // Strip surrounding quotes.
        let val = if (val.starts_with('"') && val.ends_with('"'))
            || (val.starts_with('\'') && val.ends_with('\''))
        {
            &val[1..val.len() - 1]
        } else {
            val
        };
        env.insert(key.to_string(), val.to_string());
    }
    env
}

/// Expand `${VAR}`, `${VAR:-default}`, and `$VAR` references in a string.
///
/// Looks up each variable in `vars`. If not found:
/// - `${VAR}` / `$VAR` → empty string
/// - `${VAR:-default}` → the default value
pub fn expand_variables(input: &str, vars: &HashMap<String, String>) -> String {
    let mut result = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        if chars[i] == '$' && i + 1 < len {
            if chars[i + 1] == '{' {
                // ${VAR} or ${VAR:-default}
                if let Some(close) = chars[i + 2..].iter().position(|&c| c == '}') {
                    let inner: String = chars[i + 2..i + 2 + close].iter().collect();
                    let value = if let Some((name, default)) = inner.split_once(":-") {
                        vars.get(name)
                            .filter(|v| !v.is_empty())
                            .map(|v| v.as_str())
                            .unwrap_or(default)
                    } else {
                        vars.get(inner.as_str()).map(|v| v.as_str()).unwrap_or("")
                    };
                    result.push_str(value);
                    i += 2 + close + 1; // skip past '}'
                } else {
                    // Unclosed brace, emit literally.
                    result.push('$');
                    i += 1;
                }
            } else if chars[i + 1] == '$' {
                // $$ → literal $
                result.push('$');
                i += 2;
            } else if chars[i + 1].is_ascii_alphabetic() || chars[i + 1] == '_' {
                // $VAR (simple form)
                let start = i + 1;
                let mut end = start;
                while end < len && (chars[end].is_ascii_alphanumeric() || chars[end] == '_') {
                    end += 1;
                }
                let name: String = chars[start..end].iter().collect();
                let value = vars.get(name.as_str()).map(|v| v.as_str()).unwrap_or("");
                result.push_str(value);
                i = end;
            } else {
                // Not a variable reference.
                result.push('$');
                i += 1;
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }

    result
}

/// Load `env_file` entries for a service into a HashMap.
///
/// If `compose_dir` is `None`, `env_file` directives are silently ignored.
/// If `compose_dir` is `Some`, file paths are resolved relative to it.
fn load_env_file_entries(
    svc_name: &str,
    map: &serde_yml::Mapping,
    compose_dir: Option<&Path>,
) -> Result<HashMap<String, String>, StackError> {
    let Some(value) = map.get(val("env_file")) else {
        return Ok(HashMap::new());
    };

    let compose_dir = match compose_dir {
        Some(d) => d,
        None => return Ok(HashMap::new()),
    };

    let paths = if let Some(s) = value.as_str() {
        vec![s.to_string()]
    } else if let Some(seq) = value.as_sequence() {
        seq.iter()
            .map(|v| {
                v.as_str().map(String::from).ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: `env_file` entries must be strings"
                    ))
                })
            })
            .collect::<Result<Vec<_>, _>>()?
    } else {
        return Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `env_file` must be a string or list of strings"
        )));
    };

    let mut env = HashMap::new();
    for path_str in &paths {
        let env_path = compose_dir.join(path_str);
        let content = std::fs::read_to_string(&env_path).map_err(|e| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: failed to read env_file `{}`: {e}",
                env_path.display()
            ))
        })?;
        // Later files override earlier ones.
        env.extend(parse_env_file_content(&content));
    }

    Ok(env)
}

// ── Secrets parsing ────────────────────────────────────────────────

/// Parse top-level `secrets` definitions.
///
/// Each secret must have a `file:` key pointing to a host file path.
/// External secrets (`external: true`) are rejected.
fn parse_secrets_top_level(root: &serde_yml::Mapping) -> Result<Vec<SecretDef>, StackError> {
    let Some(value) = root.get(val("secrets")) else {
        return Ok(vec![]);
    };

    let secrets_map = value
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("top-level `secrets` must be a mapping".into()))?;

    let mut secrets = Vec::new();
    for (key, secret_value) in secrets_map {
        let secret_name = key
            .as_str()
            .ok_or_else(|| StackError::ComposeParse("secret name must be a string".into()))?;

        let secret_map = secret_value.as_mapping().ok_or_else(|| {
            StackError::ComposeParse(format!(
                "secret `{secret_name}` must be a mapping with a `file` key"
            ))
        })?;

        // Reject external secrets.
        if secret_map
            .get(val("external"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
        {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("secrets.{secret_name}.external"),
                reason: "external secrets are not supported; use file-based secrets".to_string(),
            });
        }

        let file = secret_map
            .get(val("file"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                StackError::ComposeValidation(format!(
                    "secret `{secret_name}` is missing required `file` key"
                ))
            })?
            .to_string();

        secrets.push(SecretDef {
            name: secret_name.to_string(),
            file,
        });
    }

    // Sort for determinism.
    secrets.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(secrets)
}

/// Parse service-level `secrets` references.
///
/// Supports:
/// - Short form: just a string name (source and target both set to the name)
/// - Long form: mapping with `source` and optional `target` keys
///
/// Each referenced secret must be defined in the top-level `secrets` section.
fn parse_service_secrets(
    svc_name: &str,
    map: &serde_yml::Mapping,
    defined_secrets: &[&str],
) -> Result<Vec<ServiceSecretRef>, StackError> {
    let Some(value) = map.get(val("secrets")) else {
        return Ok(vec![]);
    };

    let seq = value.as_sequence().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `secrets` must be a list"))
    })?;

    let mut refs = Vec::new();
    for item in seq {
        let secret_ref = if let Some(s) = item.as_str() {
            // Short form: just the secret name.
            if !defined_secrets.contains(&s) {
                return Err(StackError::ComposeValidation(format!(
                    "service `{svc_name}` references secret `{s}` which is not defined in top-level `secrets`"
                )));
            }
            ServiceSecretRef {
                source: s.to_string(),
                target: s.to_string(),
            }
        } else if let Some(obj) = item.as_mapping() {
            // Long form: { source: ..., target: ... }
            let source = obj
                .get(val("source"))
                .and_then(|v| v.as_str())
                .ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: secret long-form requires `source`"
                    ))
                })?
                .to_string();

            if !defined_secrets.contains(&source.as_str()) {
                return Err(StackError::ComposeValidation(format!(
                    "service `{svc_name}` references secret `{source}` which is not defined in top-level `secrets`"
                )));
            }

            let target = obj
                .get(val("target"))
                .and_then(|v| v.as_str())
                .map(String::from)
                .unwrap_or_else(|| source.clone());

            ServiceSecretRef { source, target }
        } else {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: secret entry must be a string or mapping"
            )));
        };
        refs.push(secret_ref);
    }
    Ok(refs)
}

// ── Volume parsing ─────────────────────────────────────────────────

fn parse_volumes(root: &serde_yml::Mapping) -> Result<Vec<VolumeSpec>, StackError> {
    let Some(value) = root.get(val("volumes")) else {
        return Ok(vec![]);
    };

    let volumes_map = value
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("top-level `volumes` must be a mapping".into()))?;

    let mut volumes = Vec::new();
    for (key, vol_value) in volumes_map {
        let vol_name = key
            .as_str()
            .ok_or_else(|| StackError::ComposeParse("volume name must be a string".into()))?;

        // Empty value (just the name) is valid — uses defaults.
        if vol_value.is_null() {
            volumes.push(VolumeSpec {
                name: vol_name.to_string(),
                driver: "local".to_string(),
                driver_opts: None,
            });
            continue;
        }

        let vol_map = vol_value.as_mapping().ok_or_else(|| {
            StackError::ComposeParse(format!("volume `{vol_name}` must be a mapping or empty"))
        })?;

        validate_volume_keys(vol_name, vol_map)?;

        let driver = vol_map
            .get(val("driver"))
            .and_then(|v| v.as_str())
            .unwrap_or("local")
            .to_string();

        if driver != "local" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("volumes.{vol_name}.driver"),
                reason: format!("only `local` driver is supported; got `{driver}`"),
            });
        }

        let driver_opts = vol_map
            .get(val("driver_opts"))
            .and_then(|v| v.as_mapping())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| Some((k.as_str()?.to_string(), v.as_str()?.to_string())))
                    .collect::<HashMap<String, String>>()
            });

        volumes.push(VolumeSpec {
            name: vol_name.to_string(),
            driver,
            driver_opts,
        });
    }

    // Sort for determinism.
    volumes.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(volumes)
}

// ── x-vz extension parsing ────────────────────────────────────────

/// Parse the top-level `x-vz` extension section.
///
/// Currently supports:
/// ```yaml
/// x-vz:
///   disk_size: "20g"   # persistent volume disk size (human-readable)
/// ```
///
/// Size strings accept `g`/`gb`, `m`/`mb`, `k`/`kb` suffixes (case-insensitive).
/// Plain numbers are treated as megabytes.
fn parse_xvz_disk_size(root: &serde_yml::Mapping) -> Result<Option<u64>, StackError> {
    let Some(xvz_value) = root.get(val("x-vz")) else {
        return Ok(None);
    };
    let xvz_map = xvz_value
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("`x-vz` must be a mapping".into()))?;

    let Some(size_value) = xvz_map.get(val("disk_size")) else {
        return Ok(None);
    };

    // Accept integer (megabytes) or string with unit suffix.
    if let Some(n) = size_value.as_u64() {
        return Ok(Some(n));
    }
    if let Some(s) = size_value.as_str() {
        let s = s.trim().to_lowercase();
        return parse_size_to_mb(&s).map(Some).ok_or_else(|| {
            StackError::ComposeParse(format!(
                "x-vz.disk_size: invalid size `{s}`; use e.g. `10g`, `512m`, `1024`"
            ))
        });
    }

    Err(StackError::ComposeParse(
        "x-vz.disk_size must be a number (MB) or string with unit (e.g., `10g`, `512m`)".into(),
    ))
}

/// Parse a human-readable size string to megabytes.
///
/// Accepts: `"10g"`, `"10gb"`, `"512m"`, `"512mb"`, `"1024"` (plain = MB).
fn parse_size_to_mb(s: &str) -> Option<u64> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Strip unit suffix and compute multiplier.
    let (num_str, multiplier) = if let Some(n) = s.strip_suffix("gb") {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix('g') {
        (n.trim(), 1024u64)
    } else if let Some(n) = s.strip_suffix("mb") {
        (n.trim(), 1u64)
    } else if let Some(n) = s.strip_suffix('m') {
        (n.trim(), 1u64)
    } else if let Some(n) = s.strip_suffix("kb") {
        let val: u64 = n.trim().parse().ok()?;
        return Some(val.div_ceil(1024));
    } else if let Some(n) = s.strip_suffix('k') {
        let val: u64 = n.trim().parse().ok()?;
        return Some(val.div_ceil(1024));
    } else {
        // Plain number = megabytes.
        (s, 1u64)
    };

    let val: u64 = num_str.parse().ok()?;
    Some(val * multiplier)
}

// ── Network parsing ────────────────────────────────────────────────

/// Parse the top-level `networks:` section into [`NetworkSpec`] entries.
///
/// Supports the following forms:
/// ```yaml
/// networks:
///   frontend:             # minimal: name only, defaults
///   backend:
///     driver: bridge
///     ipam:
///       config:
///         - subnet: 172.20.1.0/24
/// ```
fn parse_networks(root: &serde_yml::Mapping) -> Result<Vec<NetworkSpec>, StackError> {
    let Some(value) = root.get(val("networks")) else {
        return Ok(vec![]);
    };

    let networks_map = value
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("top-level `networks` must be a mapping".into()))?;

    let mut networks = Vec::new();
    for (key, net_value) in networks_map {
        let net_name = key
            .as_str()
            .ok_or_else(|| StackError::ComposeParse("network name must be a string".into()))?;

        // Empty value (just the name) is valid -- uses defaults.
        if net_value.is_null() {
            networks.push(NetworkSpec {
                name: net_name.to_string(),
                driver: "bridge".to_string(),
                subnet: None,
            });
            continue;
        }

        let net_map = net_value.as_mapping().ok_or_else(|| {
            StackError::ComposeParse(format!("network `{net_name}` must be a mapping or empty"))
        })?;

        let driver = net_map
            .get(val("driver"))
            .and_then(|v| v.as_str())
            .unwrap_or("bridge")
            .to_string();

        if driver != "bridge" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("networks.{net_name}.driver"),
                reason: format!("only `bridge` driver is supported; got `{driver}`"),
            });
        }

        // Parse optional IPAM config for subnet.
        let subnet = net_map
            .get(val("ipam"))
            .and_then(|v| v.as_mapping())
            .and_then(|ipam| ipam.get(val("config")))
            .and_then(|v| v.as_sequence())
            .and_then(|seq| seq.first())
            .and_then(|item| item.as_mapping())
            .and_then(|cfg| cfg.get(val("subnet")))
            .and_then(|v| v.as_str())
            .map(String::from);

        networks.push(NetworkSpec {
            name: net_name.to_string(),
            driver,
            subnet,
        });
    }

    // Sort for determinism.
    networks.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(networks)
}

/// Parse the service-level `networks:` key.
///
/// Supports both list and mapping forms:
/// ```yaml
/// # List form:
/// networks:
///   - frontend
///   - backend
///
/// # Mapping form:
/// networks:
///   frontend: {}
///   backend:
/// ```
fn parse_service_networks(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Vec<String>, StackError> {
    let Some(value) = map.get(val("networks")) else {
        return Ok(vec![]);
    };

    // List form: ["frontend", "backend"]
    if let Some(seq) = value.as_sequence() {
        let mut names = Vec::new();
        for item in seq {
            let name = item.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `networks` list entries must be strings"
                ))
            })?;
            names.push(name.to_string());
        }
        return Ok(names);
    }

    // Mapping form: { frontend: {}, backend: null }
    if let Some(net_map) = value.as_mapping() {
        let mut names = Vec::new();
        for key in net_map.keys() {
            let name = key.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `networks` mapping keys must be strings"
                ))
            })?;
            names.push(name.to_string());
        }
        return Ok(names);
    }

    Err(StackError::ComposeParse(format!(
        "service `{svc_name}`: `networks` must be a list or mapping"
    )))
}

// ── Security & identity field parsers ──────────────────────────────

/// Parse a simple list of strings (e.g., `cap_add`, `cap_drop`).
fn parse_string_list(
    svc_name: &str,
    map: &serde_yml::Mapping,
    key: &str,
) -> Result<Vec<String>, StackError> {
    let Some(value) = map.get(val(key)) else {
        return Ok(vec![]);
    };

    let seq = value.as_sequence().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `{key}` must be a list"))
    })?;

    seq.iter()
        .map(|v| {
            v.as_str().map(String::from).ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `{key}` items must be strings"
                ))
            })
        })
        .collect()
}

/// Parse a string-to-string map (e.g., `sysctls`).
///
/// Supports both object and list forms:
/// ```yaml
/// sysctls:
///   net.core.somaxconn: "1024"
/// ```
/// or:
/// ```yaml
/// sysctls:
///   - net.core.somaxconn=1024
/// ```
fn parse_string_map(
    svc_name: &str,
    map: &serde_yml::Mapping,
    key: &str,
) -> Result<HashMap<String, String>, StackError> {
    let Some(value) = map.get(val(key)) else {
        return Ok(HashMap::new());
    };

    if let Some(obj) = value.as_mapping() {
        let mut result = HashMap::new();
        for (k, v) in obj {
            let k_str = k.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `{key}` keys must be strings"
                ))
            })?;
            let v_str = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => {
                    return Err(StackError::ComposeParse(format!(
                        "service `{svc_name}`: `{key}` values must be scalars"
                    )));
                }
            };
            result.insert(k_str.to_string(), v_str);
        }
        Ok(result)
    } else if let Some(seq) = value.as_sequence() {
        let mut result = HashMap::new();
        for item in seq {
            let s = item.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `{key}` list items must be strings"
                ))
            })?;
            if let Some((k, v)) = s.split_once('=') {
                result.insert(k.to_string(), v.to_string());
            } else {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `{key}` list items must be \"key=value\", got \"{s}\""
                )));
            }
        }
        Ok(result)
    } else {
        Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `{key}` must be a mapping or list"
        )))
    }
}

/// Parse `ulimits` configuration.
///
/// Supports both single-value and soft/hard forms:
/// ```yaml
/// ulimits:
///   nofile: 65535
/// ```
/// or:
/// ```yaml
/// ulimits:
///   nofile:
///     soft: 1024
///     hard: 65535
/// ```
fn parse_ulimits(svc_name: &str, map: &serde_yml::Mapping) -> Result<Vec<UlimitSpec>, StackError> {
    let Some(value) = map.get(val("ulimits")) else {
        return Ok(vec![]);
    };

    let obj = value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `ulimits` must be a mapping"))
    })?;

    let mut ulimits = Vec::new();
    for (k, v) in obj {
        let name = k.as_str().ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: ulimit name must be a string"
            ))
        })?;

        if let Some(n) = v.as_u64() {
            // Single value: soft = hard = value.
            ulimits.push(UlimitSpec {
                name: name.to_string(),
                soft: n,
                hard: n,
            });
        } else if let Some(n) = v.as_i64() {
            // Handle negative values (e.g., -1 for unlimited).
            let val = n as u64;
            ulimits.push(UlimitSpec {
                name: name.to_string(),
                soft: val,
                hard: val,
            });
        } else if let Some(inner) = v.as_mapping() {
            // Object form with soft/hard.
            let soft = inner
                .get(val("soft"))
                .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|i| i as u64)))
                .ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: ulimit `{name}` requires `soft` value"
                    ))
                })?;
            let hard = inner
                .get(val("hard"))
                .and_then(|v| v.as_u64().or_else(|| v.as_i64().map(|i| i as u64)))
                .ok_or_else(|| {
                    StackError::ComposeParse(format!(
                        "service `{svc_name}`: ulimit `{name}` requires `hard` value"
                    ))
                })?;
            ulimits.push(UlimitSpec {
                name: name.to_string(),
                soft,
                hard,
            });
        } else {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: ulimit `{name}` must be a number or mapping with soft/hard"
            )));
        }
    }

    // Sort for deterministic output.
    ulimits.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(ulimits)
}

/// Parse `labels` which can be a mapping or a list of `key=value` strings.
fn parse_labels(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<HashMap<String, String>, StackError> {
    parse_string_map(svc_name, map, "labels")
}

// ── Helpers ────────────────────────────────────────────────────────

/// Create a `serde_yml::Value::String` for use as a map key.
fn val(s: &str) -> Value {
    Value::String(s.to_string())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    // ── parse_compose: basic ──────────────────────────────────────

    #[test]
    fn minimal_compose() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.name, "myapp");
        assert_eq!(spec.services.len(), 1);
        assert_eq!(spec.services[0].name, "web");
        assert_eq!(spec.services[0].image, "nginx:latest");
    }

    #[test]
    fn compose_with_name_override() {
        let yaml = r#"
name: custom-name
services:
  web:
    image: nginx:latest
"#;
        let spec = parse_compose(yaml, "fallback").unwrap();
        assert_eq!(spec.name, "custom-name");
    }

    #[test]
    fn version_key_accepted() {
        let yaml = r#"
version: "3.8"
services:
  web:
    image: nginx:latest
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services.len(), 1);
    }

    // ── Service fields ────────────────────────────────────────────

    #[test]
    fn full_service_spec() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    command: ["nginx", "-g", "daemon off;"]
    entrypoint: ["/entrypoint.sh"]
    environment:
      PORT: "8080"
      DEBUG: "true"
    working_dir: /app
    user: "1000:1000"
    restart: always
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let svc = &spec.services[0];
        assert_eq!(svc.image, "nginx:latest");
        assert_eq!(
            svc.command,
            Some(vec![
                "nginx".to_string(),
                "-g".to_string(),
                "daemon off;".to_string()
            ])
        );
        assert_eq!(svc.entrypoint, Some(vec!["/entrypoint.sh".to_string()]));
        assert_eq!(svc.environment.get("PORT").unwrap(), "8080");
        assert_eq!(svc.environment.get("DEBUG").unwrap(), "true");
        assert_eq!(svc.working_dir, Some("/app".to_string()));
        assert_eq!(svc.user, Some("1000:1000".to_string()));
        assert_eq!(svc.restart_policy, Some(RestartPolicy::Always));
    }

    #[test]
    fn command_shell_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    command: nginx -g daemon off;
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].command,
            Some(vec![
                "nginx".to_string(),
                "-g".to_string(),
                "daemon".to_string(),
                "off;".to_string(),
            ])
        );
    }

    // ── Environment parsing ───────────────────────────────────────

    #[test]
    fn environment_list_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    environment:
      - PORT=8080
      - DEBUG=true
      - EMPTY_VAR
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let env = &spec.services[0].environment;
        assert_eq!(env.get("PORT").unwrap(), "8080");
        assert_eq!(env.get("DEBUG").unwrap(), "true");
        assert_eq!(env.get("EMPTY_VAR").unwrap(), "");
    }

    #[test]
    fn environment_numeric_and_bool_values() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    environment:
      PORT: 8080
      DEBUG: true
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let env = &spec.services[0].environment;
        assert_eq!(env.get("PORT").unwrap(), "8080");
        assert_eq!(env.get("DEBUG").unwrap(), "true");
    }

    // ── Port parsing ──────────────────────────────────────────────

    #[test]
    fn ports_short_host_container() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let port = &spec.services[0].ports[0];
        assert_eq!(port.host_port, Some(8080));
        assert_eq!(port.container_port, 80);
        assert_eq!(port.protocol, "tcp");
    }

    #[test]
    fn ports_short_container_only() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "80"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let port = &spec.services[0].ports[0];
        assert_eq!(port.host_port, None);
        assert_eq!(port.container_port, 80);
    }

    #[test]
    fn ports_short_with_protocol() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "8080:80/udp"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let port = &spec.services[0].ports[0];
        assert_eq!(port.protocol, "udp");
        assert_eq!(port.host_port, Some(8080));
        assert_eq!(port.container_port, 80);
    }

    #[test]
    fn ports_short_with_bind_address() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - "127.0.0.1:8080:80"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let port = &spec.services[0].ports[0];
        assert_eq!(port.host_port, Some(8080));
        assert_eq!(port.container_port, 80);
    }

    #[test]
    fn ports_long_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - target: 80
        published: 8080
        protocol: udp
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let port = &spec.services[0].ports[0];
        assert_eq!(port.container_port, 80);
        assert_eq!(port.host_port, Some(8080));
        assert_eq!(port.protocol, "udp");
    }

    #[test]
    fn ports_bare_number() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ports:
      - 80
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let port = &spec.services[0].ports[0];
        assert_eq!(port.container_port, 80);
        assert_eq!(port.host_port, None);
    }

    // ── Mount parsing ─────────────────────────────────────────────

    #[test]
    fn mount_bind_short() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - /host/data:/container/data
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Bind {
                source: "/host/data".to_string(),
                target: "/container/data".to_string(),
                read_only: false,
            }
        );
    }

    #[test]
    fn mount_bind_read_only() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - /host/data:/container/data:ro
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Bind {
                source: "/host/data".to_string(),
                target: "/container/data".to_string(),
                read_only: true,
            }
        );
    }

    #[test]
    fn mount_named_volume() {
        let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - dbdata:/var/lib/postgresql/data
volumes:
  dbdata:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Named {
                source: "dbdata".to_string(),
                target: "/var/lib/postgresql/data".to_string(),
                read_only: false,
            }
        );
    }

    #[test]
    fn mount_ephemeral() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - /tmp/scratch
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Ephemeral {
                target: "/tmp/scratch".to_string(),
            }
        );
    }

    #[test]
    fn mount_long_form_bind() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - type: bind
        source: /host/path
        target: /container/path
        read_only: true
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Bind {
                source: "/host/path".to_string(),
                target: "/container/path".to_string(),
                read_only: true,
            }
        );
    }

    #[test]
    fn mount_long_form_volume() {
        let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - type: volume
        source: dbdata
        target: /var/lib/postgresql/data
volumes:
  dbdata:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Named {
                source: "dbdata".to_string(),
                target: "/var/lib/postgresql/data".to_string(),
                read_only: false,
            }
        );
    }

    #[test]
    fn mount_long_form_tmpfs() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - type: tmpfs
        target: /tmp
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Ephemeral {
                target: "/tmp".to_string(),
            }
        );
    }

    // ── depends_on parsing ────────────────────────────────────────

    #[test]
    fn depends_on_list_form() {
        let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      - db
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert_eq!(web.depends_on, vec![ServiceDependency::started("db")]);
    }

    #[test]
    fn depends_on_mapping_form_service_healthy() {
        let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_healthy
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert_eq!(web.depends_on, vec![ServiceDependency::healthy("db")]);
    }

    #[test]
    fn depends_on_mapping_form_service_started() {
        let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      db:
        condition: service_started
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert_eq!(web.depends_on, vec![ServiceDependency::started("db")]);
    }

    #[test]
    fn depends_on_mapping_form_no_condition() {
        let yaml = r#"
services:
  db:
    image: postgres:15
  web:
    image: nginx:latest
    depends_on:
      db: {}
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert_eq!(web.depends_on, vec![ServiceDependency::started("db")]);
    }

    // ── Healthcheck parsing ───────────────────────────────────────

    #[test]
    fn healthcheck_list_test() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 10s
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let hc = spec.services[0].healthcheck.as_ref().unwrap();
        assert_eq!(hc.test, vec!["CMD", "curl", "-f", "http://localhost"]);
        assert_eq!(hc.interval_secs, Some(30));
        assert_eq!(hc.timeout_secs, Some(5));
        assert_eq!(hc.retries, Some(3));
        assert_eq!(hc.start_period_secs, Some(10));
    }

    #[test]
    fn healthcheck_shell_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    healthcheck:
      test: curl -f http://localhost
      interval: 10
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let hc = spec.services[0].healthcheck.as_ref().unwrap();
        assert_eq!(hc.test, vec!["CMD-SHELL", "curl -f http://localhost"]);
        assert_eq!(hc.interval_secs, Some(10));
    }

    #[test]
    fn healthcheck_disabled() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    healthcheck:
      disable: true
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert!(spec.services[0].healthcheck.is_none());
    }

    // ── Restart policy parsing ────────────────────────────────────

    #[test]
    fn restart_policies() {
        let cases = vec![
            ("no", RestartPolicy::No),
            ("always", RestartPolicy::Always),
            ("unless-stopped", RestartPolicy::UnlessStopped),
            ("on-failure", RestartPolicy::OnFailure { max_retries: None }),
            (
                "on-failure:5",
                RestartPolicy::OnFailure {
                    max_retries: Some(5),
                },
            ),
        ];

        for (input, expected) in cases {
            let yaml = format!(
                r#"
services:
  web:
    image: nginx:latest
    restart: {input}
"#
            );
            let spec = parse_compose(&yaml, "myapp").unwrap();
            assert_eq!(
                spec.services[0].restart_policy,
                Some(expected),
                "failed for restart: {input}"
            );
        }
    }

    // ── Volume parsing (top-level) ────────────────────────────────

    #[test]
    fn volumes_top_level_empty() {
        let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - dbdata:/var/lib/postgresql/data
volumes:
  dbdata:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.volumes.len(), 1);
        assert_eq!(spec.volumes[0].name, "dbdata");
        assert_eq!(spec.volumes[0].driver, "local");
        assert!(spec.volumes[0].driver_opts.is_none());
    }

    #[test]
    fn volumes_top_level_with_driver_opts() {
        let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - dbdata:/var/lib/postgresql/data
volumes:
  dbdata:
    driver: local
    driver_opts:
      type: none
      device: /data/db
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let opts = spec.volumes[0].driver_opts.as_ref().unwrap();
        assert_eq!(opts.get("type").unwrap(), "none");
        assert_eq!(opts.get("device").unwrap(), "/data/db");
    }

    // ── Duration parsing ──────────────────────────────────────────

    #[test]
    fn duration_parsing() {
        assert_eq!(parse_duration_string("30s"), Some(30));
        assert_eq!(parse_duration_string("5m"), Some(300));
        assert_eq!(parse_duration_string("1h"), Some(3600));
        assert_eq!(parse_duration_string("1m30s"), Some(90));
        assert_eq!(parse_duration_string("1h30m15s"), Some(5415));
        assert_eq!(parse_duration_string("0s"), Some(0));
    }

    // ── Stop lifecycle parsing ───────────────────────────────────

    #[test]
    fn stop_signal_parsed() {
        let yaml = r#"
services:
  db:
    image: postgres:16
    stop_signal: SIGQUIT
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].stop_signal.as_deref(), Some("SIGQUIT"));
    }

    #[test]
    fn stop_grace_period_parsed() {
        let yaml = r#"
services:
  db:
    image: postgres:16
    stop_grace_period: 30s
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].stop_grace_period_secs, Some(30));
    }

    #[test]
    fn stop_signal_defaults_to_none() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].stop_signal, None);
        assert_eq!(spec.services[0].stop_grace_period_secs, None);
    }

    #[test]
    fn stop_grace_period_compound_duration() {
        let yaml = r#"
services:
  db:
    image: postgres:16
    stop_grace_period: 1m30s
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].stop_grace_period_secs, Some(90));
    }

    // ── Rejection tests ───────────────────────────────────────────

    #[test]
    fn reject_build() {
        let yaml = r#"
services:
  web:
    build: .
    image: nginx:latest
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("build"), "error should mention `build`: {msg}");
        assert!(
            msg.contains("pre-built OCI images"),
            "error should be actionable: {msg}"
        );
    }

    // ── Network parsing ──────────────────────────────────────────

    #[test]
    fn parse_top_level_networks_minimal() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
networks:
  frontend:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        // "default" is not created since no service lacks explicit networks.
        // "frontend" is the only defined network.
        assert!(spec.networks.iter().any(|n| n.name == "frontend"));
        let frontend = spec.networks.iter().find(|n| n.name == "frontend").unwrap();
        assert_eq!(frontend.driver, "bridge");
        assert_eq!(frontend.subnet, None);
        assert_eq!(spec.services[0].networks, vec!["frontend"]);
    }

    #[test]
    fn parse_networks_with_ipam_subnet() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
  db:
    image: postgres:16
    networks:
      - backend
networks:
  frontend:
    driver: bridge
    ipam:
      config:
        - subnet: 172.20.1.0/24
  backend:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.networks.len(), 2);

        let frontend = spec.networks.iter().find(|n| n.name == "frontend").unwrap();
        assert_eq!(frontend.subnet.as_deref(), Some("172.20.1.0/24"));

        let backend = spec.networks.iter().find(|n| n.name == "backend").unwrap();
        assert_eq!(backend.subnet, None);

        // Check service network assignments.
        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert_eq!(web.networks, vec!["frontend"]);

        let db = spec.services.iter().find(|s| s.name == "db").unwrap();
        assert_eq!(db.networks, vec!["backend"]);
    }

    #[test]
    fn parse_service_networks_list_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
      - backend
networks:
  frontend:
  backend:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert!(web.networks.contains(&"frontend".to_string()));
        assert!(web.networks.contains(&"backend".to_string()));
    }

    #[test]
    fn parse_service_networks_mapping_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      frontend: {}
      backend:
networks:
  frontend:
  backend:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert!(web.networks.contains(&"frontend".to_string()));
        assert!(web.networks.contains(&"backend".to_string()));
    }

    #[test]
    fn no_networks_section_creates_implicit_default() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
  db:
    image: postgres:16
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.networks.len(), 1);
        assert_eq!(spec.networks[0].name, "default");
        assert_eq!(spec.networks[0].driver, "bridge");

        // All services join the default network.
        for svc in &spec.services {
            assert_eq!(svc.networks, vec!["default"]);
        }
    }

    #[test]
    fn custom_networks_services_without_explicit_get_default() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - frontend
  db:
    image: postgres:16
networks:
  frontend:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        // "default" network is auto-created for db.
        assert!(spec.networks.iter().any(|n| n.name == "default"));
        assert!(spec.networks.iter().any(|n| n.name == "frontend"));

        let web = spec.services.iter().find(|s| s.name == "web").unwrap();
        assert_eq!(web.networks, vec!["frontend"]);

        let db = spec.services.iter().find(|s| s.name == "db").unwrap();
        assert_eq!(db.networks, vec!["default"]);
    }

    #[test]
    fn reject_undefined_network_reference() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    networks:
      - nonexistent
networks:
  frontend:
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("nonexistent"),
            "error should mention the undefined network: {msg}"
        );
    }

    #[test]
    fn reject_non_bridge_driver() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
networks:
  mynet:
    driver: overlay
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("overlay"),
            "error should mention the unsupported driver: {msg}"
        );
    }

    #[test]
    fn accept_deploy_replicas() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      replicas: 3
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].resources.replicas, 3);
    }

    #[test]
    fn deploy_resources_reservations_accepted() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        reservations:
          cpus: "0.25"
          memory: "256m"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let res = &spec.services[0].resources;
        assert_eq!(res.reservation_cpus, Some(0.25));
        assert_eq!(res.reservation_memory_bytes, Some(256 * 1024 * 1024));
        assert_eq!(res.cpus, None);
        assert_eq!(res.memory_bytes, None);
    }

    #[test]
    fn reject_extends() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    extends:
      service: base
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("extends"));
    }

    #[test]
    fn reject_configs() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
configs:
  myconfig:
    file: ./config.txt
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("configs"));
    }

    #[test]
    fn secrets_file_based_accepted() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - mysecret
secrets:
  mysecret:
    file: ./secret.txt
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.secrets.len(), 1);
        assert_eq!(spec.secrets[0].name, "mysecret");
        assert_eq!(spec.secrets[0].file, "./secret.txt");
        assert_eq!(spec.services[0].secrets.len(), 1);
        assert_eq!(spec.services[0].secrets[0].source, "mysecret");
        assert_eq!(spec.services[0].secrets[0].target, "mysecret");
    }

    #[test]
    fn secrets_long_form_with_target() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - source: db_password
        target: password.txt
secrets:
  db_password:
    file: ./db_pass.txt
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].secrets.len(), 1);
        assert_eq!(spec.services[0].secrets[0].source, "db_password");
        assert_eq!(spec.services[0].secrets[0].target, "password.txt");
    }

    #[test]
    fn secrets_long_form_without_target() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - source: api_key
secrets:
  api_key:
    file: ./api.key
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].secrets[0].source, "api_key");
        assert_eq!(spec.services[0].secrets[0].target, "api_key");
    }

    #[test]
    fn secrets_external_rejected() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
secrets:
  mysecret:
    external: true
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("external"),
            "error should mention external: {msg}"
        );
    }

    #[test]
    fn secrets_missing_file_rejected() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
secrets:
  mysecret:
    name: something
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("file"), "error should mention file: {msg}");
    }

    #[test]
    fn secrets_undefined_ref_rejected() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - undefined_secret
secrets:
  mysecret:
    file: ./secret.txt
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("undefined_secret"),
            "error should mention the undefined secret: {msg}"
        );
    }

    #[test]
    fn secrets_multiple() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    secrets:
      - db_pass
      - api_key
secrets:
  db_pass:
    file: ./db.txt
  api_key:
    file: ./api.key
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.secrets.len(), 2);
        assert_eq!(spec.services[0].secrets.len(), 2);
    }

    #[test]
    fn secrets_top_level_without_service_refs() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
secrets:
  unused_secret:
    file: ./unused.txt
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.secrets.len(), 1);
        assert!(spec.services[0].secrets.is_empty());
    }

    #[test]
    fn reject_devices() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    devices:
      - /dev/sda:/dev/xvdc
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("devices"));
    }

    #[test]
    fn reject_ipc() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ipc: host
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("ipc"));
    }

    #[test]
    fn reject_pid() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    pid: host
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("pid"));
    }

    #[test]
    fn reject_runtime() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    runtime: runc
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("runtime"));
    }

    #[test]
    fn reject_profiles() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    profiles:
      - debug
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("profiles"));
    }

    #[test]
    fn parse_extra_hosts_entries() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    extra_hosts:
      - "myhost:192.168.1.10"
      - "other:10.0.0.1"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let web = &spec.services[0];
        assert_eq!(web.extra_hosts.len(), 2);
        assert_eq!(
            web.extra_hosts[0],
            ("myhost".to_string(), "192.168.1.10".to_string())
        );
        assert_eq!(
            web.extra_hosts[1],
            ("other".to_string(), "10.0.0.1".to_string())
        );
    }

    #[test]
    fn reject_cgroup() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    cgroup: host
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("cgroup"));
    }

    #[test]
    fn reject_unknown_top_level_key() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
x-custom:
  foo: bar
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("x-custom"));
    }

    #[test]
    fn reject_unknown_service_key() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    stdin_open: true
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("stdin_open"));
    }

    #[test]
    fn reject_non_local_volume_driver() {
        let yaml = r#"
services:
  db:
    image: postgres:15
volumes:
  dbdata:
    driver: nfs
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("nfs") || msg.contains("local"), "{msg}");
    }

    // ── Validation tests ──────────────────────────────────────────

    #[test]
    fn validate_undefined_dependency() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    depends_on:
      - nonexistent
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("nonexistent"));
    }

    #[test]
    fn validate_undefined_volume_reference() {
        let yaml = r#"
services:
  db:
    image: postgres:15
    volumes:
      - missing_vol:/data
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("missing_vol"));
    }

    #[test]
    fn missing_image_fails() {
        let yaml = r#"
services:
  web:
    command: ["echo", "hello"]
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("image"));
    }

    // ── Multi-service compose ─────────────────────────────────────

    #[test]
    fn web_redis_compose() {
        let yaml = r#"
services:
  web:
    image: myapp:latest
    ports:
      - "8080:80"
    depends_on:
      - redis
    environment:
      REDIS_URL: redis://redis:6379
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 5

volumes:
  redis-data:
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();

        // Services sorted by name.
        assert_eq!(spec.services.len(), 2);
        assert_eq!(spec.services[0].name, "redis");
        assert_eq!(spec.services[1].name, "web");

        // Redis service.
        let redis = &spec.services[0];
        assert_eq!(redis.image, "redis:7-alpine");
        assert_eq!(redis.ports[0].container_port, 6379);
        assert_eq!(redis.ports[0].host_port, Some(6379));
        assert!(redis.healthcheck.is_some());
        let hc = redis.healthcheck.as_ref().unwrap();
        assert_eq!(hc.interval_secs, Some(10));
        assert_eq!(hc.timeout_secs, Some(3));
        assert_eq!(hc.retries, Some(5));
        assert_eq!(
            redis.mounts[0],
            MountSpec::Named {
                source: "redis-data".to_string(),
                target: "/data".to_string(),
                read_only: false,
            }
        );

        // Web service.
        let web = &spec.services[1];
        assert_eq!(web.depends_on, vec![ServiceDependency::started("redis")]);
        assert_eq!(
            web.environment.get("REDIS_URL").unwrap(),
            "redis://redis:6379"
        );
        assert_eq!(web.ports[0].host_port, Some(8080));
        assert_eq!(web.ports[0].container_port, 80);

        // Volume.
        assert_eq!(spec.volumes.len(), 1);
        assert_eq!(spec.volumes[0].name, "redis-data");
    }

    #[test]
    fn services_sorted_deterministically() {
        let yaml = r#"
services:
  zeta:
    image: img:latest
  alpha:
    image: img:latest
  middle:
    image: img:latest
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let names: Vec<&str> = spec.services.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["alpha", "middle", "zeta"]);
    }

    #[test]
    fn relative_path_bind_mount() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    volumes:
      - ./src:/app/src
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].mounts[0],
            MountSpec::Bind {
                source: "./src".to_string(),
                target: "/app/src".to_string(),
                read_only: false,
            }
        );
    }

    // ── env_file and variable expansion ──────────────────────────

    #[test]
    fn parse_env_file_basic() {
        let content = r#"
# This is a comment
KEY1=value1
KEY2=value2

# Another comment
KEY3=value with spaces
"#;
        let env = parse_env_file_content(content);
        assert_eq!(env.get("KEY1").unwrap(), "value1");
        assert_eq!(env.get("KEY2").unwrap(), "value2");
        assert_eq!(env.get("KEY3").unwrap(), "value with spaces");
        assert_eq!(env.len(), 3);
    }

    #[test]
    fn parse_env_file_quoted_values() {
        let content = r#"
SINGLE='single quoted'
DOUBLE="double quoted"
UNQUOTED=plain
"#;
        let env = parse_env_file_content(content);
        assert_eq!(env.get("SINGLE").unwrap(), "single quoted");
        assert_eq!(env.get("DOUBLE").unwrap(), "double quoted");
        assert_eq!(env.get("UNQUOTED").unwrap(), "plain");
    }

    #[test]
    fn parse_env_file_export_prefix() {
        let content = "export DB_HOST=localhost\nexport DB_PORT=5432\n";
        let env = parse_env_file_content(content);
        assert_eq!(env.get("DB_HOST").unwrap(), "localhost");
        assert_eq!(env.get("DB_PORT").unwrap(), "5432");
    }

    #[test]
    fn parse_env_file_empty_value() {
        let content = "EMPTY=\n";
        let env = parse_env_file_content(content);
        assert_eq!(env.get("EMPTY").unwrap(), "");
    }

    #[test]
    fn expand_braced_variable() {
        let mut vars = HashMap::new();
        vars.insert("DB_HOST".to_string(), "localhost".to_string());
        vars.insert("DB_PORT".to_string(), "5432".to_string());
        let result = expand_variables("host=${DB_HOST} port=${DB_PORT}", &vars);
        assert_eq!(result, "host=localhost port=5432");
    }

    #[test]
    fn expand_simple_variable() {
        let mut vars = HashMap::new();
        vars.insert("TAG".to_string(), "latest".to_string());
        let result = expand_variables("image: nginx:$TAG", &vars);
        assert_eq!(result, "image: nginx:latest");
    }

    #[test]
    fn expand_default_value() {
        let vars = HashMap::new();
        let result = expand_variables("port=${PORT:-8080}", &vars);
        assert_eq!(result, "port=8080");
    }

    #[test]
    fn expand_default_not_used_when_set() {
        let mut vars = HashMap::new();
        vars.insert("PORT".to_string(), "3000".to_string());
        let result = expand_variables("port=${PORT:-8080}", &vars);
        assert_eq!(result, "port=3000");
    }

    #[test]
    fn expand_default_used_when_empty() {
        let mut vars = HashMap::new();
        vars.insert("PORT".to_string(), String::new());
        let result = expand_variables("port=${PORT:-8080}", &vars);
        assert_eq!(result, "port=8080");
    }

    #[test]
    fn expand_missing_variable_empty() {
        let vars = HashMap::new();
        let result = expand_variables("val=${MISSING}", &vars);
        assert_eq!(result, "val=");
    }

    #[test]
    fn expand_dollar_dollar_literal() {
        let vars = HashMap::new();
        let result = expand_variables("cost: $$100", &vars);
        assert_eq!(result, "cost: $100");
    }

    #[test]
    fn expand_no_variables_unchanged() {
        let vars = HashMap::new();
        let input = "plain text without variables";
        assert_eq!(expand_variables(input, &vars), input);
    }

    #[test]
    fn env_file_accepted_without_dir() {
        // env_file is accepted but silently ignored without compose_dir.
        let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: .env
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].image, "nginx:latest");
    }

    #[test]
    fn env_file_loads_from_directory() {
        let dir = tempfile::tempdir().unwrap();
        let env_path = dir.path().join("app.env");
        std::fs::write(&env_path, "DB_HOST=postgres\nDB_PORT=5432\n").unwrap();

        let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: app.env
"#;
        let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
        let env = &spec.services[0].environment;
        assert_eq!(env.get("DB_HOST").unwrap(), "postgres");
        assert_eq!(env.get("DB_PORT").unwrap(), "5432");
    }

    #[test]
    fn env_file_list_loads_multiple() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("base.env"), "A=1\nB=2\n").unwrap();
        std::fs::write(dir.path().join("override.env"), "B=99\nC=3\n").unwrap();

        let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file:
      - base.env
      - override.env
"#;
        let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
        let env = &spec.services[0].environment;
        assert_eq!(env.get("A").unwrap(), "1");
        assert_eq!(env.get("B").unwrap(), "99"); // overridden
        assert_eq!(env.get("C").unwrap(), "3");
    }

    #[test]
    fn explicit_env_overrides_env_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".env"), "PORT=3000\nHOST=0.0.0.0\n").unwrap();

        let yaml = r#"
services:
  web:
    image: nginx:latest
    env_file: .env
    environment:
      PORT: "8080"
"#;
        let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
        let env = &spec.services[0].environment;
        assert_eq!(env.get("PORT").unwrap(), "8080"); // explicit wins
        assert_eq!(env.get("HOST").unwrap(), "0.0.0.0"); // from env_file
    }

    #[test]
    fn variable_expansion_in_yaml() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(".env"), "TAG=v2.1\nPORT=9090\n").unwrap();

        let yaml = r#"
services:
  web:
    image: myapp:${TAG}
    ports:
      - "${PORT}:80"
"#;
        let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
        assert_eq!(spec.services[0].image, "myapp:v2.1");
        assert_eq!(spec.services[0].ports[0].host_port, Some(9090));
        assert_eq!(spec.services[0].ports[0].container_port, 80);
    }

    #[test]
    fn variable_expansion_with_defaults() {
        let dir = tempfile::tempdir().unwrap();
        // No .env file — all defaults should kick in.
        let yaml = r#"
services:
  web:
    image: nginx:${TAG:-latest}
    environment:
      PORT: "${PORT:-8080}"
"#;
        let spec = parse_compose_with_dir(yaml, "myapp", dir.path()).unwrap();
        assert_eq!(spec.services[0].image, "nginx:latest");
        assert_eq!(spec.services[0].environment.get("PORT").unwrap(), "8080");
    }

    // ── Deploy / resource limits parsing ─────────────────────────

    #[test]
    fn deploy_resource_limits_cpus_and_memory() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          cpus: "0.5"
          memory: "512m"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        let res = &spec.services[0].resources;
        assert!((res.cpus.unwrap() - 0.5).abs() < f64::EPSILON);
        assert_eq!(res.memory_bytes.unwrap(), 512 * 1024 * 1024);
    }

    #[test]
    fn deploy_resource_limits_cpus_as_number() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          cpus: 2.0
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert!((spec.services[0].resources.cpus.unwrap() - 2.0).abs() < f64::EPSILON);
    }

    #[test]
    fn deploy_resource_limits_memory_gigabytes() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          memory: "2g"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].resources.memory_bytes.unwrap(),
            2 * 1024 * 1024 * 1024
        );
    }

    #[test]
    fn deploy_resource_limits_memory_kilobytes() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          memory: "256k"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].resources.memory_bytes.unwrap(), 256 * 1024);
    }

    #[test]
    fn deploy_empty_is_accepted() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
"#;
        // Empty limits mapping → no resources set.
        // serde_yml parses `limits:` (no value) as null, not an empty mapping.
        // We should handle this gracefully.
        let result = parse_compose(yaml, "myapp");
        // This might parse `limits` as null, which is fine — returns default.
        assert!(result.is_ok());
        let spec = result.unwrap();
        // Default replicas is 1 (not 0)
        let mut expected = ResourcesSpec::default();
        expected.replicas = 1;
        assert_eq!(spec.services[0].resources, expected);
    }

    #[test]
    fn deploy_no_resources_accepted() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
"#;
        // resources with no value is null.
        let result = parse_compose(yaml, "myapp");
        assert!(result.is_ok());
    }

    #[test]
    fn deploy_only_cpus() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          cpus: "1.5"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert!((spec.services[0].resources.cpus.unwrap() - 1.5).abs() < f64::EPSILON);
        assert!(spec.services[0].resources.memory_bytes.is_none());
    }

    #[test]
    fn deploy_only_memory() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          memory: "1g"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert!(spec.services[0].resources.cpus.is_none());
        assert_eq!(
            spec.services[0].resources.memory_bytes.unwrap(),
            1024 * 1024 * 1024
        );
    }

    #[test]
    fn reject_deploy_unsupported_limit_key() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          devices: []
"#;
        let err = parse_compose(yaml, "myapp").unwrap_err();
        assert!(err.to_string().contains("devices"));
    }

    #[test]
    fn parse_memory_string_variants() {
        assert_eq!(
            parse_memory_string("test", "512m").unwrap(),
            512 * 1024 * 1024
        );
        assert_eq!(
            parse_memory_string("test", "512M").unwrap(),
            512 * 1024 * 1024
        );
        assert_eq!(
            parse_memory_string("test", "1g").unwrap(),
            1024 * 1024 * 1024
        );
        assert_eq!(
            parse_memory_string("test", "1G").unwrap(),
            1024 * 1024 * 1024
        );
        assert_eq!(parse_memory_string("test", "256k").unwrap(), 256 * 1024);
        assert_eq!(parse_memory_string("test", "256K").unwrap(), 256 * 1024);
        assert_eq!(parse_memory_string("test", "1024").unwrap(), 1024);
        assert_eq!(parse_memory_string("test", "1024b").unwrap(), 1024);
        assert!(parse_memory_string("test", "abc").is_err());
        assert!(parse_memory_string("test", "").is_err());
    }

    // ── Security fields ──────────────────────────────────────────

    #[test]
    fn cap_add_parses_string_list() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    cap_add:
      - NET_ADMIN
      - SYS_PTRACE
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].cap_add, vec!["NET_ADMIN", "SYS_PTRACE"]);
    }

    #[test]
    fn cap_drop_parses_string_list() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    cap_drop:
      - MKNOD
      - AUDIT_WRITE
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].cap_drop, vec!["MKNOD", "AUDIT_WRITE"]);
    }

    #[test]
    fn privileged_true() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    privileged: true
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert!(spec.services[0].privileged);
    }

    #[test]
    fn privileged_defaults_to_false() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert!(!spec.services[0].privileged);
    }

    #[test]
    fn read_only_true() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    read_only: true
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert!(spec.services[0].read_only);
    }

    #[test]
    fn sysctls_mapping_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    sysctls:
      net.core.somaxconn: "1024"
      net.ipv4.tcp_syncookies: "0"
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].sysctls.len(), 2);
        assert_eq!(spec.services[0].sysctls["net.core.somaxconn"], "1024");
        assert_eq!(spec.services[0].sysctls["net.ipv4.tcp_syncookies"], "0");
    }

    #[test]
    fn sysctls_list_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    sysctls:
      - net.core.somaxconn=1024
      - net.ipv4.tcp_syncookies=0
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].sysctls.len(), 2);
        assert_eq!(spec.services[0].sysctls["net.core.somaxconn"], "1024");
    }

    // ── Ulimits ──────────────────────────────────────────────────

    #[test]
    fn ulimits_single_value() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ulimits:
      nofile: 65536
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].ulimits.len(), 1);
        assert_eq!(spec.services[0].ulimits[0].name, "nofile");
        assert_eq!(spec.services[0].ulimits[0].soft, 65536);
        assert_eq!(spec.services[0].ulimits[0].hard, 65536);
    }

    #[test]
    fn ulimits_soft_hard_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ulimits:
      nofile:
        soft: 1024
        hard: 65536
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].ulimits.len(), 1);
        assert_eq!(spec.services[0].ulimits[0].name, "nofile");
        assert_eq!(spec.services[0].ulimits[0].soft, 1024);
        assert_eq!(spec.services[0].ulimits[0].hard, 65536);
    }

    #[test]
    fn ulimits_multiple() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    ulimits:
      nofile:
        soft: 1024
        hard: 65536
      nproc: 2048
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].ulimits.len(), 2);
    }

    #[test]
    fn pids_limit_in_deploy() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    deploy:
      resources:
        limits:
          pids: 100
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].resources.pids_limit, Some(100));
    }

    // ── Container identity ───────────────────────────────────────

    #[test]
    fn container_name_parsed() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    container_name: my-web-container
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(
            spec.services[0].container_name,
            Some("my-web-container".to_string())
        );
    }

    #[test]
    fn container_name_defaults_to_none() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].container_name, None);
    }

    #[test]
    fn hostname_parsed() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    hostname: my-web-host
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].hostname, Some("my-web-host".to_string()));
    }

    #[test]
    fn domainname_parsed() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    domainname: example.com
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].domainname, Some("example.com".to_string()));
    }

    #[test]
    fn labels_mapping_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    labels:
      com.example.description: "Web frontend"
      com.example.tier: frontend
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].labels.len(), 2);
        assert_eq!(
            spec.services[0].labels["com.example.description"],
            "Web frontend"
        );
        assert_eq!(spec.services[0].labels["com.example.tier"], "frontend");
    }

    #[test]
    fn labels_list_form() {
        let yaml = r#"
services:
  web:
    image: nginx:latest
    labels:
      - com.example.description=Web frontend
      - com.example.tier=frontend
"#;
        let spec = parse_compose(yaml, "myapp").unwrap();
        assert_eq!(spec.services[0].labels.len(), 2);
        assert_eq!(
            spec.services[0].labels["com.example.description"],
            "Web frontend"
        );
    }

    // ── x-vz extension tests ──────────────────────────────────────

    #[test]
    fn xvz_disk_size_string_gigabytes() {
        let yaml = r#"
services:
  db:
    image: postgres:16
x-vz:
  disk_size: "20g"
"#;
        let spec = parse_compose(yaml, "test").unwrap();
        assert_eq!(spec.disk_size_mb, Some(20 * 1024));
    }

    #[test]
    fn xvz_disk_size_string_megabytes() {
        let yaml = r#"
services:
  db:
    image: postgres:16
x-vz:
  disk_size: "512m"
"#;
        let spec = parse_compose(yaml, "test").unwrap();
        assert_eq!(spec.disk_size_mb, Some(512));
    }

    #[test]
    fn xvz_disk_size_integer() {
        let yaml = r#"
services:
  db:
    image: postgres:16
x-vz:
  disk_size: 1024
"#;
        let spec = parse_compose(yaml, "test").unwrap();
        assert_eq!(spec.disk_size_mb, Some(1024));
    }

    #[test]
    fn xvz_disk_size_absent() {
        let yaml = r#"
services:
  db:
    image: postgres:16
"#;
        let spec = parse_compose(yaml, "test").unwrap();
        assert_eq!(spec.disk_size_mb, None);
    }

    #[test]
    fn xvz_empty_section() {
        let yaml = r#"
services:
  db:
    image: postgres:16
x-vz: {}
"#;
        let spec = parse_compose(yaml, "test").unwrap();
        assert_eq!(spec.disk_size_mb, None);
    }

    #[test]
    fn parse_size_to_mb_variants() {
        assert_eq!(super::parse_size_to_mb("10g"), Some(10 * 1024));
        assert_eq!(super::parse_size_to_mb("10gb"), Some(10 * 1024));
        assert_eq!(super::parse_size_to_mb("512m"), Some(512));
        assert_eq!(super::parse_size_to_mb("512mb"), Some(512));
        assert_eq!(super::parse_size_to_mb("2048k"), Some(2));
        assert_eq!(super::parse_size_to_mb("1024kb"), Some(1));
        assert_eq!(super::parse_size_to_mb("100"), Some(100));
        assert_eq!(super::parse_size_to_mb(""), None);
    }
}
