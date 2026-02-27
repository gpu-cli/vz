//! Compose YAML subset importer.
//!
//! Parses a Docker Compose YAML file into a typed [`StackSpec`],
//! accepting only the feature subset defined in the v1 compliance
//! contract. Unsupported keys are rejected with stable error codes
//! before any reconciliation starts.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use serde_yml::Value;

use crate::error::StackError;
use crate::spec::{
    DependencyCondition, HealthCheckSpec, LoggingConfig, MountSpec, NetworkSpec, PortSpec,
    ResourcesSpec, RestartPolicy, SecretDef, SecretSource, ServiceDependency, ServiceKind,
    ServiceSecretRef, ServiceSpec, StackSpec, UlimitSpec, VolumeSpec,
};

mod env;
mod extensions;
mod fields;
mod helpers;
mod interaction;
mod networks;
mod secrets;
mod security;
mod service;
#[cfg(test)]
mod tests;
mod validation;
mod volumes;

pub use env::{expand_variables, parse_env_file_content};

use self::extensions::parse_xvz_disk_size;
use self::helpers::val;
use self::networks::parse_networks;
use self::secrets::parse_secrets_top_level;
use self::service::{parse_build_directive, parse_service, validate_workspace_service_invariants};
use self::validation::validate_top_level_keys;
use self::volumes::parse_volumes;
#[cfg(test)]
use self::{
    extensions::parse_size_to_mb,
    fields::{parse_duration_string, parse_memory_string},
};

// ── Accepted key sets ──────────────────────────────────────────────

/// Top-level keys allowed in the Compose file.
const ACCEPTED_TOP_LEVEL: &[&str] = &[
    "version", "name", "services", "volumes", "secrets", "networks", "x-vz",
];

/// Service-level keys allowed inside `services.<name>`.
const ACCEPTED_SERVICE: &[&str] = &[
    "image",
    "build",
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
    "network_mode",
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
    // Interactive mode
    "expose",
    "stdin_open",
    "tty",
    // Logging
    "logging",
    // Service-level extensions
    "x-vz",
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

/// Normalized Compose build directive for a single service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposeBuildSpec {
    /// Service name owning this build directive.
    pub service_name: String,
    /// Build context path (string form from Compose after variable expansion).
    pub context: String,
    /// Optional Dockerfile path relative to context.
    pub dockerfile: Option<String>,
    /// Optional multi-stage target.
    pub target: Option<String>,
    /// Build arguments.
    pub args: BTreeMap<String, String>,
    /// Optional cache source references.
    pub cache_from: Vec<String>,
}

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

/// Extract normalized build directives from Compose services.
///
/// Variable expansion follows the same `.env` behavior as
/// [`parse_compose_with_dir`]. Relative paths are preserved in string form;
/// callers decide how to resolve them.
pub fn collect_compose_build_specs_with_dir(
    yaml: &str,
    compose_dir: &Path,
) -> Result<BTreeMap<String, ComposeBuildSpec>, StackError> {
    let dot_env_path = compose_dir.join(".env");
    let dot_env = if dot_env_path.is_file() {
        let content = std::fs::read_to_string(&dot_env_path).map_err(|e| {
            StackError::ComposeParse(format!("failed to read {}: {e}", dot_env_path.display()))
        })?;
        parse_env_file_content(&content)
    } else {
        HashMap::new()
    };

    let expanded = expand_variables(yaml, &dot_env);
    collect_compose_build_specs(&expanded)
}

/// Extract normalized build directives from already-expanded Compose YAML.
pub fn collect_compose_build_specs(
    expanded_yaml: &str,
) -> Result<BTreeMap<String, ComposeBuildSpec>, StackError> {
    let root: Value =
        serde_yml::from_str(expanded_yaml).map_err(|e| StackError::ComposeParse(e.to_string()))?;
    let root_map = root
        .as_mapping()
        .ok_or_else(|| StackError::ComposeParse("compose file must be a YAML mapping".into()))?;

    let Some(services_map) = root_map.get(val("services")).and_then(Value::as_mapping) else {
        return Ok(BTreeMap::new());
    };

    let mut builds = BTreeMap::new();
    for (key, svc_value) in services_map {
        let svc_name = key
            .as_str()
            .ok_or_else(|| StackError::ComposeParse("service name must be a string".into()))?;
        let svc_map = svc_value.as_mapping().ok_or_else(|| {
            StackError::ComposeParse(format!("service `{svc_name}` must be a YAML mapping"))
        })?;
        if let Some(build_spec) = parse_build_directive(svc_name, svc_map)? {
            builds.insert(svc_name.to_string(), build_spec);
        }
    }

    Ok(builds)
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

    validate_workspace_service_invariants(&services)?;

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
