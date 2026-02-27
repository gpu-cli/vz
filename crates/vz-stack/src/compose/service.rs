use super::env::load_env_file_entries;
use super::fields::{
    parse_depends_on, parse_deploy, parse_duration_string, parse_environment, parse_extra_hosts,
    parse_healthcheck, parse_mounts, parse_ports, parse_restart, parse_string_or_list, parse_tmpfs,
};
use super::helpers::val;
use super::interaction::{parse_expose, parse_labels, parse_logging};
use super::networks::{parse_network_mode, parse_service_networks};
use super::secrets::parse_service_secrets;
use super::security::{parse_string_list, parse_string_map, parse_ulimits};
use super::validation::validate_service_keys;
use super::*;

pub(super) fn parse_service(
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

    let build = parse_build_directive(name, svc_map)?;
    let has_build = build.is_some();
    let image = svc_map
        .get(val("image"))
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .or_else(|| {
            if has_build {
                Some(default_compose_build_image(name))
            } else {
                None
            }
        })
        .ok_or_else(|| {
            StackError::ComposeValidation(format!(
                "service `{name}` must define `image` or `build`"
            ))
        })?;
    let kind = parse_service_kind(name, svc_map)?;

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
    let network_mode = parse_network_mode(name, svc_map)?;
    let networks = parse_service_networks(name, svc_map)?;
    if network_mode.is_some() && svc_map.get(val("networks")).is_some() {
        return Err(StackError::ComposeValidation(format!(
            "service `{name}` cannot set both `network_mode` and `networks`; choose one networking model"
        )));
    }

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

    // Interactive mode
    let expose = parse_expose(name, svc_map)?;
    let stdin_open = svc_map
        .get(val("stdin_open"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);
    let tty = svc_map
        .get(val("tty"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Logging
    let logging = parse_logging(name, svc_map)?;

    Ok(ServiceSpec {
        name: name.to_string(),
        kind,
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
        expose,
        stdin_open,
        tty,
        logging,
    })
}

pub(super) fn parse_build_directive(
    svc_name: &str,
    svc_map: &serde_yml::Mapping,
) -> Result<Option<ComposeBuildSpec>, StackError> {
    let Some(value) = svc_map.get(val("build")) else {
        return Ok(None);
    };

    if let Some(context) = value.as_str() {
        if context.trim().is_empty() {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: `build` context must not be empty"
            )));
        }
        return Ok(Some(ComposeBuildSpec {
            service_name: svc_name.to_string(),
            context: context.trim().to_string(),
            dockerfile: None,
            target: None,
            args: BTreeMap::new(),
            cache_from: Vec::new(),
        }));
    }

    let Some(build_map) = value.as_mapping() else {
        return Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `build` must be a string or mapping"
        )));
    };

    for key in build_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "context"
            && key_str != "dockerfile"
            && key_str != "args"
            && key_str != "target"
            && key_str != "cache_from"
        {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.build.{key_str}"),
                reason: "only `context`, `dockerfile`, `args`, `target`, and `cache_from` are supported under `build`".to_string(),
            });
        }
    }

    if let Some(context_value) = build_map.get(val("context")) {
        let context = context_value.as_str().ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: `build.context` must be a string"
            ))
        })?;
        if context.trim().is_empty() {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: `build.context` must not be empty"
            )));
        }
    }

    if let Some(dockerfile_value) = build_map.get(val("dockerfile")) {
        if dockerfile_value.as_str().is_none() {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: `build.dockerfile` must be a string"
            )));
        }
    }

    if let Some(target_value) = build_map.get(val("target")) {
        if target_value.as_str().is_none() {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: `build.target` must be a string"
            )));
        }
    }

    let context = build_map
        .get(val("context"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(".")
        .to_string();
    let dockerfile = build_map
        .get(val("dockerfile"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let target = build_map
        .get(val("target"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let args = if let Some(args_value) = build_map.get(val("args")) {
        parse_build_args(svc_name, args_value)?
    } else {
        BTreeMap::new()
    };
    let cache_from = if let Some(cache_from_value) = build_map.get(val("cache_from")) {
        parse_build_cache_from(svc_name, cache_from_value)?
    } else {
        Vec::new()
    };

    Ok(Some(ComposeBuildSpec {
        service_name: svc_name.to_string(),
        context,
        dockerfile,
        target,
        args,
        cache_from,
    }))
}

fn parse_build_args(
    svc_name: &str,
    value: &serde_yml::Value,
) -> Result<BTreeMap<String, String>, StackError> {
    if let Some(obj) = value.as_mapping() {
        let mut args = BTreeMap::new();
        for (k, v) in obj {
            let key = k.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `build.args` keys must be strings"
                ))
            })?;
            let value = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                Value::Null => String::new(),
                _ => {
                    return Err(StackError::ComposeParse(format!(
                        "service `{svc_name}`: `build.args` values must be scalars"
                    )));
                }
            };
            args.insert(key.to_string(), value);
        }
        return Ok(args);
    }

    if let Some(seq) = value.as_sequence() {
        let mut args = BTreeMap::new();
        for entry in seq {
            let item = entry.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `build.args` list entries must be strings"
                ))
            })?;
            let Some((key, val)) = item.split_once('=') else {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `build.args` entries must be \"KEY=VALUE\", got \"{item}\""
                )));
            };
            args.insert(key.to_string(), val.to_string());
        }
        return Ok(args);
    }

    Err(StackError::ComposeParse(format!(
        "service `{svc_name}`: `build.args` must be a mapping or list"
    )))
}

fn parse_build_cache_from(
    svc_name: &str,
    value: &serde_yml::Value,
) -> Result<Vec<String>, StackError> {
    if value.is_null() {
        return Ok(Vec::new());
    }

    if let Some(single) = value.as_str() {
        if single.trim().is_empty() {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: `build.cache_from` entries must not be empty"
            )));
        }
        return Ok(vec![single.to_string()]);
    }

    if let Some(seq) = value.as_sequence() {
        let mut values = Vec::with_capacity(seq.len());
        for entry in seq {
            let image = entry.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `build.cache_from` entries must be strings"
                ))
            })?;
            if image.trim().is_empty() {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `build.cache_from` entries must not be empty"
                )));
            }
            values.push(image.to_string());
        }
        return Ok(values);
    }

    Err(StackError::ComposeParse(format!(
        "service `{svc_name}`: `build.cache_from` must be a string or list"
    )))
}

fn default_compose_build_image(service_name: &str) -> String {
    let mut normalized = String::new();
    for ch in service_name.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            normalized.push(ch.to_ascii_lowercase());
        } else {
            normalized.push('-');
        }
    }
    let stem = normalized.trim_matches('-');
    if stem.is_empty() {
        "compose-build:latest".to_string()
    } else {
        format!("{stem}:latest")
    }
}

fn parse_service_kind(
    svc_name: &str,
    svc_map: &serde_yml::Mapping,
) -> Result<ServiceKind, StackError> {
    let Some(xvz_value) = svc_map.get(val("x-vz")) else {
        return Ok(ServiceKind::Service);
    };

    let xvz_map = xvz_value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `x-vz` must be a mapping"))
    })?;

    for key in xvz_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "kind" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.x-vz.{key_str}"),
                reason: "unknown `x-vz` service extension key; accepted keys are: kind".to_string(),
            });
        }
    }

    let Some(kind_value) = xvz_map.get(val("kind")) else {
        return Ok(ServiceKind::Service);
    };
    let kind_str = kind_value.as_str().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: `x-vz.kind` must be a string"
        ))
    })?;

    match kind_str.trim() {
        "workspace" => Ok(ServiceKind::Workspace),
        "service" => Ok(ServiceKind::Service),
        "task" => Ok(ServiceKind::Task),
        other => Err(StackError::ComposeValidation(format!(
            "service `{svc_name}`: invalid `x-vz.kind` `{other}`; expected workspace|service|task"
        ))),
    }
}

pub(super) fn validate_workspace_service_invariants(
    services: &[ServiceSpec],
) -> Result<(), StackError> {
    let workspace_services: Vec<&str> = services
        .iter()
        .filter(|svc| svc.kind == ServiceKind::Workspace)
        .map(|svc| svc.name.as_str())
        .collect();

    if workspace_services.len() > 1 {
        return Err(StackError::ComposeValidation(format!(
            "multiple workspace services defined ({}); only one workspace service is allowed",
            workspace_services.join(", ")
        )));
    }

    let service_map: HashMap<&str, &ServiceSpec> = services
        .iter()
        .map(|svc| (svc.name.as_str(), svc))
        .collect();
    for service in services {
        for dep in &service.depends_on {
            if dep.condition != DependencyCondition::ServiceHealthy {
                continue;
            }
            let Some(dep_service) = service_map.get(dep.service.as_str()) else {
                continue;
            };
            if dep_service.healthcheck.is_none() {
                return Err(StackError::ComposeValidation(format!(
                    "service `{}` depends_on `{}` with condition `service_healthy`, but `{}` has no healthcheck",
                    service.name, dep.service, dep.service
                )));
            }
        }
    }

    Ok(())
}
