use super::helpers::val;
use super::*;

pub(super) fn parse_string_or_list(
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
pub(super) fn parse_environment(
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
pub(super) fn parse_ports(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Vec<PortSpec>, StackError> {
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
pub(super) fn parse_mounts(
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
pub(super) fn parse_tmpfs(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Vec<MountSpec>, StackError> {
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
    for key in obj.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "type" && key_str != "source" && key_str != "target" && key_str != "read_only"
        {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.volumes.{key_str}"),
                reason:
                    "only `type`, `source`, `target`, and `read_only` are supported for long-form volumes"
                        .to_string(),
            });
        }
    }

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
pub(super) fn parse_depends_on(
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

    for key in obj.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "condition" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.depends_on.{dep_name}.{key_str}"),
                reason: "only `condition` is supported for `depends_on` entries".to_string(),
            });
        }
    }

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
pub(super) fn parse_healthcheck(
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
pub(super) fn parse_duration_string(s: &str) -> Option<u64> {
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
pub(super) fn parse_restart(
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
pub(super) fn parse_extra_hosts(
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
pub(super) fn parse_deploy(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<ResourcesSpec, StackError> {
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
        return Ok(ResourcesSpec {
            replicas,
            ..ResourcesSpec::default()
        });
    };

    if resources_value.is_null() {
        // resources is null, but we might have replicas
        return Ok(ResourcesSpec {
            replicas,
            ..ResourcesSpec::default()
        });
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
pub(super) fn parse_memory_string(svc_name: &str, s: &str) -> Result<u64, StackError> {
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
