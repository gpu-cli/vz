use super::helpers::val;
use super::*;

pub(super) fn parse_networks(root: &serde_yml::Mapping) -> Result<Vec<NetworkSpec>, StackError> {
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

        for key in net_map.keys() {
            let key_str = key.as_str().unwrap_or("");
            if key_str != "driver" && key_str != "ipam" {
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: format!("networks.{net_name}.{key_str}"),
                    reason: "only `driver` and `ipam` are supported for networks".to_string(),
                });
            }
        }

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
        if let Some(ipam_value) = net_map.get(val("ipam"))
            && let Some(ipam_map) = ipam_value.as_mapping()
        {
            for key in ipam_map.keys() {
                let key_str = key.as_str().unwrap_or("");
                if key_str != "config" {
                    return Err(StackError::ComposeUnsupportedFeature {
                        feature: format!("networks.{net_name}.ipam.{key_str}"),
                        reason: "only `ipam.config` is supported".to_string(),
                    });
                }
            }
            if let Some(config_value) = ipam_map.get(val("config"))
                && let Some(config_seq) = config_value.as_sequence()
            {
                for (index, item) in config_seq.iter().enumerate() {
                    if let Some(config_map) = item.as_mapping() {
                        for key in config_map.keys() {
                            let key_str = key.as_str().unwrap_or("");
                            if key_str != "subnet" {
                                return Err(StackError::ComposeUnsupportedFeature {
                                    feature: format!(
                                        "networks.{net_name}.ipam.config[{index}].{key_str}"
                                    ),
                                    reason:
                                        "only `subnet` is supported inside `ipam.config` entries"
                                            .to_string(),
                                });
                            }
                        }
                    }
                }
            }
        }

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
pub(super) fn parse_service_networks(
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
        for (key, network_value) in net_map {
            let name = key.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `networks` mapping keys must be strings"
                ))
            })?;

            if network_value.is_null() {
                names.push(name.to_string());
                continue;
            }

            let Some(attachment_map) = network_value.as_mapping() else {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `networks.{name}` must be a mapping or null"
                )));
            };

            if let Some(attachment_key) = attachment_map.keys().next() {
                let attachment_key = attachment_key.as_str().unwrap_or("");
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: format!("services.{svc_name}.networks.{name}.{attachment_key}"),
                    reason:
                        "network attachment options are not supported; use plain network membership"
                            .to_string(),
                });
            }

            names.push(name.to_string());
        }
        return Ok(names);
    }

    Err(StackError::ComposeParse(format!(
        "service `{svc_name}`: `networks` must be a list or mapping"
    )))
}

/// Parse `network_mode` for deterministic diagnostics.
///
/// Current runtime supports default bridge networking only.
/// `network_mode: bridge` is accepted as an explicit no-op.
pub(super) fn parse_network_mode(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Option<String>, StackError> {
    let Some(value) = map.get(val("network_mode")) else {
        return Ok(None);
    };

    let mode = value.as_str().ok_or_else(|| {
        StackError::ComposeParse(format!(
            "service `{svc_name}`: `network_mode` must be a string"
        ))
    })?;
    let mode = mode.trim();
    if mode.is_empty() {
        return Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `network_mode` must not be empty"
        )));
    }

    match mode {
        "bridge" => Ok(Some(mode.to_string())),
        "host" | "none" => Err(StackError::ComposeUnsupportedFeature {
            feature: format!("services.{svc_name}.network_mode"),
            reason: format!(
                "`network_mode: {mode}` is not supported by this runtime; supported value is `bridge`"
            ),
        }),
        _ if mode.starts_with("service:") || mode.starts_with("container:") => {
            Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.network_mode"),
                reason: format!(
                    "`network_mode: {mode}` is not supported; use default bridge networking and service DNS"
                ),
            })
        }
        other => Err(StackError::ComposeValidation(format!(
            "service `{svc_name}`: unsupported `network_mode` `{other}`; supported value is `bridge`"
        ))),
    }
}
