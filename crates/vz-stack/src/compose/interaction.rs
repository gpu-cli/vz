use super::fields::parse_memory_string;
use super::helpers::val;
use super::security::parse_string_map;
use super::*;

pub(super) fn parse_labels(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<HashMap<String, String>, StackError> {
    parse_string_map(svc_name, map, "labels")
}

// ── Interactive mode & logging parsers ──────────────────────────────

/// Parse the `expose` field — a list of ports to expose without host binding.
///
/// Supports both integer and string forms:
/// ```yaml
/// expose:
///   - 3000
///   - "8000"
/// ```
pub(super) fn parse_expose(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Vec<u16>, StackError> {
    let Some(value) = map.get(val("expose")) else {
        return Ok(vec![]);
    };

    let seq = value.as_sequence().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `expose` must be a list"))
    })?;

    let mut ports = Vec::new();
    for item in seq {
        let port = if let Some(n) = item.as_u64() {
            u16::try_from(n).map_err(|_| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `expose` port {n} is out of range (1-65535)"
                ))
            })?
        } else if let Some(s) = item.as_str() {
            s.parse::<u16>().map_err(|_| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `expose` port `{s}` is not a valid port number"
                ))
            })?
        } else {
            return Err(StackError::ComposeParse(format!(
                "service `{svc_name}`: `expose` items must be port numbers"
            )));
        };
        ports.push(port);
    }
    Ok(ports)
}

/// Parse the `logging` configuration block.
///
/// ```yaml
/// logging:
///   driver: json-file
///   options:
///     max-size: "10m"
///     max-file: "3"
/// ```
pub(super) fn parse_logging(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Option<LoggingConfig>, StackError> {
    let Some(value) = map.get(val("logging")) else {
        return Ok(None);
    };

    let log_map = value.as_mapping().ok_or_else(|| {
        StackError::ComposeParse(format!("service `{svc_name}`: `logging` must be a mapping"))
    })?;

    let driver = log_map
        .get(val("driver"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: `logging.driver` is required and must be a string"
            ))
        })?;
    let driver = driver.trim().to_ascii_lowercase();
    if driver.is_empty() {
        return Err(StackError::ComposeParse(format!(
            "service `{svc_name}`: `logging.driver` must not be empty"
        )));
    }

    let options = if let Some(opts_value) = log_map.get(val("options")) {
        let opts_map = opts_value.as_mapping().ok_or_else(|| {
            StackError::ComposeParse(format!(
                "service `{svc_name}`: `logging.options` must be a mapping"
            ))
        })?;
        let mut result = HashMap::new();
        for (k, v) in opts_map {
            let key = k.as_str().ok_or_else(|| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `logging.options` keys must be strings"
                ))
            })?;
            let value_str = match v {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                _ => {
                    return Err(StackError::ComposeParse(format!(
                        "service `{svc_name}`: `logging.options` values must be scalars"
                    )));
                }
            };
            result.insert(key.to_string(), value_str);
        }
        result
    } else {
        HashMap::new()
    };

    // Reject unknown keys inside logging.
    for key in log_map.keys() {
        let key_str = key.as_str().unwrap_or("");
        if key_str != "driver" && key_str != "options" {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.logging.{key_str}"),
                reason: "only `driver` and `options` are supported inside `logging`".to_string(),
            });
        }
    }

    match driver.as_str() {
        "json-file" | "local" | "none" => {}
        "syslog" => {
            return Err(StackError::ComposeUnsupportedFeature {
                feature: format!("services.{svc_name}.logging.driver"),
                reason: "logging driver `syslog` is not supported by this runtime; supported drivers are `json-file`, `local`, and `none`".to_string(),
            });
        }
        other => {
            return Err(StackError::ComposeValidation(format!(
                "service `{svc_name}`: unsupported logging.driver `{other}`; supported drivers are `json-file`, `local`, and `none`"
            )));
        }
    }

    if driver == "none" && !options.is_empty() {
        return Err(StackError::ComposeValidation(format!(
            "service `{svc_name}`: `logging.options` is not allowed when `logging.driver` is `none`"
        )));
    }

    if driver == "json-file" || driver == "local" {
        for key in options.keys() {
            if key == "labels" || key == "tag" {
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: format!("services.{svc_name}.logging.options.{key}"),
                    reason: format!("logging option `{key}` is not supported yet"),
                });
            }
            if key != "max-size" && key != "max-file" {
                return Err(StackError::ComposeUnsupportedFeature {
                    feature: format!("services.{svc_name}.logging.options.{key}"),
                    reason: "supported logging options are `max-size` and `max-file`".to_string(),
                });
            }
        }

        if let Some(max_size) = options.get("max-size") {
            parse_memory_string(svc_name, max_size).map_err(|_| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: invalid `logging.options.max-size` value `{max_size}`"
                ))
            })?;
        }
        if let Some(max_file) = options.get("max-file") {
            let parsed = max_file.parse::<u32>().map_err(|_| {
                StackError::ComposeParse(format!(
                    "service `{svc_name}`: `logging.options.max-file` must be a positive integer"
                ))
            })?;
            if parsed == 0 {
                return Err(StackError::ComposeParse(format!(
                    "service `{svc_name}`: `logging.options.max-file` must be at least 1"
                )));
            }
        }
    }

    Ok(Some(LoggingConfig { driver, options }))
}
