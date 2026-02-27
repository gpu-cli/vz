use super::helpers::val;
use super::*;

pub(super) fn parse_string_list(
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
pub(super) fn parse_string_map(
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
pub(super) fn parse_ulimits(
    svc_name: &str,
    map: &serde_yml::Mapping,
) -> Result<Vec<UlimitSpec>, StackError> {
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
