use super::helpers::val;
use super::*;

pub(super) fn parse_secrets_top_level(
    root: &serde_yml::Mapping,
) -> Result<Vec<SecretDef>, StackError> {
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
                "secret `{secret_name}` must be a mapping with a `file` or `environment` key"
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
                reason: "external secrets are not supported; use file-based or environment-based secrets".to_string(),
            });
        }

        let file = secret_map
            .get(val("file"))
            .and_then(|v| v.as_str())
            .map(String::from);

        let environment = secret_map
            .get(val("environment"))
            .and_then(|v| v.as_str())
            .map(String::from);

        let source = match (file, environment) {
            (Some(f), _) => SecretSource::File(f),
            (None, Some(env_var)) => SecretSource::Environment(env_var),
            (None, None) => {
                return Err(StackError::ComposeValidation(format!(
                    "secret `{secret_name}` must define either `file` or `environment`"
                )));
            }
        };

        secrets.push(SecretDef {
            name: secret_name.to_string(),
            source,
        });
    }

    // Sort for determinism.
    secrets.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(secrets)
}

/// Default file mode for secret mounts: read-only for all (0o444 = 292 decimal).
const DEFAULT_SECRET_MODE: u32 = 0o444;

/// Parse service-level `secrets` references.
///
/// Supports:
/// - Short form: just a string name (source and target both set to the name,
///   mode defaults to 0o444, uid/gid default to 0)
/// - Long form: mapping with `source`, optional `target`, `mode`, `uid`, `gid`
///
/// Each referenced secret must be defined in the top-level `secrets` section.
/// Secrets are always mounted read-only with lifecycle scoped to the container.
pub(super) fn parse_service_secrets(
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
                mode: DEFAULT_SECRET_MODE,
                uid: 0,
                gid: 0,
            }
        } else if let Some(obj) = item.as_mapping() {
            // Long form: { source: ..., target: ..., mode: ..., uid: ..., gid: ... }
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

            // Mode can be specified as an integer (octal or decimal in YAML).
            // Compose convention: `0444` in YAML is parsed as decimal 444 by
            // some parsers, but serde_yml treats unquoted `0444` as octal 292.
            // We accept the raw integer value from YAML as-is.
            let mode = obj
                .get(val("mode"))
                .map(|v| {
                    // Accept both integer and string forms.
                    if let Some(n) = v.as_u64() {
                        Ok(n as u32)
                    } else if let Some(s) = v.as_str() {
                        // Parse octal string like "0444".
                        u32::from_str_radix(s.trim_start_matches('0'), 8).map_err(|_| {
                            StackError::ComposeParse(format!(
                                "service `{svc_name}`: invalid secret mode `{s}`"
                            ))
                        })
                    } else {
                        Err(StackError::ComposeParse(format!(
                            "service `{svc_name}`: secret `mode` must be an integer or octal string"
                        )))
                    }
                })
                .transpose()?
                .unwrap_or(DEFAULT_SECRET_MODE);

            let uid = obj
                .get(val("uid"))
                .and_then(|v| v.as_u64())
                .map(|n| n as u32)
                .unwrap_or(0);

            let gid = obj
                .get(val("gid"))
                .and_then(|v| v.as_u64())
                .map(|n| n as u32)
                .unwrap_or(0);

            ServiceSecretRef {
                source,
                target,
                mode,
                uid,
                gid,
            }
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
