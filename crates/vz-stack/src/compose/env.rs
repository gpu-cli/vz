use super::helpers::val;
use super::*;

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
pub(super) fn load_env_file_entries(
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
