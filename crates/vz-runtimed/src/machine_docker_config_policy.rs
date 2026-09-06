//! Restricted, Machine-local Docker client configuration policy.
//!
//! Registry credentials are mutable opaque strings, never ownership material.
//! Validation neither rewrites them nor includes their contents in diagnostics.
//! The nonempty `credHelpers` map suppresses Docker's automatic default-keychain
//! selection; its reserved, non-routable entry has an empty helper value, which
//! selects the file store. No ambient credential configuration is imported.
//! Filesystem ownership and atomic publication belong to the caller; this module
//! checks the configuration bytes and exact source-admitted plugin directories.

use anyhow::{Result, anyhow, ensure};
use serde::de::{Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
use serde_json::{Map, Number, Value};
use std::fmt;
use std::path::{Component, PathBuf};

const LIMIT: usize = 1024 * 1024;
const FILE_STORE_GUARD: &str = "vz-managed-file-store.invalid";

/// Construct a fresh configuration without consulting ambient credentials.
pub(crate) fn initial_config(plugin_directories: &[PathBuf]) -> Result<Vec<u8>> {
    let directories = plugin_paths(plugin_directories)?;
    let value = serde_json::json!({
        "currentContext": "default",
        "auths": {},
        "credHelpers": {FILE_STORE_GUARD: ""},
        "cliPluginsExtraDirs": directories,
    });
    let mut bytes = serde_json::to_vec(&value)
        .map_err(|_| anyhow!("cannot encode initial Machine Docker configuration"))?;
    bytes.push(b'\n');
    validate_config(&bytes, plugin_directories)?;
    Ok(bytes)
}

/// Admit routing policy while preserving caller-owned credential bytes exactly.
pub(crate) fn validate_config(bytes: &[u8], expected_plugin_directories: &[PathBuf]) -> Result<()> {
    ensure!(
        !bytes.is_empty() && bytes.len() <= LIMIT,
        "Machine Docker configuration exceeds byte bounds"
    );
    let directories = plugin_paths(expected_plugin_directories)?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    // serde_json's bounded recursion remains enabled. Never expose its detailed
    // errors: type errors may otherwise quote a credential-bearing string.
    let StrictValue(value) = StrictValue::deserialize(&mut decoder)
        .map_err(|_| anyhow!("invalid Machine Docker configuration JSON"))?;
    decoder
        .end()
        .map_err(|_| anyhow!("invalid Machine Docker configuration JSON"))?;
    let config = value
        .as_object()
        .ok_or_else(|| anyhow!("Machine Docker configuration must be an object"))?;
    ensure!(
        config.keys().all(|key| matches!(
            key.as_str(),
            "currentContext" | "auths" | "credHelpers" | "credsStore" | "cliPluginsExtraDirs"
        )),
        "unsupported Machine Docker configuration fields"
    );
    ensure!(
        config.get("currentContext").and_then(Value::as_str) == Some("default"),
        "Machine Docker default context differs"
    );
    ensure!(
        config
            .get("credsStore")
            .is_none_or(|value| value.as_str() == Some("")),
        "Machine Docker external credential store is forbidden"
    );
    let helpers = config.get("credHelpers").and_then(Value::as_object);
    ensure!(
        helpers.is_some_and(|helpers| helpers.len() == 1
            && helpers.get(FILE_STORE_GUARD).and_then(Value::as_str) == Some("")),
        "Machine Docker file-store guard differs"
    );
    // Docker's cliPluginsExtraDirs uses `omitempty` when saving credentials.
    // Omission is an empty list only when that is the source-selected policy.
    ensure!(
        config.get("cliPluginsExtraDirs").map_or_else(
            || directories.is_empty(),
            |value| value == &serde_json::json!(directories)
        ),
        "Machine Docker plugin directories differ"
    );
    let auths = config
        .get("auths")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow!("Machine Docker auths must be an object"))?;
    for (registry, credentials) in auths {
        ensure!(
            !registry.is_empty()
                && registry.len() <= 2048
                && !registry
                    .chars()
                    .any(|character| character.is_control() || character.is_whitespace()),
            "invalid Machine Docker registry key"
        );
        let credentials = credentials
            .as_object()
            .ok_or_else(|| anyhow!("Machine Docker auth record must be an object"))?;
        ensure!(
            credentials.iter().all(|(key, value)| {
                matches!(
                    key.as_str(),
                    "auth"
                        | "email"
                        | "username"
                        | "password"
                        | "serveraddress"
                        | "identitytoken"
                        | "registrytoken"
                ) && value.is_string()
            }),
            "invalid Machine Docker auth record fields"
        );
    }
    Ok(())
}

fn plugin_paths(paths: &[PathBuf]) -> Result<Vec<String>> {
    ensure!(
        paths.len() <= 32,
        "too many Machine Docker plugin directories"
    );
    let mut selected = Vec::new();
    for path in paths {
        ensure!(
            path.is_absolute()
                && path
                    .components()
                    .all(|part| matches!(part, Component::RootDir | Component::Normal(_)))
                && path.is_dir()
                && path.canonicalize().ok().as_ref() == Some(path),
            "Machine Docker plugin directory is not canonical"
        );
        let text = path
            .to_str()
            .filter(|text| !text.chars().any(char::is_control))
            .ok_or_else(|| anyhow!("invalid Machine Docker plugin directory"))?;
        ensure!(
            !selected.iter().any(|item| item == text),
            "duplicate Machine Docker plugin directory"
        );
        selected.push(text.to_owned());
    }
    Ok(selected)
}

// No Debug implementation: this wrapper may contain credentials. Each object,
// including nested auth records, is checked before conversion to serde_json.
struct StrictValue(Value);

impl<'de> Deserialize<'de> for StrictValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct StrictVisitor;
        impl<'de> Visitor<'de> for StrictVisitor {
            type Value = StrictValue;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("bounded duplicate-free JSON")
            }
            fn visit_bool<E: serde::de::Error>(
                self,
                value: bool,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Bool(value)))
            }
            fn visit_i64<E: serde::de::Error>(
                self,
                value: i64,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(value.into())))
            }
            fn visit_u64<E: serde::de::Error>(
                self,
                value: u64,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Number(value.into())))
            }
            fn visit_f64<E: serde::de::Error>(
                self,
                value: f64,
            ) -> std::result::Result<Self::Value, E> {
                Number::from_f64(value)
                    .map(|value| StrictValue(Value::Number(value)))
                    .ok_or_else(|| E::custom("nonfinite JSON number"))
            }
            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value.to_owned())))
            }
            fn visit_string<E: serde::de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::String(value)))
            }
            fn visit_unit<E: serde::de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(StrictValue(Value::Null))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(StrictValue(value)) = sequence.next_element()? {
                    values.push(value);
                }
                Ok(StrictValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut object: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = object.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(serde::de::Error::custom("duplicate JSON key"));
                    }
                    let StrictValue(value) = object.next_value()?;
                    values.insert(key, value);
                }
                Ok(StrictValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(StrictVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Result<Value> {
        Ok(serde_json::from_slice(&initial_config(&[])?)?)
    }

    fn rejected(value: &Value) -> Result<()> {
        assert!(validate_config(&serde_json::to_vec(value)?, &[]).is_err());
        Ok(())
    }

    #[test]
    fn initial_is_empty_file_store_without_ambient_inputs() -> Result<()> {
        let bytes = initial_config(&[])?;
        let value: Value = serde_json::from_slice(&bytes)?;
        assert_eq!(value["auths"], serde_json::json!({}));
        assert_eq!(
            value["credHelpers"],
            serde_json::json!({FILE_STORE_GUARD: ""})
        );
        assert!(value.get("credsStore").is_none());
        assert_eq!(bytes, initial_config(&[])?);
        validate_config(&bytes, &[])
    }

    #[test]
    fn mutable_auth_additions_removals_preserve_original_bytes() -> Result<()> {
        let mut value = config()?;
        value["auths"] = serde_json::json!({
            "127.0.0.1:5000": {"auth": "opaque-not-decoded", "email": "public@example.invalid"},
            "https://index.docker.io/v1/": {"username": "user", "password": "secret\n\\\"",
                "serveraddress": "https://index.docker.io/v1/", "identitytoken": "token", "registrytoken": "token"},
            "registry.example.invalid": {},
        });
        let bytes = serde_json::to_vec_pretty(&value)?;
        let original = bytes.clone();
        validate_config(&bytes, &[])?;
        assert_eq!(bytes, original);
        value["auths"] = serde_json::json!({});
        validate_config(&serde_json::to_vec(&value)?, &[])
    }

    #[test]
    fn helper_guard_is_exact_and_external_store_is_forbidden() -> Result<()> {
        for helpers in [
            Value::Null,
            serde_json::json!({}),
            serde_json::json!({FILE_STORE_GUARD: "osxkeychain"}),
            serde_json::json!({"other.invalid": ""}),
            serde_json::json!({FILE_STORE_GUARD: "", "registry.invalid": "helper"}),
        ] {
            let mut value = config()?;
            value["credHelpers"] = helpers;
            rejected(&value)?;
        }
        for store in [
            Value::Null,
            Value::Bool(false),
            serde_json::json!("osxkeychain"),
        ] {
            let mut value = config()?;
            value["credsStore"] = store;
            rejected(&value)?;
        }
        let mut value = config()?;
        value["credsStore"] = serde_json::json!("");
        validate_config(&serde_json::to_vec(&value)?, &[])
    }

    #[test]
    fn routing_and_unknown_fields_are_never_adopted() -> Result<()> {
        for key in ["proxies", "HttpHeaders", "plugins", "detachKeys", "unknown"] {
            let mut value = config()?;
            value[key] = serde_json::json!({});
            rejected(&value)?;
        }
        for context in [Value::Null, Value::Bool(false), serde_json::json!("other")] {
            let mut value = config()?;
            value["currentContext"] = context;
            rejected(&value)?;
        }
        for key in ["currentContext", "auths", "credHelpers"] {
            let mut value = config()?;
            value
                .as_object_mut()
                .ok_or_else(|| anyhow!("expected object fixture"))?
                .remove(key);
            rejected(&value)?;
        }
        Ok(())
    }

    #[test]
    fn duplicate_keys_at_all_depths_and_escaped_equivalents_are_rejected() -> Result<()> {
        for bytes in [
            br#"{"currentContext":"default","currentContext":"default"}"#.as_slice(),
            br#"{"auths":{"registry":{},"registry":{}}}"#,
            br#"{"auths":{"registry":{"auth":"private-canary","auth":"x"}}}"#,
            br#"{"auths":{"registry":{"auth":"private-canary","\u0061uth":"x"}}}"#,
        ] {
            let error = validate_config(bytes, &[])
                .err()
                .ok_or_else(|| anyhow!("duplicate key accepted"))?;
            assert_eq!(
                error.to_string(),
                "invalid Machine Docker configuration JSON"
            );
            assert!(!format!("{error:?}").contains("private-canary"));
        }
        Ok(())
    }

    #[test]
    fn auth_schema_and_registry_keys_fail_without_echoing_credentials() -> Result<()> {
        for record in [
            Value::Null,
            serde_json::json!("private-canary"),
            serde_json::json!({"auth": false}),
            serde_json::json!({"unknown": "private-canary"}),
        ] {
            let mut value = config()?;
            value["auths"] = serde_json::json!({"registry.invalid": record});
            let error = validate_config(&serde_json::to_vec(&value)?, &[])
                .err()
                .ok_or_else(|| anyhow!("invalid record accepted"))?;
            assert!(!format!("{error:?}").contains("private-canary"));
        }
        for key in ["", "registry private-canary", "registry\nprivate-canary"] {
            let mut value = config()?;
            value["auths"] = serde_json::json!({key: {}});
            rejected(&value)?;
        }
        Ok(())
    }

    #[test]
    fn malformed_nonobject_trailing_oversized_and_deep_json_are_rejected() -> Result<()> {
        for bytes in [
            b"".as_slice(),
            b"null",
            b"[]",
            b"true",
            b"{",
            b"{\"private-canary\":NaN}",
            b"\xff",
        ] {
            assert!(validate_config(bytes, &[]).is_err());
        }
        let mut bytes = initial_config(&[])?;
        bytes.extend_from_slice(b"{}");
        assert!(validate_config(&bytes, &[]).is_err());
        assert!(validate_config(&vec![b' '; LIMIT + 1], &[]).is_err());
        assert!(
            validate_config(
                format!("{}0{}", "[".repeat(256), "]".repeat(256)).as_bytes(),
                &[]
            )
            .is_err()
        );
        Ok(())
    }

    #[test]
    fn omitted_plugin_directories_require_empty_expected_list() -> Result<()> {
        let mut value = config()?;
        value
            .as_object_mut()
            .ok_or_else(|| anyhow!("expected object fixture"))?
            .remove("cliPluginsExtraDirs");
        let saved = serde_json::to_vec(&value)?;
        validate_config(&saved, &[])?;
        let directory = tempfile::tempdir()?;
        assert!(validate_config(&saved, &[directory.path().canonicalize()?]).is_err());
        for changed in [
            Value::Null,
            Value::Bool(false),
            serde_json::json!(["/foreign"]),
        ] {
            value["cliPluginsExtraDirs"] = changed;
            rejected(&value)?;
        }
        Ok(())
    }

    #[test]
    fn exact_ordered_canonical_plugin_directories_are_required() -> Result<()> {
        let first = tempfile::tempdir()?;
        let second = tempfile::tempdir()?;
        let paths = vec![first.path().canonicalize()?, second.path().canonicalize()?];
        let bytes = initial_config(&paths)?;
        validate_config(&bytes, &paths)?;
        assert!(validate_config(&bytes, &[]).is_err());
        assert!(validate_config(&bytes, &[paths[1].clone(), paths[0].clone()]).is_err());
        assert!(initial_config(&[paths[0].clone(), paths[0].clone()]).is_err());
        assert!(initial_config(&[PathBuf::from("relative")]).is_err());
        assert!(initial_config(&[paths[0].join("missing")]).is_err());
        let link = first.path().join("link");
        std::os::unix::fs::symlink(&paths[1], &link)?;
        assert!(initial_config(&[link]).is_err());
        Ok(())
    }
}
