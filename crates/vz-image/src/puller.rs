//! OCI image puller and image config resolution.

use std::str::FromStr;

use docker_credential::{DockerCredential, get_credential};
use oci_distribution::client::ClientConfig;
use oci_distribution::errors::OciDistributionError;
use oci_distribution::manifest::ImageIndexEntry;
use oci_distribution::manifest::OciDescriptor;
use oci_distribution::secrets::RegistryAuth;
use oci_distribution::{Client, Reference};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_stream::StreamExt;

use crate::error::ImageError;
use crate::store::ImageStore;

/// Registry authentication used when pulling OCI images.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Auth {
    /// Access the registry anonymously.
    #[default]
    Anonymous,
    /// Authenticate to the registry with username and password.
    Basic {
        /// Registry username.
        username: String,
        /// Registry password.
        password: String,
    },
    /// Load credentials from the local Docker credential configuration.
    DockerConfig,
}

/// Canonical digest identity for a pulled OCI image manifest.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ImageId(pub String);

/// Minimal image runtime configuration extracted from OCI image config JSON.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ImageConfigSummary {
    /// Optional default entrypoint command.
    pub entrypoint: Option<Vec<String>>,
    /// Optional default command arguments.
    pub cmd: Option<Vec<String>>,
    /// Optional default environment entries (`KEY=VALUE`).
    pub env: Option<Vec<String>>,
    /// Optional working directory.
    pub working_dir: Option<String>,
    /// Optional default user.
    pub user: Option<String>,
}

impl ImageConfigSummary {
    /// Resolve the final command: user command wins if non-empty,
    /// otherwise falls back to image entrypoint + cmd.
    pub fn resolve_cmd(&self, user_cmd: &[String]) -> Option<Vec<String>> {
        if !user_cmd.is_empty() {
            return Some(user_cmd.to_vec());
        }

        let mut image_cmd = Vec::new();
        if let Some(ref entrypoint) = self.entrypoint {
            image_cmd.extend(entrypoint.iter().cloned());
        }
        if let Some(ref cmd) = self.cmd {
            image_cmd.extend(cmd.iter().cloned());
        }

        if image_cmd.is_empty() {
            None
        } else {
            Some(image_cmd)
        }
    }

    /// Merge image env with user env. User values override image values.
    /// Always injects `VZ_CONTAINER_ID`.
    pub fn resolve_env(
        &self,
        user_env: &[(String, String)],
        container_id: &str,
    ) -> Vec<(String, String)> {
        let mut merged: Vec<(String, String)> = self
            .env
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(|entry| {
                entry
                    .split_once('=')
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .unwrap_or_else(|| (entry.clone(), String::new()))
            })
            .collect();

        for (run_key, run_value) in user_env {
            let mut was_updated = false;
            for (existing_key, existing_value) in merged.iter_mut() {
                if *existing_key == *run_key {
                    *existing_value = run_value.clone();
                    was_updated = true;
                }
            }

            if !was_updated && !merged.iter().any(|(key, _)| *key == *run_key) {
                merged.push((run_key.clone(), run_value.clone()));
            }
        }

        merged.retain(|(key, _)| key != "VZ_CONTAINER_ID");
        merged.push(("VZ_CONTAINER_ID".to_string(), container_id.to_string()));

        merged
    }

    /// User working_dir wins over image working_dir.
    pub fn resolve_working_dir(&self, user_dir: Option<&str>) -> Option<String> {
        user_dir
            .map(String::from)
            .or_else(|| self.working_dir.clone())
    }

    /// User user wins over image user.
    pub fn resolve_user(&self, user_user: Option<&str>) -> Option<String> {
        user_user.map(String::from).or_else(|| self.user.clone())
    }
}

#[derive(Debug, Deserialize, Default)]
struct RawImageConfigEnvelope {
    #[serde(default)]
    config: RawImageConfig,
}

#[derive(Debug, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
struct RawImageConfig {
    #[serde(default)]
    entrypoint: Vec<String>,
    #[serde(default)]
    cmd: Vec<String>,
    #[serde(default)]
    env: Vec<String>,
    #[serde(default)]
    working_dir: String,
    #[serde(default)]
    user: String,
}

/// Pulls OCI images from remote registries and stores them in `ImageStore`.
#[derive(Clone)]
pub struct ImagePuller {
    client: Client,
    store: ImageStore,
}

impl ImagePuller {
    /// Build an image puller that auto-detects the host platform.
    ///
    /// On `aarch64` hosts: resolves `linux/arm64` variants.
    /// On `x86_64` hosts: resolves `linux/amd64` variants.
    pub fn new(store: ImageStore) -> Self {
        let target_os = "linux";
        let target_arch = if cfg!(target_arch = "aarch64") {
            "arm64"
        } else {
            "amd64"
        };
        Self::with_platform(store, target_os, target_arch)
    }

    /// Build an image puller targeting a specific OS and architecture.
    pub fn with_platform(store: ImageStore, os: &str, arch: &str) -> Self {
        let target_os = os.to_string();
        let target_arch = arch.to_string();
        let config = ClientConfig {
            platform_resolver: Some(Box::new(move |entries: &[ImageIndexEntry]| {
                select_platform_digest(entries, &target_os, &target_arch)
            })),
            ..ClientConfig::default()
        };
        let client = Client::new(config);
        Self { client, store }
    }

    /// Pull `reference` into local storage and return its manifest digest id.
    pub async fn pull(&self, reference: &str, auth: &Auth) -> Result<ImageId, ImageError> {
        self.store.ensure_layout()?;

        let parsed =
            Reference::from_str(reference).map_err(|error| ImageError::ImageReferenceParse {
                reference: reference.to_string(),
                reason: error.to_string(),
            })?;

        // Return cached image if reference is already resolved locally.
        if let Ok(cached_digest) = self.store.read_reference(&parsed.whole()) {
            if self.store.read_manifest_json(&cached_digest).is_ok() {
                tracing::debug!(reference = %reference, digest = %cached_digest, "using cached image");
                return Ok(ImageId(cached_digest));
            }
        }

        let registry_auth = resolve_registry_auth(&parsed, auth)?;

        let (manifest, digest, config_json) = self
            .client
            .pull_manifest_and_config(&parsed, &registry_auth)
            .await
            .map_err(|error| map_pull_error(reference, error))?;

        let _config_summary = parse_image_config_summary(&config_json)?;

        for layer in &manifest.layers {
            if self.store.has_layer_blob(&layer.digest) {
                continue;
            }

            let layer_bytes = self
                .pull_layer_blob_bytes(&parsed, &layer.digest, layer, reference)
                .await?;
            self.store
                .write_layer_blob(&layer.digest, &layer.media_type, &layer_bytes)?;
        }

        let manifest_json = serde_json::to_vec(&manifest).map_err(|error| {
            ImageError::InvalidConfig(format!(
                "failed to serialize pulled manifest for {reference}: {error}"
            ))
        })?;

        let image_id = ImageId(digest);
        self.store
            .write_manifest_json(&image_id.0, &manifest_json)?;
        self.store
            .write_config_json(&image_id.0, config_json.as_bytes())?;
        self.store.write_reference(&parsed.whole(), &image_id.0)?;

        Ok(image_id)
    }

    async fn pull_layer_blob_bytes(
        &self,
        reference: &Reference,
        descriptor_digest: &str,
        descriptor: &OciDescriptor,
        original_reference: &str,
    ) -> Result<Vec<u8>, ImageError> {
        let mut stream = self
            .client
            .pull_blob_stream(reference, descriptor)
            .await
            .map_err(|error| map_pull_error(original_reference, error))?;

        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk?;
            out.extend_from_slice(&bytes);
        }

        verify_blob_digest(descriptor_digest, &out)?;
        Ok(out)
    }
}

fn map_pull_error(reference: &str, error: OciDistributionError) -> ImageError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("platform") {
        return ImageError::PlatformMismatch {
            reference: reference.to_string(),
            details: message,
        };
    }

    ImageError::Pull {
        reference: reference.to_string(),
        source: error,
    }
}

fn select_platform_digest(
    entries: &[ImageIndexEntry],
    target_os: &str,
    target_arch: &str,
) -> Option<String> {
    entries.iter().find_map(|entry| {
        let platform = entry.platform.as_ref()?;
        if platform.os == target_os && platform.architecture == target_arch {
            return Some(entry.digest.clone());
        }

        None
    })
}

fn resolve_registry_auth(reference: &Reference, auth: &Auth) -> Result<RegistryAuth, ImageError> {
    match auth {
        Auth::Anonymous => Ok(RegistryAuth::Anonymous),
        Auth::Basic { username, password } => {
            Ok(RegistryAuth::Basic(username.clone(), password.clone()))
        }
        Auth::DockerConfig => {
            let server = docker_server_for_registry(reference.registry());
            let credential = get_credential(&server)?;
            match credential {
                DockerCredential::UsernamePassword(username, password) => {
                    Ok(RegistryAuth::Basic(username, password))
                }
                DockerCredential::IdentityToken(_) => Err(ImageError::AuthenticationUnsupported(
                    "docker identity-token credentials are not supported".to_string(),
                )),
            }
        }
    }
}

fn docker_server_for_registry(registry: &str) -> String {
    if matches!(
        registry,
        "docker.io" | "index.docker.io" | "registry-1.docker.io"
    ) {
        return "https://index.docker.io/v1/".to_string();
    }

    registry.to_string()
}

fn parse_image_config_summary(config_json: &str) -> Result<ImageConfigSummary, ImageError> {
    let envelope: RawImageConfigEnvelope = serde_json::from_str(config_json)?;
    let config = envelope.config;

    let entrypoint = (!config.entrypoint.is_empty()).then_some(config.entrypoint);
    let cmd = (!config.cmd.is_empty()).then_some(config.cmd);
    let env = (!config.env.is_empty()).then_some(config.env);
    let working_dir = (!config.working_dir.is_empty()).then_some(config.working_dir);
    let user = (!config.user.is_empty()).then_some(config.user);

    Ok(ImageConfigSummary {
        entrypoint,
        cmd,
        env,
        working_dir,
        user,
    })
}

/// Read and parse image config JSON from local image storage.
pub fn parse_image_config_summary_from_store(
    store: &ImageStore,
    image_id: &str,
) -> Result<ImageConfigSummary, ImageError> {
    let config_json = store.read_config_json(image_id)?;
    parse_image_config_summary(std::str::from_utf8(&config_json).map_err(|error| {
        ImageError::InvalidConfig(format!("stored image config is not valid utf-8: {error}"))
    })?)
}

fn verify_blob_digest(descriptor_digest: &str, bytes: &[u8]) -> Result<(), ImageError> {
    let (algorithm, expected) = descriptor_digest.split_once(':').ok_or_else(|| {
        ImageError::InvalidConfig(format!("invalid digest format: {descriptor_digest}"))
    })?;

    if algorithm != "sha256" {
        return Err(ImageError::UnsupportedDigestAlgorithm {
            algorithm: algorithm.to_string(),
        });
    }

    let expected = expected.to_ascii_lowercase();
    let actual = format!("{:x}", Sha256::digest(bytes));
    if expected != actual {
        return Err(ImageError::DigestMismatch {
            digest: descriptor_digest.to_string(),
            expected,
            actual,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    #[test]
    fn parse_config_summary_reads_common_fields() {
        let raw = r#"{
  "config": {
    "Entrypoint": ["/usr/bin/python3"],
    "Cmd": ["-c", "print(42)"],
    "Env": ["A=1", "B=2"],
    "WorkingDir": "/workspace",
    "User": "1000:1000"
  }
}"#;

        let summary = parse_image_config_summary(raw).expect("config summary should parse");
        assert_eq!(
            summary.entrypoint,
            Some(vec!["/usr/bin/python3".to_string()])
        );
        assert_eq!(
            summary.cmd,
            Some(vec!["-c".to_string(), "print(42)".to_string()])
        );
        assert_eq!(
            summary.env,
            Some(vec!["A=1".to_string(), "B=2".to_string()])
        );
        assert_eq!(summary.working_dir.as_deref(), Some("/workspace"));
        assert_eq!(summary.user.as_deref(), Some("1000:1000"));
    }

    #[test]
    fn verify_blob_digest_accepts_sha256() {
        let digest = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        assert!(verify_blob_digest(digest, b"hello").is_ok());
    }

    #[test]
    fn verify_blob_digest_rejects_mismatch() {
        let digest = "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824";
        let error = verify_blob_digest(digest, b"world").expect_err("must reject mismatched hash");
        assert!(matches!(error, ImageError::DigestMismatch { .. }));
    }

    #[test]
    fn verify_blob_digest_rejects_unsupported_algorithms() {
        let error = verify_blob_digest("sha512:abc", b"hello")
            .expect_err("sha512 should be rejected for now");
        assert!(matches!(
            error,
            ImageError::UnsupportedDigestAlgorithm { .. }
        ));
    }

    // ── resolve_cmd tests ────────────────────────────────────────

    #[test]
    fn resolve_cmd_prefers_user_command() {
        let config = ImageConfigSummary {
            entrypoint: Some(vec!["/default".to_string()]),
            cmd: Some(vec!["arg".to_string()]),
            ..Default::default()
        };
        let result = config.resolve_cmd(&["custom".to_string(), "cmd".to_string()]);
        assert_eq!(result, Some(vec!["custom".to_string(), "cmd".to_string()]));
    }

    #[test]
    fn resolve_cmd_falls_back_to_image_entrypoint_and_cmd() {
        let config = ImageConfigSummary {
            entrypoint: Some(vec!["/entrypoint".to_string()]),
            cmd: Some(vec!["arg".to_string()]),
            ..Default::default()
        };
        let result = config.resolve_cmd(&[]);
        assert_eq!(
            result,
            Some(vec!["/entrypoint".to_string(), "arg".to_string()])
        );
    }

    #[test]
    fn resolve_cmd_returns_none_when_no_command_available() {
        let config = ImageConfigSummary::default();
        assert_eq!(config.resolve_cmd(&[]), None);
    }

    // ── resolve_env tests ────────────────────────────────────────

    #[test]
    fn resolve_env_merges_with_user_precedence() {
        let config = ImageConfigSummary {
            env: Some(vec!["BASE=1".to_string(), "OVERRIDE=old".to_string()]),
            ..Default::default()
        };
        let user_env = vec![
            ("OVERRIDE".to_string(), "new".to_string()),
            ("EXTRA".to_string(), "val".to_string()),
        ];
        let result = config.resolve_env(&user_env, "ctr-123");
        assert_eq!(
            result,
            vec![
                ("BASE".to_string(), "1".to_string()),
                ("OVERRIDE".to_string(), "new".to_string()),
                ("EXTRA".to_string(), "val".to_string()),
                ("VZ_CONTAINER_ID".to_string(), "ctr-123".to_string()),
            ]
        );
    }

    #[test]
    fn resolve_env_injects_container_id() {
        let config = ImageConfigSummary::default();
        let result = config.resolve_env(&[], "ctr-abc");
        assert_eq!(
            result,
            vec![("VZ_CONTAINER_ID".to_string(), "ctr-abc".to_string())]
        );
    }

    // ── resolve_working_dir tests ────────────────────────────────

    #[test]
    fn resolve_working_dir_prefers_user() {
        let config = ImageConfigSummary {
            working_dir: Some("/image".to_string()),
            ..Default::default()
        };
        assert_eq!(
            config.resolve_working_dir(Some("/user")),
            Some("/user".to_string())
        );
    }

    #[test]
    fn resolve_working_dir_falls_back_to_image() {
        let config = ImageConfigSummary {
            working_dir: Some("/image".to_string()),
            ..Default::default()
        };
        assert_eq!(config.resolve_working_dir(None), Some("/image".to_string()));
    }

    // ── resolve_user tests ───────────────────────────────────────

    #[test]
    fn resolve_user_prefers_user() {
        let config = ImageConfigSummary {
            user: Some("1000".to_string()),
            ..Default::default()
        };
        assert_eq!(config.resolve_user(Some("root")), Some("root".to_string()));
    }

    #[test]
    fn resolve_user_falls_back_to_image() {
        let config = ImageConfigSummary {
            user: Some("1000".to_string()),
            ..Default::default()
        };
        assert_eq!(config.resolve_user(None), Some("1000".to_string()));
    }
}
