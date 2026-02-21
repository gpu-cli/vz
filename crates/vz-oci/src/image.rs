use std::str::FromStr;

use docker_credential::{DockerCredential, get_credential};
use oci_distribution::client::ClientConfig;
use oci_distribution::errors::OciDistributionError;
use oci_distribution::manifest::{ImageIndexEntry, OciDescriptor};
use oci_distribution::secrets::RegistryAuth;
use oci_distribution::{Client, Reference};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio_stream::StreamExt;

use crate::config::Auth;
use crate::error::OciError;
use crate::store::ImageStore;

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
    /// Build an image puller configured to resolve `linux/arm64` image variants.
    pub fn new(store: ImageStore) -> Self {
        let config = ClientConfig {
            platform_resolver: Some(Box::new(select_linux_arm64_digest)),
            ..ClientConfig::default()
        };
        let client = Client::new(config);
        Self { client, store }
    }

    /// Pull `reference` into local storage and return its manifest digest id.
    pub async fn pull(&self, reference: &str, auth: &Auth) -> Result<ImageId, OciError> {
        self.store.ensure_layout()?;

        let parsed =
            Reference::from_str(reference).map_err(|error| OciError::ImageReferenceParse {
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
            OciError::InvalidConfig(format!(
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
    ) -> Result<Vec<u8>, OciError> {
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

fn map_pull_error(reference: &str, error: OciDistributionError) -> OciError {
    let message = error.to_string();
    if message.to_ascii_lowercase().contains("platform") {
        return OciError::PlatformMismatch {
            reference: reference.to_string(),
            details: message,
        };
    }

    OciError::Pull {
        reference: reference.to_string(),
        source: error,
    }
}

fn select_linux_arm64_digest(entries: &[ImageIndexEntry]) -> Option<String> {
    entries.iter().find_map(|entry| {
        let platform = entry.platform.as_ref()?;
        if platform.os == "linux" && platform.architecture == "arm64" {
            return Some(entry.digest.clone());
        }

        None
    })
}

fn resolve_registry_auth(reference: &Reference, auth: &Auth) -> Result<RegistryAuth, OciError> {
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
                DockerCredential::IdentityToken(_) => Err(OciError::AuthenticationUnsupported(
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

fn parse_image_config_summary(config_json: &str) -> Result<ImageConfigSummary, OciError> {
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
) -> Result<ImageConfigSummary, OciError> {
    let config_json = store.read_config_json(image_id)?;
    parse_image_config_summary(std::str::from_utf8(&config_json).map_err(|error| {
        OciError::InvalidConfig(format!("stored image config is not valid utf-8: {error}"))
    })?)
}

fn verify_blob_digest(descriptor_digest: &str, bytes: &[u8]) -> Result<(), OciError> {
    let (algorithm, expected) = descriptor_digest.split_once(':').ok_or_else(|| {
        OciError::InvalidConfig(format!("invalid digest format: {descriptor_digest}"))
    })?;

    if algorithm != "sha256" {
        return Err(OciError::UnsupportedDigestAlgorithm {
            algorithm: algorithm.to_string(),
        });
    }

    let expected = expected.to_ascii_lowercase();
    let actual = format!("{:x}", Sha256::digest(bytes));
    if expected != actual {
        return Err(OciError::DigestMismatch {
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
        assert!(matches!(error, OciError::DigestMismatch { .. }));
    }

    #[test]
    fn verify_blob_digest_rejects_unsupported_algorithms() {
        let error = verify_blob_digest("sha512:abc", b"hello")
            .expect_err("sha512 should be rejected for now");
        assert!(matches!(error, OciError::UnsupportedDigestAlgorithm { .. }));
    }
}
