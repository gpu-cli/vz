use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64_STANDARD};
use ring::signature;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::space_cache_key::SpaceCacheKey;

pub const SPACE_REMOTE_CACHE_MANIFEST_SCHEMA_VERSION: u16 = 1;
const REMOTE_CACHE_DIR_ENV: &str = "VZ_SPACE_REMOTE_CACHE_DIR";
const REMOTE_CACHE_PUBKEY_ENV: &str = "VZ_SPACE_REMOTE_CACHE_PUBKEY";
const REMOTE_MANIFEST_FILE: &str = "manifest.json";
const REMOTE_SIGNATURE_FILE: &str = "signature.sig";
const REMOTE_BLOB_FILE: &str = "payload.tar.zst";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SpaceRemoteCacheManifestV1 {
    pub schema_version: u16,
    pub cache_name: String,
    pub key_digest_hex: String,
    pub blob_digest_sha256: String,
    pub publisher: String,
    pub signed_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRemoteCacheVerifiedArtifact {
    pub manifest: SpaceRemoteCacheManifestV1,
    pub manifest_path: PathBuf,
    pub signature_path: PathBuf,
    pub blob_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpaceRemoteCacheMissReason {
    NotFound,
    MissingSignature,
    MissingBlob,
    InvalidManifest,
    UnsupportedSchema {
        expected: u16,
        actual: u16,
    },
    IdentityMismatch {
        expected_cache_name: String,
        actual_cache_name: String,
        expected_key_digest: String,
        actual_key_digest: String,
    },
    InvalidSignatureFormat,
    InvalidSignature,
    DigestMismatch {
        expected: String,
        actual: String,
    },
    IoError(String),
}

impl SpaceRemoteCacheMissReason {
    pub fn diagnostic(&self) -> String {
        match self {
            Self::NotFound => "not found".to_string(),
            Self::MissingSignature => "missing signature".to_string(),
            Self::MissingBlob => "missing blob".to_string(),
            Self::InvalidManifest => "invalid manifest".to_string(),
            Self::UnsupportedSchema { expected, actual } => {
                format!("unsupported schema (expected v{expected}, got v{actual})")
            }
            Self::IdentityMismatch {
                expected_cache_name,
                actual_cache_name,
                expected_key_digest,
                actual_key_digest,
            } => format!(
                "identity mismatch (cache `{actual_cache_name}` vs `{expected_cache_name}`, digest `{actual_key_digest}` vs `{expected_key_digest}`)"
            ),
            Self::InvalidSignatureFormat => "invalid signature format".to_string(),
            Self::InvalidSignature => "signature verification failed".to_string(),
            Self::DigestMismatch { expected, actual } => {
                format!("digest mismatch (expected {expected}, got {actual})")
            }
            Self::IoError(detail) => format!("io error: {detail}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpaceRemoteCacheVerificationOutcome {
    Verified {
        artifact: SpaceRemoteCacheVerifiedArtifact,
    },
    Miss(SpaceRemoteCacheMissReason),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRemoteCacheTrustConfig {
    remote_root: PathBuf,
    public_key: Vec<u8>,
}

impl SpaceRemoteCacheTrustConfig {
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let Some(remote_root) = std::env::var_os(REMOTE_CACHE_DIR_ENV) else {
            return Ok(None);
        };
        let pubkey_value = std::env::var_os(REMOTE_CACHE_PUBKEY_ENV).ok_or_else(|| {
            anyhow::anyhow!(
                "{REMOTE_CACHE_DIR_ENV} is set but {REMOTE_CACHE_PUBKEY_ENV} is missing"
            )
        })?;

        let remote_root = PathBuf::from(remote_root);
        let public_key = read_public_key_value(&pubkey_value)?;
        Ok(Some(Self {
            remote_root,
            public_key,
        }))
    }

    fn artifact_dir(&self, key: &SpaceCacheKey) -> PathBuf {
        self.remote_root.join(&key.cache_name).join(&key.digest_hex)
    }

    pub fn verify_key(&self, key: &SpaceCacheKey) -> SpaceRemoteCacheVerificationOutcome {
        let artifact_dir = self.artifact_dir(key);
        verify_remote_cache_artifact(
            artifact_dir.join(REMOTE_MANIFEST_FILE).as_path(),
            artifact_dir.join(REMOTE_SIGNATURE_FILE).as_path(),
            artifact_dir.join(REMOTE_BLOB_FILE).as_path(),
            key.cache_name.as_str(),
            key.digest_hex.as_str(),
            self.public_key.as_slice(),
        )
    }
}

fn read_public_key_value(raw: &std::ffi::OsStr) -> anyhow::Result<Vec<u8>> {
    let value = raw.to_string_lossy();
    let candidate = PathBuf::from(value.as_ref());
    if candidate.is_file() {
        let bytes = std::fs::read(&candidate)
            .with_context(|| format!("failed to read public key {}", candidate.display()))?;
        return parse_ed25519_public_key(bytes.as_slice());
    }
    parse_ed25519_public_key(value.as_bytes())
}

fn parse_ed25519_public_key(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    if bytes.len() == 32 {
        return Ok(bytes.to_vec());
    }
    let trimmed = String::from_utf8_lossy(bytes).trim().as_bytes().to_vec();

    if let Ok(decoded) = BASE64_STANDARD.decode(&trimmed)
        && decoded.len() == 32
    {
        return Ok(decoded);
    }

    if trimmed.len() == 64 {
        let mut out = Vec::with_capacity(32);
        for chunk in trimmed.chunks(2) {
            let pair = std::str::from_utf8(chunk).context("invalid hex public key encoding")?;
            let byte = u8::from_str_radix(pair, 16).context("invalid hex public key encoding")?;
            out.push(byte);
        }
        return Ok(out);
    }

    bail!("public key must be raw 32-byte bytes, base64 text, or hex text");
}

pub fn verify_remote_cache_artifact(
    manifest_path: &Path,
    signature_path: &Path,
    blob_path: &Path,
    expected_cache_name: &str,
    expected_key_digest: &str,
    public_key: &[u8],
) -> SpaceRemoteCacheVerificationOutcome {
    let manifest_bytes = match std::fs::read(manifest_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return SpaceRemoteCacheVerificationOutcome::Miss(SpaceRemoteCacheMissReason::NotFound);
        }
        Err(error) => {
            return SpaceRemoteCacheVerificationOutcome::Miss(SpaceRemoteCacheMissReason::IoError(
                format!("failed to read {}: {error}", manifest_path.display()),
            ));
        }
    };
    let manifest = match serde_json::from_slice::<SpaceRemoteCacheManifestV1>(&manifest_bytes) {
        Ok(parsed) => parsed,
        Err(_) => {
            return SpaceRemoteCacheVerificationOutcome::Miss(
                SpaceRemoteCacheMissReason::InvalidManifest,
            );
        }
    };
    if manifest.schema_version != SPACE_REMOTE_CACHE_MANIFEST_SCHEMA_VERSION {
        return SpaceRemoteCacheVerificationOutcome::Miss(
            SpaceRemoteCacheMissReason::UnsupportedSchema {
                expected: SPACE_REMOTE_CACHE_MANIFEST_SCHEMA_VERSION,
                actual: manifest.schema_version,
            },
        );
    }
    if manifest.cache_name != expected_cache_name || manifest.key_digest_hex != expected_key_digest
    {
        return SpaceRemoteCacheVerificationOutcome::Miss(
            SpaceRemoteCacheMissReason::IdentityMismatch {
                expected_cache_name: expected_cache_name.to_string(),
                actual_cache_name: manifest.cache_name.clone(),
                expected_key_digest: expected_key_digest.to_string(),
                actual_key_digest: manifest.key_digest_hex.clone(),
            },
        );
    }

    let signature_bytes = match std::fs::read(signature_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return SpaceRemoteCacheVerificationOutcome::Miss(
                SpaceRemoteCacheMissReason::MissingSignature,
            );
        }
        Err(error) => {
            return SpaceRemoteCacheVerificationOutcome::Miss(SpaceRemoteCacheMissReason::IoError(
                format!("failed to read {}: {error}", signature_path.display()),
            ));
        }
    };
    let detached_signature = match parse_detached_signature(signature_bytes.as_slice()) {
        Ok(sig) => sig,
        Err(_) => {
            return SpaceRemoteCacheVerificationOutcome::Miss(
                SpaceRemoteCacheMissReason::InvalidSignatureFormat,
            );
        }
    };
    let verifier = signature::UnparsedPublicKey::new(&signature::ED25519, public_key);
    if verifier
        .verify(manifest_bytes.as_slice(), detached_signature.as_slice())
        .is_err()
    {
        return SpaceRemoteCacheVerificationOutcome::Miss(
            SpaceRemoteCacheMissReason::InvalidSignature,
        );
    }

    let blob_bytes = match std::fs::read(blob_path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return SpaceRemoteCacheVerificationOutcome::Miss(
                SpaceRemoteCacheMissReason::MissingBlob,
            );
        }
        Err(error) => {
            return SpaceRemoteCacheVerificationOutcome::Miss(SpaceRemoteCacheMissReason::IoError(
                format!("failed to read {}: {error}", blob_path.display()),
            ));
        }
    };
    let actual_blob_digest = sha256_hex(blob_bytes.as_slice());
    if actual_blob_digest != manifest.blob_digest_sha256 {
        return SpaceRemoteCacheVerificationOutcome::Miss(
            SpaceRemoteCacheMissReason::DigestMismatch {
                expected: manifest.blob_digest_sha256.clone(),
                actual: actual_blob_digest,
            },
        );
    }

    SpaceRemoteCacheVerificationOutcome::Verified {
        artifact: SpaceRemoteCacheVerifiedArtifact {
            manifest,
            manifest_path: manifest_path.to_path_buf(),
            signature_path: signature_path.to_path_buf(),
            blob_path: blob_path.to_path_buf(),
        },
    }
}

fn parse_detached_signature(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    if bytes.len() == 64 {
        return Ok(bytes.to_vec());
    }
    let decoded = BASE64_STANDARD
        .decode(String::from_utf8_lossy(bytes).trim().as_bytes())
        .context("invalid base64 signature")?;
    if decoded.len() != 64 {
        bail!(
            "detached signature must be a 64-byte Ed25519 signature (got {} bytes)",
            decoded.len()
        );
    }
    Ok(decoded)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tempfile::tempdir;

    struct SignedFixture {
        _tempdir: tempfile::TempDir,
        manifest_path: PathBuf,
        signature_path: PathBuf,
        blob_path: PathBuf,
        public_key: Vec<u8>,
        cache_name: String,
        digest_hex: String,
    }

    fn write_signed_fixture() -> SignedFixture {
        let dir = tempdir().expect("tempdir");
        let artifact = dir.path().join("deps").join("abc123");
        std::fs::create_dir_all(&artifact).expect("artifact dir");

        let blob_bytes = b"verified-space-cache-payload";
        let blob_digest = sha256_hex(blob_bytes);
        let manifest = SpaceRemoteCacheManifestV1 {
            schema_version: SPACE_REMOTE_CACHE_MANIFEST_SCHEMA_VERSION,
            cache_name: "deps".to_string(),
            key_digest_hex: "abc123".to_string(),
            blob_digest_sha256: blob_digest,
            publisher: "acme-ci".to_string(),
            signed_at: 1_746_000_000,
        };
        let manifest_bytes = serde_json::to_vec(&manifest).expect("manifest json");

        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("pkcs8");
        let key_pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("keypair");
        let signature = key_pair.sign(manifest_bytes.as_slice());

        let manifest_path = artifact.join(REMOTE_MANIFEST_FILE);
        let signature_path = artifact.join(REMOTE_SIGNATURE_FILE);
        let blob_path = artifact.join(REMOTE_BLOB_FILE);
        std::fs::write(&manifest_path, &manifest_bytes).expect("manifest");
        std::fs::write(&signature_path, signature.as_ref()).expect("signature");
        std::fs::write(&blob_path, blob_bytes).expect("blob");

        let public_key = key_pair.public_key().as_ref().to_vec();
        SignedFixture {
            _tempdir: dir,
            manifest_path,
            signature_path,
            blob_path,
            public_key,
            cache_name: manifest.cache_name,
            digest_hex: manifest.key_digest_hex,
        }
    }

    #[test]
    fn verify_remote_cache_artifact_accepts_valid_signed_manifest_and_blob() {
        let fixture = write_signed_fixture();
        let result = verify_remote_cache_artifact(
            fixture.manifest_path.as_path(),
            fixture.signature_path.as_path(),
            fixture.blob_path.as_path(),
            fixture.cache_name.as_str(),
            fixture.digest_hex.as_str(),
            fixture.public_key.as_slice(),
        );
        assert!(matches!(
            result,
            SpaceRemoteCacheVerificationOutcome::Verified { .. }
        ));
    }

    #[test]
    fn verify_remote_cache_artifact_fails_closed_when_signature_missing() {
        let fixture = write_signed_fixture();
        std::fs::remove_file(&fixture.signature_path).expect("remove signature");

        let result = verify_remote_cache_artifact(
            fixture.manifest_path.as_path(),
            fixture.signature_path.as_path(),
            fixture.blob_path.as_path(),
            fixture.cache_name.as_str(),
            fixture.digest_hex.as_str(),
            fixture.public_key.as_slice(),
        );
        assert_eq!(
            result,
            SpaceRemoteCacheVerificationOutcome::Miss(SpaceRemoteCacheMissReason::MissingSignature)
        );
    }

    #[test]
    fn verify_remote_cache_artifact_fails_closed_when_manifest_tampered() {
        let fixture = write_signed_fixture();
        let tampered = SpaceRemoteCacheManifestV1 {
            schema_version: SPACE_REMOTE_CACHE_MANIFEST_SCHEMA_VERSION,
            cache_name: fixture.cache_name.clone(),
            key_digest_hex: fixture.digest_hex.clone(),
            blob_digest_sha256: "0".repeat(64),
            publisher: "attacker".to_string(),
            signed_at: 1_746_000_123,
        };
        std::fs::write(
            &fixture.manifest_path,
            serde_json::to_vec(&tampered).expect("tampered json"),
        )
        .expect("write tampered manifest");

        let result = verify_remote_cache_artifact(
            fixture.manifest_path.as_path(),
            fixture.signature_path.as_path(),
            fixture.blob_path.as_path(),
            fixture.cache_name.as_str(),
            fixture.digest_hex.as_str(),
            fixture.public_key.as_slice(),
        );
        assert_eq!(
            result,
            SpaceRemoteCacheVerificationOutcome::Miss(SpaceRemoteCacheMissReason::InvalidSignature)
        );
    }

    #[test]
    fn verify_remote_cache_artifact_fails_closed_when_blob_tampered() {
        let fixture = write_signed_fixture();
        std::fs::write(&fixture.blob_path, b"tampered").expect("tamper blob");

        let result = verify_remote_cache_artifact(
            fixture.manifest_path.as_path(),
            fixture.signature_path.as_path(),
            fixture.blob_path.as_path(),
            fixture.cache_name.as_str(),
            fixture.digest_hex.as_str(),
            fixture.public_key.as_slice(),
        );
        assert!(matches!(
            result,
            SpaceRemoteCacheVerificationOutcome::Miss(
                SpaceRemoteCacheMissReason::DigestMismatch { .. }
            )
        ));
    }
}
