//! `vz vm mac patch` -- Signed patch bundles plus binary image deltas.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt, lchown, symlink};
use std::path::{Component, Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use base64::Engine as _;
use clap::{Args, Subcommand};
use ring::signature;
use ring::signature::KeyPair;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ipsw;

mod apply;
mod planning;
mod rollback;
#[cfg(test)]
mod tests;

use self::apply::{apply, apply_delta, create_delta};
use self::planning::create;
use self::rollback::{guest_path_to_relative, write_file_atomic};

const MANIFEST_FILE: &str = "manifest.json";
const PAYLOAD_FILE: &str = "payload.tar.zst";
const SIGNATURE_FILE: &str = "signature.sig";

const SIGNING_IDENTITY_PREFIX: &str = "ed25519:";
const ED25519_PUBLIC_KEY_LEN: usize = 32;
const ED25519_SIGNATURE_LEN: usize = 64;
const DEFAULT_FILE_MODE: u32 = 0o644;
const PATCH_STATE_FILE: &str = "patch-state.json";
const PATCH_STATE_VERSION: u32 = 1;
const PATCH_STATE_FILE_MODE: u32 = 0o600;
const IMAGE_DELTA_MAGIC: &[u8; 8] = b"VZDELTA1";
const IMAGE_DELTA_VERSION: u32 = 1;
const DEFAULT_IMAGE_DELTA_CHUNK_SIZE_MIB: u32 = 4;
const MIN_IMAGE_DELTA_CHUNK_SIZE_MIB: u32 = 1;
const MAX_IMAGE_DELTA_CHUNK_SIZE_MIB: u32 = 64;
const IMAGE_SIDECAR_EXTENSIONS: [&str; 3] = ["aux", "hwmodel", "machineid"];

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Manage signed patch bundles.
#[derive(Args, Debug)]
pub struct VmPatchArgs {
    #[command(subcommand)]
    pub action: VmPatchCommand,
}

/// `vz vm mac patch` subcommands.
#[derive(Subcommand, Debug)]
pub enum VmPatchCommand {
    /// Create a signed patch bundle from operations + payload inputs.
    Create(CreateArgs),
    /// Verify bundle signature and digests before apply.
    Verify(VerifyArgs),
    /// Verify, then transactionally apply patch operations.
    Apply(ApplyArgs),
    /// Create a binary image delta by applying a bundle to a temporary image copy.
    CreateDelta(CreateDeltaArgs),
    /// Apply a binary image delta to a base image without sudo/mounting.
    ApplyDelta(ApplyDeltaArgs),
}

/// Arguments for `vz vm mac patch create`.
#[derive(Args, Debug)]
pub struct CreateArgs {
    /// Output bundle directory to write (for example: `/tmp/patch-1.vzpatch`).
    #[arg(long)]
    pub bundle: PathBuf,

    /// Pinned base selector (`base_id`, `stable`, or `previous`).
    #[arg(long, value_name = "SELECTOR")]
    pub base_id: String,

    /// Advanced mode: JSON file containing an ordered array of patch operations.
    #[arg(long)]
    pub operations: Option<PathBuf>,

    /// Advanced mode: directory containing payload files named by SHA-256 digest.
    #[arg(long)]
    pub payload_dir: Option<PathBuf>,

    /// Inline mode: add `write_file` operation from host file path to guest path.
    /// Format: `HOST_PATH:GUEST_PATH[:MODE]` (mode accepts octal like `755` or decimal).
    #[arg(long = "write-file", value_name = "HOST:GUEST[:MODE]")]
    pub write_file: Vec<String>,

    /// Inline mode: add `mkdir` operation.
    /// Format: `GUEST_PATH[:MODE]` (mode accepts octal like `755` or decimal).
    #[arg(long = "mkdir", value_name = "GUEST[:MODE]")]
    pub mkdir: Vec<String>,

    /// Inline mode: add `symlink` operation.
    /// Format: `GUEST_PATH:TARGET`.
    #[arg(long = "symlink", value_name = "GUEST:TARGET")]
    pub symlink: Vec<String>,

    /// Inline mode: add `delete_file` operation.
    /// Format: `GUEST_PATH`.
    #[arg(long = "delete-file", value_name = "GUEST")]
    pub delete_file: Vec<String>,

    /// Inline mode: add `set_mode` operation.
    /// Format: `GUEST_PATH:MODE` (mode accepts octal like `755` or decimal).
    #[arg(long = "set-mode", value_name = "GUEST:MODE")]
    pub set_mode: Vec<String>,

    /// Inline mode: add `set_owner` operation.
    /// Format: `GUEST_PATH:UID:GID`.
    #[arg(long = "set-owner", value_name = "GUEST:UID:GID")]
    pub set_owner: Vec<String>,

    /// Ed25519 private key path (PKCS#8 DER or PEM).
    #[arg(long)]
    pub signing_key: PathBuf,

    /// Optional JSON object file for post-state hashes (`path -> sha256`).
    /// When omitted, hashes are derived from `write_file` and `symlink` operations.
    #[arg(long)]
    pub post_state_hashes: Option<PathBuf>,

    /// Patch version label.
    #[arg(long, default_value = "1.0.0")]
    pub patch_version: String,

    /// Optional explicit bundle identifier.
    #[arg(long)]
    pub bundle_id: Option<String>,

    /// Optional creation timestamp metadata.
    #[arg(long)]
    pub created_at: Option<String>,
}

/// Arguments for `vz vm mac patch verify`.
#[derive(Args, Debug)]
pub struct VerifyArgs {
    /// Bundle directory containing manifest, payload, and detached signature.
    #[arg(long)]
    pub bundle: PathBuf,
}

/// Arguments for `vz vm mac patch apply`.
#[derive(Args, Debug)]
pub struct ApplyArgs {
    /// Bundle directory containing manifest, payload, and detached signature.
    #[arg(long)]
    pub bundle: PathBuf,

    /// Mounted root path to apply operations under.
    #[arg(long, conflicts_with = "image", required_unless_present = "image")]
    pub root: Option<PathBuf>,

    /// Raw VM disk image path to mount/apply/detach automatically.
    #[arg(long, conflicts_with = "root", required_unless_present = "root")]
    pub image: Option<PathBuf>,
}

/// Arguments for `vz vm mac patch create-delta`.
#[derive(Args, Debug)]
pub struct CreateDeltaArgs {
    /// Bundle directory to apply when producing the patched image snapshot.
    #[arg(long)]
    pub bundle: PathBuf,

    /// Base raw VM image path (for example: `~/.vz/images/base.img`).
    #[arg(long)]
    pub base_image: PathBuf,

    /// Output binary delta file path (for example: `/tmp/patch-1.vzdelta`).
    #[arg(long)]
    pub delta: PathBuf,

    /// Chunk size in MiB used for diffing.
    #[arg(
        long,
        default_value_t = DEFAULT_IMAGE_DELTA_CHUNK_SIZE_MIB,
        value_parser = clap::value_parser!(u32).range(MIN_IMAGE_DELTA_CHUNK_SIZE_MIB as i64..=(MAX_IMAGE_DELTA_CHUNK_SIZE_MIB as i64))
    )]
    pub chunk_size_mib: u32,
}

/// Arguments for `vz vm mac patch apply-delta`.
#[derive(Args, Debug)]
pub struct ApplyDeltaArgs {
    /// Base raw VM image path used as delta source.
    #[arg(long)]
    pub base_image: PathBuf,

    /// Binary delta file produced by `vz vm mac patch create-delta`.
    #[arg(long)]
    pub delta: PathBuf,

    /// Output raw VM image path to write patched result to.
    #[arg(long)]
    pub output_image: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ImageDeltaHeader {
    chunk_size: u32,
    base_size: u64,
    target_size: u64,
    base_sha256: [u8; 32],
    target_sha256: [u8; 32],
    changed_chunks: u64,
}

/// Typed bundle manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PatchBundleManifest {
    pub bundle_id: String,
    pub patch_version: String,
    pub target_base_id: String,
    pub target_base_fingerprint: BundleBaseFingerprint,
    pub operations_digest: String,
    pub payload_digest: String,
    pub post_state_hashes: BTreeMap<String, String>,
    pub created_at: String,
    pub signing_identity: String,
    pub operations: Vec<PatchOperation>,
}

/// Target base identity that the patch is pinned to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BundleBaseFingerprint {
    pub img_sha256: String,
    pub aux_sha256: String,
    pub hwmodel_sha256: String,
    pub machineid_sha256: String,
}

/// Deterministic ordered patch operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PatchOperation {
    Mkdir {
        path: String,
        mode: Option<u32>,
    },
    WriteFile {
        path: String,
        content_digest: String,
        mode: Option<u32>,
    },
    DeleteFile {
        path: String,
    },
    Symlink {
        path: String,
        target: String,
    },
    SetOwner {
        path: String,
        uid: u32,
        gid: u32,
    },
    SetMode {
        path: String,
        mode: u32,
    },
}

#[derive(Debug, Clone)]
enum PathSnapshot {
    Missing,
    File {
        contents: Vec<u8>,
        metadata: PosixMetadata,
    },
    Directory {
        metadata: PosixMetadata,
    },
    Symlink {
        target: PathBuf,
        owner: PosixOwner,
    },
}

#[derive(Debug, Clone, Copy)]
struct PosixMetadata {
    mode: u32,
    owner: PosixOwner,
}

#[derive(Debug, Clone, Copy)]
struct PosixOwner {
    uid: u32,
    gid: u32,
}

#[derive(Debug, Clone)]
struct RollbackEntry {
    operation_index: usize,
    path: String,
    host_path: PathBuf,
    snapshot: PathSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PatchApplyState {
    version: u32,
    #[serde(default)]
    receipts: BTreeMap<String, PatchApplyReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct PatchApplyReceipt {
    apply_target: String,
    apply_target_digest: String,
    bundle_id: String,
    target_base_id: String,
    target_base_fingerprint: BundleBaseFingerprint,
    operations_digest: String,
    payload_digest: String,
}

impl Default for PatchApplyState {
    fn default() -> Self {
        Self {
            version: PATCH_STATE_VERSION,
            receipts: BTreeMap::new(),
        }
    }
}

impl PatchBundleManifest {
    /// Parse and validate manifest JSON content.
    pub fn from_json_bytes(contents: &[u8]) -> anyhow::Result<Self> {
        let manifest: Self = serde_json::from_slice(contents)
            .context("failed to parse patch bundle manifest JSON")?;
        manifest.validate()?;
        Ok(manifest)
    }

    fn validate(&self) -> anyhow::Result<()> {
        validate_non_empty("manifest.bundle_id", &self.bundle_id)?;
        validate_non_empty("manifest.patch_version", &self.patch_version)?;
        validate_non_empty("manifest.target_base_id", &self.target_base_id)?;
        validate_non_empty("manifest.created_at", &self.created_at)?;
        validate_non_empty("manifest.signing_identity", &self.signing_identity)?;
        self.target_base_fingerprint.validate()?;

        let _ = normalize_sha256_field("manifest.operations_digest", &self.operations_digest)?;
        let _ = normalize_sha256_field("manifest.payload_digest", &self.payload_digest)?;

        for (path, digest) in &self.post_state_hashes {
            validate_non_empty("manifest.post_state_hashes path", path)?;
            let field = format!("manifest.post_state_hashes['{path}']");
            let _ = normalize_sha256_field(&field, digest)?;
        }

        if self.operations.is_empty() {
            bail!("manifest.operations must contain at least one operation");
        }
        for (index, operation) in self.operations.iter().enumerate() {
            operation.validate(index)?;
        }

        Ok(())
    }
}

impl BundleBaseFingerprint {
    fn validate(&self) -> anyhow::Result<()> {
        let fields = [
            (
                "manifest.target_base_fingerprint.img_sha256",
                &self.img_sha256,
            ),
            (
                "manifest.target_base_fingerprint.aux_sha256",
                &self.aux_sha256,
            ),
            (
                "manifest.target_base_fingerprint.hwmodel_sha256",
                &self.hwmodel_sha256,
            ),
            (
                "manifest.target_base_fingerprint.machineid_sha256",
                &self.machineid_sha256,
            ),
        ];
        for (field, value) in fields {
            validate_non_empty(field, value)?;
            let _ = normalize_sha256_field(field, value)?;
        }
        Ok(())
    }

    fn identity_details(&self) -> String {
        format!(
            "img_sha256={}, aux_sha256={}, hwmodel_sha256={}, machineid_sha256={}",
            self.img_sha256, self.aux_sha256, self.hwmodel_sha256, self.machineid_sha256
        )
    }
}

impl PatchApplyState {
    fn load(path: &Path) -> anyhow::Result<Self> {
        let bytes = match fs::read(path) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(Self::default()),
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to read patch state file {}", path.display())
                });
            }
        };

        let state: Self = serde_json::from_slice(&bytes).map_err(|err| {
            anyhow!(
                "patch state file {} is malformed: {}. Move or delete the file and retry.",
                path.display(),
                err
            )
        })?;

        if state.version != PATCH_STATE_VERSION {
            bail!(
                "patch state file {} uses unsupported version {} (expected {})",
                path.display(),
                state.version,
                PATCH_STATE_VERSION
            );
        }

        Ok(state)
    }

    fn save(&self, path: &Path) -> anyhow::Result<()> {
        let parent = path.parent().ok_or_else(|| {
            anyhow!(
                "patch state file path '{}' has no parent directory",
                path.display()
            )
        })?;
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create patch state directory {}",
                parent.display()
            )
        })?;

        let mut encoded =
            serde_json::to_vec_pretty(self).context("failed to serialize patch apply state")?;
        encoded.push(b'\n');
        write_file_atomic(parent, path, &encoded, PATCH_STATE_FILE_MODE)
            .with_context(|| format!("failed to write patch state file {}", path.display()))
    }

    fn has_receipt(&self, candidate: &PatchApplyReceipt) -> bool {
        self.receipts.contains_key(&candidate.key())
    }

    fn find_conflicting_receipt(
        &self,
        candidate: &PatchApplyReceipt,
    ) -> Option<&PatchApplyReceipt> {
        let key = candidate.key();
        self.receipts.iter().find_map(|(existing_key, receipt)| {
            if existing_key == &key {
                return None;
            }
            if receipt.same_apply_target_and_bundle(candidate) {
                Some(receipt)
            } else {
                None
            }
        })
    }

    fn record_receipt(&mut self, receipt: PatchApplyReceipt) {
        self.receipts.insert(receipt.key(), receipt);
    }
}

impl PatchApplyReceipt {
    fn from_manifest(root: &Path, manifest: &PatchBundleManifest) -> anyhow::Result<Self> {
        Ok(Self {
            apply_target: root.display().to_string(),
            apply_target_digest: sha256_bytes_hex(root.as_os_str().as_bytes()),
            bundle_id: manifest.bundle_id.clone(),
            target_base_id: manifest.target_base_id.clone(),
            target_base_fingerprint: manifest.target_base_fingerprint.clone(),
            operations_digest: normalize_sha256_field(
                "manifest.operations_digest",
                &manifest.operations_digest,
            )?,
            payload_digest: normalize_sha256_field(
                "manifest.payload_digest",
                &manifest.payload_digest,
            )?,
        })
    }

    fn same_apply_target_and_bundle(&self, other: &Self) -> bool {
        self.apply_target_digest == other.apply_target_digest && self.bundle_id == other.bundle_id
    }

    fn key(&self) -> String {
        let mut material = String::new();
        material.push_str(&self.apply_target_digest);
        material.push('\n');
        material.push_str(&self.bundle_id);
        material.push('\n');
        material.push_str(&self.target_base_id);
        material.push('\n');
        material.push_str(&self.target_base_fingerprint.img_sha256);
        material.push('\n');
        material.push_str(&self.target_base_fingerprint.aux_sha256);
        material.push('\n');
        material.push_str(&self.target_base_fingerprint.hwmodel_sha256);
        material.push('\n');
        material.push_str(&self.target_base_fingerprint.machineid_sha256);
        material.push('\n');
        material.push_str(&self.operations_digest);
        material.push('\n');
        material.push_str(&self.payload_digest);
        sha256_bytes_hex(material.as_bytes())
    }

    fn identity_details(&self) -> String {
        format!(
            "apply_target='{}', bundle_id='{}', target_base_id='{}', target_base_fingerprint=[{}], operations_digest={}, payload_digest={}",
            self.apply_target,
            self.bundle_id,
            self.target_base_id,
            self.target_base_fingerprint.identity_details(),
            self.operations_digest,
            self.payload_digest
        )
    }
}

impl PatchOperation {
    fn validate(&self, index: usize) -> anyhow::Result<()> {
        match self {
            Self::Mkdir { path, mode } => {
                validate_operation_path(&operation_field(index, "path"), path)?;
                if let Some(mode) = mode {
                    validate_mode(&operation_field(index, "mode"), *mode)?;
                }
            }
            Self::WriteFile {
                path,
                content_digest,
                mode,
            } => {
                validate_operation_path(&operation_field(index, "path"), path)?;
                let _ = normalize_sha256_field(
                    &operation_field(index, "content_digest"),
                    content_digest,
                )?;
                if let Some(mode) = mode {
                    validate_mode(&operation_field(index, "mode"), *mode)?;
                }
            }
            Self::DeleteFile { path } => {
                validate_operation_path(&operation_field(index, "path"), path)?;
            }
            Self::Symlink { path, target } => {
                validate_operation_path(&operation_field(index, "path"), path)?;
                validate_non_empty(&operation_field(index, "target"), target)?;
            }
            Self::SetOwner { path, .. } => {
                validate_operation_path(&operation_field(index, "path"), path)?;
            }
            Self::SetMode { path, mode } => {
                validate_operation_path(&operation_field(index, "path"), path)?;
                validate_mode(&operation_field(index, "mode"), *mode)?;
            }
        }
        Ok(())
    }

    fn path(&self) -> &str {
        match self {
            Self::Mkdir { path, .. }
            | Self::WriteFile { path, .. }
            | Self::DeleteFile { path }
            | Self::Symlink { path, .. }
            | Self::SetOwner { path, .. }
            | Self::SetMode { path, .. } => path,
        }
    }
}

fn operation_field(index: usize, field: &str) -> String {
    format!("manifest.operations[{index}].{field}")
}

fn validate_non_empty(field: &str, value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}

fn validate_operation_path(field: &str, value: &str) -> anyhow::Result<()> {
    validate_non_empty(field, value)?;
    if !value.starts_with('/') {
        bail!("{field} must be an absolute path under the mounted root");
    }
    Ok(())
}

fn validate_mode(field: &str, mode: u32) -> anyhow::Result<()> {
    if mode > 0o7777 {
        bail!("{field} must be <= 0o7777");
    }
    Ok(())
}

fn normalize_sha256_field(field: &str, value: &str) -> anyhow::Result<String> {
    ipsw::normalize_sha256(value).with_context(|| format!("invalid {field}"))
}

/// Entry point for `vz vm mac patch`.
pub async fn run(args: VmPatchArgs) -> anyhow::Result<()> {
    match args.action {
        VmPatchCommand::Create(args) => create(args),
        VmPatchCommand::Verify(args) => verify(args),
        VmPatchCommand::Apply(args) => apply(args),
        VmPatchCommand::CreateDelta(args) => create_delta(args),
        VmPatchCommand::ApplyDelta(args) => apply_delta(args),
    }
}

fn verify(args: VerifyArgs) -> anyhow::Result<()> {
    let manifest = verify_bundle(&args.bundle)?;
    validate_patch_target_base_policy(&manifest)?;
    println!(
        "Patch bundle '{}' verified for target base '{}'",
        manifest.bundle_id, manifest.target_base_id
    );
    Ok(())
}

fn ensure_regular_file(path: &Path, arg_name: &str) -> anyhow::Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{} not found: {}", arg_name, path.display()))?;
    if !metadata.file_type().is_file() {
        bail!("{} must be a regular file: {}", arg_name, path.display());
    }
    Ok(())
}

fn ensure_dir(path: &Path, arg_name: &str) -> anyhow::Result<()> {
    let metadata = fs::symlink_metadata(path)
        .with_context(|| format!("{} not found: {}", arg_name, path.display()))?;
    if !metadata.file_type().is_dir() {
        bail!("{} must be a directory: {}", arg_name, path.display());
    }
    Ok(())
}

fn ensure_output_file_parent(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("output path '{}' has no parent directory", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create parent directory {}", parent.display()))
}

fn ensure_output_path_absent(path: &Path, arg_name: &str) -> anyhow::Result<()> {
    if path.exists() {
        bail!("{} already exists: {}", arg_name, path.display());
    }
    Ok(())
}

fn mib_to_bytes(mib: u32) -> anyhow::Result<usize> {
    let bytes = (mib as u64)
        .checked_mul(1024 * 1024)
        .ok_or_else(|| anyhow!("chunk size overflow"))?;
    usize::try_from(bytes).context("chunk size does not fit usize")
}

struct TempWorkspace {
    path: PathBuf,
}

impl TempWorkspace {
    fn new(prefix: &str) -> anyhow::Result<Self> {
        let temp_root = std::env::temp_dir();
        for _ in 0..64 {
            let suffix = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
            let candidate = temp_root.join(format!("{prefix}-{}-{suffix}", now_unix_seconds()));
            match fs::create_dir(&candidate) {
                Ok(()) => return Ok(Self { path: candidate }),
                Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to create {}", candidate.display()));
                }
            }
        }
        bail!(
            "failed to allocate temporary workspace under {}",
            temp_root.display()
        );
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempWorkspace {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn clone_or_copy_file(source: &Path, destination: &Path) -> anyhow::Result<()> {
    if destination == source {
        bail!(
            "destination must be different from source: {}",
            destination.display()
        );
    }
    ensure_regular_file(source, "source image")?;
    let parent = destination.parent().ok_or_else(|| {
        anyhow!(
            "destination path '{}' has no parent directory",
            destination.display()
        )
    })?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    ensure_output_path_absent(destination, "destination image")?;

    let cp_status = Command::new("cp")
        .arg("-c")
        .arg(source)
        .arg(destination)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    if let Ok(status) = cp_status {
        if status.success() {
            return Ok(());
        }
    }

    fs::copy(source, destination).with_context(|| {
        format!(
            "failed to copy {} to {}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(())
}

fn clone_or_copy_image_with_sidecars(
    source_image: &Path,
    destination_image: &Path,
    require_sidecars: bool,
) -> anyhow::Result<()> {
    clone_or_copy_file(source_image, destination_image).with_context(|| {
        format!(
            "failed to copy image {} to {}",
            source_image.display(),
            destination_image.display()
        )
    })?;

    for extension in IMAGE_SIDECAR_EXTENSIONS {
        let source_sidecar = source_image.with_extension(extension);
        let destination_sidecar = destination_image.with_extension(extension);

        match fs::symlink_metadata(&source_sidecar) {
            Ok(metadata) => {
                if !metadata.file_type().is_file() {
                    bail!(
                        "image sidecar {} must be a regular file",
                        source_sidecar.display()
                    );
                }
                clone_or_copy_file(&source_sidecar, &destination_sidecar).with_context(|| {
                    format!(
                        "failed to copy sidecar {} to {}",
                        source_sidecar.display(),
                        destination_sidecar.display()
                    )
                })?;
            }
            Err(err) if err.kind() == ErrorKind::NotFound => {
                if require_sidecars {
                    bail!(
                        "required image sidecar not found: {}",
                        source_sidecar.display()
                    );
                }
            }
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to inspect sidecar {}", source_sidecar.display())
                });
            }
        }
    }

    Ok(())
}

fn create_image_delta_file(
    base_image: &Path,
    target_image: &Path,
    delta_path: &Path,
    chunk_size: usize,
) -> anyhow::Result<ImageDeltaHeader> {
    if chunk_size == 0 {
        bail!("chunk size must be greater than zero");
    }
    ensure_regular_file(base_image, "base image")?;
    ensure_regular_file(target_image, "target image")?;
    ensure_output_file_parent(delta_path)?;
    ensure_output_path_absent(delta_path, "delta output")?;

    let base_size = fs::metadata(base_image)
        .with_context(|| format!("failed to inspect {}", base_image.display()))?
        .len();
    let target_size = fs::metadata(target_image)
        .with_context(|| format!("failed to inspect {}", target_image.display()))?
        .len();

    let mut base_reader = File::open(base_image)
        .with_context(|| format!("failed to open {}", base_image.display()))?;
    let mut target_reader = File::open(target_image)
        .with_context(|| format!("failed to open {}", target_image.display()))?;
    let mut delta_file = OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .open(delta_path)
        .with_context(|| format!("failed to create {}", delta_path.display()))?;

    let mut header = ImageDeltaHeader {
        chunk_size: u32::try_from(chunk_size).context("chunk size exceeds u32")?,
        base_size,
        target_size,
        base_sha256: [0u8; 32],
        target_sha256: [0u8; 32],
        changed_chunks: 0,
    };
    write_image_delta_header(&mut delta_file, &header)?;

    let mut base_hasher = Sha256::new();
    let mut target_hasher = Sha256::new();
    let mut base_buf = vec![0u8; chunk_size];
    let mut target_buf = vec![0u8; chunk_size];
    let mut chunk_index = 0u64;

    loop {
        let base_n = read_full_chunk(&mut base_reader, &mut base_buf)?;
        let target_n = read_full_chunk(&mut target_reader, &mut target_buf)?;
        if base_n == 0 && target_n == 0 {
            break;
        }

        base_hasher.update(&base_buf[..base_n]);
        target_hasher.update(&target_buf[..target_n]);

        let unchanged = base_n == target_n && base_buf[..base_n] == target_buf[..target_n];
        if !unchanged && target_n > 0 {
            let compressed =
                zstd::stream::encode_all(std::io::Cursor::new(&target_buf[..target_n]), 0)
                    .context("failed to compress changed chunk")?;
            write_u64_le(&mut delta_file, chunk_index)?;
            write_u32_le(
                &mut delta_file,
                u32::try_from(target_n).context("chunk length exceeds u32")?,
            )?;
            write_u32_le(
                &mut delta_file,
                u32::try_from(compressed.len()).context("compressed chunk exceeds u32")?,
            )?;
            delta_file
                .write_all(&compressed)
                .context("failed to write compressed chunk")?;
            header.changed_chunks = header
                .changed_chunks
                .checked_add(1)
                .ok_or_else(|| anyhow!("changed chunk counter overflow"))?;
        }

        chunk_index = chunk_index
            .checked_add(1)
            .ok_or_else(|| anyhow!("chunk index overflow"))?;
    }

    let base_digest = base_hasher.finalize();
    let target_digest = target_hasher.finalize();
    header.base_sha256.copy_from_slice(base_digest.as_ref());
    header.target_sha256.copy_from_slice(target_digest.as_ref());

    delta_file
        .seek(SeekFrom::Start(0))
        .with_context(|| format!("failed to rewind {}", delta_path.display()))?;
    write_image_delta_header(&mut delta_file, &header)?;
    delta_file
        .sync_all()
        .with_context(|| format!("failed to sync {}", delta_path.display()))?;

    Ok(header)
}

fn apply_image_delta_file(
    base_image: &Path,
    delta_path: &Path,
    output_image: &Path,
) -> anyhow::Result<ImageDeltaHeader> {
    ensure_regular_file(base_image, "base image")?;
    ensure_regular_file(delta_path, "delta file")?;
    ensure_output_file_parent(output_image)?;
    ensure_output_path_absent(output_image, "output image")?;

    let mut delta_reader = File::open(delta_path)
        .with_context(|| format!("failed to open {}", delta_path.display()))?;
    let header = read_image_delta_header(&mut delta_reader)?;

    let actual_base_size = fs::metadata(base_image)
        .with_context(|| format!("failed to inspect {}", base_image.display()))?
        .len();
    if actual_base_size != header.base_size {
        bail!(
            "base image size mismatch: expected {} bytes, actual {} bytes",
            header.base_size,
            actual_base_size
        );
    }

    let actual_base_sha = sha256_file_raw(base_image)?;
    if actual_base_sha != header.base_sha256 {
        bail!(
            "base image digest mismatch: expected {}, actual {}",
            sha256_digest_hex(&header.base_sha256),
            sha256_digest_hex(&actual_base_sha)
        );
    }

    clone_or_copy_image_with_sidecars(base_image, output_image, true)?;
    let mut output = OpenOptions::new()
        .write(true)
        .read(true)
        .open(output_image)
        .with_context(|| format!("failed to open {}", output_image.display()))?;
    output
        .set_len(header.target_size)
        .with_context(|| format!("failed to resize {}", output_image.display()))?;

    let mut previous_index = None::<u64>;
    for _ in 0..header.changed_chunks {
        let chunk_index = read_u64_le(&mut delta_reader).context("failed to read chunk index")?;
        if let Some(prev) = previous_index {
            if chunk_index <= prev {
                bail!(
                    "delta chunk records must be strictly increasing (got {} after {})",
                    chunk_index,
                    prev
                );
            }
        }
        previous_index = Some(chunk_index);

        let target_len =
            read_u32_le(&mut delta_reader).context("failed to read chunk target length")? as usize;
        let compressed_len = read_u32_le(&mut delta_reader)
            .context("failed to read compressed chunk length")?
            as usize;
        if target_len == 0 || target_len > header.chunk_size as usize {
            bail!(
                "invalid chunk length {} (chunk size is {})",
                target_len,
                header.chunk_size
            );
        }

        let offset = chunk_index
            .checked_mul(header.chunk_size as u64)
            .ok_or_else(|| anyhow!("chunk offset overflow"))?;
        let end = offset
            .checked_add(target_len as u64)
            .ok_or_else(|| anyhow!("chunk end overflow"))?;
        if end > header.target_size {
            bail!(
                "chunk {} writes past target size (end={}, target_size={})",
                chunk_index,
                end,
                header.target_size
            );
        }

        let mut compressed = vec![0u8; compressed_len];
        delta_reader
            .read_exact(&mut compressed)
            .with_context(|| format!("failed to read compressed data for chunk {chunk_index}"))?;
        let decompressed = zstd::stream::decode_all(std::io::Cursor::new(compressed))
            .with_context(|| format!("failed to decompress chunk {chunk_index}"))?;
        if decompressed.len() != target_len {
            bail!(
                "chunk {} length mismatch after decompression: expected {}, actual {}",
                chunk_index,
                target_len,
                decompressed.len()
            );
        }

        output
            .seek(SeekFrom::Start(offset))
            .with_context(|| format!("failed to seek output to {offset}"))?;
        output
            .write_all(&decompressed)
            .with_context(|| format!("failed to write chunk {chunk_index}"))?;
    }

    let mut trailing = [0u8; 1];
    if delta_reader.read(&mut trailing)? != 0 {
        bail!(
            "delta file {} has unexpected trailing data",
            delta_path.display()
        );
    }

    output
        .sync_all()
        .with_context(|| format!("failed to sync {}", output_image.display()))?;
    let output_sha = sha256_file_raw(output_image)?;
    if output_sha != header.target_sha256 {
        bail!(
            "patched output digest mismatch: expected {}, actual {}",
            sha256_digest_hex(&header.target_sha256),
            sha256_digest_hex(&output_sha)
        );
    }

    Ok(header)
}

fn write_image_delta_header(writer: &mut File, header: &ImageDeltaHeader) -> anyhow::Result<()> {
    writer
        .write_all(IMAGE_DELTA_MAGIC)
        .context("failed to write delta magic")?;
    write_u32_le(writer, IMAGE_DELTA_VERSION)?;
    write_u32_le(writer, header.chunk_size)?;
    write_u64_le(writer, header.base_size)?;
    write_u64_le(writer, header.target_size)?;
    writer
        .write_all(&header.base_sha256)
        .context("failed to write base digest")?;
    writer
        .write_all(&header.target_sha256)
        .context("failed to write target digest")?;
    write_u64_le(writer, header.changed_chunks)?;
    Ok(())
}

fn read_image_delta_header(reader: &mut File) -> anyhow::Result<ImageDeltaHeader> {
    let mut magic = [0u8; 8];
    reader
        .read_exact(&mut magic)
        .context("failed to read delta magic")?;
    if &magic != IMAGE_DELTA_MAGIC {
        bail!("invalid delta file magic");
    }

    let version = read_u32_le(reader).context("failed to read delta version")?;
    if version != IMAGE_DELTA_VERSION {
        bail!(
            "unsupported delta version {} (expected {})",
            version,
            IMAGE_DELTA_VERSION
        );
    }

    let chunk_size = read_u32_le(reader).context("failed to read chunk size")?;
    if chunk_size == 0 {
        bail!("delta chunk size must be greater than zero");
    }

    let base_size = read_u64_le(reader).context("failed to read base size")?;
    let target_size = read_u64_le(reader).context("failed to read target size")?;
    let mut base_sha256 = [0u8; 32];
    let mut target_sha256 = [0u8; 32];
    reader
        .read_exact(&mut base_sha256)
        .context("failed to read base digest")?;
    reader
        .read_exact(&mut target_sha256)
        .context("failed to read target digest")?;
    let changed_chunks = read_u64_le(reader).context("failed to read changed chunk count")?;

    Ok(ImageDeltaHeader {
        chunk_size,
        base_size,
        target_size,
        base_sha256,
        target_sha256,
        changed_chunks,
    })
}

fn write_u32_le(writer: &mut File, value: u32) -> anyhow::Result<()> {
    writer
        .write_all(&value.to_le_bytes())
        .context("failed to write u32")
}

fn write_u64_le(writer: &mut File, value: u64) -> anyhow::Result<()> {
    writer
        .write_all(&value.to_le_bytes())
        .context("failed to write u64")
}

fn read_u32_le(reader: &mut File) -> anyhow::Result<u32> {
    let mut bytes = [0u8; 4];
    reader
        .read_exact(&mut bytes)
        .context("failed to read u32")?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64_le(reader: &mut File) -> anyhow::Result<u64> {
    let mut bytes = [0u8; 8];
    reader
        .read_exact(&mut bytes)
        .context("failed to read u64")?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_full_chunk(reader: &mut File, buf: &mut [u8]) -> anyhow::Result<usize> {
    let mut read_total = 0usize;
    while read_total < buf.len() {
        match reader.read(&mut buf[read_total..]) {
            Ok(0) => break,
            Ok(read_n) => read_total += read_n,
            Err(err) if err.kind() == ErrorKind::Interrupted => continue,
            Err(err) => return Err(err).context("failed while reading chunk"),
        }
    }
    Ok(read_total)
}

fn sha256_file_raw(path: &Path) -> anyhow::Result<[u8; 32]> {
    let mut file =
        File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];

    loop {
        let read_n = file
            .read(&mut buffer)
            .with_context(|| format!("failed to read {}", path.display()))?;
        if read_n == 0 {
            break;
        }
        hasher.update(&buffer[..read_n]);
    }

    let digest = hasher.finalize();
    let mut result = [0u8; 32];
    result.copy_from_slice(digest.as_ref());
    Ok(result)
}

fn sha256_digest_hex(digest: &[u8; 32]) -> String {
    let mut hex = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    hex
}

fn prepare_bundle_output_dir(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir() {
                bail!(
                    "bundle output path {} exists and is not a directory",
                    path.display()
                );
            }
            let mut entries = fs::read_dir(path).with_context(|| {
                format!("failed to inspect existing bundle dir {}", path.display())
            })?;
            if entries.next().is_some() {
                bail!(
                    "bundle output directory {} already exists and is not empty",
                    path.display()
                );
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {
            fs::create_dir_all(path).with_context(|| {
                format!("failed to create bundle output dir {}", path.display())
            })?;
        }
        Err(err) => {
            return Err(err).with_context(|| {
                format!("failed to inspect bundle output path {}", path.display())
            });
        }
    }
    Ok(())
}

fn load_operations_file(path: &Path) -> anyhow::Result<Vec<PatchOperation>> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read operations file {}", path.display()))?;
    let operations: Vec<PatchOperation> = serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse operations JSON {}", path.display()))?;
    if operations.is_empty() {
        bail!(
            "operations file {} must contain at least one operation",
            path.display()
        );
    }
    for (index, operation) in operations.iter().enumerate() {
        operation
            .validate(index)
            .with_context(|| format!("invalid operation in {}", path.display()))?;
    }
    Ok(operations)
}

fn load_post_state_hashes_file(path: &Path) -> anyhow::Result<BTreeMap<String, String>> {
    let bytes = fs::read(path)
        .with_context(|| format!("failed to read post_state_hashes file {}", path.display()))?;
    serde_json::from_slice(&bytes)
        .with_context(|| format!("failed to parse post_state_hashes JSON {}", path.display()))
}

fn derive_post_state_hashes(
    operations: &[PatchOperation],
) -> anyhow::Result<BTreeMap<String, String>> {
    let mut hashes = BTreeMap::new();
    for (index, operation) in operations.iter().enumerate() {
        match operation {
            PatchOperation::WriteFile {
                path,
                content_digest,
                ..
            } => {
                let digest = normalize_sha256_field(
                    &operation_field(index, "content_digest"),
                    content_digest,
                )?;
                hashes.insert(path.clone(), digest);
            }
            PatchOperation::Symlink { path, target } => {
                hashes.insert(
                    path.clone(),
                    sha256_bytes_hex(Path::new(target).as_os_str().as_bytes()),
                );
            }
            PatchOperation::DeleteFile { path } => {
                hashes.remove(path);
            }
            PatchOperation::Mkdir { .. }
            | PatchOperation::SetOwner { .. }
            | PatchOperation::SetMode { .. } => {}
        }
    }
    Ok(hashes)
}

fn default_bundle_id(target_base_id: &str) -> String {
    format!("patch-{target_base_id}-{}", now_unix_seconds())
}

fn default_created_at() -> String {
    format!("{}", now_unix_seconds())
}

fn now_unix_seconds() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_secs(),
        Err(_) => 0,
    }
}

fn load_payload_entries(payload_dir: &Path) -> anyhow::Result<Vec<(String, Vec<u8>)>> {
    let metadata = fs::symlink_metadata(payload_dir)
        .with_context(|| format!("failed to inspect payload dir {}", payload_dir.display()))?;
    if !metadata.is_dir() {
        bail!("payload dir {} is not a directory", payload_dir.display());
    }

    let mut entries = Vec::new();
    let read_dir = fs::read_dir(payload_dir)
        .with_context(|| format!("failed to read payload dir {}", payload_dir.display()))?;
    for entry in read_dir {
        let entry = entry.with_context(|| {
            format!(
                "failed to iterate payload entries under {}",
                payload_dir.display()
            )
        })?;
        let path = entry.path();
        let metadata = fs::symlink_metadata(&path)
            .with_context(|| format!("failed to inspect payload entry {}", path.display()))?;
        if !metadata.file_type().is_file() {
            bail!(
                "payload entry {} must be a regular file named by SHA-256 digest",
                path.display()
            );
        }

        let digest_label = entry.file_name();
        let digest_label = digest_label.to_str().ok_or_else(|| {
            anyhow!(
                "payload entry {} file name is not valid UTF-8",
                path.display()
            )
        })?;
        let digest = normalize_sha256_field(
            &format!("payload entry name '{}'", path.display()),
            digest_label,
        )?;
        let bytes = fs::read(&path)
            .with_context(|| format!("failed to read payload entry {}", path.display()))?;
        let actual = sha256_bytes_hex(&bytes);
        if actual != digest {
            bail!(
                "payload entry {} digest mismatch: file name is {}, content hash is {}",
                path.display(),
                digest,
                actual
            );
        }
        entries.push((digest, bytes));
    }

    entries.sort_by(|a, b| a.0.cmp(&b.0));
    for pair in entries.windows(2) {
        if pair[0].0 == pair[1].0 {
            bail!(
                "payload dir {} contains duplicate digest {}",
                payload_dir.display(),
                pair[0].0
            );
        }
    }

    Ok(entries)
}

fn payload_digest_index(entries: &[(String, Vec<u8>)]) -> BTreeMap<String, Vec<u8>> {
    let mut index = BTreeMap::new();
    for (digest, _) in entries {
        index.insert(digest.clone(), Vec::new());
    }
    index
}

fn build_payload_archive(entries: &[(String, Vec<u8>)]) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    let encoder = zstd::Encoder::new(&mut payload, 0).context("failed to create zstd encoder")?;
    let mut builder = tar::Builder::new(encoder);

    for (digest, bytes) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(bytes.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        builder
            .append_data(&mut header, digest, bytes.as_slice())
            .with_context(|| format!("failed to append payload entry '{digest}'"))?;
    }

    let encoder = builder
        .into_inner()
        .context("failed to finalize tar payload")?;
    encoder
        .finish()
        .context("failed to finalize zstd payload")?;
    Ok(payload)
}

fn load_ed25519_key_pair(path: &Path) -> anyhow::Result<signature::Ed25519KeyPair> {
    let pkcs8 = load_pkcs8_private_key(path)?;
    signature::Ed25519KeyPair::from_pkcs8(&pkcs8)
        .or_else(|_| signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(&pkcs8))
        .map_err(|_| {
            anyhow!(
                "failed to parse Ed25519 private key from {} (expected PKCS#8 DER or PEM)",
                path.display()
            )
        })
}

fn load_pkcs8_private_key(path: &Path) -> anyhow::Result<Vec<u8>> {
    let raw =
        fs::read(path).with_context(|| format!("failed to read signing key {}", path.display()))?;
    if raw.starts_with(b"-----BEGIN ") {
        decode_pem_private_key(&raw)
            .with_context(|| format!("failed to decode PEM private key {}", path.display()))
    } else {
        Ok(raw)
    }
}

fn decode_pem_private_key(raw: &[u8]) -> anyhow::Result<Vec<u8>> {
    let text = std::str::from_utf8(raw).context("private key PEM is not valid UTF-8")?;
    let mut inside = false;
    let mut saw_footer = false;
    let mut body = String::new();

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("-----BEGIN ") && trimmed.ends_with("-----") {
            inside = true;
            continue;
        }
        if trimmed.starts_with("-----END ") && trimmed.ends_with("-----") {
            saw_footer = true;
            break;
        }
        if inside && !trimmed.is_empty() {
            body.push_str(trimmed);
        }
    }

    if !inside || !saw_footer || body.is_empty() {
        bail!("private key PEM is missing BEGIN/END markers or base64 body");
    }

    base64::engine::general_purpose::STANDARD
        .decode(body)
        .context("private key PEM body is not valid base64")
}

fn expand_home(path: &Path) -> PathBuf {
    let s = path.to_string_lossy();
    if s.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(format!("{}{}", home, &s[1..]));
        }
    }
    path.to_path_buf()
}

fn verify_bundle(bundle_dir: &Path) -> anyhow::Result<PatchBundleManifest> {
    let paths = BundlePaths::from_bundle_dir(bundle_dir);

    let manifest_bytes = fs::read(&paths.manifest)
        .with_context(|| format!("failed to read {}", paths.manifest.display()))?;
    let manifest = PatchBundleManifest::from_json_bytes(&manifest_bytes)
        .with_context(|| format!("invalid bundle metadata in {}", paths.manifest.display()))?;

    verify_manifest_signature(
        &manifest_bytes,
        &paths.signature,
        &manifest.signing_identity,
    )?;
    verify_payload_digest(&paths.payload, &manifest.payload_digest)?;
    verify_operations_digest(&manifest)?;

    Ok(manifest)
}

fn validate_patch_target_base_policy(manifest: &PatchBundleManifest) -> anyhow::Result<()> {
    let matrix = crate::commands::vm_base::BaseMatrix::load()?;
    validate_patch_target_base_policy_with_matrix(manifest, &matrix)
}

fn validate_patch_target_base_policy_with_matrix(
    manifest: &PatchBundleManifest,
    matrix: &crate::commands::vm_base::BaseMatrix,
) -> anyhow::Result<()> {
    crate::commands::vm_base::resolve_base_selector_or_err(matrix, &manifest.target_base_id)
        .with_context(|| {
            format!(
                "patch bundle '{}' targets unsupported or retired base '{}'",
                manifest.bundle_id, manifest.target_base_id
            )
        })?;
    Ok(())
}

fn validate_apply_preflight(
    manifest: &PatchBundleManifest,
    payload_by_digest: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    for (index, operation) in manifest.operations.iter().enumerate() {
        let path = operation.path();
        let _ = guest_path_to_relative(path)
            .with_context(|| format!("operation[{index}] path '{path}' failed safety checks"))?;

        if let PatchOperation::WriteFile { content_digest, .. } = operation {
            let digest =
                normalize_sha256_field(&operation_field(index, "content_digest"), content_digest)?;
            if !payload_by_digest.contains_key(&digest) {
                bail!(
                    "operation[{index}] path '{}' references missing payload content digest '{}'",
                    path,
                    digest
                );
            }
        }
    }

    for (path, digest) in &manifest.post_state_hashes {
        let _ = guest_path_to_relative(path).with_context(|| {
            format!("manifest.post_state_hashes path '{path}' failed safety checks")
        })?;
        let _ = normalize_sha256_field(&format!("manifest.post_state_hashes['{path}']"), digest)?;
    }

    Ok(())
}

fn payload_entry_digest(entry_path: &Path) -> anyhow::Result<String> {
    let file_name = entry_path
        .file_name()
        .ok_or_else(|| anyhow!("payload entry '{}' has no file name", entry_path.display()))?;
    let digest = file_name.to_str().ok_or_else(|| {
        anyhow!(
            "payload entry '{}' is not valid UTF-8",
            entry_path.display()
        )
    })?;
    normalize_sha256_field(&format!("payload entry '{}'", entry_path.display()), digest)
}

fn verify_manifest_signature(
    manifest_bytes: &[u8],
    signature_path: &Path,
    signing_identity: &str,
) -> anyhow::Result<()> {
    let signature_bytes = fs::read(signature_path).with_context(|| {
        format!(
            "failed to read detached signature {}",
            signature_path.display()
        )
    })?;
    let signature = parse_detached_signature(&signature_bytes)?;
    let public_key = parse_signing_identity(signing_identity)?;

    let verifier = signature::UnparsedPublicKey::new(&signature::ED25519, public_key);
    verifier.verify(manifest_bytes, &signature).map_err(|_| {
        anyhow!(
            "signature verification failed for manifest.json using signing identity '{}'",
            signing_identity
        )
    })?;

    Ok(())
}

fn verify_payload_digest(payload_path: &Path, expected_digest: &str) -> anyhow::Result<()> {
    let expected = normalize_sha256_field("manifest.payload_digest", expected_digest)?;
    let actual = ipsw::sha256_file(payload_path)
        .with_context(|| format!("failed to hash payload {}", payload_path.display()))?;
    if actual != expected {
        bail!(
            "payload digest mismatch for {}: expected {}, actual {}",
            payload_path.display(),
            expected,
            actual
        );
    }
    Ok(())
}

fn verify_operations_digest(manifest: &PatchBundleManifest) -> anyhow::Result<()> {
    let expected =
        normalize_sha256_field("manifest.operations_digest", &manifest.operations_digest)?;
    let actual = operations_digest_hex(&manifest.operations)?;

    if actual != expected {
        bail!(
            "operations digest mismatch: expected {}, actual {}. Recompute manifest.operations_digest from manifest.operations.",
            expected,
            actual
        );
    }

    Ok(())
}

fn parse_signing_identity(signing_identity: &str) -> anyhow::Result<Vec<u8>> {
    validate_non_empty("manifest.signing_identity", signing_identity)?;
    let Some(encoded_key) = signing_identity.strip_prefix(SIGNING_IDENTITY_PREFIX) else {
        bail!("manifest.signing_identity must use 'ed25519:<base64-public-key>' format");
    };

    let key = base64::engine::general_purpose::STANDARD
        .decode(encoded_key)
        .context("manifest.signing_identity contains invalid base64 public key")?;

    if key.len() != ED25519_PUBLIC_KEY_LEN {
        bail!(
            "manifest.signing_identity Ed25519 public key must be {} bytes, found {}",
            ED25519_PUBLIC_KEY_LEN,
            key.len()
        );
    }

    Ok(key)
}

fn parse_detached_signature(bytes: &[u8]) -> anyhow::Result<Vec<u8>> {
    if bytes.len() == ED25519_SIGNATURE_LEN {
        return Ok(bytes.to_vec());
    }

    let raw_text = std::str::from_utf8(bytes).context(
        "signature.sig must be a raw 64-byte Ed25519 signature or base64-encoded signature text",
    )?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw_text.trim())
        .context("signature.sig is not valid base64")?;

    if decoded.len() != ED25519_SIGNATURE_LEN {
        bail!(
            "signature.sig must contain a {}-byte Ed25519 signature (found {} bytes)",
            ED25519_SIGNATURE_LEN,
            decoded.len()
        );
    }

    Ok(decoded)
}

fn operations_digest_hex(operations: &[PatchOperation]) -> anyhow::Result<String> {
    let encoded = serde_json::to_vec(operations)
        .context("failed to serialize manifest.operations for digest computation")?;
    Ok(sha256_bytes_hex(&encoded))
}

fn sha256_bytes_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

struct BundlePaths {
    manifest: PathBuf,
    payload: PathBuf,
    signature: PathBuf,
}

impl BundlePaths {
    fn from_bundle_dir(bundle_dir: &Path) -> Self {
        Self {
            manifest: bundle_dir.join(MANIFEST_FILE),
            payload: bundle_dir.join(PAYLOAD_FILE),
            signature: bundle_dir.join(SIGNATURE_FILE),
        }
    }
}
