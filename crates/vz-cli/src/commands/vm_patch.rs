//! `vz vm patch` -- Signed patch bundles plus binary image deltas.

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

static TEMP_FILE_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Manage signed patch bundles.
#[derive(Args, Debug)]
pub struct VmPatchArgs {
    #[command(subcommand)]
    pub action: VmPatchCommand,
}

/// `vz vm patch` subcommands.
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

/// Arguments for `vz vm patch create`.
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

/// Arguments for `vz vm patch verify`.
#[derive(Args, Debug)]
pub struct VerifyArgs {
    /// Bundle directory containing manifest, payload, and detached signature.
    #[arg(long)]
    pub bundle: PathBuf,
}

/// Arguments for `vz vm patch apply`.
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

/// Arguments for `vz vm patch create-delta`.
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

/// Arguments for `vz vm patch apply-delta`.
#[derive(Args, Debug)]
pub struct ApplyDeltaArgs {
    /// Base raw VM image path used as delta source.
    #[arg(long)]
    pub base_image: PathBuf,

    /// Binary delta file produced by `vz vm patch create-delta`.
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

/// Entry point for `vz vm patch`.
pub async fn run(args: VmPatchArgs) -> anyhow::Result<()> {
    match args.action {
        VmPatchCommand::Create(args) => create(args),
        VmPatchCommand::Verify(args) => verify(args),
        VmPatchCommand::Apply(args) => apply(args),
        VmPatchCommand::CreateDelta(args) => create_delta(args),
        VmPatchCommand::ApplyDelta(args) => apply_delta(args),
    }
}

fn create(args: CreateArgs) -> anyhow::Result<()> {
    prepare_bundle_output_dir(&args.bundle)?;

    let resolved_base =
        super::vm_base::resolve_base_selector(&args.base_id).with_context(|| {
            format!(
                "failed to resolve --base-id selector '{}' for patch creation",
                args.base_id
            )
        })?;

    let inline_mode_requested = create_inline_mode_requested(&args);
    let (operations, payload_entries) = match (
        args.operations.as_ref(),
        args.payload_dir.as_ref(),
        inline_mode_requested,
    ) {
        (Some(operations), Some(payload_dir), false) => {
            let operations = load_operations_file(operations)?;
            let payload_entries = load_payload_entries(payload_dir)?;
            (operations, payload_entries)
        }
        (None, None, true) => build_inline_create_inputs(&args)?,
        (Some(_), Some(_), true) => bail!(
            "choose one create input mode: either (--operations + --payload-dir) or inline flags (--write-file/--mkdir/--symlink/--delete-file/--set-mode/--set-owner)"
        ),
        (Some(_), None, _) | (None, Some(_), _) => {
            bail!("--operations and --payload-dir must be provided together")
        }
        (None, None, false) => bail!(
            "no patch inputs provided. Use either (--operations + --payload-dir) or inline flags (--write-file/--mkdir/--symlink/--delete-file/--set-mode/--set-owner)"
        ),
    };

    let payload_digest_index = payload_digest_index(&payload_entries);
    let payload = build_payload_archive(&payload_entries)?;

    let post_state_hashes = if let Some(path) = args.post_state_hashes.as_ref() {
        load_post_state_hashes_file(path)?
    } else {
        derive_post_state_hashes(&operations)?
    };

    let key_pair = load_ed25519_key_pair(&args.signing_key)?;
    let signing_identity = format!(
        "ed25519:{}",
        base64::engine::general_purpose::STANDARD.encode(key_pair.public_key().as_ref())
    );
    let created_at = args.created_at.unwrap_or_else(default_created_at);
    let bundle_id = args
        .bundle_id
        .unwrap_or_else(|| default_bundle_id(&resolved_base.base.base_id));
    let target_base_fingerprint = BundleBaseFingerprint {
        img_sha256: resolved_base.base.fingerprint.img_sha256.clone(),
        aux_sha256: resolved_base.base.fingerprint.aux_sha256.clone(),
        hwmodel_sha256: resolved_base.base.fingerprint.hwmodel_sha256.clone(),
        machineid_sha256: resolved_base.base.fingerprint.machineid_sha256.clone(),
    };

    let manifest = PatchBundleManifest {
        bundle_id,
        patch_version: args.patch_version,
        target_base_id: resolved_base.base.base_id.clone(),
        target_base_fingerprint,
        operations_digest: operations_digest_hex(&operations)?,
        payload_digest: sha256_bytes_hex(&payload),
        post_state_hashes,
        created_at,
        signing_identity,
        operations,
    };
    manifest.validate()?;
    validate_apply_preflight(&manifest, &payload_digest_index)?;

    let manifest_bytes =
        serde_json::to_vec_pretty(&manifest).context("failed to serialize manifest")?;
    fs::write(args.bundle.join(MANIFEST_FILE), &manifest_bytes).with_context(|| {
        format!(
            "failed to write bundle manifest {}",
            args.bundle.join(MANIFEST_FILE).display()
        )
    })?;
    fs::write(args.bundle.join(PAYLOAD_FILE), &payload).with_context(|| {
        format!(
            "failed to write bundle payload {}",
            args.bundle.join(PAYLOAD_FILE).display()
        )
    })?;
    let signature = key_pair.sign(&manifest_bytes);
    fs::write(args.bundle.join(SIGNATURE_FILE), signature.as_ref()).with_context(|| {
        format!(
            "failed to write detached signature {}",
            args.bundle.join(SIGNATURE_FILE).display()
        )
    })?;

    let verified = verify_bundle(&args.bundle)?;
    validate_patch_target_base_policy(&verified)?;

    println!(
        "Patch bundle '{}' created at {} for target base '{}'",
        verified.bundle_id,
        args.bundle.display(),
        verified.target_base_id
    );
    Ok(())
}

fn create_inline_mode_requested(args: &CreateArgs) -> bool {
    !args.write_file.is_empty()
        || !args.mkdir.is_empty()
        || !args.symlink.is_empty()
        || !args.delete_file.is_empty()
        || !args.set_mode.is_empty()
        || !args.set_owner.is_empty()
}

fn build_inline_create_inputs(
    args: &CreateArgs,
) -> anyhow::Result<(Vec<PatchOperation>, Vec<(String, Vec<u8>)>)> {
    let mut operations = Vec::new();
    let mut payload_by_digest = BTreeMap::<String, Vec<u8>>::new();

    for spec in &args.mkdir {
        let (path, mode) = parse_mkdir_spec(spec)?;
        operations.push(PatchOperation::Mkdir { path, mode });
    }

    for spec in &args.write_file {
        let (host_path, guest_path, mode_override) = parse_write_file_spec(spec)?;
        let metadata = fs::symlink_metadata(&host_path).with_context(|| {
            format!(
                "failed to inspect host file in --write-file spec '{}'",
                host_path.display()
            )
        })?;
        if !metadata.file_type().is_file() {
            bail!(
                "host path '{}' from --write-file is not a regular file",
                host_path.display()
            );
        }
        let bytes = fs::read(&host_path).with_context(|| {
            format!(
                "failed to read host file in --write-file spec '{}'",
                host_path.display()
            )
        })?;
        let digest = sha256_bytes_hex(&bytes);
        let _ = payload_by_digest.entry(digest.clone()).or_insert(bytes);
        let mode = mode_override.or(Some(metadata.mode() & 0o7777));
        operations.push(PatchOperation::WriteFile {
            path: guest_path,
            content_digest: digest,
            mode,
        });
    }

    for spec in &args.symlink {
        let (path, target) = parse_symlink_spec(spec)?;
        operations.push(PatchOperation::Symlink { path, target });
    }

    for spec in &args.set_owner {
        let (path, uid, gid) = parse_set_owner_spec(spec)?;
        operations.push(PatchOperation::SetOwner { path, uid, gid });
    }

    for spec in &args.set_mode {
        let (path, mode) = parse_set_mode_spec(spec)?;
        operations.push(PatchOperation::SetMode { path, mode });
    }

    for spec in &args.delete_file {
        let path = parse_delete_file_spec(spec)?;
        operations.push(PatchOperation::DeleteFile { path });
    }

    if operations.is_empty() {
        bail!("inline create mode produced no operations");
    }

    let mut payload_entries = Vec::new();
    for (digest, bytes) in payload_by_digest {
        payload_entries.push((digest, bytes));
    }

    Ok((operations, payload_entries))
}

fn parse_write_file_spec(spec: &str) -> anyhow::Result<(PathBuf, String, Option<u32>)> {
    let parts: Vec<&str> = spec.split(':').collect();
    if !(2..=3).contains(&parts.len()) {
        bail!(
            "invalid --write-file spec '{}'; expected HOST_PATH:GUEST_PATH[:MODE]",
            spec
        );
    }

    let host_path_raw = parts[0].trim();
    let guest_path = parts[1].trim();
    if host_path_raw.is_empty() || guest_path.is_empty() {
        bail!(
            "invalid --write-file spec '{}'; host path and guest path must be non-empty",
            spec
        );
    }

    let mode = if parts.len() == 3 {
        Some(parse_mode_value("--write-file MODE", parts[2].trim())?)
    } else {
        None
    };

    Ok((PathBuf::from(host_path_raw), guest_path.to_string(), mode))
}

fn parse_mkdir_spec(spec: &str) -> anyhow::Result<(String, Option<u32>)> {
    let parts: Vec<&str> = spec.split(':').collect();
    if !(1..=2).contains(&parts.len()) {
        bail!(
            "invalid --mkdir spec '{}'; expected GUEST_PATH[:MODE]",
            spec
        );
    }
    let path = parts[0].trim();
    if path.is_empty() {
        bail!(
            "invalid --mkdir spec '{}'; guest path must be non-empty",
            spec
        );
    }
    let mode = if parts.len() == 2 {
        Some(parse_mode_value("--mkdir MODE", parts[1].trim())?)
    } else {
        None
    };
    Ok((path.to_string(), mode))
}

fn parse_symlink_spec(spec: &str) -> anyhow::Result<(String, String)> {
    let Some((path, target)) = spec.split_once(':') else {
        bail!(
            "invalid --symlink spec '{}'; expected GUEST_PATH:TARGET",
            spec
        );
    };
    let path = path.trim();
    let target = target.trim();
    if path.is_empty() || target.is_empty() {
        bail!(
            "invalid --symlink spec '{}'; guest path and target must be non-empty",
            spec
        );
    }
    Ok((path.to_string(), target.to_string()))
}

fn parse_delete_file_spec(spec: &str) -> anyhow::Result<String> {
    let path = spec.trim();
    if path.is_empty() {
        bail!(
            "invalid --delete-file spec '{}'; guest path must be non-empty",
            spec
        );
    }
    Ok(path.to_string())
}

fn parse_set_mode_spec(spec: &str) -> anyhow::Result<(String, u32)> {
    let Some((path, mode_raw)) = spec.split_once(':') else {
        bail!(
            "invalid --set-mode spec '{}'; expected GUEST_PATH:MODE",
            spec
        );
    };
    let path = path.trim();
    if path.is_empty() {
        bail!(
            "invalid --set-mode spec '{}'; guest path must be non-empty",
            spec
        );
    }
    let mode = parse_mode_value("--set-mode MODE", mode_raw.trim())?;
    Ok((path.to_string(), mode))
}

fn parse_set_owner_spec(spec: &str) -> anyhow::Result<(String, u32, u32)> {
    let parts: Vec<&str> = spec.split(':').collect();
    if parts.len() != 3 {
        bail!(
            "invalid --set-owner spec '{}'; expected GUEST_PATH:UID:GID",
            spec
        );
    }
    let path = parts[0].trim();
    if path.is_empty() {
        bail!(
            "invalid --set-owner spec '{}'; guest path must be non-empty",
            spec
        );
    }
    let uid = parse_u32_value("--set-owner UID", parts[1].trim())?;
    let gid = parse_u32_value("--set-owner GID", parts[2].trim())?;
    Ok((path.to_string(), uid, gid))
}

fn parse_mode_value(field: &str, value: &str) -> anyhow::Result<u32> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    let parsed = if let Some(stripped) = value.strip_prefix("0o") {
        u32::from_str_radix(stripped, 8)
            .with_context(|| format!("{field} must be valid octal, received '{value}'"))?
    } else if value.len() > 1 && value.starts_with('0') {
        u32::from_str_radix(value, 8)
            .with_context(|| format!("{field} must be valid octal, received '{value}'"))?
    } else if value.len() <= 4 && value.chars().all(|c| matches!(c, '0'..='7')) {
        u32::from_str_radix(value, 8)
            .with_context(|| format!("{field} must be valid octal, received '{value}'"))?
    } else {
        value
            .parse::<u32>()
            .with_context(|| format!("{field} must be valid u32, received '{value}'"))?
    };
    validate_mode(field, parsed)?;
    Ok(parsed)
}

fn parse_u32_value(field: &str, value: &str) -> anyhow::Result<u32> {
    if value.is_empty() {
        bail!("{field} must not be empty");
    }
    value
        .parse::<u32>()
        .with_context(|| format!("{field} must be valid u32, received '{value}'"))
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

fn apply(args: ApplyArgs) -> anyhow::Result<()> {
    let patch_state_path = patch_state_path();
    apply_with_state_path(args, &patch_state_path)
}

fn patch_state_path() -> PathBuf {
    crate::registry::vz_home().join(PATCH_STATE_FILE)
}

fn apply_with_state_path(args: ApplyArgs, patch_state_path: &Path) -> anyhow::Result<()> {
    match (args.root.as_ref(), args.image.as_ref()) {
        (Some(root), None) => apply_with_root(&args.bundle, root, patch_state_path),
        (None, Some(image)) => apply_with_image(&args.bundle, image, patch_state_path),
        _ => bail!("exactly one apply target is required: --root <path> or --image <path>"),
    }
}

fn create_delta(args: CreateDeltaArgs) -> anyhow::Result<()> {
    let base_image = expand_home(&args.base_image);
    let bundle = expand_home(&args.bundle);
    let delta = expand_home(&args.delta);
    ensure_regular_file(&base_image, "--base-image")?;
    ensure_dir(&bundle, "--bundle")?;
    ensure_output_file_parent(&delta)?;
    ensure_output_path_absent(&delta, "--delta")?;

    let workspace = TempWorkspace::new("vz-image-delta-create")?;
    let patched_image = workspace.path().join("patched.img");

    clone_or_copy_file(&base_image, &patched_image)?;
    let state_path = workspace.path().join("patch-state.json");
    apply_with_state_path(
        ApplyArgs {
            bundle: bundle.clone(),
            root: None,
            image: Some(patched_image.clone()),
        },
        &state_path,
    )?;

    let chunk_size = mib_to_bytes(args.chunk_size_mib)?;
    let header = create_image_delta_file(&base_image, &patched_image, &delta, chunk_size)
        .with_context(|| {
            format!(
                "failed to build image delta from {} to {}",
                base_image.display(),
                patched_image.display()
            )
        })?;

    println!(
        "Image delta created at {} (chunk={} MiB, changed_chunks={}, base={}, target={})",
        delta.display(),
        args.chunk_size_mib,
        header.changed_chunks,
        base_image.display(),
        patched_image.display()
    );
    Ok(())
}

fn apply_delta(args: ApplyDeltaArgs) -> anyhow::Result<()> {
    let base_image = expand_home(&args.base_image);
    let delta = expand_home(&args.delta);
    let output_image = expand_home(&args.output_image);
    ensure_regular_file(&base_image, "--base-image")?;
    ensure_regular_file(&delta, "--delta")?;
    ensure_output_file_parent(&output_image)?;
    ensure_output_path_absent(&output_image, "--output-image")?;

    let header = apply_image_delta_file(&base_image, &delta, &output_image).with_context(|| {
        format!(
            "failed to apply image delta {} to {}",
            delta.display(),
            base_image.display()
        )
    })?;
    println!(
        "Image delta applied to {} (changed_chunks={}, target_size={} bytes)",
        output_image.display(),
        header.changed_chunks,
        header.target_size
    );
    Ok(())
}

fn apply_with_image(bundle: &Path, image: &Path, patch_state_path: &Path) -> anyhow::Result<()> {
    let image = expand_home(image);
    if !image.exists() {
        bail!("disk image not found: {}", image.display());
    }

    let manifest = verify_bundle(bundle)?;
    validate_patch_target_base_policy(&manifest)?;
    super::vm_base::verify_image_for_base_id(&image, &manifest.target_base_id).with_context(
        || {
            format!(
                "pinned base verification failed before applying patch to image {}",
                image.display()
            )
        },
    )?;

    let disk = crate::provision::attach_and_mount(&image).with_context(|| {
        format!(
            "failed to attach and mount image {} before patch apply",
            image.display()
        )
    })?;

    let result =
        apply_verified_manifest_with_root(manifest, bundle, &disk.mount_point, patch_state_path);
    let detach_result = disk.detach();

    result?;
    detach_result?;

    println!("Patch apply completed for image {}", image.display());
    Ok(())
}

fn apply_with_root(bundle: &Path, root: &Path, patch_state_path: &Path) -> anyhow::Result<()> {
    let manifest = verify_bundle(bundle)?;
    validate_patch_target_base_policy(&manifest)?;
    apply_verified_manifest_with_root(manifest, bundle, root, patch_state_path)
}

fn apply_verified_manifest_with_root(
    manifest: PatchBundleManifest,
    bundle: &Path,
    root_arg: &Path,
    patch_state_path: &Path,
) -> anyhow::Result<()> {
    let root = fs::canonicalize(root_arg)
        .with_context(|| format!("failed to resolve apply root {}", root_arg.display()))?;
    if !root.is_dir() {
        bail!("apply root {} is not a directory", root.display());
    }

    let apply_receipt = PatchApplyReceipt::from_manifest(&root, &manifest)?;
    let mut patch_state = PatchApplyState::load(patch_state_path)?;
    if patch_state.has_receipt(&apply_receipt) {
        println!(
            "Patch bundle '{}' already applied at {} for target base '{}'; no changes made.",
            manifest.bundle_id,
            root.display(),
            manifest.target_base_id
        );
        return Ok(());
    }
    if let Some(existing) = patch_state.find_conflicting_receipt(&apply_receipt) {
        bail!(
            "patch receipt mismatch for bundle '{}' at {}. expected(existing receipt): {}. actual(requested apply): {}. Refusing to re-apply; inspect or remove {} if this is intentional.",
            manifest.bundle_id,
            root.display(),
            existing.identity_details(),
            apply_receipt.identity_details(),
            patch_state_path.display()
        );
    }

    let paths = BundlePaths::from_bundle_dir(bundle);
    let payload_by_digest = load_payload_archive(&paths.payload)?;
    validate_apply_preflight(&manifest, &payload_by_digest)?;
    apply_operations_transactional(&root, &manifest, &payload_by_digest)?;
    patch_state.record_receipt(apply_receipt);
    patch_state.save(patch_state_path).with_context(|| {
        format!(
            "patch operations were applied at {} but failed to persist receipt in {}",
            root.display(),
            patch_state_path.display()
        )
    })?;

    println!(
        "Patch bundle '{}' applied successfully at {}",
        manifest.bundle_id,
        root.display()
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

    clone_or_copy_file(base_image, output_image)?;
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
    let matrix = super::vm_base::BaseMatrix::load()?;
    validate_patch_target_base_policy_with_matrix(manifest, &matrix)
}

fn validate_patch_target_base_policy_with_matrix(
    manifest: &PatchBundleManifest,
    matrix: &super::vm_base::BaseMatrix,
) -> anyhow::Result<()> {
    super::vm_base::resolve_base_selector_or_err(matrix, &manifest.target_base_id).with_context(
        || {
            format!(
                "patch bundle '{}' targets unsupported or retired base '{}'",
                manifest.bundle_id, manifest.target_base_id
            )
        },
    )?;
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

fn apply_operations_transactional(
    root: &Path,
    manifest: &PatchBundleManifest,
    payload_by_digest: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    let mut applied = Vec::new();
    for (index, operation) in manifest.operations.iter().enumerate() {
        let path = operation.path();
        let relative =
            guest_path_to_relative(path).with_context(|| operation_error_context(index, path))?;
        let host_path = root.join(&relative);
        let snapshot =
            capture_snapshot(&host_path).with_context(|| operation_error_context(index, path))?;
        applied.push(RollbackEntry {
            operation_index: index,
            path: path.to_string(),
            host_path: host_path.clone(),
            snapshot,
        });

        if let Err(err) = apply_operation(root, &relative, &host_path, operation, payload_by_digest)
        {
            let message = format!("{}: {err:#}", operation_error_context(index, path));
            let rollback_errors = rollback_operations(&applied);
            return Err(error_with_rollback(message, rollback_errors));
        }
    }

    if let Err(err) = verify_post_state_hashes(root, manifest) {
        let rollback_errors = rollback_operations(&applied);
        return Err(error_with_rollback(
            format!("post-state hash verification failed: {err:#}"),
            rollback_errors,
        ));
    }

    Ok(())
}

fn operation_error_context(index: usize, path: &str) -> String {
    format!("operation[{index}] path '{path}'")
}

fn error_with_rollback(message: String, rollback_errors: Vec<String>) -> anyhow::Error {
    if rollback_errors.is_empty() {
        anyhow!(message)
    } else {
        anyhow!(
            "{message}; rollback encountered {} error(s): {}",
            rollback_errors.len(),
            rollback_errors.join(" | ")
        )
    }
}

fn rollback_operations(entries: &[RollbackEntry]) -> Vec<String> {
    let mut errors = Vec::new();
    for entry in entries.iter().rev() {
        if let Err(err) = restore_snapshot(&entry.host_path, &entry.snapshot) {
            errors.push(format!(
                "operation[{}] path '{}': {:#}",
                entry.operation_index, entry.path, err
            ));
        }
    }
    errors
}

fn apply_operation(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    operation: &PatchOperation,
    payload_by_digest: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    match operation {
        PatchOperation::Mkdir { mode, .. } => apply_mkdir(root, relative, host_path, *mode),
        PatchOperation::WriteFile {
            content_digest,
            mode,
            ..
        } => apply_write_file(
            root,
            relative,
            host_path,
            content_digest,
            *mode,
            payload_by_digest,
        ),
        PatchOperation::DeleteFile { .. } => apply_delete_file(root, relative, host_path),
        PatchOperation::Symlink { target, .. } => apply_symlink(root, relative, host_path, target),
        PatchOperation::SetOwner { uid, gid, .. } => {
            apply_set_owner(root, relative, host_path, *uid, *gid)
        }
        PatchOperation::SetMode { mode, .. } => apply_set_mode(root, relative, host_path, *mode),
    }
}

fn apply_mkdir(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    mode: Option<u32>,
) -> anyhow::Result<()> {
    ensure_ancestors_within_root(root, relative.parent().unwrap_or(Path::new("")))?;

    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!("refusing to follow symlink at {}", host_path.display());
            }
            if !metadata.is_dir() {
                bail!("mkdir target already exists and is not a directory");
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {
            fs::create_dir_all(host_path)
                .with_context(|| format!("failed to create directory {}", host_path.display()))?;
        }
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to inspect directory {}", host_path.display()));
        }
    }

    if let Some(mode) = mode {
        set_path_mode(host_path, mode)?;
    }

    Ok(())
}

fn apply_write_file(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    content_digest: &str,
    mode: Option<u32>,
    payload_by_digest: &BTreeMap<String, Vec<u8>>,
) -> anyhow::Result<()> {
    let parent = ensure_parent_dir_within_root(root, relative)?;
    let digest = normalize_sha256_field("write_file.content_digest", content_digest)?;
    let contents = payload_by_digest
        .get(&digest)
        .ok_or_else(|| anyhow!("missing payload content for digest '{}'", digest))?;

    let mut existing_mode = None;
    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                bail!("write_file target {} is a directory", host_path.display());
            }
            if metadata.file_type().is_symlink() {
                fs::remove_file(host_path).with_context(|| {
                    format!("failed to remove existing symlink {}", host_path.display())
                })?;
            } else if metadata.is_file() {
                existing_mode = Some(metadata.mode() & 0o7777);
            } else {
                bail!(
                    "write_file target {} has unsupported file type",
                    host_path.display()
                );
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", host_path.display()));
        }
    }

    let effective_mode = mode.unwrap_or(existing_mode.unwrap_or(DEFAULT_FILE_MODE));
    write_file_atomic(&parent, host_path, contents, effective_mode)
}

fn apply_delete_file(root: &Path, relative: &Path, host_path: &Path) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;

    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                bail!("delete_file target {} is a directory", host_path.display());
            }
            fs::remove_file(host_path)
                .with_context(|| format!("failed to delete {}", host_path.display()))?;
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", host_path.display()));
        }
    }

    Ok(())
}

fn apply_symlink(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    target: &str,
) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;

    match fs::symlink_metadata(host_path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                bail!("symlink target path {} is a directory", host_path.display());
            }
            if metadata.file_type().is_symlink() {
                let existing_target = fs::read_link(host_path).with_context(|| {
                    format!("failed to inspect symlink {}", host_path.display())
                })?;
                if existing_target == Path::new(target) {
                    return Ok(());
                }
            }
            fs::remove_file(host_path)
                .with_context(|| format!("failed to remove {}", host_path.display()))?;
        }
        Err(err) if err.kind() == ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err).with_context(|| format!("failed to inspect {}", host_path.display()));
        }
    }

    symlink(target, host_path).with_context(|| {
        format!(
            "failed to create symlink {} -> {}",
            host_path.display(),
            target
        )
    })?;
    Ok(())
}

fn apply_set_owner(
    root: &Path,
    relative: &Path,
    host_path: &Path,
    uid: u32,
    gid: u32,
) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;
    let _ = fs::symlink_metadata(host_path)
        .with_context(|| format!("set_owner path {} does not exist", host_path.display()))?;
    set_path_owner(host_path, uid, gid)
}

fn apply_set_mode(root: &Path, relative: &Path, host_path: &Path, mode: u32) -> anyhow::Result<()> {
    ensure_parent_dir_within_root(root, relative)?;
    let metadata = fs::symlink_metadata(host_path)
        .with_context(|| format!("set_mode path {} does not exist", host_path.display()))?;
    if metadata.file_type().is_symlink() {
        bail!(
            "set_mode refuses to operate on symlink {}",
            host_path.display()
        );
    }
    set_path_mode(host_path, mode)
}

fn verify_post_state_hashes(root: &Path, manifest: &PatchBundleManifest) -> anyhow::Result<()> {
    for (guest_path, digest) in &manifest.post_state_hashes {
        let relative = guest_path_to_relative(guest_path)
            .with_context(|| format!("invalid post_state path '{guest_path}'"))?;
        ensure_parent_dir_within_root(root, &relative)?;
        let host_path = root.join(&relative);
        let expected = normalize_sha256_field(
            &format!("manifest.post_state_hashes['{guest_path}']"),
            digest,
        )?;

        let metadata = fs::symlink_metadata(&host_path)
            .with_context(|| format!("post_state path '{}' does not exist", host_path.display()))?;
        let actual = if metadata.is_file() {
            ipsw::sha256_file(&host_path)
                .with_context(|| format!("failed to hash {}", host_path.display()))?
        } else if metadata.file_type().is_symlink() {
            let target = fs::read_link(&host_path).with_context(|| {
                format!("failed to read symlink target for {}", host_path.display())
            })?;
            sha256_bytes_hex(target.as_os_str().as_bytes())
        } else {
            bail!(
                "post_state path '{}' is not a regular file or symlink",
                guest_path
            );
        };

        if actual != expected {
            bail!(
                "post-state hash mismatch for '{}': expected {}, actual {}",
                guest_path,
                expected,
                actual
            );
        }
    }

    Ok(())
}

fn write_file_atomic(
    parent: &Path,
    destination: &Path,
    contents: &[u8],
    mode: u32,
) -> anyhow::Result<()> {
    let (temp_path, mut temp_file) = create_temp_file(parent)?;
    if let Err(err) = (|| -> anyhow::Result<()> {
        temp_file
            .write_all(contents)
            .with_context(|| format!("failed to write temp file {}", temp_path.display()))?;
        temp_file
            .sync_all()
            .with_context(|| format!("failed to sync temp file {}", temp_path.display()))?;
        drop(temp_file);
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(mode))
            .with_context(|| format!("failed to set mode on {}", temp_path.display()))?;
        fs::rename(&temp_path, destination).with_context(|| {
            format!(
                "failed to move temp file {} to {}",
                temp_path.display(),
                destination.display()
            )
        })?;
        Ok(())
    })() {
        let _ = fs::remove_file(&temp_path);
        return Err(err);
    }

    Ok(())
}

fn create_temp_file(parent: &Path) -> anyhow::Result<(PathBuf, File)> {
    for _ in 0..64 {
        let next = TEMP_FILE_COUNTER.fetch_add(1, Ordering::Relaxed);
        let candidate = parent.join(format!(".vzpatch.tmp.{next}"));
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((candidate, file)),
            Err(err) if err.kind() == ErrorKind::AlreadyExists => continue,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to create temp file {}", candidate.display())
                });
            }
        }
    }

    bail!(
        "failed to create unique temp file under {} after multiple attempts",
        parent.display()
    )
}

fn guest_path_to_relative(guest_path: &str) -> anyhow::Result<PathBuf> {
    validate_operation_path("operation.path", guest_path)?;
    let mut relative = PathBuf::new();
    for component in Path::new(guest_path).components() {
        match component {
            Component::RootDir => {}
            Component::Normal(part) => relative.push(part),
            Component::CurDir | Component::ParentDir => {
                bail!("path must not contain traversal components");
            }
            Component::Prefix(_) => {
                bail!("path contains unsupported platform prefix");
            }
        }
    }

    if relative.as_os_str().is_empty() {
        bail!("path '/' is not allowed");
    }

    Ok(relative)
}

fn ensure_ancestors_within_root(root: &Path, relative_ancestor: &Path) -> anyhow::Result<()> {
    let mut current = root.to_path_buf();
    for component in relative_ancestor.components() {
        let Component::Normal(part) = component else {
            bail!(
                "invalid path component '{}' while enforcing path safety",
                component.as_os_str().to_string_lossy()
            );
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!(
                        "path escapes apply root via symlink component {}",
                        current.display()
                    );
                }
                if !metadata.is_dir() {
                    bail!(
                        "path escapes apply root because component {} is not a directory",
                        current.display()
                    );
                }
            }
            Err(err) if err.kind() == ErrorKind::NotFound => break,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to inspect ancestor component {}", current.display())
                });
            }
        }
    }

    Ok(())
}

fn ensure_parent_dir_within_root(root: &Path, relative: &Path) -> anyhow::Result<PathBuf> {
    let parent_relative = relative.parent().unwrap_or(Path::new(""));
    ensure_ancestors_within_root(root, parent_relative)?;
    let parent = root.join(parent_relative);
    let metadata = fs::symlink_metadata(&parent)
        .with_context(|| format!("parent directory {} does not exist", parent.display()))?;
    if metadata.file_type().is_symlink() {
        bail!("parent directory {} is a symlink", parent.display());
    }
    if !metadata.is_dir() {
        bail!("parent path {} is not a directory", parent.display());
    }
    Ok(parent)
}

fn capture_snapshot(path: &Path) -> anyhow::Result<PathSnapshot> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                let target = fs::read_link(path)
                    .with_context(|| format!("failed to read symlink {}", path.display()))?;
                Ok(PathSnapshot::Symlink {
                    target,
                    owner: PosixOwner {
                        uid: metadata.uid(),
                        gid: metadata.gid(),
                    },
                })
            } else if metadata.is_file() {
                let contents = fs::read(path)
                    .with_context(|| format!("failed to snapshot file {}", path.display()))?;
                Ok(PathSnapshot::File {
                    contents,
                    metadata: PosixMetadata {
                        mode: metadata.mode() & 0o7777,
                        owner: PosixOwner {
                            uid: metadata.uid(),
                            gid: metadata.gid(),
                        },
                    },
                })
            } else if metadata.is_dir() {
                Ok(PathSnapshot::Directory {
                    metadata: PosixMetadata {
                        mode: metadata.mode() & 0o7777,
                        owner: PosixOwner {
                            uid: metadata.uid(),
                            gid: metadata.gid(),
                        },
                    },
                })
            } else {
                bail!(
                    "cannot snapshot unsupported file type at {}",
                    path.display()
                )
            }
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(PathSnapshot::Missing),
        Err(err) => Err(err).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn restore_snapshot(path: &Path, snapshot: &PathSnapshot) -> anyhow::Result<()> {
    match snapshot {
        PathSnapshot::Missing => remove_path_if_exists(path),
        PathSnapshot::File { contents, metadata } => {
            ensure_parent_exists_for_restore(path)?;
            remove_path_if_exists(path)?;
            fs::write(path, contents)
                .with_context(|| format!("failed to restore file {}", path.display()))?;
            set_path_mode(path, metadata.mode)?;
            set_path_owner(path, metadata.owner.uid, metadata.owner.gid)?;
            Ok(())
        }
        PathSnapshot::Directory { metadata } => {
            match fs::symlink_metadata(path) {
                Ok(current) if current.is_dir() => {}
                Ok(_) => {
                    remove_path_if_exists(path)?;
                    fs::create_dir_all(path).with_context(|| {
                        format!("failed to recreate directory {}", path.display())
                    })?;
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {
                    fs::create_dir_all(path).with_context(|| {
                        format!("failed to recreate directory {}", path.display())
                    })?;
                }
                Err(err) => {
                    return Err(err)
                        .with_context(|| format!("failed to inspect {}", path.display()));
                }
            }
            set_path_mode(path, metadata.mode)?;
            set_path_owner(path, metadata.owner.uid, metadata.owner.gid)?;
            Ok(())
        }
        PathSnapshot::Symlink { target, owner } => {
            ensure_parent_exists_for_restore(path)?;
            remove_path_if_exists(path)?;
            symlink(target, path).with_context(|| {
                format!(
                    "failed to restore symlink {} -> {}",
                    path.display(),
                    target.display()
                )
            })?;
            set_path_owner(path, owner.uid, owner.gid)?;
            Ok(())
        }
    }
}

fn ensure_parent_exists_for_restore(path: &Path) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path {} has no parent", path.display()))?;
    fs::create_dir_all(parent)
        .with_context(|| format!("failed to create parent directory {}", parent.display()))
}

fn remove_path_if_exists(path: &Path) -> anyhow::Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.is_dir() {
                fs::remove_dir_all(path)
                    .with_context(|| format!("failed to remove directory {}", path.display()))?;
            } else {
                fs::remove_file(path)
                    .with_context(|| format!("failed to remove file {}", path.display()))?;
            }
            Ok(())
        }
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("failed to inspect {}", path.display())),
    }
}

fn set_path_mode(path: &Path, mode: u32) -> anyhow::Result<()> {
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("failed to set mode {mode:o} on {}", path.display()))
}

fn set_path_owner(path: &Path, uid: u32, gid: u32) -> anyhow::Result<()> {
    lchown(path, Some(uid), Some(gid))
        .with_context(|| format!("failed to set owner {uid}:{gid} on {}", path.display()))
}

fn load_payload_archive(payload_path: &Path) -> anyhow::Result<BTreeMap<String, Vec<u8>>> {
    let payload_file = File::open(payload_path)
        .with_context(|| format!("failed to open payload {}", payload_path.display()))?;
    let decoder = zstd::Decoder::new(payload_file)
        .with_context(|| format!("failed to decode zstd payload {}", payload_path.display()))?;
    let mut archive = tar::Archive::new(decoder);
    let mut payload_by_digest = BTreeMap::new();

    let entries = archive
        .entries()
        .with_context(|| format!("failed to read tar payload {}", payload_path.display()))?;

    for entry in entries {
        let mut entry = entry.with_context(|| {
            format!("failed to inspect tar entry in {}", payload_path.display())
        })?;
        if !entry.header().entry_type().is_file() {
            continue;
        }

        let entry_path = entry
            .path()
            .context("failed to read tar entry path")?
            .into_owned();
        let digest = payload_entry_digest(&entry_path)?;

        let mut bytes = Vec::new();
        entry
            .read_to_end(&mut bytes)
            .with_context(|| format!("failed to read payload entry {}", entry_path.display()))?;
        let actual = sha256_bytes_hex(&bytes);
        if actual != digest {
            bail!(
                "payload entry '{}' digest mismatch: expected {}, actual {}",
                entry_path.display(),
                digest,
                actual
            );
        }

        if payload_by_digest.insert(digest.clone(), bytes).is_some() {
            bail!(
                "payload contains duplicate content digest entry '{}'",
                digest
            );
        }
    }

    Ok(payload_by_digest)
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

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::commands::vm_base::BASE_CHANNEL_STABLE;
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tempfile::{TempDir, tempdir};

    const ACTIVE_BASE_ID: &str = "macos-15.3.1-24D70-arm64-64g";
    const PREVIOUS_BASE_ID: &str = "macos-14.6-23G80-arm64-64g";
    const RETIRED_BASE_ID: &str = "macos-13.6.7-22H123-arm64-64g";

    fn make_signing_key_pair() -> Ed25519KeyPair {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate test key");
        Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse test key")
    }

    fn valid_manifest(key_pair: &Ed25519KeyPair, payload: &[u8]) -> PatchBundleManifest {
        let operations = vec![
            PatchOperation::Mkdir {
                path: "/usr/local/libexec".to_string(),
                mode: Some(0o755),
            },
            PatchOperation::WriteFile {
                path: "/usr/local/libexec/vz-agent".to_string(),
                content_digest: sha256_bytes_hex(b"agent-binary"),
                mode: Some(0o755),
            },
            PatchOperation::SetOwner {
                path: "/usr/local/libexec/vz-agent".to_string(),
                uid: 0,
                gid: 0,
            },
            PatchOperation::SetMode {
                path: "/usr/local/libexec/vz-agent".to_string(),
                mode: 0o755,
            },
            PatchOperation::Symlink {
                path: "/usr/local/bin/vz-agent".to_string(),
                target: "/usr/local/libexec/vz-agent".to_string(),
            },
            PatchOperation::DeleteFile {
                path: "/tmp/old-vz-agent".to_string(),
            },
        ];

        PatchBundleManifest {
            bundle_id: "vz-cih-2-1-bundle".to_string(),
            patch_version: "1.0.0".to_string(),
            target_base_id: ACTIVE_BASE_ID.to_string(),
            target_base_fingerprint: BundleBaseFingerprint {
                img_sha256: "1".repeat(64),
                aux_sha256: "2".repeat(64),
                hwmodel_sha256: "3".repeat(64),
                machineid_sha256: "4".repeat(64),
            },
            operations_digest: operations_digest_hex(&operations).expect("hash operations"),
            payload_digest: sha256_bytes_hex(payload),
            post_state_hashes: BTreeMap::from([(
                "/usr/local/bin/vz-agent".to_string(),
                sha256_bytes_hex(b"post-state-vz-agent"),
            )]),
            created_at: "2026-02-24T17:20:00Z".to_string(),
            signing_identity: format!(
                "ed25519:{}",
                base64::engine::general_purpose::STANDARD.encode(key_pair.public_key().as_ref())
            ),
            operations,
        }
    }

    fn write_signed_bundle(
        dir: &Path,
        key_pair: &Ed25519KeyPair,
        manifest: &PatchBundleManifest,
        payload: &[u8],
    ) {
        let manifest_bytes = serde_json::to_vec_pretty(manifest).expect("serialize manifest");
        fs::write(dir.join(MANIFEST_FILE), &manifest_bytes).expect("write manifest");
        fs::write(dir.join(PAYLOAD_FILE), payload).expect("write payload");
        let signature = key_pair.sign(&manifest_bytes);
        fs::write(dir.join(SIGNATURE_FILE), signature.as_ref()).expect("write signature");
    }

    fn create_valid_bundle() -> TempDir {
        let dir = tempdir().expect("create temp dir");
        let key_pair = make_signing_key_pair();
        let payload = b"payload archive bytes";
        let manifest = valid_manifest(&key_pair, payload);
        write_signed_bundle(dir.path(), &key_pair, &manifest, payload);
        dir
    }

    fn build_payload_archive(entries: &[(String, Vec<u8>)]) -> Vec<u8> {
        let mut payload = Vec::new();
        let encoder = zstd::Encoder::new(&mut payload, 0).expect("create zstd encoder");
        let mut builder = tar::Builder::new(encoder);

        let mut sorted = entries.to_vec();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        for (digest, bytes) in sorted {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, digest, bytes.as_slice())
                .expect("append payload entry");
        }

        let encoder = builder.into_inner().expect("finish tar builder");
        encoder.finish().expect("finish zstd encoding");
        payload
    }

    fn write_test_signing_key(path: &Path) {
        let rng = SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate test key");
        fs::write(path, pkcs8.as_ref()).expect("write signing key");
    }

    fn default_test_base_fingerprint() -> BundleBaseFingerprint {
        BundleBaseFingerprint {
            img_sha256: "1".repeat(64),
            aux_sha256: "2".repeat(64),
            hwmodel_sha256: "3".repeat(64),
            machineid_sha256: "4".repeat(64),
        }
    }

    fn build_apply_bundle_with_target(
        root: &Path,
        bundle_id: &str,
        target_base_id: &str,
        target_base_fingerprint: BundleBaseFingerprint,
        operations: Vec<PatchOperation>,
        post_state_hashes: BTreeMap<String, String>,
        payload_entries: &[(String, Vec<u8>)],
    ) -> TempDir {
        let bundle = tempdir().expect("create bundle");
        let key_pair = make_signing_key_pair();
        let payload = build_payload_archive(payload_entries);

        let manifest = PatchBundleManifest {
            bundle_id: bundle_id.to_string(),
            patch_version: "1.0.1".to_string(),
            target_base_id: target_base_id.to_string(),
            target_base_fingerprint,
            operations_digest: operations_digest_hex(&operations).expect("hash operations"),
            payload_digest: sha256_bytes_hex(&payload),
            post_state_hashes,
            created_at: "2026-02-24T18:40:00Z".to_string(),
            signing_identity: format!(
                "ed25519:{}",
                base64::engine::general_purpose::STANDARD.encode(key_pair.public_key().as_ref())
            ),
            operations,
        };

        write_signed_bundle(bundle.path(), &key_pair, &manifest, &payload);
        assert!(root.exists());
        bundle
    }

    fn build_apply_bundle(
        root: &Path,
        operations: Vec<PatchOperation>,
        post_state_hashes: BTreeMap<String, String>,
        payload_entries: &[(String, Vec<u8>)],
    ) -> TempDir {
        build_apply_bundle_with_target(
            root,
            "vz-cih-2-2-apply",
            ACTIVE_BASE_ID,
            default_test_base_fingerprint(),
            operations,
            post_state_hashes,
            payload_entries,
        )
    }

    fn apply_with_test_state(
        bundle: &Path,
        root: &Path,
        patch_state_path: &Path,
    ) -> anyhow::Result<()> {
        apply_with_state_path(
            ApplyArgs {
                bundle: bundle.to_path_buf(),
                root: Some(root.to_path_buf()),
                image: None,
            },
            patch_state_path,
        )
    }

    #[test]
    fn verify_bundle_valid_path() {
        let bundle = create_valid_bundle();
        let manifest = verify_bundle(bundle.path()).expect("bundle should verify");
        assert_eq!(manifest.bundle_id, "vz-cih-2-1-bundle");
        assert_eq!(manifest.patch_version, "1.0.0");
    }

    #[test]
    fn patch_create_builds_signed_bundle_from_inputs() {
        let dir = tempdir().expect("create temp dir");
        let bundle_dir = dir.path().join("created-bundle.vzpatch");
        let payload_dir = dir.path().join("payload");
        fs::create_dir_all(&payload_dir).expect("create payload dir");

        let payload_bytes = b"tool-bytes".to_vec();
        let payload_digest = sha256_bytes_hex(&payload_bytes);
        fs::write(payload_dir.join(&payload_digest), &payload_bytes).expect("write payload entry");

        let operations = vec![
            PatchOperation::WriteFile {
                path: "/opt/tool".to_string(),
                content_digest: payload_digest.clone(),
                mode: Some(0o755),
            },
            PatchOperation::Symlink {
                path: "/usr/local/bin/tool".to_string(),
                target: "/opt/tool".to_string(),
            },
        ];
        let operations_path = dir.path().join("operations.json");
        fs::write(
            &operations_path,
            serde_json::to_vec_pretty(&operations).expect("serialize operations"),
        )
        .expect("write operations file");

        let signing_key_path = dir.path().join("signing-key.pkcs8");
        write_test_signing_key(&signing_key_path);

        create(CreateArgs {
            bundle: bundle_dir.clone(),
            base_id: BASE_CHANNEL_STABLE.to_string(),
            operations: Some(operations_path),
            payload_dir: Some(payload_dir),
            signing_key: signing_key_path,
            post_state_hashes: None,
            patch_version: "2.0.0".to_string(),
            bundle_id: Some("bundle-create-test".to_string()),
            created_at: Some("2026-02-24T19:00:00Z".to_string()),
            write_file: Vec::new(),
            mkdir: Vec::new(),
            symlink: Vec::new(),
            delete_file: Vec::new(),
            set_mode: Vec::new(),
            set_owner: Vec::new(),
        })
        .expect("create should succeed");

        assert!(bundle_dir.join(MANIFEST_FILE).exists());
        assert!(bundle_dir.join(PAYLOAD_FILE).exists());
        assert!(bundle_dir.join(SIGNATURE_FILE).exists());

        let manifest = verify_bundle(&bundle_dir).expect("created bundle should verify");
        assert_eq!(manifest.bundle_id, "bundle-create-test");
        assert_eq!(manifest.patch_version, "2.0.0");
        assert_eq!(manifest.target_base_id, ACTIVE_BASE_ID);
        assert_eq!(manifest.operations, operations);
        assert_eq!(
            manifest
                .post_state_hashes
                .get("/opt/tool")
                .expect("write file hash"),
            &payload_digest
        );
        assert_eq!(
            manifest
                .post_state_hashes
                .get("/usr/local/bin/tool")
                .expect("symlink hash"),
            &sha256_bytes_hex(Path::new("/opt/tool").as_os_str().as_bytes())
        );
    }

    #[test]
    fn patch_create_rejects_payload_digest_mismatch() {
        let dir = tempdir().expect("create temp dir");
        let bundle_dir = dir.path().join("created-bundle.vzpatch");
        let payload_dir = dir.path().join("payload");
        fs::create_dir_all(&payload_dir).expect("create payload dir");

        let expected_digest = sha256_bytes_hex(b"expected");
        fs::write(payload_dir.join(&expected_digest), b"unexpected").expect("write payload entry");

        let operations = vec![PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: expected_digest,
            mode: Some(0o755),
        }];
        let operations_path = dir.path().join("operations.json");
        fs::write(
            &operations_path,
            serde_json::to_vec_pretty(&operations).expect("serialize operations"),
        )
        .expect("write operations file");

        let signing_key_path = dir.path().join("signing-key.pkcs8");
        write_test_signing_key(&signing_key_path);

        let err = create(CreateArgs {
            bundle: bundle_dir,
            base_id: BASE_CHANNEL_STABLE.to_string(),
            operations: Some(operations_path),
            payload_dir: Some(payload_dir),
            signing_key: signing_key_path,
            post_state_hashes: None,
            patch_version: "2.0.0".to_string(),
            bundle_id: None,
            created_at: None,
            write_file: Vec::new(),
            mkdir: Vec::new(),
            symlink: Vec::new(),
            delete_file: Vec::new(),
            set_mode: Vec::new(),
            set_owner: Vec::new(),
        })
        .expect_err("mismatched payload digest should fail");
        assert!(format!("{err:#}").contains("digest mismatch"));
    }

    #[test]
    fn patch_create_inline_mode_builds_bundle_from_write_specs() {
        let dir = tempdir().expect("create temp dir");
        let bundle_dir = dir.path().join("created-inline-bundle.vzpatch");
        let host_file = dir.path().join("vz-agent");
        fs::write(&host_file, b"inline-agent-bytes").expect("write host file");

        create(CreateArgs {
            bundle: bundle_dir.clone(),
            base_id: BASE_CHANNEL_STABLE.to_string(),
            operations: None,
            payload_dir: None,
            signing_key: {
                let path = dir.path().join("signing-key.pkcs8");
                write_test_signing_key(&path);
                path
            },
            post_state_hashes: None,
            patch_version: "2.1.0".to_string(),
            bundle_id: Some("bundle-inline-test".to_string()),
            created_at: Some("2026-02-24T19:30:00Z".to_string()),
            write_file: vec![format!("{}:/opt/vz-agent:755", host_file.display())],
            mkdir: vec!["/opt:755".to_string()],
            symlink: vec!["/usr/local/bin/vz-agent:/opt/vz-agent".to_string()],
            delete_file: Vec::new(),
            set_mode: vec!["/opt/vz-agent:755".to_string()],
            set_owner: Vec::new(),
        })
        .expect("inline create should succeed");

        let manifest = verify_bundle(&bundle_dir).expect("created bundle should verify");
        assert_eq!(manifest.bundle_id, "bundle-inline-test");
        assert_eq!(manifest.patch_version, "2.1.0");
        assert_eq!(manifest.target_base_id, ACTIVE_BASE_ID);
        assert!(manifest.operations.iter().any(|operation| matches!(
            operation,
            PatchOperation::WriteFile { path, .. } if path == "/opt/vz-agent"
        )));
        assert!(manifest.operations.iter().any(|operation| matches!(
            operation,
            PatchOperation::Symlink { path, target }
                if path == "/usr/local/bin/vz-agent" && target == "/opt/vz-agent"
        )));
        assert_eq!(
            manifest
                .post_state_hashes
                .get("/usr/local/bin/vz-agent")
                .expect("symlink hash"),
            &sha256_bytes_hex(Path::new("/opt/vz-agent").as_os_str().as_bytes())
        );
    }

    #[test]
    fn patch_create_rejects_mixed_input_modes() {
        let dir = tempdir().expect("create temp dir");
        let bundle_dir = dir.path().join("mixed-mode-bundle.vzpatch");
        let payload_dir = dir.path().join("payload");
        fs::create_dir_all(&payload_dir).expect("create payload dir");

        let payload_bytes = b"tool-bytes".to_vec();
        let payload_digest = sha256_bytes_hex(&payload_bytes);
        fs::write(payload_dir.join(&payload_digest), &payload_bytes).expect("write payload entry");

        let operations = vec![PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: payload_digest,
            mode: Some(0o755),
        }];
        let operations_path = dir.path().join("operations.json");
        fs::write(
            &operations_path,
            serde_json::to_vec_pretty(&operations).expect("serialize operations"),
        )
        .expect("write operations file");

        let signing_key_path = dir.path().join("signing-key.pkcs8");
        write_test_signing_key(&signing_key_path);

        let err = create(CreateArgs {
            bundle: bundle_dir,
            base_id: BASE_CHANNEL_STABLE.to_string(),
            operations: Some(operations_path),
            payload_dir: Some(payload_dir),
            signing_key: signing_key_path,
            post_state_hashes: None,
            patch_version: "2.0.0".to_string(),
            bundle_id: None,
            created_at: None,
            write_file: vec![format!(
                "{}:/opt/tool:755",
                dir.path().join("some-file").display()
            )],
            mkdir: Vec::new(),
            symlink: Vec::new(),
            delete_file: Vec::new(),
            set_mode: Vec::new(),
            set_owner: Vec::new(),
        })
        .expect_err("mixing input modes should fail");
        assert!(format!("{err:#}").contains("choose one create input mode"));
    }

    #[test]
    fn verify_bundle_signature_mismatch_fails() {
        let bundle = create_valid_bundle();
        fs::write(
            bundle.path().join(SIGNATURE_FILE),
            [0u8; ED25519_SIGNATURE_LEN],
        )
        .expect("overwrite signature");

        let err = verify_bundle(bundle.path()).expect_err("signature mismatch should fail");
        assert!(err.to_string().contains("signature verification failed"));
    }

    #[test]
    fn verify_bundle_payload_digest_mismatch_fails() {
        let bundle = create_valid_bundle();
        fs::write(bundle.path().join(PAYLOAD_FILE), b"tampered payload")
            .expect("overwrite payload");

        let err = verify_bundle(bundle.path()).expect_err("payload digest mismatch should fail");
        assert!(err.to_string().contains("payload digest mismatch"));
    }

    #[test]
    fn verify_bundle_operations_digest_mismatch_fails() {
        let dir = tempdir().expect("create temp dir");
        let key_pair = make_signing_key_pair();
        let payload = b"payload archive bytes";
        let mut manifest = valid_manifest(&key_pair, payload);
        manifest.operations_digest = "0".repeat(64);
        write_signed_bundle(dir.path(), &key_pair, &manifest, payload);

        let err = verify_bundle(dir.path()).expect_err("operations digest mismatch should fail");
        assert!(err.to_string().contains("operations digest mismatch"));
    }

    #[test]
    fn verify_bundle_malformed_manifest_metadata_fails() {
        let dir = tempdir().expect("create temp dir");
        let key_pair = make_signing_key_pair();
        let payload = b"payload archive bytes";
        let mut manifest = valid_manifest(&key_pair, payload);
        manifest.bundle_id = " ".to_string();
        write_signed_bundle(dir.path(), &key_pair, &manifest, payload);

        let err = verify_bundle(dir.path()).expect_err("malformed metadata should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("manifest.bundle_id"));
    }

    #[test]
    fn patch_verify_rejects_unsupported_target_base_descriptor() {
        let dir = tempdir().expect("create temp dir");
        let key_pair = make_signing_key_pair();
        let payload = b"payload archive bytes";
        let mut manifest = valid_manifest(&key_pair, payload);
        manifest.target_base_id = "macos-99.9.9-unknown-arm64-64g".to_string();
        write_signed_bundle(dir.path(), &key_pair, &manifest, payload);

        let err = verify(VerifyArgs {
            bundle: dir.path().to_path_buf(),
        })
        .expect_err("unsupported target base should fail verify");
        let msg = format!("{err:#}");
        assert!(msg.contains("unsupported or retired base"));
        assert!(msg.contains("unknown base selector"));
        assert!(msg.contains("vz vm init --base stable"));
    }

    #[test]
    fn patch_apply_rejects_retired_target_base_descriptor() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("opt")).expect("create parent");

        let bytes = b"patched-bytes".to_vec();
        let digest = sha256_bytes_hex(&bytes);
        let operations = vec![PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: digest.clone(),
            mode: Some(0o755),
        }];
        let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
        let bundle = build_apply_bundle_with_target(
            root.path(),
            "vz-cih-2-2-retired",
            RETIRED_BASE_ID,
            default_test_base_fingerprint(),
            operations,
            post_state_hashes,
            &[(digest, bytes)],
        );

        let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect_err("retired target base should fail apply");
        let msg = format!("{err:#}");
        assert!(msg.contains("unsupported or retired base"));
        assert!(msg.contains("retired base"));
        assert!(msg.contains(RETIRED_BASE_ID));
        assert!(msg.contains("vz vm init --base stable"));
        assert!(msg.contains(BASE_CHANNEL_STABLE));
        assert!(!root.path().join("opt/tool").exists());
    }

    #[test]
    fn patch_state_roundtrip_load_save() {
        let dir = tempdir().expect("create temp dir");
        let state_path = dir.path().join("patch-state.json");

        let mut state = PatchApplyState::default();
        let receipt = PatchApplyReceipt {
            apply_target: "/tmp/target".to_string(),
            apply_target_digest: "a".repeat(64),
            bundle_id: "bundle-a".to_string(),
            target_base_id: "base-a".to_string(),
            target_base_fingerprint: default_test_base_fingerprint(),
            operations_digest: "b".repeat(64),
            payload_digest: "c".repeat(64),
        };
        state.record_receipt(receipt.clone());
        state.save(&state_path).expect("save patch state");

        let loaded = PatchApplyState::load(&state_path).expect("load patch state");
        assert_eq!(loaded, state);
        assert!(loaded.has_receipt(&receipt));
    }

    #[test]
    fn patch_state_malformed_file_is_actionable() {
        let dir = tempdir().expect("create temp dir");
        let state_path = dir.path().join("patch-state.json");
        fs::write(&state_path, "{ not-valid-json").expect("write malformed state");

        let err =
            PatchApplyState::load(&state_path).expect_err("malformed state should return error");
        let message = format!("{err:#}");
        assert!(message.contains("patch state file"));
        assert!(message.contains("is malformed"));
        assert!(message.contains("Move or delete"));
    }

    #[test]
    fn apply_first_apply_writes_receipt() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("opt")).expect("create parent");

        let bytes = b"patched-bytes".to_vec();
        let digest = sha256_bytes_hex(&bytes);
        let operations = vec![PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: digest.clone(),
            mode: Some(0o755),
        }];
        let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
        let bundle = build_apply_bundle(
            root.path(),
            operations,
            post_state_hashes,
            &[(digest.clone(), bytes)],
        );

        apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect("first apply should succeed");

        let manifest = verify_bundle(bundle.path()).expect("manifest should verify");
        let canonical_root = fs::canonicalize(root.path()).expect("canonicalize root");
        let expected_receipt =
            PatchApplyReceipt::from_manifest(&canonical_root, &manifest).expect("build receipt");

        let state = PatchApplyState::load(&patch_state_path).expect("load state");
        assert!(patch_state_path.exists());
        assert_eq!(state.receipts.len(), 1);
        assert!(state.has_receipt(&expected_receipt));
    }

    #[test]
    fn apply_second_identical_apply_noops() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("opt")).expect("create parent");

        let bytes = b"patched-bytes".to_vec();
        let digest = sha256_bytes_hex(&bytes);
        let operations = vec![PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: digest.clone(),
            mode: Some(0o755),
        }];
        let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
        let bundle = build_apply_bundle(
            root.path(),
            operations,
            post_state_hashes,
            &[(digest.clone(), bytes)],
        );

        apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect("first apply should succeed");
        fs::write(root.path().join("opt/tool"), b"drifted").expect("mutate post first apply");
        apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect("second apply should no-op");

        assert_eq!(
            fs::read(root.path().join("opt/tool")).expect("read tool after no-op"),
            b"drifted"
        );
        let state = PatchApplyState::load(&patch_state_path).expect("load state");
        assert_eq!(state.receipts.len(), 1);
    }

    #[test]
    fn apply_receipt_base_mismatch_fails_with_diagnostics() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("opt")).expect("create parent");

        let bytes = b"patched-bytes".to_vec();
        let digest = sha256_bytes_hex(&bytes);
        let operations = vec![PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: digest.clone(),
            mode: Some(0o755),
        }];
        let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), digest.clone())]);
        let first_bundle = build_apply_bundle_with_target(
            root.path(),
            "vz-cih-2-2-apply",
            ACTIVE_BASE_ID,
            default_test_base_fingerprint(),
            operations.clone(),
            post_state_hashes.clone(),
            &[(digest.clone(), bytes.clone())],
        );
        apply_with_test_state(first_bundle.path(), root.path(), &patch_state_path)
            .expect("first apply should succeed");

        let second_bundle = build_apply_bundle_with_target(
            root.path(),
            "vz-cih-2-2-apply",
            PREVIOUS_BASE_ID,
            BundleBaseFingerprint {
                img_sha256: "a".repeat(64),
                aux_sha256: "2".repeat(64),
                hwmodel_sha256: "3".repeat(64),
                machineid_sha256: "4".repeat(64),
            },
            operations,
            post_state_hashes,
            &[(digest, bytes)],
        );
        let err = apply_with_test_state(second_bundle.path(), root.path(), &patch_state_path)
            .expect_err("base mismatch should fail");
        let message = format!("{err:#}");
        assert!(message.contains("patch receipt mismatch"));
        assert!(message.contains("expected(existing receipt):"));
        assert!(message.contains("actual(requested apply):"));
        assert!(message.contains(ACTIVE_BASE_ID));
        assert!(message.contains(PREVIOUS_BASE_ID));
        assert!(message.contains("img_sha256=aaaaaaaa"));
        assert_eq!(
            fs::read(root.path().join("opt/tool")).expect("file should remain from first apply"),
            b"patched-bytes"
        );
    }

    #[test]
    fn apply_successful_deterministic_replay() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("usr/local/bin")).expect("create symlink parent");
        fs::create_dir_all(root.path().join("tmp")).expect("create tmp");
        fs::write(root.path().join("tmp/old-vz-agent"), b"legacy").expect("write old file");

        let owner = fs::metadata(root.path()).expect("root metadata");
        let uid = owner.uid();
        let gid = owner.gid();
        let agent_bytes = b"agent-binary-v2".to_vec();
        let agent_digest = sha256_bytes_hex(&agent_bytes);
        let link_target = "/usr/local/libexec/vz-agent";

        let operations = vec![
            PatchOperation::Mkdir {
                path: "/usr/local/libexec".to_string(),
                mode: Some(0o755),
            },
            PatchOperation::WriteFile {
                path: "/usr/local/libexec/vz-agent".to_string(),
                content_digest: agent_digest.clone(),
                mode: Some(0o700),
            },
            PatchOperation::Symlink {
                path: "/usr/local/bin/vz-agent".to_string(),
                target: link_target.to_string(),
            },
            PatchOperation::SetOwner {
                path: "/usr/local/libexec/vz-agent".to_string(),
                uid,
                gid,
            },
            PatchOperation::SetMode {
                path: "/usr/local/libexec/vz-agent".to_string(),
                mode: 0o755,
            },
            PatchOperation::DeleteFile {
                path: "/tmp/old-vz-agent".to_string(),
            },
        ];
        let post_state_hashes = BTreeMap::from([
            (
                "/usr/local/libexec/vz-agent".to_string(),
                agent_digest.clone(),
            ),
            (
                "/usr/local/bin/vz-agent".to_string(),
                sha256_bytes_hex(Path::new(link_target).as_os_str().as_bytes()),
            ),
        ]);

        let bundle = build_apply_bundle(
            root.path(),
            operations,
            post_state_hashes,
            &[(agent_digest.clone(), agent_bytes.clone())],
        );

        apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect("first apply should succeed");
        apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect("second apply should be deterministic");

        let file_path = root.path().join("usr/local/libexec/vz-agent");
        assert_eq!(fs::read(&file_path).expect("read file"), agent_bytes);
        assert_eq!(
            fs::metadata(&file_path)
                .expect("metadata")
                .permissions()
                .mode()
                & 0o7777,
            0o755
        );
        assert_eq!(
            fs::read_link(root.path().join("usr/local/bin/vz-agent")).expect("read symlink"),
            PathBuf::from(link_target)
        );
        assert!(!root.path().join("tmp/old-vz-agent").exists());
    }

    #[test]
    fn apply_rejects_path_traversal_before_mutation() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("safe")).expect("create safe directory");

        let first_bytes = b"first".to_vec();
        let second_bytes = b"second".to_vec();
        let first_digest = sha256_bytes_hex(&first_bytes);
        let second_digest = sha256_bytes_hex(&second_bytes);

        let operations = vec![
            PatchOperation::WriteFile {
                path: "/safe/ok.txt".to_string(),
                content_digest: first_digest.clone(),
                mode: Some(0o644),
            },
            PatchOperation::WriteFile {
                path: "/safe/../escape.txt".to_string(),
                content_digest: second_digest.clone(),
                mode: Some(0o644),
            },
        ];
        let bundle = build_apply_bundle(
            root.path(),
            operations,
            BTreeMap::new(),
            &[
                (first_digest.clone(), first_bytes),
                (second_digest.clone(), second_bytes),
            ],
        );

        let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect_err("path traversal should fail");
        let message = format!("{err:#}");
        assert!(message.contains("operation[1]"));
        assert!(message.contains("failed safety checks"));
        assert!(!root.path().join("safe/ok.txt").exists());
    }

    #[test]
    fn apply_post_state_hash_mismatch_fails() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("opt")).expect("create parent");

        let bytes = b"patched-bytes".to_vec();
        let digest = sha256_bytes_hex(&bytes);
        let operations = vec![PatchOperation::WriteFile {
            path: "/opt/tool".to_string(),
            content_digest: digest.clone(),
            mode: Some(0o755),
        }];
        let post_state_hashes = BTreeMap::from([("/opt/tool".to_string(), "f".repeat(64))]);
        let bundle = build_apply_bundle(
            root.path(),
            operations,
            post_state_hashes,
            &[(digest.clone(), bytes)],
        );

        let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect_err("post state hash mismatch should fail");
        let message = format!("{err:#}");
        assert!(message.contains("post-state hash mismatch"));
        assert!(
            !root.path().join("opt/tool").exists(),
            "rollback should restore pre-state"
        );
    }

    #[test]
    fn apply_rolls_back_when_operation_fails_mid_sequence() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("data")).expect("create data");
        fs::write(root.path().join("data/original.txt"), b"original").expect("write original");

        let new_bytes = b"new-data".to_vec();
        let new_digest = sha256_bytes_hex(&new_bytes);
        let operations = vec![
            PatchOperation::WriteFile {
                path: "/data/new.txt".to_string(),
                content_digest: new_digest.clone(),
                mode: Some(0o644),
            },
            PatchOperation::DeleteFile {
                path: "/data/original.txt".to_string(),
            },
            PatchOperation::SetMode {
                path: "/data/missing.txt".to_string(),
                mode: 0o644,
            },
        ];
        let bundle = build_apply_bundle(
            root.path(),
            operations,
            BTreeMap::new(),
            &[(new_digest, new_bytes)],
        );

        let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect_err("mid-sequence failure should rollback");
        let message = format!("{err:#}");
        assert!(message.contains("operation[2]"));

        assert_eq!(
            fs::read(root.path().join("data/original.txt")).expect("original restored"),
            b"original"
        );
        assert!(!root.path().join("data/new.txt").exists());
    }

    #[test]
    fn apply_operation_error_includes_index_and_path() {
        let root = tempdir().expect("create root");
        let patch_state_path = root.path().join("patch-state.json");
        fs::create_dir_all(root.path().join("etc")).expect("create etc");

        let operations = vec![PatchOperation::SetMode {
            path: "/etc/does-not-exist".to_string(),
            mode: 0o644,
        }];
        let bundle = build_apply_bundle(root.path(), operations, BTreeMap::new(), &[]);

        let err = apply_with_test_state(bundle.path(), root.path(), &patch_state_path)
            .expect_err("missing file should fail");
        let message = format!("{err:#}");
        assert!(message.contains("operation[0]"));
        assert!(message.contains("/etc/does-not-exist"));
    }

    #[test]
    fn image_delta_roundtrip_matches_target() {
        let dir = tempdir().expect("create temp dir");
        let base = dir.path().join("base.img");
        let target = dir.path().join("target.img");
        let delta = dir.path().join("patch.vzdelta");
        let output = dir.path().join("output.img");

        let mut base_bytes = vec![0u8; 1024 * 1024];
        for (idx, byte) in base_bytes.iter_mut().enumerate() {
            *byte = (idx % 251) as u8;
        }
        let mut target_bytes = base_bytes.clone();
        target_bytes[16_384..16_384 + 1024].fill(0xAA);
        target_bytes[512_000..512_000 + 2048].fill(0x55);
        target_bytes.extend_from_slice(b"tail-bytes");

        fs::write(&base, &base_bytes).expect("write base");
        fs::write(&target, &target_bytes).expect("write target");

        let header =
            create_image_delta_file(&base, &target, &delta, 128 * 1024).expect("create delta");
        assert!(header.changed_chunks > 0);
        let applied_header =
            apply_image_delta_file(&base, &delta, &output).expect("apply delta should succeed");
        assert_eq!(applied_header, header);
        assert_eq!(fs::read(&output).expect("read output"), target_bytes);
    }

    #[test]
    fn image_delta_apply_rejects_base_digest_mismatch() {
        let dir = tempdir().expect("create temp dir");
        let base = dir.path().join("base.img");
        let target = dir.path().join("target.img");
        let delta = dir.path().join("patch.vzdelta");
        let output = dir.path().join("output.img");

        fs::write(&base, b"base-original").expect("write base");
        fs::write(&target, b"base-modified").expect("write target");
        create_image_delta_file(&base, &target, &delta, 64 * 1024).expect("create delta");

        fs::write(&base, b"base-tampered").expect("tamper base");
        let err = apply_image_delta_file(&base, &delta, &output)
            .expect_err("tampered base must fail digest check");
        assert!(format!("{err:#}").contains("base image digest mismatch"));
    }

    #[test]
    fn image_delta_apply_rejects_existing_output_path() {
        let dir = tempdir().expect("create temp dir");
        let base = dir.path().join("base.img");
        let target = dir.path().join("target.img");
        let delta = dir.path().join("patch.vzdelta");
        let output = dir.path().join("output.img");

        fs::write(&base, b"abc").expect("write base");
        fs::write(&target, b"abd").expect("write target");
        fs::write(&output, b"existing").expect("write existing output");
        create_image_delta_file(&base, &target, &delta, 64 * 1024).expect("create delta");

        let err = apply_image_delta_file(&base, &delta, &output)
            .expect_err("existing output should fail");
        assert!(format!("{err:#}").contains("output image already exists"));
    }
}
