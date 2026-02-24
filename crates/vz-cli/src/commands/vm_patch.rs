//! `vz vm patch` -- Signed patch bundle verification and apply.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{MetadataExt, PermissionsExt, lchown, symlink};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, anyhow, bail};
use base64::Engine as _;
use clap::{Args, Subcommand};
use ring::signature;
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
    /// Verify bundle signature and digests before apply.
    Verify(VerifyArgs),
    /// Verify, then transactionally apply patch operations.
    Apply(ApplyArgs),
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
    #[arg(long)]
    pub root: PathBuf,
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
        VmPatchCommand::Verify(args) => verify(args),
        VmPatchCommand::Apply(args) => apply(args),
    }
}

fn verify(args: VerifyArgs) -> anyhow::Result<()> {
    let manifest = verify_bundle(&args.bundle)?;
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
    let manifest = verify_bundle(&args.bundle)?;
    let root = fs::canonicalize(&args.root)
        .with_context(|| format!("failed to resolve apply root {}", args.root.display()))?;
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

    let paths = BundlePaths::from_bundle_dir(&args.bundle);
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
    use ring::rand::SystemRandom;
    use ring::signature::{Ed25519KeyPair, KeyPair};
    use tempfile::{TempDir, tempdir};

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
            target_base_id: "macos-15.3.1-24D70-arm64-64g".to_string(),
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
            "macos-15.3.1-24D70-arm64-64g",
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
                root: root.to_path_buf(),
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
            "macos-15.3.1-24D70-arm64-64g",
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
            "macos-15.4.0-25A64-arm64-64g",
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
        assert!(message.contains("macos-15.3.1-24D70-arm64-64g"));
        assert!(message.contains("macos-15.4.0-25A64-arm64-64g"));
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
}
