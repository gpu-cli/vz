//! `vz vm base` -- Supported base matrix commands.

use std::collections::HashSet;
use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};

use crate::ipsw;

/// CI policy env var required to allow unpinned VM automation flows in CI.
pub const ALLOW_UNPINNED_IN_CI_ENV: &str = "VZ_ALLOW_UNPINNED_IN_CI";
pub const BASE_CHANNEL_STABLE: &str = "stable";
pub const BASE_CHANNEL_PREVIOUS: &str = "previous";
const BASE_SUPPORT_STATUS_ACTIVE: &str = "active";
const BASE_SUPPORT_STATUS_RETIRED: &str = "retired";

/// Manage supported VM bases.
#[derive(Args, Debug)]
pub struct VmBaseArgs {
    #[command(subcommand)]
    pub action: VmBaseCommand,
}

/// `vz vm base` subcommands.
#[derive(Subcommand, Debug)]
pub enum VmBaseCommand {
    /// List supported base definitions from the versioned matrix.
    List,

    /// Verify a local base image and sidecars against a pinned base fingerprint.
    Verify(VerifyArgs),
}

/// Arguments for `vz vm base verify`.
#[derive(Args, Debug)]
pub struct VerifyArgs {
    /// Path to the base image (.img).
    #[arg(long)]
    pub image: PathBuf,

    /// Pinned base selector: immutable base ID, `stable`, or `previous`.
    #[arg(long, value_name = "SELECTOR")]
    pub base_id: String,
}

/// Versioned supported base matrix loaded from `config/base-images.json`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaseMatrix {
    pub version: u32,
    pub default_base: String,
    pub channels: BaseChannels,
    pub bases: Vec<BaseImage>,
}

/// Named channels for user-friendly base selection.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaseChannels {
    pub stable: String,
    pub previous: String,
}

/// A single pinned base definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaseImage {
    pub base_id: String,
    pub macos_version: String,
    pub macos_build: String,
    pub ipsw_url: String,
    pub ipsw_sha256: String,
    pub disk_size_gb: u64,
    pub fingerprint: BaseFingerprint,
    #[serde(default)]
    pub support: BaseSupportPolicy,
}

/// Fingerprint hashes for base image identity.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BaseFingerprint {
    pub img_sha256: String,
    pub aux_sha256: String,
    pub hwmodel_sha256: String,
    pub machineid_sha256: String,
}

/// Support lifecycle and fallback policy for a pinned base.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct BaseSupportPolicy {
    #[serde(default)]
    pub status: BaseSupportStatus,
    #[serde(default)]
    pub retired_at: Option<String>,
    #[serde(default)]
    pub replacement_selector: Option<String>,
    #[serde(default)]
    pub replacement_base_id: Option<String>,
}

/// Lifecycle state for a pinned base descriptor.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum BaseSupportStatus {
    #[default]
    Active,
    Retired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedBase {
    pub selector: String,
    pub channel: Option<String>,
    pub base: BaseImage,
}

impl BaseMatrix {
    /// Load the base matrix from the repository default path.
    pub fn load() -> anyhow::Result<Self> {
        Self::load_from_path(default_matrix_path())
    }

    /// Load and validate a base matrix JSON file.
    pub fn load_from_path(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read base matrix from {}", path.display()))?;
        Self::from_json_str(&contents)
            .with_context(|| format!("invalid base matrix at {}", path.display()))
    }

    /// Parse and validate base matrix JSON data.
    pub fn from_json_str(contents: &str) -> anyhow::Result<Self> {
        let matrix: Self =
            serde_json::from_str(contents).context("failed to parse base matrix JSON")?;
        matrix.validate()?;
        Ok(matrix)
    }

    /// Return the configured default base entry.
    pub fn default_base(&self) -> Option<&BaseImage> {
        self.lookup_base(&self.default_base)
    }

    /// Resolve a channel alias to a base identifier.
    pub fn channel_base_id(&self, channel: &str) -> Option<&str> {
        self.channels.lookup(channel)
    }

    /// Look up a base entry by immutable base identifier.
    pub fn lookup_base(&self, base_id: &str) -> Option<&BaseImage> {
        self.bases.iter().find(|base| base.base_id == base_id)
    }

    fn resolve_selector_to_base_id(&self, selector: &str) -> Option<&str> {
        self.lookup_base(selector)
            .map(|base| base.base_id.as_str())
            .or_else(|| self.channel_base_id(selector))
    }

    fn validate(&self) -> anyhow::Result<()> {
        if self.version == 0 {
            bail!("version must be greater than 0");
        }
        validate_non_empty("default_base", &self.default_base)?;

        if self.bases.is_empty() {
            bail!("bases must contain at least one entry");
        }

        let mut seen_base_ids: HashSet<&str> = HashSet::new();
        for (index, base) in self.bases.iter().enumerate() {
            base.validate(index)?;
            if !seen_base_ids.insert(base.base_id.as_str()) {
                bail!("duplicate base_id '{}' found in bases", base.base_id);
            }
        }

        self.channels.validate(&seen_base_ids)?;
        self.validate_support_policies()?;

        if self.default_base().is_none() {
            bail!(
                "default_base '{}' does not match any bases[].base_id",
                self.default_base
            );
        }

        Ok(())
    }

    fn validate_support_policies(&self) -> anyhow::Result<()> {
        for (index, base) in self.bases.iter().enumerate() {
            base.support.validate(index, self, base)?;
        }
        Ok(())
    }
}

impl BaseImage {
    fn validate(&self, index: usize) -> anyhow::Result<()> {
        let prefix = format!("bases[{index}]");
        validate_non_empty(&format!("{prefix}.base_id"), &self.base_id)?;
        validate_non_empty(&format!("{prefix}.macos_version"), &self.macos_version)?;
        validate_non_empty(&format!("{prefix}.macos_build"), &self.macos_build)?;
        validate_non_empty(&format!("{prefix}.ipsw_url"), &self.ipsw_url)?;
        validate_non_empty(&format!("{prefix}.ipsw_sha256"), &self.ipsw_sha256)?;
        validate_url_https(&format!("{prefix}.ipsw_url"), &self.ipsw_url)?;
        validate_sha256(&format!("{prefix}.ipsw_sha256"), &self.ipsw_sha256)?;

        if self.disk_size_gb == 0 {
            bail!("{prefix}.disk_size_gb must be greater than 0");
        }

        self.fingerprint.validate(index)?;
        Ok(())
    }
}

impl BaseSupportPolicy {
    fn validate(&self, index: usize, matrix: &BaseMatrix, owner: &BaseImage) -> anyhow::Result<()> {
        let prefix = format!("bases[{index}].support");
        match self.status {
            BaseSupportStatus::Active => {
                if self.retired_at.is_some() {
                    bail!(
                        "{prefix}.retired_at must be omitted when status is '{BASE_SUPPORT_STATUS_ACTIVE}'"
                    );
                }
                if self.replacement_selector.is_some() {
                    bail!(
                        "{prefix}.replacement_selector must be omitted when status is '{BASE_SUPPORT_STATUS_ACTIVE}'"
                    );
                }
                if self.replacement_base_id.is_some() {
                    bail!(
                        "{prefix}.replacement_base_id must be omitted when status is '{BASE_SUPPORT_STATUS_ACTIVE}'"
                    );
                }
            }
            BaseSupportStatus::Retired => {
                let retired_at = self.retired_at.as_deref().ok_or_else(|| {
                    anyhow::anyhow!(
                        "{prefix}.retired_at must be set when status is '{BASE_SUPPORT_STATUS_RETIRED}'"
                    )
                })?;
                validate_non_empty(&format!("{prefix}.retired_at"), retired_at)?;

                if let Some(selector) = self.replacement_selector.as_deref() {
                    validate_non_empty(&format!("{prefix}.replacement_selector"), selector)?;
                    let Some(target_base_id) = matrix.resolve_selector_to_base_id(selector) else {
                        bail!(
                            "{prefix}.replacement_selector '{}' does not match a base_id or channel alias ({BASE_CHANNEL_STABLE}, {BASE_CHANNEL_PREVIOUS})",
                            selector
                        );
                    };
                    let replacement = matrix.lookup_base(target_base_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "{prefix}.replacement_selector '{}' resolves to unknown base '{}'",
                            selector,
                            target_base_id
                        )
                    })?;
                    if replacement.support.status == BaseSupportStatus::Retired {
                        bail!(
                            "{prefix}.replacement_selector '{}' must resolve to an active base",
                            selector
                        );
                    }
                    if let Some(explicit_base_id) = self.replacement_base_id.as_deref() {
                        validate_non_empty(
                            &format!("{prefix}.replacement_base_id"),
                            explicit_base_id,
                        )?;
                        if explicit_base_id != target_base_id {
                            bail!(
                                "{prefix}.replacement_selector '{}' resolves to '{}' but replacement_base_id is '{}'",
                                selector,
                                target_base_id,
                                explicit_base_id
                            );
                        }
                    }
                }

                if let Some(base_id) = self.replacement_base_id.as_deref() {
                    validate_non_empty(&format!("{prefix}.replacement_base_id"), base_id)?;
                    let replacement = matrix.lookup_base(base_id).ok_or_else(|| {
                        anyhow::anyhow!(
                            "{prefix}.replacement_base_id '{}' does not match any bases[].base_id",
                            base_id
                        )
                    })?;
                    if replacement.support.status == BaseSupportStatus::Retired {
                        bail!(
                            "{prefix}.replacement_base_id '{}' must point to an active base",
                            base_id
                        );
                    }
                }

                if self.replacement_selector.is_none() && self.replacement_base_id.is_none() {
                    bail!(
                        "{prefix} must include replacement_selector or replacement_base_id when status is '{BASE_SUPPORT_STATUS_RETIRED}'"
                    );
                }

                if let Some(replacement_base_id) = self.replacement_base_id.as_deref() {
                    if replacement_base_id == owner.base_id {
                        bail!(
                            "{prefix}.replacement_base_id must not equal retired base_id '{}'",
                            owner.base_id
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn status_label(&self) -> &'static str {
        match self.status {
            BaseSupportStatus::Active => BASE_SUPPORT_STATUS_ACTIVE,
            BaseSupportStatus::Retired => BASE_SUPPORT_STATUS_RETIRED,
        }
    }
}

impl BaseChannels {
    fn lookup(&self, channel: &str) -> Option<&str> {
        match channel {
            BASE_CHANNEL_STABLE => Some(self.stable.as_str()),
            BASE_CHANNEL_PREVIOUS => Some(self.previous.as_str()),
            _ => None,
        }
    }

    fn validate(&self, seen_base_ids: &HashSet<&str>) -> anyhow::Result<()> {
        validate_non_empty("channels.stable", &self.stable)?;
        validate_non_empty("channels.previous", &self.previous)?;

        if !seen_base_ids.contains(self.stable.as_str()) {
            bail!(
                "channels.stable '{}' does not match any bases[].base_id",
                self.stable
            );
        }
        if !seen_base_ids.contains(self.previous.as_str()) {
            bail!(
                "channels.previous '{}' does not match any bases[].base_id",
                self.previous
            );
        }
        Ok(())
    }
}

impl BaseFingerprint {
    fn validate(&self, index: usize) -> anyhow::Result<()> {
        let prefix = format!("bases[{index}].fingerprint");
        validate_non_empty(&format!("{prefix}.img_sha256"), &self.img_sha256)?;
        validate_non_empty(&format!("{prefix}.aux_sha256"), &self.aux_sha256)?;
        validate_non_empty(&format!("{prefix}.hwmodel_sha256"), &self.hwmodel_sha256)?;
        validate_non_empty(
            &format!("{prefix}.machineid_sha256"),
            &self.machineid_sha256,
        )?;
        validate_sha256(&format!("{prefix}.img_sha256"), &self.img_sha256)?;
        validate_sha256(&format!("{prefix}.aux_sha256"), &self.aux_sha256)?;
        validate_sha256(&format!("{prefix}.hwmodel_sha256"), &self.hwmodel_sha256)?;
        validate_sha256(
            &format!("{prefix}.machineid_sha256"),
            &self.machineid_sha256,
        )?;
        Ok(())
    }
}

fn validate_non_empty(field: &str, value: &str) -> anyhow::Result<()> {
    if value.trim().is_empty() {
        bail!("{field} must not be empty");
    }
    Ok(())
}

fn validate_sha256(field: &str, value: &str) -> anyhow::Result<()> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("{field} must be a 64-character hex SHA-256 digest");
    }
    Ok(())
}

fn validate_url_https(field: &str, value: &str) -> anyhow::Result<()> {
    let url = reqwest::Url::parse(value)
        .with_context(|| format!("{field} must be a valid absolute URL"))?;
    if url.scheme() != "https" {
        bail!("{field} must use https scheme");
    }
    Ok(())
}

fn default_matrix_path() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("config")
        .join("base-images.json")
}

/// Require explicit unpinned policy flags before running unpinned automation.
pub fn require_unpinned_policy(
    allow_unpinned: bool,
    flow: &str,
    pinned_hint: &str,
) -> anyhow::Result<()> {
    require_unpinned_policy_with_context(
        allow_unpinned,
        flow,
        pinned_hint,
        current_unpinned_policy_context(),
    )
}

/// Resolve a base selector (`base_id`, `stable`, `previous`) from matrix metadata.
pub fn resolve_base_selector(selector: &str) -> anyhow::Result<ResolvedBase> {
    let matrix = BaseMatrix::load()?;
    let resolved = resolve_base_selector_or_err(&matrix, selector)?;
    Ok(ResolvedBase {
        selector: selector.to_string(),
        channel: resolved.channel.map(|channel| channel.to_string()),
        base: resolved.base.clone(),
    })
}

/// Verify a local image and sidecars against the pinned fingerprint for a selector.
pub fn verify_image_for_base_id(image_path: &Path, selector: &str) -> anyhow::Result<ResolvedBase> {
    let resolved = resolve_base_selector(selector)?;
    verify_base_image(image_path, &resolved.base)?;
    Ok(resolved)
}

#[derive(Debug, Clone, Copy)]
struct UnpinnedPolicyContext {
    in_ci: bool,
    allow_unpinned_in_ci: bool,
}

fn require_unpinned_policy_with_context(
    allow_unpinned: bool,
    flow: &str,
    pinned_hint: &str,
    context: UnpinnedPolicyContext,
) -> anyhow::Result<()> {
    if !allow_unpinned {
        bail!(
            "unpinned {flow} path requires explicit --allow-unpinned.\n\
             Use `{pinned_hint}` for pinned verification."
        );
    }

    if context.in_ci && !context.allow_unpinned_in_ci {
        bail!(
            "unpinned {flow} path is blocked in CI by policy.\n\
             Use `{pinned_hint}` for pinned verification, or set {ALLOW_UNPINNED_IN_CI_ENV}=1 to allow unpinned {flow} in CI."
        );
    }

    Ok(())
}

fn current_unpinned_policy_context() -> UnpinnedPolicyContext {
    UnpinnedPolicyContext {
        in_ci: env_var_is_enabled("CI"),
        allow_unpinned_in_ci: env_var_is_truthy(ALLOW_UNPINNED_IN_CI_ENV),
    }
}

fn env_var_is_enabled(name: &str) -> bool {
    let Ok(value) = env::var(name) else {
        return false;
    };
    let value = value.trim();
    if value.is_empty() {
        return false;
    }
    let lower = value.to_ascii_lowercase();
    !matches!(lower.as_str(), "0" | "false" | "no" | "off")
}

fn env_var_is_truthy(name: &str) -> bool {
    let Ok(value) = env::var(name) else {
        return false;
    };
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ResolvedBaseRef<'a> {
    pub base: &'a BaseImage,
    pub channel: Option<&'static str>,
}

pub(crate) fn resolve_base_selector_or_err<'a>(
    matrix: &'a BaseMatrix,
    selector: &str,
) -> anyhow::Result<ResolvedBaseRef<'a>> {
    let resolved = if matrix.lookup_base(selector).is_some() {
        let base = find_base_or_err(matrix, selector)?;
        ResolvedBaseRef {
            base,
            channel: None,
        }
    } else {
        let mut resolved = None;
        for channel in [BASE_CHANNEL_STABLE, BASE_CHANNEL_PREVIOUS] {
            if selector == channel {
                let base_id = matrix.channel_base_id(channel).with_context(|| {
                    format!("base matrix missing required channel mapping for '{channel}'")
                })?;
                let base = find_base_or_err(matrix, base_id).with_context(|| {
                    format!(
                        "base matrix channel '{channel}' points to unknown base_id '{}'",
                        base_id
                    )
                })?;
                resolved = Some(ResolvedBaseRef {
                    base,
                    channel: Some(channel),
                });
                break;
            }
        }
        resolved.ok_or_else(|| {
            anyhow::anyhow!(
                "unknown base selector '{selector}'. Use a base ID or channel alias ({BASE_CHANNEL_STABLE}, {BASE_CHANNEL_PREVIOUS}). Known base IDs: {}.\n\
                 Fallback: run `vz vm init --base {BASE_CHANNEL_STABLE}` and retry with a supported selector.",
                known_base_ids(matrix)
            )
        })?
    };

    ensure_resolved_base_is_supported(matrix, selector, resolved)
}

/// Entry point for `vz vm base`.
pub async fn run(args: VmBaseArgs) -> anyhow::Result<()> {
    match args.action {
        VmBaseCommand::List => list_bases(),
        VmBaseCommand::Verify(args) => verify_base(args),
    }
}

fn list_bases() -> anyhow::Result<()> {
    let matrix = BaseMatrix::load()?;
    println!("Matrix version: {}", matrix.version);
    println!("Default base: {}", matrix.default_base);
    println!("Channels:");
    println!("  {BASE_CHANNEL_STABLE} -> {}", matrix.channels.stable);
    println!("  {BASE_CHANNEL_PREVIOUS} -> {}", matrix.channels.previous);
    println!();

    for base in &matrix.bases {
        let marker = if base.base_id == matrix.default_base {
            "*"
        } else {
            " "
        };
        println!(
            "{marker} {}  macOS {} ({})  {}G  [{}]",
            base.base_id,
            base.macos_version,
            base.macos_build,
            base.disk_size_gb,
            base.support.status_label()
        );
        if base.support.status == BaseSupportStatus::Retired {
            println!(
                "    retired_at: {}",
                base.support.retired_at.as_deref().unwrap_or("unknown")
            );
        }
    }

    Ok(())
}

fn verify_base(args: VerifyArgs) -> anyhow::Result<()> {
    let resolved = verify_image_for_base_id(&args.image, &args.base_id)?;
    if let Some(channel) = resolved.channel.as_deref() {
        println!(
            "Base fingerprint verified for channel '{}' (resolved to '{}') using image {}",
            channel,
            resolved.base.base_id,
            args.image.display()
        );
    } else {
        println!(
            "Base fingerprint verified for '{}' using image {}",
            resolved.base.base_id,
            args.image.display()
        );
    }
    Ok(())
}

pub(crate) fn find_base_or_err<'a>(
    matrix: &'a BaseMatrix,
    base_id: &str,
) -> anyhow::Result<&'a BaseImage> {
    if let Some(base) = matrix.lookup_base(base_id) {
        return Ok(base);
    }

    bail!(
        "unknown base_id '{base_id}'. Known base IDs: {}",
        known_base_ids(matrix)
    );
}

fn ensure_resolved_base_is_supported<'a>(
    matrix: &'a BaseMatrix,
    selector: &str,
    resolved: ResolvedBaseRef<'a>,
) -> anyhow::Result<ResolvedBaseRef<'a>> {
    if resolved.base.support.status == BaseSupportStatus::Active {
        return Ok(resolved);
    }

    let base = resolved.base;
    let retired_at = base.support.retired_at.as_deref().unwrap_or("unknown");
    let mut message = format!(
        "base selector '{}' resolves to retired base '{}' (retired_at: {}).\n\
         Fallback: re-init on {BASE_CHANNEL_STABLE}: `vz vm init --base {BASE_CHANNEL_STABLE}`.",
        selector, base.base_id, retired_at
    );

    let replacement_selector = base.support.replacement_selector.as_deref();
    let replacement_base_id = base.support.replacement_base_id.as_deref().or_else(|| {
        replacement_selector.and_then(|candidate| matrix.resolve_selector_to_base_id(candidate))
    });

    match (replacement_selector, replacement_base_id) {
        (Some(replacement_selector), Some(replacement_base_id)) => {
            message.push_str(&format!(
                "\nRecommended replacement: selector '{}' -> base '{}'.",
                replacement_selector, replacement_base_id
            ));
        }
        (Some(replacement_selector), None) => {
            message.push_str(&format!(
                "\nRecommended replacement selector: '{}'.",
                replacement_selector
            ));
        }
        (None, Some(replacement_base_id)) => {
            message.push_str(&format!(
                "\nRecommended replacement base: '{}'.",
                replacement_base_id
            ));
        }
        (None, None) => {}
    }

    if let Some(channel) = resolved.channel {
        message.push_str(&format!(
            "\nChannel '{}' now points to a retired base; switch to '{BASE_CHANNEL_STABLE}' or another supported selector.",
            channel
        ));
    }

    bail!("{message}");
}

fn known_base_ids(matrix: &BaseMatrix) -> String {
    matrix
        .bases
        .iter()
        .map(|base| base.base_id.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn verify_base_image(image_path: &Path, base: &BaseImage) -> anyhow::Result<()> {
    let fingerprint = &base.fingerprint;
    let paths = base_artifact_paths(image_path);
    let artifacts = [
        ("img_sha256", &paths[0], fingerprint.img_sha256.as_str()),
        ("aux_sha256", &paths[1], fingerprint.aux_sha256.as_str()),
        (
            "hwmodel_sha256",
            &paths[2],
            fingerprint.hwmodel_sha256.as_str(),
        ),
        (
            "machineid_sha256",
            &paths[3],
            fingerprint.machineid_sha256.as_str(),
        ),
    ];

    let mut mismatches = Vec::new();
    for (field, path, expected) in artifacts {
        let expected = expected.to_ascii_lowercase();
        let actual = ipsw::sha256_file(path)
            .with_context(|| format!("failed to hash {field} from {}", path.display()))?;
        if actual != expected {
            mismatches.push(format!(
                "{field} ({}): expected {expected}, actual {actual}",
                path.display()
            ));
        }
    }

    if mismatches.is_empty() {
        return Ok(());
    }

    bail!(
        "fingerprint mismatch for base_id '{}':\n{}",
        base.base_id,
        mismatches.join("\n")
    );
}

fn base_artifact_paths(image_path: &Path) -> [PathBuf; 4] {
    [
        image_path.to_path_buf(),
        image_path.with_extension("aux"),
        image_path.with_extension("hwmodel"),
        image_path.with_extension("machineid"),
    ]
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use serde_json::{Value, json};
    use std::io::Write;
    use tempfile::{NamedTempFile, TempDir, tempdir};

    const BASE_ID_1: &str = "macos-15.3.1-24D70-arm64-64g";
    const BASE_ID_2: &str = "macos-14.6-23G80-arm64-64g";

    fn valid_matrix_json() -> Value {
        json!({
            "version": 1,
            "default_base": BASE_ID_1,
            "channels": {
                "stable": BASE_ID_1,
                "previous": BASE_ID_2
            },
            "bases": [
                {
                    "base_id": BASE_ID_1,
                    "macos_version": "15.3.1",
                    "macos_build": "24D70",
                    "ipsw_url": "https://updates.cdn-apple.com/UniversalMac_15.3.1_24D70_Restore.ipsw",
                    "ipsw_sha256": "1111111111111111111111111111111111111111111111111111111111111111",
                    "disk_size_gb": 64,
                    "fingerprint": {
                        "img_sha256": "2222222222222222222222222222222222222222222222222222222222222222",
                        "aux_sha256": "3333333333333333333333333333333333333333333333333333333333333333",
                        "hwmodel_sha256": "4444444444444444444444444444444444444444444444444444444444444444",
                        "machineid_sha256": "5555555555555555555555555555555555555555555555555555555555555555"
                    },
                    "support": {
                        "status": "active"
                    }
                },
                {
                    "base_id": BASE_ID_2,
                    "macos_version": "14.6",
                    "macos_build": "23G80",
                    "ipsw_url": "https://updates.cdn-apple.com/UniversalMac_14.6_23G80_Restore.ipsw",
                    "ipsw_sha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "disk_size_gb": 64,
                    "fingerprint": {
                        "img_sha256": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                        "aux_sha256": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
                        "hwmodel_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
                        "machineid_sha256": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                    },
                    "support": {
                        "status": "active"
                    }
                }
            ]
        })
    }

    fn matrix_with_retired_base() -> Value {
        let mut json = valid_matrix_json();
        json["bases"][1]["support"] = json!({
            "status": "retired",
            "retired_at": "2026-01-31",
            "replacement_selector": BASE_CHANNEL_STABLE,
            "replacement_base_id": BASE_ID_1
        });
        json
    }

    fn load_error(json: Value) -> String {
        BaseMatrix::from_json_str(&json.to_string())
            .expect_err("expected validation error")
            .to_string()
    }

    fn write_base_artifacts() -> (TempDir, PathBuf) {
        let dir = tempdir().expect("create temp dir");
        let image_path = dir.path().join("base.img");
        std::fs::write(&image_path, b"img").expect("write image");
        std::fs::write(image_path.with_extension("aux"), b"aux").expect("write aux");
        std::fs::write(image_path.with_extension("hwmodel"), b"hwmodel").expect("write hwmodel");
        std::fs::write(image_path.with_extension("machineid"), b"machineid")
            .expect("write machineid");
        (dir, image_path)
    }

    fn make_base(fingerprint: BaseFingerprint) -> BaseImage {
        BaseImage {
            base_id: BASE_ID_1.to_string(),
            macos_version: "15.3.1".to_string(),
            macos_build: "24D70".to_string(),
            ipsw_url: "https://updates.cdn-apple.com/restore.ipsw".to_string(),
            ipsw_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                .to_string(),
            disk_size_gb: 64,
            fingerprint,
            support: BaseSupportPolicy::default(),
        }
    }

    fn fingerprint_for(image_path: &Path) -> BaseFingerprint {
        BaseFingerprint {
            img_sha256: ipsw::sha256_file(image_path).expect("hash img"),
            aux_sha256: ipsw::sha256_file(&image_path.with_extension("aux")).expect("hash aux"),
            hwmodel_sha256: ipsw::sha256_file(&image_path.with_extension("hwmodel"))
                .expect("hash hwmodel"),
            machineid_sha256: ipsw::sha256_file(&image_path.with_extension("machineid"))
                .expect("hash machineid"),
        }
    }

    #[test]
    fn parse_and_lookup_base_matrix() {
        let matrix = BaseMatrix::from_json_str(&valid_matrix_json().to_string())
            .expect("valid matrix should parse");
        assert_eq!(matrix.version, 1);
        assert_eq!(matrix.bases.len(), 2);
        assert_eq!(
            matrix.default_base().map(|base| base.base_id.as_str()),
            Some(BASE_ID_1)
        );
        assert_eq!(
            matrix
                .lookup_base(BASE_ID_2)
                .map(|base| base.macos_build.as_str()),
            Some("23G80")
        );
        assert_eq!(matrix.channel_base_id(BASE_CHANNEL_STABLE), Some(BASE_ID_1));
        assert_eq!(
            matrix.channel_base_id(BASE_CHANNEL_PREVIOUS),
            Some(BASE_ID_2)
        );
        assert_eq!(
            matrix
                .lookup_base(BASE_ID_1)
                .map(|base| base.support.status),
            Some(BaseSupportStatus::Active)
        );
        assert_eq!(
            matrix
                .lookup_base(BASE_ID_2)
                .map(|base| base.support.status),
            Some(BaseSupportStatus::Active)
        );
        assert!(matrix.lookup_base("missing-base-id").is_none());
    }

    #[test]
    fn load_from_path_validates_and_parses() {
        let mut file = NamedTempFile::new().expect("create temp file");
        file.write_all(valid_matrix_json().to_string().as_bytes())
            .expect("write matrix file");

        let matrix = BaseMatrix::load_from_path(file.path()).expect("matrix should load");
        assert_eq!(
            matrix.default_base().map(|base| base.base_id.as_str()),
            Some(BASE_ID_1)
        );
    }

    #[test]
    fn rejects_duplicate_base_id() {
        let mut json = valid_matrix_json();
        json["bases"][1]["base_id"] = json["bases"][0]["base_id"].clone();
        let err = load_error(json);
        assert!(err.contains("duplicate base_id"));
    }

    #[test]
    fn rejects_missing_default_base_reference() {
        let mut json = valid_matrix_json();
        json["default_base"] = json!("missing-base");
        let err = load_error(json);
        assert!(err.contains("default_base"));
    }

    #[test]
    fn rejects_missing_channel_target_reference() {
        let mut json = valid_matrix_json();
        json["channels"]["previous"] = json!("missing-base");
        let err = load_error(json);
        assert!(err.contains("channels.previous"));
    }

    #[test]
    fn rejects_invalid_sha256_values() {
        let mut json = valid_matrix_json();
        json["bases"][0]["fingerprint"]["img_sha256"] = json!("not-a-sha");
        let err = load_error(json);
        assert!(err.contains("img_sha256"));
    }

    #[test]
    fn rejects_invalid_url_scheme() {
        let mut json = valid_matrix_json();
        json["bases"][0]["ipsw_url"] = json!("ftp://updates.cdn-apple.com/restore.ipsw");
        let err = load_error(json);
        assert!(err.contains("https scheme"));
    }

    #[test]
    fn rejects_empty_required_fields() {
        let mut json = valid_matrix_json();
        json["bases"][0]["macos_build"] = json!("  ");
        let err = load_error(json);
        assert!(err.contains("macos_build"));
    }

    #[test]
    fn rejects_retired_base_without_replacement_guidance() {
        let mut json = valid_matrix_json();
        json["bases"][1]["support"] = json!({
            "status": "retired",
            "retired_at": "2026-01-31"
        });
        let err = load_error(json);
        assert!(err.contains("replacement_selector"));
    }

    #[test]
    fn verify_base_image_matches_expected_fingerprint() {
        let (_dir, image_path) = write_base_artifacts();
        let base = make_base(fingerprint_for(&image_path));
        verify_base_image(&image_path, &base).expect("fingerprint should match");
    }

    #[test]
    fn verify_base_image_reports_expected_and_actual_mismatch() {
        let (_dir, image_path) = write_base_artifacts();
        let mut fingerprint = fingerprint_for(&image_path);
        fingerprint.aux_sha256 =
            "0000000000000000000000000000000000000000000000000000000000000000".to_string();
        let base = make_base(fingerprint);

        let err = verify_base_image(&image_path, &base).expect_err("should fail fingerprint check");
        let msg = err.to_string();
        assert!(msg.contains("aux_sha256"));
        assert!(msg.contains("expected"));
        assert!(msg.contains("actual"));
        assert!(msg.contains("base_id"));
    }

    #[test]
    fn verify_base_image_derives_expected_sidecar_paths() {
        let image_path = PathBuf::from("/tmp/base.img");
        let artifacts = base_artifact_paths(&image_path);
        assert_eq!(artifacts[0], PathBuf::from("/tmp/base.img"));
        assert_eq!(artifacts[1], PathBuf::from("/tmp/base.aux"));
        assert_eq!(artifacts[2], PathBuf::from("/tmp/base.hwmodel"));
        assert_eq!(artifacts[3], PathBuf::from("/tmp/base.machineid"));
    }

    #[test]
    fn resolve_base_selector_accepts_direct_base_id() {
        let matrix = BaseMatrix::from_json_str(&valid_matrix_json().to_string())
            .expect("valid matrix should parse");
        let resolved =
            resolve_base_selector_or_err(&matrix, BASE_ID_2).expect("base should resolve");
        assert_eq!(resolved.base.base_id, BASE_ID_2);
        assert_eq!(resolved.channel, None);
    }

    #[test]
    fn resolve_base_selector_accepts_channel_aliases() {
        let matrix = BaseMatrix::from_json_str(&valid_matrix_json().to_string())
            .expect("valid matrix should parse");

        let stable = resolve_base_selector_or_err(&matrix, BASE_CHANNEL_STABLE)
            .expect("stable should resolve");
        assert_eq!(stable.base.base_id, BASE_ID_1);
        assert_eq!(stable.channel, Some(BASE_CHANNEL_STABLE));

        let previous = resolve_base_selector_or_err(&matrix, BASE_CHANNEL_PREVIOUS)
            .expect("previous should resolve");
        assert_eq!(previous.base.base_id, BASE_ID_2);
        assert_eq!(previous.channel, Some(BASE_CHANNEL_PREVIOUS));
    }

    #[test]
    fn resolve_base_selector_rejects_retired_base_with_fallback_guidance() {
        let matrix = BaseMatrix::from_json_str(&matrix_with_retired_base().to_string())
            .expect("matrix should parse");
        let err = resolve_base_selector_or_err(&matrix, BASE_ID_2).expect_err("should fail");
        let msg = err.to_string();
        assert!(msg.contains("retired base"));
        assert!(msg.contains(BASE_ID_2));
        assert!(msg.contains("vz vm init --base stable"));
        assert!(msg.contains("Recommended replacement"));
        assert!(msg.contains(BASE_CHANNEL_STABLE));
        assert!(msg.contains(BASE_ID_1));
    }

    #[test]
    fn resolve_base_selector_reports_aliases_and_known_ids_when_missing() {
        let matrix = BaseMatrix::from_json_str(&valid_matrix_json().to_string())
            .expect("valid matrix should parse");
        let err = resolve_base_selector_or_err(&matrix, "canary").expect_err("should fail");
        let msg = err.to_string();
        assert!(msg.contains("unknown base selector"));
        assert!(msg.contains(BASE_CHANNEL_STABLE));
        assert!(msg.contains(BASE_CHANNEL_PREVIOUS));
        assert!(msg.contains(BASE_ID_1));
        assert!(msg.contains(BASE_ID_2));
        assert!(msg.contains("vz vm init --base stable"));
    }

    #[test]
    fn require_unpinned_policy_requires_flag() {
        let err = require_unpinned_policy_with_context(
            false,
            "provision",
            "vz vm provision --base-id <id>",
            UnpinnedPolicyContext {
                in_ci: false,
                allow_unpinned_in_ci: false,
            },
        )
        .expect_err("should require --allow-unpinned");
        assert!(err.to_string().contains("--allow-unpinned"));
    }

    #[test]
    fn require_unpinned_policy_allows_local_unpinned() {
        require_unpinned_policy_with_context(
            true,
            "init",
            "vz vm init --base <id>",
            UnpinnedPolicyContext {
                in_ci: false,
                allow_unpinned_in_ci: false,
            },
        )
        .expect("local unpinned flow should be allowed with explicit flag");
    }

    #[test]
    fn require_unpinned_policy_blocks_ci_without_override() {
        let err = require_unpinned_policy_with_context(
            true,
            "init",
            "vz vm init --base <id>",
            UnpinnedPolicyContext {
                in_ci: true,
                allow_unpinned_in_ci: false,
            },
        )
        .expect_err("CI unpinned flow should require policy env override");
        let msg = err.to_string();
        assert!(msg.contains("blocked in CI"));
        assert!(msg.contains(ALLOW_UNPINNED_IN_CI_ENV));
    }

    #[test]
    fn require_unpinned_policy_allows_ci_with_override() {
        require_unpinned_policy_with_context(
            true,
            "provision",
            "vz vm provision --base-id <id>",
            UnpinnedPolicyContext {
                in_ci: true,
                allow_unpinned_in_ci: true,
            },
        )
        .expect("CI unpinned flow should be allowed with explicit override");
    }
}
