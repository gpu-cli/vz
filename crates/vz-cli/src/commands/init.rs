//! `vz init` -- Create a golden macOS VM image from an IPSW.

use anyhow::Context;
use std::path::PathBuf;

use clap::Args;
use tracing::info;

use super::vm_base::{BaseImage, require_unpinned_policy, resolve_base_selector};
use crate::ipsw;

/// Create a golden macOS VM image from an IPSW.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// Path to a local IPSW file. If omitted, detects local installers or downloads.
    #[arg(long, conflicts_with = "base")]
    pub ipsw: Option<PathBuf>,

    /// Disk image size (e.g., "64G", "128G").
    #[arg(long, default_value = "64G", conflicts_with = "base")]
    pub disk_size: String,

    /// Pinned base selector: immutable base ID, `stable`, or `previous`.
    #[arg(long, value_name = "SELECTOR")]
    pub base: Option<String>,

    /// Explicitly allow unpinned init flow (`--ipsw` or auto-discovered/downloaded IPSW).
    #[arg(long, default_value_t = false)]
    pub allow_unpinned: bool,

    /// Output path for the disk image.
    #[arg(long, default_value = "~/.vz/images/base.img")]
    pub output: PathBuf,

    /// Number of CPU cores for the VM.
    #[arg(long, default_value_t = 4)]
    pub cpus: u32,

    /// Memory in GB for the VM.
    #[arg(long, default_value_t = 8)]
    pub memory: u64,
}

pub async fn run(args: InitArgs) -> anyhow::Result<()> {
    info!(
        ipsw = ?args.ipsw,
        base = ?args.base,
        allow_unpinned = args.allow_unpinned,
        disk_size = %args.disk_size,
        output = %args.output.display(),
        "initializing golden image"
    );

    let (resolved, disk_size_bytes) = if let Some(base_selector) = args.base.as_deref() {
        let resolved_base = resolve_base_selector(base_selector)?;
        let base = &resolved_base.base;
        let disk_size_bytes = disk_size_bytes_from_base(base)?;
        let needs_download = !ipsw::pinned_cache_available(&base.ipsw_sha256)?;
        ipsw::check_disk_space(disk_size_bytes, needs_download)?;
        if let Some(channel) = resolved_base.channel.as_deref() {
            println!(
                "Using channel '{}' -> pinned base {}  macOS {} ({})",
                channel, base.base_id, base.macos_version, base.macos_build
            );
        } else {
            println!(
                "Using pinned base: {}  macOS {} ({})",
                base.base_id, base.macos_version, base.macos_build
            );
        }
        let resolved = ipsw::resolve_pinned(&base.ipsw_url, &base.ipsw_sha256).await?;
        (resolved, disk_size_bytes)
    } else {
        require_allow_unpinned(args.allow_unpinned)?;
        print_unpinned_warning();

        let disk_size_bytes = ipsw::parse_disk_size(&args.disk_size)?;
        let needs_download = args.ipsw.is_none();
        ipsw::check_disk_space(disk_size_bytes, needs_download)?;
        let resolved = ipsw::resolve(args.ipsw.as_deref()).await?;
        (resolved, disk_size_bytes)
    };

    info!(
        source = ?resolved.source,
        path = %resolved.path.display(),
        "IPSW resolved"
    );

    // Expand output path
    let output = expand_home(&args.output);
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Run macOS installation
    let ipsw_source = vz::IpswSource::Path(resolved.path.clone());
    let result = vz::install_macos(ipsw_source, &output, disk_size_bytes)
        .await
        .map_err(|e| anyhow::anyhow!("installation failed: {e}"))?;

    info!(
        disk = %result.disk_path.display(),
        hw_model = %result.hardware_model_path.display(),
        machine_id = %result.machine_identifier_path.display(),
        aux = %result.auxiliary_storage_path.display(),
        "macOS installation complete"
    );

    // Apply auto-configuration (skip Setup Assistant, create dev user, install agent)
    // The disk image needs to be mounted first for auto-config
    // For now, we rely on first-boot provisioning via the guest agent
    info!("golden image created successfully");
    println!("Golden image created at: {}", result.disk_path.display());
    println!("Platform identity files:");
    println!(
        "  Hardware model:     {}",
        result.hardware_model_path.display()
    );
    println!(
        "  Machine identifier: {}",
        result.machine_identifier_path.display()
    );
    println!(
        "  Auxiliary storage:  {}",
        result.auxiliary_storage_path.display()
    );
    println!("\nNext steps:");
    println!(
        "  vz run --image {} --name my-vm",
        result.disk_path.display()
    );

    Ok(())
}

fn disk_size_bytes_from_base(base: &BaseImage) -> anyhow::Result<u64> {
    const BYTES_PER_GIB: u64 = 1024 * 1024 * 1024;
    base.disk_size_gb
        .checked_mul(BYTES_PER_GIB)
        .with_context(|| format!("disk_size_gb overflow for base '{}'", base.base_id))
}

fn require_allow_unpinned(allow_unpinned: bool) -> anyhow::Result<()> {
    require_unpinned_policy(allow_unpinned, "init", "vz vm init --base <id>")
}

fn print_unpinned_warning() {
    eprintln!(
        "Warning: running unpinned init mode. This image is not validated against the supported base matrix."
    );
}

fn expand_home(path: &std::path::Path) -> std::path::PathBuf {
    let s = path.to_string_lossy();
    if s.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return std::path::PathBuf::from(format!("{}{}", home, &s[1..]));
        }
    }
    path.to_path_buf()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used, clippy::unwrap_used)]

    use super::*;
    use crate::commands::vm_base::{BaseChannels, BaseFingerprint, BaseMatrix, find_base_or_err};

    fn test_matrix() -> BaseMatrix {
        BaseMatrix {
            version: 1,
            default_base: "base-1".to_string(),
            channels: BaseChannels {
                stable: "base-1".to_string(),
                previous: "base-2".to_string(),
            },
            bases: vec![
                BaseImage {
                    base_id: "base-1".to_string(),
                    macos_version: "15.3.1".to_string(),
                    macos_build: "24D70".to_string(),
                    ipsw_url: "https://example.com/base-1.ipsw".to_string(),
                    ipsw_sha256: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        .to_string(),
                    disk_size_gb: 64,
                    fingerprint: BaseFingerprint {
                        img_sha256:
                            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                                .to_string(),
                        aux_sha256:
                            "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
                                .to_string(),
                        hwmodel_sha256:
                            "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
                                .to_string(),
                        machineid_sha256:
                            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
                                .to_string(),
                    },
                    support: crate::commands::vm_base::BaseSupportPolicy::default(),
                },
                BaseImage {
                    base_id: "base-2".to_string(),
                    macos_version: "14.6".to_string(),
                    macos_build: "23G80".to_string(),
                    ipsw_url: "https://example.com/base-2.ipsw".to_string(),
                    ipsw_sha256: "1111111111111111111111111111111111111111111111111111111111111111"
                        .to_string(),
                    disk_size_gb: 128,
                    fingerprint: BaseFingerprint {
                        img_sha256:
                            "2222222222222222222222222222222222222222222222222222222222222222"
                                .to_string(),
                        aux_sha256:
                            "3333333333333333333333333333333333333333333333333333333333333333"
                                .to_string(),
                        hwmodel_sha256:
                            "4444444444444444444444444444444444444444444444444444444444444444"
                                .to_string(),
                        machineid_sha256:
                            "5555555555555555555555555555555555555555555555555555555555555555"
                                .to_string(),
                    },
                    support: crate::commands::vm_base::BaseSupportPolicy::default(),
                },
            ],
        }
    }

    #[test]
    fn find_base_or_err_returns_pinned_base() {
        let matrix = test_matrix();
        let base = find_base_or_err(&matrix, "base-2").expect("base should resolve");
        assert_eq!(base.macos_build, "23G80");
    }

    #[test]
    fn find_base_or_err_lists_known_bases_when_missing() {
        let matrix = test_matrix();
        let err = find_base_or_err(&matrix, "missing").expect_err("should fail");
        let msg = err.to_string();
        assert!(msg.contains("unknown base_id"));
        assert!(msg.contains("base-1"));
        assert!(msg.contains("base-2"));
    }

    #[test]
    fn disk_size_bytes_from_base_uses_matrix_disk_size() {
        let matrix = test_matrix();
        let base = find_base_or_err(&matrix, "base-2").unwrap();
        let bytes = disk_size_bytes_from_base(base).unwrap();
        assert_eq!(bytes, 128 * 1024 * 1024 * 1024);
    }

    #[test]
    fn require_allow_unpinned_requires_explicit_flag() {
        let err = require_allow_unpinned(false).expect_err("should require flag");
        assert!(err.to_string().contains("--allow-unpinned"));
    }
}
