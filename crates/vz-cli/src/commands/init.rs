//! `vz init` -- Create a golden macOS VM image from an IPSW.

use std::path::PathBuf;

use clap::Args;
use tracing::info;

use crate::ipsw;

/// Create a golden macOS VM image from an IPSW.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// Path to a local IPSW file. If omitted, detects local installers or downloads.
    #[arg(long)]
    pub ipsw: Option<PathBuf>,

    /// Disk image size (e.g., "64G", "128G").
    #[arg(long, default_value = "64G")]
    pub disk_size: String,

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
        disk_size = %args.disk_size,
        output = %args.output.display(),
        "initializing golden image"
    );

    // Parse disk size
    let disk_size_bytes = ipsw::parse_disk_size(&args.disk_size)?;

    // Pre-flight: check disk space
    let needs_download = args.ipsw.is_none();
    ipsw::check_disk_space(disk_size_bytes, needs_download)?;

    // Resolve IPSW (local-first)
    let resolved = ipsw::resolve(args.ipsw.as_deref()).await?;
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

fn expand_home(path: &std::path::Path) -> std::path::PathBuf {
    let s = path.to_string_lossy();
    if s.starts_with("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return std::path::PathBuf::from(format!("{}{}", home, &s[1..]));
        }
    }
    path.to_path_buf()
}
