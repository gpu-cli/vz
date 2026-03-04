//! `vz vm` -- VM command namespaces.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use clap::{Args, Subcommand};
use serde::{Deserialize, Serialize};
use tonic::Code;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::{DaemonClient, DaemonClientError};

use super::runtime_daemon::{
    ControlPlaneTransport, connect_control_plane_for_state_db, control_plane_transport,
    default_state_db_path,
};
use super::sandbox;

/// Manage VM command namespaces.
#[derive(Args, Debug)]
pub struct VmArgs {
    #[command(subcommand)]
    pub action: VmCommand,
}

/// VM namespace operations.
#[derive(Subcommand, Debug)]
pub enum VmCommand {
    /// macOS VM management commands (Virtualization.framework).
    #[cfg(target_os = "macos")]
    Mac(MacVmArgs),

    /// Linux VM orchestration/testing workflows.
    Linux(LinuxVmArgs),
}

/// Entry point for `vz vm`.
pub async fn run(args: VmArgs) -> anyhow::Result<()> {
    match args.action {
        #[cfg(target_os = "macos")]
        VmCommand::Mac(mac_args) => run_mac(mac_args).await,
        VmCommand::Linux(linux_args) => run_linux(linux_args).await,
    }
}

/// `vz vm mac` arguments.
#[cfg(target_os = "macos")]
#[derive(Args, Debug)]
pub struct MacVmArgs {
    #[command(subcommand)]
    pub action: MacVmCommand,
}

/// macOS VM management operations.
#[cfg(target_os = "macos")]
#[derive(Subcommand, Debug)]
pub enum MacVmCommand {
    /// Create a golden macOS VM image from an IPSW.
    Init(super::init::InitArgs),
    /// Start a VM with optional mounts.
    Run(super::run::RunArgs),
    /// Execute a command inside a running VM.
    Exec(super::exec::ExecArgs),
    /// Save VM state for fast restore.
    Save(super::save::SaveArgs),
    /// Restore VM from saved state.
    Restore(super::restore::RestoreArgs),
    /// List running VMs.
    List(super::list::ListArgs),
    /// Stop a running VM.
    Stop(super::stop::StopArgs),
    /// Remove VM runtime metadata (and optionally image artifacts).
    Rm(super::rm::RmArgs),
    /// Manage cached files (IPSWs, downloads).
    Cache(super::cache::CacheArgs),
    /// Provision a disk image (user account, guest agent, auto-login).
    Provision(super::provision::ProvisionArgs),
    /// Detect and clean up orphaned VMs.
    Cleanup(super::cleanup::CleanupArgs),
    /// Ad-hoc sign the vz binary with required entitlements.
    SelfSign(super::self_sign::SelfSignArgs),
    /// Run validation suites against image cohorts.
    Validate(super::validate::ValidateArgs),
    /// Manage supported base image definitions.
    Base(super::vm_base::VmBaseArgs),
    /// Create/apply patch bundles and image deltas.
    Patch(super::vm_patch::VmPatchArgs),
}

/// Entry point for `vz vm mac` and legacy `vz debug vm`.
#[cfg(target_os = "macos")]
pub async fn run_mac(args: MacVmArgs) -> anyhow::Result<()> {
    match args.action {
        MacVmCommand::Init(a) => super::init::run(a).await,
        MacVmCommand::Run(a) => super::run::run(a).await,
        MacVmCommand::Exec(a) => super::exec::run(a).await,
        MacVmCommand::Save(a) => super::save::run(a).await,
        MacVmCommand::Restore(a) => super::restore::run(a).await,
        MacVmCommand::List(a) => super::list::run(a).await,
        MacVmCommand::Stop(a) => super::stop::run(a).await,
        MacVmCommand::Rm(a) => super::rm::run(a).await,
        MacVmCommand::Cache(a) => super::cache::run(a).await,
        MacVmCommand::Provision(a) => super::provision::run(a).await,
        MacVmCommand::Cleanup(a) => super::cleanup::run(a).await,
        MacVmCommand::SelfSign(a) => super::self_sign::run(a).await,
        MacVmCommand::Validate(a) => super::validate::run(a).await,
        MacVmCommand::Base(a) => super::vm_base::run(a).await,
        MacVmCommand::Patch(a) => super::vm_patch::run(a).await,
    }
}

/// `vz vm linux` arguments.
#[derive(Args, Debug)]
pub struct LinuxVmArgs {
    #[command(subcommand)]
    pub action: LinuxVmCommand,
}

/// Linux VM workflow operations.
#[derive(Subcommand, Debug)]
pub enum LinuxVmCommand {
    /// Initialize Linux guest image metadata and persistent disk artifacts.
    Init(LinuxVmInitArgs),
    /// Create/start a Linux space without attaching.
    Run(LinuxVmRunArgs),
    /// List Linux spaces.
    List(LinuxVmListArgs),
    /// Show Linux space details.
    Inspect(LinuxVmInspectArgs),
    /// Attach interactive shell to a Linux space.
    Attach(LinuxVmAttachArgs),
    /// Execute a command in a Linux space with streamed output.
    Exec(LinuxVmExecArgs),
    /// Stop a Linux space.
    Stop(LinuxVmStopArgs),
    /// Remove (terminate) a Linux space.
    Rm(LinuxVmRmArgs),
    /// Linux VM test workflows.
    Test(LinuxVmTestArgs),
    /// Run local no-SSH Linux VM E2E harness orchestration (legacy alias).
    #[command(hide = true)]
    E2e(LinuxVmE2eArgs),
}

/// Arguments for `vz vm linux init`.
#[derive(Args, Debug)]
pub struct LinuxVmInitArgs {
    /// Logical Linux guest image name.
    #[arg(long)]
    pub name: String,
    /// Output directory for Linux guest image metadata and disk.
    #[arg(long, default_value = "~/.vz/images")]
    pub output_dir: String,
    /// Linux guest persistent disk size in GiB.
    #[arg(long, default_value_t = 64)]
    pub disk_size_gb: u64,
    /// Override kernel artifact path (defaults to ~/.vz/linux/vmlinux).
    #[arg(long)]
    pub kernel: Option<PathBuf>,
    /// Override initramfs artifact path (defaults to ~/.vz/linux/initramfs.img).
    #[arg(long)]
    pub initramfs: Option<PathBuf>,
    /// Replace existing image artifacts when target already exists.
    #[arg(long)]
    pub force: bool,
}

/// Arguments for `vz vm linux run`.
#[derive(Args, Debug)]
pub struct LinuxVmRunArgs {
    /// Linux guest image name (descriptor file `<name>.linux.json`).
    #[arg(long)]
    pub name: String,
    /// Optional explicit descriptor path.
    #[arg(long)]
    pub descriptor: Option<PathBuf>,
    /// Descriptor directory when `--descriptor` is not set.
    #[arg(long, default_value = "~/.vz/images")]
    pub output_dir: String,
    /// Number of virtual CPUs.
    #[arg(long, default_value = "2")]
    pub cpus: u8,
    /// Memory in MB.
    #[arg(long, default_value = "2048")]
    pub memory: u64,
    /// Optional kernel command line override.
    #[arg(long)]
    pub cmdline: Option<String>,
    /// Optional rootfs directory mounted as `rootfs` VirtioFS tag.
    #[arg(long)]
    pub rootfs_dir: Option<PathBuf>,
    /// Additional shared directory mounts (`TAG:HOST_PATH[:ro|rw]`).
    #[arg(long = "mount")]
    pub mounts: Vec<String>,
    /// Stop the VM once guest agent is ready (smoke mode).
    #[arg(long)]
    pub stop_after_ready: bool,
    /// Guest agent readiness timeout in seconds.
    #[arg(long, default_value_t = 30)]
    pub agent_timeout_secs: u64,
    /// Optional shell command executed inside the guest after agent readiness.
    ///
    /// When set, `vz vm linux run` streams command output, propagates exit code,
    /// and stops the VM when the command completes.
    #[arg(long)]
    pub guest_command: Option<String>,
    /// Timeout in seconds for `--guest-command` execution.
    #[arg(long, default_value_t = 900)]
    pub guest_command_timeout_secs: u64,
    /// Optional guest user for `--guest-command`.
    #[arg(long)]
    pub guest_command_user: Option<String>,
}

/// Arguments for `vz vm linux list`.
#[derive(Args, Debug)]
pub struct LinuxVmListArgs {
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
    /// Output payload as JSON.
    #[arg(long)]
    pub json: bool,
    /// Include non-linux backends.
    #[arg(long)]
    pub all: bool,
}

/// Arguments for `vz vm linux inspect`.
#[derive(Args, Debug)]
pub struct LinuxVmInspectArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// Arguments for `vz vm linux attach`.
#[derive(Args, Debug)]
pub struct LinuxVmAttachArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// Arguments for `vz vm linux exec`.
#[derive(Args, Debug)]
pub struct LinuxVmExecArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Command and arguments to run.
    #[arg(last = true, required = true)]
    pub command: Vec<String>,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
    /// Timeout in seconds (0 disables timeout).
    #[arg(long, default_value_t = 0)]
    pub timeout_secs: u64,
    /// Force PTY allocation for the command.
    #[arg(long)]
    pub pty: bool,
}

/// Arguments for `vz vm linux stop`.
#[derive(Args, Debug)]
pub struct LinuxVmStopArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// Arguments for `vz vm linux rm`.
#[derive(Args, Debug)]
pub struct LinuxVmRmArgs {
    /// Sandbox identifier.
    pub sandbox_id: String,
    /// Path to runtime state DB.
    #[arg(long)]
    pub state_db: Option<PathBuf>,
}

/// `vz vm linux test` arguments.
#[derive(Args, Debug)]
pub struct LinuxVmTestArgs {
    #[command(subcommand)]
    pub action: LinuxVmTestCommand,
}

/// Linux VM test workflow operations.
#[derive(Subcommand, Debug)]
pub enum LinuxVmTestCommand {
    /// Run local no-SSH Linux VM E2E harness orchestration.
    E2e(LinuxVmE2eArgs),
}

/// Arguments for `vz vm linux test e2e`.
#[derive(Args, Debug)]
pub struct LinuxVmE2eArgs {
    /// VM name used by `vz vm mac`.
    #[arg(long)]
    pub vm_name: String,
    /// Repo path inside guest.
    #[arg(long)]
    pub guest_repo: String,
    /// In-guest btrfs workspace path.
    #[arg(long, default_value = "/mnt/vz-btrfs")]
    pub workspace: String,
    /// Harness profile.
    #[arg(long, default_value = "debug")]
    pub profile: String,
    /// Auto-start VM if not already running.
    #[arg(long)]
    pub auto_start: bool,
    /// VM image path for auto-start.
    #[arg(long)]
    pub vm_image: Option<PathBuf>,
    /// CPUs for auto-started VM.
    #[arg(long, default_value_t = 4)]
    pub vm_cpus: u32,
    /// Memory GB for auto-started VM.
    #[arg(long, default_value_t = 8)]
    pub vm_memory_gb: u64,
    /// Max wait seconds for VM availability after auto-start.
    #[arg(long, default_value_t = 90)]
    pub wait_secs: u64,
    /// VirtioFS mount (TAG:HOST_PATH). Repeatable.
    #[arg(long = "mount")]
    pub mounts: Vec<String>,
    /// Skip guest btrfs provisioning.
    #[arg(long)]
    pub no_provision_btrfs: bool,
    /// In-guest loopback btrfs image path.
    #[arg(long, default_value = "/var/lib/vz-btrfs-workspace.img")]
    pub btrfs_image: String,
    /// In-guest loopback btrfs image size in GiB.
    #[arg(long, default_value_t = 64)]
    pub btrfs_size_gb: u64,
}

async fn run_linux(args: LinuxVmArgs) -> anyhow::Result<()> {
    match args.action {
        LinuxVmCommand::Init(init_args) => run_linux_init(init_args).await,
        LinuxVmCommand::Run(run_args) => run_linux_run(run_args).await,
        LinuxVmCommand::List(list_args) => run_linux_list(list_args).await,
        LinuxVmCommand::Inspect(inspect_args) => run_linux_inspect(inspect_args).await,
        LinuxVmCommand::Attach(attach_args) => run_linux_attach(attach_args).await,
        LinuxVmCommand::Exec(exec_args) => run_linux_exec(exec_args).await,
        LinuxVmCommand::Stop(stop_args) => run_linux_stop(stop_args).await,
        LinuxVmCommand::Rm(rm_args) => run_linux_rm(rm_args).await,
        LinuxVmCommand::Test(test_args) => run_linux_test(test_args).await,
        LinuxVmCommand::E2e(e2e_args) => {
            eprintln!(
                "warning: `vz vm linux e2e` is deprecated; use `vz vm linux test e2e` instead"
            );
            run_linux_e2e(e2e_args)
        }
    }
}

async fn run_linux_init(args: LinuxVmInitArgs) -> anyhow::Result<()> {
    let output_dir = expand_home_path(args.output_dir.as_str())?;
    let kernel = args.kernel.unwrap_or_else(default_linux_kernel_path);
    let initramfs = args.initramfs.unwrap_or_else(default_linux_initramfs_path);
    let version_path = default_linux_version_json_path();

    ensure_file_exists(kernel.as_path(), "kernel")?;
    ensure_file_exists(initramfs.as_path(), "initramfs")?;
    ensure_file_exists(version_path.as_path(), "version metadata")?;

    let version = load_linux_version_metadata(version_path.as_path())?;
    validate_linux_artifacts(kernel.as_path(), initramfs.as_path(), &version)?;

    std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))?;

    let disk_path = output_dir.join(format!("{}.img", args.name));
    let descriptor_path = output_dir.join(format!("{}.linux.json", args.name));
    let requested_disk_size_bytes = gib_to_bytes(args.disk_size_gb)?;
    let disk_result =
        provision_linux_disk_image(disk_path.as_path(), requested_disk_size_bytes, args.force)?;

    if descriptor_path.exists() && !args.force {
        let existing = load_linux_vm_image_descriptor(descriptor_path.as_path())?;
        if existing.disk_path != disk_path {
            bail!(
                "existing descriptor disk path {} does not match expected {}",
                existing.disk_path.display(),
                disk_path.display()
            );
        }
        if existing.disk_size_gb != args.disk_size_gb {
            bail!(
                "existing descriptor disk size {} GiB does not match requested {} GiB; re-run with --force to replace",
                existing.disk_size_gb,
                args.disk_size_gb
            );
        }
        validate_descriptor_against_current_artifacts(&existing)?;
        println!(
            "Linux image descriptor already exists and is compatible: {}",
            descriptor_path.display()
        );
        println!(
            "Disk image: {} ({})",
            disk_path.display(),
            disk_result.as_str()
        );
        println!("Re-run with --force to overwrite descriptor and disk metadata.");
        return Ok(());
    }

    let descriptor = LinuxVmImageDescriptor {
        schema_version: LINUX_VM_IMAGE_DESCRIPTOR_SCHEMA_VERSION,
        image_name: args.name,
        kernel_path: kernel,
        initramfs_path: initramfs,
        version_json_path: version_path,
        disk_path,
        disk_size_gb: args.disk_size_gb,
        linux_artifact_version: version.kernel,
        sha256_vmlinux: version.sha256_vmlinux,
        sha256_initramfs: version.sha256_initramfs,
        created_at_unix_secs: now_unix_secs()?,
    };

    write_linux_vm_image_descriptor(descriptor_path.as_path(), &descriptor)?;
    println!(
        "Initialized Linux image descriptor: {}",
        descriptor_path.display()
    );
    println!("Kernel: {}", descriptor.kernel_path.display());
    println!("Initramfs: {}", descriptor.initramfs_path.display());
    println!(
        "Disk image: {} ({}; {} GiB)",
        descriptor.disk_path.display(),
        disk_result.as_str(),
        descriptor.disk_size_gb
    );
    Ok(())
}

const LINUX_VM_IMAGE_DESCRIPTOR_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LinuxVmImageDescriptor {
    schema_version: u16,
    image_name: String,
    kernel_path: PathBuf,
    initramfs_path: PathBuf,
    version_json_path: PathBuf,
    disk_path: PathBuf,
    disk_size_gb: u64,
    linux_artifact_version: String,
    sha256_vmlinux: String,
    sha256_initramfs: String,
    created_at_unix_secs: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct LinuxArtifactVersionJson {
    kernel: String,
    sha256_vmlinux: String,
    sha256_initramfs: String,
}

fn expand_home_path(raw: &str) -> anyhow::Result<PathBuf> {
    if raw == "~" {
        return home_dir();
    }
    if let Some(rest) = raw.strip_prefix("~/") {
        return Ok(home_dir()?.join(rest));
    }
    Ok(PathBuf::from(raw))
}

fn home_dir() -> anyhow::Result<PathBuf> {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .ok_or_else(|| anyhow!("HOME is not set"))
}

fn default_linux_artifact_dir() -> PathBuf {
    match std::env::var_os("HOME") {
        Some(home) => PathBuf::from(home).join(".vz").join("linux"),
        None => PathBuf::from(".vz").join("linux"),
    }
}

fn default_linux_kernel_path() -> PathBuf {
    default_linux_artifact_dir().join("vmlinux")
}

fn default_linux_initramfs_path() -> PathBuf {
    default_linux_artifact_dir().join("initramfs.img")
}

fn default_linux_version_json_path() -> PathBuf {
    default_linux_artifact_dir().join("version.json")
}

fn ensure_file_exists(path: &Path, label: &str) -> anyhow::Result<()> {
    if !path.is_file() {
        bail!("{label} file not found at {}", path.display());
    }
    Ok(())
}

fn load_linux_version_metadata(path: &Path) -> anyhow::Result<LinuxArtifactVersionJson> {
    let raw = std::fs::read(path)
        .with_context(|| format!("failed to read linux version metadata {}", path.display()))?;
    serde_json::from_slice::<LinuxArtifactVersionJson>(&raw)
        .with_context(|| format!("failed to parse linux version metadata {}", path.display()))
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
    let bytes =
        std::fs::read(path).with_context(|| format!("failed to read file {}", path.display()))?;
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn validate_linux_artifacts(
    kernel: &Path,
    initramfs: &Path,
    version: &LinuxArtifactVersionJson,
) -> anyhow::Result<()> {
    let kernel_sha = sha256_file(kernel)?;
    let initramfs_sha = sha256_file(initramfs)?;
    if kernel_sha != version.sha256_vmlinux {
        bail!(
            "linux kernel checksum mismatch for {}: expected {}, got {}",
            kernel.display(),
            version.sha256_vmlinux,
            kernel_sha
        );
    }
    if initramfs_sha != version.sha256_initramfs {
        bail!(
            "linux initramfs checksum mismatch for {}: expected {}, got {}",
            initramfs.display(),
            version.sha256_initramfs,
            initramfs_sha
        );
    }
    Ok(())
}

fn write_linux_vm_image_descriptor(
    path: &Path,
    descriptor: &LinuxVmImageDescriptor,
) -> anyhow::Result<()> {
    let json = serde_json::to_vec_pretty(descriptor)
        .context("failed to serialize linux vm image descriptor")?;
    std::fs::write(path, json).with_context(|| {
        format!(
            "failed to write linux vm image descriptor {}",
            path.display()
        )
    })
}

fn load_linux_vm_image_descriptor(path: &Path) -> anyhow::Result<LinuxVmImageDescriptor> {
    let raw = std::fs::read(path).with_context(|| {
        format!(
            "failed to read linux vm image descriptor {}",
            path.display()
        )
    })?;
    let descriptor = serde_json::from_slice::<LinuxVmImageDescriptor>(&raw).with_context(|| {
        format!(
            "failed to parse linux vm image descriptor {}",
            path.display()
        )
    })?;
    if descriptor.schema_version != LINUX_VM_IMAGE_DESCRIPTOR_SCHEMA_VERSION {
        bail!(
            "unsupported linux vm image descriptor schema {} in {}",
            descriptor.schema_version,
            path.display()
        );
    }
    Ok(descriptor)
}

fn validate_descriptor_against_current_artifacts(
    descriptor: &LinuxVmImageDescriptor,
) -> anyhow::Result<()> {
    ensure_file_exists(descriptor.kernel_path.as_path(), "descriptor kernel")?;
    ensure_file_exists(descriptor.initramfs_path.as_path(), "descriptor initramfs")?;
    ensure_file_exists(
        descriptor.version_json_path.as_path(),
        "descriptor version metadata",
    )?;
    let version = load_linux_version_metadata(descriptor.version_json_path.as_path())?;
    if version.kernel != descriptor.linux_artifact_version {
        bail!(
            "linux artifact version mismatch for descriptor {}: expected {}, got {}",
            descriptor.image_name,
            descriptor.linux_artifact_version,
            version.kernel
        );
    }
    validate_linux_artifacts(
        descriptor.kernel_path.as_path(),
        descriptor.initramfs_path.as_path(),
        &version,
    )
}

fn now_unix_secs() -> anyhow::Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_secs())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiskProvisioningResult {
    Created,
    Reused,
    Replaced,
}

impl DiskProvisioningResult {
    fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Reused => "reused",
            Self::Replaced => "replaced",
        }
    }
}

fn gib_to_bytes(gib: u64) -> anyhow::Result<u64> {
    gib.checked_mul(1024 * 1024 * 1024)
        .ok_or_else(|| anyhow!("disk size too large: {gib} GiB"))
}

fn provision_linux_disk_image(
    path: &Path,
    expected_size_bytes: u64,
    force_replace: bool,
) -> anyhow::Result<DiskProvisioningResult> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create parent directory for disk image {}",
                path.display()
            )
        })?;
    }

    if path.exists() {
        if force_replace {
            std::fs::remove_file(path)
                .with_context(|| format!("failed to remove disk image {}", path.display()))?;
            create_sparse_disk(path, expected_size_bytes)?;
            return Ok(DiskProvisioningResult::Replaced);
        }

        let metadata = std::fs::metadata(path)
            .with_context(|| format!("failed to stat disk image {}", path.display()))?;
        if !metadata.is_file() {
            bail!("disk image path is not a regular file: {}", path.display());
        }
        if metadata.len() != expected_size_bytes {
            bail!(
                "disk image {} exists with size {} bytes but expected {} bytes; re-run with --force to replace",
                path.display(),
                metadata.len(),
                expected_size_bytes
            );
        }
        return Ok(DiskProvisioningResult::Reused);
    }

    create_sparse_disk(path, expected_size_bytes)?;
    Ok(DiskProvisioningResult::Created)
}

fn create_sparse_disk(path: &Path, size_bytes: u64) -> anyhow::Result<()> {
    let file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("failed to create disk image {}", path.display()))?;
    file.set_len(size_bytes)
        .with_context(|| format!("failed to size disk image {}", path.display()))?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn parse_linux_vm_mount_specs(specs: &[String]) -> anyhow::Result<Vec<vz::SharedDirConfig>> {
    let mut mounts = Vec::with_capacity(specs.len());
    for spec in specs {
        let (tag, source, read_only) = parse_linux_vm_mount_spec(spec)?;
        mounts.push(vz::SharedDirConfig {
            tag,
            source,
            read_only,
        });
    }
    Ok(mounts)
}

#[cfg(target_os = "macos")]
fn parse_linux_vm_mount_spec(spec: &str) -> anyhow::Result<(String, PathBuf, bool)> {
    let mut parts = spec.split(':');
    let tag = parts
        .next()
        .ok_or_else(|| anyhow!("mount spec must be TAG:HOST_PATH[:ro|rw]"))?;
    let source = parts
        .next()
        .ok_or_else(|| anyhow!("mount spec must be TAG:HOST_PATH[:ro|rw]"))?;
    let mode = parts.next();
    if parts.next().is_some() {
        bail!("mount spec has too many ':' separators: {spec}");
    }
    if tag.trim().is_empty() {
        bail!("mount tag must not be empty in spec: {spec}");
    }
    if source.trim().is_empty() {
        bail!("mount source path must not be empty in spec: {spec}");
    }
    let read_only = match mode {
        None => false,
        Some("rw") => false,
        Some("ro") => true,
        Some(other) => bail!("unsupported mount mode `{other}` in spec: {spec}"),
    };
    Ok((tag.to_string(), PathBuf::from(source), read_only))
}

async fn run_linux_run(args: LinuxVmRunArgs) -> anyhow::Result<()> {
    run_linux_host_boot(args).await
}

async fn run_linux_host_boot(args: LinuxVmRunArgs) -> anyhow::Result<()> {
    let output_dir = expand_home_path(args.output_dir.as_str())?;
    let descriptor_path = match args.descriptor.as_ref() {
        Some(path) => path.clone(),
        None => output_dir.join(format!("{}.linux.json", args.name)),
    };
    ensure_file_exists(descriptor_path.as_path(), "linux image descriptor")?;
    let descriptor = load_linux_vm_image_descriptor(descriptor_path.as_path())?;
    validate_descriptor_against_current_artifacts(&descriptor)?;

    let expected_disk_size = gib_to_bytes(descriptor.disk_size_gb)?;
    let disk_metadata = std::fs::metadata(descriptor.disk_path.as_path()).with_context(|| {
        format!(
            "failed to stat disk image {}",
            descriptor.disk_path.display()
        )
    })?;
    if !disk_metadata.is_file() {
        bail!(
            "descriptor disk path is not a regular file: {}",
            descriptor.disk_path.display()
        );
    }
    if disk_metadata.len() != expected_disk_size {
        bail!(
            "descriptor disk image {} has size {} bytes but expected {} bytes ({} GiB)",
            descriptor.disk_path.display(),
            disk_metadata.len(),
            expected_disk_size,
            descriptor.disk_size_gb
        );
    }

    #[cfg(not(target_os = "macos"))]
    {
        let _ = args;
        bail!("`vz vm linux run` host boot is only supported on macOS hosts");
    }

    #[cfg(target_os = "macos")]
    {
        let mut config = vz_linux::LinuxVmConfig::new(
            descriptor.kernel_path.clone(),
            descriptor.initramfs_path.clone(),
        );
        config.cpus = args.cpus;
        config.memory_mb = args.memory;
        config.disk_image = Some(descriptor.disk_path.clone());
        if let Some(cmdline) = args.cmdline {
            config.cmdline = cmdline;
        }
        if let Some(rootfs_dir) = args.rootfs_dir {
            config = config.with_rootfs_dir(rootfs_dir);
        }
        config.shared_dirs = parse_linux_vm_mount_specs(&args.mounts)?;
        config
            .validate()
            .context("invalid Linux VM host boot configuration")?;

        let vm = vz_linux::LinuxVm::create(config)
            .await
            .context("failed to create Linux VM from descriptor")?;
        let timeout = Duration::from_secs(args.agent_timeout_secs);
        let boot_elapsed = vm
            .start_and_wait_for_agent_with_progress(timeout, |attempt, last_error| {
                if attempt == 1 || attempt % 10 == 0 {
                    eprintln!("waiting for guest agent (attempt {attempt}): {last_error}");
                }
            })
            .await
            .context("Linux VM boot failed before guest agent became ready")?;

        println!(
            "Linux VM booted from descriptor {}",
            descriptor_path.display()
        );
        println!(
            "Guest agent ready in {:.3}s; press Ctrl+C to stop VM",
            boot_elapsed.as_secs_f64()
        );

        if let Some(guest_command) = args.guest_command {
            println!("Executing guest command via /bin/sh -lc ...");
            let exec_timeout = Duration::from_secs(args.guest_command_timeout_secs);
            let exec_options = vz_linux::ExecOptions {
                user: args.guest_command_user,
                ..Default::default()
            };
            let exec_output = vm
                .exec_capture_with_options_streaming(
                    "/bin/sh".to_string(),
                    vec!["-lc".to_string(), guest_command],
                    exec_timeout,
                    exec_options,
                    |event| match event {
                        vz::protocol::ExecEvent::Stdout(bytes) => {
                            let _ = std::io::stdout().write_all(bytes);
                            let _ = std::io::stdout().flush();
                        }
                        vz::protocol::ExecEvent::Stderr(bytes) => {
                            let _ = std::io::stderr().write_all(bytes);
                            let _ = std::io::stderr().flush();
                        }
                        vz::protocol::ExecEvent::Exit(_) => {}
                    },
                )
                .await
                .context("guest command execution failed")?;

            vm.stop()
                .await
                .context("failed to stop Linux VM after guest command")?;
            println!("Stopped Linux VM.");
            if exec_output.exit_code != 0 {
                std::process::exit(exec_output.exit_code);
            }
            return Ok(());
        }

        if args.stop_after_ready {
            vm.stop()
                .await
                .context("failed to stop Linux VM in --stop-after-ready mode")?;
            println!("Stopped Linux VM (stop-after-ready).");
            return Ok(());
        }

        tokio::signal::ctrl_c()
            .await
            .context("failed waiting for Ctrl+C signal")?;
        vm.stop()
            .await
            .context("failed to stop Linux VM after Ctrl+C")?;
        println!("Stopped Linux VM.");
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct LinuxVmListItem {
    sandbox_id: String,
    backend: String,
    state: String,
    cpus: u32,
    memory_mb: u64,
    created_at: u64,
    updated_at: u64,
}

async fn run_linux_list(args: LinuxVmListArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let mut client = connect_linux_daemon(&state_db).await?;
    let mut sandboxes = client
        .list_sandboxes(runtime_v2::ListSandboxesRequest { metadata: None })
        .await
        .context("failed to list sandboxes via daemon")?
        .sandboxes;

    if !args.all {
        sandboxes.retain(|sandbox| is_linux_backend_name(&sandbox.backend));
    }

    let items: Vec<LinuxVmListItem> = sandboxes
        .into_iter()
        .map(|sandbox| LinuxVmListItem {
            sandbox_id: sandbox.sandbox_id,
            backend: sandbox.backend,
            state: sandbox.state,
            cpus: sandbox.cpus,
            memory_mb: sandbox.memory_mb,
            created_at: sandbox.created_at,
            updated_at: sandbox.updated_at,
        })
        .collect();

    if args.json {
        let json = serde_json::to_string_pretty(&items).context("failed to serialize payload")?;
        println!("{json}");
        return Ok(());
    }

    if items.is_empty() {
        if args.all {
            println!("No spaces found.");
        } else {
            println!("No linux spaces found.");
        }
        return Ok(());
    }

    println!(
        "{:<16} {:<20} {:<12} {:<6} {:<10}",
        "SANDBOX ID", "BACKEND", "STATE", "CPUS", "MEMORY_MB"
    );
    for item in &items {
        println!(
            "{:<16} {:<20} {:<12} {:<6} {:<10}",
            item.sandbox_id, item.backend, item.state, item.cpus, item.memory_mb
        );
    }
    Ok(())
}

async fn run_linux_inspect(args: LinuxVmInspectArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let mut client = connect_linux_daemon(&state_db).await?;
    let response = match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: args.sandbox_id.clone(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("linux space {} not found", args.sandbox_id)
        }
        Err(error) => return Err(anyhow!(error).context("failed to inspect linux space")),
    };
    let payload = response
        .sandbox
        .ok_or_else(|| anyhow!("daemon inspect response missing sandbox payload"))?;
    if !is_linux_backend_name(&payload.backend) {
        bail!(
            "sandbox {} is backend {}, not linux",
            args.sandbox_id,
            payload.backend
        );
    }
    let inspect = LinuxVmListItem {
        sandbox_id: payload.sandbox_id,
        backend: payload.backend,
        state: payload.state,
        cpus: payload.cpus,
        memory_mb: payload.memory_mb,
        created_at: payload.created_at,
        updated_at: payload.updated_at,
    };
    let json =
        serde_json::to_string_pretty(&inspect).context("failed to serialize sandbox payload")?;
    println!("{json}");
    Ok(())
}

async fn run_linux_attach(args: LinuxVmAttachArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    ensure_linux_sandbox(&state_db, &args.sandbox_id).await?;
    sandbox::cmd_attach(sandbox::SandboxAttachArgs {
        sandbox_id: args.sandbox_id,
        state_db: Some(state_db),
    })
    .await
}

async fn run_linux_exec(args: LinuxVmExecArgs) -> anyhow::Result<()> {
    if args.command.is_empty() {
        bail!("exec requires a command");
    }
    let (cmd, tail) = args
        .command
        .split_first()
        .ok_or_else(|| anyhow!("exec requires a command"))?;
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    let mut client = connect_linux_daemon(&state_db).await?;
    let (container_id, shell_execution_id) =
        resolve_linux_container_for_sandbox(&mut client, &args.sandbox_id).await?;
    if let Some(execution_id) = shell_execution_id.as_deref() {
        close_shell_session(&mut client, &args.sandbox_id, execution_id).await?;
    }

    let execution = client
        .create_execution(runtime_v2::CreateExecutionRequest {
            metadata: None,
            container_id,
            cmd: vec![cmd.to_string()],
            args: tail.iter().map(|arg| arg.to_string()).collect(),
            env_override: HashMap::new(),
            timeout_secs: args.timeout_secs,
            pty_mode: if args.pty {
                runtime_v2::create_execution_request::PtyMode::Enabled as i32
            } else {
                runtime_v2::create_execution_request::PtyMode::Disabled as i32
            },
        })
        .await
        .context("failed to create linux execution")?;
    let execution_payload = execution
        .execution
        .ok_or_else(|| anyhow!("daemon create_execution missing execution payload"))?;
    let execution_id = execution_payload.execution_id;

    let mut stream = client
        .stream_exec_output(runtime_v2::StreamExecOutputRequest {
            execution_id: execution_id.clone(),
            metadata: None,
        })
        .await
        .context("failed to stream linux execution output")?;

    let mut exit_code = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed to read linux execution stream")?
    {
        match event.payload {
            Some(runtime_v2::exec_output_event::Payload::Stdout(bytes)) => {
                std::io::stdout()
                    .write_all(&bytes)
                    .context("failed writing stdout payload")?;
                std::io::stdout()
                    .flush()
                    .context("failed flushing stdout payload")?;
            }
            Some(runtime_v2::exec_output_event::Payload::Stderr(bytes)) => {
                std::io::stderr()
                    .write_all(&bytes)
                    .context("failed writing stderr payload")?;
                std::io::stderr()
                    .flush()
                    .context("failed flushing stderr payload")?;
            }
            Some(runtime_v2::exec_output_event::Payload::ExitCode(code)) => {
                exit_code = Some(code);
            }
            Some(runtime_v2::exec_output_event::Payload::Error(error)) => {
                bail!("execution stream error: {error}");
            }
            None => {}
        }
    }

    let code = match exit_code {
        Some(code) => code,
        None => {
            let response = client
                .get_execution(runtime_v2::GetExecutionRequest {
                    execution_id: execution_id.clone(),
                    metadata: None,
                })
                .await
                .context("failed to inspect execution after stream completion")?;
            let payload = response
                .execution
                .ok_or_else(|| anyhow!("daemon get_execution missing payload"))?;
            payload.exit_code
        }
    };

    if code != 0 {
        std::process::exit(code);
    }
    Ok(())
}

async fn run_linux_stop(args: LinuxVmStopArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    ensure_linux_sandbox(&state_db, &args.sandbox_id).await?;
    sandbox::cmd_terminate(sandbox::SandboxTerminateArgs {
        sandbox_id: args.sandbox_id,
        state_db: Some(state_db),
    })
    .await
}

async fn run_linux_rm(args: LinuxVmRmArgs) -> anyhow::Result<()> {
    let state_db = args.state_db.unwrap_or_else(default_state_db_path);
    ensure_linux_sandbox(&state_db, &args.sandbox_id).await?;
    sandbox::cmd_terminate(sandbox::SandboxTerminateArgs {
        sandbox_id: args.sandbox_id,
        state_db: Some(state_db),
    })
    .await
}

async fn run_linux_test(args: LinuxVmTestArgs) -> anyhow::Result<()> {
    match args.action {
        LinuxVmTestCommand::E2e(e2e_args) => run_linux_e2e(e2e_args),
    }
}

async fn connect_linux_daemon(state_db: &Path) -> anyhow::Result<DaemonClient> {
    match control_plane_transport()? {
        ControlPlaneTransport::DaemonGrpc => {}
        ControlPlaneTransport::ApiHttp => {
            bail!("vz vm linux commands require daemon-grpc transport")
        }
    }
    let client = connect_control_plane_for_state_db(state_db).await?;
    let backend = client.handshake().backend_name.trim().to_ascii_lowercase();
    if !is_linux_backend_name(&backend) {
        bail!(
            "connected daemon backend `{backend}` is not linux; point VZ_RUNTIME_DAEMON_SOCKET at a linux daemon"
        );
    }
    Ok(client)
}

fn is_linux_backend_name(name: &str) -> bool {
    matches!(
        name.trim().to_ascii_lowercase().as_str(),
        "linux_firecracker"
            | "linux-firecracker"
            | "firecracker"
            | "linux-native"
            | "linux_native"
            | "linux"
    )
}

async fn ensure_linux_sandbox(state_db: &Path, sandbox_id: &str) -> anyhow::Result<()> {
    let mut client = connect_linux_daemon(state_db).await?;
    let response = match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(response) => response,
        Err(DaemonClientError::Grpc(status)) if status.code() == Code::NotFound => {
            bail!("linux space {sandbox_id} not found")
        }
        Err(error) => return Err(anyhow!(error).context("failed to resolve linux space")),
    };
    let payload = response
        .sandbox
        .ok_or_else(|| anyhow!("daemon get_sandbox missing payload"))?;
    if !is_linux_backend_name(payload.backend.as_str()) {
        bail!(
            "sandbox {sandbox_id} is backend `{}`, not linux",
            payload.backend
        );
    }
    Ok(())
}

async fn resolve_linux_container_for_sandbox(
    client: &mut DaemonClient,
    sandbox_id: &str,
) -> anyhow::Result<(String, Option<String>)> {
    let mut stream = client
        .open_sandbox_shell(runtime_v2::OpenSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
        .context("failed to open sandbox shell for container resolution")?;
    let mut completion = None;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading open_sandbox_shell stream")?
    {
        match event.payload {
            Some(runtime_v2::open_sandbox_shell_event::Payload::Progress(progress)) => {
                eprintln!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::open_sandbox_shell_event::Payload::Completion(done)) => {
                completion = Some(done);
                break;
            }
            None => {}
        }
    }
    let completion = completion
        .ok_or_else(|| anyhow!("open_sandbox_shell stream ended without completion event"))?;
    if completion.container_id.trim().is_empty() {
        bail!("open_sandbox_shell completion missing container_id");
    }
    let shell_execution_id = if completion.execution_id.trim().is_empty() {
        None
    } else {
        Some(completion.execution_id)
    };
    Ok((completion.container_id, shell_execution_id))
}

async fn close_shell_session(
    client: &mut DaemonClient,
    sandbox_id: &str,
    execution_id: &str,
) -> anyhow::Result<()> {
    let mut stream = client
        .close_sandbox_shell(runtime_v2::CloseSandboxShellRequest {
            sandbox_id: sandbox_id.to_string(),
            execution_id: execution_id.to_string(),
            metadata: None,
        })
        .await
        .context("failed to close bootstrap shell session")?;
    while let Some(event) = stream
        .message()
        .await
        .context("failed reading close_sandbox_shell stream")?
    {
        match event.payload {
            Some(runtime_v2::close_sandbox_shell_event::Payload::Progress(progress)) => {
                eprintln!("[{}] {}", progress.phase, progress.detail);
            }
            Some(runtime_v2::close_sandbox_shell_event::Payload::Completion(_)) => break,
            None => {}
        }
    }
    Ok(())
}

fn run_linux_e2e(args: LinuxVmE2eArgs) -> anyhow::Result<()> {
    let script = PathBuf::from("scripts/run-vz-linux-vm-e2e-local.sh");
    if !script.is_file() {
        bail!(
            "missing harness wrapper {}; run from repository root",
            script.display()
        );
    }

    let mut cmd = Command::new(&script);
    cmd.arg("--vm-name")
        .arg(&args.vm_name)
        .arg("--guest-repo")
        .arg(&args.guest_repo)
        .arg("--workspace")
        .arg(&args.workspace)
        .arg("--profile")
        .arg(&args.profile)
        .arg("--vm-cpus")
        .arg(args.vm_cpus.to_string())
        .arg("--vm-memory-gb")
        .arg(args.vm_memory_gb.to_string())
        .arg("--wait-secs")
        .arg(args.wait_secs.to_string())
        .arg("--btrfs-image")
        .arg(&args.btrfs_image)
        .arg("--btrfs-size-gb")
        .arg(args.btrfs_size_gb.to_string());

    if args.auto_start {
        cmd.arg("--auto-start");
    }
    if let Some(vm_image) = args.vm_image {
        cmd.arg("--vm-image").arg(vm_image);
    }
    if args.no_provision_btrfs {
        cmd.arg("--no-provision-btrfs");
    }
    for mount in args.mounts {
        cmd.arg("--mount").arg(mount);
    }

    cmd.stdin(Stdio::inherit())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let status = cmd
        .status()
        .context("failed to launch local Linux VM E2E wrapper")?;
    if !status.success() {
        match status.code() {
            Some(code) => bail!("local Linux VM E2E wrapper failed with exit code {code}"),
            None => bail!("local Linux VM E2E wrapper terminated by signal"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sha2::Digest;
    use tempfile::tempdir;

    fn sha256_hex(bytes: &[u8]) -> String {
        let mut hasher = sha2::Sha256::new();
        hasher.update(bytes);
        format!("{:x}", hasher.finalize())
    }

    #[test]
    fn expand_home_path_expands_tilde_prefix() {
        let home = std::env::var("HOME").expect("HOME must be set for tests");
        let expanded = expand_home_path("~/foo/bar").expect("expand");
        assert_eq!(expanded, PathBuf::from(home).join("foo").join("bar"));
    }

    #[test]
    fn validate_linux_artifacts_accepts_matching_checksums() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        let kernel_bytes = b"kernel-blob";
        let initramfs_bytes = b"initramfs-blob";
        std::fs::write(&kernel, kernel_bytes).expect("write kernel");
        std::fs::write(&initramfs, initramfs_bytes).expect("write initramfs");

        let version = LinuxArtifactVersionJson {
            kernel: "6.12.11".to_string(),
            sha256_vmlinux: sha256_hex(kernel_bytes),
            sha256_initramfs: sha256_hex(initramfs_bytes),
        };

        validate_linux_artifacts(kernel.as_path(), initramfs.as_path(), &version)
            .expect("checksums should match");
    }

    #[test]
    fn validate_linux_artifacts_rejects_mismatched_checksums() {
        let tmp = tempdir().expect("tempdir");
        let kernel = tmp.path().join("vmlinux");
        let initramfs = tmp.path().join("initramfs.img");
        std::fs::write(&kernel, b"kernel-blob").expect("write kernel");
        std::fs::write(&initramfs, b"initramfs-blob").expect("write initramfs");

        let version = LinuxArtifactVersionJson {
            kernel: "6.12.11".to_string(),
            sha256_vmlinux: "00".repeat(32),
            sha256_initramfs: "11".repeat(32),
        };

        let error = validate_linux_artifacts(kernel.as_path(), initramfs.as_path(), &version)
            .expect_err("checksum mismatch should fail");
        let message = error.to_string();
        assert!(message.contains("checksum mismatch"));
    }

    #[test]
    fn descriptor_roundtrip_preserves_fields() {
        let tmp = tempdir().expect("tempdir");
        let descriptor_path = tmp.path().join("linux-test.linux.json");
        let descriptor = LinuxVmImageDescriptor {
            schema_version: LINUX_VM_IMAGE_DESCRIPTOR_SCHEMA_VERSION,
            image_name: "linux-test".to_string(),
            kernel_path: tmp.path().join("vmlinux"),
            initramfs_path: tmp.path().join("initramfs.img"),
            version_json_path: tmp.path().join("version.json"),
            disk_path: tmp.path().join("linux-test.img"),
            disk_size_gb: 64,
            linux_artifact_version: "6.12.11".to_string(),
            sha256_vmlinux: "aa".repeat(32),
            sha256_initramfs: "bb".repeat(32),
            created_at_unix_secs: 1_700_000_000,
        };

        write_linux_vm_image_descriptor(descriptor_path.as_path(), &descriptor).expect("write");
        let loaded = load_linux_vm_image_descriptor(descriptor_path.as_path()).expect("load");
        assert_eq!(loaded.image_name, descriptor.image_name);
        assert_eq!(loaded.kernel_path, descriptor.kernel_path);
        assert_eq!(loaded.initramfs_path, descriptor.initramfs_path);
        assert_eq!(loaded.disk_path, descriptor.disk_path);
        assert_eq!(loaded.disk_size_gb, descriptor.disk_size_gb);
        assert_eq!(
            loaded.linux_artifact_version,
            descriptor.linux_artifact_version
        );
    }

    #[test]
    fn provision_linux_disk_image_creates_and_reuses() {
        let tmp = tempdir().expect("tempdir");
        let disk_path = tmp.path().join("linux-test.img");
        let size_bytes = gib_to_bytes(1).expect("size bytes");

        let created = provision_linux_disk_image(disk_path.as_path(), size_bytes, false)
            .expect("create disk");
        assert_eq!(created, DiskProvisioningResult::Created);
        assert!(disk_path.is_file());
        let size = std::fs::metadata(&disk_path).expect("metadata").len();
        assert_eq!(size, size_bytes);

        let reused =
            provision_linux_disk_image(disk_path.as_path(), size_bytes, false).expect("reuse disk");
        assert_eq!(reused, DiskProvisioningResult::Reused);
    }

    #[test]
    fn provision_linux_disk_image_replaces_when_forced() {
        let tmp = tempdir().expect("tempdir");
        let disk_path = tmp.path().join("linux-test.img");
        std::fs::write(&disk_path, b"seed").expect("seed disk");

        let replaced = provision_linux_disk_image(
            disk_path.as_path(),
            gib_to_bytes(1).expect("size bytes"),
            true,
        )
        .expect("replace disk");
        assert_eq!(replaced, DiskProvisioningResult::Replaced);
        let size = std::fs::metadata(&disk_path).expect("metadata").len();
        assert_eq!(size, gib_to_bytes(1).expect("size bytes"));
    }

    #[test]
    fn provision_linux_disk_image_rejects_size_mismatch_without_force() {
        let tmp = tempdir().expect("tempdir");
        let disk_path = tmp.path().join("linux-test.img");
        std::fs::write(&disk_path, b"seed").expect("seed disk");

        let error = provision_linux_disk_image(
            disk_path.as_path(),
            gib_to_bytes(1).expect("size bytes"),
            false,
        )
        .expect_err("mismatch should fail");
        assert!(error.to_string().contains("exists with size"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_linux_vm_mount_spec_accepts_rw_and_ro_modes() {
        let (tag_rw, source_rw, ro_rw) =
            parse_linux_vm_mount_spec("repo:/tmp/workspace:rw").expect("parse rw");
        assert_eq!(tag_rw, "repo");
        assert_eq!(source_rw, PathBuf::from("/tmp/workspace"));
        assert!(!ro_rw);

        let (tag_ro, source_ro, ro_ro) =
            parse_linux_vm_mount_spec("cache:/tmp/cache:ro").expect("parse ro");
        assert_eq!(tag_ro, "cache");
        assert_eq!(source_ro, PathBuf::from("/tmp/cache"));
        assert!(ro_ro);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn parse_linux_vm_mount_spec_rejects_invalid_specs() {
        let missing_source =
            parse_linux_vm_mount_spec("repo").expect_err("missing source should fail");
        assert!(missing_source.to_string().contains("TAG:HOST_PATH"));

        let bad_mode =
            parse_linux_vm_mount_spec("repo:/tmp:xxx").expect_err("bad mode should fail");
        assert!(bad_mode.to_string().contains("unsupported mount mode"));
    }
}
