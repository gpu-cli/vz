//! OCI container runtime operations (top-level commands).
#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::process;
use std::time::Duration;

use clap::{Args, ValueEnum};
use std::fmt;
use tracing::info;

use vz_runtime_contract::{MountAccess, MountSpec, MountType, PortMapping, PortProtocol};

#[cfg(target_os = "macos")]
use std::time::{Instant, SystemTime, UNIX_EPOCH};
#[cfg(target_os = "macos")]
use tokio::time::sleep;

#[cfg(target_os = "macos")]
const DETACH_START_TIMEOUT: Duration = Duration::from_secs(12);
#[cfg(target_os = "macos")]
const DETACH_POLL_INTERVAL: Duration = Duration::from_millis(100);

// ── Shared container options ─────────────────────────────────────

/// Shared options for OCI container commands.
#[derive(Args, Debug, Clone, Default)]
pub struct ContainerOpts {
    /// OCI cache base directory.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Pre-downloaded rootfs bundle directory.
    #[arg(long)]
    pub bundle_dir: Option<PathBuf>,

    /// Kernel install cache directory.
    #[arg(long)]
    pub install_dir: Option<PathBuf>,

    /// Use credentials from local Docker credential configuration.
    #[arg(long, conflicts_with_all = ["username", "password"])]
    pub docker_config: bool,

    /// Registry username when using basic auth.
    #[arg(long, requires = "password", conflicts_with = "docker_config")]
    pub username: Option<String>,

    /// Registry password when using basic auth.
    #[arg(long, requires = "username", conflicts_with = "docker_config")]
    pub password: Option<String>,
}

// ── Execution mode ───────────────────────────────────────────────

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum ExecutionModeArg {
    /// Execute command directly via guest agent.
    #[value(name = "guest-exec")]
    GuestExec,
    /// Placeholder for OCI runtime inside the guest.
    #[value(name = "oci-runtime")]
    OciRuntime,
}

impl fmt::Display for ExecutionModeArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GuestExec => write!(f, "guest-exec"),
            Self::OciRuntime => write!(f, "oci-runtime"),
        }
    }
}

// ── Command argument structs ─────────────────────────────────────

#[derive(Args, Debug)]
pub struct PullArgs {
    /// Image reference, for example `ubuntu:24.04`.
    pub image: String,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct RunArgs {
    /// Image reference, for example `ubuntu:24.04`.
    pub image: String,

    /// Command and arguments to run. If omitted, image defaults are used.
    #[arg(last = true)]
    pub command: Vec<String>,

    /// Environment override (`KEY=VALUE`). Can be repeated.
    #[arg(long, value_name = "KEY=VALUE")]
    pub env: Vec<String>,

    /// Publish a host port to a container port (`HOST:CONTAINER[/PROTO]`).
    #[arg(short = 'p', long = "publish", value_name = "HOST:CONTAINER[/PROTO]")]
    pub publish: Vec<String>,

    /// Working directory in the container.
    #[arg(long)]
    pub workdir: Option<String>,

    /// User to execute the command as.
    #[arg(long)]
    pub user: Option<String>,

    /// Number of vCPUs.
    #[arg(long)]
    pub cpus: Option<u8>,

    /// Memory in MB.
    #[arg(long)]
    pub memory_mb: Option<u64>,

    /// Disable network access for this run.
    #[arg(long)]
    pub no_network: bool,

    /// Execution timeout in seconds.
    #[arg(long)]
    pub timeout_secs: Option<u64>,

    /// Optional file path for guest serial console output.
    #[arg(long)]
    pub serial_log_file: Option<PathBuf>,

    /// Run container in background and return immediately.
    #[arg(long)]
    pub detach: bool,

    /// Internal flag used by detached child process.
    #[arg(long, hide = true)]
    pub internal_detached_child: bool,

    /// Internal explicit container identifier used by detached runs.
    #[arg(long, hide = true)]
    pub internal_container_id: Option<String>,

    /// Bind mount a host directory into the container (`SOURCE:TARGET[:ro]`).
    #[arg(long = "volume", value_name = "SOURCE:TARGET[:ro]")]
    pub volume: Vec<String>,

    /// Execution strategy for workload startup.
    #[arg(long, default_value_t = ExecutionModeArg::GuestExec)]
    pub execution_mode: ExecutionModeArg,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct CreateArgs {
    /// Image reference, for example `ubuntu:24.04`.
    pub image: String,

    /// Command and arguments for the container init process.
    #[arg(last = true)]
    pub command: Vec<String>,

    /// Environment override (`KEY=VALUE`). Can be repeated.
    #[arg(long, value_name = "KEY=VALUE")]
    pub env: Vec<String>,

    /// Publish a host port to a container port (`HOST:CONTAINER[/PROTO]`).
    #[arg(short = 'p', long = "publish", value_name = "HOST:CONTAINER[/PROTO]")]
    pub publish: Vec<String>,

    /// Bind mount a host directory into the container (`SOURCE:TARGET[:ro]`).
    #[arg(long = "volume", value_name = "SOURCE:TARGET[:ro]")]
    pub volume: Vec<String>,

    /// Working directory in the container.
    #[arg(long)]
    pub workdir: Option<String>,

    /// User to execute the command as.
    #[arg(long)]
    pub user: Option<String>,

    /// Number of vCPUs.
    #[arg(long)]
    pub cpus: Option<u8>,

    /// Memory in MB.
    #[arg(long)]
    pub memory_mb: Option<u64>,

    /// Disable network access for this container.
    #[arg(long)]
    pub no_network: bool,

    /// Optional file path for guest serial console output.
    #[arg(long)]
    pub serial_log_file: Option<PathBuf>,

    /// Explicit container identifier.
    #[arg(long)]
    pub name: Option<String>,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct ExecArgs {
    /// Container identifier.
    pub id: String,

    /// Command and arguments to execute.
    #[arg(last = true)]
    pub command: Vec<String>,

    /// Environment override (`KEY=VALUE`). Can be repeated.
    #[arg(long, value_name = "KEY=VALUE")]
    pub env: Vec<String>,

    /// Working directory inside the container.
    #[arg(long)]
    pub workdir: Option<String>,

    /// User to execute the command as.
    #[arg(long)]
    pub user: Option<String>,

    /// Execution timeout in seconds.
    #[arg(long)]
    pub timeout_secs: Option<u64>,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct ImagesArgs {
    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct PruneArgs {
    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct PsArgs {
    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct StopArgs {
    /// Container identifier.
    pub id: String,

    /// Force immediate termination (SIGKILL).
    #[arg(long)]
    pub force: bool,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct RmArgs {
    /// Container identifier.
    pub id: String,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

#[derive(Args, Debug)]
pub struct LogsArgs {
    /// Container identifier.
    pub id: String,

    /// Follow log output (poll for new lines).
    #[arg(short, long)]
    pub follow: bool,

    /// Number of lines to show from the end of the logs.
    #[arg(short = 'n', long, default_value_t = 100)]
    pub tail: u32,

    #[command(flatten)]
    pub opts: ContainerOpts,
}

// ── Per-command entry points ─────────────────────────────────────

/// Entry point for `vz pull`.
pub async fn run_pull(args: PullArgs) -> anyhow::Result<()> {
    super::image::run_pull_stream(args).await
}

/// Entry point for `vz run`.
pub async fn run_container(args: RunArgs) -> anyhow::Result<()> {
    let _ = args;
    anyhow::bail!(
        "unsupported_operation: surface=oci; operation=run; reason=legacy local-runtime path removed in daemon-only mode; guidance=use daemon-backed sandbox or stack commands"
    )
}

/// Entry point for `vz create`.
pub async fn run_create(args: CreateArgs) -> anyhow::Result<()> {
    let _ = args;
    anyhow::bail!(
        "unsupported_operation: surface=oci; operation=create; reason=legacy local-runtime path removed in daemon-only mode; guidance=use daemon-backed stack workflows"
    )
}

/// Entry point for `vz exec`.
pub async fn run_exec(args: ExecArgs) -> anyhow::Result<()> {
    let _ = args;
    anyhow::bail!(
        "unsupported_operation: surface=oci; operation=exec; reason=legacy local-runtime path removed in daemon-only mode; guidance=use daemon-backed execution APIs"
    )
}

/// Entry point for `vz ps`.
pub async fn run_ps(args: PsArgs) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let runtime = build_macos_runtime(&args.opts)?;
        list_containers(&runtime)
    }
    #[cfg(target_os = "linux")]
    {
        let backend = build_linux_backend(&args.opts);
        list_containers_linux(&backend)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = args;
        anyhow::bail!("Container commands are not supported on this platform")
    }
}

/// Entry point for `vz stop`.
pub async fn run_stop(args: StopArgs) -> anyhow::Result<()> {
    let _ = args;
    anyhow::bail!(
        "unsupported_operation: surface=oci; operation=stop; reason=legacy local-runtime path removed in daemon-only mode; guidance=use daemon-backed stack service controls"
    )
}

/// Entry point for `vz rm`.
pub async fn run_rm(args: RmArgs) -> anyhow::Result<()> {
    let _ = args;
    anyhow::bail!(
        "unsupported_operation: surface=oci; operation=rm; reason=legacy local-runtime path removed in daemon-only mode; guidance=use daemon-backed stack/sandbox removal flows"
    )
}

/// Entry point for `vz logs`.
pub async fn run_logs(args: LogsArgs) -> anyhow::Result<()> {
    #[cfg(target_os = "macos")]
    {
        let runtime = build_macos_runtime(&args.opts)?;
        container_logs(&runtime, args).await
    }
    #[cfg(target_os = "linux")]
    {
        let backend = build_linux_backend(&args.opts);
        container_logs_linux(&backend, args).await
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = args;
        anyhow::bail!("Container commands are not supported on this platform")
    }
}

// ── macOS implementation (uses vz_oci_macos::Runtime directly) ──

#[cfg(target_os = "macos")]
pub(crate) fn build_macos_runtime_config(
    opts: &ContainerOpts,
) -> anyhow::Result<vz_oci_macos::RuntimeConfig> {
    if opts.username.is_some() && opts.password.is_none() {
        anyhow::bail!("--username requires --password");
    }

    if opts.password.is_some() && opts.username.is_none() {
        anyhow::bail!("--password requires --username");
    }

    let mut config = vz_oci_macos::RuntimeConfig::default();
    if let Some(path) = &opts.data_dir {
        config.data_dir = path.clone();
    }
    if let Some(path) = &opts.bundle_dir {
        config.linux_bundle_dir = Some(path.clone());
    }
    if let Some(path) = &opts.install_dir {
        config.linux_install_dir = Some(path.clone());
    }

    config.auth = match (opts.docker_config, &opts.username, &opts.password) {
        (true, _, _) => vz_oci_macos::Auth::DockerConfig,
        (false, Some(username), Some(password)) => vz_oci_macos::Auth::Basic {
            username: username.clone(),
            password: password.clone(),
        },
        _ => vz_oci_macos::Auth::Anonymous,
    };

    Ok(config)
}

#[cfg(target_os = "macos")]
fn build_macos_runtime(opts: &ContainerOpts) -> anyhow::Result<vz_oci_macos::Runtime> {
    Ok(vz_oci_macos::Runtime::new(build_macos_runtime_config(
        opts,
    )?))
}

#[cfg(target_os = "macos")]
async fn pull_image(runtime: &vz_oci_macos::Runtime, args: PullArgs) -> anyhow::Result<()> {
    info!(image = %args.image, "pulling OCI image");
    let image_id = runtime.pull(&args.image).await?;
    println!(
        "Pulled {image} as {id}",
        image = args.image,
        id = image_id.0
    );
    Ok(())
}

#[cfg(target_os = "macos")]
async fn run_image(runtime: vz_oci_macos::Runtime, args: RunArgs) -> anyhow::Result<()> {
    if args.detach && !args.internal_detached_child {
        return run_image_detached_parent(&runtime, &args).await;
    }

    let run_config = build_run_config(&args)?;
    info!(image = %args.image, command = ?args.command, "running OCI container");

    let output = runtime.run(&args.image, run_config).await?;

    if !output.stdout.is_empty() {
        print!("{}", output.stdout);
    }

    if !output.stderr.is_empty() {
        eprint!("{}", output.stderr);
    }

    if output.exit_code != 0 {
        println!("container exited with code {}", output.exit_code);
        process::exit(output.exit_code.rem_euclid(256));
    }

    println!("container completed successfully");
    Ok(())
}

#[cfg(target_os = "macos")]
async fn run_image_detached_parent(
    runtime: &vz_oci_macos::Runtime,
    args: &RunArgs,
) -> anyhow::Result<()> {
    let container_id = args
        .internal_container_id
        .clone()
        .unwrap_or_else(generate_detached_container_id);

    let executable = std::env::current_exe()?;
    let mut child = process::Command::new(executable)
        .args(std::env::args_os().skip(1))
        .arg("--internal-detached-child")
        .arg("--internal-container-id")
        .arg(container_id.clone())
        .stdin(process::Stdio::null())
        .stdout(process::Stdio::null())
        .stderr(process::Stdio::null())
        .spawn()?;

    let started = Instant::now();
    loop {
        if let Some(status) = child.try_wait()? {
            anyhow::bail!(
                "detached run process exited before startup for container {container_id}: {status}"
            );
        }

        if let Some(container) = runtime
            .list_containers()?
            .into_iter()
            .find(|container| container.id == container_id)
        {
            match container.status {
                vz_oci_macos::ContainerStatus::Running => {
                    println!("container running in background: {container_id}");
                    return Ok(());
                }
                vz_oci_macos::ContainerStatus::Stopped { exit_code } => {
                    anyhow::bail!(
                        "detached container {container_id} stopped during startup with exit code {exit_code}"
                    );
                }
                vz_oci_macos::ContainerStatus::Created => {}
            }
        }

        if started.elapsed() >= DETACH_START_TIMEOUT {
            anyhow::bail!(
                "timed out waiting for detached container {container_id} to reach running state"
            );
        }

        sleep(DETACH_POLL_INTERVAL).await;
    }
}

#[cfg(target_os = "macos")]
async fn create_container(runtime: &vz_oci_macos::Runtime, args: CreateArgs) -> anyhow::Result<()> {
    let env = parse_env_vars(&args.env)?;
    let ports = parse_port_mappings(&args.publish)?;
    let mounts = parse_volume_mounts(&args.volume)?;
    let network_enabled = if args.no_network { Some(false) } else { None };

    let run_config = vz_oci_macos::RunConfig {
        cmd: args.command.clone(),
        working_dir: args.workdir,
        env,
        user: args.user,
        ports,
        mounts,
        cpus: args.cpus,
        memory_mb: args.memory_mb,
        network_enabled,
        serial_log_file: args.serial_log_file,
        execution_mode: vz_oci_macos::ExecutionMode::OciRuntime,
        timeout: None,
        container_id: args.name,
        init_process: if args.command.is_empty() {
            None
        } else {
            Some(args.command)
        },
        oci_annotations: Vec::new(),
        extra_hosts: Vec::new(),
        network_namespace_path: None,
        cpu_quota: None,
        cpu_period: None,
        capture_logs: true,
        cap_add: Vec::new(),
        cap_drop: Vec::new(),
        privileged: false,
        read_only_rootfs: false,
        sysctls: HashMap::new(),
        ulimits: Vec::new(),
        pids_limit: None,
        hostname: None,
        domainname: None,
        stop_signal: None,
        stop_grace_period_secs: None,
        share_host_network: false,
        mount_tag_offset: 0,
    };

    info!(image = %args.image, "creating long-lived container");
    let container_id = runtime.create_container(&args.image, run_config).await?;
    println!("{container_id}");
    Ok(())
}

#[cfg(target_os = "macos")]
async fn exec_container(runtime: &vz_oci_macos::Runtime, args: ExecArgs) -> anyhow::Result<()> {
    let env = parse_env_vars(&args.env)?;
    let timeout = args.timeout_secs.map(Duration::from_secs);

    let exec_config = vz_oci_macos::ExecConfig {
        execution_id: None,
        cmd: args.command,
        working_dir: args.workdir,
        env,
        user: args.user,
        pty: false,
        term_rows: None,
        term_cols: None,
        timeout,
    };

    let output = runtime.exec_container(&args.id, exec_config).await?;

    if !output.stdout.is_empty() {
        print!("{}", output.stdout);
    }
    if !output.stderr.is_empty() {
        eprint!("{}", output.stderr);
    }
    if output.exit_code != 0 {
        process::exit(output.exit_code.rem_euclid(256));
    }

    Ok(())
}

#[cfg(target_os = "macos")]
fn list_containers(runtime: &vz_oci_macos::Runtime) -> anyhow::Result<()> {
    let containers = runtime.list_containers()?;

    if containers.is_empty() {
        println!("No containers tracked");
        return Ok(());
    }

    println!("{:<20} {:<35} {:<10} CREATED", "ID", "IMAGE", "STATUS");
    println!("{}", "-".repeat(90));

    for container in containers {
        let status = match container.status {
            vz_oci_macos::ContainerStatus::Created => "created".to_string(),
            vz_oci_macos::ContainerStatus::Running => "running".to_string(),
            vz_oci_macos::ContainerStatus::Stopped { exit_code } => {
                format!("stopped (exit {exit_code})")
            }
        };

        println!(
            "{:<20} {:<35} {:<10} {}",
            container.id, container.image, status, container.created_unix_secs
        );
    }

    Ok(())
}

#[cfg(target_os = "macos")]
async fn stop_container(runtime: &vz_oci_macos::Runtime, args: StopArgs) -> anyhow::Result<()> {
    let container = runtime
        .stop_container(&args.id, args.force, None, None)
        .await?;
    match container.status {
        vz_oci_macos::ContainerStatus::Running => {
            println!("Container {} remains running", args.id);
        }
        vz_oci_macos::ContainerStatus::Created => {
            println!("Container {} is created but not running", args.id);
        }
        vz_oci_macos::ContainerStatus::Stopped { exit_code } => {
            println!("Stopped container {} (exit {exit_code})", args.id);
        }
    }

    Ok(())
}

#[cfg(target_os = "macos")]
async fn remove_container(runtime: &vz_oci_macos::Runtime, args: RmArgs) -> anyhow::Result<()> {
    runtime.remove_container(&args.id).await?;
    println!("Removed container {id}", id = args.id);
    Ok(())
}

#[cfg(target_os = "macos")]
async fn container_logs(runtime: &vz_oci_macos::Runtime, args: LogsArgs) -> anyhow::Result<()> {
    let log_file = "/var/log/vz-oci/output.log";

    // Initial fetch: bounded tail -n <count>.
    let tail_n = args.tail.to_string();
    let exec_config = vz_oci_macos::ExecConfig {
        execution_id: None,
        cmd: vec!["tail".into(), "-n".into(), tail_n, log_file.into()],
        working_dir: None,
        env: vec![],
        user: None,
        pty: false,
        term_rows: None,
        term_cols: None,
        timeout: Some(Duration::from_secs(5)),
    };

    let output = runtime.exec_container(&args.id, exec_config).await?;
    if output.exit_code == 0 && !output.stdout.is_empty() {
        print!("{}", output.stdout);
    }

    if !args.follow {
        return Ok(());
    }

    // Follow mode: track byte offset, poll with tail -c +<offset>.
    let size_config = vz_oci_macos::ExecConfig {
        execution_id: None,
        cmd: vec!["wc".into(), "-c".into(), log_file.into()],
        working_dir: None,
        env: vec![],
        user: None,
        pty: false,
        term_rows: None,
        term_cols: None,
        timeout: Some(Duration::from_secs(5)),
    };

    let size_output = runtime.exec_container(&args.id, size_config).await?;
    let mut offset: u64 = size_output
        .stdout
        .split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    loop {
        sleep(Duration::from_secs(1)).await;

        let offset_arg = format!("+{}", offset + 1);
        let poll_config = vz_oci_macos::ExecConfig {
            execution_id: None,
            cmd: vec!["tail".into(), "-c".into(), offset_arg, log_file.into()],
            working_dir: None,
            env: vec![],
            user: None,
            pty: false,
            term_rows: None,
            term_cols: None,
            timeout: Some(Duration::from_secs(5)),
        };

        let poll_output = runtime.exec_container(&args.id, poll_config).await?;
        if poll_output.exit_code == 0 && !poll_output.stdout.is_empty() {
            print!("{}", poll_output.stdout);
            offset += poll_output.stdout.len() as u64;
        }
    }
}

#[cfg(target_os = "macos")]
fn build_run_config(args: &RunArgs) -> anyhow::Result<vz_oci_macos::RunConfig> {
    let env = parse_env_vars(&args.env)?;
    let ports = parse_port_mappings(&args.publish)?;
    let mounts = parse_volume_mounts(&args.volume)?;

    let network_enabled = if args.no_network { Some(false) } else { None };
    let timeout = args.timeout_secs.map(Duration::from_secs);

    Ok(vz_oci_macos::RunConfig {
        cmd: args.command.clone(),
        working_dir: args.workdir.clone(),
        env,
        user: args.user.clone(),
        ports,
        mounts,
        cpus: args.cpus,
        memory_mb: args.memory_mb,
        network_enabled,
        serial_log_file: args.serial_log_file.clone(),
        execution_mode: args.execution_mode.into(),
        timeout,
        container_id: args.internal_container_id.clone(),
        init_process: None,
        oci_annotations: Vec::new(),
        extra_hosts: Vec::new(),
        network_namespace_path: None,
        cpu_quota: None,
        cpu_period: None,
        capture_logs: false,
        cap_add: Vec::new(),
        cap_drop: Vec::new(),
        privileged: false,
        read_only_rootfs: false,
        sysctls: HashMap::new(),
        ulimits: Vec::new(),
        pids_limit: None,
        hostname: None,
        domainname: None,
        stop_signal: None,
        stop_grace_period_secs: None,
        share_host_network: false,
        mount_tag_offset: 0,
    })
}

#[cfg(target_os = "macos")]
impl From<ExecutionModeArg> for vz_oci_macos::ExecutionMode {
    fn from(value: ExecutionModeArg) -> Self {
        match value {
            ExecutionModeArg::GuestExec => vz_oci_macos::ExecutionMode::GuestExec,
            ExecutionModeArg::OciRuntime => vz_oci_macos::ExecutionMode::OciRuntime,
        }
    }
}

#[cfg(target_os = "macos")]
fn generate_detached_container_id() -> String {
    let millis = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis(),
        Err(_) => 0,
    };

    format!("ctr-{millis}-{}", process::id())
}

// ── Linux implementation (uses LinuxNativeBackend) ────────────────

#[cfg(target_os = "linux")]
fn build_linux_backend(opts: &ContainerOpts) -> vz_linux_native::LinuxNativeBackend {
    use vz_linux_native::{LinuxNativeBackend, LinuxNativeConfig};

    let mut config = LinuxNativeConfig::default();
    if let Some(ref path) = opts.data_dir {
        config.data_dir = path.clone();
    }
    LinuxNativeBackend::new(config)
}

#[cfg(target_os = "linux")]
async fn pull_image_linux(
    backend: &vz_linux_native::LinuxNativeBackend,
    args: PullArgs,
) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    info!(image = %args.image, "pulling OCI image");
    let image_id = backend
        .pull(&args.image)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("Pulled {image} as {id}", image = args.image, id = image_id);
    Ok(())
}

#[cfg(target_os = "linux")]
async fn run_image_linux(
    backend: &vz_linux_native::LinuxNativeBackend,
    args: RunArgs,
) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    let env = parse_env_vars(&args.env)?;
    let ports = parse_port_mappings(&args.publish)?;
    let mounts = parse_volume_mounts(&args.volume)?;
    let network_enabled = if args.no_network { Some(false) } else { None };
    let timeout = args.timeout_secs.map(Duration::from_secs);

    let config = vz_runtime_contract::RunConfig {
        cmd: args.command.clone(),
        working_dir: args.workdir.clone(),
        env,
        user: args.user.clone(),
        ports,
        mounts,
        cpus: args.cpus,
        memory_mb: args.memory_mb,
        network_enabled,
        timeout,
        container_id: args.internal_container_id.clone(),
        ..Default::default()
    };

    info!(image = %args.image, command = ?args.command, "running OCI container");
    let output = backend
        .run(&args.image, config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if !output.stdout.is_empty() {
        print!("{}", output.stdout);
    }
    if !output.stderr.is_empty() {
        eprint!("{}", output.stderr);
    }
    if output.exit_code != 0 {
        println!("container exited with code {}", output.exit_code);
        process::exit(output.exit_code.rem_euclid(256));
    }
    println!("container completed successfully");
    Ok(())
}

#[cfg(target_os = "linux")]
async fn create_container_linux(
    backend: &vz_linux_native::LinuxNativeBackend,
    args: CreateArgs,
) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    let env = parse_env_vars(&args.env)?;
    let ports = parse_port_mappings(&args.publish)?;
    let mounts = parse_volume_mounts(&args.volume)?;
    let network_enabled = if args.no_network { Some(false) } else { None };

    let config = vz_runtime_contract::RunConfig {
        cmd: args.command.clone(),
        working_dir: args.workdir,
        env,
        user: args.user,
        ports,
        mounts,
        cpus: args.cpus,
        memory_mb: args.memory_mb,
        network_enabled,
        container_id: args.name,
        init_process: if args.command.is_empty() {
            None
        } else {
            Some(args.command)
        },
        ..Default::default()
    };

    info!(image = %args.image, "creating long-lived container");
    let container_id = backend
        .create_container(&args.image, config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("{container_id}");
    Ok(())
}

#[cfg(target_os = "linux")]
async fn exec_container_linux(
    backend: &vz_linux_native::LinuxNativeBackend,
    args: ExecArgs,
) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    let env = parse_env_vars(&args.env)?;
    let timeout = args.timeout_secs.map(Duration::from_secs);

    let config = vz_runtime_contract::ExecConfig {
        execution_id: None,
        cmd: args.command,
        working_dir: args.workdir,
        env,
        user: args.user,
        pty: false,
        term_rows: None,
        term_cols: None,
        timeout,
    };

    let output = backend
        .exec_container(&args.id, config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if !output.stdout.is_empty() {
        print!("{}", output.stdout);
    }
    if !output.stderr.is_empty() {
        eprint!("{}", output.stderr);
    }
    if output.exit_code != 0 {
        process::exit(output.exit_code.rem_euclid(256));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn list_containers_linux(backend: &vz_linux_native::LinuxNativeBackend) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    let containers = backend
        .list_containers()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    if containers.is_empty() {
        println!("No containers tracked");
        return Ok(());
    }

    println!("{:<20} {:<35} {:<10} CREATED", "ID", "IMAGE", "STATUS");
    println!("{}", "-".repeat(90));

    for container in containers {
        let status = match container.status {
            vz_runtime_contract::ContainerStatus::Created => "created".to_string(),
            vz_runtime_contract::ContainerStatus::Running => "running".to_string(),
            vz_runtime_contract::ContainerStatus::Stopped { exit_code } => {
                format!("stopped (exit {exit_code})")
            }
        };
        println!(
            "{:<20} {:<35} {:<10} {}",
            container.id, container.image, status, container.created_unix_secs
        );
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn stop_container_linux(
    backend: &vz_linux_native::LinuxNativeBackend,
    args: StopArgs,
) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    let container = backend
        .stop_container(&args.id, args.force, None, None)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    match container.status {
        vz_runtime_contract::ContainerStatus::Running => {
            println!("Container {} remains running", args.id);
        }
        vz_runtime_contract::ContainerStatus::Created => {
            println!("Container {} is created but not running", args.id);
        }
        vz_runtime_contract::ContainerStatus::Stopped { exit_code } => {
            println!("Stopped container {} (exit {exit_code})", args.id);
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
async fn remove_container_linux(
    backend: &vz_linux_native::LinuxNativeBackend,
    args: RmArgs,
) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    backend
        .remove_container(&args.id)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    println!("Removed container {id}", id = args.id);
    Ok(())
}

#[cfg(target_os = "linux")]
async fn container_logs_linux(
    backend: &vz_linux_native::LinuxNativeBackend,
    args: LogsArgs,
) -> anyhow::Result<()> {
    use vz_runtime_contract::RuntimeBackend;

    let log_file = "/var/log/vz-oci/output.log";

    // Initial fetch: bounded tail -n <count>.
    let tail_n = args.tail.to_string();
    let exec_config = vz_runtime_contract::ExecConfig {
        execution_id: None,
        cmd: vec!["tail".into(), "-n".into(), tail_n, log_file.into()],
        working_dir: None,
        env: vec![],
        user: None,
        pty: false,
        term_rows: None,
        term_cols: None,
        timeout: Some(Duration::from_secs(5)),
    };

    let output = backend
        .exec_container(&args.id, exec_config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    if output.exit_code == 0 && !output.stdout.is_empty() {
        print!("{}", output.stdout);
    }

    if !args.follow {
        return Ok(());
    }

    // Follow mode: track byte offset, poll with tail -c +<offset>.
    let size_config = vz_runtime_contract::ExecConfig {
        execution_id: None,
        cmd: vec!["wc".into(), "-c".into(), log_file.into()],
        working_dir: None,
        env: vec![],
        user: None,
        pty: false,
        term_rows: None,
        term_cols: None,
        timeout: Some(Duration::from_secs(5)),
    };

    let size_output = backend
        .exec_container(&args.id, size_config)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let mut offset: u64 = size_output
        .stdout
        .split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;

        let offset_arg = format!("+{}", offset + 1);
        let poll_config = vz_runtime_contract::ExecConfig {
            execution_id: None,
            cmd: vec!["tail".into(), "-c".into(), offset_arg, log_file.into()],
            working_dir: None,
            env: vec![],
            user: None,
            pty: false,
            term_rows: None,
            term_cols: None,
            timeout: Some(Duration::from_secs(5)),
        };

        let poll_output = backend
            .exec_container(&args.id, poll_config)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        if poll_output.exit_code == 0 && !poll_output.stdout.is_empty() {
            print!("{}", poll_output.stdout);
            offset += poll_output.stdout.len() as u64;
        }
    }
}

// ── Cross-platform parsing helpers ────────────────────────────────

fn parse_env_vars(vars: &[String]) -> anyhow::Result<Vec<(String, String)>> {
    let mut env = Vec::with_capacity(vars.len());

    for pair in vars {
        let Some((key, value)) = pair.split_once('=') else {
            anyhow::bail!("invalid --env value '{pair}', expected KEY=VALUE");
        };
        env.push((key.to_string(), value.to_string()));
    }

    Ok(env)
}

fn parse_port_mappings(specs: &[String]) -> anyhow::Result<Vec<PortMapping>> {
    let mut ports = Vec::with_capacity(specs.len());
    for spec in specs {
        ports.push(parse_port_mapping(spec)?);
    }
    Ok(ports)
}

fn parse_port_mapping(spec: &str) -> anyhow::Result<PortMapping> {
    let (ports_part, protocol_part) = match spec.split_once('/') {
        Some((ports, protocol)) => (ports, protocol),
        None => (spec, "tcp"),
    };

    let protocol = match protocol_part.to_ascii_lowercase().as_str() {
        "tcp" => PortProtocol::Tcp,
        "udp" => PortProtocol::Udp,
        _ => anyhow::bail!(
            "invalid --publish protocol '{protocol_part}' in '{spec}', expected tcp or udp"
        ),
    };

    let mut parts = ports_part.split(':');
    let Some(host_str) = parts.next() else {
        anyhow::bail!("invalid --publish value '{spec}', expected HOST:CONTAINER[/PROTO]");
    };
    let Some(container_str) = parts.next() else {
        anyhow::bail!("invalid --publish value '{spec}', expected HOST:CONTAINER[/PROTO]");
    };

    if parts.next().is_some() {
        anyhow::bail!(
            "invalid --publish value '{spec}', host IP is not supported yet; expected HOST:CONTAINER[/PROTO]"
        );
    }

    let host = host_str.parse::<u16>().map_err(|error| {
        anyhow::anyhow!("invalid host port '{host_str}' in --publish '{spec}': {error}")
    })?;
    let container = container_str.parse::<u16>().map_err(|error| {
        anyhow::anyhow!("invalid container port '{container_str}' in --publish '{spec}': {error}")
    })?;

    Ok(PortMapping {
        host,
        container,
        protocol,
        target_host: None,
    })
}

fn parse_volume_mounts(specs: &[String]) -> anyhow::Result<Vec<MountSpec>> {
    let mut mounts = Vec::with_capacity(specs.len());
    for spec in specs {
        mounts.push(parse_volume_mount(spec)?);
    }
    Ok(mounts)
}

fn parse_volume_mount(spec: &str) -> anyhow::Result<MountSpec> {
    let parts: Vec<&str> = spec.split(':').collect();

    let (source, target, access) = match parts.len() {
        2 => (parts[0], parts[1], MountAccess::ReadWrite),
        3 => {
            let access = match parts[2] {
                "ro" => MountAccess::ReadOnly,
                "rw" => MountAccess::ReadWrite,
                other => anyhow::bail!(
                    "invalid --volume access mode '{other}' in '{spec}', expected 'ro' or 'rw'"
                ),
            };
            (parts[0], parts[1], access)
        }
        _ => anyhow::bail!(
            "invalid --volume value '{spec}', expected SOURCE:TARGET or SOURCE:TARGET:ro"
        ),
    };

    if source.is_empty() {
        anyhow::bail!("invalid --volume value '{spec}', source path must not be empty");
    }
    if target.is_empty() || !target.starts_with('/') {
        anyhow::bail!("invalid --volume value '{spec}', target must be an absolute path");
    }

    Ok(MountSpec {
        source: Some(PathBuf::from(source)),
        target: PathBuf::from(target),
        mount_type: MountType::Bind,
        access,
        subpath: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    fn run_async<F>(future: F) -> anyhow::Result<()>
    where
        F: Future<Output = anyhow::Result<()>>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        runtime.block_on(future)
    }

    fn assert_daemon_only_error(error: anyhow::Error, operation: &str) {
        let message = error.to_string();
        assert!(message.contains("unsupported_operation: surface=oci"));
        assert!(message.contains(&format!("operation={operation}")));
        assert!(message.contains("daemon-only mode"));
    }

    fn default_opts() -> ContainerOpts {
        ContainerOpts::default()
    }

    #[test]
    fn parse_port_mapping_defaults_to_tcp() {
        let mapping = parse_port_mapping("8080:80");
        match mapping {
            Ok(mapping) => {
                assert_eq!(mapping.host, 8080);
                assert_eq!(mapping.container, 80);
                assert_eq!(mapping.protocol, PortProtocol::Tcp);
            }
            Err(error) => panic!("unexpected parse error: {error}"),
        }
    }

    #[test]
    fn parse_port_mapping_accepts_udp_suffix() {
        let mapping = parse_port_mapping("5353:5353/udp");
        match mapping {
            Ok(mapping) => {
                assert_eq!(mapping.host, 5353);
                assert_eq!(mapping.container, 5353);
                assert_eq!(mapping.protocol, PortProtocol::Udp);
            }
            Err(error) => panic!("unexpected parse error: {error}"),
        }
    }

    #[test]
    fn parse_port_mapping_rejects_host_ip_prefix() {
        let mapping = parse_port_mapping("127.0.0.1:8080:80");
        assert!(mapping.is_err());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn build_run_config_sets_internal_container_id() {
        let args = RunArgs {
            image: "nginx:latest".to_string(),
            command: vec!["echo".to_string(), "hello".to_string()],
            env: vec![],
            publish: vec![],
            volume: vec![],
            workdir: None,
            user: None,
            cpus: None,
            memory_mb: None,
            no_network: false,
            timeout_secs: None,
            serial_log_file: None,
            detach: false,
            internal_detached_child: false,
            internal_container_id: Some("container-123".to_string()),
            execution_mode: ExecutionModeArg::GuestExec,
            opts: ContainerOpts::default(),
        };

        let run_config = build_run_config(&args).expect("run config should build");
        assert_eq!(run_config.container_id, Some("container-123".to_string()));
    }

    #[test]
    fn parse_volume_mount_bind_rw() {
        let mount = parse_volume_mount("/host/path:/container/path").unwrap();
        assert_eq!(mount.source, Some(PathBuf::from("/host/path")));
        assert_eq!(mount.target, PathBuf::from("/container/path"));
        assert_eq!(mount.mount_type, MountType::Bind);
        assert_eq!(mount.access, MountAccess::ReadWrite);
    }

    #[test]
    fn parse_volume_mount_bind_ro() {
        let mount = parse_volume_mount("/host/path:/container/path:ro").unwrap();
        assert_eq!(mount.source, Some(PathBuf::from("/host/path")));
        assert_eq!(mount.target, PathBuf::from("/container/path"));
        assert_eq!(mount.access, MountAccess::ReadOnly);
    }

    #[test]
    fn parse_volume_mount_bind_explicit_rw() {
        let mount = parse_volume_mount("/src:/dst:rw").unwrap();
        assert_eq!(mount.access, MountAccess::ReadWrite);
    }

    #[test]
    fn parse_volume_mount_rejects_relative_target() {
        let result = parse_volume_mount("/host:relative");
        assert!(result.is_err());
    }

    #[test]
    fn parse_volume_mount_rejects_empty_source() {
        let result = parse_volume_mount(":/container/path");
        assert!(result.is_err());
    }

    #[test]
    fn parse_volume_mount_rejects_invalid_access_mode() {
        let result = parse_volume_mount("/host:/container:wx");
        assert!(result.is_err());
    }

    #[test]
    fn parse_volume_mount_rejects_bare_path() {
        let result = parse_volume_mount("/just/one/path");
        assert!(result.is_err());
    }

    #[test]
    fn run_container_fails_closed_in_daemon_only_mode() {
        let args = RunArgs {
            image: "alpine:latest".to_string(),
            command: vec!["echo".to_string(), "hi".to_string()],
            env: Vec::new(),
            publish: Vec::new(),
            workdir: None,
            user: None,
            cpus: None,
            memory_mb: None,
            no_network: false,
            timeout_secs: None,
            serial_log_file: None,
            detach: false,
            internal_detached_child: false,
            internal_container_id: None,
            volume: Vec::new(),
            execution_mode: ExecutionModeArg::GuestExec,
            opts: default_opts(),
        };
        let error = run_async(run_container(args)).expect_err("run should fail-closed");
        assert_daemon_only_error(error, "run");
    }

    #[test]
    fn run_exec_fails_closed_in_daemon_only_mode() {
        let args = ExecArgs {
            id: "ctr-test".to_string(),
            command: vec!["true".to_string()],
            env: Vec::new(),
            workdir: None,
            user: None,
            timeout_secs: None,
            opts: default_opts(),
        };
        let error = run_async(run_exec(args)).expect_err("exec should fail-closed");
        assert_daemon_only_error(error, "exec");
    }

    #[test]
    fn run_rm_fails_closed_in_daemon_only_mode() {
        let args = RmArgs {
            id: "ctr-test".to_string(),
            opts: default_opts(),
        };
        let error = run_async(run_rm(args)).expect_err("rm should fail-closed");
        assert_daemon_only_error(error, "rm");
    }
}
