//! `vz oci` -- OCI runtime operations.

use std::path::PathBuf;
use std::process;
use std::time::Duration;

use clap::{Args, Subcommand};
use tracing::info;

use vz_oci::{PortMapping, PortProtocol, RunConfig};

/// OCI runtime top-level command and shared options.
#[derive(Args, Debug)]
pub struct OciArgs {
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

    /// OCI operation.
    #[command(subcommand)]
    pub action: OciCommand,
}

/// OCI-specific operations.
#[derive(Subcommand, Debug)]
pub enum OciCommand {
    /// Pull and cache an OCI image locally.
    Pull(PullArgs),

    /// Run a container from an OCI image.
    Run(RunArgs),

    /// List cached OCI images.
    Images,

    /// Remove stale image and layer artifacts.
    Prune,

    /// List known containers from OCI metadata.
    Ps,

    /// Remove container metadata and rootfs artifacts.
    Rm(RmArgs),
}

#[derive(Args, Debug)]
pub struct PullArgs {
    /// Image reference, for example `ubuntu:24.04`.
    pub image: String,
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
}

#[derive(Args, Debug)]
pub struct RmArgs {
    /// Container identifier.
    pub id: String,
}

/// Entry point for `vz oci`.
pub async fn run(args: OciArgs) -> anyhow::Result<()> {
    let runtime = build_runtime(&args)?;

    match args.action {
        OciCommand::Pull(args) => pull_image(&runtime, args).await,
        OciCommand::Run(args) => run_image(runtime, args).await,
        OciCommand::Images => list_images(&runtime),
        OciCommand::Prune => prune_images(&runtime),
        OciCommand::Ps => list_containers(&runtime),
        OciCommand::Rm(args) => remove_container(&runtime, args),
    }
}

fn build_runtime(args: &OciArgs) -> anyhow::Result<vz_oci::Runtime> {
    if args.username.is_some() && args.password.is_none() {
        anyhow::bail!("--username requires --password");
    }

    if args.password.is_some() && args.username.is_none() {
        anyhow::bail!("--password requires --username");
    }

    let mut config = vz_oci::RuntimeConfig::default();
    if let Some(path) = &args.data_dir {
        config.data_dir = path.clone();
    }
    if let Some(path) = &args.bundle_dir {
        config.linux_bundle_dir = Some(path.clone());
    }
    if let Some(path) = &args.install_dir {
        config.linux_install_dir = Some(path.clone());
    }

    config.auth = match (args.docker_config, &args.username, &args.password) {
        (true, _, _) => vz_oci::Auth::DockerConfig,
        (false, Some(username), Some(password)) => vz_oci::Auth::Basic {
            username: username.clone(),
            password: password.clone(),
        },
        _ => vz_oci::Auth::Anonymous,
    };

    Ok(vz_oci::Runtime::new(config))
}

async fn pull_image(runtime: &vz_oci::Runtime, args: PullArgs) -> anyhow::Result<()> {
    info!(image = %args.image, "pulling OCI image");
    let image_id = runtime.pull(&args.image).await?;
    println!(
        "Pulled {image} as {id}",
        image = args.image,
        id = image_id.0
    );
    Ok(())
}

async fn run_image(runtime: vz_oci::Runtime, args: RunArgs) -> anyhow::Result<()> {
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

fn list_images(runtime: &vz_oci::Runtime) -> anyhow::Result<()> {
    let images = runtime.images()?;

    if images.is_empty() {
        println!("No cached images");
        return Ok(());
    }

    println!("{:<35} IMAGE ID", "REFERENCE");
    println!("{}", "-".repeat(70));
    for image in images {
        println!("{:<35} {}", image.reference, image.image_id);
    }

    Ok(())
}

fn prune_images(runtime: &vz_oci::Runtime) -> anyhow::Result<()> {
    let result = runtime.prune_images()?;

    println!(
        "Prune complete: {} refs, {} manifests, {} configs, {} layer dirs",
        result.removed_refs,
        result.removed_manifests,
        result.removed_configs,
        result.removed_layer_dirs,
    );

    Ok(())
}

fn list_containers(runtime: &vz_oci::Runtime) -> anyhow::Result<()> {
    let containers = runtime.list_containers()?;

    if containers.is_empty() {
        println!("No containers tracked");
        return Ok(());
    }

    println!("{:<20} {:<35} {:<10} CREATED", "ID", "IMAGE", "STATUS");
    println!("{}", "-".repeat(90));

    for container in containers {
        let status = match container.status {
            vz_oci::ContainerStatus::Created => "created".to_string(),
            vz_oci::ContainerStatus::Running => "running".to_string(),
            vz_oci::ContainerStatus::Stopped { exit_code } => {
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

fn remove_container(runtime: &vz_oci::Runtime, args: RmArgs) -> anyhow::Result<()> {
    runtime.remove_container(&args.id)?;
    println!("Removed container {id}", id = args.id);
    Ok(())
}

fn build_run_config(args: &RunArgs) -> anyhow::Result<RunConfig> {
    let env = parse_env_vars(&args.env)?;
    let ports = parse_port_mappings(&args.publish)?;

    let network_enabled = if args.no_network { Some(false) } else { None };
    let timeout = args.timeout_secs.map(Duration::from_secs);

    Ok(RunConfig {
        cmd: args.command.clone(),
        working_dir: args.workdir.clone(),
        env,
        user: args.user.clone(),
        ports,
        cpus: args.cpus,
        memory_mb: args.memory_mb,
        network_enabled,
        serial_log_file: args.serial_log_file.clone(),
        timeout,
    })
}

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
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
