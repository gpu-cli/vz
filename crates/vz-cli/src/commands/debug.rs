//! `vz debug` — advanced debugging and low-level operations.
//!
//! Hidden from default help output. Groups raw container, Docker compat,
//! VM infrastructure, and state entity management commands.

use clap::{Args, Subcommand};

/// Advanced debugging and low-level operations.
#[derive(Args, Debug)]
#[command(hide = true)]
pub struct DebugArgs {
    #[command(subcommand)]
    pub action: DebugCommand,
}

#[derive(Subcommand, Debug)]
pub enum DebugCommand {
    /// Raw OCI container operations.
    Container(ContainerDebugArgs),

    /// Docker-compatible translation shim.
    Docker(super::docker::DockerArgs),

    /// Manage macOS virtual machines (legacy alias for `vz vm mac`).
    #[cfg(target_os = "macos")]
    Vm(super::vm::MacVmArgs),

    /// Manage lease access grants.
    Lease(super::lease::LeaseArgs),

    /// Manage container executions.
    Execution(super::execution::ExecutionArgs),

    /// Manage checkpoint fingerprints and lineage.
    Checkpoint(super::checkpoint::CheckpointArgs),

    /// Manage asynchronous build operations.
    Build(super::build_mgmt::BuildMgmtArgs),

    /// Manage sandbox filesystem operations.
    File(super::file::FileArgs),
}

/// Raw OCI container operations for debugging.
#[derive(Args, Debug)]
pub struct ContainerDebugArgs {
    #[command(subcommand)]
    pub action: ContainerDebugCommand,
}

#[derive(Subcommand, Debug)]
pub enum ContainerDebugCommand {
    /// Run a container from an OCI image.
    Run(Box<super::oci::RunArgs>),

    /// Create and start a long-lived container.
    Create(Box<super::oci::CreateArgs>),

    /// Execute a command in a running container.
    Exec(super::oci::ExecArgs),

    /// List containers.
    Ps(super::oci::PsArgs),

    /// Stop a running container.
    Stop(super::oci::StopArgs),

    /// Remove container metadata and rootfs.
    Rm(super::oci::RmArgs),

    /// Show container logs.
    Logs(super::oci::LogsArgs),
}

/// Run the debug subcommand.
pub async fn run(args: DebugArgs) -> anyhow::Result<()> {
    match args.action {
        DebugCommand::Container(container_args) => run_container_debug(container_args).await,
        DebugCommand::Docker(docker_args) => super::docker::run(docker_args).await,
        #[cfg(target_os = "macos")]
        DebugCommand::Vm(vm_args) => super::vm::run_mac(vm_args).await,
        DebugCommand::Lease(lease_args) => super::lease::run(lease_args).await,
        DebugCommand::Execution(exec_args) => super::execution::run(exec_args).await,
        DebugCommand::Checkpoint(checkpoint_args) => super::checkpoint::run(checkpoint_args).await,
        DebugCommand::Build(build_args) => super::build_mgmt::run(build_args).await,
        DebugCommand::File(file_args) => super::file::run(file_args).await,
    }
}

async fn run_container_debug(args: ContainerDebugArgs) -> anyhow::Result<()> {
    match args.action {
        ContainerDebugCommand::Run(run_args) => super::oci::run_container(*run_args).await,
        ContainerDebugCommand::Create(create_args) => super::oci::run_create(*create_args).await,
        ContainerDebugCommand::Exec(exec_args) => super::oci::run_exec(exec_args).await,
        ContainerDebugCommand::Ps(ps_args) => super::oci::run_ps(ps_args).await,
        ContainerDebugCommand::Stop(stop_args) => super::oci::run_stop(stop_args).await,
        ContainerDebugCommand::Rm(rm_args) => super::oci::run_rm(rm_args).await,
        ContainerDebugCommand::Logs(logs_args) => super::oci::run_logs(logs_args).await,
    }
}
