//! `vz run` -- Start a VM with optional mounts.

use std::path::PathBuf;
use std::sync::Arc;

use clap::Args;
use tracing::{error, info, warn};

/// Start a VM with optional mounts.
#[derive(Args, Debug)]
pub struct RunArgs {
    /// Path to the disk image.
    #[arg(long)]
    pub image: PathBuf,

    /// VirtioFS mount in tag:host-path format. Can be repeated.
    #[arg(long, value_name = "TAG:PATH")]
    pub mount: Vec<String>,

    /// Number of CPU cores.
    #[arg(long, default_value_t = 4)]
    pub cpus: u32,

    /// Memory in GB.
    #[arg(long, default_value_t = 8)]
    pub memory: u64,

    /// Run without display (server mode).
    #[arg(long)]
    pub headless: bool,

    /// Restore from saved state instead of cold boot.
    #[arg(long)]
    pub restore: Option<PathBuf>,

    /// VM name for registry tracking.
    #[arg(long)]
    pub name: Option<String>,
}

pub async fn run(args: RunArgs) -> anyhow::Result<()> {
    let name = args.name.clone().unwrap_or_else(|| "default".to_string());

    info!(
        name = %name,
        image = %args.image.display(),
        cpus = args.cpus,
        memory_gb = args.memory,
        headless = args.headless,
        "starting VM"
    );

    // Verify image exists
    if !args.image.exists() {
        anyhow::bail!(
            "disk image not found: {}\n\nRun `vz init` to create a golden image.",
            args.image.display()
        );
    }

    // Build VM configuration
    let mut builder = vz::VmConfigBuilder::new()
        .cpus(args.cpus)
        .memory_gb(args.memory as u32)
        .boot_loader(vz::BootLoader::MacOS)
        .disk(args.image.clone())
        .enable_vsock();

    // Look for platform identity files alongside the disk image
    let hw_model_path = args.image.with_extension("hwmodel");
    let machine_id_path = args.image.with_extension("machineid");
    let aux_path = args.image.with_extension("aux");

    if hw_model_path.exists() && machine_id_path.exists() && aux_path.exists() {
        builder = builder.mac_platform(vz::MacPlatformConfig {
            hardware_model_path: hw_model_path,
            machine_identifier_path: machine_id_path,
            auxiliary_storage_path: aux_path,
        });
    } else {
        anyhow::bail!(
            "platform identity files not found alongside {}.\n\
             Expected: .hwmodel, .machineid, .aux files.\n\
             These are created by `vz init`.",
            args.image.display()
        );
    }

    if !args.headless {
        builder = builder.with_display();
    }

    // Parse and add VirtioFS mounts
    for mount_str in &args.mount {
        let parts: Vec<&str> = mount_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            anyhow::bail!("invalid mount format: '{}'. Expected TAG:PATH", mount_str);
        }
        let tag = parts[0].to_string();
        let source = PathBuf::from(parts[1]);
        if !source.exists() {
            anyhow::bail!("mount source path does not exist: {}", source.display());
        }
        builder = builder.shared_dir(vz::SharedDirConfig {
            tag,
            source,
            read_only: false,
        });
    }

    // Build and create VM
    let config = builder
        .build()
        .map_err(|e| anyhow::anyhow!("invalid VM config: {e}"))?;
    let vm = vz::Vm::create(config)
        .await
        .map_err(|e| anyhow::anyhow!("failed to create VM: {e}"))?;
    let vm = Arc::new(vm);

    // Start or restore
    if let Some(ref state_path) = args.restore {
        info!(state = %state_path.display(), "restoring VM from saved state");
        vm.restore_state(state_path)
            .await
            .map_err(|e| anyhow::anyhow!("restore failed: {e}"))?;
        vm.resume()
            .await
            .map_err(|e| anyhow::anyhow!("resume failed: {e}"))?;
        info!("VM restored and running");
    } else {
        info!("starting VM (cold boot)");
        vm.start()
            .await
            .map_err(|e| anyhow::anyhow!("start failed: {e}"))?;
        info!("VM running");
    }

    // Register in registry
    let mut registry = crate::registry::Registry::load()?;
    registry.insert(
        name.clone(),
        crate::registry::VmEntry {
            image: args.image.to_string_lossy().to_string(),
            state: "running".to_string(),
            pid: std::process::id(),
            vsock_port: Some(vz_sandbox::protocol::AGENT_PORT),
            cpus: Some(args.cpus),
            memory_gb: Some(args.memory),
            mounts: args
                .mount
                .iter()
                .filter_map(|m| {
                    let parts: Vec<&str> = m.splitn(2, ':').collect();
                    if parts.len() == 2 {
                        Some(crate::registry::Mount {
                            tag: parts[0].to_string(),
                            source: parts[1].to_string(),
                        })
                    } else {
                        None
                    }
                })
                .collect(),
        },
    );
    registry.save()?;

    println!("VM '{}' is running (PID {})", name, std::process::id());

    // Start control server
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let vm_stopped = Arc::new(tokio::sync::Notify::new());
    let control_vm = vm.clone();
    let control_name = name.clone();
    let control_stopped = vm_stopped.clone();
    let control_handle = tokio::spawn(async move {
        if let Err(e) =
            crate::control::serve(&control_name, control_vm, control_stopped, shutdown_rx).await
        {
            error!(error = %e, "control server error");
        }
    });

    // Wait for Ctrl+C or VM stopped via control socket
    let stopped_by_control = tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("received Ctrl+C, stopping VM");
            false
        }
        _ = vm_stopped.notified() => {
            info!("VM stopped via control socket");
            true
        }
    };

    // Shutdown control server
    let _ = shutdown_tx.send(true);
    let _ = control_handle.await;

    // Only stop VM if we got Ctrl+C (control socket already stopped it)
    if !stopped_by_control {
        if let Err(e) = vm.request_stop().await {
            warn!(error = %e, "graceful stop failed, forcing");
            let _ = vm.stop().await;
        }
    }

    // Update registry
    let mut registry = crate::registry::Registry::load()?;
    registry.remove(&name);
    registry.save()?;

    // Clean up control socket
    let socket = crate::control::socket_path(&name);
    let _ = std::fs::remove_file(&socket);

    println!("VM '{}' stopped", name);

    if stopped_by_control {
        // Hard-exit to prevent dropping the VM object, which triggers ObjC
        // deallocation and flushes disk buffers. After save+stop the disk must
        // remain byte-identical to the moment the state was captured.
        std::process::exit(0);
    }

    Ok(())
}
