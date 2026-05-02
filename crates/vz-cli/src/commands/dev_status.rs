//! `vz status` — show the current project's VM status.

use std::path::PathBuf;

use anyhow::Context;
use clap::Args;
use vz_runtime_proto::runtime_v2;
use vz_runtimed_client::DaemonClient;

use super::runtime_daemon::{connect_control_plane_for_state_db, default_state_db_path};

/// Show the current project's Linux VM status.
#[derive(Args, Debug)]
pub struct DevStatusArgs {
    /// Path to vz.json (default: search cwd and parents).
    #[arg(long)]
    pub config: Option<PathBuf>,
}

pub async fn cmd_dev_status(args: DevStatusArgs) -> anyhow::Result<()> {
    let project_dir = resolve_project_dir(args.config.as_deref())?;
    let sandbox_id = super::dev::sandbox_id_for_project(&project_dir);

    let state_db = default_state_db_path();

    // Try to connect to daemon. If it's not running, that's a valid status.
    let client = match connect_control_plane_for_state_db(&state_db).await {
        Ok(client) => client,
        Err(_) => {
            println!("Daemon: not running");
            println!("VM:     not running");
            println!("Project: {}", project_dir.display());
            return Ok(());
        }
    };

    print_status(&client, &sandbox_id, &project_dir).await
}

async fn print_status(
    client: &DaemonClient,
    sandbox_id: &str,
    project_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let handshake = client.handshake();
    println!(
        "Daemon:  running (v{}, pid file exists)",
        handshake.daemon_version
    );
    println!("Backend: {}", handshake.backend_name);

    let mut client = client.clone();
    match client
        .get_sandbox(runtime_v2::GetSandboxRequest {
            sandbox_id: sandbox_id.to_string(),
            metadata: None,
        })
        .await
    {
        Ok(response) => {
            if let Some(sandbox) = response.sandbox {
                println!("VM:      {} (id: {})", sandbox.state, sandbox_id);
                println!("Project: {}", project_dir.display());
                println!(
                    "Image:   {}",
                    sandbox
                        .labels
                        .get("vz.sandbox.base_image_ref")
                        .map(String::as_str)
                        .unwrap_or("unknown")
                );

                // Show resource info from labels
                if let Some(workspace) = sandbox.labels.get("vz.run.workspace") {
                    println!("Workspace: {workspace}");
                }

                // Show mount info
                let mount_labels: Vec<_> = sandbox
                    .labels
                    .iter()
                    .filter(|(k, _)| k.starts_with("vz.run.mount."))
                    .collect();
                if !mount_labels.is_empty() {
                    println!("Mounts:");
                    for (key, value) in mount_labels {
                        let tag = key.strip_prefix("vz.run.mount.").unwrap_or(key);
                        println!("  {tag} -> {value}");
                    }
                }
            } else {
                println!("VM:      not running");
                println!("Project: {}", project_dir.display());
            }
        }
        Err(_) => {
            println!("VM:      not running");
            println!("Project: {}", project_dir.display());
        }
    }

    Ok(())
}

fn resolve_project_dir(explicit_config: Option<&std::path::Path>) -> anyhow::Result<PathBuf> {
    if let Some(config_path) = explicit_config {
        return config_path
            .parent()
            .map(|p| p.to_path_buf())
            .ok_or_else(|| anyhow::anyhow!("invalid config path"));
    }

    // Search for vz.json to determine project dir, but don't fail if missing.
    let mut dir = std::env::current_dir().context("failed to get current directory")?;
    loop {
        if dir.join("vz.json").is_file() {
            return Ok(dir);
        }
        if !dir.pop() {
            // No vz.json found — use cwd as project dir.
            return std::env::current_dir().context("failed to get current directory");
        }
    }
}
