//! `vz vm rm` -- Remove VM runtime metadata and optional image artifacts.

use std::path::{Path, PathBuf};

use clap::Args;
use tracing::{info, warn};

/// Remove VM bookkeeping state, and optionally image artifacts.
#[derive(Args, Debug)]
pub struct RmArgs {
    /// VM name in the local registry.
    pub name: String,

    /// Force stop a running VM before removing metadata.
    #[arg(long)]
    pub force: bool,

    /// Remove image artifacts (.img/.aux/.hwmodel/.machineid/.state/.password).
    #[arg(long)]
    pub delete_image: bool,

    /// Explicit image path to remove with --delete-image.
    #[arg(long)]
    pub image: Option<PathBuf>,
}

pub async fn run(args: RmArgs) -> anyhow::Result<()> {
    info!(
        name = %args.name,
        force = args.force,
        delete_image = args.delete_image,
        image = ?args.image,
        "removing vm"
    );

    let mut registry = crate::registry::Registry::load()?;
    let entry = registry.get(&args.name).cloned();

    if let Some(entry) = entry.as_ref()
        && crate::registry::is_pid_alive(entry.pid)
    {
        if !args.force {
            anyhow::bail!(
                "VM '{}' is running (PID {}). Stop it first or pass --force.",
                args.name,
                entry.pid
            );
        }

        stop_running_vm(&args.name, entry.pid).await;
    }

    let removed_registry_entry = registry.remove(&args.name).is_some();
    if removed_registry_entry {
        registry.save()?;
    }

    let removed_runtime_files = remove_runtime_artifacts(&args.name)?;

    let removed_image_files = if args.delete_image {
        let image_path = match args.image {
            Some(path) => path,
            None => entry
                .as_ref()
                .map(|entry| PathBuf::from(&entry.image))
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "cannot infer image path for '{}'. Pass --image with --delete-image.",
                        args.name
                    )
                })?,
        };
        remove_image_artifacts(&image_path)?
    } else {
        Vec::new()
    };

    if !removed_registry_entry && removed_runtime_files.is_empty() && removed_image_files.is_empty()
    {
        println!("Nothing to remove for '{}'.", args.name);
        return Ok(());
    }

    if removed_registry_entry {
        println!("Removed VM '{}' from registry.", args.name);
    }

    if !removed_runtime_files.is_empty() {
        println!("Removed runtime artifacts:");
        for path in &removed_runtime_files {
            println!("  {}", path.display());
        }
    }

    if !removed_image_files.is_empty() {
        println!("Removed image artifacts:");
        for path in &removed_image_files {
            println!("  {}", path.display());
        }
    }

    Ok(())
}

async fn stop_running_vm(name: &str, pid: u32) {
    match crate::control::connect(name).await {
        Ok(mut stream) => {
            let request = crate::control::ControlRequest::Stop { force: true };
            match crate::control::request(&mut stream, &request).await {
                Ok(crate::control::ControlResponse::Stopped) => {
                    info!(name, "stopped vm via control socket before rm");
                    return;
                }
                Ok(crate::control::ControlResponse::Error { message }) => {
                    warn!(name, error = %message, "control stop returned error, falling back to SIGKILL");
                }
                Ok(other) => {
                    warn!(name, response = ?other, "unexpected control stop response, falling back to SIGKILL");
                }
                Err(error) => {
                    warn!(name, error = %error, "control stop failed, falling back to SIGKILL");
                }
            }
        }
        Err(error) => {
            warn!(name, error = %error, "failed to connect control socket, falling back to SIGKILL");
        }
    }

    #[allow(unsafe_code)]
    unsafe {
        libc::kill(pid as libc::pid_t, libc::SIGKILL);
    }
}

fn remove_runtime_artifacts(name: &str) -> anyhow::Result<Vec<PathBuf>> {
    let run_dir = crate::registry::vz_home().join("run");
    let candidates = [
        run_dir.join(format!("{name}.sock")),
        run_dir.join(format!("{name}.pid")),
        run_dir.join(format!("{name}.lock")),
    ];

    let mut removed = Vec::new();
    for path in candidates {
        if remove_file_if_exists(&path)? {
            removed.push(path);
        }
    }

    Ok(removed)
}

fn image_artifact_paths(image_path: &Path) -> Vec<PathBuf> {
    vec![
        image_path.to_path_buf(),
        image_path.with_extension("aux"),
        image_path.with_extension("hwmodel"),
        image_path.with_extension("machineid"),
        image_path.with_extension("state"),
        image_path.with_extension("password"),
    ]
}

fn remove_image_artifacts(image_path: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut removed = Vec::new();
    for path in image_artifact_paths(image_path) {
        if remove_file_if_exists(&path)? {
            removed.push(path);
        }
    }
    Ok(removed)
}

fn remove_file_if_exists(path: &Path) -> anyhow::Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    std::fs::remove_file(path)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn image_artifact_paths_include_expected_sidecars() {
        let image = PathBuf::from("/tmp/base-user.img");
        let artifacts = image_artifact_paths(&image);
        assert_eq!(artifacts[0], PathBuf::from("/tmp/base-user.img"));
        assert_eq!(artifacts[1], PathBuf::from("/tmp/base-user.aux"));
        assert_eq!(artifacts[2], PathBuf::from("/tmp/base-user.hwmodel"));
        assert_eq!(artifacts[3], PathBuf::from("/tmp/base-user.machineid"));
        assert_eq!(artifacts[4], PathBuf::from("/tmp/base-user.state"));
        assert_eq!(artifacts[5], PathBuf::from("/tmp/base-user.password"));
    }
}
