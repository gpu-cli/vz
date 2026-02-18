//! `vz provision` -- Provision a VM disk image offline.

use std::path::PathBuf;

use clap::Args;
use tracing::info;

/// Provision a VM disk image with user account and guest agent.
///
/// Mounts the raw disk image, skips Setup Assistant, creates the dev user,
/// enables auto-login, and installs the guest agent binary + launchd plist.
/// The VM can then cold-boot directly into a working state with the agent
/// running.
#[derive(Args, Debug)]
pub struct ProvisionArgs {
    /// Path to the raw disk image (e.g., ~/.vz/images/base.img).
    #[arg(long)]
    pub image: PathBuf,

    /// Username for the dev account.
    #[arg(long, default_value = "dev")]
    pub user: String,

    /// Path to a pre-built guest agent binary. Auto-detected if omitted.
    #[arg(long)]
    pub agent: Option<PathBuf>,
}

pub async fn run(args: ProvisionArgs) -> anyhow::Result<()> {
    let image = expand_home(&args.image);

    if !image.exists() {
        anyhow::bail!("disk image not found: {}", image.display());
    }

    let user_config = crate::provision::UserConfig {
        username: args.user.clone(),
        home: format!("/Users/{}", args.user),
        ..Default::default()
    };

    // Find guest agent binary
    let agent_path = match args.agent {
        Some(ref p) => {
            let p = expand_home(p);
            if !p.exists() {
                anyhow::bail!("guest agent binary not found: {}", p.display());
            }
            Some(p)
        }
        None => {
            let found = crate::provision::find_guest_agent_binary();
            if let Some(ref p) = found {
                info!(path = %p.display(), "auto-detected guest agent binary");
            } else {
                anyhow::bail!(
                    "guest agent binary not found. Build it first:\n  \
                     cd crates && cargo build -p vz-guest-agent\n\
                     Or specify --agent /path/to/vz-guest-agent"
                );
            }
            found
        }
    };

    info!(
        image = %image.display(),
        user = %user_config.username,
        agent = ?agent_path,
        "provisioning disk image"
    );

    let result = crate::provision::provision_image(
        &image,
        &user_config,
        agent_path.as_deref(),
    )?;

    println!("Image provisioned successfully: {}", image.display());
    println!("  User: {} (UID {}, auto-login enabled)", user_config.username, user_config.uid);
    if agent_path.is_some() {
        println!("  Guest agent: installed (starts automatically on boot)");
    }

    if result.needs_ownership_fix {
        println!("\nWARNING: LaunchDaemon files need root ownership to work.");
        println!("Run this to fix:");
        println!(
            "  sudo vz provision --image {}",
            image.display()
        );
        println!("Or fix manually after mounting:");
        println!("  hdiutil attach -imagekey diskimage-class=CRawDiskImage -nomount {}", image.display());
        println!("  # find the Data volume, then:");
        println!("  sudo chown 0:0 /tmp/vz-provision/Library/LaunchDaemons/com.vz.guest-agent.plist");
        println!("  sudo chown 0:0 /tmp/vz-provision/usr/local/bin/vz-guest-agent");
    }

    println!("\nNext steps:");
    println!("  vz run --image {} --name my-vm --headless", image.display());
    println!("  vz exec my-vm -- whoami");

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
