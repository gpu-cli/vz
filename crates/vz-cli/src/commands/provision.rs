//! `vz vm mac provision` -- Provision a VM disk image offline.

use std::path::PathBuf;

use anyhow::Context;
use clap::{Args, ValueEnum};
use tracing::info;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum AgentModeArg {
    /// Use the vz-agent-loader startup manifest (default, no root needed).
    Loader,
    /// Install as a system LaunchDaemon (requires root ownership).
    System,
    /// Install as a per-user LaunchAgent (login-dependent).
    User,
}

impl From<AgentModeArg> for crate::provision::AgentInstallMode {
    fn from(value: AgentModeArg) -> Self {
        match value {
            AgentModeArg::Loader => crate::provision::AgentInstallMode::LoaderManifest,
            AgentModeArg::System => crate::provision::AgentInstallMode::SystemLaunchDaemon,
            AgentModeArg::User => crate::provision::AgentInstallMode::UserLaunchAgent,
        }
    }
}

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

    /// Pinned base selector: immutable base ID, `stable`, or `previous`.
    #[arg(long, value_name = "SELECTOR", conflicts_with = "allow_unpinned")]
    pub base_id: Option<String>,

    /// Explicitly allow unpinned provisioning flow (skip base fingerprint verification).
    #[arg(long, default_value_t = false, conflicts_with = "base_id")]
    pub allow_unpinned: bool,

    /// Username for the dev account.
    #[arg(long, default_value = "dev")]
    pub user: String,

    /// Path to a pre-built guest agent binary. Auto-detected if omitted.
    #[arg(long)]
    pub agent: Option<PathBuf>,

    /// Guest agent install mode: system LaunchDaemon or user LaunchAgent.
    #[arg(long, value_enum, default_value_t = AgentModeArg::Loader)]
    pub agent_mode: AgentModeArg,
}

pub async fn run(args: ProvisionArgs) -> anyhow::Result<()> {
    let image = expand_home(&args.image);
    let mut resolved_base: Option<super::vm_base::ResolvedBase> = None;

    if !image.exists() {
        anyhow::bail!("disk image not found: {}", image.display());
    }

    if let Some(base_selector) = args.base_id.as_deref() {
        let resolved = super::vm_base::verify_image_for_base_id(&image, base_selector)
            .with_context(|| {
                format!(
                    "pinned base verification failed before provisioning image {}",
                    image.display()
                )
            })?;
        print_base_resolution(&resolved);
        resolved_base = Some(resolved);
    } else {
        super::vm_base::require_unpinned_policy(
            args.allow_unpinned,
            "provision",
            "vz vm mac provision --base-id <id> --image <path>",
        )?;
        print_unpinned_warning();
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
        base_selector = ?args.base_id,
        resolved_base_id = ?resolved_base.as_ref().map(|resolved| resolved.base.base_id.as_str()),
        allow_unpinned = args.allow_unpinned,
        user = %user_config.username,
        agent = ?agent_path,
        mode = ?args.agent_mode,
        "provisioning disk image"
    );

    let install_mode = crate::provision::AgentInstallMode::from(args.agent_mode);
    let mode_label = match args.agent_mode {
        AgentModeArg::Loader => "loader",
        AgentModeArg::System => "system",
        AgentModeArg::User => "user",
    };
    print_runtime_policy_message(args.agent_mode);
    let result = crate::provision::provision_image(
        &image,
        &user_config,
        agent_path.as_deref(),
        install_mode,
        None,
    )?;

    println!("Image provisioned successfully: {}", image.display());
    println!(
        "  User: {} (UID {}, auto-login enabled)",
        user_config.username, user_config.uid
    );
    println!("  Password: {}", user_config.password);
    if agent_path.is_some() {
        println!("  Guest agent mode: {}", mode_label);
        println!("  Guest agent: installed");
    }

    if result.needs_ownership_fix {
        let rerun_policy = match resolved_base.as_ref() {
            Some(resolved) => format!(" --base-id {}", resolved.base.base_id),
            None => " --allow-unpinned".to_string(),
        };
        println!("\nWARNING: LaunchDaemon files need root ownership to work.");
        println!("Run this to fix:");
        println!(
            "  sudo vz vm mac provision --image {}{}",
            image.display(),
            rerun_policy
        );
        println!("Or fix manually after mounting:");
        println!(
            "  hdiutil attach -imagekey diskimage-class=CRawDiskImage -nomount {}",
            image.display()
        );
        println!("  # find the Data volume, then:");
        println!(
            "  sudo chown 0:0 /tmp/vz-provision/Library/LaunchDaemons/com.vz.guest-agent.plist"
        );
        println!("  sudo chown 0:0 /tmp/vz-provision/usr/local/bin/vz-guest-agent");
        println!("\nNo-local-sudo alternative (opt-in user runtime policy):");
        println!(
            "  vz vm mac provision --image {}{} --agent-mode user",
            image.display(),
            rerun_policy
        );
        println!(
            "  user mode avoids local sudo but is login/session dependent; system mode remains the default for reliability."
        );
        println!(
            "  For channel workflows ({}, {}), prefer CI-published pre-provisioned artifacts when available.",
            super::vm_base::BASE_CHANNEL_STABLE,
            super::vm_base::BASE_CHANNEL_PREVIOUS
        );
    }

    println!("\nNext steps:");
    println!(
        "  vz vm mac run --image {} --name my-vm --headless",
        image.display()
    );
    println!("  vz vm mac exec my-vm -- whoami");

    Ok(())
}

fn print_unpinned_warning() {
    eprintln!(
        "Warning: running unpinned provision mode. This image is not validated against the supported base matrix."
    );
}

fn print_base_resolution(resolved: &super::vm_base::ResolvedBase) {
    if let Some(channel) = resolved.channel.as_deref() {
        println!(
            "Verified channel '{}' -> pinned base {}  macOS {} ({})",
            channel, resolved.base.base_id, resolved.base.macos_version, resolved.base.macos_build
        );
    } else {
        println!(
            "Verified pinned base: {}  macOS {} ({})",
            resolved.base.base_id, resolved.base.macos_version, resolved.base.macos_build
        );
    }
}

fn print_runtime_policy_message(agent_mode: AgentModeArg) {
    match agent_mode {
        AgentModeArg::Loader => {
            println!(
                "Runtime policy: loader mode (default). Agent starts via vz-agent-loader bootstrap."
            );
            println!("No sudo required. The loader must be pre-installed via bootstrap patch.");
        }
        AgentModeArg::System => {
            println!(
                "Runtime policy: system mode. Agent installed as LaunchDaemon (requires root ownership)."
            );
        }
        AgentModeArg::User => {
            println!(
                "Runtime policy: user mode. Agent installed as per-user LaunchAgent (login-dependent)."
            );
        }
    }
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
