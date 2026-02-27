//! `vz self-sign` -- Ad-hoc sign the vz binary with required entitlements.

use clap::Args;
use tracing::{info, warn};

/// Ad-hoc sign the vz binary with required entitlements.
///
/// After `cargo install vz-cli`, the binary needs the
/// `com.apple.security.virtualization` entitlement to use
/// Virtualization.framework. This command applies an ad-hoc
/// code signature with the required entitlements.
#[derive(Args, Debug)]
pub struct SelfSignArgs {
    /// Path to the binary to sign. Defaults to the currently running binary.
    #[arg(long)]
    pub binary: Option<std::path::PathBuf>,

    /// Skip auto-signing adjacent `vz-runtimed` binary.
    #[arg(long, default_value_t = false)]
    pub no_runtimed: bool,
}

pub async fn run(args: SelfSignArgs) -> anyhow::Result<()> {
    let binary = match args.binary {
        Some(path) => path,
        None => std::env::current_exe()?,
    };

    if !binary.exists() {
        anyhow::bail!("binary not found: {}", binary.display());
    }

    // Locate the entitlements plist
    let entitlements = find_entitlements()?;
    let mut signed_targets = vec![binary.clone()];
    if !args.no_runtimed
        && let Some(parent) = binary.parent()
    {
        let sibling = parent.join("vz-runtimed");
        if sibling.exists() && sibling != binary {
            signed_targets.push(sibling);
        }
    }

    for target in signed_targets {
        sign_binary(&target, &entitlements)?;
    }

    Ok(())
}

fn sign_binary(binary: &std::path::Path, entitlements: &std::path::Path) -> anyhow::Result<()> {
    info!(binary = %binary.display(), "ad-hoc signing binary");

    let status = std::process::Command::new("codesign")
        .args([
            "--sign",
            "-",
            "--entitlements",
            &entitlements.to_string_lossy(),
            "--force",
            &binary.to_string_lossy(),
        ])
        .status()?;

    if !status.success() {
        anyhow::bail!(
            "codesign failed with exit code {} for {}",
            status.code().unwrap_or(-1),
            binary.display()
        );
    }

    info!(binary = %binary.display(), "signing complete");
    println!("Signed: {}", binary.display());

    let verify = std::process::Command::new("codesign")
        .args(["--verify", "--verbose", &binary.to_string_lossy()])
        .status()?;

    if verify.success() {
        println!("Verification: OK");
    } else {
        warn!(binary = %binary.display(), "signature verification failed");
        println!(
            "Verification: FAILED for {} (binary may not work with Virtualization.framework)",
            binary.display()
        );
    }

    Ok(())
}

/// Find the entitlements plist file.
///
/// Search order:
/// 1. Next to the binary: `<binary-dir>/../entitlements/vz-cli.entitlements.plist`
/// 2. In the vz home: `~/.vz/entitlements/vz-cli.entitlements.plist`
/// 3. In the repo checkout (if running from source): `entitlements/vz-cli.entitlements.plist`
fn find_entitlements() -> anyhow::Result<std::path::PathBuf> {
    let candidates = [
        // Next to the binary (installed via release)
        std::env::current_exe().ok().and_then(|p| {
            p.parent()
                .map(|d| d.join("../entitlements/vz-cli.entitlements.plist"))
        }),
        // In vz home
        Some(crate::registry::vz_home().join("entitlements/vz-cli.entitlements.plist")),
    ];

    for candidate in candidates.into_iter().flatten() {
        if candidate.exists() {
            return Ok(candidate);
        }
    }

    // Fall back to creating a temporary entitlements file
    info!("no entitlements file found, creating temporary one");
    let tmp_dir = std::env::temp_dir().join("vz-self-sign");
    std::fs::create_dir_all(&tmp_dir)?;
    let plist_path = tmp_dir.join("vz-cli.entitlements.plist");
    std::fs::write(
        &plist_path,
        r#"<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>com.apple.security.virtualization</key>
    <true/>
</dict>
</plist>
"#,
    )?;
    Ok(plist_path)
}
