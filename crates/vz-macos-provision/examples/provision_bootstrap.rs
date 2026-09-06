//! Maintainer-only loader provisioning of an owned, stopped image clone.
//! Consumers use an authenticated base/patch pair and never invoke this tool.
use anyhow::{Context, Result, ensure};
use std::path::PathBuf;
use vz_macos_provision::{AgentInstallMode, UserConfig, apply_auto_config, attach_and_mount};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    ensure!(
        args.len() == 3,
        "usage: provision_bootstrap <owned-stopped-image-clone> <loader-binary> <guest-agent-binary>"
    );
    // Privileged work happens only in the maintainer recipe, before publication.
    #[cfg(unix)]
    {
        // SAFETY: geteuid only reads the caller's effective user ID.
        #[allow(unsafe_code)]
        let uid = unsafe { libc::geteuid() };
        ensure!(
            uid == 0,
            "maintainer provisioning requires root to write guest launchd ownership; end users must use the published patch"
        );
    }
    let image = PathBuf::from(&args[0]).canonicalize()?;
    let loader = PathBuf::from(&args[1]).canonicalize()?;
    let agent = PathBuf::from(&args[2]).canonicalize()?;
    for path in [&image, &loader, &agent] {
        ensure!(
            path.is_file(),
            "input must be a regular file: {}",
            path.display()
        );
    }
    ensure!(
        !image.with_extension("state").exists(),
        "refuse an image with saved VM state"
    );
    let disk = attach_and_mount(&image).context("mount owned stopped image")?;
    let user = UserConfig::default();
    let result = (|| {
        apply_auto_config(
            &disk.mount_point,
            &user,
            Some(&loader),
            AgentInstallMode::SystemLaunchDaemon,
            Some("vz-agent-loader"),
        )?;
        apply_auto_config(
            &disk.mount_point,
            &user,
            Some(&agent),
            AgentInstallMode::LoaderManifest,
            None,
        )?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let plist = disk
                .mount_point
                .join("Library/LaunchDaemons/com.vz-agent-loader.plist");
            ensure!(
                plist.metadata()?.uid() == 0,
                "loader LaunchDaemon is not owned by root"
            );
        }
        Ok::<_, anyhow::Error>(())
    })();
    let detached = disk.detach();
    result?;
    detached?;
    Ok(())
}
