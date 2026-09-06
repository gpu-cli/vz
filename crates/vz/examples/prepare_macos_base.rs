//! Maintainer-only IPSW installation. Consumer setup downloads the published base.
#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};

    let args: Vec<_> = std::env::args_os().skip(1).collect();
    ensure!(
        args.len() == 2,
        "usage: prepare_macos_base <verified.ipsw> <new-output-directory>"
    );
    let ipsw = PathBuf::from(&args[0]).canonicalize()?;
    ensure!(ipsw.is_file(), "restore input must be a verified IPSW file");
    let output = PathBuf::from(&args[1]);
    ensure!(output.is_absolute(), "output must be absolute");
    let parent = output
        .parent()
        .context("output needs a parent")?
        .canonicalize()?;
    let output = parent.join(output.file_name().context("output needs a name")?);
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&output)
        .context("output directory must be new; existing images are never replaced")?;
    let disk = output.join("base.img");
    let started = Instant::now();
    let operation = vz::install_macos(vz::IpswSource::Path(ipsw), &disk, 80 * 1024 * 1024 * 1024);
    tokio::pin!(operation);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(30));
    let installed = loop {
        tokio::select! {
            result = &mut operation => break result?,
            _ = heartbeat.tick() => eprintln!("{}", serde_json::json!({"phase":"installing_macos", "elapsed_seconds":started.elapsed().as_secs()})),
        }
    };
    writeln!(
        std::io::stdout(),
        "{}",
        serde_json::json!({
            "phase":"base_installed", "disk":installed.disk_path,
            "hardware_model":installed.hardware_model_path,
            "machine_identifier":installed.machine_identifier_path,
            "auxiliary_storage":installed.auxiliary_storage_path,
            "elapsed_seconds":started.elapsed().as_secs(),
            "consumer_e2e_validated":false
        })
    )?;
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("macOS host required")
}
