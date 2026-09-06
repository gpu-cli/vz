//! Exercise the consumer preparation API from a reviewed manifest pin.
//! This is a maintainer/integration tool, not a new public vz lifecycle verb.
#[cfg(unix)]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use anyhow::{Context, bail};
    use std::io::Write;
    use std::path::PathBuf;
    use std::time::{Duration, Instant};
    use vz_macos_provision::artifact_cache::Artifact;
    use vz_macos_provision::bootstrap::{BootstrapCache, Progress};

    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if !(2..=3).contains(&args.len()) {
        bail!(
            "usage: prepare_bootstrap <trusted-manifest-pin.json> <absolute-cache> [new-private-disk-path]"
        );
    }
    let pin: Artifact = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    let cache = BootstrapCache::new(PathBuf::from(&args[1]))?;
    let start = Instant::now();
    let mut last = Instant::now();
    let mut previous = None;
    let operation = cache.prepare(&pin, |event| {
        // Throttle intermediate samples; preserve each phase/component transition.
        let value = serde_json::to_value(&event)?;
        let key = (
            value["phase"].clone(),
            value["component"].clone(),
            value["progress"]["phase"].clone(),
        );
        if previous.as_ref() != Some(&key)
            || last.elapsed() >= Duration::from_secs(1)
            || matches!(event, Progress::TemplateReady { .. })
        {
            serde_json::to_writer(std::io::stderr(), &event)?;
            writeln!(std::io::stderr())?;
            last = Instant::now();
        }
        previous = Some(key);
        Ok(())
    });
    let ready = tokio::select! {
        result = operation => result?,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            bail!("bootstrap preparation cancelled");
        }
    };
    let preparation_seconds = start.elapsed().as_secs_f64();
    let mut clone_seconds = None;
    if let Some(destination) = args.get(2) {
        let destination = PathBuf::from(destination);
        let cloned = tokio::task::spawn_blocking(move || {
            let start = Instant::now();
            ready.clone_disk(&destination)?;
            Ok::<_, anyhow::Error>((ready, start.elapsed().as_secs_f64()))
        })
        .await
        .context("clone worker failed")??;
        clone_seconds = Some(cloned.1);
    }
    serde_json::to_writer(
        std::io::stdout(),
        &serde_json::json!({
            "manifest_sha256": pin.sha256,
            "preparation_seconds": preparation_seconds,
            "clone_seconds": clone_seconds,
            "native_machine_ready": false
        }),
    )?;
    writeln!(std::io::stdout())?;
    Ok(())
}

#[cfg(not(unix))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("macOS bootstrap preparation requires a Unix host")
}
