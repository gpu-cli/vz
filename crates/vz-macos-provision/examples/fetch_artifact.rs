//! Maintainer artifact acquisition with the same progress events intended for Up.
//! A pin must come from reviewed release inputs, never from untrusted workload data.
use anyhow::{Result, bail};
use indicatif::{ProgressBar, ProgressStyle};
use std::io::{IsTerminal, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use vz_macos_provision::artifact_cache::{Artifact, ArtifactCache, Phase};

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.len() != 2 {
        bail!("usage: fetch_artifact <trusted-pin.json> <absolute-cache-directory>");
    }
    let artifact: Artifact = serde_json::from_slice(&std::fs::read(&args[0])?)?;
    artifact.validate()?;
    let cache = ArtifactCache::new(PathBuf::from(&args[1]))?;
    let bar = if std::io::stderr().is_terminal() {
        let bar = ProgressBar::new(artifact.size_bytes);
        bar.set_style(ProgressStyle::with_template(
            "{msg} {wide_bar} {bytes}/{total_bytes} {eta}",
        )?);
        Some(bar)
    } else {
        None
    };
    let mut previous = None;
    let mut last = Instant::now();
    let operation = cache.ensure(&artifact, |p| {
        if let Some(bar) = &bar {
            bar.set_message(match p.phase {
                Phase::Waiting => "Waiting",
                Phase::Downloading => "Downloading macOS",
                Phase::VerifyingCache => "Verifying cached macOS",
                Phase::Available => "Available",
            });
            bar.set_position(p.completed);
        } else if previous != Some(p.phase)
            || p.completed == p.total
            || last.elapsed() >= Duration::from_secs(1)
        {
            serde_json::to_writer(std::io::stderr(), &p)?;
            writeln!(std::io::stderr())?;
            last = Instant::now();
        }
        previous = Some(p.phase);
        Ok(())
    });
    let result = tokio::select! {
        result = operation => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            Err(anyhow::anyhow!("artifact download cancelled"))
        }
    };
    if let Some(bar) = bar {
        bar.finish_and_clear();
    }
    serde_json::to_writer(std::io::stdout(), &result?)?;
    writeln!(std::io::stdout())?;
    Ok(())
}
