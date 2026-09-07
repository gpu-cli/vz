//! Explicit host setup; Environment lifecycle remains in the `vz` executable.
#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .init();
    vz_cli::native_setup::run(vz_cli::native_setup::Args::parse()).await
}
#[cfg(not(target_os = "macos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("macOS setup requires an Apple-silicon Mac")
}
