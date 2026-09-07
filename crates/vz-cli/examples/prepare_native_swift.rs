//! Maintainer entry point for the shared local toolchain preparation operation.
#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use clap::Parser;
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();
    vz_cli::native_setup::toolchain_install::run(
        vz_cli::native_setup::toolchain_install::Args::parse(),
    )
    .await
}
#[cfg(not(target_os = "macos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("macOS host required")
}
