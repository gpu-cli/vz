//! Maintainer-only Swift installation into a new private native disk clone.
//! Inputs stream over the guest agent only during artifact production. Consumers
//! receive the resulting exact base/patch pair with no host shares or injection.
#[cfg(target_os = "macos")]
mod native {
    use anyhow::{Context, Result, ensure};
    use clap::Parser;
    use sha2::{Digest, Sha256};
    use std::{
        fs, io::Read, os::unix::fs::DirBuilderExt, path::PathBuf, sync::Arc, time::Duration,
    };
    use vz::{DiskConfig, MacPlatformConfig, NetworkConfig, Vm, VmConfigBuilder, VmState};
    use vz_linux::grpc_client::GrpcAgentClient;
    use vz_macos_provision::toolchain::ToolchainManifest;

    #[derive(Parser)]
    struct Args {
        #[arg(long)]
        disk: PathBuf,
        #[arg(long)]
        hardware: PathBuf,
        #[arg(long)]
        auxiliary: PathBuf,
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        payload: PathBuf,
        #[arg(long)]
        toolchain_sha256: String,
    }

    async fn execute(
        client: &mut GrpcAgentClient,
        script: &str,
        seconds: u64,
    ) -> Result<vz::protocol::ExecOutput> {
        let output = tokio::time::timeout(Duration::from_secs(seconds), async {
            client
                .exec_stream(
                    "/bin/sh".into(),
                    vec!["-c".into(), script.into()],
                    Default::default(),
                )
                .await?
                .collect_checked()
                .await
        })
        .await
        .context("maintainer command deadline")??;
        ensure!(
            output.exit_code == 0,
            "maintainer command failed: {output:?}"
        );
        Ok(output)
    }

    async fn upload(
        client: &mut GrpcAgentClient,
        source: &std::path::Path,
        command: &str,
    ) -> Result<()> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
        tokio::time::timeout_at(deadline, async {
            let ticket = client.prepare_machine_exec_request().await?;
            let (stream, id) = client
                .exec_machine_stream_ready_for_request(
                    vz_linux::ContainerExecDispatchGate::new(deadline),
                    ticket,
                    "/bin/sh".into(),
                    vec!["-c".into(), command.into()],
                    Default::default(),
                    None,
                )
                .await?;
            let send = async {
                let mut file = fs::File::open(source)?;
                let mut buffer = vec![0; 64 * 1024];
                let mut sent = 0_u64;
                loop {
                    let count = file.read(&mut buffer)?;
                    if count == 0 {
                        break;
                    }
                    client.stdin_write(id, &buffer[..count]).await?;
                    sent += count as u64;
                    if sent % (128 * 1024 * 1024) == 0 {
                        tracing::info!(sent, "maintainer input transfer");
                    }
                }
                client.stdin_close(id).await?;
                Ok::<_, anyhow::Error>(())
            };
            let receive = async { Ok::<_, anyhow::Error>(stream.collect_checked().await?) };
            let (_, output) = tokio::try_join!(send, receive)?;
            ensure!(
                output.exit_code == 0 && output.stderr.is_empty(),
                "maintainer upload failed: {output:?}"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await
        .context("maintainer upload deadline")?
    }

    pub async fn run() -> Result<()> {
        tracing_subscriber::fmt()
            .with_writer(std::io::stderr)
            .init();
        let args = Args::parse();
        ensure!(
            args.output.is_absolute(),
            "absolute new output directory required"
        );
        let payload = args.payload.canonicalize()?;
        let manifest = ToolchainManifest::from_verified_bytes(
            &fs::read(payload.join("toolchain.json"))?,
            &args.toolchain_sha256,
        )?;
        let archive = payload.join("toolchain.tar.gz");
        ensure!(
            fs::symlink_metadata(&archive)?.is_file(),
            "archive must be a regular file"
        );
        let mut archive_file = fs::File::open(&archive)?;
        ensure!(
            archive_file.metadata()?.len() == manifest.archive.size_bytes,
            "archive size mismatch"
        );
        let mut hash = Sha256::new();
        let mut buffer = vec![0; 4 * 1024 * 1024];
        loop {
            let read = archive_file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        ensure!(
            format!("{:x}", hash.finalize()) == manifest.archive.sha256,
            "archive digest mismatch"
        );
        fs::DirBuilder::new().mode(0o700).create(&args.output)?;
        ensure!(
            std::process::Command::new("/bin/cp")
                .arg("-c")
                .arg(&args.disk)
                .arg(args.output.join("disk.img"))
                .status()?
                .success(),
            "native disk clone failed"
        );
        fs::copy(&args.hardware, args.output.join("hardware-model"))?;
        fs::copy(&args.auxiliary, args.output.join("auxiliary-storage"))?;
        fs::write(
            args.output.join("machine-identifier"),
            vz::install::generate_machine_id_data()?,
        )?;
        let config = VmConfigBuilder::new()
            .boot_macos()
            .cpus(4)
            .memory_mb(8192)
            .disk(DiskConfig {
                id: "system".into(),
                path: args.output.join("disk.img"),
                read_only: false,
            })
            .mac_platform(MacPlatformConfig {
                hardware_model_path: args.output.join("hardware-model"),
                auxiliary_storage_path: args.output.join("auxiliary-storage"),
                machine_identifier_path: args.output.join("machine-identifier"),
            })
            .network(NetworkConfig::None)
            .enable_vsock()
            .build()?;
        let vm = Arc::new(Vm::create(config).await?);
        vm.start().await?;
        let result = async {
            let deadline = tokio::time::Instant::now() + Duration::from_secs(120);
            let mut client = loop {
                ensure!(tokio::time::Instant::now() < deadline, "maintainer agent did not become ready");
                if let Ok(mut client) = GrpcAgentClient::connect_default(Arc::clone(&vm)).await {
                    if client.ping().await.is_ok() { break client; }
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            };
            execute(&mut client, "mkdir -m 700 /private/var/tmp/vz-toolchain-inputs", 30).await?;
            upload(&mut client, &archive, "umask 077; cat > /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz").await?;
            upload(&mut client, &payload.join("toolchain.json"), "umask 077; cat > /private/var/tmp/vz-toolchain-inputs/toolchain.json").await?;
            let hash = execute(&mut client, "/usr/bin/shasum -a 256 /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz", 120).await?;
            ensure!(hash.stdout == format!("{}  /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz\n", manifest.archive.sha256), "guest archive digest mismatch");
            let install = execute(&mut client, "set -eu; test ! -e /Library/Developer/CommandLineTools; mkdir -p /Library/Developer; /usr/bin/tar -xzf /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz -C /Library/Developer; /usr/bin/xcode-select -s /Library/Developer/CommandLineTools; mkdir -p /usr/local/share/vz; cp /private/var/tmp/vz-toolchain-inputs/toolchain.json /usr/local/share/vz/toolchain.json; chmod 644 /usr/local/share/vz/toolchain.json; rm /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz /private/var/tmp/vz-toolchain-inputs/toolchain.json; rmdir /private/var/tmp/vz-toolchain-inputs; sync", 600).await?;
            fs::write(args.output.join("install.json"), serde_json::to_vec_pretty(&install)?)?;
            let (script, expected) = manifest.verification()?;
            let output = execute(&mut client, &script, 60).await?;
            fs::write(args.output.join("verification.json"), serde_json::to_vec_pretty(&output)?)?;
            ensure!(output.stdout == expected && output.stderr.is_empty(), "installed Swift identity differs from pinned receipt: {output:?}");
            Ok::<_, anyhow::Error>(())
        }.await;
        let shutdown = async {
            let mut client = GrpcAgentClient::connect_default(Arc::clone(&vm)).await?;
            let _ = client
                .exec_stream(
                    "/sbin/shutdown".into(),
                    vec!["-h".into(), "now".into()],
                    Default::default(),
                )
                .await?
                .collect()
                .await;
            Ok::<_, anyhow::Error>(())
        };
        let _ = tokio::time::timeout(Duration::from_secs(10), shutdown).await;
        let mut state = vm.state_stream();
        let stopped = tokio::time::timeout(Duration::from_secs(90), async {
            while *state.borrow_and_update() != VmState::Stopped {
                state.changed().await?;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await;
        if !matches!(stopped, Ok(Ok(()))) {
            vm.stop().await?;
        }
        fs::write(
            args.output.join("shutdown.json"),
            serde_json::to_vec_pretty(&serde_json::json!({
                "stopped": *state.borrow() == VmState::Stopped, "forced": !matches!(stopped, Ok(Ok(()))),
                "provisioned": result.is_ok(), "toolchain_sha256": args.toolchain_sha256,
            }))?,
        )?;
        result?;
        ensure!(
            matches!(stopped, Ok(Ok(()))),
            "graceful maintainer shutdown failed"
        );
        Ok(())
    }
}

#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    native::run().await
}
#[cfg(not(target_os = "macos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("native maintainer preparation requires Apple-silicon macOS")
}
