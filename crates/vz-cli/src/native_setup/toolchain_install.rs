//! Guest toolchain preparation shared by setup and setup tooling.
use anyhow::{Context, Result, ensure};
use clap::Parser;
use sha2::{Digest, Sha256};
use std::{fs, io::Read, os::unix::fs::DirBuilderExt, path::PathBuf, sync::Arc, time::Duration};
use vz::{DiskConfig, MacPlatformConfig, NetworkConfig, Vm, VmConfigBuilder, VmState};
use vz_linux::grpc_client::GrpcAgentClient;
use vz_macos_provision::toolchain::ToolchainManifest;

#[derive(Parser)]
pub struct Args {
    #[arg(long)]
    pub disk: PathBuf,
    #[arg(long)]
    pub hardware: PathBuf,
    #[arg(long)]
    pub auxiliary: PathBuf,
    #[arg(long)]
    pub output: PathBuf,
    #[arg(long)]
    pub payload: PathBuf,
    #[arg(long)]
    pub toolchain_sha256: String,
    /// Verify an already-installed matching receipt in a new private clone.
    #[arg(long)]
    pub reuse_installed_toolchain: bool,
    /// Accept the supplied Xcode license only after explicit operator approval.
    #[arg(long)]
    pub accept_xcode_license: bool,
    /// Run the checked-in Swift fixture as dev before publishing a candidate.
    #[arg(long)]
    pub fixture: Option<PathBuf>,
    /// Require this exact OS version and build during local setup.
    #[arg(long)]
    pub expected_os: Option<String>,
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
    .context("setup command deadline")??;
    ensure!(output.exit_code == 0, "setup command failed: {output:?}");
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
                    tracing::info!(sent, "setup input transfer");
                }
            }
            client.stdin_close(id).await?;
            Ok::<_, anyhow::Error>(())
        };
        let receive = async { Ok::<_, anyhow::Error>(stream.collect_checked().await?) };
        let (_, output) = tokio::try_join!(send, receive)?;
        ensure!(
            output.exit_code == 0 && output.stderr.is_empty(),
            "setup upload failed: {output:?}"
        );
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("setup upload deadline")?
}

async fn preflight(
    client: &mut GrpcAgentClient,
    fixture: &std::path::Path,
    output: &std::path::Path,
) -> Result<()> {
    const ROOT: &str = "/Users/dev/.vz-toolchain-preflight";
    execute(client, "set -eu; test ! -e /Users/dev/.vz-toolchain-preflight; install -d -m 700 -o dev -g staff /Users/dev/.vz-toolchain-preflight /Users/dev/.vz-toolchain-preflight/Sources/NativeProbe /Users/dev/.vz-toolchain-preflight/Tests/NativeProbeTests", 30).await?;
    for relative in [
        "Package.swift",
        "Sources/NativeProbe/NativeProbe.swift",
        "Tests/NativeProbeTests/NativeProbeTests.swift",
    ] {
        upload(
            client,
            &fixture.join(relative),
            &format!("set -eu; cat > '{ROOT}/{relative}'; chmod 644 '{ROOT}/{relative}'"),
        )
        .await?;
    }
    for (name, command, arguments) in [
        (
            "build",
            "/usr/bin/xcrun",
            vec!["swift", "build", "-c", "release"],
        ),
        ("test", "/usr/bin/xcrun", vec!["swift", "test"]),
        ("run", "./.build/release/native-probe", vec![]),
    ] {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(600);
        let observed = tokio::time::timeout_at(deadline, async {
            let ticket = client.prepare_machine_exec_request().await?;
            let (stream, _) = client
                .exec_machine_stream_ready_for_request(
                    vz_linux::ContainerExecDispatchGate::new(deadline),
                    ticket,
                    command.into(),
                    arguments.into_iter().map(str::to_string).collect(),
                    vz_linux::ExecOptions {
                        user: Some("dev".into()),
                        working_dir: Some(ROOT.into()),
                        ..Default::default()
                    },
                    None,
                )
                .await?;
            Ok::<_, anyhow::Error>(stream.collect_checked().await?)
        })
        .await
        .context("setup Swift fixture deadline")??;
        fs::write(
            output.join(format!("preflight-{name}.json")),
            serde_json::to_vec_pretty(&observed)?,
        )?;
        ensure!(
            observed.exit_code == 0,
            "setup Swift {name} failed: {observed:?}"
        );
        if name == "test" {
            ensure!(
                observed
                    .stdout
                    .contains("physicalMacCannotSatisfyGuestProbe")
                    && observed.stdout.contains("passed"),
                "expected Swift test did not run"
            );
        }
        if name == "run" {
            let record: serde_json::Value = serde_json::from_str(&observed.stdout)?;
            ensure!(
                record["hardware_model"] == "VirtualMac2,1",
                "preflight did not run in a native VM"
            );
        }
    }
    execute(
        client,
        "rm -rf /Users/dev/.vz-toolchain-preflight; sync",
        30,
    )
    .await?;
    Ok(())
}

pub async fn run(args: Args) -> Result<()> {
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
            ensure!(tokio::time::Instant::now() < deadline, "setup agent did not become ready");
            if let Ok(mut client) = GrpcAgentClient::connect_default(Arc::clone(&vm)).await {
                if client.ping().await.is_ok() { break client; }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let os = execute(&mut client, "/usr/bin/sw_vers -productVersion; /usr/bin/sw_vers -buildVersion", 30).await?;
        if let Some(expected) = &args.expected_os {
            ensure!(os.stdout.trim().replace('\n', "/") == *expected, "installed macOS version/build differs from recipe");
        }
        fs::write(args.output.join("guest-os.json"), serde_json::to_vec_pretty(&os)?)?;
        // Older agent-only DEV images created the account but left its
        // home owned by root. Repair this while producing the artifact;
        // installed consumers must never need a chown step.
        execute(&mut client, "set -eu; test \"$(id -u dev)\" = 501; test \"$(dscl . -read /Users/dev NFSHomeDirectory)\" = 'NFSHomeDirectory: /Users/dev'; test ! -L /Users/dev; chown dev:staff /Users/dev; chmod 700 /Users/dev", 30).await?;
        let home = client.exec_stream("/bin/sh".into(), vec!["-c".into(), "set -eu; test \"$HOME\" = /Users/dev; test -w \"$HOME\"; directory=$(mktemp -d \"$HOME/vz-home-check.XXXXXX\"); rmdir \"$directory\"".into()], vz_linux::ExecOptions { user: Some("dev".into()), working_dir: Some("/Users/dev".into()), ..Default::default() }).await?.collect_checked().await?;
        ensure!(home.exit_code == 0, "native dev home is not usable: {home:?}");
        fs::write(args.output.join("dev-home.json"), serde_json::to_vec_pretty(&home)?)?;
        if args.reuse_installed_toolchain {
            let receipt = execute(&mut client, "head -c 32769 /usr/local/share/vz/toolchain.json", 10).await?;
            let installed = ToolchainManifest::from_verified_bytes(receipt.stdout.as_bytes(), &args.toolchain_sha256)?;
            ensure!(installed == manifest, "existing toolchain receipt differs");
        } else {
        execute(&mut client, "mkdir -m 700 /private/var/tmp/vz-toolchain-inputs", 30).await?;
        upload(&mut client, &archive, "umask 077; cat > /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz").await?;
        upload(&mut client, &payload.join("toolchain.json"), "umask 077; cat > /private/var/tmp/vz-toolchain-inputs/toolchain.json").await?;
        let hash = execute(&mut client, "/usr/bin/shasum -a 256 /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz", 120).await?;
        ensure!(hash.stdout == format!("{}  /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz\n", manifest.archive.sha256), "guest archive digest mismatch");
        let (installation, parent) = match manifest.layout {
            vz_macos_provision::toolchain::ToolchainLayout::Clt => ("/Library/Developer/CommandLineTools", "/Library/Developer"),
            vz_macos_provision::toolchain::ToolchainLayout::Xcode => ("/Applications/Xcode.app", "/Applications"),
        };
        let script = format!("set -eu; test ! -e '{installation}'; mkdir -p '{parent}'; /usr/bin/tar -xzf /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz -C '{parent}'; /usr/bin/xcode-select -s '{}'; mkdir -p /usr/local/share/vz; cp /private/var/tmp/vz-toolchain-inputs/toolchain.json /usr/local/share/vz/toolchain.json; chmod 644 /usr/local/share/vz/toolchain.json; rm /private/var/tmp/vz-toolchain-inputs/toolchain.tar.gz /private/var/tmp/vz-toolchain-inputs/toolchain.json; rmdir /private/var/tmp/vz-toolchain-inputs; sync", manifest.developer_dir());
        let install = execute(&mut client, &script, 600).await?;
        fs::write(args.output.join("install.json"), serde_json::to_vec_pretty(&install)?)?;
        }
        if args.accept_xcode_license {
            ensure!(manifest.layout == vz_macos_provision::toolchain::ToolchainLayout::Xcode, "license acceptance requires an Xcode receipt");
            let accepted = execute(&mut client, "/usr/bin/xcodebuild -license accept", 60).await?;
            fs::write(args.output.join("license-acceptance.json"), serde_json::to_vec_pretty(&accepted)?)?;
        }
        let (script, expected) = manifest.verification()?;
        let output = execute(&mut client, &script, 60).await?;
        fs::write(args.output.join("verification.json"), serde_json::to_vec_pretty(&output)?)?;
        ensure!(output.stdout == expected && output.stderr.is_empty(), "installed Swift identity differs from pinned receipt: {output:?}");
        if let Some(fixture) = &args.fixture {
            preflight(&mut client, fixture, &args.output).await?;
        }
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
        "graceful setup shutdown failed"
    );
    Ok(())
}
