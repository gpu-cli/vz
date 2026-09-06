//! DEV native VM prerequisite probe. This is not the installed five-verb gate.
#[cfg(target_os = "macos")]
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    use anyhow::{Context, ensure};
    use objc2::AnyThread;
    use objc2_virtualization::VZMacMachineIdentifier;
    use std::io::Write;
    use std::os::unix::fs::DirBuilderExt;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
    use vz::{DiskConfig, MacPlatformConfig, NetworkConfig, Vm, VmConfigBuilder, VmState};
    use vz_linux::{ExecOptions, grpc_client::GrpcAgentClient};

    let args: Vec<_> = std::env::args_os().skip(1).collect();
    ensure!(
        args.len() == 4,
        "usage: native_bootstrap_probe <patched-disk> <hardware-model> <aux-seed> <new-machine-directory>"
    );
    let disk = PathBuf::from(&args[0]).canonicalize()?;
    let hardware = PathBuf::from(&args[1]).canonicalize()?;
    let auxiliary = PathBuf::from(&args[2]).canonicalize()?;
    let machine = PathBuf::from(&args[3]);
    ensure!(machine.is_absolute(), "Machine directory must be absolute");
    std::fs::DirBuilder::new()
        .mode(0o700)
        .create(&machine)
        .context("Machine directory must be new")?;
    let machine = machine.canonicalize()?;
    let cloned = machine.join("disk.img");
    let clone_started = tokio::time::Instant::now();
    ensure!(
        std::process::Command::new("/bin/cp")
            .arg("-c")
            .arg(&disk)
            .arg(&cloned)
            .status()?
            .success(),
        "APFS disk clone failed"
    );
    let clone_seconds = clone_started.elapsed().as_secs_f64();
    std::fs::copy(&hardware, machine.join("hardware-model"))?;
    std::fs::copy(&auxiliary, machine.join("auxiliary-storage"))?;
    // SAFETY: framework creates a fresh opaque identifier. It is serialized on
    // this thread immediately and no Objective-C reference crosses an await.
    #[allow(unsafe_code)]
    let identity = unsafe {
        let id = VZMacMachineIdentifier::init(VZMacMachineIdentifier::alloc());
        id.dataRepresentation().to_vec()
    };
    std::fs::write(machine.join("machine-identifier"), &identity)?;
    let config = VmConfigBuilder::new()
        .boot_macos()
        .cpus(4)
        .memory_mb(8192)
        .disk(DiskConfig {
            id: "system".into(),
            path: cloned,
            read_only: false,
        })
        .mac_platform(MacPlatformConfig {
            hardware_model_path: machine.join("hardware-model"),
            machine_identifier_path: machine.join("machine-identifier"),
            auxiliary_storage_path: machine.join("auxiliary-storage"),
        })
        .network(NetworkConfig::None)
        .enable_vsock()
        .build()?;
    let vm = Arc::new(Vm::create(config).await?);
    for cycle in 0..2 {
        let operation = async {
            vm.start().await?;
            let started = tokio::time::Instant::now();
            let mut loader = loop {
                ensure!(
                    started.elapsed() < Duration::from_secs(300),
                    "loader did not become ready in 300 seconds"
                );
                match tokio::time::timeout(Duration::from_secs(5), vm.vsock_connect(7420)).await {
                    Ok(Ok(stream)) => break BufReader::new(stream),
                    _ => tokio::time::sleep(Duration::from_secs(2)).await,
                }
            };
            loader.get_mut().write_all(b"{\"type\":\"ping\"}\n").await?;
            let mut pong = String::new();
            tokio::time::timeout(
                Duration::from_secs(10),
                loader.take(4097).read_line(&mut pong),
            )
            .await??;
            ensure!(
                pong.len() <= 4096 && pong.ends_with('\n'),
                "loader response exceeded its frame bound"
            );
            let pong: serde_json::Value = serde_json::from_str(&pong)?;
            ensure!(pong["type"] == "pong", "loader returned no pong");
            let mut client = loop {
                ensure!(
                    started.elapsed() < Duration::from_secs(300),
                    "guest agent did not become ready"
                );
                if let Ok(mut client) = GrpcAgentClient::connect_default(vm.clone()).await {
                    if client.ping().await.is_ok() {
                        break client;
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
            };
            let info = client.system_info().await?;
            let marker = identity
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let marker_script = if cycle == 0 {
                format!(
                    "set -eu; printf '%s' '{marker}' > /private/var/tmp/vz-native-probe-marker; /bin/sync"
                )
            } else {
                format!("test \"$(cat /private/var/tmp/vz-native-probe-marker)\" = '{marker}'")
            };
            let persistence = client
                .exec_stream(
                    "/bin/sh".into(),
                    vec!["-c".into(), marker_script],
                    ExecOptions::default(),
                )
                .await?
                .collect()
                .await;
            ensure!(
                persistence.exit_code == 0,
                "guest disk persistence check failed: {persistence:?}"
            );
            ensure!(
                std::fs::read(machine.join("machine-identifier"))? == identity,
                "VM identity changed during restart"
            );
            let probe = client.exec_stream("/bin/sh".into(), vec!["-c".into(),
            "/usr/bin/sw_vers -productVersion; /usr/bin/sw_vers -buildVersion; /usr/sbin/sysctl -n hw.model".into()], ExecOptions::default()).await?.collect().await;
            ensure!(
                probe.exit_code == 0 && probe.stdout.contains("VirtualMac"),
                "native guest probe failed: {probe:?}"
            );
            let inventory = client.exec_stream("/bin/sh".into(), vec!["-c".into(),
            "/usr/bin/shasum -a 256 /usr/local/bin/vz-agent-loader /usr/local/bin/vz-guest-agent; /usr/bin/xcode-select -p".into()], ExecOptions::default()).await?.collect().await;
            let record = serde_json::json!({
                "scope":"DEV_NATIVE_BOOT_AGENT_PREREQUISITE_NOT_INSTALLED_E2E",
                "machine_directory":machine,"loader":pong,"guest_probe":probe,"cycle":cycle,
                "apfs_disk_clone_seconds":clone_seconds,
                "disk_persistence_verified":cycle == 1,
                "agent_os_version":info.os_version,"agent_protocol_revision":info.agent_protocol_revision,
                "boot_to_agent_seconds":started.elapsed().as_secs_f64(),"fresh_machine_identifier":cycle == 0,
                "guest_binary_and_toolchain_inventory":inventory,
                "consumer_e2e_validated":false
            });
            std::fs::write(
                machine.join(if cycle == 0 {
                    "probe.json"
                } else {
                    "restart-probe.json"
                }),
                serde_json::to_vec_pretty(&record)?,
            )?;
            writeln!(std::io::stdout(), "{record}")?;
            Ok::<_, anyhow::Error>(())
        };
        let result = tokio::time::timeout(Duration::from_secs(360), operation)
            .await
            .context("native probe exceeded deadline")
            .and_then(|r| r);
        // The only VM we may stop is the one created above from our new private copy.
        let mut state = vm.state_stream();
        let graceful = vm.request_stop().await;
        if graceful.is_ok() {
            let _ = tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    if *state.borrow_and_update() == VmState::Stopped {
                        break;
                    }
                    if state.changed().await.is_err() {
                        break;
                    }
                }
            })
            .await;
        }
        let forced_stop = *state.borrow() != VmState::Stopped;
        if forced_stop {
            tokio::time::timeout(Duration::from_secs(30), vm.stop())
                .await
                .context("owned probe VM stop exceeded deadline")?
                .context("stop owned probe VM")?;
        }
        ensure!(*state.borrow() == VmState::Stopped, "owned VM did not stop");
        std::fs::write(
            machine.join(if cycle == 0 {
                "shutdown.json"
            } else {
                "restart-shutdown.json"
            }),
            serde_json::to_vec_pretty(&serde_json::json!({
                "stopped":true,"forced_stop":forced_stop,"probe_succeeded":result.is_ok(),
                "consumer_e2e_validated":false
            }))?,
        )?;
        result?;
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn main() -> anyhow::Result<()> {
    anyhow::bail!("macOS host required")
}
